import os
import time
import json
from pyspark.sql import SparkSession, functions as F, types as T, Window
from delta.tables import DeltaTable

# =========================
# Env / Config
# =========================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_POS_TRANSACTIONS = os.getenv("TOPIC_POS_TRANSACTIONS", "pos_transactions")
TOPIC_BATCH_STATE = os.getenv("TOPIC_BATCH_STATE", "shelf_batch_state")

JDBC_PG_URL = os.getenv("JDBC_PG_URL")
JDBC_PG_USER = os.getenv("JDBC_PG_USER")
JDBC_PG_PASSWORD = os.getenv("JDBC_PG_PASSWORD")
BOOTSTRAP_FROM_PG = os.getenv("BOOTSTRAP_FROM_PG", "0") in ("1", "true", "True")

DELTA_ROOT = os.getenv("DELTA_ROOT", "/delta")
BATCH_STATE_PATH = f"{DELTA_ROOT}/cleansed/shelf_batch_state"
CHECKPOINT_PATH = f"{DELTA_ROOT}/_checkpoints/batch_state_updater"
POS_RAW_PATH = os.getenv("DL_POS_TRANSACTIONS_PATH", f"{DELTA_ROOT}/raw/pos_transactions")
POS_RAW_CHECKPOINT = os.getenv("CKP_POS_TRANSACTIONS", f"{CHECKPOINT_PATH}/raw_pos")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "earliest")

# =========================
# Spark Session
# =========================
spark = (
    SparkSession.builder.appName("BatchStateUpdater")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# =========================
# Schemas
# =========================
schema_tx = T.StructType([
    T.StructField("event_type", T.StringType()),
    T.StructField("transaction_id", T.StringType()),
    T.StructField("customer_id", T.StringType()),
    T.StructField("timestamp", T.StringType()),
    T.StructField("items", T.ArrayType(T.StructType([
        T.StructField("item_id", T.StringType()),
        T.StructField("batch_code", T.StringType()),
        T.StructField("quantity", T.IntegerType()),
        T.StructField("unit_price", T.DoubleType()),
        T.StructField("discount", T.DoubleType()),
        T.StructField("total_price", T.DoubleType()),
        T.StructField("expiry_date", T.StringType()),
    ])))
])

schema_bootstrap = T.StructType([
    T.StructField("shelf_id", T.StringType()),
    T.StructField("batch_code", T.StringType()),
    T.StructField("received_date", T.DateType()),
    T.StructField("expiry_date", T.DateType()),
    T.StructField("batch_quantity_store", T.IntegerType()),
])

# =========================
# Bootstrap
# =========================
def bootstrap_if_needed():
    if DeltaTable.isDeltaTable(spark, BATCH_STATE_PATH):
        if spark.read.format("delta").load(BATCH_STATE_PATH).limit(1).count() > 0:
            print("[bootstrap] Existing delta table found, skipping.")
            return

    if not BOOTSTRAP_FROM_PG:
        print("[bootstrap] Skipping bootstrap from Postgres.")
        return

    if not (JDBC_PG_URL and JDBC_PG_USER and JDBC_PG_PASSWORD):
        raise RuntimeError("Missing JDBC params for bootstrap.")

    for attempt in range(1, 6):
        try:
            df = (
                spark.read.format("jdbc")
                .option("url", JDBC_PG_URL)
                .option("user", JDBC_PG_USER)
                .option("password", JDBC_PG_PASSWORD)
                .option("dbtable", """
                    (SELECT shelf_id, batch_code, received_date, expiry_date, batch_quantity_store
                     FROM ref.store_batches_snapshot) AS t
                """)
                .load()
            )
            break
        except Exception as e:
            print(f"[bootstrap] Attempt {attempt} failed: {e}")
            time.sleep(3)
    else:
        raise RuntimeError("[bootstrap] Failed to read store_batches_snapshot")

    df_with_ts = df.withColumn("last_update_ts", F.current_timestamp())
    df_with_ts.write.format("delta").mode("overwrite").save(BATCH_STATE_PATH)
    print("[bootstrap] Bootstrapped shelf_batch_state.")

    kafka_payload = (
        df_with_ts
        .withColumn("key", F.concat_ws("::", "shelf_id", "batch_code"))
        .withColumn(
            "value",
            F.to_json(F.struct(
                "shelf_id", "batch_code", "received_date", "expiry_date",
                "batch_quantity_store", "last_update_ts"
            ))
        )
        .select(
            F.col("key").cast("string"),
            F.col("value").cast("string")
        )
    )

    kafka_payload.write.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", TOPIC_BATCH_STATE) \
        .save()

bootstrap_if_needed()

# =========================
# Stream from POS → Update Delta → Publish Kafka
# =========================
def process_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    exploded = (
        batch_df
        .withColumn("event_ts", F.to_timestamp("timestamp"))
        .withColumn("item", F.explode("items"))
        .select(
            F.col("item.item_id"),
            F.col("item.batch_code"),
            F.col("item.quantity"),
            F.col("item.expiry_date").cast("date").alias("expiry_date"),
            F.col("event_ts"),
        )
        .filter(F.col("batch_code").isNotNull())
        .withColumnRenamed("item_id", "shelf_id")
    )

    grouped = (
        exploded.groupBy("shelf_id", "batch_code", "expiry_date")
        .agg(
            F.sum("quantity").alias("delta_qty"),
            F.max("event_ts").alias("last_event_ts")
        )
    )

    updates = (
        grouped
        .withColumn("received_date", F.lit(None).cast("date"))
        .withColumn("last_update_ts", F.current_timestamp())
    )

    delta_tbl = DeltaTable.forPath(spark, BATCH_STATE_PATH)
    delta_tbl.alias("t").merge(
        updates.alias("s"),
        "t.shelf_id = s.shelf_id AND t.batch_code = s.batch_code"
    ).whenMatchedUpdate(set={
        "batch_quantity_store": F.expr("t.batch_quantity_store - s.delta_qty"),
        "last_update_ts": F.col("s.last_update_ts")
    }).whenNotMatchedInsert(values={
        "shelf_id": F.col("s.shelf_id"),
        "batch_code": F.col("s.batch_code"),
        "received_date": F.col("s.received_date"),
        "expiry_date": F.col("s.expiry_date"),
        "batch_quantity_store": F.expr("-s.delta_qty"),
        "last_update_ts": F.col("s.last_update_ts")
    }).execute()

    # Kafka compacted topic publish
    state_df = spark.read.format("delta").load(BATCH_STATE_PATH)
    keys = grouped.select("shelf_id", "batch_code").distinct()
    to_publish = (
        keys.join(state_df, on=["shelf_id", "batch_code"], how="left")
        .withColumn("key", F.concat_ws("::", F.col("shelf_id"), F.col("batch_code")))
        .withColumn("value_json", F.to_json(F.struct(
            "shelf_id", "batch_code", "received_date", "expiry_date", "batch_quantity_store", "last_update_ts"
        )))
        .select("key", F.col("value_json").alias("value"))
    )

    to_publish.write.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", TOPIC_BATCH_STATE) \
        .save()

# =========================
# Stream Definition
# =========================
kafka_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC_POS_TRANSACTIONS)
    .option("startingOffsets", STARTING_OFFSETS)
    .option("failOnDataLoss", "false")
    .load()
)

pos_events = (
    kafka_stream
    .select(F.col("value").cast("string").alias("value"))
    .withColumn("json", F.from_json("value", schema_tx))
    .select("json.*")
    .filter(F.col("event_type") == "pos_transaction")
)

raw_pos_events = (
    pos_events
    .select(
        "event_type",
        "transaction_id",
        "customer_id",
        F.to_timestamp("timestamp").alias("timestamp"),
        "items"
    )
)

raw_query = (
    raw_pos_events.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", POS_RAW_CHECKPOINT)
    .option("mergeSchema", "true")
    .option("path", POS_RAW_PATH)
    .start()
)

query = (
    pos_events.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start()
)

spark.streams.awaitAnyTermination()
