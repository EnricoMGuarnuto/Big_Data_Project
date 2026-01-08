import os
import time
import json
from pyspark.sql import SparkSession, functions as F, types as T, Window
from delta.tables import DeltaTable
from simulated_time.redis_helpers import get_simulated_timestamp


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
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "earliest")
MAX_OFFSETS_PER_TRIGGER = os.getenv("MAX_OFFSETS_PER_TRIGGER")  # optional throttle for Kafka source

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

spark.sparkContext.setLogLevel("WARN")

# =========================
# Delta MERGE retry helpers  <--- QUI
# =========================
def _is_delta_conflict(err: Exception) -> bool:
    name = err.__class__.__name__
    msg = str(err)
    return (
        "ConcurrentAppendException" in name
        or "ConcurrentWriteException" in name
        or "MetadataChangedException" in name
        or "ProtocolChangedException" in name
        or "DELTA_CONCURRENT" in msg
        or "DELTA_METADATA_CHANGED" in msg
    )

def _merge_with_retries(spark, updates_df, batch_state_path: str,
                        max_retries: int = 10, base_sleep_s: float = 0.8) -> None:
    last = None
    for attempt in range(1, max_retries + 1):
        try:
            delta_tbl = DeltaTable.forPath(spark, batch_state_path)
            (delta_tbl.alias("t")
                .merge(
                    updates_df.alias("s"),
                    "t.shelf_id = s.shelf_id AND t.batch_code = s.batch_code"
                )
                .whenMatchedUpdate(set={
                    "batch_quantity_store": F.expr("t.batch_quantity_store - s.delta_qty"),
                    "last_update_ts": F.greatest(F.col("t.last_update_ts"), F.col("s.last_update_ts"))
                })
                .execute()
            )
            return
        except Exception as e:
            last = e
            if (not _is_delta_conflict(e)) or attempt == max_retries:
                raise
            sleep_s = base_sleep_s * attempt
            print(f"[delta-merge-retry] conflict ({attempt}/{max_retries}) -> sleep {sleep_s}s: {e}")
            time.sleep(sleep_s)
    raise last


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

    SIM_START = os.getenv("SIM_START")  # es: 2025-10-01T00:00:00

    df_with_ts = df.withColumn(
        "last_update_ts",
        F.lit(get_simulated_timestamp()).cast("timestamp")
    )
    last = None
    for attempt in range(1, 6):
        try:
            df_with_ts.write.format("delta").mode("overwrite").save(BATCH_STATE_PATH)
            break
        except Exception as e:
            last = e
            if DeltaTable.isDeltaTable(spark, BATCH_STATE_PATH):
                break
            msg = str(e)
            if "DELTA_PROTOCOL_CHANGED" not in msg and "ProtocolChangedException" not in msg:
                raise
            time.sleep(0.5 * attempt)
    else:
        raise last
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
def process_batch(batch_df, batch_id: int):
    if batch_df.rdd.isEmpty():
        return

    # Leggo lo stato corrente (serve per mappare batch_code -> shelf_id)
    state_df = spark.read.format("delta").load(BATCH_STATE_PATH).select(
        "shelf_id", "batch_code", "expiry_date"
    )

    # Timestamp ISO8601 con timezone: 2025-10-01T10:00:50+00:00
    # Spark spesso lo parse-a ok, ma questo è più robusto.
    event_ts_col = F.to_timestamp(
        F.regexp_replace(F.col("timestamp"), "Z$", "+00:00")
    )

    # 1) Flatten dei POS items
    items = (
        batch_df
        .withColumn("event_ts", event_ts_col)
        .withColumn("item", F.explode("items"))
        .select(
            F.col("event_ts"),
            F.col("item.item_id").alias("item_id"),
            F.col("item.batch_code").alias("batch_code"),
            F.col("item.quantity").alias("quantity"),
            F.to_date(F.col("item.expiry_date")).alias("expiry_date_item")
        )
        .filter(F.col("batch_code").isNotNull())
        .filter(F.col("quantity").isNotNull())
        .filter(F.col("event_ts").isNotNull())
    )

    # 2) Enrichment: recupero shelf_id dallo stato tramite batch_code
    # Se vuoi essere super-strict, puoi anche matchare expiry_date quando presente.
    enriched = (
        items.alias("i")
        .join(
            state_df.alias("s"),
            on=[
                F.col("i.batch_code") == F.col("s.batch_code")
            ],
            how="inner"
        )
        .select(
            F.col("s.shelf_id").alias("shelf_id"),
            F.col("i.batch_code").alias("batch_code"),
            # preferisco l'expiry_date dello stato (coerenza), ma se vuoi quella POS, inverti coalesce
            F.col("s.expiry_date").alias("expiry_date"),
            F.col("i.quantity").alias("quantity"),
            F.col("i.event_ts").alias("event_ts")
        )
    )

    # (Opzionale) loggare batch_code sconosciuti (utile in demo)
    # unknown = items.join(state_df, on="batch_code", how="left_anti")
    # unknown.show(5, truncate=False)

    # 3) Aggregazione per key di stato
    grouped = (
        enriched
        .groupBy("shelf_id", "batch_code", "expiry_date")
        .agg(
            F.sum("quantity").alias("delta_qty"),
            F.max("event_ts").alias("last_event_ts")
        )
    )

    updates = (
        grouped
        .withColumn("received_date", F.lit(None).cast("date"))
        .withColumnRenamed("last_event_ts", "last_update_ts")  # EVENT-TIME!
    )

    # 4) Merge Delta (aggiorna quantità e timestamp)
    # 4) Merge Delta (aggiorna quantità e timestamp) con retry
    _merge_with_retries(
        spark,
        updates_df=updates,
        batch_state_path=BATCH_STATE_PATH,
        max_retries=10,
        base_sleep_s=0.8
    )


    # 5) Publish Kafka (solo chiavi aggiornate)
    keys = grouped.select("shelf_id", "batch_code").distinct()
    refreshed = (
        keys.join(
            spark.read.format("delta").load(BATCH_STATE_PATH),
            on=["shelf_id", "batch_code"],
            how="inner"
        )
        .withColumn("key", F.concat_ws("::", F.col("shelf_id"), F.col("batch_code")))
        .withColumn("value", F.to_json(F.struct(
            "shelf_id", "batch_code", "received_date", "expiry_date", "batch_quantity_store", "last_update_ts"
        )))
        .select(F.col("key").cast("string"), F.col("value").cast("string"))
    )

    (
        refreshed.write.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("topic", TOPIC_BATCH_STATE)
        .save()
    )


# =========================
# Stream Definition
# =========================
kafka_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC_POS_TRANSACTIONS)
    .option("startingOffsets", STARTING_OFFSETS)
    .option("failOnDataLoss", "false")
)
if MAX_OFFSETS_PER_TRIGGER:
    kafka_stream = kafka_stream.option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)

kafka_stream = kafka_stream.load()

pos_events = (
    kafka_stream
    .select(F.col("value").cast("string").alias("value"))
    .withColumn("json", F.from_json("value", schema_tx))
    .select("json.*")
    .filter(F.col("event_type") == "pos_transaction")
)

def process_batch_with_raw(batch_df, batch_id: int):
    if batch_df.rdd.isEmpty():
        return

    raw_batch = (
        batch_df.select(
            "event_type",
            "transaction_id",
            "customer_id",
            F.to_timestamp("timestamp").alias("timestamp"),
            "items",
        )
    )

    (raw_batch.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .option("txnAppId", "batch_state_updater_raw_pos_transactions")
        .option("txnVersion", str(batch_id))
        .save(POS_RAW_PATH))

    process_batch(batch_df, batch_id)

query = (
    pos_events.writeStream
    .foreachBatch(process_batch_with_raw)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start()
)

spark.streams.awaitAnyTermination()
