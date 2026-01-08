import os
import time
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from simulated_time.clock import get_simulated_now  # ✅ clock simulato


# =========================
# Env / Config
# =========================
KAFKA_BROKER     = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_WH_EVENTS  = os.getenv("TOPIC_WH_EVENTS", "wh_events")   # append-only
TOPIC_WH_STATE   = os.getenv("TOPIC_WH_STATE", "wh_state")     # compacted

DELTA_ROOT       = os.getenv("DELTA_ROOT", "/delta")
RAW_PATH         = f"{DELTA_ROOT}/raw/wh_events"
STATE_PATH       = f"{DELTA_ROOT}/cleansed/wh_state"

CHECKPOINT_ROOT  = os.getenv("CHECKPOINT_ROOT", f"{DELTA_ROOT}/_checkpoints/wh_aggregator")
CKP_RAW          = f"{CHECKPOINT_ROOT}/raw"
CKP_AGG          = f"{CHECKPOINT_ROOT}/agg"

STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "earliest")

# Postgres bootstrap
JDBC_PG_URL       = os.getenv("JDBC_PG_URL")
JDBC_PG_USER      = os.getenv("JDBC_PG_USER")
JDBC_PG_PASSWORD  = os.getenv("JDBC_PG_PASSWORD")
BOOTSTRAP_FROM_PG = os.getenv("BOOTSTRAP_FROM_PG", "1") in ("1", "true", "True")

# Default table for bootstrap snapshot
PG_BOOTSTRAP_TABLE = os.getenv("PG_BOOTSTRAP_TABLE", "ref.warehouse_inventory_snapshot")

# =========================
# Spark Session (Delta)
# =========================
spark = (
    SparkSession.builder.appName("WH_Aggregator")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# =========================
# Schemas
# =========================
schema_evt = T.StructType([
    T.StructField("event_type",  T.StringType()),   # wh_in | wh_out
    T.StructField("event_id",    T.StringType()),
    T.StructField("plan_id",     T.StringType()),
    T.StructField("shelf_id",    T.StringType()),
    T.StructField("batch_code",  T.StringType()),
    T.StructField("qty",         T.IntegerType()),
    T.StructField("unit",        T.StringType()),
    T.StructField("timestamp",   T.TimestampType()),
    # optional extra fields ignored by this aggregator
    T.StructField("fifo",        T.BooleanType()),
    T.StructField("received_date", T.StringType()),
    T.StructField("expiry_date",   T.StringType()),
    T.StructField("reason",      T.StringType()),
])

# =========================
# Bootstrap from Postgres (one-off)
# =========================
def bootstrap_state_if_missing():
    # If Delta exists and not empty -> skip
    if DeltaTable.isDeltaTable(spark, STATE_PATH):
        existing = spark.read.format("delta").load(STATE_PATH)
        if existing.limit(1).count() > 0:
            print(f"[bootstrap] wh_state already present at {STATE_PATH} -> skip bootstrap.")
            return

    if not BOOTSTRAP_FROM_PG:
        print("[bootstrap] BOOTSTRAP_FROM_PG=0 -> skip bootstrap.")
        return

    if not (JDBC_PG_URL and JDBC_PG_USER and JDBC_PG_PASSWORD):
        print("[bootstrap] JDBC params missing -> skip bootstrap.")
        return

    last_err = None
    for attempt in range(1, 6):
        try:
            base = (
                spark.read.format("jdbc")
                .option("url", JDBC_PG_URL)
                .option("user", JDBC_PG_USER)
                .option("password", JDBC_PG_PASSWORD)
                .option("dbtable", f"(SELECT shelf_id, current_stock, snapshot_ts FROM {PG_BOOTSTRAP_TABLE}) t")
                .load()
            )
            break
        except Exception as e:
            last_err = e
            print(f"[bootstrap] Postgres read failed ({attempt}/5): {e}")
            time.sleep(3)
    else:
        raise RuntimeError("Unable to bootstrap wh_state from Postgres") from last_err

    w = Window.partitionBy("shelf_id").orderBy(F.col("snapshot_ts").desc())
    latest = (
        base.withColumn("rn", F.row_number().over(w))
            .where("rn = 1")
            .select(
                F.col("shelf_id"),
                F.col("current_stock").cast("int").alias("wh_current_stock"),
                F.lit(get_simulated_now()).alias("last_update_ts")  # ✅ simulato
            )
    )


    # Write initial Delta state (retry on concurrent create)
    last = None
    for attempt in range(1, 6):
        try:
            latest.write.format("delta").mode("overwrite").save(STATE_PATH)
            break
        except Exception as e:
            last = e
            if DeltaTable.isDeltaTable(spark, STATE_PATH):
                break
            msg = str(e)
            if "DELTA_PROTOCOL_CHANGED" not in msg and "ProtocolChangedException" not in msg:
                raise
            time.sleep(0.5 * attempt)
    else:
        raise last

    # Publish initial compacted snapshot to Kafka (key = shelf_id)
    to_publish = (
        latest.withColumn(
            "value_json",
            F.to_json(F.struct("shelf_id", "wh_current_stock", "last_update_ts"))
        )
        .select(
            F.col("shelf_id").cast("string").alias("key"),
            F.col("value_json").cast("string").alias("value")
        )
    )

    to_publish.write.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", TOPIC_WH_STATE) \
        .save()

    print(f"[bootstrap] Initial wh_state created and published on {TOPIC_WH_STATE}.")

bootstrap_state_if_missing()

# =========================
# 1) Stream: Kafka -> Delta RAW (append-only)
# =========================
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC_WH_EVENTS)
    .option("startingOffsets", STARTING_OFFSETS)
    .option("failOnDataLoss", "false")
    .load()
)

events = (
    raw.select(F.col("value").cast("string").alias("v"))
       .withColumn("json", F.from_json("v", schema_evt))
       .select("json.*")
       .filter(F.col("shelf_id").isNotNull() & F.col("qty").isNotNull())
)

raw_events = (
    events.select(
        "event_type", "event_id", "plan_id", "shelf_id", "batch_code",
        "qty", "unit", F.col("timestamp").alias("timestamp")
    )
)

raw_query = (
    raw_events.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CKP_RAW)
    .option("mergeSchema", "true")
    .option("path", RAW_PATH)
    .start()
)

# =========================
# 2) Aggregate into wh_state (Delta + Kafka compacted)
# =========================
def upsert_and_publish(batch_df, batch_id: int):
    if batch_df.rdd.isEmpty():
        return

    # wh_in -> +qty ; wh_out -> -qty
    deltas = (
        batch_df.groupBy("shelf_id")
        .agg(
            F.sum(
                F.when(F.col("event_type") == "wh_in",  F.col("qty"))
                 .when(F.col("event_type") == "wh_out", -F.col("qty"))
                 .otherwise(F.lit(0))
            ).alias("delta_qty"),
            F.max("timestamp").alias("last_event_ts")
        )
    )

    updates = (
        deltas.withColumn("last_update_ts", F.lit(get_simulated_now()))
              .select("shelf_id", "delta_qty", "last_event_ts", "last_update_ts")
    )

    # Merge into Delta state
    if DeltaTable.isDeltaTable(spark, STATE_PATH):
        t = DeltaTable.forPath(spark, STATE_PATH)
        t.alias("t").merge(
            updates.alias("s"),
            "t.shelf_id = s.shelf_id"
        ).whenMatchedUpdate(set={
            "wh_current_stock": F.expr("COALESCE(t.wh_current_stock, 0) + s.delta_qty"),
            "last_update_ts":   F.expr("GREATEST(t.last_update_ts, s.last_event_ts, s.last_update_ts)")
        }).whenNotMatchedInsert(values={
            "shelf_id":         F.col("s.shelf_id"),
            "wh_current_stock": F.col("s.delta_qty"),
            "last_update_ts":   F.col("s.last_update_ts")
        }).execute()
    else:
        initial = (
            updates.select(
                "shelf_id",
                F.col("delta_qty").alias("wh_current_stock"),
                "last_update_ts"
            )
        )
        initial.write.format("delta").mode("overwrite").save(STATE_PATH)

    # Publish ONLY touched keys
    updated_keys = deltas.select("shelf_id").distinct()
    state_df = spark.read.format("delta").load(STATE_PATH)

    to_publish = (
        updated_keys.join(state_df, on="shelf_id", how="left")
        .withColumn("value_json", F.to_json(F.struct("shelf_id","wh_current_stock","last_update_ts")))
        .select(
            F.col("shelf_id").cast("string").alias("key"),
            F.col("value_json").cast("string").alias("value")
        )
    )

    to_publish.write.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", TOPIC_WH_STATE) \
        .save()

agg_query = (
    events.writeStream
    .foreachBatch(upsert_and_publish)
    .option("checkpointLocation", CKP_AGG)
    .start()
)

spark.streams.awaitAnyTermination()
