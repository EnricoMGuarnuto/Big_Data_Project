import os
from pyspark.sql import SparkSession, functions as F, types as T
from delta.tables import DeltaTable

# =========================
# Env / Config
# =========================
KAFKA_BROKER     = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_ALERTS     = os.getenv("TOPIC_ALERTS", "alerts")

DELTA_ROOT       = os.getenv("DELTA_ROOT", "/delta")
DL_SHELF_STATE   = os.getenv("DL_SHELF_STATE_PATH", f"{DELTA_ROOT}/cleansed/shelf_state")
DL_SHELF_BATCH   = os.getenv("DL_SHELF_BATCH_PATH", f"{DELTA_ROOT}/cleansed/shelf_batch_state")

# Run mode:
# - next_day: remove where expiry_date < today  (default, "day after expiry")
# - same_day_evening: remove where expiry_date <= today (if you run at closing time)
REMOVE_MODE      = os.getenv("REMOVE_MODE", "next_day").lower()
EMIT_ALERTS      = os.getenv("EMIT_ALERTS", "1") in ("1","true","True")

# =========================
# Spark Session (Delta)
# =========================
spark = (
    SparkSession.builder.appName("Removal_Scheduler")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

def is_delta(path: str) -> bool:
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return False

if not is_delta(DL_SHELF_STATE) or not is_delta(DL_SHELF_BATCH):
    raise RuntimeError(
        f"Delta tables missing. Need:\n"
        f"- {DL_SHELF_STATE}\n"
        f"- {DL_SHELF_BATCH}"
    )

shelf_state = spark.read.format("delta").load(DL_SHELF_STATE)
shelf_batch = spark.read.format("delta").load(DL_SHELF_BATCH)

required_state = {"shelf_id", "current_stock"}
required_batch = {"shelf_id", "batch_code", "expiry_date", "batch_quantity_store"}
if not required_state.issubset(set(shelf_state.columns)):
    raise RuntimeError(f"shelf_state missing columns: {required_state - set(shelf_state.columns)}")
if not required_batch.issubset(set(shelf_batch.columns)):
    raise RuntimeError(f"shelf_batch_state missing columns: {required_batch - set(shelf_batch.columns)}")

today = F.current_date()

if REMOVE_MODE == "same_day_evening":
    expired_cond = (F.col("expiry_date") <= today)
else:
    # default: remove the day AFTER expiry
    expired_cond = (F.col("expiry_date") < today)

# 1) Find expired batches still on shelf
expired_batches = (
    shelf_batch
    .filter(F.col("batch_quantity_store") > 0)
    .filter(expired_cond)
    .select("shelf_id", "batch_code", "expiry_date", "batch_quantity_store")
    .withColumnRenamed("batch_quantity_store", "qty_to_remove")
)

if expired_batches.rdd.isEmpty():
    print("[removal_scheduler] No expired in-store batches to remove. Exiting.")
    spark.stop()
    raise SystemExit(0)

# 2) Aggregate removed qty per shelf for shelf_state update
removed_per_shelf = (
    expired_batches
    .groupBy("shelf_id")
    .agg(F.sum("qty_to_remove").alias("removed_qty"))
)

# 3) Prepare new shelf_state rows (decrement stock, clamp >= 0)
new_state = (
    shelf_state.select("shelf_id", "current_stock")
    .join(removed_per_shelf, on="shelf_id", how="inner")
    .withColumn("current_stock_new", F.greatest(F.lit(0), F.col("current_stock") - F.col("removed_qty")))
    .withColumn("last_update_ts", F.current_timestamp())
    .select(
        "shelf_id",
        F.col("current_stock_new").alias("current_stock"),
        F.lit(None).cast("double").alias("shelf_weight"),  # keep null if you don't track it here
        "last_update_ts"
    )
)

# 4) Prepare shelf_batch_state updates: set batch_quantity_store = 0 for those expired batches
batch_updates = (
    expired_batches
    .withColumn("batch_quantity_store", F.lit(0))
    .withColumn("last_update_ts", F.current_timestamp())
    .select("shelf_id", "batch_code", "batch_quantity_store", "last_update_ts")
)

# =========================
# Delta upserts
# =========================
# shelf_state upsert by shelf_id
t_state = DeltaTable.forPath(spark, DL_SHELF_STATE)
(
    t_state.alias("t")
    .merge(new_state.alias("s"), "t.shelf_id = s.shelf_id")
    .whenMatchedUpdate(set={
        "current_stock":  F.col("s.current_stock"),
        "shelf_weight":   F.col("s.shelf_weight"),
        "last_update_ts": F.col("s.last_update_ts"),
    })
    .whenNotMatchedInsert(values={
        "shelf_id":       F.col("s.shelf_id"),
        "current_stock":  F.col("s.current_stock"),
        "shelf_weight":   F.col("s.shelf_weight"),
        "last_update_ts": F.col("s.last_update_ts"),
    })
    .execute()
)

# shelf_batch_state upsert by (shelf_id, batch_code)
t_batch = DeltaTable.forPath(spark, DL_SHELF_BATCH)
(
    t_batch.alias("t")
    .merge(batch_updates.alias("s"), "t.shelf_id = s.shelf_id AND t.batch_code = s.batch_code")
    .whenMatchedUpdate(set={
        "batch_quantity_store": F.col("s.batch_quantity_store"),
        "last_update_ts":       F.col("s.last_update_ts"),
    })
    .execute()
)

print(f"[removal_scheduler] Removed expired stock from {expired_batches.count()} batches.")

# =========================
# Kafka publish (compacted mirrors)
# =========================
# Publish new shelf_state to Kafka topic shelf_state (compacted)
# NOTE: topic name assumed "shelf_state"; change via env if you want.
TOPIC_SHELF_STATE = os.getenv("TOPIC_SHELF_STATE", "shelf_state")
state_to_kafka = (
    new_state
    .withColumn("value", F.to_json(F.struct("shelf_id","current_stock","shelf_weight","last_update_ts")))
    .select(F.col("shelf_id").alias("key"), F.col("value").alias("value"))
)
state_to_kafka.write.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("topic", TOPIC_SHELF_STATE) \
    .save()

# Publish updated shelf_batch_state rows to Kafka topic shelf_batch_state (compacted)
TOPIC_SHELF_BATCH_STATE = os.getenv("TOPIC_SHELF_BATCH_STATE", "shelf_batch_state")
batch_to_kafka = (
    batch_updates
    .withColumn("received_date", F.lit(None).cast("date"))
    .withColumn("expiry_date",   F.lit(None).cast("date"))
    .withColumn("value", F.to_json(F.struct(
        "shelf_id","batch_code","received_date","expiry_date","batch_quantity_store","last_update_ts"
    )))
    .select(F.concat_ws("::", F.col("shelf_id"), F.col("batch_code")).alias("key"), F.col("value").alias("value"))
)
batch_to_kafka.write.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("topic", TOPIC_SHELF_BATCH_STATE) \
    .save()

# =========================
# Optional alerts (audit)
# =========================
if EMIT_ALERTS:
    alerts = (
        expired_batches
        .withColumn("event_type", F.lit("expired_removal"))
        .withColumn("location",   F.lit("store"))
        .withColumn("severity",   F.lit("high"))
        .withColumn("suggested_qty", F.col("qty_to_remove"))  # reuse as "removed_qty"
        .withColumn("created_at", F.current_timestamp())
        .select(
            "event_type","shelf_id","location","severity",
            F.lit(None).cast("int").alias("current_stock"),
            F.lit(None).cast("int").alias("min_qty"),
            F.lit(None).cast("double").alias("threshold_pct"),
            F.lit(None).cast("double").alias("stock_pct"),
            "suggested_qty",
            "created_at"
        )
    )

    out = (
        alerts
        .withColumn("value", F.to_json(F.struct(
            "event_type","shelf_id","location","severity",
            "current_stock","min_qty","threshold_pct","stock_pct","suggested_qty","created_at"
        )))
        .select(F.lit(None).cast("string").alias("key"), "value")
    )

    out.write.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", TOPIC_ALERTS) \
        .save()

spark.stop()
