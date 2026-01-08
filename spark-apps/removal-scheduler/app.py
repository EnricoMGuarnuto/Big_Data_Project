import os
import time
from pyspark.sql import SparkSession, functions as F
from delta.tables import DeltaTable
from simulated_time.redis_helpers import get_simulated_date, get_simulated_timestamp

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
EMIT_ALERTS      = os.getenv("EMIT_ALERTS", "1") in ("1", "true", "True")

# How often to poll simulated date (real seconds). Not part of the simulation semantics.
POLL_SECONDS     = float(os.getenv("POLL_SECONDS", "1.0"))

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

def delta_ready() -> bool:
    return is_delta(DL_SHELF_STATE) and is_delta(DL_SHELF_BATCH)

def run_once(sim_date_str: str, sim_ts_str: str) -> None:
    """
    Executes one daily check (for the given simulated day).
    IMPORTANT: This is a batch job executed once per simulated day.
    """
    # Read current snapshots each run (so we see updates from other jobs)
    shelf_state = spark.read.format("delta").load(DL_SHELF_STATE)
    shelf_batch = spark.read.format("delta").load(DL_SHELF_BATCH)

    required_state = {"shelf_id", "current_stock"}
    required_batch = {"shelf_id", "batch_code", "expiry_date", "batch_quantity_store"}
    if not required_state.issubset(set(shelf_state.columns)):
        raise RuntimeError(f"shelf_state missing columns: {required_state - set(shelf_state.columns)}")
    if not required_batch.issubset(set(shelf_batch.columns)):
        raise RuntimeError(f"shelf_batch_state missing columns: {required_batch - set(shelf_batch.columns)}")

    HAS_SHELF_WEIGHT = "shelf_weight" in shelf_state.columns
    HAS_STATE_TS = "last_update_ts" in shelf_state.columns
    HAS_BATCH_TS = "last_update_ts" in shelf_batch.columns
    HAS_RECEIVED_DATE = "received_date" in shelf_batch.columns

    today = F.lit(sim_date_str).cast("date")

    if REMOVE_MODE == "same_day_evening":
        expired_cond = (F.col("expiry_date") <= today)
    else:
        # default: remove the day AFTER expiry
        expired_cond = (F.col("expiry_date") < today)

    # 1) Find expired batches still on shelf
    batch_select_cols = ["shelf_id", "batch_code"]
    if HAS_RECEIVED_DATE:
        batch_select_cols.append("received_date")
    batch_select_cols += ["expiry_date", "batch_quantity_store"]

    expired_batches = (
        shelf_batch
        .filter(F.col("batch_quantity_store") > 0)
        .filter(expired_cond)
        .select(*batch_select_cols)
        .withColumnRenamed("batch_quantity_store", "qty_to_remove")
    )

    if expired_batches.rdd.isEmpty():
        print(f"[removal_scheduler] No expired in-store batches to remove for {sim_date_str}.")
        return  # ✅ do NOT exit the container; we'll check again next simulated day

    # 2) Aggregate removed qty per shelf for shelf_state update
    removed_per_shelf = (
        expired_batches
        .groupBy("shelf_id")
        .agg(F.sum("qty_to_remove").alias("removed_qty"))
    )

    # 3) Prepare new shelf_state rows (decrement stock, clamp >= 0)
    new_state = (
        shelf_state.select("shelf_id", "current_stock", *(["shelf_weight"] if HAS_SHELF_WEIGHT else []))
        .join(removed_per_shelf, on="shelf_id", how="inner")
        .withColumn("current_stock_new", F.greatest(F.lit(0), F.col("current_stock") - F.col("removed_qty")))
        .withColumn("last_update_ts", F.lit(sim_ts_str).cast("timestamp"))
        .select(
            "shelf_id",
            F.col("current_stock_new").alias("current_stock"),
            (F.col("shelf_weight") if HAS_SHELF_WEIGHT else F.lit(None).cast("double")).alias("shelf_weight"),
            "last_update_ts"
        )
    )

    # 4) Prepare shelf_batch_state updates: set batch_quantity_store = 0 for those expired batches
    batch_updates = (
        expired_batches
        .withColumn("received_date", F.col("received_date") if HAS_RECEIVED_DATE else F.lit(None).cast("date"))
        .withColumn("batch_quantity_store", F.lit(0))
        .withColumn("last_update_ts", F.lit(sim_ts_str).cast("timestamp"))
        .select("shelf_id", "batch_code", "received_date", "expiry_date", "batch_quantity_store", "last_update_ts")
    )

    # =========================
    # Delta upserts
    # =========================
    # shelf_state upsert by shelf_id
    t_state = DeltaTable.forPath(spark, DL_SHELF_STATE)
    state_update_set = {"current_stock": F.col("s.current_stock")}
    state_insert_set = {"shelf_id": F.col("s.shelf_id"), "current_stock": F.col("s.current_stock")}
    if HAS_SHELF_WEIGHT:
        state_update_set["shelf_weight"] = F.col("s.shelf_weight")
        state_insert_set["shelf_weight"] = F.col("s.shelf_weight")
    if HAS_STATE_TS:
        state_update_set["last_update_ts"] = F.col("s.last_update_ts")
        state_insert_set["last_update_ts"] = F.col("s.last_update_ts")

    (
        t_state.alias("t")
        .merge(new_state.alias("s"), "t.shelf_id = s.shelf_id")
        .whenMatchedUpdate(set=state_update_set)
        .whenNotMatchedInsert(values=state_insert_set)
        .execute()
    )

    # shelf_batch_state upsert by (shelf_id, batch_code)
    t_batch = DeltaTable.forPath(spark, DL_SHELF_BATCH)
    batch_update_set = {"batch_quantity_store": F.col("s.batch_quantity_store")}
    if HAS_BATCH_TS:
        batch_update_set["last_update_ts"] = F.col("s.last_update_ts")

    (
        t_batch.alias("t")
        .merge(batch_updates.alias("s"), "t.shelf_id = s.shelf_id AND t.batch_code = s.batch_code")
        .whenMatchedUpdate(set=batch_update_set)
        .execute()
    )

    removed_cnt = expired_batches.count()
    print(f"[removal_scheduler] Removed expired stock from {removed_cnt} batches for {sim_date_str}.")

    # =========================
    # Kafka publish (compacted mirrors)
    # =========================
    TOPIC_SHELF_STATE = os.getenv("TOPIC_SHELF_STATE", "shelf_state")
    state_struct_cols = ["shelf_id", "current_stock", "shelf_weight"]
    if HAS_STATE_TS:
        state_struct_cols.append("last_update_ts")

    state_to_kafka = (
        new_state
        .withColumn("value", F.to_json(F.struct(*state_struct_cols)))
        .select(F.col("shelf_id").cast("string").alias("key"), F.col("value").cast("string").alias("value"))
    )
    state_to_kafka.write.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", TOPIC_SHELF_STATE) \
        .save()

    TOPIC_SHELF_BATCH_STATE = os.getenv("TOPIC_SHELF_BATCH_STATE", "shelf_batch_state")
    batch_struct_cols = ["shelf_id", "batch_code", "received_date", "expiry_date", "batch_quantity_store"]
    if HAS_BATCH_TS:
        batch_struct_cols.append("last_update_ts")

    batch_to_kafka = (
        batch_updates
        .withColumn("value", F.to_json(F.struct(*batch_struct_cols)))
        .select(
            F.concat_ws("::", F.col("shelf_id"), F.col("batch_code")).cast("string").alias("key"),
            F.col("value").cast("string").alias("value")
        )
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
            .withColumn("created_at", F.lit(sim_ts_str).cast("timestamp"))
            .select(
                "event_type", "shelf_id", "location", "severity",
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

def main() -> None:
    """
    Main loop:
    - stays alive
    - runs the removal check ONCE per simulated day (based on get_simulated_date()).
    """
    print("[removal_scheduler] Started (simulated-time daily trigger)")
    last_processed_day = None
    warned_missing = False

    while True:
        if not delta_ready():
            if not warned_missing:
                print(
                    "[removal_scheduler] Delta tables missing. Waiting for:\n"
                    f"- {DL_SHELF_STATE}\n"
                    f"- {DL_SHELF_BATCH}"
                )
                warned_missing = True
            time.sleep(POLL_SECONDS)
            continue
        warned_missing = False

        sim_day = get_simulated_date()
        sim_ts  = get_simulated_timestamp()

        if sim_day != last_processed_day:
            last_processed_day = sim_day
            try:
                run_once(sim_day, sim_ts)
            except Exception as e:
                # Don't crash the container; log and keep checking next days
                print(f"[removal_scheduler] ERROR during run for {sim_day}: {e}")

        # Real-time sleep just to avoid busy-loop; simulation semantics come from Redis.
        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main()
