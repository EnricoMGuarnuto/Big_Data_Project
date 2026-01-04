import os
from pyspark.sql import SparkSession, functions as F, types as T
from delta.tables import DeltaTable

from simulated_time.redis_helpers import get_simulated_date, get_simulated_timestamp

# =========================
# Env / Config
# =========================
KAFKA_BROKER          = os.getenv("KAFKA_BROKER", "kafka:9092")

TOPIC_DAILY_DISCOUNTS = os.getenv("TOPIC_DAILY_DISCOUNTS", "daily_discounts")
TOPIC_ALERTS          = os.getenv("TOPIC_ALERTS", "alerts")            # optional

DELTA_ROOT            = os.getenv("DELTA_ROOT", "/delta")
DL_SHELF_BATCH_PATH   = os.getenv("DL_SHELF_BATCH_PATH", f"{DELTA_ROOT}/cleansed/shelf_batch_state")
DL_DAILY_DISC_PATH    = os.getenv("DL_DAILY_DISC_PATH", f"{DELTA_ROOT}/analytics/daily_discounts")

# extra discount range for near-expiry products
DISCOUNT_MIN          = float(os.getenv("DISCOUNT_MIN", "0.30"))   # 30%
DISCOUNT_MAX          = float(os.getenv("DISCOUNT_MAX", "0.50"))   # 50%

# =========================
# Spark Session (Delta)
# =========================
spark = (
    SparkSession.builder.appName("Daily_Discount_Manager")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

def delta_exists(path: str) -> bool:
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return False
    
# =========================
# Tempo simulato
# =========================
TODAY_STR = get_simulated_date()
NOW_STR = get_simulated_timestamp()

if TODAY_STR is None or NOW_STR is None:
    raise RuntimeError("[daily_discount_manager] sim:today or sim:now missing in Redis!")

today_col    = F.lit(TODAY_STR).cast("date")
tomorrow_col = F.date_add(today_col, 1)
now_col      = F.lit(NOW_STR).cast("timestamp")

# =========================
# 1) Read store batch state from Delta
# =========================
shelf_batches = spark.read.format("delta").load(DL_SHELF_BATCH_PATH)

required_cols = {"shelf_id", "batch_code", "expiry_date", "batch_quantity_store"}
missing = required_cols - set(shelf_batches.columns)
if missing:
    raise RuntimeError(f"[daily_discount_manager] shelf_batch_state missing columns: {missing}")

# =========================
# 2) Batches expiring today/tomorrow
# =========================
shelf_expiring = (
    shelf_batches
    .filter(F.col("batch_quantity_store") > 0)
    .withColumn(
        "in_window",
        (F.col("expiry_date") >= today_col) & (F.col("expiry_date") <= tomorrow_col)
    )
    .filter(F.col("in_window"))
    .select("shelf_id", "expiry_date")
    .distinct()
)

if shelf_expiring.rdd.isEmpty():
    print("[daily_discount_manager] No batches expiring today/tomorrow. Exiting.")
    spark.stop()
    time.sleep(3600)

# =========================
# 3) Calcolo delle date sconto
# =========================
exp_today = (
    shelf_expiring
    .filter(F.col("expiry_date") == today_col)
    .select("shelf_id", F.col("expiry_date"), today_col.alias("discount_date"))
)

exp_tomorrow = (
    shelf_expiring
    .filter(F.col("expiry_date") == tomorrow_col)
    .select("shelf_id", "expiry_date")
    .withColumn("discount_date", F.explode(F.array(today_col, tomorrow_col)))
)

candidates = (
    exp_today
    .unionByName(exp_tomorrow, allowMissingColumns=True)
    .select("shelf_id", "discount_date")
    .distinct()
)


# =========================
# 4) Sconti esistenti
# =========================
if delta_exists(DL_DAILY_DISC_PATH):
    existing_all = spark.read.format("delta").load(DL_DAILY_DISC_PATH)
    existing = (
        existing_all
        .select("shelf_id", "discount_date", "discount")
        .withColumnRenamed("discount", "base_discount")
    )
else:
    existing = spark.createDataFrame([], T.StructType([
        T.StructField("shelf_id",      T.StringType()),
        T.StructField("discount_date", T.DateType()),
        T.StructField("base_discount", T.DoubleType()),
    ]))

# =========================
# 5) Calcolo nuovi sconti
# =========================
steps = int(round((DISCOUNT_MAX - DISCOUNT_MIN) / 0.10)) + 1

discounts = (
    candidates
    .withColumn(
        "extra_discount",
        F.round(
            F.floor(F.rand(seed=42) * F.lit(steps)) * F.lit(0.10) + F.lit(DISCOUNT_MIN),
            2
        )
    )
    .join(existing, on=["shelf_id", "discount_date"], how="left")
    .withColumn("base_discount", F.coalesce(F.col("base_discount"), F.lit(0.0)))
    .withColumn(
        "discount",
        F.lit(1.0) - (F.lit(1.0) - F.col("base_discount")) * (F.lit(1.0) - F.col("extra_discount"))
    )
    .withColumn("created_at", now_col)
    .select("shelf_id", "discount_date", "discount", "created_at")
)

# =========================
# 6) Kafka output
# =========================
to_kafka = (
    discounts
    .withColumn("value", F.to_json(F.struct("shelf_id", "discount_date", "discount", "created_at")))
    .select(F.col("shelf_id").cast("string").alias("key"), F.col("value").cast("string"))
)

to_kafka.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("topic", TOPIC_DAILY_DISCOUNTS) \
    .save()

print(f"[daily_discount_manager] Wrote {discounts.count()} daily discounts to {TOPIC_DAILY_DISCOUNTS}")


# =========================
# 7) Delta upsert
# =========================
if delta_exists(DL_DAILY_DISC_PATH):
    tgt = DeltaTable.forPath(spark, DL_DAILY_DISC_PATH)
    (
        tgt.alias("t")
        .merge(
            discounts.alias("s"),
            "t.shelf_id = s.shelf_id AND t.discount_date = s.discount_date"
        )
        .whenMatchedUpdate(set={
            "discount":   F.col("s.discount"),
            "created_at": F.col("s.created_at"),
        })
        .whenNotMatchedInsert(values={
            "shelf_id":      F.col("s.shelf_id"),
            "discount_date": F.col("s.discount_date"),
            "discount":      F.col("s.discount"),
            "created_at":    F.col("s.created_at"),
        })
        .execute()
    )
else:
    discounts.write.format("delta").mode("overwrite").save(DL_DAILY_DISC_PATH)

print(f"[daily_discount_manager] Delta mirror aggiornato in {DL_DAILY_DISC_PATH}")

# =========================
# 8) Alert Kafka (opzionale)
# =========================
alerts = (
    discounts
    .select("shelf_id")
    .distinct()
    .withColumn("event_type", F.lit("near_expiry_discount"))
    .withColumn("location",  F.lit("store"))
    .withColumn("severity",  F.lit("high"))
    .withColumn("current_stock", F.lit(None).cast("int"))
    .withColumn("min_qty",       F.lit(None).cast("int"))
    .withColumn("threshold_pct", F.lit(None).cast("double"))
    .withColumn("stock_pct",     F.lit(None).cast("double"))
    .withColumn("suggested_qty", F.lit(0))
    .withColumn("created_at",    now_col)
)

alerts_out = (
    alerts
    .withColumn("value", F.to_json(F.struct(
        "event_type", "shelf_id", "location", "severity",
        "current_stock", "min_qty", "threshold_pct",
        "stock_pct", "suggested_qty", "created_at"
    )))
    .select(F.lit(None).cast("string").alias("key"), F.col("value").cast("string"))
)

alerts_out.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("topic", TOPIC_ALERTS) \
    .save()

print(f"[daily_discount_manager] Emessi {alerts.count()} alert near_expiry_discount su {TOPIC_ALERTS}")

spark.stop()
