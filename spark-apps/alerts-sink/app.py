import os
import time
from pyspark.sql import SparkSession, functions as F, types as T

# =========================
# Env / Config
# =========================
KAFKA_BROKER  = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_ALERTS  = os.getenv("TOPIC_ALERTS", "alerts")              # append-only
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "earliest")

DELTA_ROOT    = os.getenv("DELTA_ROOT", "/delta")
DL_ALERTS_PATH = os.getenv("DL_ALERTS_PATH", f"{DELTA_ROOT}/ops/alerts")
CHECKPOINT    = os.getenv("CHECKPOINT", f"{DELTA_ROOT}/_checkpoints/alerts_sink")
DL_TXN_APP_ID = os.getenv("DL_TXN_APP_ID", "alerts_sink_delta_ops_alerts")

# Postgres sink (optional)
WRITE_TO_PG   = os.getenv("WRITE_TO_PG", "1") in ("1","true","True")
JDBC_PG_URL      = os.getenv("JDBC_PG_URL")
JDBC_PG_USER     = os.getenv("JDBC_PG_USER")
JDBC_PG_PASSWORD = os.getenv("JDBC_PG_PASSWORD")
PG_TABLE         = os.getenv("PG_TABLE", "ops.alerts")
PG_MAX_RETRIES   = int(os.getenv("PG_MAX_RETRIES", "5"))
PG_RETRY_SLEEP_S = float(os.getenv("PG_RETRY_SLEEP_S", "2.0"))

# =========================
# Spark Session (Delta)
# =========================
spark = (
    SparkSession.builder.appName("Alerts_Sink")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# =========================
# Helpers
# =========================
def _jdbc_url_with_stringtype_unspecified(url: str) -> str:
    """
    Ensures Postgres JDBC sends strings as 'unknown' so Postgres can implicitly cast
    them to enum columns (e.g. severity_level, alert_status).
    """
    if not url:
        return url
    if "stringtype=unspecified" in url:
        return url
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}stringtype=unspecified"

# =========================
# Schema for alerts
# (as emitted by shelf/wh alert engines)
# =========================
schema_alert = T.StructType([
    T.StructField("event_type", T.StringType()),
    T.StructField("shelf_id", T.StringType()),
    T.StructField("location", T.StringType()),           # 'store' | 'warehouse'
    T.StructField("severity", T.StringType()),           # 'low'|'medium'|'high'|'critical'
    T.StructField("current_stock", T.IntegerType()),
    T.StructField("min_qty", T.IntegerType()),           # from shelf engine
    T.StructField("threshold_pct", T.DoubleType()),      # from shelf engine
    T.StructField("stock_pct", T.DoubleType()),          # from shelf engine
    T.StructField("suggested_qty", T.IntegerType()),
    T.StructField("created_at", T.TimestampType()),
])

# =========================
# ForeachBatch -> Delta + (optional) Postgres
# =========================

def write_batch_to_sinks(parsed_df, batch_id: int):
    if parsed_df.rdd.isEmpty():
        return

    # 1) Delta Lake (append) with idempotency on retries (same batch_id)
    (parsed_df
     .write.format("delta")
     .mode("append")
     .option("mergeSchema", "true")
     .option("txnAppId", DL_TXN_APP_ID)
     .option("txnVersion", str(batch_id))
     .save(DL_ALERTS_PATH))

    # 2) Postgres (append) — map to ops.alerts columns only
    if WRITE_TO_PG and JDBC_PG_URL and JDBC_PG_USER and JDBC_PG_PASSWORD:
        pg_df = (
            parsed_df.select(
                F.col("event_type"),
                F.col("shelf_id"),
                F.col("location"),
                F.col("severity").cast("string").alias("severity"),
                F.col("current_stock"),
                # columns not present in the incoming alert but present in table -> set as null/defaults
                F.lit(None).cast("int").alias("max_stock"),
                F.col("threshold_pct").alias("target_pct"),  # reuse threshold as target if you want visibility
                F.col("suggested_qty"),
                F.lit("open").cast("string").alias("status"),
                F.col("created_at").alias("created_at"),
                F.current_timestamp().alias("updated_at")
            )
        )
        jdbc_url = _jdbc_url_with_stringtype_unspecified(JDBC_PG_URL)
        last_err = None
        for attempt in range(1, PG_MAX_RETRIES + 1):
            try:
                (pg_df.write
                    .format("jdbc")
                    .option("url", jdbc_url)
                    .option("user", JDBC_PG_USER)
                    .option("password", JDBC_PG_PASSWORD)
                    .option("dbtable", PG_TABLE)
                    # also set as driver properties (safe even if URL already includes it)
                    .option("stringtype", "unspecified")
                    .mode("append")
                    .save())
                last_err = None
                break
            except Exception as e:
                last_err = e
                print(f"[pg] write failed (attempt {attempt}/{PG_MAX_RETRIES}): {e}")
                time.sleep(PG_RETRY_SLEEP_S)
        if last_err is not None:
            # Fail the micro-batch so operators notice; Delta write above is idempotent via txnAppId/txnVersion.
            raise last_err

# =========================
# Streaming read from Kafka (alerts)
# =========================
raw = (spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", KAFKA_BROKER)
       .option("subscribe", TOPIC_ALERTS)
       .option("startingOffsets", STARTING_OFFSETS)
       .option("failOnDataLoss", "false")
       .load())

parsed = (raw
          .select(F.col("value").cast("string").alias("json_str"))
          .select(F.from_json("json_str", schema_alert).alias("a"))
          .select("a.*")
          .withColumn("created_at", F.coalesce(F.col("created_at"), F.current_timestamp()))
          )

query = (parsed.writeStream
         .foreachBatch(write_batch_to_sinks)
         .option("checkpointLocation", CHECKPOINT)
         .start())

query.awaitTermination()
