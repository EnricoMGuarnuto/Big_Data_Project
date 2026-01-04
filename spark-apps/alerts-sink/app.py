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
DELTA_WRITE_MAX_RETRIES = int(os.getenv("DELTA_WRITE_MAX_RETRIES", "8"))
DELTA_WRITE_RETRY_BASE_S = float(os.getenv("DELTA_WRITE_RETRY_BASE_S", "1.0"))

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


def _is_delta_conflict(err: Exception) -> bool:
    name = err.__class__.__name__
    msg = str(err)
    return (
        "Concurrent" in name
        or "DELTA_CONCURRENT" in msg
        or "MetadataChangedException" in name
        or "DELTA_METADATA_CHANGED" in msg
        or "ProtocolChangedException" in name
    )


def _write_delta_with_retries(df, batch_id: int) -> None:
    last = None
    for attempt in range(1, DELTA_WRITE_MAX_RETRIES + 1):
        try:
            (df.write.format("delta")
             .mode("append")
             .option("mergeSchema", "true")
             .option("txnAppId", DL_TXN_APP_ID)
             .option("txnVersion", str(batch_id))
             .save(DL_ALERTS_PATH))
            return
        except Exception as e:
            last = e
            if not _is_delta_conflict(e) or attempt == DELTA_WRITE_MAX_RETRIES:
                raise
            sleep_s = DELTA_WRITE_RETRY_BASE_S * attempt
            print(f"[delta-retry] alerts_sink append conflict ({attempt}/{DELTA_WRITE_MAX_RETRIES}) -> sleep {sleep_s}s: {e}")
            time.sleep(sleep_s)
    raise last  # pragma: no cover

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
    # Optional fields for status change events
    T.StructField("alert_id", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("timestamp", T.TimestampType()),
    T.StructField("alert_event_type", T.StringType()),
])

# =========================
# ForeachBatch -> Delta + (optional) Postgres
# =========================

def _pg_connection():
    jdbc_url = _jdbc_url_with_stringtype_unspecified(JDBC_PG_URL)
    jvm = spark._sc._gateway.jvm
    props = jvm.java.util.Properties()
    props.setProperty("user", JDBC_PG_USER)
    props.setProperty("password", JDBC_PG_PASSWORD)
    props.setProperty("stringtype", "unspecified")
    return jvm.java.sql.DriverManager.getConnection(jdbc_url, props)


def _apply_status_updates(status_df):
    if not (WRITE_TO_PG and JDBC_PG_URL and JDBC_PG_USER and JDBC_PG_PASSWORD):
        return
    rows = status_df.select(
        "alert_id", "shelf_id", "location", "alert_event_type", "status"
    ).collect()
    if not rows:
        return

    conn = _pg_connection()
    stmt_id = None
    stmt_key = None
    stmt_key_no_type = None
    try:
        stmt_id = conn.prepareStatement(
            f"UPDATE {PG_TABLE} SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE alert_id = ?"
        )
        stmt_key = conn.prepareStatement(
            f"UPDATE {PG_TABLE} SET status = ?, updated_at = CURRENT_TIMESTAMP "
            "WHERE shelf_id = ? AND location = ? AND event_type = ? "
            "AND status IN ('open','active','pending')"
        )
        stmt_key_no_type = conn.prepareStatement(
            f"UPDATE {PG_TABLE} SET status = ?, updated_at = CURRENT_TIMESTAMP "
            "WHERE shelf_id = ? AND location = ? AND status IN ('open','active','pending')"
        )

        for r in rows:
            raw_status = (r["status"] or "ack").lower()

            # Map application-level statuses to DB enum values
            STATUS_MAP = {
                "open": "open",
                "active": "open",     # active alerts are still 'open' in DB
                "pending": "open",
                "ack": "ack",
                "acknowledged": "ack",
                "closed": "closed",
                "resolved": "closed",
            }

            status = STATUS_MAP.get(raw_status, "ack")

            alert_id = r["alert_id"]
            shelf_id = r["shelf_id"]
            location = r["location"]
            alert_event_type = r["alert_event_type"]

            if alert_id:
                stmt_id.setString(1, status)
                stmt_id.setString(2, alert_id)
                stmt_id.executeUpdate()
                continue

            if not shelf_id or not location:
                continue

            if alert_event_type:
                stmt_key.setString(1, status)
                stmt_key.setString(2, shelf_id)
                stmt_key.setString(3, location)
                stmt_key.setString(4, alert_event_type)
                stmt_key.executeUpdate()
            else:
                stmt_key_no_type.setString(1, status)
                stmt_key_no_type.setString(2, shelf_id)
                stmt_key_no_type.setString(3, location)
                stmt_key_no_type.executeUpdate()
    finally:
        if stmt_id is not None:
            stmt_id.close()
        if stmt_key is not None:
            stmt_key.close()
        if stmt_key_no_type is not None:
            stmt_key_no_type.close()
        conn.close()


def write_batch_to_sinks(parsed_df, batch_id: int):
    if parsed_df.rdd.isEmpty():
        return

    status_df = parsed_df.filter(F.col("event_type") == F.lit("alert_status_change"))
    alerts_df = parsed_df.filter(
        (F.col("event_type").isNotNull()) & (F.col("event_type") != F.lit("alert_status_change"))
    )

    if alerts_df.rdd.isEmpty() is False:
        alert_cols = [
            "event_type", "shelf_id", "location", "severity",
            "current_stock", "min_qty", "threshold_pct", "stock_pct",
            "suggested_qty", "created_at",
        ]
        alerts_out = alerts_df.select(*alert_cols)

        # 1) Delta Lake (append) with idempotency on retries (same batch_id)
        _write_delta_with_retries(alerts_out, batch_id)

        # 2) Postgres (append) — map to ops.alerts columns only
        if WRITE_TO_PG and JDBC_PG_URL and JDBC_PG_USER and JDBC_PG_PASSWORD:
            pg_df = (
                alerts_out.select(
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

    if status_df.rdd.isEmpty() is False:
        _apply_status_updates(status_df)

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
