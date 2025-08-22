import os
import json
from typing import Any, Dict
from decimal import Decimal

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    to_date,
    expr,
    when,
    explode,
    current_timestamp,
    lit,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, MapType
)

# =============================================================
# Spark Structured Streaming consumer for POS transactions
# =============================================================

# ------------------
# ENV / Config
# ------------------
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
POS_TOPIC = os.getenv("POS_TOPIC", "pos_transactions")
STARTING_OFFSETS = os.getenv("POS_STARTING_OFFSETS", "latest")  # o "earliest"

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "retaildb")
PG_USER = os.getenv("PG_USER", "retail")
PG_PASS = os.getenv("PG_PASS", "retailpass")

# MinIO / S3A
MINIO_URL = os.getenv("MINIO_URL", "s3a://retail-lake")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minio")
S3_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")
BRONZE_PATH = f"{MINIO_URL}/bronze/pos"
SILVER_PATH = f"{MINIO_URL}/silver/pos"

# Psycopg2 only on the driver inside foreachBatch
import psycopg2

# ------------------
# Spark session
# ------------------
spark = (
    SparkSession.builder.appName("pos-events-pipeline")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ------------------
# Schemas
# ------------------
item_schema = StructType([
    StructField("item_id",    StringType(), True),
    StructField("quantity",   IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("discount",   DoubleType(), True),
    StructField("total_price",DoubleType(), True),
])

pos_schema = StructType([
    StructField("event_type",     StringType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("customer_id",    StringType(), True),
    StructField("timestamp",      StringType(), True),
    StructField("items",          ArrayType(item_schema), True),
    StructField("meta",           MapType(StringType(), StringType()), True),
])

# ------------------
# Kafka source
# ------------------
raw = (
    spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", POS_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .load()
)

parsed = (
    raw.withColumn("value_str", col("value").cast("string"))
       .select(from_json(col("value_str"), pos_schema).alias("d"))
       .select("d.*")
       .filter(col("event_type") == lit("pos_transaction"))
       .withColumn("transaction_id", when(col("transaction_id").isNull(), expr("uuid()"))
                                      .otherwise(col("transaction_id")))
       .withColumn("closed_ts", to_timestamp(col("timestamp")))
       .withColumn("biz_date",  to_date(col("timestamp")))
       .withColumn("ingestion_ts", current_timestamp())
)

# Bronze: header
bronze_df = parsed

# Silver: righe (explode items)
exploded = (
    parsed.withColumn("it", explode("items"))
          .select(
              "transaction_id", "customer_id", "biz_date", "closed_ts", "meta",
              col("it.item_id").alias("shelf_id"),
              col("it.quantity").alias("quantity"),
              col("it.unit_price").alias("unit_price"),
              col("it.discount").alias("discount"),
          )
          .withColumn("line_total", expr("quantity * (unit_price - coalesce(discount, 0.0))"))
          .withColumn("ingestion_ts", current_timestamp())
)

# ------------------
# Foreach-batch sink to Postgres
# ------------------
def _connect_pg():
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS)
    conn.autocommit = False
    return conn

def write_batch_to_pg(batch_df: DataFrame, batch_id: int):
    """Chiama apply_pos_transaction + apply_sale_event + stream_events"""
    if batch_df.rdd.isEmpty():
        return

    rows = (
        batch_df.select(
            "transaction_id", "customer_id", "biz_date", "closed_ts", "meta",
            "shelf_id", "quantity", "unit_price", "discount"
        ).toLocalIterator()
    )

    grouped: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        tx = r["transaction_id"]
        g = grouped.setdefault(
            tx,
            {"customer_id": r["customer_id"], "biz_date": r["biz_date"], "closed_ts": r["closed_ts"], "meta": r["meta"], "lines": []},
        )
        g["lines"].append({
            "item_id": r["shelf_id"],
            "quantity": int(r["quantity"]) if r["quantity"] is not None else 0,
            "unit_price": float(r["unit_price"]),
            "discount": float(r["discount"]) if r["discount"] is not None else 0.0,
            "total_price": float(r["quantity"] or 0) * (float(r["unit_price"]) - float(r["discount"] or 0.0)),
        })

    conn = _connect_pg()
    try:
        with conn.cursor() as cur:
            for tx_id, payload in grouped.items():
                cust = payload["customer_id"]
                closed_ts = payload["closed_ts"]
                lines = payload["lines"]
                meta = payload["meta"] or {}

                # 1) apply_pos_transaction
                tx_json = {
                    "event_type": "pos_transaction",
                    "transaction_id": tx_id,
                    "customer_id": cust,
                    "timestamp": closed_ts.isoformat() if closed_ts else None,
                    "items": lines,
                    "meta": meta,
                }
                cur.execute("SELECT apply_pos_transaction(%s::jsonb)", (json.dumps(tx_json),))

                # 2) apply_sale_event per riga
                for ln in lines:
                    cur.execute(
                        "SELECT apply_sale_event(%s,%s,%s,%s,%s)",
                        (f"pos-{tx_id}", closed_ts, ln["item_id"], int(ln["quantity"]), json.dumps(meta)),
                    )

                # 3) log stream_events
                cur.execute(
                    """
                    INSERT INTO stream_events(event_id, source, event_ts, payload)
                    VALUES (%s,%s,%s,%s)
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    (f"pos-{tx_id}", "pos.sales", closed_ts, json.dumps(tx_json)),
                )

            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

# ------------------
# Start the sinks
# ------------------
bronze_query = (
    bronze_df.writeStream.format("json")
        .partitionBy("biz_date")
        .option("path", BRONZE_PATH)
        .option("checkpointLocation", "/chk/pos/bronze")
        .outputMode("append")
        .start()
)

silver_query = (
    exploded.writeStream.format("parquet")
        .partitionBy("biz_date")
        .option("path", SILVER_PATH)
        .option("checkpointLocation", "/chk/pos/silver")
        .outputMode("append")
        .start()
)

pg_query = (
    exploded.writeStream
        .foreachBatch(write_batch_to_pg)
        .option("checkpointLocation", "/chk/pos/postgres")
        .outputMode("update")
        .start()
)

spark.streams.awaitAnyTermination()
