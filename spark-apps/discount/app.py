import os
import json
import time
import psycopg2
from typing import Any, Dict

from kafka.admin import KafkaAdminClient

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_timestamp, to_date,
    current_timestamp, lit, explode
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, ArrayType
)

# ========================
# ENV / Config
# ========================
KAFKA_SERVERS    = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
DISCOUNT_TOPIC   = os.getenv("DISCOUNT_TOPIC", "weekly_discounts")
STARTING_OFFSETS = os.getenv("DISCOUNT_STARTING_OFFSETS", "earliest")

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "retaildb")
PG_USER = os.getenv("PG_USER", "retail")
PG_PASS = os.getenv("PG_PASS", "retailpass")

MINIO_URL     = os.getenv("MINIO_URL", "s3a://retail-lake")
S3_ENDPOINT   = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minio")
S3_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")

BRONZE_PATH = f"{MINIO_URL}/bronze/discounts"
SILVER_PATH = f"{MINIO_URL}/silver/discounts"

# ========================
# Helper: wait for topic
# ========================
def wait_for_topic(topic, bootstrap, attempts=20, sleep_s=3):
    """Aspetta che il topic Kafka sia disponibile prima di far partire Spark"""
    for i in range(1, attempts+1):
        try:
            admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="spark-discount-check")
            topics = admin.list_topics()
            admin.close()
            if topic in topics:
                print(f"[spark-discount] ✅ Trovato topic {topic}")
                return
            print(f"[spark-discount] ⏳ Topic {topic} non ancora presente (tentativo {i}/{attempts})")
        except Exception as e:
            print(f"[spark-discount] ⚠️ Errore check topic: {e}")
        time.sleep(sleep_s)
    raise RuntimeError(f"[spark-discount] ❌ Topic {topic} non trovato dopo {attempts} tentativi")

# ========================
# Spark session
# ========================
spark = (
    SparkSession.builder.appName("discount-updater-pipeline")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ========================
# Schema messaggi Kafka
# ========================
discount_schema = StructType([
    StructField("event_type", StringType(), True),   # "weekly_discount"
    StructField("week",       StringType(), True),
    StructField("discounts",  ArrayType(
        StructType([
            StructField("item_id", StringType(), True),
            StructField("discount", DoubleType(), True),
        ])
    ), True),
    StructField("created_at", StringType(), True),
])

# ========================
# Kafka source (con wait_for_topic)
# ========================
wait_for_topic(DISCOUNT_TOPIC, KAFKA_SERVERS)

raw = (
    spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", DISCOUNT_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .load()
)

# ========================
# DEBUG: stampa messaggi grezzi Kafka
# ========================
raw_console_q = (
    raw.selectExpr("CAST(value AS STRING)")
       .writeStream
       .format("console")
       .option("truncate", "false")
       .start()
)

# ========================
# Parsing
# ========================
parsed = (
    raw.withColumn("value_str", col("value").cast("string"))
       .select(from_json(col("value_str"), discount_schema).alias("d"))
       .select("d.*")
       .filter(col("event_type") == lit("weekly_discount"))
       .withColumn("created_at_ts", to_timestamp("created_at"))
       .withColumn("biz_date", to_date("created_at"))
       .withColumn("ingestion_ts", current_timestamp())
)

# Explode discounts[]
exploded = (
    parsed.withColumn("disc", explode("discounts"))
          .select(
              "week", "created_at_ts", "biz_date", "ingestion_ts",
              col("disc.item_id").alias("item_id"),
              col("disc.discount").alias("discount"),
          )
)

# Bronze: JSON (messaggio intero)
bronze_df = parsed

# Silver: flat table (una riga per item)
silver_df = exploded

# ========================
# Write to Postgres
# ========================
def _connect_pg():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASS
    )

def write_batch_to_pg(batch_df: DataFrame, batch_id: int):
    if batch_df.rdd.isEmpty():
        print(f"[spark-discount] ⚠️ Batch {batch_id} vuoto, skip")
        return

    print(f"[spark-discount] 🔥 Batch {batch_id} con {batch_df.count()} righe")

    rows = batch_df.toLocalIterator()
    conn = _connect_pg()
    try:
        with conn.cursor() as cur:
            for r in rows:
                print(f"[spark-discount] → Inserisco {r['item_id']} con sconto {r['discount']} (week {r['week']})")
                
                # 1) scrivi in discount_history con mapping shelf_id -> item_id
                cur.execute("""
                    INSERT INTO discount_history(item_id, week, discount, created_at)
                    SELECT i.item_id, %s, %s, now()
                    FROM items i
                    WHERE i.shelf_id = %s
                    ON CONFLICT (item_id, week) DO UPDATE SET
                        discount=EXCLUDED.discount,
                        created_at=EXCLUDED.created_at
                """, (r["week"], r["discount"], r["item_id"]))

                # 2) aggiorna product_inventory usando lo stesso mapping shelf_id -> item_id
                cur.execute("""
                    UPDATE product_inventory pi
                    SET price = price * (1 - %s)
                    FROM items i
                    WHERE pi.item_id = i.item_id
                    AND i.shelf_id = %s
                    AND pi.location_id = (SELECT location_id FROM locations WHERE location='instore')
                """, (r["discount"], r["item_id"]))

        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[spark-discount] ❌ Errore batch {batch_id}: {e}")
        raise
    finally:
        conn.close()

# ========================
# Start sinks
# ========================
bronze_q = (
    bronze_df.writeStream.format("json")
        .partitionBy("biz_date")
        .option("path", BRONZE_PATH)
        .option("checkpointLocation", "/chk/discounts/bronze")
        .outputMode("append")
        .start()
)

silver_q = (
    silver_df.writeStream.format("parquet")
        .partitionBy("biz_date")
        .option("path", SILVER_PATH)
        .option("checkpointLocation", "/chk/discounts/silver")
        .outputMode("append")
        .start()
)

silver_console_q = (
    silver_df.writeStream
        .format("console")
        .option("truncate", "false")
        .outputMode("append")
        .start()
)

pg_q = (
    silver_df.writeStream
        .foreachBatch(write_batch_to_pg)
        .option("checkpointLocation", "/chk/discounts/postgres")
        .outputMode("append")
        .start()
)

spark.streams.awaitAnyTermination()
