import os
from kafka import KafkaAdminClient
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, to_date, expr, when, lit, coalesce
)
from pyspark.sql.types import StructType, StructField, StringType


def wait_for_topic(topic, bootstrap, attempts=20, sleep_s=10):
    """Aspetta che il topic Kafka sia disponibile prima di far partire Spark"""
    for i in range(1, attempts+1):
        try:
            admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="spark-foot-traffic-realistic-check")
            topics = admin.list_topics()
            admin.close()
            if topic in topics:
                print(f"[spark-foot-traffic] ✅ Trovato topic {topic}")
                return
            print(f"[spark-foot-traffic] ⏳ Topic {topic} non ancora presente (tentativo {i}/{attempts})")
        except Exception as e:
            print(f"[spark-foot-traffic] ⚠️ Errore check topic: {e}")
        time.sleep(sleep_s)
    raise RuntimeError(f"[spark-foot-traffic] ❌ Topic {topic} non trovato dopo {attempts} tentativi")
# -----------------------------
# Spark
# -----------------------------
spark = (
    SparkSession.builder.appName("foot-traffic-pipeline")
    .config("spark.sql.streaming.metadata.compression", "false")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    .config("spark.sql.parquet.output.committer.class", "org.apache.parquet.hadoop.ParquetOutputCommitter")
    .config("spark.hadoop.fs.s3a.committer.name", "directory")
    .config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a", "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "minio"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "minio123"))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# Env
# -----------------------------
KAFKA_SERVERS   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_REALISTIC = os.getenv("TOPIC_FOOT_TRAFFIC_REALISTIC", "foot_traffic_realistic")  # entry/exit
MINIO_URL       = os.getenv("MINIO_URL", "s3a://retail-lake")  # /bronze /silver
PG_URL          = f"jdbc:postgresql://{os.getenv('PG_HOST','postgres')}:{os.getenv('PG_PORT','5432')}/{os.getenv('PG_DB','retaildb')}"
PG_PROPS        = {"user": os.getenv("PG_USER","retail"), "password": os.getenv("PG_PASS","retailpass"), "driver":"org.postgresql.Driver"}

# -----------------------------
# Schema realistic (ENTRY/EXIT)
# -----------------------------
ft_realistic_schema = StructType([
    StructField("event_type", StringType(), True),  # "entry" | "exit"
    StructField("time",       StringType(), True),  # ISO-8601
    StructField("weekday",    StringType(), True),
    StructField("time_slot",  StringType(), True),  # "07:00–09:59"
])

# ===== Kafka → raw realistic

wait_for_topic(TOPIC_REALISTIC, KAFKA_SERVERS)

parsed_df = (
    spark.readStream.format("kafka")
         .option("kafka.bootstrap.servers", KAFKA_SERVERS)
         .option("subscribe", TOPIC_REALISTIC)
         .option("startingOffsets", "latest")
         .option("failOnDataLoss", "false")
         .load()
         .withColumn("value_str", col("value").cast("string"))
         .select(from_json(col("value_str"), ft_realistic_schema).alias("data"))
         .select("data.*")
         .withColumn("event_id", expr("uuid()"))
         .withColumn("event_time", to_timestamp("time"))
         .withColumn("dt", to_date("time"))
         .withColumn("event_type",
                     when(col("event_type").isin("entry","exit"), col("event_type")).otherwise(lit("entry")))
)

# ===== Bronze (JSON) → MinIO
bronze_q = (
    parsed_df
      .select("event_id","event_type","event_time","weekday","time_slot","dt")
      .writeStream
      .format("json")
      .option("path", f"{MINIO_URL}/bronze/foot_traffic")
      .option("checkpointLocation", "/chk/foot_traffic/bronze")
      .partitionBy("dt")
      .start()
)

# ===== Silver (Parquet) → MinIO
silver_q = (
    parsed_df
      .select("event_id","event_type","event_time","weekday","time_slot","dt")
      .writeStream
      .format("parquet")
      .option("path", f"{MINIO_URL}/silver/foot_traffic")
      .option("checkpointLocation", "/chk/foot_traffic/silver")
      .partitionBy("dt")
      .start()
)

# ===== Sink → Postgres (foreachBatch)
def write_to_pg(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    rows = (
        batch_df
        .select("event_id", "event_time", "event_type", "weekday", "time_slot")
        .toLocalIterator()
    )

    import psycopg2
    from psycopg2.extras import execute_batch

    conn = psycopg2.connect(
        host=os.getenv("PG_HOST", "postgres"),
        port=os.getenv("PG_PORT", "5432"),
        dbname=os.getenv("PG_DB", "retaildb"),
        user=os.getenv("PG_USER", "retail"),
        password=os.getenv("PG_PASS", "retailpass"),
    )
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            execute_batch(
                cur,
                "SELECT apply_foot_traffic_event(%s, %s, %s, %s, %s)",
                [(str(r["event_id"]), r["event_time"], r["event_type"], r["weekday"], r["time_slot"]) for r in rows],
                page_size=1000
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

# ===== Sink → Postgres (foreachBatch)
pg_q = (
    parsed_df
      .writeStream
      .foreachBatch(write_to_pg)
      .option("checkpointLocation", "/chk/foot_traffic/postgres")
      .start()
)


spark.streams.awaitAnyTermination()
