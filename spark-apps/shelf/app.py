import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, to_date, coalesce, expr, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Spark
spark = (SparkSession.builder.appName("shelf-events-pipeline").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# Env
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_SHELF    = os.getenv("TOPIC_SHELF", "shelf_events")
MINIO_URL      = "s3a://retail-lake"  # bucket unico con cartelle /bronze /silver
PG_URL  = f"jdbc:postgresql://{os.getenv('PG_HOST','postgres')}:{os.getenv('PG_PORT','5432')}/{os.getenv('PG_DB','retaildb')}"
PG_PROPS = {"user": os.getenv("PG_USER","retail"), "password": os.getenv("PG_PASS","retailpass"), "driver":"org.postgresql.Driver"}

# Schema permissivo (basta per i tuoi eventi)
shelf_schema = StructType([
    StructField("event_id",     StringType(),  True),     # se manca lo generiamo
    StructField("event_type",   StringType(),  True),     # "weight_change"
    StructField("customer_id",  StringType(),  True),
    StructField("item_id",      StringType(),  True),
    StructField("shelf_id",     StringType(),  True),     # se manca, usiamo item_id
    StructField("delta_weight", DoubleType(),  True),
    StructField("timestamp",    StringType(),  True),
])

# ===== Kafka → raw
raw_df = (
    spark.readStream.format("kafka")
         .option("kafka.bootstrap.servers", KAFKA_SERVERS)
         .option("subscribe", TOPIC_SHELF)
         .option("startingOffsets", "latest")
         .load()
         .withColumn("value_str", col("value").cast("string"))
)

# ===== Bronze su MinIO
bronze_q = (
    raw_df.writeStream
          .format("parquet")
          .option("path", f"{MINIO_URL}/bronze/shelf_events")
          .option("checkpointLocation", "/chk/shelf_events/bronze")  # checkpoint locale nel container
          .start()
)

# ===== Parse → normalizza
parsed_df = (
    raw_df
      .select(from_json(col("value_str"), shelf_schema).alias("data"))
      .select("data.*")
      .withColumn("shelf_id", coalesce(col("shelf_id"), col("item_id")))
      .withColumn("event_id", when(col("event_id").isNull(), expr("uuid()")).otherwise(col("event_id")))
      .withColumn("event_time", to_timestamp("timestamp"))
      .withColumn("dt", to_date("timestamp"))
)

# ===== Silver su MinIO
silver_q = (
    parsed_df.writeStream
             .format("parquet")
             .option("path", f"{MINIO_URL}/silver/shelf_events")
             .option("checkpointLocation", "/chk/shelf_events/silver")
             .partitionBy("dt")
             .start()
)

# ===== Sink → Postgres SOLO weight_change
def write_to_pg(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    from pyspark.sql.functions import struct
    rows = (batch_df
            .filter(col("event_type") == "weight_change")
            .filter(col("delta_weight").isNotNull())
            .select("event_id","event_time","shelf_id","delta_weight")
            .collect())

    import psycopg2
    from psycopg2.extras import execute_batch
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST","postgres"),
        port=os.getenv("PG_PORT","5432"),
        dbname=os.getenv("PG_DB","retaildb"),
        user=os.getenv("PG_USER","retail"),
        password=os.getenv("PG_PASS","retailpass"),
    )
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            execute_batch(
                cur,
                "SELECT apply_shelf_weight_event(%s,%s,%s,%s,false,'{}'::jsonb,0.25)",
                [(r['event_id'], r['event_time'], r['shelf_id'], float(r['delta_weight'])) for r in rows],
                page_size=1000
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

spark.streams.awaitAnyTermination()
