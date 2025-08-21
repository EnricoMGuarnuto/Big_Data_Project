import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# =====================
# Schema dei dati shelf_events
# =====================
shelf_schema = StructType([
    StructField("event_type", StringType()),
    StructField("customer_id", StringType()),
    StructField("item_id", StringType()),
    StructField("weight", DoubleType()),
    StructField("quantity", IntegerType()),
    StructField("timestamp", StringType()),  # verrà convertito
])

# =====================
# Spark Session
# =====================
spark = (
    SparkSession.builder
    .appName("shelf-events-pipeline")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Variabili ambiente
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_SHELF = os.getenv("TOPIC_SHELF", "shelf_events")

MINIO_URL = "s3a://retail-lake"
PG_URL = f"jdbc:postgresql://{os.getenv('PG_HOST', 'postgres')}:{os.getenv('PG_PORT', '5432')}/{os.getenv('PG_DB', 'retaildb')}"
PG_PROPS = {"user": os.getenv("PG_USER", "retail"), "password": os.getenv("PG_PASS", "retailpass")}

# =====================
# Lettura da Kafka (raw → Bronze)
# =====================
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVERS)
    .option("subscribe", TOPIC_SHELF)
    .option("startingOffsets", "latest")
    .load()
    .withColumn("value_str", col("value").cast("string"))
)

# Bronze = dati grezzi
bronze_query = (
    raw_df.writeStream
    .format("parquet")
    .option("path", f"{MINIO_URL}/bronze/shelf_events")
    .option("checkpointLocation", f"{MINIO_URL}/checkpoints/shelf_events/bronze")
    .start()
)

# =====================
# Parsing → Silver
# =====================
parsed_df = (
    raw_df
    .select(from_json(col("value_str"), shelf_schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp("timestamp"))
    .withColumn("dt", to_date("timestamp"))
)

silver_query = (
    parsed_df.writeStream
    .format("parquet")
    .option("path", f"{MINIO_URL}/silver/shelf_events")
    .option("checkpointLocation", f"{MINIO_URL}/checkpoints/shelf_events/silver")
    .partitionBy("dt")
    .start()
)

# =====================
# Sink → PostgreSQL (solo dati Silver validati)
# =====================
def write_to_pg(batch_df, batch_id):
    (batch_df.write
        .jdbc(url=PG_URL, table="shelf_events", mode="append", properties=PG_PROPS))

pg_query = (
    parsed_df.writeStream
    .foreachBatch(write_to_pg)
    .option("checkpointLocation", f"{MINIO_URL}/checkpoints/shelf_events/pgsink")
    .start()
)

spark.streams.awaitAnyTermination()
