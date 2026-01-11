import os
from pyspark.sql import SparkSession, functions as F, types as T

# =========================
# Env / Config
# =========================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_FOOT_TRAFFIC = os.getenv("TOPIC_FOOT_TRAFFIC", "foot_traffic")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "earliest")

DELTA_ROOT = os.getenv("DELTA_ROOT", "/delta")
DL_FOOT_TRAFFIC_PATH = os.getenv("DL_FOOT_TRAFFIC_PATH", f"{DELTA_ROOT}/raw/foot_traffic")
CHECKPOINT = os.getenv("CHECKPOINT", f"{DELTA_ROOT}/_checkpoints/foot_traffic_raw_sink")

# =========================
# Spark Session (Delta)
# =========================
spark = (
    SparkSession.builder.appName("FootTrafficRawSink")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Incoming schema (matches foot_traffic producer payload)
schema_foot = T.StructType([
    T.StructField("event_type", T.StringType()),
    T.StructField("customer_id", T.StringType()),
    T.StructField("entry_time", T.StringType()),
    T.StructField("exit_time", T.StringType()),
    T.StructField("trip_duration_minutes", T.IntegerType()),
    T.StructField("weekday", T.StringType()),
    T.StructField("time_slot", T.StringType()),
])

# =========================
# Streaming read (Kafka) -> Delta append
# =========================
kafka_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC_FOOT_TRAFFIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .option("kafka.group.id", "foot_traffic_raw_sink")

    # robust timeouts
    .option("kafka.request.timeout.ms", "180000")
    .option("kafka.default.api.timeout.ms", "180000")
    .option("kafka.session.timeout.ms", "30000")
    .option("kafka.heartbeat.interval.ms", "10000")
    .option("kafka.metadata.max.age.ms", "10000")

    .option("maxOffsetsPerTrigger", "5000")
    .load()
)



parsed = (
    kafka_stream
    .select(F.col("value").cast("string").alias("payload"))
    .withColumn("json", F.from_json("payload", schema_foot))
    .select(
        F.col("json.event_type").alias("event_type"),
        F.col("json.customer_id").alias("customer_id"),
        F.to_timestamp("json.entry_time").alias("entry_time"),
        F.to_timestamp("json.exit_time").alias("exit_time"),
        F.col("json.trip_duration_minutes").cast("int").alias("trip_duration_minutes"),
        F.col("json.weekday").alias("weekday"),
        F.col("json.time_slot").alias("time_slot"),
    )
    .filter(F.col("event_type").isNotNull())
)

query = (
    parsed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .option("mergeSchema", "true")
    .option("path", DL_FOOT_TRAFFIC_PATH)
    .start()
)

query.awaitTermination()
