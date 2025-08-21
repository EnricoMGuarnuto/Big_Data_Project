
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, lit
from pyspark.sql.types import StructType, StringType, IntegerType

# Define the schema for foot traffic records
schema = StructType() \
    .add("Visit_ID", StringType()) \
    .add("Customer_ID", StringType()) \
    .add("Entry_Timestamp", StringType()) \
    .add("Exit_Timestamp", StringType()) \
    .add("Duration_Minutes", IntegerType())

# Start Spark session
spark = SparkSession.builder \
    .appName("KafkaToMinIO_FootTraffic") \
    .getOrCreate()

# Read from Kafka topic
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "foot_traffic") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON messages
df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json_str") \
    .withColumn("data", from_json(col("json_str"), schema)) \
    .select("data.*") \
    .withColumn("ingestion_time", current_timestamp()) \
    .withColumn("source", lit("kafka:foot_traffic"))

# Write raw data to MinIO in JSON format
query = df_parsed.writeStream \
    .format("json") \
    .option("path", "s3a://retail/raw/foot_traffic/") \
    .option("checkpointLocation", "s3a://retail/raw/checkpoints/foot_traffic/") \
    .partitionBy("ingestion_time") \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .start()

query.awaitTermination()
