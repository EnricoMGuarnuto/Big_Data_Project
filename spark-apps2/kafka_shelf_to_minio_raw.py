from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, lit
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType

# Define the schema for shelf sensor data
schema = StructType() \
    .add("Sensor_ID", StringType()) \
    .add("Item_Identifier", StringType()) \
    .add("Item_Type", StringType()) \
    .add("Shelf_Stock", IntegerType()) \
    .add("Last_Updated", StringType())

# Start Spark session
spark = SparkSession.builder \
    .appName("KafkaToMinIO_ShelfSensors") \
    .getOrCreate()

# Read from Kafka topic
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "shelf_events") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON and enrich
df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json_str") \
    .withColumn("data", from_json(col("json_str"), schema)) \
    .select("data.*") \
    .withColumn("ingestion_time", current_timestamp()) \
    .withColumn("source", lit("kafka:shelf_events"))

# Write raw JSON to MinIO
query = df_parsed.writeStream \
    .format("json") \
    .option("path", "s3a://retail/raw/shelf_events/") \
    .option("checkpointLocation", "s3a://retail/raw/checkpoints/shelf_events/") \
    .partitionBy("ingestion_time") \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .start()

query.awaitTermination()