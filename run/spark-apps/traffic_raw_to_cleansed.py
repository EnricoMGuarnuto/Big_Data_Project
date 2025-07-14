from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType

# Define the schema explicitly
schema = StructType() \
    .add("Visit_ID", StringType()) \
    .add("Customer_ID", StringType()) \
    .add("Entry_Timestamp", StringType()) \
    .add("Exit_Timestamp", StringType()) \
    .add("Duration_Minutes", IntegerType()) \
    .add("ingestion_time", TimestampType()) \
    .add("source", StringType())

# Start Spark session
spark = SparkSession.builder \
    .appName("Cleanse_FootTraffic_Raw") \
    .getOrCreate()

# Read raw JSON files from MinIO
df_raw = spark.read.schema(schema).json("s3a://retail/raw/foot_traffic/")

# Drop rows with nulls in critical fields
df_clean = df_raw.dropna(subset=["Visit_ID", "Customer_ID", "Entry_Timestamp", "Exit_Timestamp"])

# Deduplicate by Visit_ID (assuming unique visit ID)
df_clean = df_clean.dropDuplicates(["Visit_ID"])

# Cast timestamps correctly
df_clean = df_clean.withColumn("Entry_Timestamp", col("Entry_Timestamp").cast(TimestampType()))
df_clean = df_clean.withColumn("Exit_Timestamp", col("Exit_Timestamp").cast(TimestampType()))

# Write to cleansed layer in Parquet format
df_clean.coalesce(1).write.mode("overwrite").parquet("s3a://retail/cleansed/foot_traffic/")

print("✅ Foot traffic raw data cleansed and written to Parquet.")