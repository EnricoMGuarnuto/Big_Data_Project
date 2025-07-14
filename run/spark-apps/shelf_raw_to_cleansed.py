from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType

# Define the schema explicitly
schema = StructType() \
    .add("Sensor_ID", StringType()) \
    .add("Item_Identifier", StringType()) \
    .add("Item_Type", StringType()) \
    .add("Shelf_Stock", IntegerType()) \
    .add("Last_Updated", StringType()) \
    .add("ingestion_time", TimestampType()) \
    .add("source", StringType())

# Start Spark session
spark = SparkSession.builder \
    .appName("Cleanse_Shelf_Raw") \
    .getOrCreate()

# Read raw JSON files from MinIO
df_raw = spark.read.schema(schema).json("s3a://retail/raw/shelf_events/")

# Drop rows with nulls in critical fields
df_clean = df_raw.dropna(subset=["Sensor_ID", "Item_Identifier", "Shelf_Stock"])

# Deduplicate by Sensor_ID + Item_Identifier + ingestion_time
df_clean = df_clean.dropDuplicates(["Sensor_ID", "Item_Identifier", "ingestion_time"])

# Cast Last_Updated to timestamp
df_clean = df_clean.withColumn("Last_Updated", col("Last_Updated").cast(TimestampType()))

# Write to cleansed layer in Parquet format
df_clean.coalesce(1).write.mode("overwrite").parquet("s3a://retail/cleansed/shelf_events/")

print("✅ Shelf sensor raw data cleansed and written to Parquet.")