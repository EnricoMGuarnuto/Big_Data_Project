from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType, TimestampType

# Define the schema explicitly
schema = StructType() \
    .add("Transaction_ID", StringType()) \
    .add("Timestamp", StringType()) \
    .add("Customer_ID", StringType()) \
    .add("Item_Identifier", StringType()) \
    .add("Item_Type", StringType()) \
    .add("Quantity", IntegerType()) \
    .add("Item_Price_EUR", DoubleType()) \
    .add("ingestion_time", TimestampType()) \
    .add("source", StringType())

# Start Spark session
spark = SparkSession.builder \
    .appName("Cleanse_POS_Raw") \
    .getOrCreate()

# Read raw JSON files from MinIO
df_raw = spark.read.schema(schema).json("s3a://retail/raw/pos_transactions/")

# Drop rows with nulls in critical fields
df_clean = df_raw.dropna(subset=["Transaction_ID", "Customer_ID", "Item_Identifier", "Quantity", "Item_Price_EUR"])

# Deduplicate by Transaction_ID (assuming unique ID per transaction)
df_clean = df_clean.dropDuplicates(["Transaction_ID"])

# Cast Timestamp properly if needed
df_clean = df_clean.withColumn("Timestamp", col("Timestamp").cast(TimestampType()))

# Write to cleansed layer in Parquet format
df_clean.coalesce(1).write.mode("overwrite").parquet("s3a://retail/cleansed/pos_transactions/")

print("✅ POS raw data cleansed and written to Parquet.")