from pyspark.sql import SparkSession
from pyspark.sql.functions import col, abs, unix_timestamp, row_number
from pyspark.sql.window import Window

# Start Spark session
spark = SparkSession.builder.appName("Join_POS_Shelf_Traffic").getOrCreate()

# Load datasets from cleansed layer
pos_df = spark.read.parquet("s3a://retail/cleansed/pos_transactions/")
traffic_df = spark.read.parquet("s3a://retail/cleansed/foot_traffic/")
shelf_df = spark.read.parquet("s3a://retail/cleansed/shelf_events/")

# Rename columns to avoid collisions
traffic_df = traffic_df.withColumnRenamed("timestamp", "foot_traffic_timestamp")
shelf_df = shelf_df.withColumnRenamed("Last_Updated", "shelf_timestamp")

# Join POS with foot traffic on Customer_ID
pos_traffic_df = pos_df.join(
    traffic_df,
    on="Customer_ID",
    how="inner"
)

# Join with shelf using nearest timestamp (within same Item_Identifier)
# Step 1: Cross join on Item_Identifier and compute time difference
joined_df = pos_traffic_df.join(
    shelf_df,
    on="Item_Identifier"
).withColumn(
    "time_diff",
    abs(unix_timestamp("timestamp") - unix_timestamp("shelf_timestamp"))
)

# Step 2: Window to pick the shelf record with smallest time_diff
window_spec = Window.partitionBy("timestamp", "Item_Identifier").orderBy("time_diff")

ranked_df = joined_df.withColumn("rank", row_number().over(window_spec)).filter(col("rank") == 1)

# Final selection of columns
final_df = ranked_df.select(
    "timestamp",
    "Customer_ID",
    "Item_Identifier",
    "Quantity_Sold",
    "foot_traffic_timestamp",
    "Shelf_Stock",
    "Sensor_ID",
    "shelf_timestamp"
)

# Write curated output to Parquet
final_df.coalesce(1).write.mode("overwrite").parquet("s3a://retail/curated/transactions_enriched/")

print("✅ Curated dataset written to s3a://retail/curated/transactions_enriched/")