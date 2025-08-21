from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import random
from datetime import datetime

spark = SparkSession.builder.appName("GenerateShelfSensorBatch").getOrCreate()

# Load inventory from Parquet
store_inventory = spark.read.parquet("s3a://retail/cleansed/store_inventory/")

# Sample 300 items
sampled = store_inventory.sample(withReplacement=True, fraction=0.2).limit(300)

now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
sensor_ids = [f"SHELF-{random.randint(1, 50):03}" for _ in range(300)]
shelf_stocks = [random.randint(0, 20) for _ in range(300)]

# Convert to pandas to add simulated fields
pdf = sampled.toPandas()
pdf["Sensor_ID"] = sensor_ids
pdf["Shelf_Stock"] = shelf_stocks
pdf["Last_Updated"] = now

# Back to Spark
shelf_df = spark.createDataFrame(pdf)

# Write to MinIO as raw sensor data
shelf_df.write.mode("overwrite").json("s3a://retail/raw/shelf_events/")
print("✅ Shelf sensor batch data generated and written to MinIO.")