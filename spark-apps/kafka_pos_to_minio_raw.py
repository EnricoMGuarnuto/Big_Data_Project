from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, current_date
import os
os.environ['HADOOP_USER_NAME'] = 'spark'

# Initialize Spark configuration
spark = SparkSession.builder.getOrCreate()


# Read from Kafka topic 'pos_transactions' and write to MinIO in Parquet format
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "pos_transactions") \
    .option("startingOffsets", "earliest") \
    .load()

df_raw = df_kafka.selectExpr("CAST(value AS STRING) as value") \
    .withColumn("ingestion_time", current_timestamp()) \
    .withColumn("source", lit("kafka:pos_transactions")) \
    .withColumn("dt", current_date())

query = df_raw.writeStream \
    .format("json") \
    .option("path", "s3a://retail/raw/pos_transactions/") \
    .option("checkpointLocation", "s3a://retail/raw/checkpoints/pos_transactions/") \
    .partitionBy("dt") \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .start()

query.awaitTermination()
