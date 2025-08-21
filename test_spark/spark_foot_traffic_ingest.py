from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, TimestampType, IntegerType

# 1. Spark Session
spark = SparkSession.builder \
    .appName("FootTrafficKafkaIngest") \
    .getOrCreate()

# 2. Schema JSON dell’evento foot_traffic
foot_schema = StructType() \
    .add("event_type", StringType()) \
    .add("customer_id", StringType()) \
    .add("entry_time", TimestampType()) \
    .add("exit_time", TimestampType()) \
    .add("trip_duration_minutes", IntegerType()) \
    .add("weekday", StringType()) \
    .add("time_slot", StringType())

# 3. Lettura dal topic Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "foot_traffic") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Parsing JSON nel campo "value"
df_parsed = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), foot_schema).alias("data")) \
    .select("data.*")

# 5. Output su console per debug
query = df_parsed.writeStream \
    .format("console") \
    .option("truncate", "false") \
    .outputMode("append") \
    .start()

query.awaitTermination()
