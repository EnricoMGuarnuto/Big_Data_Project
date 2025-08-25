from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, TimestampType

spark = SparkSession.builder.appName("NearExpiryDiscounts").getOrCreate()

# Config
kafka_bootstrap = "kafka:9092"
topic = "near_expiry_discounts"
pg_url = "jdbc:postgresql://postgres:5432/retaildb"
pg_user = "retail"
pg_pass = "retailpass"

# Schema JSON evento Kafka
discount_schema = StructType([
    StructField("kind", StringType()),
    StructField("item_id", StringType()),
    StructField("batch_id", StringType()),
    StructField("location_id", StringType()),
    StructField("discount", DoubleType()),
    StructField("valid_from", TimestampType()),
    StructField("valid_to", TimestampType()),
    StructField("expiry_date", StringType()),
    StructField("reason", StringType())
])

schema = StructType([
    StructField("event_type", StringType()),
    StructField("source", StringType()),
    StructField("created_at", StringType()),
    StructField("discounts", ArrayType(discount_schema))
])

# Leggi da Kafka
df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka_bootstrap)
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .load())

parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Esplode array di discounts
exploded = parsed.withColumn("discount", explode(col("discounts")))

# Normalizza le colonne
final_df = exploded.select(
    col("discount.batch_id").cast("long").alias("batch_id"),
    col("discount.item_id").cast("long").alias("item_id"),
    col("discount.location_id").cast("int").alias("location_id"),
    col("discount.discount").alias("discount"),
    col("discount.valid_from").alias("valid_from"),
    col("discount.valid_to").alias("valid_to"),
    col("discount.expiry_date").cast("date").alias("expiry_date"),
    col("discount.reason").alias("reason"),
    col("source").alias("source"),
    col("created_at").cast("timestamp").alias("created_at")
)

# Scrivi su Postgres in modalità append
query = (final_df.writeStream
         .foreachBatch(lambda batch_df, batch_id: (
             batch_df.write
             .format("jdbc")
             .option("url", pg_url)
             .option("dbtable", "near_expiry_history")
             .option("user", pg_user)
             .option("password", pg_pass)
             .option("driver", "org.postgresql.Driver")
             .mode("append")
             .save()
         ))
         .outputMode("append")
         .start())

query.awaitTermination()
