import os, json, uuid
from datetime import datetime
from decimal import Decimal

from pyspark.sql import SparkSession, functions as F, types as T, Row

# =========================
# ENV
# =========================
def _env(name, default=None):
    v = os.getenv(name)
    return v if v not in (None, "", "null") else default

KAFKA_BROKER = _env("KAFKA_BOOTSTRAP_SERVERS", _env("KAFKA_BROKER", "kafka:9092"))
TOPIC_SHELF  = _env("TOPIC_SHELF", "shelf_events")
TOPIC_POS    = _env("TOPIC_POS", "pos_transactions")
TOPIC_FOOT   = _env("TOPIC_FOOT", "foot_traffic")

STARTING_OFFSETS = _env("STARTING_OFFSETS", "latest")
CHK_DIR = _env("CHECKPOINT_DIR", "/chk")

PG_HOST = _env("PG_HOST", "postgres")
PG_PORT = _env("PG_PORT", "5432")
PG_DB   = _env("PG_DB", "retaildb")
PG_USER = _env("PG_USER", "retail")
PG_PASS = _env("PG_PASS", "retailpass")

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
JDBC_PROPS = {
    "user": PG_USER,
    "password": PG_PASS,
    "driver": "org.postgresql.Driver",
}

def to_iso(ts):
    if ts is None:
        return datetime.utcnow().isoformat()
    return ts

# =========================
# Spark
# =========================
spark = (
    SparkSession.builder
        .appName("retail-streaming-supervisor")
        .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# -------------------------
# SCHEMI JSON
# -------------------------
shelf_schema = T.StructType([
    T.StructField("event_type",   T.StringType()),
    T.StructField("customer_id",  T.StringType()),
    T.StructField("item_id",      T.StringType()),
    T.StructField("shelf_id",     T.StringType()),
    T.StructField("delta_weight", T.DoubleType()),
    T.StructField("timestamp",    T.StringType()),
])

pos_item_schema = T.StructType([
    T.StructField("item_id",      T.StringType()),
    T.StructField("shelf_id",     T.StringType()),
    T.StructField("quantity",     T.IntegerType()),
    T.StructField("unit_price",   T.DoubleType()),
    T.StructField("discount",     T.DoubleType()),
    T.StructField("total_price",  T.DoubleType()),
])

pos_schema = T.StructType([
    T.StructField("event_type",     T.StringType()),
    T.StructField("transaction_id", T.StringType()),
    T.StructField("customer_id",    T.StringType()),
    T.StructField("timestamp",      T.StringType()),
    T.StructField("items",          T.ArrayType(pos_item_schema)),
])

foot_schema = T.StructType([
    T.StructField("event_type", T.StringType()),
    T.StructField("customer_id",T.StringType()),
    T.StructField("entry_time", T.StringType()),
    T.StructField("exit_time",  T.StringType()),
    T.StructField("trip_duration_minutes", T.IntegerType()),
    T.StructField("weekday", T.StringType()),
    T.StructField("time_slot", T.StringType()),
])

discount_schema = T.StructType([
    T.StructField("event_type", T.StringType()),
    T.StructField("week", T.StringType()),
    T.StructField("discounts", T.ArrayType(
        T.StructType([
            T.StructField("item_id", T.StringType()),
            T.StructField("discount", T.DoubleType())
        ])
    )),
    T.StructField("created_at", T.StringType())
])

# -------------------------
# SINK: JDBC (utility)
# -------------------------
def write_to_pg(df, table_name, mode="append"):
    df.write.format("jdbc") \
      .option("url", JDBC_URL) \
      .option("dbtable", table_name) \
      .option("user", PG_USER) \
      .option("password", PG_PASS) \
      .option("driver", "org.postgresql.Driver") \
      .mode(mode) \
      .save()

# -------------------------
# SOURCE: shelf_events
# -------------------------
raw_shelf = (
    spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC_SHELF)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
)

shelf = (
    raw_shelf
      .select(
          F.col("partition"), F.col("offset"),
          F.from_json(F.col("value").cast("string"), shelf_schema).alias("j")
      )
      .select(
          "partition", "offset",
          F.col("j.event_type").alias("event_type"),
          F.coalesce("j.shelf_id", "j.item_id").alias("shelf_id"),
          F.col("j.customer_id").alias("customer_id"),
          F.col("j.delta_weight").alias("delta_weight"),
          F.col("j.timestamp").alias("event_ts_str"),
          F.to_timestamp("j.timestamp").alias("event_ts")
      )
      .filter(F.col("event_type") == F.lit("weight_change"))
)

def process_shelf_batch(df, batch_id):
    if df.count() == 0:
        return
    
    df_raw = df.withColumn("payload", F.to_json(F.struct(df.columns))) \
               .withColumn("source", F.lit("shelf.sensors")) \
               .withColumn("event_id", F.format_string("spark-shelf-%s-%s", F.col("partition"), F.col("offset"))) \
               .withColumn("ingest_ts", F.current_timestamp()) \
               .select("event_id", "source", "event_ts", "payload", "ingest_ts")
    
    write_to_pg(df_raw, "stream_events")

    df_filtered = df.withColumn("meta", F.to_json(F.struct(F.col("customer_id").alias("customer_id")))) \
                    .select(
                        F.format_string("spark-shelf-%s-%s", F.col("partition"), F.col("offset")).alias("p_event_id"),
                        F.col("event_ts_str").alias("p_event_ts"),
                        F.col("shelf_id").alias("p_shelf_id"),
                        F.col("delta_weight").cast(T.DoubleType()).alias("p_weight_change"),
                        F.lit(False).alias("p_is_refill"),
                        F.col("meta").alias("p_meta"),
                        F.lit(0.25).alias("p_noise_tol")
                    )
    
    # Esegue la stored procedure per ogni riga
    df_filtered.write.format("jdbc").options(
        url=JDBC_URL,
        dbtable=f"(SELECT apply_shelf_weight_event(p_event_id, p_event_ts, p_shelf_id, p_weight_change, p_is_refill, p_meta::jsonb, p_noise_tol) FROM {df_filtered.name})",
        user=PG_USER,
        password=PG_PASS,
        driver="org.postgresql.Driver"
    ).mode("append").save()


shelf_q = (
    shelf.writeStream
         .foreachBatch(process_shelf_batch)
         .option("checkpointLocation", f"{CHK_DIR}/shelf")
         .trigger(processingTime="2 seconds")
         .start()
)

# -------------------------
# SOURCE: pos_transactions
# -------------------------
raw_pos = (
    spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC_POS)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
)

pos = (
    raw_pos
      .select(
          F.from_json(F.col("value").cast("string"), pos_schema).alias("j")
      )
      .select(
          F.col("j.event_type").alias("event_type"),
          F.col("j.transaction_id").alias("tx_id"),
          F.col("j.customer_id").alias("customer_id"),
          F.col("j.timestamp").alias("ts_str"),
          F.to_timestamp("j.timestamp").alias("ts"),
          F.col("j.items").alias("items")
      )
      .filter(F.col("event_type") == F.lit("pos_transaction"))
)

pos_lines = (
    pos
      .withColumn("item", F.explode_outer("items"))
      .select(
          "tx_id","customer_id","ts","ts_str",
          F.coalesce(F.col("item.shelf_id"), F.col("item.item_id")).alias("shelf_id"),
          F.col("item.quantity").alias("qty"),
          F.col("item.unit_price").alias("unit_price"),
          F.col("item.discount").alias("discount"),
          F.col("item.total_price").alias("total_price")
      )
)

def process_pos_batch(df, batch_id):
    if df.count() == 0:
        return
    
    df_agg = df.groupBy("tx_id", "customer_id", "ts_str") \
               .agg(F.sum("total_price").alias("total_gross"), F.collect_list(F.struct(df.columns)).alias("lines"))
    
    # 1. Scrivi i dati su `receipts`
    receipts_df = df_agg.select(
        F.col("tx_id").alias("transaction_id"),
        F.col("customer_id"),
        F.to_date(F.col("ts_str")).alias("business_date"),
        F.to_timestamp(F.col("ts_str")).alias("closed_at"),
        F.col("total_gross"),
        F.lit("CLOSED").alias("status")
    )
    write_to_pg(receipts_df, "receipts")

    # 2. Ottieni i receipt_id per le righe
    receipts_with_id = spark.read.format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", "receipts") \
        .option("user", PG_USER) \
        .option("password", PG_PASS) \
        .option("driver", "org.postgresql.Driver") \
        .load() \
        .filter(F.col("transaction_id").isin(list(df.select("tx_id").distinct().toPandas()["tx_id"]))) \
        .select("receipt_id", "transaction_id")
        
    df_with_receipts = df.join(receipts_with_id, df.tx_id == receipts_with_id.transaction_id, "inner")
    
    # 3. Scrivi le righe su `receipt_lines`
    receipt_lines_df = df_with_receipts.select(
        F.col("receipt_id"),
        F.col("shelf_id"),
        F.col("qty").alias("quantity"),
        F.col("unit_price"),
        F.col("discount"),
        F.col("total_price")
    )
    write_to_pg(receipt_lines_df, "receipt_lines")

    # 4. Chiama `apply_sale_event` per ogni riga
    sale_events_df = df_with_receipts.withColumn("event_id", F.concat(F.col("tx_id"), F.lit("-line-"), F.monotonically_increasing_id())) \
                                    .withColumn("meta", F.to_json(F.struct(F.col("receipt_id").alias("receipt_id")))) \
                                    .select(
                                        F.col("event_id").alias("p_event_id"),
                                        F.col("ts_str").alias("p_event_ts"),
                                        F.col("shelf_id").alias("p_shelf_id"),
                                        F.col("qty").alias("p_qty"),
                                        F.col("meta").alias("p_meta")
                                    )
    
    sale_events_df.write.format("jdbc").options(
        url=JDBC_URL,
        dbtable=f"(SELECT apply_sale_event(p_event_id, p_event_ts, p_shelf_id, p_qty, p_meta::jsonb) FROM {sale_events_df.name})",
        user=PG_USER,
        password=PG_PASS,
        driver="org.postgresql.Driver"
    ).mode("append").save()

pos_q = (
    pos_lines.writeStream
             .foreachBatch(process_pos_batch)
             .option("checkpointLocation", f"{CHK_DIR}/pos")
             .trigger(processingTime="2 seconds")
             .start()
)


def process_discount_batch(df, batch_id):
    if df.count() == 0:
        return

    df_raw = df.withColumn("payload", F.to_json(F.struct(df.columns))) \
               .withColumn("source", F.lit("discount.updater")) \
               .withColumn("event_id", F.format_string("spark-discount-%s-%s", F.col("partition"), F.col("offset"))) \
               .withColumn("event_ts", F.col("created_at")) \
               .withColumn("ingest_ts", F.current_timestamp()) \
               .select("event_id", "source", "event_ts", "payload", "ingest_ts")

    write_to_pg(df_raw, "stream_events")

    df_exploded = df.withColumn("item", F.explode("discounts")) \
                    .select(
                        F.col("item.item_id").alias("item_id"),
                        F.col("week"),
                        F.col("item.discount"),
                        F.col("created_at").alias("inserted_at")
                    )

    write_to_pg(df_exploded, "weekly_discounts")


# -------------------------
# discounts
# -------------------------

raw_discounts = (
    spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", "weekly_discounts")
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
)

discounts = (
    raw_discounts
      .select(
          F.col("partition"),
          F.col("offset"),
          F.from_json(F.col("value").cast("string"), discount_schema).alias("j")
      )
      .select("partition", "offset", "j.*")
      .filter(F.col("event_type") == "weekly_discount")
)

# -------------------------
# (Optional) foot_traffic only log
# -------------------------
if TOPIC_FOOT:
    raw_foot = (
        spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKER)
            .option("subscribe", TOPIC_FOOT)
            .option("startingOffsets", STARTING_OFFSETS)
            .option("failOnDataLoss", "false")
            .load()
    )
    
    foot = (
        raw_foot
          .select("partition", "offset", F.from_json(F.col("value").cast("string"), foot_schema).alias("j"))
          .select("partition", "offset", "j.*")
          .filter(F.col("event_type") == F.lit("foot_traffic"))
    )

    def process_foot_batch(df, batch_id):
        if df.count() == 0:
            return
            
        df_raw = df.withColumn("payload", F.to_json(F.struct(df.columns))) \
                   .withColumn("source", F.lit("foot.traffic")) \
                   .withColumn("event_id", F.format_string("spark-foot-%s-%s", F.col("partition"), F.col("offset"))) \
                   .withColumn("ingest_ts", F.current_timestamp()) \
                   .select("event_id", "source", F.col("entry_time").alias("event_ts"), "payload", "ingest_ts")
        
        write_to_pg(df_raw, "stream_events")

    foot_q = (
        foot.writeStream
            .foreachBatch(process_foot_batch)
            .option("checkpointLocation", f"{CHK_DIR}/foot")
            .trigger(processingTime="5 seconds")
            .start()
    )

# -------------------------
# Avvia la query per i weekly discounts
# -------------------------
discounts_q = (
    discounts.writeStream
        .foreachBatch(process_discount_batch)
        .option("checkpointLocation", f"{CHK_DIR}/discounts")
        .trigger(processingTime="10 seconds")
        .start()
)


# -------------------------
# await
# -------------------------
spark.streams.awaitAnyTermination()