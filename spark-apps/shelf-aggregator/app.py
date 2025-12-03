import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession, functions as F, types as T, Window
from delta.tables import DeltaTable

# =========================
# Env / Config
# =========================
KAFKA_BROKER          = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_SHELF_EVENTS    = os.getenv("TOPIC_SHELF_EVENTS", "shelf_events")
TOPIC_SHELF_PROFILES  = os.getenv("TOPIC_SHELF_PROFILES", "shelf_profiles")  # compacted (key = shelf_id)
TOPIC_SHELF_STATE     = os.getenv("TOPIC_SHELF_STATE", "shelf_state")        # compacted (sink)

JDBC_PG_URL = os.getenv("JDBC_PG_URL")
JDBC_PG_USER = os.getenv("JDBC_PG_USER")
JDBC_PG_PASSWORD = os.getenv("JDBC_PG_PASSWORD")
BOOTSTRAP_FROM_PG = os.getenv("BOOTSTRAP_FROM_PG", "0") in ("1", "true", "True")

DELTA_ROOT            = os.getenv("DELTA_ROOT", "/delta")
RAW_PATH              = f"{DELTA_ROOT}/raw/shelf_events"
STATE_PATH            = f"{DELTA_ROOT}/cleansed/shelf_state"

CHECKPOINT_ROOT       = os.getenv("CHECKPOINT_ROOT", f"{DELTA_ROOT}/checkpoints/shelf_aggregator")
CKP_RAW               = f"{CHECKPOINT_ROOT}/raw"
CKP_AGG               = f"{CHECKPOINT_ROOT}/agg"

STARTING_OFFSETS      = os.getenv("STARTING_OFFSETS", "earliest")  # primo run: earliest; poi i checkpoint governano

# =========================
# Spark Session (Delta)
# =========================
spark = (
    SparkSession.builder.appName("Shelf_Aggregator")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# =========================
# Schemi
# =========================
schema_shelf_events = T.StructType([
    T.StructField("event_type",   T.StringType()),
    T.StructField("customer_id",  T.StringType()),
    T.StructField("item_id",      T.StringType()),  # sarà mappato a shelf_id
    T.StructField("weight",       T.DoubleType()),
    T.StructField("quantity",     T.IntegerType()),
    T.StructField("timestamp",    T.StringType()),
])

schema_profiles = T.StructType([
    T.StructField("shelf_id",     T.StringType()),
    T.StructField("item_weight",  T.DoubleType()),
])

# =========================
# 0) Snapshot statico profili (compacted)
# =========================
profiles_kafka_df = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC_SHELF_PROFILES)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

profiles_parsed = (
    profiles_kafka_df
    .select(
        F.col("key").cast("string").alias("shelf_id_key"),
        F.col("value").cast("string").alias("value_str"),
        "partition", "offset"
    )
    .withColumn("value_json", F.from_json(F.col("value_str"), schema_profiles))
    .select(
        F.coalesce(F.col("value_json.shelf_id"), F.col("shelf_id_key")).alias("shelf_id"),
        F.col("value_json.item_weight").alias("item_weight"),
        "partition", "offset"
    )
)

w_prof = Window.partitionBy("shelf_id").orderBy(F.col("offset").desc())
profiles_latest = (
    profiles_parsed
    .withColumn("rn", F.row_number().over(w_prof))
    .where("rn = 1")
    .drop("rn", "partition", "offset")
    .cache()
)

# =========================
# Bootstrap dallo snapshot Postgres (una tantum)
# =========================
def bootstrap_state_if_missing():
    if DeltaTable.isDeltaTable(spark, STATE_PATH):
        print(f"[bootstrap] Delta state già presente: {STATE_PATH} → salto bootstrap.")
        return

    if not BOOTSTRAP_FROM_PG:
        print("[bootstrap] BOOTSTRAP_FROM_PG=0 → salto bootstrap (nessuno stato iniziale creato).")
        return

    if not (JDBC_PG_URL and JDBC_PG_USER and JDBC_PG_PASSWORD):
        raise RuntimeError("Parametri JDBC mancanti per bootstrap: JDBC_PG_URL, JDBC_PG_USER, JDBC_PG_PASSWORD.")

    base_df = (
        spark.read.format("jdbc")
        .option("url", JDBC_PG_URL)
        .option("dbtable", """
            (
              SELECT shelf_id, aisle, item_weight, shelf_weight, item_category, item_subcategory,
                     maximum_stock, current_stock, item_price, snapshot_ts
              FROM ref.store_inventory_snapshot
            ) AS t
        """)
        .option("user", JDBC_PG_USER)
        .option("password", JDBC_PG_PASSWORD)
        .load()
    )

    w = Window.partitionBy("shelf_id").orderBy(F.col("snapshot_ts").desc())
    latest = (
        base_df.withColumn("rn", F.row_number().over(w))
               .where("rn = 1")
               .select(
                   F.col("shelf_id"),
                   F.col("current_stock").cast("int").alias("current_stock"),
                   # N.B. salviamo nel campo shelf_weight il peso unitario (item_weight)
                   F.col("item_weight").cast("double").alias("shelf_weight"),
                   F.current_timestamp().alias("last_update_ts")
               )
    )

    # Scrivi Delta stato iniziale
    latest.write.format("delta").mode("overwrite").save(STATE_PATH)

    # Pubblica su topic compacted `shelf_state` (key=shelf_id)
    to_publish = (
        latest.withColumn(
            "value_json",
            F.to_json(F.struct(
                "shelf_id","current_stock",
                F.col("shelf_weight").alias("item_weight"),
                "last_update_ts"
            ))
        )
        .select(F.col("shelf_id").cast("string").alias("key"),
                F.col("value_json").cast("string").alias("value"))
    )

    to_publish.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", TOPIC_SHELF_STATE) \
        .save()

    print(f"[bootstrap] Stato iniziale creato da Postgres e pubblicato su {TOPIC_SHELF_STATE}.")

bootstrap_state_if_missing()

# =========================
# 1) Stream: Kafka -> Delta (/raw/shelf_events)
# =========================
kafka_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC_SHELF_EVENTS)
    .option("startingOffsets", STARTING_OFFSETS)
    .option("failOnDataLoss", "false")
    .load()
)

events = (
    kafka_stream
    .select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("value_str"),
        "topic","partition","offset","timestamp"
    )
    .withColumn("value_json", F.from_json(F.col("value_str"), schema_shelf_events))
    .select(
        F.col("value_json.event_type").alias("event_type"),
        F.col("value_json.customer_id").alias("customer_id"),
        F.col("value_json.item_id").alias("shelf_id"),
        F.col("value_json.weight").alias("weight"),
        F.col("value_json.quantity").alias("quantity"),
        F.to_timestamp("value_json.timestamp").alias("event_ts"),
        "topic","partition","offset"
    )
)

# Salvataggio RAW append su Delta
raw_query = (
    events.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CKP_RAW)
    .option("path", RAW_PATH)
    .start()
)

# =========================
# 2) Aggregazione verso shelf_state (Delta + Kafka compacted)
# =========================
def upsert_and_publish(batch_df, batch_id: int):
    if batch_df.rdd.isEmpty():
        return

    # Delta corretto: pickup = -qty, putback = +qty
    deltas = (
        batch_df.groupBy("shelf_id")
        .agg(
            F.sum(
                F.when(F.col("event_type") == "pickup",  -F.col("quantity"))
                 .when(F.col("event_type") == "putback",  F.col("quantity"))
                 .otherwise(F.lit(0))
            ).alias("delta_qty"),
            F.max("event_ts").alias("last_event_ts")
        )
        .filter(F.col("delta_qty").isNotNull())
    )

    # arricchisci con profiles (peso unitario)
    enriched = deltas.join(F.broadcast(profiles_latest), on="shelf_id", how="left")

    # MERGE sullo stato
    if DeltaTable.isDeltaTable(spark, STATE_PATH):
        updates = (
            enriched
            .withColumn("last_update_ts", F.current_timestamp())
            .select("shelf_id","delta_qty","last_event_ts","item_weight","last_update_ts")
        )
        updates.createOrReplaceTempView("updates_view")

        spark.sql(f"""
            MERGE INTO delta.`{STATE_PATH}` AS t
            USING updates_view AS s
            ON t.shelf_id = s.shelf_id
            WHEN MATCHED THEN UPDATE SET
              t.current_stock    = COALESCE(t.current_stock, 0) + s.delta_qty,
              t.shelf_weight     = COALESCE(s.item_weight, t.shelf_weight),
              t.last_update_ts   = GREATEST(t.last_update_ts, s.last_event_ts, s.last_update_ts)
            WHEN NOT MATCHED THEN INSERT (shelf_id, current_stock, shelf_weight, last_update_ts)
              VALUES (s.shelf_id, s.delta_qty, s.item_weight, s.last_update_ts)
        """)
    else:
        initial_state = (
            enriched
            .withColumn("current_stock", F.col("delta_qty"))
            .withColumn("last_update_ts", F.current_timestamp())
            .select("shelf_id", "current_stock", F.col("item_weight").alias("shelf_weight"), "last_update_ts")
        )
        initial_state.write.format("delta").mode("overwrite").save(STATE_PATH)

    # Pubblica solo le chiavi toccate
    updated_keys = deltas.select("shelf_id").distinct()
    state_df = spark.read.format("delta").load(STATE_PATH)

    to_publish = (
        updated_keys.join(state_df, on="shelf_id", how="left")
        .withColumn(
            "value_json",
            F.to_json(
                F.struct(
                    F.col("shelf_id"),
                    F.col("current_stock"),
                    F.col("shelf_weight").alias("item_weight"),
                    F.col("last_update_ts")
                )
            )
        )
        .select(
            F.col("shelf_id").cast("string").alias("key"),
            F.col("value_json").cast("string").alias("value")
        )
    )

    to_publish.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", TOPIC_SHELF_STATE) \
        .save()

agg_query = (
    events.writeStream
    .foreachBatch(upsert_and_publish)
    .option("checkpointLocation", CKP_AGG)
    .start()
)

spark.streams.awaitAnyTermination()
