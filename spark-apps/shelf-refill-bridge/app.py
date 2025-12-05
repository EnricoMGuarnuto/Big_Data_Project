import os
from pyspark.sql import SparkSession, functions as F, types as T, Window
from delta.tables import DeltaTable

# ======== Config ========
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_WH_EVENTS = os.getenv("TOPIC_WH_EVENTS", "wh_events")          # input (append-only)
TOPIC_SHELF_EVENTS = os.getenv("TOPIC_SHELF_EVENTS", "shelf_events") # output (append-only)
TOPIC_SHELF_PROFILES = os.getenv("TOPIC_SHELF_PROFILES", "shelf_profiles")  # compacted (key=shelf_id)

STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "earliest")

DELTA_ROOT = os.getenv("DELTA_ROOT", "/delta")
CKP = os.getenv("CKP", f"{DELTA_ROOT}/_checkpoints/shelf_refill_bridge")

# NEW: delay config & pending store
DELAY_MINUTES = int(os.getenv("DELAY_MINUTES", "3"))  # es. 2–5 minuti
PENDING_PATH  = os.getenv("PENDING_PATH", f"{DELTA_ROOT}/staging/shelf_refill_pending")

# ======== Spark ========
spark = (
    SparkSession.builder.appName("Shelf_Refill_Bridge")
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ======== Schemas ========
schema_wh_evt = T.StructType([
    T.StructField("event_type", T.StringType()),   # 'wh_out'
    T.StructField("event_id", T.StringType()),
    T.StructField("plan_id", T.StringType()),
    T.StructField("shelf_id", T.StringType()),
    T.StructField("batch_code", T.StringType()),
    T.StructField("qty", T.IntegerType()),
    T.StructField("unit", T.StringType()),
    T.StructField("timestamp", T.TimestampType()),
])

schema_profiles = T.StructType([
    T.StructField("shelf_id", T.StringType()),
    T.StructField("item_weight", T.DoubleType()),
])

# ======== Snapshot: shelf_profiles latest (for weight_change) ========
profiles_k = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC_SHELF_PROFILES)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

profiles_latest = (
    profiles_k
    .select(
        F.col("key").cast("string").alias("shelf_id_key"),
        F.col("value").cast("string").alias("value_str"),
        "partition","offset"
    )
    .withColumn("value_json", F.from_json("value_str", schema_profiles))
    .select(
        F.coalesce(F.col("value_json.shelf_id"), F.col("shelf_id_key")).alias("shelf_id"),
        F.col("value_json.item_weight").alias("item_weight"),
        "partition","offset"
    )
)
w_prof = Window.partitionBy("shelf_id").orderBy(F.col("offset").desc())
SHELF_PROFILES_LATEST = (
    profiles_latest.withColumn("rn", F.row_number().over(w_prof))
    .where("rn=1")
    .drop("rn","partition","offset")
    .cache()
)

# ======== Stream: wh_events ========
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC_WH_EVENTS)
    .option("startingOffsets", STARTING_OFFSETS)
    .option("failOnDataLoss", "false")
    .load()
)

wh_out = (
    raw.select(F.col("value").cast("string").alias("value_str"))
       .select(F.from_json("value_str", schema_wh_evt).alias("e"))
       .select("e.*")
       .filter(
           (F.col("event_type") == "wh_out") &
           F.col("shelf_id").isNotNull() &
           F.col("event_id").isNotNull() &
           (F.col("qty") > 0)
       )
)

# enrich with item_weight + available_at
incoming = (
    wh_out.join(SHELF_PROFILES_LATEST, on="shelf_id", how="left")
          .withColumn("available_at", F.expr(f"timestamp + interval {DELAY_MINUTES} minutes"))
          .select("event_id","shelf_id","batch_code","qty","item_weight","timestamp","available_at")
)

def foreach_batch(batch_df, batch_id: int):
    if batch_df.rdd.isEmpty():
        return

    now_ts = spark.range(1).select(F.current_timestamp().alias("now")).collect()[0]["now"]

    # 1) Pending attuali maturi da emettere
    if DeltaTable.isDeltaTable(spark, PENDING_PATH):
        pending_tbl = DeltaTable.forPath(spark, PENDING_PATH)
        pending_df = spark.read.format("delta").load(PENDING_PATH)
        due_from_pending = pending_df.filter(F.col("available_at") <= F.lit(now_ts))
        still_pending    = pending_df.filter(F.col("available_at") >  F.lit(now_ts))
    else:
        pending_tbl = None
        due_from_pending = spark.createDataFrame([], schema=T.StructType([
            T.StructField("event_id", T.StringType()),
            T.StructField("shelf_id", T.StringType()),
            T.StructField("batch_code", T.StringType()),
            T.StructField("qty", T.IntegerType()),
            T.StructField("item_weight", T.DoubleType()),
            T.StructField("timestamp", T.TimestampType()),
            T.StructField("available_at", T.TimestampType()),
        ]))
        still_pending = due_from_pending

    # 2) Nuovi wh_out di questo micro-batch → split tra due e not-yet
    due_now   = batch_df.filter(F.col("available_at") <= F.lit(now_ts))
    not_yet   = batch_df.filter(F.col("available_at") >  F.lit(now_ts))

    # 3) Unisci tutti i “due” (pending maturi + batch maturi)
    due_all = due_from_pending.unionByName(due_now)

    # 4) Emetti eventi shelf_events (putback + weight_change)
    if not due_all.rdd.isEmpty():
        putbacks = (
            due_all.select(
                F.lit("putback").alias("event_type"),
                F.lit("restock_bot").alias("customer_id"),
                F.col("shelf_id").alias("item_id"),
                F.col("shelf_id"),
                F.col("item_weight").alias("weight"),
                F.col("qty").alias("quantity"),
                F.col("timestamp").alias("timestamp")
            )
        )
        weight_changes = (
            due_all.select(
                F.lit("weight_change").alias("event_type"),
                F.lit("restock_bot").alias("customer_id"),
                F.col("shelf_id").alias("item_id"),
                F.col("shelf_id"),
                (F.col("item_weight") * F.col("qty")).alias("delta_weight"),
                F.col("timestamp").alias("timestamp")
            )
        )

        def to_kafka(df, cols):
            return df.select(*cols)\
                     .withColumn("value", F.to_json(F.struct(*cols)))\
                     .select(F.lit(None).cast("string").alias("key"), "value")

        out_kafka = to_kafka(
            putbacks, ["event_type","customer_id","item_id","shelf_id","weight","quantity","timestamp"]
        ).unionByName(
            to_kafka(weight_changes, ["event_type","customer_id","item_id","shelf_id","delta_weight","timestamp"])
        )

        out_kafka.write.format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("topic", TOPIC_SHELF_EVENTS) \
            .save()

    # 5) Aggiorna la pending Delta:
    #    - scrivi/merge i not_yet (nuovi o aggiornati)
    #    - mantieni gli still_pending (non maturi) + i nuovi not_yet
    remaining_pending = still_pending.unionByName(not_yet)

    if pending_tbl is None:
        remaining_pending.write.format("delta").mode("overwrite").save(PENDING_PATH)
    else:
        # upsert by event_id
        src = remaining_pending.alias("s")
        tgt = pending_tbl.alias("t")
        pending_tbl.merge(
            src,
            "t.event_id = s.event_id"
        ).whenMatchedUpdate(set={
            "shelf_id": "s.shelf_id",
            "batch_code": "s.batch_code",
            "qty": "s.qty",
            "item_weight": "s.item_weight",
            "timestamp": "s.timestamp",
            "available_at": "s.available_at"
        }).whenNotMatchedInsert(values={
            "event_id": "s.event_id",
            "shelf_id": "s.shelf_id",
            "batch_code": "s.batch_code",
            "qty": "s.qty",
            "item_weight": "s.item_weight",
            "timestamp": "s.timestamp",
            "available_at": "s.available_at"
        }).execute()

        # rimuovi quelli appena emessi (presenti in due_from_pending)
        if not due_from_pending.rdd.isEmpty():
            ids = [r["event_id"] for r in due_from_pending.select("event_id").distinct().collect()]
            if ids:
                pending_tbl.delete(F.col("event_id").isin(ids))

# run
q = (
    incoming.writeStream
    .foreachBatch(foreach_batch)
    .option("checkpointLocation", CKP)
    .start()
)
q.awaitTermination()
