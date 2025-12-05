import os, time
from pyspark.sql import SparkSession, functions as F, types as T, Window
from delta.tables import DeltaTable

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_RESTOCK = os.getenv("TOPIC_RESTOCK", "shelf_restock_plan")   # compacted
TOPIC_WH_EVENTS = os.getenv("TOPIC_WH_EVENTS", "wh_events")        # append-only
TOPIC_ALERTS = os.getenv("TOPIC_ALERTS", "alerts")                 # append-only

DELTA_ROOT = os.getenv("DELTA_ROOT", "/delta")
DL_WH_BATCH_PATH = os.getenv("DL_WH_BATCH_PATH", f"{DELTA_ROOT}/cleansed/wh_batch_state")
DL_RESTOCK_PATH  = os.getenv("DL_RESTOCK_PATH", f"{DELTA_ROOT}/ops/shelf_restock_plan")
CKP_ROOT = os.getenv("CKP_ROOT", f"{DELTA_ROOT}/_checkpoints/shelf_restock_manager")

PLAN_DELAY_SEC = int(os.getenv("PLAN_DELAY_SEC", "60"))
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "earliest")

spark = (
    SparkSession.builder.appName("Shelf_Restock_Manager")
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema_plan = T.StructType([
    T.StructField("plan_id", T.StringType()),
    T.StructField("shelf_id", T.StringType()),
    T.StructField("suggested_qty", T.IntegerType()),
    T.StructField("status", T.StringType()),
    T.StructField("created_at", T.TimestampType()),
    T.StructField("updated_at", T.TimestampType()),
    # opzionale: se il tuo alert_engine la popola
    T.StructField("alert_id", T.StringType()),
])

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", KAFKA_BROKER)
       .option("subscribe", TOPIC_RESTOCK)
       .option("startingOffsets", STARTING_OFFSETS)
       .option("failOnDataLoss", "false")
       .load())

plans = (raw
    .select(F.col("value").cast("string").alias("v"))
    .select(F.from_json("v", schema_plan).alias("p")).select("p.*")
    .filter(F.col("shelf_id").isNotNull() & (F.col("suggested_qty") > 0))
)

def foreach_batch(batch_df, batch_id: int):
    if batch_df.rdd.isEmpty(): return

    # Prendi i piani candidati (vecchi almeno PLAN_DELAY_SEC e status='pending')
    candidates = (batch_df
        .withColumn("now_ts", F.current_timestamp())
        .filter( (F.col("status") == F.lit("pending")) &
                 (F.col("created_at") <= F.expr(f"now() - interval {PLAN_DELAY_SEC} seconds")) )
        .select("plan_id","shelf_id","suggested_qty","created_at","updated_at","alert_id")
        .cache()
    )
    if candidates.rdd.isEmpty(): return

    # Stato warehouse per FIFO (Delta mirror aggiornato da wh-batch-state-updater)
    wh = spark.read.format("delta").load(DL_WH_BATCH_PATH) \
            .select("shelf_id","batch_code","received_date","expiry_date","batch_quantity_warehouse") \
            .filter(F.col("batch_quantity_warehouse") > 0)

    # Join piano x shelf con lotti WH
    j = (candidates.alias("pl").join(wh.alias("w"), on="shelf_id", how="left"))

    if j.rdd.isEmpty(): return

    # Ordina FIFO: received_date asc, expiry_date asc
    w = Window.partitionBy("plan_id").orderBy(F.col("received_date").asc(), F.col("expiry_date").asc(), F.col("batch_code").asc())
    alloc = (j
        .withColumn("qty", F.col("w.batch_quantity_warehouse"))
        .withColumn("cum_before", F.sum("qty").over(w.rowsBetween(Window.unboundedPreceding, -1)))
        .fillna({"cum_before": 0})
        .withColumn("need", F.col("pl.suggested_qty"))
        .withColumn("remaining", F.col("need") - F.col("cum_before"))
        .withColumn("take", F.when(F.col("remaining") > 0, F.least(F.col("qty"), F.col("remaining"))).otherwise(F.lit(0)))
        .filter(F.col("take") > 0)
        .select(
            F.col("plan_id"), F.col("shelf_id"),
            F.col("batch_code"), F.col("received_date"), F.col("expiry_date"),
            F.col("take").cast("int").alias("alloc_qty"),
            F.col("alert_id")
        )
    )

    if alloc.rdd.isEmpty(): return

    # Emetti wh_events (wh_out) su Kafka
    wh_out = (alloc
        .withColumn("event_type", F.lit("wh_out"))
        .withColumn("event_id", F.expr("uuid()"))
        .withColumn("unit", F.lit("ea"))
        .withColumn("timestamp", F.current_timestamp())
        .withColumn("fifo", F.lit(True))
    )

    to_kafka = (wh_out
        .withColumn("value", F.to_json(F.struct(
            "event_type","event_id","plan_id","shelf_id","batch_code",
            F.col("alloc_qty").alias("qty"),
            "unit","timestamp","fifo",
            "received_date","expiry_date"
        )))
        .select(F.lit(None).cast("string").alias("key"), "value")
    )

    to_kafka.write.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", TOPIC_WH_EVENTS) \
        .save()

    # Aggiorna plan -> issued nella Delta (mirror)
    if DeltaTable.isDeltaTable(spark, DL_RESTOCK_PATH):
        tgt = DeltaTable.forPath(spark, DL_RESTOCK_PATH)
        issued = (candidates.select("plan_id")
                  .withColumn("status", F.lit("issued"))
                  .withColumn("updated_at", F.current_timestamp()))
        tgt.alias("t").merge(
            issued.alias("s"),
            "t.plan_id = s.plan_id"
        ).whenMatchedUpdate(set={
            "status": F.col("s.status"),
            "updated_at": F.col("s.updated_at")
        }).execute()

    # (Opzionale) cambio stato alert: ack/resolved
    alerts_updates = (alloc.groupBy("plan_id","alert_id")
                      .agg(F.sum("alloc_qty").alias("moved"))
                      .filter(F.col("alert_id").isNotNull()))
    if alerts_updates.rdd.isEmpty() is False:
        au = (alerts_updates
              .withColumn("event_type", F.lit("alert_status_change"))
              .withColumn("status", F.lit("ack"))
              .withColumn("timestamp", F.current_timestamp())
              .withColumn("value", F.to_json(F.struct("event_type","alert_id","status","timestamp")))
              .select(F.lit(None).cast("string").alias("key"), "value"))
        au.write.format("kafka") \
          .option("kafka.bootstrap.servers", KAFKA_BROKER) \
          .option("topic", TOPIC_ALERTS).save()

# stream piani
q = (plans.writeStream
     .foreachBatch(foreach_batch)
     .option("checkpointLocation", f"{CKP_ROOT}/foreach")
     .start())
q.awaitTermination()
