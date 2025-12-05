import os, time
from pyspark.sql import SparkSession, functions as F, types as T, Window
from delta.tables import DeltaTable

KAFKA_BROKER = os.getenv("KAFKA_BROKER","kafka:9092")
TOPIC_WH_EVENTS = os.getenv("TOPIC_WH_EVENTS","wh_events")
TOPIC_WH_STATE  = os.getenv("TOPIC_WH_STATE","wh_state")

DELTA_ROOT = os.getenv("DELTA_ROOT","/delta")
STATE_PATH = f"{DELTA_ROOT}/cleansed/wh_state"
CKP = f"{DELTA_ROOT}/_checkpoints/wh_aggregator"

STARTING_OFFSETS = os.getenv("STARTING_OFFSETS","earliest")

JDBC_PG_URL=os.getenv("JDBC_PG_URL"); JDBC_PG_USER=os.getenv("JDBC_PG_USER"); JDBC_PG_PASSWORD=os.getenv("JDBC_PG_PASSWORD")
BOOTSTRAP_FROM_PG = os.getenv("BOOTSTRAP_FROM_PG","1") in ("1","true","True")

spark = (SparkSession.builder.appName("WH_Aggregator")
         .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

schema_evt = T.StructType([
    T.StructField("event_type", T.StringType()),   # wh_in | wh_out
    T.StructField("event_id", T.StringType()),
    T.StructField("plan_id", T.StringType()),
    T.StructField("shelf_id", T.StringType()),
    T.StructField("batch_code", T.StringType()),
    T.StructField("qty", T.IntegerType()),
    T.StructField("unit", T.StringType()),
    T.StructField("timestamp", T.TimestampType())
])

def bootstrap_state_if_needed():
    if DeltaTable.isDeltaTable(spark, STATE_PATH):
        if spark.read.format("delta").load(STATE_PATH).limit(1).count()>0:
            print("[bootstrap] wh_state present, skip"); return
    if not BOOTSTRAP_FROM_PG or not (JDBC_PG_URL and JDBC_PG_USER and JDBC_PG_PASSWORD):
        print("[bootstrap] skip from PG"); return
    base = (spark.read.format("jdbc")
            .option("url", JDBC_PG_URL).option("user", JDBC_PG_USER).option("password", JDBC_PG_PASSWORD)
            .option("dbtable","(select shelf_id, current_stock as wh_current_stock, snapshot_ts from ref.warehouse_inventory_snapshot) t")
            .load())
    w = Window.partitionBy("shelf_id").orderBy(F.col("snapshot_ts").desc())
    latest = (base.withColumn("rn",F.row_number().over(w)).where("rn=1")
              .select("shelf_id",F.col("wh_current_stock").cast("int"),F.current_timestamp().alias("last_update_ts")))
    latest.write.format("delta").mode("overwrite").save(STATE_PATH)

bootstrap_state_if_needed()

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers",KAFKA_BROKER)
       .option("subscribe",TOPIC_WH_EVENTS)
       .option("startingOffsets",STARTING_OFFSETS)
       .option("failOnDataLoss","false").load())

ev = (raw.select(F.col("value").cast("string").alias("v"))
      .select(F.from_json("v", schema_evt).alias("e")).select("e.*")
      .filter(F.col("shelf_id").isNotNull() & F.col("qty").isNotNull()))

def upsert(batch_df, batch_id:int):
    if batch_df.rdd.isEmpty(): return
    # wh_in -> +qty ; wh_out -> -qty
    deltas = (batch_df.groupBy("shelf_id")
              .agg(F.sum(F.when(F.col("event_type")=="wh_in", F.col("qty"))
                          .when(F.col("event_type")=="wh_out", -F.col("qty"))
                          .otherwise(F.lit(0))).alias("delta"),
                   F.max("timestamp").alias("ts")))
    if DeltaTable.isDeltaTable(spark, STATE_PATH):
        t = DeltaTable.forPath(spark, STATE_PATH)
        upd = deltas.withColumn("last_update_ts", F.current_timestamp())
        t.alias("t").merge(upd.alias("s"), "t.shelf_id = s.shelf_id") \
         .whenMatchedUpdate(set={
             "wh_current_stock": F.expr("coalesce(t.wh_current_stock,0)+s.delta"),
             "last_update_ts": F.expr("greatest(t.last_update_ts, s.ts, s.last_update_ts)")
         }).whenNotMatchedInsert(values={
             "shelf_id":F.col("s.shelf_id"),
             "wh_current_stock":F.col("s.delta"),
             "last_update_ts":F.col("s.last_update_ts")
         }).execute()
    else:
        (deltas.withColumn("last_update_ts",F.current_timestamp())
         .select("shelf_id",F.col("delta").alias("wh_current_stock"),"last_update_ts")
         .write.format("delta").mode("overwrite").save(STATE_PATH))

    # publish compacted
    state_df = spark.read.format("delta").load(STATE_PATH)
    (state_df.select(
        F.col("shelf_id").cast("string").alias("key"),
        F.to_json(F.struct("shelf_id","wh_current_stock","last_update_ts")).alias("value"))
     .write.format("kafka").option("kafka.bootstrap.servers",KAFKA_BROKER)
     .option("topic", TOPIC_WH_STATE).save())

q = (ev.writeStream.foreachBatch(upsert).option("checkpointLocation", f"{CKP}/agg").start())
q.awaitTermination()
