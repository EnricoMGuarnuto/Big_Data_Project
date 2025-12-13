import os, time
from pyspark.sql import SparkSession, functions as F, types as T
from delta.tables import DeltaTable
from pyspark.sql.window import Window


KAFKA_BROKER=os.getenv("KAFKA_BROKER","kafka:9092")
TOPIC_WH_EVENTS=os.getenv("TOPIC_WH_EVENTS","wh_events")
TOPIC_WH_BATCH=os.getenv("TOPIC_WH_BATCH","wh_batch_state")
TOPIC_SHELF_BATCH=os.getenv("TOPIC_SHELF_BATCH","shelf_batch_state")

DELTA_ROOT=os.getenv("DELTA_ROOT","/delta")
DL_WH_BATCH=f"{DELTA_ROOT}/cleansed/wh_batch_state"
DL_SHELF_BATCH=f"{DELTA_ROOT}/cleansed/shelf_batch_state"
CKP=f"{DELTA_ROOT}/_checkpoints/wh_batch_state_updater"
STARTING_OFFSETS=os.getenv("STARTING_OFFSETS","earliest")

JDBC_PG_URL=os.getenv("JDBC_PG_URL"); JDBC_PG_USER=os.getenv("JDBC_PG_USER"); JDBC_PG_PASSWORD=os.getenv("JDBC_PG_PASSWORD")
BOOTSTRAP_FROM_PG=os.getenv("BOOTSTRAP_FROM_PG","1") in ("1","true","True")

spark=(SparkSession.builder.appName("WH_Batch_State_Updater")
       .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
       .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
       .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

schema_evt = T.StructType([
    T.StructField("event_type", T.StringType()),  # wh_in | wh_out
    T.StructField("event_id", T.StringType()),
    T.StructField("plan_id", T.StringType()),
    T.StructField("shelf_id", T.StringType()),
    T.StructField("batch_code", T.StringType()),
    T.StructField("qty", T.IntegerType()),
    T.StructField("unit", T.StringType()),
    T.StructField("timestamp", T.TimestampType()),
    T.StructField("received_date", T.DateType()),
    T.StructField("expiry_date", T.DateType())
])

def bootstrap_from_pg():
    if not BOOTSTRAP_FROM_PG or not (JDBC_PG_URL and JDBC_PG_USER and JDBC_PG_PASSWORD):
        print("[bootstrap] skip"); return
    # WH
    wh = (spark.read.format("jdbc")
          .option("url", JDBC_PG_URL).option("user", JDBC_PG_USER).option("password", JDBC_PG_PASSWORD)
          .option("dbtable","(select shelf_id,batch_code,received_date,expiry_date,batch_quantity_warehouse,batch_quantity_store,snapshot_ts from ref.warehouse_batches_snapshot) t")
          .load())
    s_wh = (wh.withColumn("rn",F.row_number().over(Window.partitionBy("shelf_id","batch_code").orderBy(F.col("snapshot_ts").desc())))
              .where("rn=1")
              .select("shelf_id","batch_code","received_date","expiry_date",
                      F.coalesce(F.col("batch_quantity_warehouse"),F.lit(0)).cast("int").alias("batch_quantity_warehouse"),
                      F.coalesce(F.col("batch_quantity_store"),F.lit(0)).cast("int").alias("batch_quantity_store"),
                      F.current_timestamp().alias("last_update_ts")))
    s_wh.write.format("delta").mode("overwrite").save(DL_WH_BATCH)
    # STORE (batches presenti in negozio)
    st = (spark.read.format("jdbc")
          .option("url", JDBC_PG_URL).option("user", JDBC_PG_USER).option("password", JDBC_PG_PASSWORD)
          .option("dbtable","(select shelf_id,batch_code,received_date,expiry_date,batch_quantity_store,snapshot_ts from ref.store_batches_snapshot) t")
          .load())
    s_st = (st.withColumn("rn",F.row_number().over(Window.partitionBy("shelf_id","batch_code").orderBy(F.col("snapshot_ts").desc())))
              .where("rn=1")
              .select("shelf_id","batch_code","received_date","expiry_date",
                      F.coalesce(F.col("batch_quantity_store"),F.lit(0)).cast("int").alias("batch_quantity_store"),
                      F.current_timestamp().alias("last_update_ts")))
    s_st.write.format("delta").mode("overwrite").save(DL_SHELF_BATCH)

bootstrap_from_pg()

raw=(spark.readStream.format("kafka")
     .option("kafka.bootstrap.servers",KAFKA_BROKER)
     .option("subscribe",TOPIC_WH_EVENTS)
     .option("startingOffsets",STARTING_OFFSETS)
     .option("failOnDataLoss","false").load())

ev=(raw.select(F.col("value").cast("string").alias("v"))
     .select(F.from_json("v", schema_evt).alias("e")).select("e.*")
     .filter(F.col("shelf_id").isNotNull() & F.col("batch_code").isNotNull() & F.col("qty").isNotNull()))

def apply_events(batch_df, batch_id:int):
    if batch_df.rdd.isEmpty(): return

    # Aggiorna WH batches
    wh_delta = (batch_df.groupBy("shelf_id","batch_code","received_date","expiry_date")
                .agg(F.sum(F.when(F.col("event_type")=="wh_in", F.col("qty"))
                           .when(F.col("event_type")=="wh_out", -F.col("qty"))
                           .otherwise(F.lit(0))).alias("d_wh"),
                     F.max("timestamp").alias("ts")))
    if DeltaTable.isDeltaTable(spark, DL_WH_BATCH):
        t = DeltaTable.forPath(spark, DL_WH_BATCH)
        upd = (wh_delta.withColumn("last_update_ts", F.current_timestamp()))
        t.alias("t").merge(upd.alias("s"),
            "t.shelf_id=s.shelf_id AND t.batch_code=s.batch_code") \
         .whenMatchedUpdate(set={
             "received_date": F.coalesce(F.col("s.received_date"), F.col("t.received_date")),
             "expiry_date": F.coalesce(F.col("s.expiry_date"), F.col("t.expiry_date")),
             "batch_quantity_warehouse": F.expr("coalesce(t.batch_quantity_warehouse,0)+s.d_wh"),
             "last_update_ts": F.expr("greatest(t.last_update_ts, s.ts, s.last_update_ts)")
         }).whenNotMatchedInsert(values={
             "shelf_id":F.col("s.shelf_id"),
             "batch_code":F.col("s.batch_code"),
             "received_date":F.col("s.received_date"),
             "expiry_date":F.col("s.expiry_date"),
             "batch_quantity_warehouse":F.col("s.d_wh"),
             "batch_quantity_store":F.lit(0),
             "last_update_ts":F.col("s.last_update_ts")
         }).execute()
    else:
        (wh_delta.withColumn("last_update_ts",F.current_timestamp())
         .select("shelf_id","batch_code","received_date","expiry_date",
                 F.col("d_wh").alias("batch_quantity_warehouse"),
                 F.lit(0).alias("batch_quantity_store"),
                 "last_update_ts")
         .write.format("delta").mode("overwrite").save(DL_WH_BATCH))

    # Se wh_out, aumenta store batches
    to_store = (batch_df.filter(F.col("event_type")=="wh_out")
                .groupBy("shelf_id","batch_code","received_date","expiry_date")
                .agg(F.sum("qty").alias("d_store"), F.max("timestamp").alias("ts")))
    if not to_store.rdd.isEmpty():
        if DeltaTable.isDeltaTable(spark, DL_SHELF_BATCH):
            t2 = DeltaTable.forPath(spark, DL_SHELF_BATCH)
            upd2 = to_store.withColumn("last_update_ts", F.current_timestamp())
            t2.alias("t").merge(upd2.alias("s"),
                "t.shelf_id=s.shelf_id AND t.batch_code=s.batch_code") \
            .whenMatchedUpdate(set={
                "received_date": F.coalesce(F.col("s.received_date"), F.col("t.received_date")),
                "expiry_date": F.coalesce(F.col("s.expiry_date"), F.col("t.expiry_date")),
                "batch_quantity_store": F.expr("coalesce(t.batch_quantity_store,0)+s.d_store"),
                "last_update_ts": F.expr("greatest(t.last_update_ts, s.ts, s.last_update_ts)")
            }).whenNotMatchedInsert(values={
                "shelf_id":F.col("s.shelf_id"),
                "batch_code":F.col("s.batch_code"),
                "received_date":F.col("s.received_date"),
                "expiry_date":F.col("s.expiry_date"),
                "batch_quantity_store":F.col("s.d_store"),
                "last_update_ts":F.col("s.last_update_ts")
            }).execute()
        else:
            (to_store.withColumn("last_update_ts",F.current_timestamp())
             .select("shelf_id","batch_code","received_date","expiry_date",
                     F.col("d_store").alias("batch_quantity_store"),
                     "last_update_ts")
             .write.format("delta").mode("overwrite").save(DL_SHELF_BATCH))

    # Publish compacted topics for keys touched
    wh_keys = wh_delta.select("shelf_id","batch_code").distinct()
    wh_state = spark.read.format("delta").load(DL_WH_BATCH)
    (wh_keys.join(wh_state, ["shelf_id","batch_code"], "left")
      .withColumn("key", F.concat_ws("::","shelf_id","batch_code"))
      .withColumn("value", F.to_json(F.struct("shelf_id","batch_code","received_date","expiry_date",
                                              "batch_quantity_warehouse","batch_quantity_store","last_update_ts")))
      .select("key","value")
      .write.format("kafka").option("kafka.bootstrap.servers",KAFKA_BROKER)
      .option("topic", TOPIC_WH_BATCH).save())

    st_keys = to_store.select("shelf_id","batch_code").distinct() if not to_store.rdd.isEmpty() else None
    if st_keys is not None:
        st_state = spark.read.format("delta").load(DL_SHELF_BATCH)
        (st_keys.join(st_state, ["shelf_id","batch_code"], "left")
          .withColumn("key", F.concat_ws("::","shelf_id","batch_code"))
          .withColumn("value", F.to_json(F.struct("shelf_id","batch_code","received_date","expiry_date",
                                                  "batch_quantity_store","last_update_ts")))
          .select("key","value")
          .write.format("kafka").option("kafka.bootstrap.servers",KAFKA_BROKER)
          .option("topic", TOPIC_SHELF_BATCH).save())

q=(ev.writeStream.foreachBatch(apply_events).option("checkpointLocation", f"{CKP}/foreach").start())
q.awaitTermination()
