import os
import time
from datetime import datetime, timedelta, timezone, date
from simulated_time.clock import get_simulated_now

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# =========================
# Env / Config
# =========================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

TOPIC_WH_SUPPLIER_PLAN = os.getenv("TOPIC_WH_SUPPLIER_PLAN", "wh_supplier_plan")  # compacted (key shelf_id)
TOPIC_WH_EVENTS        = os.getenv("TOPIC_WH_EVENTS", "wh_events")                # append-only
TOPIC_ALERTS           = os.getenv("TOPIC_ALERTS", "alerts")                      # append-only

DELTA_ROOT = os.getenv("DELTA_ROOT", "/delta")

# Input mirror written by wh-alert-engine
DL_SUPPLIER_PLAN_PATH = os.getenv("DL_SUPPLIER_PLAN_PATH", f"{DELTA_ROOT}/ops/wh_supplier_plan")

# Ledgers (idempotency + audit)
DL_ORDERS_PATH   = os.getenv("DL_ORDERS_PATH",   f"{DELTA_ROOT}/ops/wh_supplier_orders")
DL_RECEIPTS_PATH = os.getenv("DL_RECEIPTS_PATH", f"{DELTA_ROOT}/ops/wh_supplier_receipts")

# Append-only WH events (optional raw mirror)
DL_WH_EVENTS_RAW = os.getenv("DL_WH_EVENTS_RAW", f"{DELTA_ROOT}/raw/wh_events")

CHECKPOINT_ROOT  = os.getenv("CHECKPOINT_ROOT", f"{DELTA_ROOT}/_checkpoints/wh_supplier_manager")
CKP_TICK         = os.getenv("CKP_TICK", f"{CHECKPOINT_ROOT}/tick")

TICK_MINUTES     = int(os.getenv("TICK_MINUTES", "1"))

# schedule
CUTOFF_HOUR      = int(os.getenv("CUTOFF_HOUR", "12"))  # 12:00
CUTOFF_MINUTE    = int(os.getenv("CUTOFF_MINUTE", "0"))
DELIVERY_HOUR    = int(os.getenv("DELIVERY_HOUR", "8")) # 08:00
DELIVERY_MINUTE  = int(os.getenv("DELIVERY_MINUTE", "0"))

# expiry policy for inbound batches
DEFAULT_EXPIRY_DAYS = int(os.getenv("DEFAULT_EXPIRY_DAYS", "365"))

# =========================
# Spark (Delta)
# =========================
spark = (
    SparkSession.builder.appName("WH_Supplier_Manager")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# =========================
# Schemas
# =========================
schema_plan = T.StructType([
    T.StructField("supplier_plan_id", T.StringType()),
    T.StructField("shelf_id", T.StringType()),
    T.StructField("suggested_qty", T.IntegerType()),
    T.StructField("standard_batch_size", T.IntegerType()),
    T.StructField("status", T.StringType()),     # pending/issued/completed/canceled
    T.StructField("created_at", T.TimestampType()),
    T.StructField("updated_at", T.TimestampType()),
])

schema_order = T.StructType([
    T.StructField("order_id", T.StringType()),
    T.StructField("delivery_date", T.DateType()),
    T.StructField("shelf_id", T.StringType()),
    T.StructField("total_qty", T.IntegerType()),
    T.StructField("status", T.StringType()),  # issued / delivered
    T.StructField("created_at", T.TimestampType()),
    T.StructField("updated_at", T.TimestampType()),
])

schema_receipt = T.StructType([
    T.StructField("receipt_id", T.StringType()),
    T.StructField("delivery_date", T.DateType()),
    T.StructField("shelf_id", T.StringType()),
    T.StructField("received_qty", T.IntegerType()),
    T.StructField("created_at", T.TimestampType()),
])

# =========================
# Delta init helpers
# =========================
def ensure_delta_table(path: str, schema: T.StructType):
    if not DeltaTable.isDeltaTable(spark, path):
        (spark.createDataFrame([], schema)
              .write.format("delta").mode("overwrite").save(path))

ensure_delta_table(DL_ORDERS_PATH, schema_order)
ensure_delta_table(DL_RECEIPTS_PATH, schema_receipt)

# =========================
# Time helpers (UTC)
# =========================
def now_utc() -> datetime:
    return get_simulated_now()

def is_cutoff_moment(ts: datetime) -> bool:
    # cutoff: Sunday(6), Tuesday(1), Thursday(3) at 12:00
    dow = ts.weekday()  # Mon=0..Sun=6
    return (dow in (6, 1, 3)) and ts.hour == CUTOFF_HOUR and ts.minute == CUTOFF_MINUTE

def is_delivery_moment(ts: datetime) -> bool:
    # delivery: Monday(0), Wednesday(2), Friday(4) at 08:00
    dow = ts.weekday()
    return (dow in (0, 2, 4)) and ts.hour == DELIVERY_HOUR and ts.minute == DELIVERY_MINUTE

def next_delivery_date(from_ts: datetime) -> date:
    """
    Next delivery among Mon/Wed/Fri, based on from_ts date (UTC).
    """
    d = from_ts.date()
    # candidate next 7 days
    for i in range(0, 8):
        dd = d + timedelta(days=i)
        if dd.weekday() in (0, 2, 4):  # Mon/Wed/Fri
            # if today is delivery day but already past delivery time, move forward
            if i == 0:
                delivery_time = datetime.combine(dd, datetime.min.time()).replace(
                    tzinfo=timezone.utc, hour=DELIVERY_HOUR, minute=DELIVERY_MINUTE
                )
                if from_ts >= delivery_time:
                    continue
            return dd
    return d + timedelta(days=7)

# =========================
# Read latest supplier plans (from Delta mirror)
# =========================
def read_plans_pending():
    plans = spark.read.format("delta").load(DL_SUPPLIER_PLAN_PATH)
    # keep only valid rows
    plans = plans.filter(
        F.col("supplier_plan_id").isNotNull() &
        F.col("shelf_id").isNotNull() &
        F.col("suggested_qty").isNotNull()
    )
    # we only manage pending
    return plans.filter(F.col("status") == F.lit("pending"))

def read_plans_by_ids(plan_ids_df):
    plans = spark.read.format("delta").load(DL_SUPPLIER_PLAN_PATH)
    return plan_ids_df.select("supplier_plan_id").distinct().join(plans, on="supplier_plan_id", how="left")

# =========================
# Update plans status (Kafka + Delta upsert)
# =========================
def publish_plan_updates(plans_df):
    """
    plans_df must contain:
      supplier_plan_id, shelf_id, suggested_qty, standard_batch_size, status, created_at, updated_at
    Kafka compacted key = shelf_id (your convention)
    """
    kafka_out = (
        plans_df
        .withColumn(
            "value",
            F.to_json(F.struct(
                "supplier_plan_id","shelf_id","suggested_qty","standard_batch_size",
                "status","created_at","updated_at"
            ))
        )
        .select(F.col("shelf_id").cast("string").alias("key"), F.col("value").cast("string").alias("value"))
    )

    kafka_out.write.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", TOPIC_WH_SUPPLIER_PLAN) \
        .save()

    # Also reflect in Delta mirror (upsert by supplier_plan_id)
    if DeltaTable.isDeltaTable(spark, DL_SUPPLIER_PLAN_PATH):
        tgt = DeltaTable.forPath(spark, DL_SUPPLIER_PLAN_PATH)
        tgt.alias("t").merge(
            plans_df.alias("s"),
            "t.supplier_plan_id = s.supplier_plan_id"
        ).whenMatchedUpdate(set={
            "status": F.col("s.status"),
            "updated_at": F.col("s.updated_at")
        }).execute()

# =========================
# Emit WH IN events (Kafka + Delta raw)
# =========================
def emit_wh_in_events(events_df):
    """
    events_df must have columns:
      event_type, event_id, plan_id, shelf_id, batch_code, qty, unit, timestamp, fifo, received_date, expiry_date, reason
      (extra columns are ok)
    """
    kafka_out = (
        events_df
        .withColumn("value", F.to_json(F.struct(
            "event_type","event_id","plan_id","shelf_id","batch_code","qty","unit","timestamp",
            "fifo","received_date","expiry_date","reason"
        )))
        .select(F.lit(None).cast("string").alias("key"), F.col("value").cast("string").alias("value"))
    )

    kafka_out.write.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", TOPIC_WH_EVENTS) \
        .save()

    # raw delta mirror (append)
    (events_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(DL_WH_EVENTS_RAW))

# =========================
# Orders ledger upsert (idempotent)
# =========================
def upsert_orders(orders_df):
    """
    PK: (delivery_date, shelf_id)
    """
    tgt = DeltaTable.forPath(spark, DL_ORDERS_PATH)
    tgt.alias("t").merge(
        orders_df.alias("s"),
        "t.delivery_date = s.delivery_date AND t.shelf_id = s.shelf_id"
    ).whenMatchedUpdate(set={
        "total_qty":  F.col("s.total_qty"),
        "status":     F.col("s.status"),
        "updated_at": F.col("s.updated_at"),
    }).whenNotMatchedInsert(values={
        "order_id":     F.col("s.order_id"),
        "delivery_date":F.col("s.delivery_date"),
        "shelf_id":     F.col("s.shelf_id"),
        "total_qty":    F.col("s.total_qty"),
        "status":       F.col("s.status"),
        "created_at":   F.col("s.created_at"),
        "updated_at":   F.col("s.updated_at"),
    }).execute()

# =========================
# Receipts ledger upsert (idempotent)
# =========================
def upsert_receipts(receipts_df):
    """
    PK: (delivery_date, shelf_id) — we enforce idempotency by merge
    """
    tgt = DeltaTable.forPath(spark, DL_RECEIPTS_PATH)
    tgt.alias("t").merge(
        receipts_df.alias("s"),
        "t.delivery_date = s.delivery_date AND t.shelf_id = s.shelf_id"
    ).whenMatchedUpdate(set={
        "received_qty": F.col("s.received_qty"),
        "created_at":   F.col("s.created_at"),
        "receipt_id":   F.col("s.receipt_id"),
    }).whenNotMatchedInsert(values={
        "receipt_id":   F.col("s.receipt_id"),
        "delivery_date":F.col("s.delivery_date"),
        "shelf_id":     F.col("s.shelf_id"),
        "received_qty": F.col("s.received_qty"),
        "created_at":   F.col("s.created_at"),
    }).execute()

# =========================
# Core logic per tick
# =========================
def do_cutoff(now_ts: datetime):
    """
    At cutoff (Sun/Tue/Thu 12:00):
      - group pending plans by shelf_id
      - write orders ledger for next delivery date (Mon/Wed/Fri)
      - mark involved plans as issued
    """
    delivery = next_delivery_date(now_ts)
    pending = read_plans_pending()
    if pending.rdd.isEmpty():
        return

    # aggregate orders per shelf
    orders = (
        pending.groupBy("shelf_id")
        .agg(F.sum(F.col("suggested_qty")).alias("total_qty"))
        .withColumn("delivery_date", F.lit(delivery).cast("date"))
        .withColumn("order_id", F.expr("uuid()"))
        .withColumn("status", F.lit("issued"))
        .withColumn("created_at", F.lit(get_simulated_now()).cast("timestamp"))
        .withColumn("updated_at", F.lit(get_simulated_now()).cast("timestamp"))
        .select("order_id","delivery_date","shelf_id","total_qty","status","created_at","updated_at")
    )

    # idempotent upsert
    upsert_orders(orders)

    # mark plans as issued (all pending plans we used)
    issued = (
        pending
        .withColumn("status", F.lit("issued"))
        .withColumn("updated_at", F.lit(get_simulated_now()).cast("timestamp"))
        .select("supplier_plan_id","shelf_id","suggested_qty","standard_batch_size","status","created_at","updated_at")
    )
    publish_plan_updates(issued)

    # Ack related warehouse alerts when supplier plan is issued
    alert_acks = (
        pending.select("shelf_id").distinct()
        .withColumn("event_type", F.lit("alert_status_change"))
        .withColumn("alert_event_type", F.lit("supplier_request"))
        .withColumn("location", F.lit("warehouse"))
        .withColumn("status", F.lit("ack"))
        .withColumn("timestamp", F.lit(get_simulated_now()).cast("timestamp"))
        .withColumn("value", F.to_json(F.struct(
            "event_type", "shelf_id", "location", "alert_event_type", "status", "timestamp"
        )))
        .select(F.lit(None).cast("string").alias("key"), "value")
    )
    if alert_acks.rdd.isEmpty() is False:
        alert_acks.write.format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("topic", TOPIC_ALERTS) \
            .save()

    print(f"[wh-supplier-manager] cutoff executed for delivery_date={delivery} (issued orders).")

def do_delivery(now_ts: datetime):
    """
    At delivery (Mon/Wed/Fri 08:00):
      - read orders ledger for today with status=issued
      - skip shelves already in receipts ledger (idempotent)
      - create wh_in events (can split into batches using standard_batch_size if available from plan table)
      - write receipts ledger
      - mark related plans as completed
      - mark orders as delivered
    """
    today = now_ts.date()

    orders = (
        spark.read.format("delta").load(DL_ORDERS_PATH)
        .filter((F.col("delivery_date") == F.lit(today)) & (F.col("status") == F.lit("issued")))
        .select("delivery_date","shelf_id","total_qty")
    )
    if orders.rdd.isEmpty():
        return

    # exclude already-received shelves (idempotency)
    receipts_existing = (
        spark.read.format("delta").load(DL_RECEIPTS_PATH)
        .filter(F.col("delivery_date") == F.lit(today))
        .select("delivery_date","shelf_id")
        .distinct()
    )

    due = orders.join(receipts_existing, on=["delivery_date","shelf_id"], how="left_anti").cache()
    if due.rdd.isEmpty():
        return

    # Emit wh_in events:
    # - if standard_batch_size exists in the latest plans table, we can split into multiple batches
    # - otherwise one batch per shelf
    plans_all = spark.read.format("delta").load(DL_SUPPLIER_PLAN_PATH).select(
        "supplier_plan_id","shelf_id","standard_batch_size","status"
    )

    # choose a batch size per shelf = max(standard_batch_size) among issued plans, fallback null
    batch_sizes = (
        plans_all.filter(F.col("status") == F.lit("issued"))
        .groupBy("shelf_id")
        .agg(F.max("standard_batch_size").alias("standard_batch_size"))
    )

    due2 = due.join(batch_sizes, on="shelf_id", how="left")

    # build a small “exploded” plan of batches
    # num_batches = ceil(total_qty / std_size) if std_size>0 else 1
    exploded = (
        due2
        .withColumn("std", F.col("standard_batch_size"))
        .withColumn("num_batches",
            F.when(F.col("std").isNotNull() & (F.col("std") > 0),
                   F.ceil(F.col("total_qty") / F.col("std")).cast("int")
            ).otherwise(F.lit(1))
        )
        .withColumn("batch_idx", F.expr("sequence(1, num_batches)"))
        .withColumn("batch_idx", F.explode("batch_idx"))
        .withColumn("qty_batch",
            F.when(F.col("std").isNotNull() & (F.col("std") > 0),
                   F.when(F.col("batch_idx") < F.col("num_batches"), F.col("std"))
                    .otherwise(F.col("total_qty") - (F.col("std") * (F.col("num_batches") - 1)))
            ).otherwise(F.col("total_qty"))
        )
        .drop("std")
    )

    received_date = today
    expiry_date = today + timedelta(days=DEFAULT_EXPIRY_DAYS)

    events = (
        exploded
        .withColumn("event_type", F.lit("wh_in"))
        .withColumn("event_id", F.expr("uuid()"))
        .withColumn("plan_id", F.lit(None).cast("string"))
        .withColumn("batch_code", F.concat_ws("-", F.lit("SUP"), F.date_format(F.lit(today), "yyyyMMdd"), F.col("shelf_id"), F.col("batch_idx").cast("string")))
        .withColumn("qty", F.col("qty_batch").cast("int"))
        .withColumn("unit", F.lit("ea"))
        .withColumn("timestamp", F.lit(get_simulated_now()).cast("timestamp"))
        .withColumn("fifo", F.lit(True))
        .withColumn("received_date", F.lit(received_date).cast("date"))
        .withColumn("expiry_date", F.lit(expiry_date).cast("date"))
        .withColumn("reason", F.lit("supplier_delivery"))
        .select(
            "event_type","event_id","plan_id","shelf_id","batch_code","qty","unit","timestamp",
            "fifo","received_date","expiry_date","reason"
        )
    )

    emit_wh_in_events(events)

    # receipts ledger (idempotent)
    receipts = (
        due.select("delivery_date","shelf_id","total_qty")
        .withColumnRenamed("total_qty", "received_qty")
        .withColumn("receipt_id", F.expr("uuid()"))
        .withColumn("created_at", F.lit(get_simulated_now()).cast("timestamp"))
        .select("receipt_id","delivery_date","shelf_id","received_qty","created_at")
    )
    upsert_receipts(receipts)

    # mark orders as delivered
    delivered_orders = (
        due.select("delivery_date","shelf_id","total_qty")
        .withColumn("order_id", F.expr("uuid()"))  # ignored on merge for matched rows
        .withColumn("status", F.lit("delivered"))
        .withColumn("created_at", F.lit(get_simulated_now()).cast("timestamp"))
        .withColumn("updated_at", F.lit(get_simulated_now()).cast("timestamp"))
        .select("order_id","delivery_date","shelf_id",F.col("total_qty"),"status","created_at","updated_at")
        .withColumnRenamed("total_qty", "total_qty")
    )
    upsert_orders(delivered_orders)

    # mark plans as completed (all issued plans for shelves delivered today)
    shelves_today = due.select("shelf_id").distinct()
    issued_plans_today = (
        spark.read.format("delta").load(DL_SUPPLIER_PLAN_PATH)
        .join(shelves_today, on="shelf_id", how="inner")
        .filter(F.col("status") == F.lit("issued"))
        .withColumn("status", F.lit("completed"))
        .withColumn("updated_at", F.lit(get_simulated_now()).cast("timestamp"))
        .select("supplier_plan_id","shelf_id","suggested_qty","standard_batch_size","status","created_at","updated_at")
    )
    if not issued_plans_today.rdd.isEmpty():
        publish_plan_updates(issued_plans_today)

    print(f"[wh-supplier-manager] delivery executed for {today} (wh_in emitted, orders delivered, plans completed).")

# =========================
# Tick stream (rate source)
# =========================
ticks = (
    spark.readStream.format("rate")
    .option("rowsPerSecond", 1)
    .load()
    .select(F.window("timestamp", f"{TICK_MINUTES} minutes").alias("w"))
    .select(F.col("w.start").alias("tick_ts"))
)

def on_tick(batch_df, batch_id: int):
    # run only once per microbatch
    ts = now_utc()

    # We run logic only exactly at minute boundary conditions you want.
    # The stream ticks every minute via window; idempotency handles duplicates anyway.
    if is_cutoff_moment(ts):
        do_cutoff(ts)

    if is_delivery_moment(ts):
        do_delivery(ts)

query = (
    ticks.writeStream
    .foreachBatch(on_tick)
    .option("checkpointLocation", CKP_TICK)
    .trigger(processingTime=f"{TICK_MINUTES} minutes")
    .start()
)

query.awaitTermination()
