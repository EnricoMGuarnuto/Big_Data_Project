import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    TimestampType, DateType, BooleanType, ArrayType
)

# ========= Config =========
DELTA_ROOT = os.getenv("DELTA_ROOT", "/delta")
WAREHOUSE  = os.getenv("SPARK_WAREHOUSE_DIR", "/tmp/spark-warehouse")

# ========= Spark (Delta-enabled) =========
spark = (
    SparkSession.builder
    .appName("delta-bootstrap")
    .config("spark.sql.warehouse.dir", WAREHOUSE)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

def path(*parts):
    return os.path.join(DELTA_ROOT, *parts)

# ========= Schemi =========
schema_shelf_events = StructType([
    StructField("event_type",    StringType(), True),
    StructField("customer_id",   StringType(), True),
    StructField("item_id",       StringType(), True),
    StructField("shelf_id",      StringType(), True),
    StructField("quantity",      IntegerType(), True),
    StructField("weight",        DoubleType(), True),
    StructField("delta_weight",  DoubleType(), True),
    StructField("timestamp",     TimestampType(), True),
])

items_schema = ArrayType(StructType([
    StructField("item_id",     StringType(),  True),
    StructField("batch_code",  StringType(),  True),
    StructField("quantity",    IntegerType(), True),
    StructField("unit_price",  DoubleType(),  True),
    StructField("discount",    DoubleType(),  True),
    StructField("total_price", DoubleType(),  True),
    StructField("expiry_date", StringType(),  True),
]))

schema_pos_transactions = StructType([
    StructField("event_type",     StringType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("customer_id",    StringType(), True),
    StructField("timestamp",      TimestampType(), True),
    StructField("items",          items_schema, True),  
])

schema_foot_traffic = StructType([
    StructField("event_type",  StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("entry_time",  TimestampType(), True),
    StructField("exit_time",   TimestampType(), True),
    StructField("trip_duration_minutes",        IntegerType(),      True),
    StructField("weekday",     StringType(), True),
    StructField("time_slot",   StringType(), True),
])

schema_wh_events = StructType([
    StructField("event_type",  StringType(), True),   # wh_in | wh_out
    StructField("event_id",    StringType(), True),
    StructField("plan_id",     StringType(), True),
    StructField("shelf_id",    StringType(), True),
    StructField("batch_code",  StringType(), True),
    StructField("qty",         IntegerType(), True),
    StructField("unit",        StringType(), True),
    StructField("timestamp",   TimestampType(), True),
    StructField("fifo",        BooleanType(), True),
    StructField("received_date", StringType(), True),
    StructField("expiry_date",   StringType(), True),
    StructField("batch_quantity_warehouse_after", IntegerType(), True),
    StructField("batch_quantity_store_after",     IntegerType(), True),
    StructField("shelf_warehouse_qty_after",      IntegerType(), True),
    StructField("reason",      StringType(), True),
])

schema_shelf_state = StructType([
    StructField("shelf_id",       StringType(), False),
    StructField("current_stock",  IntegerType(), False),
    StructField("shelf_weight",   DoubleType(), True),
    StructField("last_update_ts", TimestampType(), False),
])

schema_wh_state = StructType([
    StructField("shelf_id",         StringType(), False),
    StructField("wh_current_stock", IntegerType(), False),
    StructField("last_update_ts",   TimestampType(), False),
])

schema_shelf_batch_state = StructType([
    StructField("shelf_id",             StringType(), False),
    StructField("batch_code",           StringType(), False),
    StructField("received_date",        DateType(),   False),
    StructField("expiry_date",          DateType(),   False),
    StructField("batch_quantity_store", IntegerType(), False),
    StructField("last_update_ts",       TimestampType(), False),
])

schema_wh_batch_state = StructType([
    StructField("shelf_id",                   StringType(), False),
    StructField("batch_code",                 StringType(), False),
    StructField("received_date",              DateType(),   False),
    StructField("expiry_date",                DateType(),   False),
    StructField("batch_quantity_warehouse",   IntegerType(), False),
    StructField("batch_quantity_store",       IntegerType(), False),
    StructField("last_update_ts",             TimestampType(), False),
])

schema_alerts = StructType([
    StructField("alert_id",     StringType(), False),
    StructField("event_type",   StringType(), False),
    StructField("shelf_id",     StringType(), True),
    StructField("location",     StringType(), True),
    StructField("severity",     StringType(), True),
    StructField("current_stock", IntegerType(), True),
    StructField("max_stock",     IntegerType(), True),
    StructField("target_pct",    DoubleType(),  True),
    StructField("suggested_qty", IntegerType(), True),
    StructField("status",        StringType(),  False),
    StructField("created_at",    TimestampType(), False),
    StructField("updated_at",    TimestampType(), False),
])

schema_product_total_state = StructType([
    StructField("shelf_id",       StringType(), False),
    StructField("total_stock",    IntegerType(), False),
    StructField("last_update_ts", TimestampType(), False),
])

schema_features_store = StructType([
    StructField("feature_date",            DateType(),   False),
    StructField("shelf_id",                StringType(), False),
    StructField("foot_traffic",            IntegerType(), True),
    StructField("sales_qty",               IntegerType(), True),
    StructField("stockout_events",         IntegerType(), True),
    StructField("label_next_day_stockout", BooleanType(), True),
])

schema_predictions = StructType([
    StructField("prediction_ts", TimestampType(), False),
    StructField("shelf_id",      StringType(),    False),
    StructField("horizon_d",     IntegerType(),   False),
    StructField("predicted_qty", DoubleType(),    False),
    StructField("model_ver",     StringType(),    True),
])

schema_wh_supplier_orders = StructType([
    StructField("order_id",      StringType(), False),   # deterministico o uuid
    StructField("delivery_date", DateType(),   False),   # prossima consegna (lun/mer/ven)
    StructField("cutoff_ts",     TimestampType(), False),
    StructField("shelf_id",      StringType(), False),
    StructField("suggested_qty", IntegerType(), False),
    StructField("standard_batch_size", IntegerType(), True),
    StructField("status",        StringType(), False),   # issued / canceled / ...
    StructField("created_at",    TimestampType(), False),
])

schema_wh_inbound_receipts = StructType([
    StructField("receipt_id",    StringType(), False),
    StructField("delivery_date", DateType(),   False),
    StructField("received_ts",   TimestampType(), False),
    StructField("shelf_id",      StringType(), False),
    StructField("qty_received",  IntegerType(), False),
    StructField("batch_code",    StringType(), False),
    StructField("received_date", DateType(),   False),
    StructField("expiry_date",   DateType(),   True),
])

schema_wh_supplier_plan = StructType([
    StructField("supplier_plan_id", StringType(), False),
    StructField("shelf_id",         StringType(), False),
    StructField("suggested_qty",    IntegerType(), False),
    StructField("standard_batch_size", IntegerType(), True),
    StructField("status",           StringType(), False),   # pending/issued/completed/...
    StructField("created_at",       TimestampType(), False),
    StructField("updated_at",       TimestampType(), False),
])


def create_empty_delta(path_str: str, schema):
    (spark.createDataFrame([], schema)
          .write.format("delta")
          .mode("ignore")
          .save(path_str))

# RAW
create_empty_delta(path("raw", "shelf_events"),      schema_shelf_events)
create_empty_delta(path("raw", "pos_transactions"),  schema_pos_transactions)
create_empty_delta(path("raw", "foot_traffic"),      schema_foot_traffic)
create_empty_delta(path("raw", "wh_events"),         schema_wh_events)

# CLEANSED
create_empty_delta(path("cleansed", "shelf_state"),    schema_shelf_state)
create_empty_delta(path("cleansed", "wh_state"),       schema_wh_state)
create_empty_delta(path("cleansed", "shelf_batch_state"), schema_shelf_batch_state)
create_empty_delta(path("cleansed", "wh_batch_state"), schema_wh_batch_state)

# CURATED
create_empty_delta(path("curated", "product_total_state"), schema_product_total_state)
create_empty_delta(path("curated", "features_store"),      schema_features_store)
create_empty_delta(path("curated", "predictions"),         schema_predictions)

# OPS (supplier pipeline)
create_empty_delta(path("ops", "wh_supplier_plan"),        schema_wh_supplier_plan)
create_empty_delta(path("ops", "wh_supplier_orders"),      schema_wh_supplier_orders)
create_empty_delta(path("ops", "wh_inbound_receipts"),     schema_wh_inbound_receipts)
create_empty_delta(path("ops", "alerts"), schema_alerts)

os.makedirs(path("models", "warehouse_optimizer"), exist_ok=True)
os.makedirs(path("checkpoints"), exist_ok=True)

print(f"[delta-bootstrap] Initialized under: {DELTA_ROOT}")
spark.stop()
