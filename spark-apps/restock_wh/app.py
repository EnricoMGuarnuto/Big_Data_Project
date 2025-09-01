from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, expr
from datetime import datetime, timedelta
import os
import uuid
import random

# === CONFIG ===
JDBC_URL = "jdbc:postgresql://postgres:5432/retaildb"
DB_PROPERTIES = {
    "user": "retail",
    "password": "retailpass",
    "driver": "org.postgresql.Driver"
}
MINIO_PATH = "s3a://retail-lake/restock_plans/warehouse"

# Spark session
spark = SparkSession.builder \
    .appName("Warehouse Restock Cycle") \
    .getOrCreate()

# Utility: generate UUID
def gen_uuid():
    return str(uuid.uuid4())
spark.udf.register("gen_uuid", gen_uuid)

# Utility: generate batch_code in format "B-XXXX"
def gen_batch_code():
    return "B-" + str(random.randint(1000, 9999))
spark.udf.register("gen_batch_code", gen_batch_code)

# Current datetime
now = datetime.now()
hour = now.hour
today_str = now.strftime("%Y-%m-%d")
yesterday_str = (now - timedelta(days=1)).strftime("%Y-%m-%d")

# MODE env var
mode = os.getenv("MODE", "time")  # "time", "generate", "execute", "auto"

# ---------------------------------------------------
# Phase 1: Generate restock plan
# ---------------------------------------------------
def run_generate():
    print("Running Phase 1: Generate requested_restock plan")

    # Get warehouse ID
    locations = spark.read.jdbc(JDBC_URL, "locations", properties=DB_PROPERTIES)
    warehouse_id = locations.filter(col("location") == "warehouse").select("location_id").first()["location_id"]

    # Get active low_stock alerts for warehouse
    alerts = spark.read.jdbc(JDBC_URL, "alerts", properties=DB_PROPERTIES)
    low_stock = alerts.filter(
        (col("rule_key") == "low_stock") &
        (col("status") == "OPEN") &
        (col("location_id") == warehouse_id)
    ).select("item_id", "location_id")

    if low_stock.count() == 0:
        print("No low_stock alerts found for warehouse, nothing to generate.")
        return

    # Current product inventory
    pi = spark.read.jdbc(JDBC_URL, "product_inventory", properties=DB_PROPERTIES) \
        .select("item_id", "location_id", "current_stock")

    # Dummy target = 200
    to_restock = low_stock.join(pi, ["item_id", "location_id"], "left") \
        .withColumn("target", lit(200)) \
        .withColumn("missing_qty", expr("GREATEST(target - current_stock, 0)")) \
        .filter(col("missing_qty") > 0)

    # Prepare ledger rows
    ledger_df = to_restock.select(
        expr("gen_uuid()").alias("event_id"),
        current_timestamp().alias("event_ts"),
        col("item_id"),
        col("location_id"),
        col("missing_qty").cast("int").alias("delta_qty"),
        lit("requested_restock").alias("reason"),
        lit(None).cast("bigint").alias("batch_id"),
        expr("to_json(named_struct('target', target, 'current_stock', current_stock))").alias("meta")
    )

    # Write to inventory_ledger
    ledger_df.write.jdbc(JDBC_URL, "inventory_ledger", mode="append", properties=DB_PROPERTIES)

    # Backup in MinIO
    output_path = f"{MINIO_PATH}/dt={today_str}/plan.parquet"
    ledger_df.write.mode("overwrite").parquet(output_path)

    print(f"Requested restock plan written to DB and MinIO: {output_path}")


# ---------------------------------------------------
# Phase 2: Execute restock
# ---------------------------------------------------
def run_execute():
    print("Running Phase 2: Execute restock")

    # Get warehouse ID
    locations = spark.read.jdbc(JDBC_URL, "locations", properties=DB_PROPERTIES)
    warehouse_id = locations.filter(col("location") == "warehouse").select("location_id").first()["location_id"]

    # Read yesterday's requested_restock
    ledger = spark.read.jdbc(JDBC_URL, "inventory_ledger", properties=DB_PROPERTIES)
    requests = ledger.filter(
        (col("reason") == "requested_restock") &
        (expr("event_ts::date") == lit(yesterday_str)) &
        (col("location_id") == warehouse_id)
    )

    if requests.count() == 0:
        print("No requested_restock found for yesterday.")
        return

    # Apply receipt event for each item
    for row in requests.collect():
        batch_code = gen_batch_code()
        sql = f"""
            SELECT apply_receipt_event(
                '{row.event_id}-exec',
                now(),
                (SELECT shelf_id FROM items WHERE item_id={row.item_id} LIMIT 1),
                '{batch_code}',
                current_date,
                current_date + interval '30 day',
                {row.delta_qty},
                '{{"source":"auto_restock"}}'::jsonb
            )
        """
        spark.read.jdbc(JDBC_URL, f"({sql}) as subq", properties=DB_PROPERTIES)
        print(f"Executed restock for item {row.item_id}, qty {row.delta_qty}, batch {batch_code}")

    # Write executed_restock record
    executed_df = requests.select(
        expr("gen_uuid()").alias("event_id"),
        current_timestamp().alias("event_ts"),
        col("item_id"),
        col("location_id"),
        col("delta_qty"),
        lit("executed_restock").alias("reason"),
        lit(None).cast("bigint").alias("batch_id"),
        lit('{"note":"executed from requested_restock"}').cast("string").alias("meta")
    )
    executed_df.write.jdbc(JDBC_URL, "inventory_ledger", mode="append", properties=DB_PROPERTIES)

    # Backup in MinIO
    output_path = f"{MINIO_PATH}/dt={today_str}/executed.parquet"
    executed_df.write.mode("overwrite").parquet(output_path)

    print(f"Executed restock saved to DB and MinIO: {output_path}")


# ---------------------------------------------------
# Control flow based on MODE
# ---------------------------------------------------
if mode == "time":
    if hour == 23:
        run_generate()
    elif hour == 8:
        run_execute()
    else:
        print("Script idle: active only at 23:00 (generate) or 08:00 (execute).")

elif mode == "generate":
    run_generate()

elif mode == "execute":
    run_execute()

elif mode == "auto":
    # Try to decide automatically
    alerts = spark.read.jdbc(JDBC_URL, "alerts", properties=DB_PROPERTIES)
    if alerts.filter((col("rule_key") == "low_stock") & (col("status") == "OPEN")).count() > 0:
        run_generate()
    else:
        ledger = spark.read.jdbc(JDBC_URL, "inventory_ledger", properties=DB_PROPERTIES)
        if ledger.filter(col("reason") == "requested_restock").count() > 0:
            run_execute()
        else:
            print("Auto mode: no pending action detected.")

else:
    print(f"Unknown MODE={mode}")

spark.stop()
