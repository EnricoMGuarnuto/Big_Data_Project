import os
from datetime import datetime, timedelta, date

import psycopg2
from pyspark.sql import SparkSession, functions as F, Window


SUPPLIER_INBOUND_DOW = {1, 4}  # Tue, Fri (0=Mon)


def env(name, default=None, required=False):
    v = os.getenv(name, default)
    if required and (v is None or v == ""):
        raise RuntimeError(f"Missing env var {name}")
    return v


def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def jdbc_url():
    return f"jdbc:postgresql://{env('PGHOST', required=True)}:{env('PGPORT', '5432')}/{env('PGDATABASE', required=True)}"


def pg_conn():
    return psycopg2.connect(
        host=env("PGHOST", required=True),
        port=env("PGPORT", "5432"),
        dbname=env("PGDATABASE", required=True),
        user=env("PGUSER", required=True),
        password=env("PGPASSWORD", required=True),
    )


def main():
    run_date_str = env("RUN_DATE", required=True)  # e.g. 2026-01-01
    D = parse_date(run_date_str)

    d_1 = (D - timedelta(days=1)).isoformat()
    d_7 = (D - timedelta(days=7)).isoformat()
    d_14 = (D - timedelta(days=14)).isoformat()
    d_30 = (D - timedelta(days=30)).isoformat()
    d_plus_6 = (D + timedelta(days=6)).isoformat()

    spark = (
        SparkSession.builder
        .appName(f"shelf-daily-features-{run_date_str}")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    url = jdbc_url()
    props = {
        "user": env("PGUSER", required=True),
        "password": env("PGPASSWORD", required=True),
        "driver": "org.postgresql.Driver",
    }

    # -----------------------------
    # 1) Static / reference: latest snapshot per shelf
    # -----------------------------
    store_snap = spark.read.jdbc(url, "ref.store_inventory_snapshot", properties=props)

    w_latest = Window.partitionBy("shelf_id").orderBy(F.col("snapshot_ts").desc())
    store_latest = (
        store_snap
        .withColumn("rn", F.row_number().over(w_latest))
        .where(F.col("rn") == 1)
        .select(
            "shelf_id",
            F.col("item_category"),
            F.col("item_subcategory"),
            F.col("maximum_stock").cast("int").alias("shelf_capacity"),
            F.col("item_price").cast("double").alias("item_price"),
        )
    )

    wh_snap = spark.read.jdbc(url, "ref.warehouse_inventory_snapshot", properties=props)
    wh_latest = (
        wh_snap
        .withColumn("rn", F.row_number().over(w_latest))
        .where(F.col("rn") == 1)
        .select(
            "shelf_id",
            F.col("maximum_stock").cast("int").alias("warehouse_capacity"),
        )
    )

    # standard_batch_size: prendo dal ref.store_batches_snapshot (più affidabile di batch_catalog se non lo popolate)
    store_batches_snap = spark.read.jdbc(url, "ref.store_batches_snapshot", properties=props) \
        .select("shelf_id", F.col("standard_batch_size").cast("int").alias("standard_batch_size"))

    batch_size = store_batches_snap.groupBy("shelf_id").agg(F.max("standard_batch_size").alias("standard_batch_size"))

    # -----------------------------
    # 2) Current state
    # -----------------------------
    shelf_state = spark.read.jdbc(url, "state.shelf_state", properties=props) \
        .select("shelf_id", F.col("current_stock").cast("int").alias("current_stock_shelf"))

    wh_state = spark.read.jdbc(url, "state.wh_state", properties=props) \
        .select("shelf_id", F.col("wh_current_stock").cast("int").alias("current_stock_warehouse"))

    # -----------------------------
    # 3) Expiry features (shelf batches)
    # -----------------------------
    shelf_batches = spark.read.jdbc(url, "state.shelf_batch_state", properties=props) \
        .select(
            "shelf_id",
            F.col("expiry_date").cast("date").alias("expiry_date"),
            F.col("batch_quantity_store").cast("int").alias("qty"),
        ) \
        .where(F.col("qty") > 0)

    expiry = (
        shelf_batches
        .withColumn("exp_days", F.datediff(F.col("expiry_date"), F.lit(run_date_str).cast("date")))
        .groupBy("shelf_id")
        .agg(
            F.min("exp_days").alias("min_expiration_days"),
            F.avg("exp_days").alias("avg_expiration_days"),
            F.sum(
                F.when(
                    (F.col("expiry_date") >= F.lit(run_date_str).cast("date")) &
                    (F.col("expiry_date") <= F.lit(d_plus_6).cast("date")),
                    F.col("qty")
                ).otherwise(F.lit(0))
            ).cast("int").alias("qty_expiring_next_7d")
        )
    )

    # -----------------------------
    # 4) Sales of day D
    # -----------------------------
    pos_tx = spark.read.jdbc(url, "ops.pos_transactions", properties=props) \
        .select("transaction_id", F.col("timestamp").cast("timestamp").alias("ts"))

    pos_items = spark.read.jdbc(url, "ops.pos_transaction_items", properties=props) \
        .select("transaction_id", "shelf_id", F.col("quantity").cast("int").alias("quantity"))

    sales_day = (
        pos_tx
        .withColumn("d", F.to_date("ts"))
        .where(F.col("d") == F.lit(run_date_str).cast("date"))
        .join(pos_items, on="transaction_id", how="inner")
        .groupBy("shelf_id")
        .agg(F.sum("quantity").cast("int").alias("sales_qty"))
    )

    # -----------------------------
    # 5) Discounts (today + next 7d)
    # -----------------------------
    disc = spark.read.jdbc(url, "analytics.daily_discounts", properties=props) \
        .select("shelf_id", F.col("discount_date").cast("date").alias("discount_date"), F.col("discount").cast("double").alias("discount"))

    disc_today = disc.where(F.col("discount_date") == F.lit(run_date_str).cast("date")) \
        .select("shelf_id", F.col("discount").alias("discount"))

    disc_next7 = (
        disc
        .where(
            (F.col("discount_date") >= F.lit(run_date_str).cast("date")) &
            (F.col("discount_date") <= F.lit(d_plus_6).cast("date")) &
            (F.col("discount") > 0)
        )
        .groupBy("shelf_id")
        .agg(F.lit(True).alias("is_discounted_next_7d"))
    )

    # -----------------------------
    # 6) Rolling from panel (D-30..D-1)
    # -----------------------------
    panel_hist = spark.read.jdbc(url, "analytics.shelf_daily_features", properties=props) \
        .select(
            "shelf_id",
            F.col("feature_date").cast("date").alias("feature_date"),
            F.col("sales_qty").cast("int").alias("sales_qty_hist"),
            F.col("is_shelf_alert").cast("boolean").alias("is_shelf_alert_hist"),
            F.col("is_warehouse_alert").cast("boolean").alias("is_warehouse_alert_hist"),
        ) \
        .where(
            (F.col("feature_date") >= F.lit(d_30).cast("date")) &
            (F.col("feature_date") <= F.lit(d_1).cast("date"))
        )

    roll = (
        panel_hist
        .groupBy("shelf_id")
        .agg(
            # last_1d
            F.sum(F.when(F.col("feature_date") == F.lit(d_1).cast("date"), F.col("sales_qty_hist")).otherwise(F.lit(0))).cast("int").alias("sales_last_1d"),
            # last_7d
            F.sum(F.when(F.col("feature_date") >= F.lit(d_7).cast("date"), F.col("sales_qty_hist")).otherwise(F.lit(0))).cast("int").alias("sales_last_7d"),
            # last_14d
            F.sum(F.when(F.col("feature_date") >= F.lit(d_14).cast("date"), F.col("sales_qty_hist")).otherwise(F.lit(0))).cast("int").alias("sales_last_14d"),
            # alerts last 30
            F.sum(F.when(F.col("is_shelf_alert_hist") == F.lit(True), F.lit(1)).otherwise(F.lit(0))).cast("int").alias("alerts_last_30d_shelf"),
            F.sum(F.when(F.col("is_warehouse_alert_hist") == F.lit(True), F.lit(1)).otherwise(F.lit(0))).cast("int").alias("alerts_last_30d_wh"),
        )
    )

    # -----------------------------
    # 7) Base frame: all shelf_ids
    # -----------------------------
    base = (
        store_latest
        .join(wh_latest, "shelf_id", "left")
        .join(batch_size, "shelf_id", "left")
        .join(shelf_state, "shelf_id", "left")
        .join(wh_state, "shelf_id", "left")
        .join(expiry, "shelf_id", "left")
        .join(sales_day, "shelf_id", "left")
        .join(disc_today, "shelf_id", "left")
        .join(disc_next7, "shelf_id", "left")
        .join(roll, "shelf_id", "left")
        .withColumn("feature_date", F.lit(run_date_str).cast("date"))
    )

    # day_of_week (0=Mon..6=Sun)
    base = base.withColumn("day_of_week", (F.dayofweek(F.col("feature_date")) + 5) % 7)

    out = (
        base
        .withColumn("is_weekend", F.col("day_of_week").isin([5, 6]))
        .withColumn("warehouse_inbound_day", F.col("day_of_week").isin(list(SUPPLIER_INBOUND_DOW)))
        # refill_day: se volete "sempre", mettete True. Qui lo lascio come False e lo riempite quando avete moved_wh_to_shelf.
        .withColumn("refill_day", F.lit(False))
        .withColumn("warehouse_capacity", F.coalesce(F.col("warehouse_capacity"), F.lit(0)).cast("int"))
        .withColumn("current_stock_shelf", F.coalesce(F.col("current_stock_shelf"), F.lit(0)).cast("int"))
        .withColumn("current_stock_warehouse", F.coalesce(F.col("current_stock_warehouse"), F.lit(0)).cast("int"))
        .withColumn("standard_batch_size", F.coalesce(F.col("standard_batch_size"), F.lit(24)).cast("int"))
        .withColumn("sales_qty", F.coalesce(F.col("sales_qty"), F.lit(0)).cast("int"))
        .withColumn("discount", F.coalesce(F.col("discount"), F.lit(0.0)))
        .withColumn("is_discounted", (F.col("discount") > 0))
        .withColumn("is_discounted_next_7d", F.coalesce(F.col("is_discounted_next_7d"), F.lit(False)))
        .withColumn("people_count", F.lit(None).cast("int"))  # se non avete footfall, resta NULL
        .withColumn("stockout_events", F.lit(0).cast("int"))  # lo calcoli davvero solo se hai domanda attesa; altrimenti 0
        .withColumn("shelf_fill_ratio",
                    F.when(F.col("shelf_capacity") > 0, (F.col("current_stock_shelf") / F.col("shelf_capacity")).cast("double"))
                     .otherwise(F.lit(0.0)))
        .withColumn("warehouse_fill_ratio",
                    F.when(F.col("warehouse_capacity") > 0, (F.col("current_stock_warehouse") / F.col("warehouse_capacity")).cast("double"))
                     .otherwise(F.lit(0.0)))
        # questi li popolano i vostri servizi/policy; qui li metto neutri
        .withColumn("shelf_threshold_qty", F.lit(None).cast("int"))
        .withColumn("alerts_last_30d_shelf", F.coalesce(F.col("alerts_last_30d_shelf"), F.lit(0)).cast("int"))
        .withColumn("is_shelf_alert", F.lit(False))
        .withColumn("wh_reorder_point_qty", F.lit(None).cast("int"))
        .withColumn("pending_supplier_qty", F.lit(0).cast("int"))
        .withColumn("alerts_last_30d_wh", F.coalesce(F.col("alerts_last_30d_wh"), F.lit(0)).cast("int"))
        .withColumn("is_warehouse_alert", F.lit(False))
        .withColumn("moved_wh_to_shelf", F.lit(0).cast("int"))
        .withColumn("expired_qty_shelf", F.lit(0).cast("int"))
        .withColumn("expired_qty_wh", F.lit(0).cast("int"))
        .withColumn("batches_to_order", F.lit(0).cast("int"))
        .withColumn("label_next_day_stockout", F.lit(None).cast("boolean"))
        .select(
            "shelf_id", "feature_date",
            "item_category", "item_subcategory",
            "day_of_week", "is_weekend",
            "warehouse_inbound_day", "refill_day",
            "item_price", "discount", "is_discounted", "is_discounted_next_7d",
            "people_count",
            "sales_qty", "sales_last_1d", "sales_last_7d", "sales_last_14d",
            "stockout_events",
            "shelf_capacity", "current_stock_shelf", "shelf_fill_ratio", "shelf_threshold_qty",
            "expired_qty_shelf", "alerts_last_30d_shelf", "is_shelf_alert",
            "warehouse_capacity", "current_stock_warehouse", "warehouse_fill_ratio", "wh_reorder_point_qty",
            "pending_supplier_qty", "expired_qty_wh", "alerts_last_30d_wh", "is_warehouse_alert",
            "moved_wh_to_shelf",
            "standard_batch_size", "min_expiration_days", "avg_expiration_days", "qty_expiring_next_7d",
            "batches_to_order",
            "label_next_day_stockout"
        )
    )

    # -----------------------------
    # 8) Write to staging then upsert
    # -----------------------------
    stg = "analytics.shelf_daily_features_stg"
    out.write.jdbc(url, stg, mode="overwrite", properties=props)

    upsert_sql = f"""
    INSERT INTO analytics.shelf_daily_features (
      shelf_id, feature_date,
      item_category, item_subcategory,
      day_of_week, is_weekend,
      warehouse_inbound_day, refill_day,
      item_price, discount, is_discounted, is_discounted_next_7d,
      people_count,
      sales_qty, sales_last_1d, sales_last_7d, sales_last_14d,
      stockout_events,
      shelf_capacity, current_stock_shelf, shelf_fill_ratio, shelf_threshold_qty,
      expired_qty_shelf, alerts_last_30d_shelf, is_shelf_alert,
      warehouse_capacity, current_stock_warehouse, warehouse_fill_ratio, wh_reorder_point_qty,
      pending_supplier_qty, expired_qty_wh, alerts_last_30d_wh, is_warehouse_alert,
      moved_wh_to_shelf,
      standard_batch_size, min_expiration_days, avg_expiration_days, qty_expiring_next_7d,
      batches_to_order,
      label_next_day_stockout
    )
    SELECT
      shelf_id, feature_date,
      item_category, item_subcategory,
      day_of_week, is_weekend,
      warehouse_inbound_day, refill_day,
      item_price, discount, is_discounted, is_discounted_next_7d,
      people_count,
      sales_qty, sales_last_1d, sales_last_7d, sales_last_14d,
      stockout_events,
      shelf_capacity, current_stock_shelf, shelf_fill_ratio, shelf_threshold_qty,
      expired_qty_shelf, alerts_last_30d_shelf, is_shelf_alert,
      warehouse_capacity, current_stock_warehouse, warehouse_fill_ratio, wh_reorder_point_qty,
      pending_supplier_qty, expired_qty_wh, alerts_last_30d_wh, is_warehouse_alert,
      moved_wh_to_shelf,
      standard_batch_size, min_expiration_days, avg_expiration_days, qty_expiring_next_7d,
      batches_to_order,
      label_next_day_stockout
    FROM {stg}
    ON CONFLICT (shelf_id, feature_date) DO UPDATE SET
      item_category = EXCLUDED.item_category,
      item_subcategory = EXCLUDED.item_subcategory,
      day_of_week = EXCLUDED.day_of_week,
      is_weekend = EXCLUDED.is_weekend,
      warehouse_inbound_day = EXCLUDED.warehouse_inbound_day,
      refill_day = EXCLUDED.refill_day,
      item_price = EXCLUDED.item_price,
      discount = EXCLUDED.discount,
      is_discounted = EXCLUDED.is_discounted,
      is_discounted_next_7d = EXCLUDED.is_discounted_next_7d,
      people_count = EXCLUDED.people_count,
      sales_qty = EXCLUDED.sales_qty,
      sales_last_1d = EXCLUDED.sales_last_1d,
      sales_last_7d = EXCLUDED.sales_last_7d,
      sales_last_14d = EXCLUDED.sales_last_14d,
      stockout_events = EXCLUDED.stockout_events,
      shelf_capacity = EXCLUDED.shelf_capacity,
      current_stock_shelf = EXCLUDED.current_stock_shelf,
      shelf_fill_ratio = EXCLUDED.shelf_fill_ratio,
      shelf_threshold_qty = EXCLUDED.shelf_threshold_qty,
      expired_qty_shelf = EXCLUDED.expired_qty_shelf,
      alerts_last_30d_shelf = EXCLUDED.alerts_last_30d_shelf,
      is_shelf_alert = EXCLUDED.is_shelf_alert,
      warehouse_capacity = EXCLUDED.warehouse_capacity,
      current_stock_warehouse = EXCLUDED.current_stock_warehouse,
      warehouse_fill_ratio = EXCLUDED.warehouse_fill_ratio,
      wh_reorder_point_qty = EXCLUDED.wh_reorder_point_qty,
      pending_supplier_qty = EXCLUDED.pending_supplier_qty,
      expired_qty_wh = EXCLUDED.expired_qty_wh,
      alerts_last_30d_wh = EXCLUDED.alerts_last_30d_wh,
      is_warehouse_alert = EXCLUDED.is_warehouse_alert,
      moved_wh_to_shelf = EXCLUDED.moved_wh_to_shelf,
      standard_batch_size = EXCLUDED.standard_batch_size,
      min_expiration_days = EXCLUDED.min_expiration_days,
      avg_expiration_days = EXCLUDED.avg_expiration_days,
      qty_expiring_next_7d = EXCLUDED.qty_expiring_next_7d,
      batches_to_order = EXCLUDED.batches_to_order,
      label_next_day_stockout = EXCLUDED.label_next_day_stockout,
      snapshot_ts = NOW()
    ;
    """

    # update label for yesterday using today's stockout_events
    label_sql = f"""
    UPDATE analytics.shelf_daily_features y
    SET label_next_day_stockout = (t.stockout_events > 0)
    FROM analytics.shelf_daily_features t
    WHERE y.feature_date = DATE '{d_1}'
      AND t.feature_date = DATE '{run_date_str}'
      AND y.shelf_id = t.shelf_id
    ;
    """

    conn = pg_conn()
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(upsert_sql)
        cur.execute(label_sql)
    conn.close()

    spark.stop()


if __name__ == "__main__":
    main()
