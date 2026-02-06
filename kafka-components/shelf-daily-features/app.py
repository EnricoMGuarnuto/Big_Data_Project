import os
import time
from datetime import datetime, timedelta, date

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from simulated_time.redis_helpers import get_simulated_date, get_simulated_timestamp


SUPPLIER_INBOUND_DOW = {1, 4}  # Tue, Fri (0=Mon)
DEFAULT_WH_REORDER_POINT_QTY = int(os.getenv("DEFAULT_WH_REORDER_POINT_QTY", "10"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "1.0"))
DELTA_ENABLED = os.getenv("DELTA_ENABLED", "1") == "1"
DELTA_FEATURES_STORE_PATH = os.getenv("DELTA_FEATURES_STORE_PATH", "/delta/curated/features_store")
DELTA_WRITE_MAX_RETRIES = int(os.getenv("DELTA_WRITE_MAX_RETRIES", "5"))
DELTA_WRITE_RETRY_BASE_S = float(os.getenv("DELTA_WRITE_RETRY_BASE_S", "0.8"))


def log_info(msg: str) -> None:
    if LOG_LEVEL in ("INFO", "DEBUG"):
        print(msg)


def env(name, default=None, required=False):
    v = os.getenv(name, default)
    if required and (v is None or v == ""):
        raise RuntimeError(f"Missing env var {name}")
    return v


def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def pg_conn():
    return psycopg2.connect(
        host=env("PGHOST", required=True),
        port=env("PGPORT", "5432"),
        dbname=env("PGDATABASE", required=True),
        user=env("PGUSER", required=True),
        password=env("PGPASSWORD", required=True),
    )


def _normalize_shelf_id(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if "shelf_id" in df.columns:
        df = df.copy()
        df["shelf_id"] = df["shelf_id"].astype(str).str.strip()
    return df


def _read_df(conn, sql: str, params=None) -> pd.DataFrame:
    return pd.read_sql_query(sql, conn, params=params)


def _compute_expiry_features(df_batches: pd.DataFrame, run_date: date, d_plus_6: date) -> pd.DataFrame:
    if df_batches.empty:
        return pd.DataFrame(columns=[
            "shelf_id", "min_expiration_days", "avg_expiration_days", "qty_expiring_next_7d"
        ])

    df = df_batches.copy()
    df["expiry_date"] = pd.to_datetime(df["expiry_date"]).dt.date
    df["exp_days"] = (df["expiry_date"] - run_date).apply(lambda x: x.days)

    in_next_7d = (df["expiry_date"] >= run_date) & (df["expiry_date"] <= d_plus_6)
    df["qty_expiring_next_7d"] = df["qty"].where(in_next_7d, 0)

    grouped = df.groupby("shelf_id", as_index=False).agg(
        min_expiration_days=("exp_days", "min"),
        avg_expiration_days=("exp_days", "mean"),
        qty_expiring_next_7d=("qty_expiring_next_7d", "sum"),
    )
    grouped["qty_expiring_next_7d"] = grouped["qty_expiring_next_7d"].fillna(0).astype(int)
    return grouped


def _compute_rollups(df_hist: pd.DataFrame, d_1: date, d_7: date, d_14: date) -> pd.DataFrame:
    if df_hist.empty:
        return pd.DataFrame(columns=[
            "shelf_id",
            "sales_last_1d",
            "sales_last_7d",
            "sales_last_14d",
            "alerts_last_30d_shelf",
            "alerts_last_30d_wh",
        ])

    df = df_hist.copy()
    df["feature_date"] = pd.to_datetime(df["feature_date"]).dt.date

    df["sales_last_1d"] = df["sales_qty_hist"].where(df["feature_date"] == d_1, 0)
    df["sales_last_7d"] = df["sales_qty_hist"].where(df["feature_date"] >= d_7, 0)
    df["sales_last_14d"] = df["sales_qty_hist"].where(df["feature_date"] >= d_14, 0)
    df["alerts_last_30d_shelf"] = (df["is_shelf_alert_hist"] == True).astype(int)
    df["alerts_last_30d_wh"] = (df["is_warehouse_alert_hist"] == True).astype(int)

    out = df.groupby("shelf_id", as_index=False)[[
        "sales_last_1d",
        "sales_last_7d",
        "sales_last_14d",
        "alerts_last_30d_shelf",
        "alerts_last_30d_wh",
    ]].sum()

    for col in ["sales_last_1d", "sales_last_7d", "sales_last_14d", "alerts_last_30d_shelf", "alerts_last_30d_wh"]:
        out[col] = out[col].fillna(0).astype(int)

    return out


def _build_output_frame(
    run_date: date,
    store_latest: pd.DataFrame,
    wh_latest: pd.DataFrame,
    batch_size: pd.DataFrame,
    shelf_state: pd.DataFrame,
    wh_state: pd.DataFrame,
    wh_policies: pd.DataFrame,
    pending_supplier: pd.DataFrame,
    expiry: pd.DataFrame,
    sales_day: pd.DataFrame,
    disc_today: pd.DataFrame,
    disc_next7: pd.DataFrame,
    roll: pd.DataFrame,
) -> pd.DataFrame:
    base = store_latest.copy()

    def ensure_col(col: str, default):
        if col not in base.columns:
            base[col] = default
        base[col] = base[col].fillna(default)

    for df in [wh_latest, batch_size, shelf_state, wh_state, wh_policies, pending_supplier, expiry, sales_day, disc_today, disc_next7, roll]:
        if df is not None and not df.empty:
            base = base.merge(df, on="shelf_id", how="left")

    base["feature_date"] = run_date

    day_of_week = run_date.weekday()  # 0=Mon..6=Sun
    base["day_of_week"] = day_of_week

    base["is_weekend"] = base["day_of_week"].isin([5, 6])
    base["warehouse_inbound_day"] = base["day_of_week"].isin(list(SUPPLIER_INBOUND_DOW))
    base["refill_day"] = False

    ensure_col("warehouse_capacity", 0)
    ensure_col("current_stock_shelf", 0)
    ensure_col("current_stock_warehouse", 0)
    ensure_col("standard_batch_size", 24)
    ensure_col("sales_qty", 0)
    ensure_col("discount", 0.0)

    base["warehouse_capacity"] = base["warehouse_capacity"].astype(int)
    base["current_stock_shelf"] = base["current_stock_shelf"].astype(int)
    base["current_stock_warehouse"] = base["current_stock_warehouse"].astype(int)
    base["standard_batch_size"] = base["standard_batch_size"].astype(int)
    base["sales_qty"] = base["sales_qty"].astype(int)
    base["discount"] = base["discount"].astype(float)
    base["is_discounted"] = base["discount"] > 0
    ensure_col("is_discounted_next_7d", False)
    base["is_discounted_next_7d"] = base["is_discounted_next_7d"].astype(bool)

    base["people_count"] = pd.Series([None] * len(base), dtype="object")
    base["stockout_events"] = 0

    base["shelf_fill_ratio"] = base.apply(
        lambda r: float(r["current_stock_shelf"]) / float(r["shelf_capacity"])
        if pd.notna(r.get("shelf_capacity")) and r.get("shelf_capacity", 0) > 0
        else 0.0,
        axis=1,
    )
    base["warehouse_fill_ratio"] = base.apply(
        lambda r: float(r["current_stock_warehouse"]) / float(r["warehouse_capacity"])
        if pd.notna(r.get("warehouse_capacity")) and r.get("warehouse_capacity", 0) > 0
        else 0.0,
        axis=1,
    )

    base["shelf_threshold_qty"] = pd.Series([None] * len(base), dtype="object")
    ensure_col("alerts_last_30d_shelf", 0)
    base["alerts_last_30d_shelf"] = base["alerts_last_30d_shelf"].astype(int)
    base["is_shelf_alert"] = False

    ensure_col("wh_reorder_point_qty", DEFAULT_WH_REORDER_POINT_QTY)
    ensure_col("pending_supplier_qty", 0)
    ensure_col("alerts_last_30d_wh", 0)

    base["wh_reorder_point_qty"] = base["wh_reorder_point_qty"].astype(int)
    base["pending_supplier_qty"] = base["pending_supplier_qty"].astype(int)
    base["alerts_last_30d_wh"] = base["alerts_last_30d_wh"].astype(int)

    base["is_warehouse_alert"] = (base["warehouse_capacity"] > 0) & (
        base["current_stock_warehouse"] < base["wh_reorder_point_qty"]
    )

    base["moved_wh_to_shelf"] = 0
    base["expired_qty_shelf"] = 0
    base["expired_qty_wh"] = 0
    base["batches_to_order"] = 0
    base["label_next_day_stockout"] = pd.Series([None] * len(base), dtype="object")

    for col in ["sales_last_1d", "sales_last_7d", "sales_last_14d", "min_expiration_days", "avg_expiration_days", "qty_expiring_next_7d"]:
        if col not in base.columns:
            base[col] = pd.Series([None] * len(base), dtype="object")

    out = base[[
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
        "label_next_day_stockout",
    ]].copy()

    return out


def _write_staging(conn, df: pd.DataFrame) -> None:
    cols = [
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
        "label_next_day_stockout",
    ]

    with conn.cursor() as cur:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS analytics.shelf_daily_features_stg "
            "(LIKE analytics.shelf_daily_features INCLUDING ALL);"
        )
        cur.execute("TRUNCATE analytics.shelf_daily_features_stg;")

        if df.empty:
            return

        df = df.where(pd.notna(df), None)
        values = [tuple(row[col] for col in cols) for _, row in df.iterrows()]
        insert_sql = (
            "INSERT INTO analytics.shelf_daily_features_stg (" + ",".join(cols) + ") VALUES %s"
        )
        execute_values(cur, insert_sql, values)


def _append_features_to_delta(df: pd.DataFrame) -> None:
    if not DELTA_ENABLED:
        return
    if df is None or df.empty:
        return

    try:
        from deltalake import DeltaTable
        from deltalake.writer import write_deltalake
    except Exception as e:
        print(f"[shelf-daily-features] warning: deltalake not available, skip curated write ({e})")
        return

    out = df.copy()
    out["feature_date"] = pd.to_datetime(out["feature_date"], errors="coerce").dt.date
    if "shelf_id" in out.columns:
        out["shelf_id"] = out["shelf_id"].astype(str).str.strip()

    existing_cols = []
    if os.path.exists(DELTA_FEATURES_STORE_PATH):
        try:
            table = DeltaTable(DELTA_FEATURES_STORE_PATH)
            existing_cols = [f.name for f in table.schema().fields]
        except Exception as e:
            print(f"[shelf-daily-features] warning: cannot read delta schema, using input schema ({e})")

    if existing_cols:
        if "foot_traffic" in existing_cols and "foot_traffic" not in out.columns:
            if "people_count" in out.columns:
                out["foot_traffic"] = pd.to_numeric(out["people_count"], errors="coerce").astype("Int64")
            else:
                out["foot_traffic"] = pd.Series([None] * len(out), dtype="Int64")

        for col in existing_cols:
            if col not in out.columns:
                out[col] = None
        out = out[existing_cols]

    last = None
    for attempt in range(1, DELTA_WRITE_MAX_RETRIES + 1):
        try:
            write_deltalake(DELTA_FEATURES_STORE_PATH, out, mode="append")
            log_info(
                f"[shelf-daily-features] appended {len(out)} row(s) to {DELTA_FEATURES_STORE_PATH}"
            )
            return
        except Exception as e:
            last = e
            sleep_s = DELTA_WRITE_RETRY_BASE_S * attempt
            print(
                f"[shelf-daily-features] delta write failed ({attempt}/{DELTA_WRITE_MAX_RETRIES}), "
                f"retry in {sleep_s}s: {e}"
            )
            time.sleep(sleep_s)

    print(f"[shelf-daily-features] warning: final delta write failure: {last}")


def run_for_day(run_date_str: str, sim_ts_str: str):
    if not run_date_str:
        raise RuntimeError("Simulated date not available from Redis")

    D = parse_date(run_date_str)
    d_1 = D - timedelta(days=1)
    d_7 = D - timedelta(days=7)
    d_14 = D - timedelta(days=14)
    d_30 = D - timedelta(days=30)
    d_plus_6 = D + timedelta(days=6)

    conn = pg_conn()
    conn.autocommit = True
    try:
        store_latest = _read_df(
            conn,
            """
            SELECT DISTINCT ON (shelf_id)
                shelf_id,
                item_category,
                item_subcategory,
                maximum_stock::int AS shelf_capacity,
                item_price::double precision AS item_price
            FROM ref.store_inventory_snapshot
            ORDER BY shelf_id, snapshot_ts DESC
            """,
        )

        wh_latest = _read_df(
            conn,
            """
            SELECT DISTINCT ON (shelf_id)
                shelf_id,
                maximum_stock::int AS warehouse_capacity
            FROM ref.warehouse_inventory_snapshot
            ORDER BY shelf_id, snapshot_ts DESC
            """,
        )

        batch_size = _read_df(
            conn,
            """
            SELECT shelf_id, MAX(standard_batch_size)::int AS standard_batch_size
            FROM ref.store_batches_snapshot
            GROUP BY shelf_id
            """,
        )

        shelf_state = _read_df(
            conn,
            """
            SELECT shelf_id, current_stock::int AS current_stock_shelf
            FROM state.shelf_state
            """,
        )

        wh_state = _read_df(
            conn,
            """
            SELECT shelf_id, wh_current_stock::int AS current_stock_warehouse
            FROM state.wh_state
            """,
        )

        wh_policies = _read_df(
            conn,
            """
            SELECT shelf_id::text AS shelf_id, reorder_point_qty::int AS wh_reorder_point_qty
            FROM config.wh_policies
            WHERE active IS TRUE
            """,
        )

        pending_supplier = _read_df(
            conn,
            """
            SELECT shelf_id, SUM(suggested_qty)::int AS pending_supplier_qty
            FROM ops.wh_supplier_plan
            WHERE lower(trim(status)) IN ('pending', 'issued')
            GROUP BY shelf_id
            """,
        )

        shelf_batches = _read_df(
            conn,
            """
            SELECT shelf_id, expiry_date::date AS expiry_date, batch_quantity_store::int AS qty
            FROM state.shelf_batch_state
            WHERE batch_quantity_store > 0
            """,
        )

        expiry = _compute_expiry_features(shelf_batches, D, d_plus_6)

        sales_day = _read_df(
            conn,
            """
            SELECT i.shelf_id, SUM(i.quantity)::int AS sales_qty
            FROM ops.pos_transactions t
            JOIN ops.pos_transaction_items i
              ON t.transaction_id = i.transaction_id
            WHERE t.timestamp::date = %s
            GROUP BY i.shelf_id
            """,
            params=(run_date_str,),
        )

        disc_today = _read_df(
            conn,
            """
            SELECT shelf_id, discount::double precision AS discount
            FROM analytics.daily_discounts
            WHERE discount_date = %s
            """,
            params=(run_date_str,),
        )

        disc_next7 = _read_df(
            conn,
            """
            SELECT shelf_id, TRUE AS is_discounted_next_7d
            FROM analytics.daily_discounts
            WHERE discount_date BETWEEN %s AND %s
              AND discount > 0
            GROUP BY shelf_id
            """,
            params=(run_date_str, d_plus_6.isoformat()),
        )

        panel_hist = _read_df(
            conn,
            """
            SELECT
                shelf_id,
                feature_date::date AS feature_date,
                sales_qty::int AS sales_qty_hist,
                is_shelf_alert::boolean AS is_shelf_alert_hist,
                is_warehouse_alert::boolean AS is_warehouse_alert_hist
            FROM analytics.shelf_daily_features
            WHERE feature_date BETWEEN %s AND %s
            """,
            params=(d_30.isoformat(), d_1.isoformat()),
        )

        store_latest = _normalize_shelf_id(store_latest)
        wh_latest = _normalize_shelf_id(wh_latest)
        batch_size = _normalize_shelf_id(batch_size)
        shelf_state = _normalize_shelf_id(shelf_state)
        wh_state = _normalize_shelf_id(wh_state)
        wh_policies = _normalize_shelf_id(wh_policies)
        pending_supplier = _normalize_shelf_id(pending_supplier)
        expiry = _normalize_shelf_id(expiry)
        sales_day = _normalize_shelf_id(sales_day)
        disc_today = _normalize_shelf_id(disc_today)
        disc_next7 = _normalize_shelf_id(disc_next7)
        panel_hist = _normalize_shelf_id(panel_hist)

        roll = _compute_rollups(panel_hist, d_1, d_7, d_14)
        roll = _normalize_shelf_id(roll)

        out = _build_output_frame(
            D,
            store_latest,
            wh_latest,
            batch_size,
            shelf_state,
            wh_state,
            wh_policies,
            pending_supplier,
            expiry,
            sales_day,
            disc_today,
            disc_next7,
            roll,
        )

        _write_staging(conn, out)

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
        FROM analytics.shelf_daily_features_stg
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
        snapshot_ts = TIMESTAMP '{sim_ts_str}'
        ;
        """

        label_sql = f"""
        UPDATE analytics.shelf_daily_features y
        SET label_next_day_stockout = (t.stockout_events > 0)
        FROM analytics.shelf_daily_features t
        WHERE y.feature_date = DATE '{d_1.isoformat()}'
        AND t.feature_date = DATE '{run_date_str}'
        AND y.shelf_id = t.shelf_id
        ;
        """

        with conn.cursor() as cur:
            cur.execute(upsert_sql)
            cur.execute(label_sql)

        _append_features_to_delta(out)

    finally:
        conn.close()



def main():
    log_info("[shelf-daily-features] Started (simulated-time daily batch)")
    last_processed_day = None

    while True:
        sim_day = get_simulated_date()
        sim_ts = get_simulated_timestamp()

        if sim_day and sim_day != last_processed_day:
            last_processed_day = sim_day
            try:
                run_for_day(sim_day, sim_ts)
            except Exception as e:
                print(f"[shelf-daily-features] ERROR for {sim_day}: {e}")

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
