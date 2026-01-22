# Dashboard (Streamlit)

Interactive Streamlit app that visualizes a supermarket layout, live alerts, and
state tables. It overlays alert and recent weight-change signals on a store map
and lets you drill into categories and shelves.

## What's inside
- `app.py`: Streamlit application.
- `store_layout.yaml`: Store map layout with normalized coordinates.
- `assets/supermarket_map.png`: Background image for the map.
- `requirements.txt`: Python dependencies.
- `Dockerfile`: Container image for the dashboard.

## Data sources
- Inventory CSV: `data/db_csv/store_inventory_final.csv`
  - Required columns: `shelf_id`, `item_category`, `item_subcategory`
- Postgres tables (read-only):
  - Alerts table (default `ops.alerts`): `alert_id`, `event_type`, `shelf_id`,
    `location`, `current_stock`, `max_stock`, `target_pct`,
    `suggested_qty`, `status`, `created_at`, `updated_at`
  - State tables: `shelf_state`, `wh_state`, `shelf_batch_state`,
    `wh_batch_state`
- Delta table (optional): `delta/raw/shelf_events`
  - Expected fields: `shelf_id`, `event_type` (or `type`), timestamp column
    (`event_ts`, `event_time`, `timestamp`, `ts`, `created_at`, `time`)

If the Delta path is missing or empty, the dashboard simply skips recent
weight-change highlights.

## Run locally
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r dashboard/requirements.txt
streamlit run dashboard/app.py
```

## Configuration (env vars)
Dashboard paths:
- `DASH_LAYOUT_YAML` / `LAYOUT_YAML` (default `dashboard/store_layout.yaml`)
- `INVENTORY_CSV_PATH` / `INVENTORY_CSV` (default `data/db_csv/store_inventory_final.csv`)
- `DELTA_SUPPLIER_PLAN_PATH` (default `delta/ops/wh_supplier_plan`)
- `DELTA_SUPPLIER_ORDERS_PATH` (default `delta/ops/wh_supplier_orders`)
- `DELTA_SHELF_STATE_PATH` (default `delta/cleansed/shelf_state`)
- `DELTA_WH_STATE_PATH` (default `delta/cleansed/wh_state`)
- `DELTA_SHELF_BATCH_STATE_PATH` (default `delta/cleansed/shelf_batch_state`)
- `DELTA_WH_BATCH_STATE_PATH` (default `delta/cleansed/wh_batch_state`)
- `DELTA_WH_EVENTS_RAW_PATH` / `DL_WH_EVENTS_RAW_PATH` (default `delta/raw/wh_events`)
- `DELTA_FOOT_TRAFFIC_PATH` / `DL_FOOT_TRAFFIC_PATH` (default `delta/raw/foot_traffic`)

Postgres:
- `POSTGRES_HOST` (default `postgres`)
- `POSTGRES_PORT` (default `5432`)
- `POSTGRES_DB` (default `smart_shelf`)
- `POSTGRES_USER` (default `bdt_user`)
- `POSTGRES_PASSWORD` (default `bdt_password`)
- `ALERTS_TABLE` (default `ops.alerts`)
- `SUPPLIER_PLAN_TABLE` (default `ops.wh_supplier_plan`)

Refresh / load tuning:
- `DASH_AUTOREFRESH_ENABLED` (default: enabled only if refresh interval > 0)
- `DASH_AUTOREFRESH_MS` / `DASH_REFRESH_MS` (default `0` = disabled; enable from sidebar if needed)
- `DASH_LIVE_FOOT_TRAFFIC` / `DASH_COMPUTE_LIVE_FOOT_TRAFFIC` (default `1` = compute live count from Delta)
- `DASH_ALERTS_LIMIT` (default `1000`)
- `DASH_ML_PREDICTIONS_LIMIT` (default `3000`)
- `DASH_TTL_ALERTS_S` (default `30`)
- `DASH_TTL_SUPPLIER_PLAN_S` (default `30`)
- `DASH_TTL_FOOT_TRAFFIC_S` (default `30`)
- `DASH_TTL_DELTA_STATE_S` (default `20`)
- `DASH_TTL_DELTA_OPS_S` (default `30`)
- `DASH_TTL_ML_S` (default `60`)

## Notes on layout
`store_layout.yaml` must use `coord_type: normalized` with bounds in `[0..1]`.
Each zone `category` should match `item_category` values (case-insensitive) to
color the map correctly.
