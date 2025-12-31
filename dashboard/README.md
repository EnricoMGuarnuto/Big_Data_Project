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
    `location`, `severity`, `current_stock`, `max_stock`, `target_pct`,
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
- `DASH_LAYOUT_YAML` (default `dashboard/store_layout.yaml`)
- `INVENTORY_CSV_PATH` (default `data/db_csv/store_inventory_final.csv`)
- `DELTA_SHELF_EVENTS_PATH` (default `delta/raw/shelf_events`)

Postgres:
- `POSTGRES_HOST` (default `postgresql`)
- `POSTGRES_PORT` (default `5432`)
- `POSTGRES_DB` (default `postgres`)
- `POSTGRES_USER` (default `postgres`)
- `POSTGRES_PASSWORD` (default `postgres`)
- `ALERTS_TABLE` (default `ops.alerts`)

Refresh and event tuning:
- `DASH_AUTOREFRESH_MS` (default `1500`)
- `WEIGHT_EVENT_LOOKBACK_SEC` (default `8`)
- `WEIGHT_EVENT_TYPE` (default `weight_change`)

## Notes on layout
`store_layout.yaml` must use `coord_type: normalized` with bounds in `[0..1]`.
Each zone `category` should match `item_category` values (case-insensitive) to
color the map correctly.
