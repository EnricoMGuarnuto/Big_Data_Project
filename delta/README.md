# Delta Lake storage

This directory is the local Delta Lake used by the Spark jobs. It is mostly
empty in git and gets populated at runtime (or by the bootstrap job
`spark-init-delta`).

## Layout and tables
- `delta/raw/`: append-only event streams.
  - `shelf_events`, `pos_transactions`, `foot_traffic`, `wh_events`
- `delta/cleansed/`: normalized state tables.
  - `shelf_state`, `wh_state`, `shelf_batch_state`, `wh_batch_state`
- `delta/curated/`: feature/ML-ready datasets.
  - `product_total_state`, `features_store`, `predictions`
- `delta/ops/`: operational outputs.
  - `alerts`, `shelf_restock_plan`, `wh_supplier_plan`,
    `wh_supplier_orders`, `wh_supplier_receipts`
- `delta/analytics/`: analytics outputs (created by jobs).
  - `daily_discounts`
- `delta/staging/`: transient handoff tables (created by jobs).
  - `shelf_refill_pending`
- `delta/models/warehouse_optimizer/`: model artifacts (created by jobs).
- `delta/_checkpoints/`: Structured Streaming checkpoints (created by jobs).

## Bootstrap
The Docker service `spark-init-delta` runs
`spark-apps/deltalake/init_delta.py` to create empty Delta tables with the
expected schemas. See `spark-apps/deltalake/init_delta.py` for schema details.
