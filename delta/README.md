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
- `delta/models/warehouse_optimizer/`: placeholder directory (created by bootstrap).
- `delta/_checkpoints/`: Structured Streaming checkpoints (created by jobs).

## Dataflow between levels (what writes what)

This repo uses **Kafka topics as the primary streaming contract** (append-only topics for events, compacted topics for latest state). Delta is used as a **durable mirror** of raw events + derived tables, and as a place where some components read state in batch form.

### 0) Bootstrap

The `spark-init-delta` service initializes empty Delta tables (schemas + folders):
- creates `raw/`, `cleansed/`, `curated/`, `ops/` tables used by the pipeline.
- creates some folders like `models/warehouse_optimizer/`.

### 1) RAW (Kafka events → Delta append-only)

These tables are append-only mirrors of Kafka event streams:
- `raw/shelf_events` written by `spark-apps/shelf-aggregator/`
- `raw/pos_transactions` written by `spark-apps/batch-state-updater/`
- `raw/foot_traffic` written by `spark-apps/foot-traffic-raw-sink/`
- `raw/wh_events` written by `spark-apps/wh-aggregator/`
  - optionally also appended by `kafka-components/wh-supplier-manager/` if `MIRROR_WH_EVENTS_DELTA=1` (usually keep it off to avoid multiple writers)

### 2) CLEANSED (state builders → normalized state tables)

These tables are the “latest truth” state, built from the event streams:
- `cleansed/shelf_state` built by `spark-apps/shelf-aggregator/` (consumes Kafka `shelf_events`)
- `cleansed/wh_state` built by `spark-apps/wh-aggregator/` (consumes Kafka `wh_events`)
- `cleansed/shelf_batch_state` built by:
  - `spark-apps/batch-state-updater/` (sales decrement from Kafka `pos_transactions`)
  - `spark-apps/wh-batch-state-updater/` (store-lot increments on `wh_out` from Kafka `wh_events`)
- `cleansed/wh_batch_state` built by `spark-apps/wh-batch-state-updater/` (from Kafka `wh_events`)

### 3) OPS (CLEANSED → operational decisions + ledgers)

Operational outputs are derived from state and then fed back into the event loop:
- `ops/alerts` written by:
  - `spark-apps/shelf-alert-engine/` and `spark-apps/wh-alert-engine/` (also publish `alerts` to Kafka)
  - `spark-apps/alerts-sink/` mirrors Kafka `alerts` into Delta
- `ops/shelf_restock_plan` written by `spark-apps/shelf-alert-engine/` (also publishes compacted Kafka `shelf_restock_plan`)
  - status updates written by `spark-apps/shelf-restock-manager/`
- `ops/wh_supplier_plan` written by:
  - `spark-apps/wh-alert-engine/` (rule-based plan + Kafka compacted `wh_supplier_plan`)
  - `ml-model/inference-service/` (ML-based pending plans)
- `ops/wh_supplier_orders`, `ops/wh_supplier_receipts` written by `kafka-components/wh-supplier-manager/`

### 4) ANALYTICS (CLEANSED → analytics outputs)

- `analytics/daily_discounts` written by `kafka-components/daily-discount-manager/`
  - reads `cleansed/shelf_batch_state` to find near-expiry inventory.

### 5) CURATED (features/predictions)

- `curated/features_store` written by `kafka-components/shelf-daily-features/` (daily snapshot).
- `curated/predictions` written by `ml-model/inference-service/` (prediction logs).
- `curated/product_total_state` is currently bootstrapped but not populated by a dedicated job in this repo.

### 6) STAGING (temporary handoff)

- `staging/shelf_refill_pending` written/read by `spark-apps/shelf-refill-bridge/` to delay shelf refill effects.

## Bootstrap
The Docker service `spark-init-delta` runs
`spark-apps/deltalake/init_delta.py` to create empty Delta tables with the
expected schemas. See `spark-apps/deltalake/init_delta.py` for schema details.
