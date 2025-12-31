# Warehouse Batch State Updater

## What it does
- Tracks warehouse batches and their in-store counterparts by processing `wh_events` (both `wh_in` and `wh_out`).
- Maintains two Delta tables: `DL_WH_BATCH` (warehouse lots) and `DL_SHELF_BATCH` (store lots seeded from warehouse shipments).
- Publishes compacted Kafka topics (`TOPIC_WH_BATCH`, `TOPIC_SHELF_BATCH`) keyed by `shelf_id::batch_code` so other jobs can read the latest batch quantities.

## Spark job steps
1. **Bootstrap (optional)** – If `BOOTSTRAP_FROM_PG=1`, load `ref.warehouse_batches_snapshot` and `ref.store_batches_snapshot`, deduplicate by `(shelf_id, batch_code)`, and overwrite the Delta mirrors.
2. **Streaming read** – Consume `TOPIC_WH_EVENTS`, parse JSON payloads, and keep only rows that contain shelf, batch, and quantity information.
3. **Warehouse batch merge** – For each micro-batch, aggregate deltas by shelf/batch and merge them into the `DL_WH_BATCH` Delta table.
4. **Store batch mirror** – For `wh_out` events, increase the corresponding `batch_quantity_store` in `DL_SHELF_BATCH`.
5. **Kafka publish** – Reload the touched keys from both tables, convert them to JSON, and upsert them to the compacted topics `TOPIC_WH_BATCH` and `TOPIC_SHELF_BATCH`.

## Key configuration
- Delta paths: `DL_WH_BATCH`, `DL_SHELF_BATCH`; checkpoint: `CKP`.
- Kafka topics: `TOPIC_WH_EVENTS`, `TOPIC_WH_BATCH`, `TOPIC_SHELF_BATCH`.
- Optional JDBC bootstrap requires `JDBC_PG_URL`, `JDBC_PG_USER`, `JDBC_PG_PASSWORD`.
- Simulated time: uses Redis `sim:now`/`sim:today` via `simulated_time`; start `sim-clock` for deterministic runs.
