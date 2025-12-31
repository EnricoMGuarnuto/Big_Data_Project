# Warehouse Aggregator

## What it does
- Maintains the warehouse stock per `shelf_id` by consuming the `wh_events` stream (both `wh_in` and `wh_out`).
- Optionally bootstraps the Delta state from the Postgres snapshot `ref.warehouse_inventory_snapshot`.
- Publishes the compacted `wh_state` topic so the warehouse alert engine can react to low stock.

## Spark job steps
1. **Bootstrap (optional)** – When `BOOTSTRAP_FROM_PG=1`, read the latest stock per shelf via JDBC and write it to `STATE_PATH`.
2. **Streaming read** – Parse the `TOPIC_WH_EVENTS` Kafka topic into structured events.
3. **ForeachBatch merge** – Aggregate quantity deltas (wh_in = +qty, wh_out = –qty) for each shelf and merge them into the Delta state table.
4. **Publish state** – Reload the entire state (or just touched keys) and write it to Kafka as a compacted topic (`TOPIC_WH_STATE`).

## Key configuration
- `STATE_PATH`, `CKP`: Delta storage for wh_state and checkpointing.
- Kafka topics: `TOPIC_WH_EVENTS` (input) and `TOPIC_WH_STATE` (output).
- `BOOTSTRAP_FROM_PG`, `JDBC_PG_*`: enable and parameterize the optional bootstrap.
- Simulated time: uses Redis `sim:now`/`sim:today` via `simulated_time`; start `sim-clock` for deterministic runs.
