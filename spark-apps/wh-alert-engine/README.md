# Warehouse Alert Engine

## What it does
- Monitors the compacted `wh_state` feed and raises warehouse-level alerts when stock drops below per-shelf reorder points or lots get close to expiry.
- Optionally bootstraps warehouse policies from Postgres so the Kafka topic `TOPIC_WH_POLICIES` always has at least one record per shelf.
- Derives replenishment requests for suppliers by publishing a compacted `wh_supplier_plan` topic and a Delta mirror.

## Spark job steps
1. **Bootstrap policies (optional)** – If `BOOTSTRAP_WH_POLICIES_FROM_PG=1`, load `WH_POLICIES_PG_TABLE` via JDBC and seed the compacted policies topic.
2. **Static snapshots** – Cache the latest active policy per shelf and, when available, infer standard batch sizes from the Delta warehouse batch mirror.
3. **Streaming source** – Consume `TOPIC_WH_STATE`, parsing records into `shelf_id`, current stock, and timestamps.
4. **ForeachBatch logic**
   - Join shelves touched in the micro-batch with policy settings and with Delta `DL_WH_BATCH_PATH` to pick near-expiry lots.
   - Emit two alert types (`supplier_request` and `near_expiry`), append them to `DL_ALERTS_PATH`, and send them to the append-only `TOPIC_ALERTS`.
   - Compute supplier plans (rounding to batch multiples when possible) and publish them to `TOPIC_WH_SUPPLIER_PLAN` while keeping the Delta mirror synced.

## Key configuration
- Kafka topics: `TOPIC_WH_STATE`, `TOPIC_WH_BATCH_STATE`, `TOPIC_WH_POLICIES`, `TOPIC_ALERTS`, `TOPIC_WH_SUPPLIER_PLAN`.
- Delta paths/checkpoints: `DL_WH_STATE_PATH`, `DL_WH_BATCH_PATH`, `DL_ALERTS_PATH`, `DL_SUPPLIER_PLAN_PATH`, `CHECKPOINT_ROOT`.
- Alert tuning knobs: `NEAR_EXPIRY_DAYS`, `DEFAULT_MULTIPLIER`, plus JDBC settings for policy bootstrap.
