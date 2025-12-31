# Shelf Alert Engine

## What it does
- Watches the compacted `shelf_state` stream and raises in-store alerts when stock drops below policy thresholds or items near expiry.
- Optionally bootstraps and caches shelf policies from Postgres (`POLICIES_PG_TABLE`) and joins with max stock snapshots to enrich the alerts.
- Produces two outputs: append-only alerts (`TOPIC_ALERTS` + Delta mirror) and the compacted `shelf_restock_plan` topic that feeds the restock manager.

## Spark job steps
1. **Bootstrap policies (optional)** – When `BOOTSTRAP_POLICIES_FROM_PG=1`, read policies from Postgres and seed the compacted Kafka topic `TOPIC_SHELF_POLICIES`.
2. **Static snapshots** – Load the latest active policy per shelf (plus a global default) from the policies topic and, if enabled, join with `MAX_STOCK_LATEST` read from Postgres.
3. **Streaming source** – Read the `TOPIC_SHELF_STATE` compacted topic as a stream, parsing it into structured columns.
4. **ForeachBatch logic**
   - Join shelves touched in the batch with policy/max-stock snapshots.
   - Compute stock %, minimum quantity breaches, and optional near-expiry warnings using the Delta mirror at `DL_SHELF_BATCH_PATH`.
   - Emit refill or near-expiry alerts via Kafka (and append to `DL_ALERTS_PATH`).
   - Build/update the `shelf_restock_plan` compacted topic and upsert the Delta mirror (`DL_RESTOCK_PATH`).

## Key configuration
- Kafka topics: `TOPIC_SHELF_STATE`, `TOPIC_SHELF_BATCH_STATE`, `TOPIC_SHELF_POLICIES`, `TOPIC_ALERTS`, `TOPIC_SHELF_RESTOCK`.
- Delta mirrors and checkpoints: `DL_*` paths plus `CHECKPOINT_ROOT`.
- Policy/stock tuning: `NEAR_EXPIRY_DAYS`, `DEFAULT_TARGET_PCT`, `LOAD_MAX_FROM_PG`, JDBC params.
- Simulated time: not used; timestamps come from payloads or Spark `current_timestamp()`.
