# Kafka Producer — Shelf Restock (Redis-backed)

Listens to `pickup` / `putback` from `shelf_events`. Maintains live inventory state in **Redis**:
- Tracks shelf stock per item and initial target stock.
- Tracks batch quantities in warehouse/store with expiry-based FIFO.
- When shelf stock <= threshold: schedules a restock after a delay.
- Restock pulls from warehouse batches (earliest expiry first), up to **initial stock** target.
- Emits events:
  - `warehouse_events`: `warehouse_pick` (per-batch)
  - `shelf_events`: `restock` (per-batch)
  - `alerts`: `shelf_restock_alert`, `warehouse_restock_required`, `expiry_alert`

## Topics
- In: `shelf_events` (pickup/putback)
- Out: `warehouse_events`, `shelf_events` (restock), `alerts`

## Environment
- `KAFKA_BROKER` (default `kafka:9092`)
- `TOPIC_SHELF_EVENTS` (default `shelf_events`)
- `TOPIC_WAREHOUSE_EVENTS` (default `warehouse_events`)
- `TOPIC_ALERTS` (default `alerts`)
- `RESTOCK_THRESHOLD` (default `10`)
- `RESTOCK_DELAY_SEC` (default `60`)
- `RESTOCK_MAX_CHUNK` (default `100000`)
- `EXPIRY_ALERT_DAYS` (default `3`)
- `EXPIRY_POLL_SEC` (default `15`)
- `REDIS_HOST` (default `redis`)
- `REDIS_PORT` (default `6379`)
- `REDIS_DB` (default `0`)
- `STORE_PARQUET` (default `/data/store_inventory_final.parquet`)
- `WH_BATCHES_PARQUET` (default `/data/warehouse_batches.parquet`)
- `STORE_BATCHES_PARQUET` (default `/data/store_batches.parquet`)

## Volumes
- Host `./data` → Container `/data`

## Notes
- Parquet files are used only for **bootstrap**. Live state is in Redis.
- If warehouse stock can’t meet initial target → emits `warehouse_restock_required`.
