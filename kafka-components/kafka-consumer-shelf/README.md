# Kafka Consumer — Shelf Materializer

Listens to:
- `shelf_events` (`restock` events)
- `warehouse_events` (`warehouse_pick` events)

Maintains **materialized parquet snapshots** (so base parquet remains unchanged):
- `/data/materialized/store_batches_state.parquet`
- `/data/materialized/warehouse_batches_state.parquet`

## Environment
- `KAFKA_BROKER` (default `kafka:9092`)
- `TOPIC_SHELF_EVENTS` (default `shelf_events`)
- `TOPIC_WAREHOUSE_EVENTS` (default `warehouse_events`)
- `WH_BATCHES_PARQUET` (default `/data/warehouse_batches.parquet`)
- `OUT_DIR` (default `/data/materialized`)

## Volumes
- Host `./data` → Container `/data`
