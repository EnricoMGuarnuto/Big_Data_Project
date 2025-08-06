# Kafka Consumer - Shelf Batches Updater

This Kafka Consumer listens to `restock` events and updates the in-store batches accordingly.

## Functionality:
- If a batch already exists in `store_batches`, it updates the quantity.
- If the batch does not exist, it adds the batch from `warehouse_batches`.

## Dataset:
- Reads `/data/store_batches.parquet` and `/data/warehouse_batches.parquet`.

## Kafka Topic:
- Default topic: `shelf_events`

## Environment Variables:
- `KAFKA_BROKER` (default: kafka:9092)
- `KAFKA_TOPIC` (default: shelf_events)

## Run inside Docker:
Ensure `/data` is mounted with both store_batches and warehouse_batches parquet files.
