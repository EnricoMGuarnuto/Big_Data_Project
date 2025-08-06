# Kafka Producer - Shelf Restock

This Kafka Producer monitors shelf stock levels and triggers restock events by pulling from warehouse batches.

## Functionality:
- Continuously checks if any product in `store_inventory` is below `RESTOCK_THRESHOLD`.
- Selects the warehouse batch with the closest expiry date.
- Deducts quantity from `warehouse_batches.parquet`.
- Sends a `restock` event to Kafka.

## Dataset:
- Reads `/data/store_inventory_final.parquet` and `/data/warehouse_batches.parquet`.

## Kafka Topic:
- Default topic: `shelf_events`

## Environment Variables:
- `KAFKA_BROKER` (default: kafka:9092)
- `KAFKA_TOPIC` (default: shelf_events)
- `RESTOCK_THRESHOLD` (default: 10)
- `RESTOCK_QUANTITY` (default: 20)
- `SLEEP`: interval in seconds between restock checks (default: 10)

## Run inside Docker:
Ensure `/data` is mounted with both inventory and warehouse batches parquet files.
