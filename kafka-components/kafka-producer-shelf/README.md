# Kafka Producer - Shelf Sensors

This Kafka Producer simulates smart shelf sensors detecting when customers pick up or put back items in-store.

## Events Produced:
- `pickup`: A product is taken from the shelf.
- `putback`: A product is returned to the shelf.

## Dataset:
- Reads `/data/store_inventory_final.parquet` to get current stock and item weights.
- Simulates adjustments in `current_stock` as items are picked/put back.

## Kafka Topic:
- Default topic: `shelf_events`

## Environment Variables:
- `KAFKA_BROKER` (default: kafka:9092)
- `KAFKA_TOPIC` (default: shelf_events)
- `SLEEP`: interval in seconds between events (default: 2)

## Run inside Docker:
Make sure `/data` volume is mounted with the inventory parquet file.
