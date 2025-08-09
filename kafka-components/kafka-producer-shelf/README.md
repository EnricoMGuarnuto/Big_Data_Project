# Kafka Producer — Shelf Sensors

Simulates smart-shelf activity driven by real foot-traffic:
- Consumes `foot_traffic` to compute a rolling events/min traffic level.
- Emits `pickup` / `putback` events to `shelf_events`.
- Adjusts event frequency and pickup probability based on traffic.

## Topics
- In: `foot_traffic`
- Out: `shelf_events`

## Environment
- `KAFKA_BROKER` (default `kafka:9092`)
- `KAFKA_TOPIC_FOOT` (default `foot_traffic`)
- `KAFKA_TOPIC_SHELF` (default `shelf_events`)
- `SHELF_SLEEP_BASE` (default `1.0`)
- `SHELF_SLEEP_MIN` (default `0.15`)
- `PICKUP_BASE_P` (default `0.70`)
- `PICKUP_BUMP` (default `0.20`)
- `PUTBACK_P` (default `0.10`)
- `ALPHA_RATE` (default `0.08`)
- `TRAFFIC_WINDOW_MIN` (default `5`)

## Volumes
Mount inventory parquet:
- Host `./data` → Container `/data`

## Run (Compose)
Service should set env vars above and mount `./data:/data`.
