# Shelf Aggregator

## What it does
- Consumes raw `shelf_events` (pickup/putback/weight_change) from Kafka and lands them into a Delta RAW table.
- Maintains the latest shelf stock (`shelf_state`) by merging event deltas and unit weights from `shelf_profiles`.
- Optionally bootstraps the shelf state from Postgres before streaming kicks in.
- Publishes the compacted `shelf_state` topic (`TOPIC_SHELF_STATE`) for downstream consumers such as the alert engine.

## Spark job steps
1. **Profiles snapshot** – Read the compacted `TOPIC_SHELF_PROFILES`, keep the most recent message per `shelf_id`, and cache it for enrichments.
2. **Bootstrap (optional)** – With `BOOTSTRAP_FROM_PG=1`, read `ref.store_inventory_snapshot`, compute the latest stock per shelf, persist it to `STATE_PATH`, and emit it to the `TOPIC_SHELF_STATE` topic.
3. **Streaming ingest** – Read the `TOPIC_SHELF_EVENTS` stream, parse JSON payloads, and append the normalized rows to `RAW_PATH` (Delta).
4. **Aggregation** – For each micro‑batch, sum `delta_qty` by shelf (pickup = –qty, putback = +qty), enrich with weights, and merge into the Delta shelf state table.
5. **Publish** – Reload only the updated shelves, transform them to JSON, and write them to the compacted Kafka topic (`TOPIC_SHELF_STATE`).

## Key configuration
- `RAW_PATH`, `STATE_PATH`, `CKP_RAW`, `CKP_AGG`: Delta destinations and checkpoints.
- `KAFKA_BROKER`, `TOPIC_SHELF_EVENTS`, `TOPIC_SHELF_PROFILES`, `TOPIC_SHELF_STATE`: Kafka topics.
- `BOOTSTRAP_FROM_PG`, `JDBC_PG_*`: enable and control the initial state seeding.
