# Shelf Batch State Updater

## What it does
- Maintains the in‑store batch inventory (`shelf_batch_state`) in Delta Lake based on POS transactions.
- Optionally bootstraps the Delta table (and Kafka compacted topic) from a Postgres snapshot before streaming starts.
- Publishes the latest per `(shelf_id, batch_code)` state back to Kafka so downstream jobs can treat it as a compacted topic.

## Spark job steps
1. **Bootstrap (optional)** – When `BOOTSTRAP_FROM_PG=1`, read `ref.store_batches_snapshot` via JDBC, write it to `BATCH_STATE_PATH`, and emit the snapshot to the Kafka topic indicated by `TOPIC_BATCH_STATE`.
2. **Streaming read** – Consume POS transactions from `TOPIC_POS_TRANSACTIONS`; explode the `items` array to get `(shelf_id, batch_code, quantity)` events.
3. **Delta merge** – Aggregate quantity deltas per batch inside each micro‑batch and merge them into the Delta table so `batch_quantity_store` decreases as sales happen.
4. **Kafka publish** – Reload the touched keys from Delta and overwrite the corresponding entries in the compacted Kafka topic (`TOPIC_BATCH_STATE`).

## Key configuration
- `BATCH_STATE_PATH`, `CHECKPOINT_PATH`: Delta destination and streaming checkpoint.
- `KAFKA_BROKER`, `TOPIC_POS_TRANSACTIONS`, `TOPIC_BATCH_STATE`: IO topics.
- `BOOTSTRAP_FROM_PG`, `JDBC_PG_URL`, `JDBC_PG_USER`, `JDBC_PG_PASSWORD`: enable and drive the optional bootstrap.
