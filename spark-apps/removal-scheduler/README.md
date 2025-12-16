# Removal Scheduler

## What it does
- Batch Spark job that removes expired in-store inventory based on the Delta mirror `shelf_batch_state`.
- Finds expired batches with `batch_quantity_store > 0`, sets their in-store quantity to 0, and decrements `shelf_state.current_stock` accordingly (clamped to `>= 0`).
- Persists updates back to Delta via idempotent merges and publishes the updated rows to compacted Kafka topics so downstream consumers see the new state.
- Optionally emits `expired_removal` alerts to the standard `alerts` topic for audit/monitoring.

## Job flow
1. Validate that the required Delta tables exist at `DL_SHELF_STATE_PATH` and `DL_SHELF_BATCH_PATH`, and that key columns are present.
2. Determine what “expired” means via `REMOVE_MODE`:
   - `next_day` (default): `expiry_date < current_date()` (remove the day after expiry).
   - `same_day_evening`: `expiry_date <= current_date()` (remove on expiry day, e.g. at store closing time).
3. Select expired batches still present on shelf and aggregate the removed quantity per `shelf_id`.
4. Upsert `shelf_state` (key: `shelf_id`) with the new `current_stock` and refreshed `last_update_ts`.
5. Upsert `shelf_batch_state` (key: `shelf_id`, `batch_code`) setting `batch_quantity_store = 0` and updating `last_update_ts`.
6. Publish:
   - Updated `shelf_state` rows to `TOPIC_SHELF_STATE` (key: `shelf_id`).
   - Updated `shelf_batch_state` rows to `TOPIC_SHELF_BATCH_STATE` (key: `shelf_id::batch_code`).
   - Optional `expired_removal` alerts to `TOPIC_ALERTS`.

## Key configuration
- Delta: `DELTA_ROOT`, `DL_SHELF_STATE_PATH`, `DL_SHELF_BATCH_PATH`.
- Kafka: `KAFKA_BROKER`, `TOPIC_SHELF_STATE`, `TOPIC_SHELF_BATCH_STATE`, `TOPIC_ALERTS`.
- Behavior: `REMOVE_MODE` (`next_day` | `same_day_evening`), `EMIT_ALERTS` (`1`/`0`).

