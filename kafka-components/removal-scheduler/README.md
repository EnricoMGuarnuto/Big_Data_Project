# Removal Scheduler (Kafka Python)

Simulated-time job that detects **expired store batches** and updates store state accordingly.

It:
- Reads `shelf_state` and `shelf_batch_state` from Delta.
- Finds expired batches (with `batch_quantity_store > 0`).
- Sets expired batches to 0 in `shelf_batch_state` and decrements `current_stock` in `shelf_state`.
- Publishes the updated rows to Kafka and emits an `expired_removal` alert.

## Inputs / Outputs

### Delta
- **Read/Write (merge update)**: `DL_SHELF_STATE_PATH` (default `/delta/cleansed/shelf_state`)
- **Read/Write (merge update)**: `DL_SHELF_BATCH_PATH` (default `/delta/cleansed/shelf_batch_state`)

Expected columns (minimum):
- `shelf_state`: `shelf_id`, `current_stock`, `last_update_ts`
- `shelf_batch_state`: `shelf_id`, `batch_code`, `expiry_date`, `batch_quantity_store`, `last_update_ts`

### Kafka
- **Produce**: `TOPIC_SHELF_STATE` (default `shelf_state`, compacted; key = `shelf_id`)
- **Produce**: `TOPIC_SHELF_BATCH_STATE` (default `shelf_batch_state`, compacted; key = `shelf_id::batch_code`)
- **Produce**: `TOPIC_ALERTS` (default `alerts`, append-only)

Alert payload example:
```json
{
  "event_type": "expired_removal",
  "shelf_id": "…",
  "location": "store",
  "suggested_qty": 12,
  "created_at": "…"
}
```

## Expiration logic (`REMOVE_MODE`)
- `next_day` (default): remove batches with `expiry_date < simulated_today` (i.e., the day after expiry).
- `same_day_evening`: remove batches with `expiry_date <= simulated_today` (same-day cleanup).

## Scheduling
- Polls Redis-based simulated time (`simulated_time.redis_helpers`) and runs **once per simulated day**.
- The service waits until both Delta paths exist before starting processing.

## Configuration (env vars)

| Var | Default | Purpose |
|---|---:|---|
| `KAFKA_BROKER` | `kafka:9092` | Kafka bootstrap servers |
| `TOPIC_ALERTS` | `alerts` | Output topic for alerts |
| `TOPIC_SHELF_STATE` | `shelf_state` | Output topic for shelf state |
| `TOPIC_SHELF_BATCH_STATE` | `shelf_batch_state` | Output topic for shelf batch state |
| `DELTA_ROOT` | `/delta` | Delta root folder |
| `DL_SHELF_STATE_PATH` | `/delta/cleansed/shelf_state` | Shelf state Delta path |
| `DL_SHELF_BATCH_PATH` | `/delta/cleansed/shelf_batch_state` | Shelf batch state Delta path |
| `REMOVE_MODE` | `next_day` | Expiration policy (`next_day` / `same_day_evening`) |
| `POLL_SECONDS` | `1.0` | Polling interval (seconds) |
| `KAFKA_WAIT_TIMEOUT_S` | `180` | Startup wait for Kafka (seconds) |
| `KAFKA_WAIT_POLL_S` | `2.0` | Kafka wait retry interval (seconds) |
| `LOG_LEVEL` | `INFO` | `INFO`/`DEBUG` enable logs |

## Build
```bash
docker build -t removal-scheduler .
```

## Run (example)
This container imports the `simulated_time` package from the repo, so mount it (and set `PYTHONPATH`).
```bash
docker run --rm \
  -e KAFKA_BROKER=kafka:9092 \
  -e DELTA_ROOT=/delta \
  -e PYTHONPATH=/app \
  -v "$PWD/delta:/delta" \
  -v "$PWD/simulated_time:/app/simulated_time:ro" \
  removal-scheduler
```

## Notes
- The service assumes Delta tables exist; initialize them with `spark-apps/deltalake/` in the full stack.
- Published `shelf_state` records include the updated stock fields; the JSON may also include a `removed_qty` field.
