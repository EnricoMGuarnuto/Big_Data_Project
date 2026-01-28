# Daily Discount Manager (Kafka Python)

Computes **daily discounts** for near-expiry products using the **simulated clock** and publishes updates to Kafka.

It:
- Reads the current shelf batches from Delta (`shelf_batch_state`).
- Finds products expiring **today** or **tomorrow** (with quantity > 0).
- Generates a random extra discount (steps of `0.10`) in `[DISCOUNT_MIN, DISCOUNT_MAX]`.
- Upserts the result into Delta (`analytics/daily_discounts`) and publishes Kafka messages.

## Inputs / Outputs

### Delta
- **Read**: `DL_SHELF_BATCH_PATH` (default `/delta/cleansed/shelf_batch_state`)
- **Write (upsert)**: `DL_DAILY_DISC_PATH` (default `/delta/analytics/daily_discounts`)

Expected input columns (minimum): `shelf_id`, `expiry_date`, `batch_quantity_store`.

### Kafka
- **Produce**: `TOPIC_DAILY_DISCOUNTS` (default `daily_discounts`, compacted; key = `shelf_id`)
- **Produce**: `TOPIC_ALERTS` (default `alerts`, append-only)

`daily_discounts` message fields: `shelf_id`, `discount_date`, `discount`, `created_at`.

`alerts` messages emitted for each updated shelf:
```json
{ "event_type": "near_expiry_discount", "shelf_id": "…", "location": "store", "created_at": "…" }
```

## Scheduling
- Polls Redis-based simulated time (`simulated_time.redis_helpers`) and runs **once per simulated day**.
- `POLL_SECONDS` controls the polling interval.

## Configuration (env vars)

| Var | Default | Purpose |
|---|---:|---|
| `KAFKA_BROKER` | `kafka:9092` | Kafka bootstrap servers |
| `TOPIC_DAILY_DISCOUNTS` | `daily_discounts` | Output topic for discount updates |
| `TOPIC_ALERTS` | `alerts` | Output topic for alert events |
| `DELTA_ROOT` | `/delta` | Delta root folder |
| `DL_SHELF_BATCH_PATH` | `/delta/cleansed/shelf_batch_state` | Input Delta table path |
| `DL_DAILY_DISC_PATH` | `/delta/analytics/daily_discounts` | Output Delta table path |
| `DISCOUNT_MIN` | `0.30` | Minimum extra discount |
| `DISCOUNT_MAX` | `0.50` | Maximum extra discount |
| `POLL_SECONDS` | `10.0` | Polling interval (seconds) |
| `KAFKA_WAIT_TIMEOUT_S` | `180` | Startup wait for Kafka (seconds) |
| `KAFKA_WAIT_POLL_S` | `2.0` | Kafka wait retry interval (seconds) |
| `LOG_LEVEL` | `INFO` | `INFO`/`DEBUG` enable logs |

## Build
```bash
docker build -t daily-discount-manager .
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
  daily-discount-manager
```

## Notes
- The first run will create the output Delta table at `DL_DAILY_DISC_PATH` if it doesn’t exist yet.
- If you run the full stack, prefer using the repo root `docker-compose.yml` instead of `docker run`.
