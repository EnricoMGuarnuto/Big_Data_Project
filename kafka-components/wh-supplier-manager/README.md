# Warehouse Supplier Manager (Kafka Python)

Turns the **warehouse supplier plan** into **supplier orders**, **receipts**, and inbound warehouse events (`wh_in`).

This is a lightweight Python service (no Spark) driven by the **simulated UTC clock**.

## What it does

### Cutoff (order issuance)
At the configured cutoff times:
- Reads pending plans from Delta (`DL_SUPPLIER_PLAN_PATH`).
- Aggregates them to **1 order per shelf** for the next delivery date.
- Upserts the orders ledger (`DL_ORDERS_PATH`) and marks plans as `issued`.
- Publishes the updated plan rows to the compacted Kafka topic `wh_supplier_plan` (key = `shelf_id`).

### Delivery (warehouse inbound)
At the configured delivery times:
- Finds due `issued` orders for the current day (idempotent via receipts ledger).
- Emits one `wh_in` event per shelf to Kafka `wh_events`.
- Writes/updates the receipts ledger (`DL_RECEIPTS_PATH`) and marks orders `delivered`.
- Marks the corresponding plans as `completed` and republishes them to `wh_supplier_plan`.

## Inputs / Outputs

### Delta
- **Read/Write**: `DL_SUPPLIER_PLAN_PATH` (default `/delta/ops/wh_supplier_plan`)
- **Write (overwrite, idempotent upsert-by-keys)**: `DL_ORDERS_PATH` (default `/delta/ops/wh_supplier_orders`)
- **Write (overwrite, idempotent upsert-by-keys)**: `DL_RECEIPTS_PATH` (default `/delta/ops/wh_supplier_receipts`)
- **Optional append mirror**: `DL_WH_EVENTS_RAW` (default `/delta/raw/wh_events`, controlled by `MIRROR_WH_EVENTS_DELTA`)

Upstream: `wh_supplier_plan` is typically produced by `spark-apps/wh-alert-engine/` (Kafka + Delta mirror).

### Kafka
- **Produce**: `TOPIC_WH_SUPPLIER_PLAN` (default `wh_supplier_plan`, compacted; key = `shelf_id`)
- **Produce**: `TOPIC_WH_EVENTS` (default `wh_events`, append-only)

Example `wh_in` event emitted on delivery:
```json
{
  "event_type": "wh_in",
  "event_id": "…",
  "plan_id": null,
  "shelf_id": "A12",
  "batch_code": "SUP-20260128-A12-1",
  "qty": 40,
  "unit": "ea",
  "timestamp": "2026-01-28T08:00:00+00:00",
  "fifo": true,
  "received_date": "2026-01-28",
  "expiry_date": "2027-01-28",
  "reason": "supplier_delivery"
}
```

## Scheduling (simulated time)
- Uses `simulated_time.clock.get_simulated_now()` and treats it as UTC.
- Weekday numbers follow Python `datetime.weekday()` (`Mon=0 … Sun=6`).
- If simulated time jumps over the exact minute, the service still runs **once per day** after the scheduled time.

Defaults:
- Cutoff: `Sun/Tue/Thu` at `12:00` UTC (`CUTOFF_DOWS=6,1,3`)
- Delivery: `Mon/Wed/Fri` at `08:00` UTC (`DELIVERY_DOWS=0,2,4`)

## Idempotency
- Deterministic UUIDv5 IDs for orders/receipts/events based on `date + shelf_id`.
- Delivery skips duplicates using the receipts ledger (`delivery_date`, `shelf_id`).

## Configuration (env vars)

| Var | Default | Purpose |
|---|---:|---|
| `KAFKA_BROKER` | `kafka:9092` | Kafka bootstrap servers |
| `TOPIC_WH_SUPPLIER_PLAN` | `wh_supplier_plan` | Output topic for plan status updates |
| `TOPIC_WH_EVENTS` | `wh_events` | Output topic for warehouse events |
| `DELTA_ROOT` | `/delta` | Delta root folder |
| `DL_SUPPLIER_PLAN_PATH` | `/delta/ops/wh_supplier_plan` | Supplier plan Delta path |
| `DL_ORDERS_PATH` | `/delta/ops/wh_supplier_orders` | Orders ledger Delta path |
| `DL_RECEIPTS_PATH` | `/delta/ops/wh_supplier_receipts` | Receipts ledger Delta path |
| `DL_WH_EVENTS_RAW` | `/delta/raw/wh_events` | Optional Delta mirror for `wh_events` |
| `POLL_SECONDS` | `10` | Polling interval (seconds) |
| `MIRROR_WH_EVENTS_DELTA` | `0` | If `1`, also append `wh_in` events into `DL_WH_EVENTS_RAW` |
| `CUTOFF_DOWS` | `6,1,3` | Cutoff weekdays (`Mon=0..Sun=6`) |
| `CUTOFF_HOUR` | `12` | Cutoff hour (UTC) |
| `CUTOFF_MINUTE` | `0` | Cutoff minute (UTC) |
| `DELIVERY_DOWS` | `0,2,4` | Delivery weekdays (`Mon=0..Sun=6`) |
| `DELIVERY_HOUR` | `8` | Delivery hour (UTC) |
| `DELIVERY_MINUTE` | `0` | Delivery minute (UTC) |
| `DEFAULT_EXPIRY_DAYS` | `365` | Expiry offset for supplier batches |
| `DELTA_WRITE_MAX_RETRIES` | `5` | Retry count for Delta write conflicts |
| `DELTA_WRITE_RETRY_BASE_S` | `0.8` | Backoff base for Delta retries |
| `KAFKA_WAIT_TIMEOUT_S` | `180` | Startup wait for Kafka (seconds) |
| `KAFKA_WAIT_POLL_S` | `2.0` | Kafka wait retry interval (seconds) |
| `LOG_LEVEL` | `INFO` | `INFO`/`DEBUG` enable logs |

## Build
```bash
docker build -t wh-supplier-manager .
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
  wh-supplier-manager
```

## Notes
- Prefer keeping `MIRROR_WH_EVENTS_DELTA=0` when `spark-apps/wh-aggregator/` is running, to avoid multiple writers to `/delta/raw/wh_events`.
- `docker-compose.yml` may set `TOPIC_ALERTS`, but this service does not emit alerts today.
