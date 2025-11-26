# WAREHOUSE SHELF
## ./wh_shelf_producer.py
### Overview
This service consumes Spark-generated **plans** and emits **warehouse events** while enforcing **FIFO** on batches. All writes go Redis → Kafka (Redis Streams as a buffer, then Kafka as the source of truth).

- **Consumes**
  - `PLAN_TOPIC` (default: `wh_restock_plan`)→ refill WH → store plans (`suggested_qty`).
  - `SUPPLIER_PLAN_TOPIC` (default: `wh_supplier_plan`)→ inbound supplier → WH plans.
- **Produces**
  - `EVENT_TOPIC` (default: `wh_events`): `wh_out` (to store) and `wh_in` (from supplier).
  - `ALERT_TOPIC` (default: `alerts`): `alert_status_change` (Kafka-first alert closing).

- **Bootstrap state**
  - Loads initial FIFO per `shelf_id` from /data/warehouse_batches.parquet. (Later, replace with a compacted topic like wh_batch_state.)

All timestamps are UTC (ISO 8601).

---

### Topics & Event Model

#### Refill plan (consumed)
Expected fields:
```json
{ "plan_id": "PL-123", "shelf_id": "BEVWAT1", "suggested_qty": 120, "alert_id": "AL-987" }
```

#### Supplier plan (consumed)
```json
{
  "plan_id": "PL-456",
  "shelf_id": "BEVWAT1",
  "qty": 500,
  "batch_code": "B-9999",           
  "expiry_date": "2026-12-31",      
  "alert_id": "AL-654"              
}
```

#### Warehouse events (produced)
wh_out (warehouse → store) and wh_in (supplier → warehouse). Snapshot fields reflect post-event quantities.
```json
{
  "event_type": "wh_out",
  "event_id": "UUID",
  "plan_id": "PL-123",
  "shelf_id": "BEVWAT1",
  "batch_code": "B-6357",
  "qty": 80,
  "unit": "ea",
  "timestamp": "2025-11-07T10:02:10+00:00",
  "fifo": true,
  "received_date": "2025-11-05",
  "expiry_date": "2026-12-14",
  "batch_quantity_warehouse_after": 310,
  "batch_quantity_store_after": 80,
  "shelf_warehouse_qty_after": 1240,
  "reason": "plan_restock"
}
```
```json
{
  "event_type": "wh_in",
  "event_id": "UUID",
  "plan_id": "PL-456",
  "shelf_id": "BEVWAT1",
  "batch_code": "B-9999",
  "qty": 500,
  "unit": "ea",
  "timestamp": "2025-11-07T08:10:00+00:00",
  "fifo": true,
  "received_date": "2025-11-07",
  "expiry_date": "2026-12-31",
  "batch_quantity_warehouse_after": 500,
  "batch_quantity_store_after": 0,
  "shelf_warehouse_qty_after": 1740,
  "reason": "plan_inbound"
}

```
#### Alerts status change (produced)
Emitted when a plan references alert_id.
On plan reception: ack.
After execution: ack partial or resolved when fully satisfied.

```json
{
  "event_type": "alert_status_change",
  "alert_id": "AL-987",
  "status": "resolved",
  "timestamp": "2025-11-07T10:02:15+00:00",
  "reason": "plan_restock: completed (120/120)"
}
```

> Note: discounts and prices are looked up from parquet files mounted at `/data`, respectively `all_discounts.parquet` and `store_inventory_final.parquet`.

---

### Configuration (env vars)

| Var | Default | Purpose |
|---|---:|---|
| `KAFKA_BROKER` | `kafka:9092` | Kafka bootstrap servers |
| `PLAN_TOPIC` | `wh_restock_plan` | Consumed refill plans (WH → store) |
| `SUPPLIER_PLAN_TOPIC` | `wh_supplier_plan` | Consumed supplier inbound plans |
| `EVENT_TOPIC` | `wh_events` | Produced warehouse events (`wh_in`/`wh_out`) |
| `ALERTS_TOPIC` | `alerts` | Produced `alert_status_change` events |
| `WH_BATCHES_PARQUET` | `/data/warehouse_batches.parquet` | Bootstrap FIFO per `shelf_id` |
| `CLIENT_ID` | `warehouse-executor` | Kafka client id |
| `POLL_MS` | `300` | Consumer poll timeout (ms) |
| `LOG_LEVEL` | `INFO` | Log verbosity |
| `MAX_SEEN_IDS` | `50000` | Id cache for dedupe/idempotency |
| `REDIS_HOST` | `redis` | Redis hostname |
| `REDIS_PORT` | `6379` | Redis port |
| `REDIS_DB` | `0` | Redis DB index |
| `REDIS_STREAM` | `wh_events` | Redis Stream used as buffer before Kafka |

Mount your parquet files from the host into `/data` (read-only).

---
## ./requirements.txt
The file contains:

```
kafka-python
redis
pandas
pyarrow
```

---
## ./Dockerfile
A `Dockerfile` is provided to containerize the application. Ensure Docker is installed and configured to build and run the container.
