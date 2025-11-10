# Shelf Events Producer
## ./shelf_producer
### Overview
Generates realistic shelf interaction events for customers currently in the store, based on **foot-traffic sessions**.  
It consumes `foot_traffic` (session with entry/exit), schedules actions during the session, and produces:
- `pickup` / `putback` events
- matching `weight_change` events (to emulate smart-shelf sensors)

All timestamps are UTC (ISO 8601).

---

## Topics

- **Consume**: `KAFKA_TOPIC_FOOT` (default `foot_traffic`)
- **Produce**: `KAFKA_TOPIC_SHELF` (default `shelf_events`)

**Event examples**

`pickup` / `putback`:
```json
{
  "event_type": "pickup",
  "customer_id": "UUID",
  "item_id": "A12",
  "weight": 0.55,
  "quantity": 2,
  "timestamp": "2025-11-07T10:01:02+00:00"
}
```

`weight_change`:
```json
{
  "event_type": "weight_change",
  "customer_id": "UUID",
  "item_id": "A12",
  "delta_weight": -1.1,
  "timestamp": "2025-11-07T10:01:02+00:00"
}
```

---

## Data & Behavior
- Loads store catalog from **Parquet** (`STORE_PARQUET`) with columns: `shelf_id`, `item_weight`, `item_visibility`.
- Optionally loads weekly discounts from **Parquet** (`DISCOUNT_PARQUET_PATH`) with columns: `shelf_id`, `discount`, `week`.
- Computes item pick probabilities via `pick_score = item_visibility * (1 + discount)`.
- For each active customer (from foot_traffic), schedules 3–30 actions uniformly between `entry+60s` and `exit-30s`.
- On each scheduled timestamp, emits a `pickup` or `putback` (probability via `PUTBACK_PROB`) and a corresponding `weight_change` with `delta_weight = ±(item_weight * quantity)`.

---

## Configuration (env vars)

| Var | Default | Purpose |
|---|---:|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka bootstrap servers |
| `KAFKA_TOPIC_SHELF` | `shelf_events` | Produced topic |
| `KAFKA_TOPIC_FOOT` | `foot_traffic` | Consumed topic (sessions) |
| `STORE_PARQUET` | `/data/store_inventory_final.parquet` | Catalog parquet (`shelf_id`, `item_weight`, `item_visibility`) |
| `DISCOUNT_PARQUET_PATH` | `/data/all_discounts.parquet` | Discounts parquet (`shelf_id`, `discount`, `week`) |
| `SHELF_SLEEP` | `1.0` | Idle sleep (seconds) when nothing to emit |
| `PUTBACK_PROB` | `0.15` | Probability that an action is `putback` instead of `pickup` |

Mount your datasets under `/data` (read-only).

---
## ./requirements.txt
The file contains:

```
kafka-python
pandas
pyarrow
```
---

## docker-compose (service excerpt)
```yaml
kafka-producer-shelf:
  build:
    context: ./kafka-components/kafka-producer-shelf
    dockerfile: Dockerfile
  container_name: kafka-producer-shelf
  depends_on:
    - kafka
  environment:
    KAFKA_BOOTSTRAP_SERVERS: kafka:9092
    KAFKA_TOPIC_SHELF: shelf_events
    KAFKA_TOPIC_FOOT: foot_traffic
    STORE_PARQUET: /data/store_inventory_final.parquet
    DISCOUNT_PARQUET_PATH: /data/all_discounts.parquet
    SHELF_SLEEP: "1.0"
    PUTBACK_PROB: "0.15"
  volumes:
    - ./data:/data:ro
  restart: unless-stopped
```

---
## ./Dockerfile
A `Dockerfile` is provided to containerize the application. Ensure Docker is installed and configured to build and run the container.