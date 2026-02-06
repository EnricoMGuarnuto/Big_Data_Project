# Shelf Events Producer
## ./shelf_producer
### Overview
Generates realistic shelf interaction events for customers currently in the store, based on **foot-traffic sessions**.  
It consumes `foot_traffic` (session with entry/exit), schedules actions during the session, and writes:
- `pickup` / `putback` events
- matching `weight_change` events (to emulate smart-shelf sensors)

Timestamps are emitted in ISO 8601 using the simulated clock (Redis).

---

## Topics

- **Consume (Kafka)**: `KAFKA_TOPIC_FOOT` (default `foot_traffic`)
- **Write (Redis stream)**: `REDIS_STREAM` (default `shelf_events`)

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
- Loads weekly discounts from **Parquet** (`DISCOUNT_PARQUET_PATH`) with columns: `shelf_id`, `discount`, `week`.
- Loads daily discounts from Postgres (`DAILY_DISCOUNT_TABLE`) for the current simulated date.
- Computes item pick probabilities via `pick_score = item_visibility * (1 + discount)`, where discounts combine weekly + daily.
- For each active customer (from foot_traffic), schedules 3–30 actions uniformly between `entry+60s` and `exit-30s`.
- On each scheduled timestamp, emits a `pickup` or `putback` (probability via `PUTBACK_PROB`) and a corresponding `weight_change` with `delta_weight = ±(item_weight * quantity)`.

---

## Configuration (env vars)

| Var | Default | Purpose |
|---|---:|---|
| `KAFKA_BROKER` | `kafka:9092` | Kafka bootstrap servers |
| `KAFKA_TOPIC_SHELF` | `shelf_events` | Produced topic |
| `KAFKA_TOPIC_FOOT` | `foot_traffic` | Consumed topic (sessions) |
| `REDIS_HOST` | `redis` | Redis host (simulated time + stream) |
| `REDIS_PORT` | `6379` | Redis port |
| `REDIS_DB` | `0` | Redis DB |
| `REDIS_STREAM` | `shelf_events` | Redis stream name |
| `STORE_PARQUET` | `/data/store_inventory_final.parquet` | Catalog parquet (`shelf_id`, `item_weight`, `item_visibility`) |
| `STORE_CSV_PATH` | `/data/db_csv/store_inventory_final.csv` | Fallback catalog CSV if parquet is missing |
| `DISCOUNT_PARQUET_PATH` | `/data/all_discounts.parquet` | Discounts parquet (`shelf_id`, `discount`, `week`) |
| `SHELF_IDLE_SLEEP` | `0.01` | Idle sleep (seconds) when nothing to emit |
| `PUTBACK_PROB` | `0.15` | Probability that an action is `putback` instead of `pickup` |
| `STORE_FILE_RETRY_SECONDS` | `10` | Retry interval when inventory file is missing |
| `PG_HOST` | `postgres` | Postgres host |
| `PG_PORT` | `5432` | Postgres port |
| `PG_DB` | `smart_shelf` | Postgres database |
| `PG_USER` | `bdt_user` | Postgres user |
| `PG_PASS` | `bdt_password` | Postgres password |
| `DAILY_DISCOUNT_TABLE` | `analytics.daily_discounts` | Daily discounts table |

Mount your datasets under `/data` (read-only).

Events are forwarded from Redis to Kafka by `redis-kafka-bridge`.

---
## ./requirements.txt
The file contains:

```
kafka-python
psycopg2-binary
pandas
pyarrow
redis
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
    - redis
  environment:
    KAFKA_BROKER: kafka:9092
    KAFKA_TOPIC_SHELF: shelf_events
    KAFKA_TOPIC_FOOT: foot_traffic
    REDIS_HOST: redis
    REDIS_PORT: "6379"
    REDIS_DB: "0"
    REDIS_STREAM: shelf_events
    STORE_PARQUET: /data/store_inventory_final.parquet
    DISCOUNT_PARQUET_PATH: /data/all_discounts.parquet
    SHELF_IDLE_SLEEP: "0.01"
    PUTBACK_PROB: "0.15"
  volumes:
    - ./data:/data:ro
    - ./simulated_time:/app/simulated_time:ro
  restart: unless-stopped
```

---
## ./Dockerfile
A `Dockerfile` is provided to containerize the application. Ensure Docker is installed and configured to build and run the container.
