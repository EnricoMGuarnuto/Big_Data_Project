# POS Producer
## ./pos_producer.py
### Overview
This service consumes **foot traffic sessions** and **shelf events** to build shopping carts in memory and emits **POS transactions** to Kafka when customers exit the store.

- **Consumes**
  - `FOOT_TOPIC` (default: `foot_traffic`): full sessions with `entry_time` and `exit_time`.
  - `SHELF_TOPIC` (default: `shelf_events`): `pickup` / `putback` events per customer and item.
- **Produces**
  - `POS_TOPIC` (default: `pos_transactions`): one transaction per checkout, with line items and discounts applied.

All timestamps are UTC (ISO 8601). The component is thread-based and performs graceful cleanup of per-customer state after checkout.

---

### Topics & Event Model

#### Shelf events (consumed)
Expected fields:
```json
{ "event_type": "pickup|putback", "customer_id": "UUID", "item_id": "string", "quantity": 1 }
```

#### Foot traffic sessions (consumed)
```json
{
  "event_type": "foot_traffic",
  "customer_id": "UUID",
  "entry_time": "2025-11-07T09:32:10+00:00",
  "exit_time":  "2025-11-07T10:02:10+00:00"
}
```

#### POS transactions (produced)
```json
{
  "event_type": "pos_transaction",
  "transaction_id": "UUID",
  "customer_id": "UUID",
  "timestamp": "2025-11-07T10:02:10+00:00",
  "items": [
    { "item_id": "A12", "quantity": 2, "unit_price": 3.5, "discount": 0.10, "total_price": 6.3 }
  ]
}
```

> Note: discounts and prices are looked up from parquet files mounted at `/data`, respectively `all_discounts.parquet` and `store_inventory_final.parquet`.

---

### Configuration (env vars)

| Var | Default | Purpose |
|---|---:|---|
| `KAFKA_BROKER` | `kafka:9092` | Kafka bootstrap servers |
| `FOOT_TOPIC` | `foot_traffic` | Consumed sessions |
| `SHELF_TOPIC` | `shelf_events` | Consumed shelf events |
| `POS_TOPIC` | `pos_transactions` | Produced transactions |
| `GROUP_ID_SHELF` | `pos-simulator-shelf` | Consumer group for shelf events |
| `GROUP_ID_FOOT` | `pos-simulator-foot` | Consumer group for foot traffic |
| `STORE_PARQUET` | `/data/store_inventory_final.parquet` | Parquet with `shelf_id`, `item_price` |
| `DISCOUNT_PARQUET_PATH` | `/data/all_discounts.parquet` | Parquet with weekly discounts (`shelf_id`, `discount`, `week`) |
| `FORCE_CHECKOUT_IF_EMPTY` | `0` | If `1`, emits transactions even with empty carts |
| `MAX_SESSION_AGE_SEC` | `10800` | Force checkout after this age (seconds) |

Mount your parquet files from the host into `/data` (read-only).

---
## ./requirements.txt
The file contains:

```
kafka-python
pandas
pyarrow
```
> `pyarrow` is required for reading Parquet files.

---
## ./Dockerfile
A `Dockerfile` is provided to containerize the application. Ensure Docker is installed and configured to build and run the container.
