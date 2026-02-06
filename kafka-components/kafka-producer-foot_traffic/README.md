# Foot Traffic Producer
## ./foot_traffic_producer.py
### Overview
`foot_traffic_producer.py` simulates store foot traffic in (near) real time and writes events to Redis Streams.  
It emits:
- **Sessions** → `foot_traffic` stream (single record with entry & exit).
- **Atomic events** → `foot_traffic:realistic` stream (separate `entry` / `exit`).

Timestamps are UTC (ISO 8601). Patterns vary by weekday and time slot.

---

### Features
- Realistic **weekday/time-slot** traffic profile  
- Two-topic design: sessions + ENTRY/EXIT  
- Deterministic runs with `SEED`  
- Graceful shutdown

---

### Event Schemas

**Session (`foot_traffic`)**
```json
{
  "event_type": "foot_traffic",
  "customer_id": "UUID",
  "entry_time": "2025-11-07T09:32:10+00:00",
  "exit_time":  "2025-11-07T10:02:10+00:00",
  "trip_duration_minutes": 30,
  "weekday": "Friday",
  "time_slot": "07:00–09:59"
}
```

**Atomic (`foot_traffic_realistic`)**
```json
{ "event_type": "entry", "time": "2025-11-07T09:32:10+00:00", "weekday": "Friday", "time_slot": "07:00–09:59" }
{ "event_type": "exit",  "time": "2025-11-07T10:02:10+00:00", "weekday": "Friday", "time_slot": "07:00–09:59" }
```

---

### Configuration (env)

| Var | Default | Purpose |
|---|---:|---|
| `REDIS_HOST` | `redis` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
| `REDIS_DB` | `0` | Redis DB |
| `REDIS_STREAM` | `foot_traffic` | Base stream name |
| `DEFAULT_DAILY_CUSTOMERS` | `1000` | Baseline per day |
| `BASE_DAILY_CUSTOMERS` | same | Override baseline |
| `DAILY_CUSTOMERS` | _unset_ | Fixed daily total (int) |
| `DAILY_VARIATION_PCT` | `0.10` | ±% daily noise |
| `DISABLE_DAILY_VARIATION` | `0` | `"1"/"true"` disables noise |
| `SEED` | _unset_ | RNG seed |

---

### Why Two Topics?
- `foot_traffic`: simulated informative events for guaranteeing consistency in the streaming process (such that shelf_events and pos_transactions can listen to foot_traffic messages)
- `foot_traffic:realistic`: sensor-like ENTRY/EXIT for realistic analytics and stateful processing

These streams are forwarded to Kafka by `redis-kafka-bridge`.

---

## ./requirements.txt
This file contains:
```
redis
```
---
## ./Dockerfile
A `Dockerfile` is provided to containerize the application. Ensure Docker is installed and configured to build and run the container.
