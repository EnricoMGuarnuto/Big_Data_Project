# Foot Traffic Producer

## Overview
`foot_traffic_producer.py` is a Kafka producer that simulates store foot traffic in (near) real time.  
It emits:
- **Sessions** → `foot_traffic` (single record with entry & exit).
- **Atomic events** → `foot_traffic_realistic` (separate `entry` / `exit`).

Timestamps are UTC (ISO 8601). Patterns vary by weekday and time slot.

---

## Features
- Realistic **weekday/time-slot** traffic profile  
- Two-topic design: sessions + ENTRY/EXIT  
- Pacing control via `TIME_SCALE`  
- Deterministic runs with `SEED`  
- Retries & graceful shutdown

---

## Event Schemas

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

## Configuration (env)

| Var | Default | Purpose |
|---|---:|---|
| `KAFKA_BROKER` | `kafka:9092` | Bootstrap servers |
| `KAFKA_TOPIC` | `foot_traffic` | Session topic |
| `KAFKA_TOPIC_REALISTIC` | `foot_traffic_realistic` | Atomic topic |
| `SLEEP` | `0.5` | Loop sleep (s) |
| `TIME_SCALE` | `1.0` | Pacing (>1 = faster) |
| `DEFAULT_DAILY_CUSTOMERS` | `1000` | Baseline per day |
| `BASE_DAILY_CUSTOMERS` | same | Override baseline |
| `DAILY_CUSTOMERS` | _unset_ | Fixed daily total (int) |
| `DAILY_VARIATION_PCT` | `0.10` | ±% daily noise |
| `DISABLE_DAILY_VARIATION` | `0` | `"1"/"true"` disables noise |
| `SEED` | _unset_ | RNG seed |
| `MAX_RETRIES` | `6` | Kafka connect retries |
| `RETRY_BACKOFF_SECONDS` | `5.0` | Retry backoff (s) |

> `TIME_SCALE` speeds the loop pacing; event timestamps remain aligned to **real UTC time**.

---

## Run with Docker Compose
Inside the same Docker network:
```yaml
environment:
  KAFKA_BROKER: kafka:9092
```
Start and tail logs:
```bash
docker compose up -d
docker compose logs -f kafka-producer-foot-traffic
```

---

## Build (example)

**Dockerfile**
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY foot_traffic_producer.py .
CMD ["python3", "-u", "foot_traffic_producer.py"]
```

**requirements.txt**
```
kafka-python>=2.0.2
```

**Build & run**
```bash
docker build -t foot-traffic-producer ./kafka-components/kafka-producer-foot_traffic
docker run --rm --network <compose_network> -e KAFKA_BROKER=kafka:9092 foot-traffic-producer
```

---

## Troubleshooting
- **`NoBrokersAvailable`** → check `KAFKA_BROKER` and topics  
- No events? → verify topic names & consumer offsets  
- Time zone → events are **UTC**; convert downstream if needed

---

## Why Two Topics?
- `foot_traffic`: convenient analytics (durations, cohorts)  
- `foot_traffic_realistic`: sensor-like ENTRY/EXIT for streaming/stateful processing
