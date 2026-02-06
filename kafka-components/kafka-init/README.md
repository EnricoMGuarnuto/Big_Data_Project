# Kafka init

Small bootstrap utility that creates Kafka topics and applies baseline configs
for retention and compaction. It runs once at startup as part of the
`docker-compose.yml` stack.

## What it does
- Connects to the broker via `kafka-python`.
- Creates two groups of topics:
  - Append-only event streams (retention-based).
  - Compacted topics for state/metadata (compaction + delete retention).
- If a topic already exists, it attempts to align its config via `alter_configs`.

## Topics created
Append-only:
- `shelf_events`, `pos_transactions`, `foot_traffic`, `foot_traffic_realistic`, `wh_events`, `alerts`

Compacted (state + metadata):
- `shelf_state`, `wh_state`, `shelf_batch_state`, `wh_batch_state`
- `daily_discounts`, `shelf_restock_plan`, `wh_supplier_plan`
- `shelf_policies`, `wh_policies`, `batch_catalog`, `shelf_profiles`

## Configuration (env vars)
- `KAFKA_BROKER` (default `kafka:9092`)
- `DEFAULT_PARTITIONS` (default `3`)
- `DEFAULT_RF` (default `1`)
- `APPEND_RETENTION_MS` (default `7 days`)
- `COMPACT_DELETE_RETENTION_MS` (default `1 day`)

## Run locally
```bash
pip install -r kafka-components/kafka-init/requirements.txt
python kafka-components/kafka-init/init.py
```
