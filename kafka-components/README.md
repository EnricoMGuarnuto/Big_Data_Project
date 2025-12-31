# Kafka components

This folder contains the Kafka-related building blocks of the project:
topic initialization, Kafka Connect setup, and the event producers. Each
subfolder has its own README with the detailed configuration and runtime notes.

## Subfolders
- `kafka-init/`: creates Kafka topics (append-only and compacted) at startup.
- `kafka-connect/`: Kafka Connect configuration and connector bootstrap.
- `kafka-producer-foot_traffic/`: emits foot traffic events to Kafka.
- `kafka-producer-shelf/`: emits shelf sensor events (pickup/putback/weight).
- `kafka-producer-pos/`: emits POS transactions to Kafka.

Use `docker-compose.yml` at the repo root to run these services as part of the
full stack.
