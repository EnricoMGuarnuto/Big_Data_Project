# Alerts Sink

## What it does
- Consumes alert events from the Kafka topic configured via `TOPIC_ALERTS`.
- Parses the JSON payloads with the schema used by the shelf/warehouse alert engines.
- Persists each micro‑batch both to Delta Lake (`DL_ALERTS_PATH`) and, if JDBC details are provided, to the Postgres table specified by `PG_TABLE`.

## Spark job steps
1. Start a Delta‑enabled Spark session and build a streaming read on Kafka (`KAFKA_BROKER`, `STARTING_OFFSETS`).
2. Deserialize the Kafka `value` column into the alert schema, defaulting missing `created_at` timestamps to `current_timestamp()`.
3. For every micro‑batch:
   - Append the alerts to the Delta Lake table (merge schema enabled).
   - When `WRITE_TO_PG=1` and JDBC credentials are set, project the alert columns needed by Postgres and append them through the JDBC sink.

## Key configuration
- `DL_ALERTS_PATH` / `CHECKPOINT`: Delta location for the alert log and streaming checkpoint.
- `WRITE_TO_PG`, `JDBC_PG_URL`, `JDBC_PG_USER`, `JDBC_PG_PASSWORD`, `PG_TABLE`: toggle and parameters for the optional Postgres sink.
- Simulated time: not used; timestamps come from payloads or Spark `current_timestamp()`.
