# Foot Traffic Raw Sink

## What it does
- Consumes the simulated `foot_traffic` Kafka topic (sessions with `entry`/`exit` timestamps).
- Normalizes the payload and appends it to the Delta RAW table `/delta/raw/foot_traffic`, keeping a replayable history for downstream analytics.

## Job flow
1. Read the Kafka stream defined by `TOPIC_FOOT_TRAFFIC`.
2. Parse each JSON message, casting the timestamp fields to Spark timestamps and keeping the metadata (`weekday`, `time_slot`, `trip_duration_minutes`).
3. Append the resulting rows into Delta with schema evolution enabled, using `CHECKPOINT` to guarantee exactly-once semantics.

## Key configuration
- `DELTA_ROOT`, `DL_FOOT_TRAFFIC_PATH`: where the Delta table lives (defaults to `/delta/raw/foot_traffic`).
- `KAFKA_BROKER`, `TOPIC_FOOT_TRAFFIC`, `STARTING_OFFSETS`: Kafka connection.
- `CHECKPOINT`: checkpoint directory (default `/delta/_checkpoints/foot_traffic_raw_sink`).
- Simulated time: not used; timestamps come from payloads or Spark `current_timestamp()`.
