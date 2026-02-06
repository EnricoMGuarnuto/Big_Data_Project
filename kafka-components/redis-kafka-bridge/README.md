# Redis Kafka Bridge

This service implements the buffering path `Redis Streams -> Kafka`.

- Reads events from Redis Streams using a consumer group (`XREADGROUP`).
- Publishes the payload to Kafka topics.
- Acks Redis messages (`XACK`) only after Kafka send success.

## Environment variables

- `REDIS_HOST` (default `redis`)
- `REDIS_PORT` (default `6379`)
- `REDIS_DB` (default `0`)
- `KAFKA_BROKER` (default `kafka:9092`)
- `STREAM_TOPIC_MAP`
  - default:
    `foot_traffic=foot_traffic,foot_traffic:realistic=foot_traffic_realistic,shelf_events=shelf_events,pos_transactions=pos_transactions`
- `REDIS_GROUP` (default `redis-kafka-bridge`)
- `REDIS_CONSUMER` (default container hostname)
- `BATCH_COUNT` (default `128`)
- `BLOCK_MS` (default `5000`)
- `RETRY_SLEEP_SEC` (default `3`)

## Notes

- Producers should write only to Redis Streams.
- Kafka receives events only from this bridge service.
