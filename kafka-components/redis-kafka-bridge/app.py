import logging
import os
import socket
import time
from typing import Dict, List

import redis
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
from kafka.admin import KafkaAdminClient, NewTopic

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s %(message)s')
log = logging.getLogger("redis-kafka-bridge")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

# stream=topic pairs, comma-separated
STREAM_TOPIC_MAP = os.getenv(
    "STREAM_TOPIC_MAP",
    "foot_traffic=foot_traffic,foot_traffic:realistic=foot_traffic_realistic,shelf_events=shelf_events,pos_transactions=pos_transactions",
)

REDIS_GROUP = os.getenv("REDIS_GROUP", "redis-kafka-bridge")
REDIS_CONSUMER = os.getenv("REDIS_CONSUMER", socket.gethostname())
BATCH_COUNT = int(os.getenv("BATCH_COUNT", "128"))
BLOCK_MS = int(os.getenv("BLOCK_MS", "5000"))
MAXLEN_TRIM = int(os.getenv("MAXLEN_TRIM", "0"))

RETRY_SLEEP_SEC = float(os.getenv("RETRY_SLEEP_SEC", "3"))
TOPIC_PARTITIONS = int(os.getenv("TOPIC_PARTITIONS", "3"))
TOPIC_RF = int(os.getenv("TOPIC_RF", "1"))


def parse_stream_topic_map(raw: str) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        if "=" not in token:
            raise ValueError(f"Invalid STREAM_TOPIC_MAP entry: '{token}'")
        stream, topic = token.split("=", 1)
        stream = stream.strip()
        topic = topic.strip()
        if not stream or not topic:
            raise ValueError(f"Invalid STREAM_TOPIC_MAP entry: '{token}'")
        mapping[stream] = topic
    if not mapping:
        raise ValueError("STREAM_TOPIC_MAP resolved to empty mapping")
    return mapping


def build_redis() -> redis.Redis:
    while True:
        try:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
            r.ping()
            log.info(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}")
            return r
        except Exception as e:
            log.warning(f"Redis not available ({e}), retrying in {RETRY_SLEEP_SEC}s")
            time.sleep(RETRY_SLEEP_SEC)


def build_producer() -> KafkaProducer:
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                acks="all",
                retries=10,
                linger_ms=5,
                compression_type="gzip",
                max_in_flight_requests_per_connection=1,
            )
            log.info(f"Connected to Kafka at {KAFKA_BROKER}")
            return producer
        except (NoBrokersAvailable, Exception) as e:
            log.warning(f"Kafka not available ({e}), retrying in {RETRY_SLEEP_SEC}s")
            time.sleep(RETRY_SLEEP_SEC)


def ensure_kafka_topics(topics: List[str]) -> None:
    unique_topics = sorted(set(t for t in topics if t))
    while True:
        admin = None
        try:
            admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER, client_id="redis-kafka-bridge-init")
            existing = set(admin.list_topics())
            to_create = [
                NewTopic(name=t, num_partitions=TOPIC_PARTITIONS, replication_factor=TOPIC_RF)
                for t in unique_topics
                if t not in existing
            ]
            if to_create:
                admin.create_topics(to_create, validate_only=False)
                log.info("Created missing Kafka topic(s): %s", ", ".join(t.name for t in to_create))
            else:
                log.info("All mapped Kafka topics already exist.")
            return
        except Exception as e:
            log.warning(f"Cannot ensure Kafka topics ({e}), retrying in {RETRY_SLEEP_SEC}s")
            time.sleep(RETRY_SLEEP_SEC)
        finally:
            if admin is not None:
                try:
                    admin.close()
                except Exception:
                    pass


def ensure_groups(rconn: redis.Redis, streams: List[str], group: str) -> None:
    for stream in streams:
        try:
            rconn.xgroup_create(name=stream, groupname=group, id="0", mkstream=True)
            log.info(f"Created Redis consumer group '{group}' on stream '{stream}'")
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" in str(e):
                log.info(f"Redis consumer group '{group}' already exists on '{stream}'")
            else:
                raise


def read_batch(rconn: redis.Redis, streams: Dict[str, str], count: int, block_ms: int):
    # Read new messages first to avoid starvation on a single poison pending entry.
    fresh = rconn.xreadgroup(
        groupname=REDIS_GROUP,
        consumername=REDIS_CONSUMER,
        streams={s: ">" for s in streams},
        count=count,
        block=block_ms,
    )
    if fresh:
        return fresh

    # Opportunistically try pending entries assigned to this consumer.
    return rconn.xreadgroup(
        groupname=REDIS_GROUP,
        consumername=REDIS_CONSUMER,
        streams={s: "0" for s in streams},
        count=count,
        block=1,
    )


def publish_and_ack(
    rconn: redis.Redis,
    producer: KafkaProducer,
    stream_to_topic: Dict[str, str],
    stream: str,
    message_id: str,
    fields: Dict[str, str],
) -> None:
    topic = stream_to_topic[stream]
    payload = fields.get("data")
    if payload is None:
        log.warning(f"Skipping message without 'data' field: stream={stream}, id={message_id}")
        rconn.xack(stream, REDIS_GROUP, message_id)
        return

    key = fields.get("key")
    key_bytes = key.encode("utf-8") if key else None
    value_bytes = payload.encode("utf-8")

    try:
        producer.send(topic, key=key_bytes, value=value_bytes).get(timeout=15)
    except KafkaError:
        raise

    rconn.xack(stream, REDIS_GROUP, message_id)

    if MAXLEN_TRIM > 0:
        try:
            rconn.xtrim(stream, maxlen=MAXLEN_TRIM, approximate=True)
        except Exception as e:
            log.warning(f"xtrim failed on {stream}: {e}")


def run_loop() -> None:
    stream_to_topic = parse_stream_topic_map(STREAM_TOPIC_MAP)
    streams = list(stream_to_topic.keys())
    topics = list(stream_to_topic.values())

    rconn = build_redis()
    ensure_kafka_topics(topics)
    producer = build_producer()
    ensure_groups(rconn, streams, REDIS_GROUP)

    log.info(
        "Bridge started with mapping: %s",
        ", ".join(f"{s}->{t}" for s, t in stream_to_topic.items()),
    )

    while True:
        try:
            batches = read_batch(rconn, stream_to_topic, BATCH_COUNT, BLOCK_MS)
            if not batches:
                continue

            forwarded = 0
            for stream, messages in batches:
                for message_id, fields in messages:
                    try:
                        publish_and_ack(rconn, producer, stream_to_topic, stream, message_id, fields)
                        forwarded += 1
                    except Exception as e:
                        # Keep the message pending (no XACK) for later inspection/retry,
                        # but continue forwarding other messages.
                        log.exception(
                            "Message forward failed (stream=%s id=%s): %s",
                            stream,
                            message_id,
                            e,
                        )

            if forwarded > 0:
                log.info(f"Forwarded {forwarded} message(s) Redis -> Kafka")

        except KeyboardInterrupt:
            raise
        except Exception as e:
            log.exception(f"Bridge loop error: {e}. Reconnecting...")
            try:
                producer.close()
            except Exception:
                pass
            time.sleep(RETRY_SLEEP_SEC)
            rconn = build_redis()
            producer = build_producer()
            ensure_groups(rconn, streams, REDIS_GROUP)


def main() -> None:
    run_loop()


if __name__ == "__main__":
    main()
