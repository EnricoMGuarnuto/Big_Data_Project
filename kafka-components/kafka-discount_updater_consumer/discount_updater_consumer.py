import os, json, time, uuid
from datetime import datetime
from kafka import KafkaConsumer
import psycopg2
import psycopg2.extras

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("DISCOUNT_TOPIC", "weekly_discounts")
GROUP_ID = os.getenv("DISCOUNT_GROUP", "db-discount-consumer")

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "retaildb")
PG_USER = os.getenv("PG_USER", "retail")
PG_PASS = os.getenv("PG_PASS", "retailpass")

def pg_connect():
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
    )
    conn.autocommit = True
    return conn

def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True
    )
    conn = pg_connect()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    print(f"[discount-db] Listening on topic '{TOPIC}'")
    for msg in consumer:
        evt = msg.value or {}
        if evt.get("event_type") != "weekly_discount":
            continue

        evt_id = evt.get("event_id") or f"ds-{uuid.uuid4()}"
        event_ts = evt.get("generated_at", datetime.utcnow().isoformat())
        week = evt.get("week", "")
        payload = json.dumps(evt)

        try:
            cur.execute(
                "INSERT INTO stream_events(event_id, source, event_ts, payload) VALUES (%s, %s, %s, %s)",
                (evt_id, "discount.updater", event_ts, payload)
            )
        except Exception as e:
            print(f"[discount-db] Failed to insert into stream_events: {e}")

        try:
            for item in evt.get("discounts", []):
                cur.execute(
                    "INSERT INTO weekly_discounts(item_id, week, discount, inserted_at) VALUES (%s, %s, %s, %s)",
                    (item["item_id"], week, item["discount"], event_ts)
                )
        except Exception as e:
            print(f"[discount-db] Failed to insert weekly discounts: {e}")

if __name__ == "__main__":
    while True:
        try:
            main()
        except Exception as e:
            print(f"[discount-db] fatal error, retrying in 5s: {e}")
            time.sleep(5)
