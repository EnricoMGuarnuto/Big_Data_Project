import os, json, time, uuid
from datetime import datetime
from kafka import KafkaConsumer
import psycopg2
import psycopg2.extras

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("TOPIC_SHELF", "shelf_events")
GROUP_ID = os.getenv("GROUP_ID", "db-shelf-consumer")

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

def to_ts(s: str):
    return datetime.fromisoformat(s.replace("Z",""))

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

    print(f"[shelf-db] Listening on topic '{TOPIC}'")
    for msg in consumer:
        evt = msg.value or {}
        etype = evt.get("event_type")
        customer_id = evt.get("customer_id")
        shelf_id = evt.get("item_id") or evt.get("shelf_id")  # producer usa item_id= shelf_id
        ts_str = evt.get("timestamp")
        if not ts_str or not shelf_id:
            continue
        event_ts = to_ts(ts_str)
        event_id = f"sw-{uuid.uuid4()}"

        # Log in stream_events (idempotenza a livello di event_id generato qui)
        try:
            cur.execute(
                "INSERT INTO stream_events(event_id,source,event_ts,payload) VALUES (%s,%s,%s,%s)",
                (event_id, "shelf.sensors", event_ts, json.dumps(evt))
            )
        except Exception:
            # già inserito? poco male
            pass

        if etype == "weight_change":
            delta = float(evt.get("delta_weight", 0))
            # chiama la funzione dominio (usa peso unitario da product_inventory instore)
            try:
                cur.execute(
                    "SELECT apply_shelf_weight_event(%s,%s,%s,%s,%s,%s::jsonb,%s)",
                    (event_id, event_ts, shelf_id, delta, False, json.dumps({"customer_id":customer_id}), 0.25)
                )
            except Exception as e:
                print(f"[shelf-db] ERROR apply_shelf_weight_event: {e}")
        else:
            # eventi 'pickup/putback' simulativi: li ignoriamo per lo stock DB, ma restano loggati
            pass

if __name__ == "__main__":
    while True:
        try:
            main()
        except Exception as e:
            print(f"[shelf-db] fatal error, retrying in 5s: {e}")
            time.sleep(5)
