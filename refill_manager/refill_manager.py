import psycopg2
from datetime import datetime
import uuid
import time
import os
from kafka import KafkaProducer
import json
from kafka.errors import NoBrokersAvailable

# PostgreSQL config
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB   = os.getenv("PG_DB", "retaildb")
PG_USER = os.getenv("PG_USER", "retail")
PG_PASS = os.getenv("PG_PASS", "retailpass")

# Kafka config
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("TOPIC_SHELF", "shelf_events")

def pg_connect():
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        dbname=PG_DB, user=PG_USER, password=PG_PASS
    )
    conn.autocommit = True
    return conn

def build_producer():
    last_err = None
    for attempt in range(1, 7):
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                linger_ms=5,
                acks="all",
                retries=10,
            )
            print("[refill-manager] Connected to Kafka.")
            return p
        except NoBrokersAvailable as e:
            last_err = e
            print(f"[refill-manager] Kafka not available (attempt {attempt}/6). Retrying in 3s…")
            time.sleep(3)
    raise RuntimeError(f"[refill-manager] Could not connect to Kafka: {last_err}")

producer = build_producer()


def perform_refills():
    conn = pg_connect()
    cur = conn.cursor()

    cur.execute("""
        SELECT DISTINCT item_id, location_id
        FROM alerts
        WHERE status = 'OPEN' AND rule_key = 'low_stock'
    """)
    alerts = cur.fetchall()

    for item_id, loc_id in alerts:
        cur.execute("SELECT shelf_id FROM items WHERE item_id = %s", (item_id,))
        res = cur.fetchone()
        if not res:
            continue
        shelf_id = res[0]

        # Get stock info
        cur.execute("""
            SELECT current_stock, unit_weight
            FROM product_inventory
            WHERE item_id = %s AND location_id = %s
        """, (item_id, loc_id))
        stock_res = cur.fetchone()
        if not stock_res:
            continue
        current_stock, unit_weight = stock_res

        # Get threshold
        cur.execute("""
            SELECT get_low_stock_threshold(%s, %s)
        """, (item_id, loc_id))
        threshold = cur.fetchone()[0] or 0

        # Compute refill qty with buffer
        buffer = 5
        refill_qty = max(threshold + buffer - current_stock, 0)

        if refill_qty <= 0:
            continue

        event_id = f"rf-{uuid.uuid4()}"
        event_ts = datetime.utcnow()

        print(f"[refill-manager] Triggering refill: {shelf_id}, qty: {refill_qty}")

        try:
            cur.execute(
                "SELECT apply_refill_event(%s, %s, %s, %s)",
                (event_id, event_ts, shelf_id, refill_qty)
            )
        except Exception as e:
            print(f"[refill-manager] ERROR apply_refill_event: {e}")
            continue

        # Simulate sensor message to Kafka
        refill_event = {
            "event_id": f"sens-{uuid.uuid4()}",
            "event_time": event_ts.isoformat(),
            "shelf_id": shelf_id,
            "weight_change": refill_qty * (unit_weight or 100),
            "is_refill": True,
            "event_type": "weight_change",
            "meta": {
                "source": "refill-manager",
                "is_refill": True
            }
        }
        producer.send(TOPIC, value=refill_event)
        print(f"[refill-manager] Sent refill event to Kafka for {shelf_id}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    print("[refill-manager] Started.")
    while True:
        perform_refills()
        time.sleep(30)
