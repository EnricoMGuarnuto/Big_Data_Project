# aggiunte al near_expiry_discount_producer.py
import time, os, json
import psycopg2
from psycopg2.extras import RealDictCursor
from kafka import KafkaProducer
from datetime import datetime, timezone

DB_DSN         = os.getenv("DB_DSN", "postgresql://user:pass@postgres:5432/retail")
KAFKA_BROKER   = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC          = os.getenv("NEAR_EXPIRY_TOPIC", "near_expiry_discounts")
SLEEP_SEC      = int(os.getenv("NEAR_EXPIRY_PERIOD_SEC", "3600"))

SCHEMA_READY_SQL = """
SELECT
  to_regclass('public.batches') IS NOT NULL AND
  to_regclass('public.batch_inventory') IS NOT NULL AND
  to_regclass('public.inventory_thresholds') IS NOT NULL AND
  to_regclass('public.items') IS NOT NULL AND
  to_regclass('public.alerts') IS NOT NULL AND
  EXISTS (SELECT 1 FROM pg_proc WHERE proname='list_near_expiry_discounts')
AS ready;
"""

def wait_for_postgres(max_attempts=120, sleep_s=3):
    last = None
    for i in range(1, max_attempts+1):
        try:
            with psycopg2.connect(DB_DSN) as conn, conn.cursor() as cur:
                cur.execute("SELECT 1")
            print(f"[near-expiry] DB ok (tentativo {i})")
            return
        except Exception as e:
            last = e
            print(f"[near-expiry] DB non pronto ({i}/{max_attempts}): {e}")
            time.sleep(sleep_s)
    raise RuntimeError(f"[near-expiry] DB mai pronto: {last}")

def wait_for_schema(max_attempts=120, sleep_s=3):
    for i in range(1, max_attempts+1):
        try:
            with psycopg2.connect(DB_DSN) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(SCHEMA_READY_SQL)
                if cur.fetchone()["ready"]:
                    print(f"[near-expiry] Schema/funzioni ok")
                    return
                print(f"[near-expiry] Schema non pronto ({i}/{max_attempts})")
        except Exception as e:
            print(f"[near-expiry] Errore check schema: {e}")
        time.sleep(sleep_s)
    raise RuntimeError("[near-expiry] Schema non disponibile")

def build_producer():
    for i in range(10):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all", linger_ms=10, retries=5
            )
        except Exception as e:
            print(f"[near-expiry] Kafka non disponibile ({i+1}/10): {e}")
            time.sleep(5)
    raise RuntimeError("[near-expiry] Kafka mai pronto")

SQL_FETCH = """
SELECT location_id, batch_id, item_id, expiry_date, discount_pct,
       valid_from, valid_to, kind, reason
FROM list_near_expiry_discounts(now(), '22:00'::timetz);
"""

def fetch_discounts():
    with psycopg2.connect(DB_DSN) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(SQL_FETCH)
        rows = cur.fetchall()
    discounts = []
    for r in rows:
        discounts.append({
            "kind": r["kind"],
            "item_id": str(r["item_id"]),
            "batch_id": r["batch_id"],
            "location_id": r["location_id"],
            "discount": float(r["discount_pct"]),
            "valid_from": r["valid_from"].isoformat(),
            "valid_to": r["valid_to"].isoformat(),
            "expiry_date": r["expiry_date"].isoformat(),
            "reason": r["reason"]
        })
    return discounts

def key_for(d):
    # chiave deterministica -> topic compatto, 1 record “ultimo” per (batch,location)
    return f"{d['kind']}:{d['batch_id']}:{d['location_id']}".encode()

def main_loop():
    wait_for_postgres()
    wait_for_schema()
    producer = build_producer()
    while True:
        try:
            ds = fetch_discounts()
            now = datetime.now(timezone.utc).isoformat()
            if ds:
                for d in ds:
                    evt = {
                        "event_type": "discounts",
                        "source": "near_expiry-engine",
                        "created_at": now,
                        "discounts": [d]
                    }
                    producer.send(TOPIC, key=key_for(d), value=evt)
                producer.flush()
                print(f"[near-expiry] pubblicati {len(ds)} sconti")
            else:
                # nessun dato: non è un errore. Lascia il loop vivo.
                print("[near-expiry] nessun near-expiry al momento")
        except Exception as e:
            print(f"[near-expiry] errore ciclo: {e}")
        time.sleep(SLEEP_SEC)

if __name__ == "__main__":
    main_loop()
