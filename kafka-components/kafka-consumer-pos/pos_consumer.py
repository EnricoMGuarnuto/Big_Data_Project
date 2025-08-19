import os, json, time, uuid
from datetime import datetime
from decimal import Decimal
from kafka import KafkaConsumer
import psycopg2
import psycopg2.extras

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("POS_TOPIC", "pos_transactions")
GROUP_ID = os.getenv("GROUP_ID", "db-pos-consumer")

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

    print(f"[pos-db] Listening on topic '{TOPIC}'")
    for msg in consumer:
        evt = msg.value or {}
        if evt.get("event_type") != "pos_transaction":
            continue

        tx_id = evt.get("transaction_id") or f"tx-{uuid.uuid4()}"
        customer_id = evt.get("customer_id")
        ts = to_ts(evt.get("timestamp"))
        bdate = ts.date()
        items = evt.get("items", [])

        # calcolo totale lordo partendo dalle righe
        total_gross = Decimal("0.00")
        for it in items:
            try:
                total_gross += Decimal(str(it.get("total_price", 0)))
            except Exception:
                pass

        # upsert "morbido": receipts unique su transaction_id
        try:
            cur.execute(
                """
                INSERT INTO receipts(transaction_id, customer_id, business_date, closed_at, total_gross, status)
                VALUES (%s,%s,%s,%s,%s,'CLOSED')
                ON CONFLICT (transaction_id) DO NOTHING
                """,
                (tx_id, customer_id, bdate, ts, total_gross)
            )
        except Exception as e:
            print(f"[pos-db] WARN receipts insert: {e}")

        # prendo receipt_id
        cur.execute("SELECT receipt_id FROM receipts WHERE transaction_id=%s", (tx_id,))
        row = cur.fetchone()
        if not row:
            continue
        receipt_id = row[0]

        # inserisco righe + applico sale_event (idempotente su inventory_ledger.event_id)
        for idx, it in enumerate(items, start=1):
            shelf_id = it.get("item_id") or it.get("shelf_id")   # producer usa item_id = shelf_id
            qty = int(it.get("quantity", 0) or 0)
            unit_price = Decimal(str(it.get("unit_price", 0)))
            discount   = Decimal(str(it.get("discount", 0)))
            total_price= Decimal(str(it.get("total_price", 0)))

            if not shelf_id or qty <= 0:
                continue

            try:
                cur.execute(
                    """
                    INSERT INTO receipt_lines(receipt_id, shelf_id, quantity, unit_price, discount, total_price)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    """,
                    (receipt_id, shelf_id, qty, unit_price, discount, total_price)
                )
            except Exception as e:
                print(f"[pos-db] WARN insert line: {e}")

            # ledger, FEFO, stock ufficiale
            ev_id = f"{tx_id}-line-{idx}"
            try:
                cur.execute(
                    "SELECT apply_sale_event(%s,%s,%s,%s,%s::jsonb)",
                    (ev_id, ts, shelf_id, qty, json.dumps({"receipt_id": receipt_id}))
                )
            except Exception as e:
                print(f"[pos-db] ERROR apply_sale_event: {e}")

        # log completo evento crudo (utile per audit)
        try:
            cur.execute(
                "INSERT INTO stream_events(event_id,source,event_ts,payload) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (f"pos-{tx_id}", "pos.sales", ts, json.dumps(evt))
            )
        except Exception:
            pass

if __name__ == "__main__":
    while True:
        try:
            main()
        except Exception as e:
            print(f"[pos-db] fatal error, retrying in 5s: {e}")
            time.sleep(5)
