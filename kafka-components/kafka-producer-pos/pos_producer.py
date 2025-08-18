import os
import json
import uuid
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, DefaultDict
from collections import defaultdict

import pandas as pd
import redis
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

# ========================
# ENV / Config
# ========================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

FOOT_TOPIC   = os.getenv("FOOT_TOPIC", "foot_traffic")
SHELF_TOPIC  = os.getenv("SHELF_TOPIC", "shelf_events")
POS_TOPIC    = os.getenv("POS_TOPIC", "pos_transactions")

GROUP_ID_SHELF = os.getenv("GROUP_ID_SHELF", "pos-simulator-shelf")
GROUP_ID_FOOT  = os.getenv("GROUP_ID_FOOT", "pos-simulator-foot")

# Prezzi dentro lo stesso parquet d'inventario
STORE_PARQUET = os.getenv("STORE_PARQUET", "/data/store_inventory_final.parquet")

# Redis solo per sconti (e fallback estremo)
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB   = int(os.getenv("REDIS_DB", 0))

# Se un cliente non fa pickup, possiamo comunque forzare il checkout
FORCE_CHECKOUT_IF_EMPTY = int(os.getenv("FORCE_CHECKOUT_IF_EMPTY", "0")) == 1

# Safety: guard-rail su sessioni troppo lunghe
MAX_SESSION_AGE_SEC = int(os.getenv("MAX_SESSION_AGE_SEC", str(3 * 60 * 60)))  # 3h

# ========================
# Helpers
# ========================
def load_price_map_from_store(path: str) -> Dict[str, float]:
    """Legge il parquet di inventario e costruisce la mappa Item_Identifier -> price."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Store parquet non trovato: {path}")
    df = pd.read_parquet(path)
    req = {"Item_Identifier", "price"}
    missing = req - set(df.columns)
    if missing:
        raise ValueError(f"{path} manca le colonne {missing} (serve 'price').")
    df = df.dropna(subset=["Item_Identifier", "price"])
    return df.set_index("Item_Identifier")["price"].astype(float).to_dict()

def now_utc() -> datetime:
    return datetime.utcnow()

# ========================
# Stato applicativo
# ========================
# carrelli[customer_id][item_id] = qty
carts: DefaultDict[str, DefaultDict[str, int]] = defaultdict(lambda: defaultdict(int))
carts_lock = threading.Lock()

# timers[customer_id] = threading.Timer(...)
timers: Dict[str, threading.Timer] = {}
timers_lock = threading.Lock()

# memorizziamo anche l'entry_time per guard-rail
entries: Dict[str, datetime] = {}
entries_lock = threading.Lock()

# Prezzi e Redis
try:
    price_by_item = load_price_map_from_store(STORE_PARQUET)
    print(f"[pos] Prezzi caricati da {STORE_PARQUET}, {len(price_by_item)} articoli trovati.")
except Exception as e:
    print(f"[pos] ERRORE caricando i prezzi da {STORE_PARQUET}: {e} — userò fallback 5.0 e sconti da Redis.")
    price_by_item = {}

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

# ========================
# Kafka Producer (per POS)
# ========================
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
            print("[pos] Connected to Kafka as producer.")
            return p
        except NoBrokersAvailable as e:
            last_err = e
            print(f"[pos] Kafka not available (attempt {attempt}/6). Retrying in 3s…")
            time.sleep(3)
    raise RuntimeError(f"Could not connect to Kafka: {last_err}")

producer = build_producer()

# ========================
# Checkout
# ========================
def _price_for(item_id: str) -> float:
    # 1) parquet (preferito); 2) fallback Redis; 3) default 5.0
    p = price_by_item.get(item_id)
    if p is None:
        try:
            p = float(r.get(f"price:{item_id}") or 5.0)
        except Exception:
            p = 5.0
    return float(p)

def _discount_for(item_id: str) -> float:
    try:
        d = float(r.get(f"discount:{item_id}") or 0.0)
    except Exception:
        d = 0.0
    return max(0.0, min(d, 0.95))

def emit_pos_transaction(customer_id: str, timestamp: datetime):
    # snapshot del carrello
    with carts_lock:
        items_map = dict(carts.get(customer_id, {}))

    if not items_map and not FORCE_CHECKOUT_IF_EMPTY:
        print(f"[pos] Checkout customer {customer_id}: carrello vuoto -> nessuna transazione.")
        return

    transaction = {
        "event_type": "pos_transaction",
        "transaction_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "timestamp": timestamp.isoformat(),
        "items": []
    }

    for item_id, qty in items_map.items():
        if qty <= 0:
            continue
        unit_price = round(_price_for(item_id), 2)
        discount   = round(_discount_for(item_id), 2)
        total_price = round(qty * unit_price * (1 - discount), 2)
        transaction["items"].append({
            "item_id": item_id,
            "quantity": int(qty),
            "unit_price": unit_price,
            "discount": discount,
            "total_price": total_price
        })

    if not transaction["items"] and not FORCE_CHECKOUT_IF_EMPTY:
        print(f"[pos] Checkout customer {customer_id}: nessun item valido -> skip.")
        return

    producer.send(POS_TOPIC, value=transaction)
    print(f"[pos] POS transaction emitted: {transaction}")

    # pulizia stato
    with carts_lock:
        carts.pop(customer_id, None)
    with timers_lock:
        t = timers.pop(customer_id, None)
        if t:
            try:
                t.cancel()
            except Exception:
                pass
    with entries_lock:
        entries.pop(customer_id, None)

def schedule_checkout(customer_id: str, exit_time: datetime):
    delay = (exit_time - now_utc()).total_seconds()
    if delay < 0:
        delay = 0.0

    def _checkout_cb():
        emit_pos_transaction(customer_id, timestamp=exit_time)

    with timers_lock:
        old = timers.get(customer_id)
        if old:
            try:
                old.cancel()
            except Exception:
                pass
        t = threading.Timer(delay, _checkout_cb)
        t.daemon = True
        t.start()
        timers[customer_id] = t
        print(f"[pos] Checkout scheduled for {customer_id} in {round(delay, 2)}s.")

# ========================
# Consumers
# ========================
def shelf_consumer_loop():
    consumer = KafkaConsumer(
        SHELF_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID_SHELF,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset='latest',
        enable_auto_commit=True
    )
    print(f"[pos] Listening shelf events on '{SHELF_TOPIC}'")

    for msg in consumer:
        evt = msg.value
        etype = evt.get("event_type")
        customer_id = evt.get("customer_id")
        item_id = evt.get("item_id")

        if etype not in ("pickup", "putback") or not customer_id or not item_id:
            continue

        qty = evt.get("quantity", 1)  # fallback a 1 se non presente (per compatibilità)
        with carts_lock:
            if etype == "pickup":
                carts[customer_id][item_id] += int(qty)
            elif etype == "putback":
                carts[customer_id][item_id] = max(0, carts[customer_id][item_id] - int(qty))


def foot_consumer_loop():
    consumer = KafkaConsumer(
        FOOT_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID_FOOT,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )
    print(f"[pos] Listening foot traffic on '{FOOT_TOPIC}'")

    for msg in consumer:
        evt = msg.value
        if evt.get("event_type") != "foot_traffic":
            continue

        customer_id = evt.get("customer_id")
        entry_time  = datetime.fromisoformat(evt["entry_time"])
        exit_time   = datetime.fromisoformat(evt["exit_time"])

        with entries_lock:
            entries[customer_id] = entry_time

        schedule_checkout(customer_id, exit_time)

def janitor_loop():
    while True:
        time.sleep(30)
        now = now_utc()
        stale = []
        with entries_lock:
            for cid, ent in list(entries.items()):
                if (now - ent).total_seconds() > MAX_SESSION_AGE_SEC:
                    stale.append(cid)
        for cid in stale:
            print(f"[pos] Janitor: forcing checkout for stale customer {cid}")
            emit_pos_transaction(cid, timestamp=now)

# ========================
# Main
# ========================
def main():
    threading.Thread(target=shelf_consumer_loop, daemon=True).start()
    threading.Thread(target=foot_consumer_loop, daemon=True).start()
    threading.Thread(target=janitor_loop, daemon=True).start()

    print("[pos] POS Producer started (building carts from shelf_events, scheduling checkouts).")
    while True:
        time.sleep(3600)

if __name__ == "__main__":
    main()
