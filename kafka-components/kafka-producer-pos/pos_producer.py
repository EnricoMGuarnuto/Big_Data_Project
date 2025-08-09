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

# Prezzi: parquet o csv con colonne: Item_Identifier, price
PRICE_FILE = os.getenv("PRICE_FILE", "/data/item_prices.parquet")

# Redis per sconti (e fallback prezzi)
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB   = int(os.getenv("REDIS_DB", 0))

# Se un cliente non fa pickup, possiamo comunque forzare il checkout
# (per evitare timer zombie). Metti a 0 per disabilitare.
FORCE_CHECKOUT_IF_EMPTY = int(os.getenv("FORCE_CHECKOUT_IF_EMPTY", "0")) == 1

# Safety: se per qualche motivo un checkout non parte, facciamo un guard rail.
MAX_SESSION_AGE_SEC = int(os.getenv("MAX_SESSION_AGE_SEC", str(3 * 60 * 60)))  # 3h

# ========================
# Helpers
# ========================

#==============
# logica presente solo perchè per adesso non abbiamo veramente i prezzi
# Prezzi e Redis
import os

if os.path.exists(PRICE_FILE):
    try:
        price_by_item = load_price_map(PRICE_FILE)
        print(f"[pos] Prezzi caricati da {PRICE_FILE}, {len(price_by_item)} articoli trovati.")
    except Exception as e:
        print(f"[pos] Errore nel caricamento del file prezzi: {e} — uso solo Redis/default.")
        price_by_item = {}
else:
    print(f"[pos] File prezzi {PRICE_FILE} non trovato — uso solo Redis/default.")
    price_by_item = {}

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

#=================

def load_price_map(path: str) -> Dict[str, float]:
    ext = os.path.splitext(path)[1].lower()
    if ext == ".parquet":
        df = pd.read_parquet(path)
    elif ext == ".csv":
        df = pd.read_csv(path)
    else:
        raise ValueError(f"Formato file prezzi non supportato: {ext} (usa .parquet o .csv)")

    if "Item_Identifier" not in df.columns or "price" not in df.columns:
        raise ValueError("Il file prezzi deve avere le colonne 'Item_Identifier' e 'price'")

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

# pianificazioni di checkout
# timers[customer_id] = threading.Timer(...)
timers: Dict[str, threading.Timer] = {}
timers_lock = threading.Lock()

# memorizziamo anche l'entry_time per guard-rail
entries: Dict[str, datetime] = {}
entries_lock = threading.Lock()

# Prezzi e Redis
price_by_item = load_price_map(PRICE_FILE)
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
    p = price_by_item.get(item_id)
    if p is None:
        # fallback: prova su Redis (price:<item_id>), poi default 5.0
        p = float(r.get(f"price:{item_id}") or 5.0)
    return float(p)

def _discount_for(item_id: str) -> float:
    d = float(r.get(f"discount:{item_id}") or 0.0)
    # clamp sicurezza
    return max(0.0, min(d, 0.95))

def emit_pos_transaction(customer_id: str, timestamp: datetime):
    # snapshot del carrello
    with carts_lock:
        items_map = dict(carts.get(customer_id, {}))

    if not items_map and not FORCE_CHECKOUT_IF_EMPTY:
        # niente da emettere
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

    # se ancora vuoto (es. tutti qty <= 0), e non forzato, esci
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
        # se esiste già un timer (es. aggiornamento), cancella e rimpiazza
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

        with carts_lock:
            if etype == "pickup":
                carts[customer_id][item_id] += 1
            elif etype == "putback":
                # non andare sotto zero
                carts[customer_id][item_id] = max(0, carts[customer_id][item_id] - 1)

        # (opzionale) log sintetico:
        # print(f"[pos] {etype} customer={customer_id} item={item_id} cart={dict(carts[customer_id])}")

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

        # registra entry (per safety/garbage collection)
        with entries_lock:
            entries[customer_id] = entry_time

        # programma il checkout all'uscita
        schedule_checkout(customer_id, exit_time)

def janitor_loop():
    """Garbage collector di sicurezza per sessioni troppo vecchie (mai chiuse)."""
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
    # thread: consumer shelf, consumer foot, janitor
    threading.Thread(target=shelf_consumer_loop, daemon=True).start()
    threading.Thread(target=foot_consumer_loop, daemon=True).start()
    threading.Thread(target=janitor_loop, daemon=True).start()

    print("[pos] POS Producer started (building carts from shelf_events, scheduling checkouts).")
    # tieni vivo il main thread
    while True:
        time.sleep(3600)

if __name__ == "__main__":
    main()
