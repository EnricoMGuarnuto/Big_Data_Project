import os
import json
import time
import threading
import random
from collections import deque
from datetime import datetime, timedelta

import pandas as pd
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable

# =========================
# Config
# =========================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_SHELF = os.getenv("KAFKA_TOPIC_SHELF", "shelf_events")
TOPIC_FOOT = os.getenv("KAFKA_TOPIC_FOOT", "foot_traffic")

SHELF_SLEEP_BASE = float(os.getenv("SHELF_SLEEP_BASE", 1.0))   # base sleep when traffic = 0
SHELF_SLEEP_MIN  = float(os.getenv("SHELF_SLEEP_MIN", 0.15))   # min sleep at high traffic
PICKUP_BASE_P    = float(os.getenv("PICKUP_BASE_P", 0.70))     # pickup prob at traffic = 0
PICKUP_BUMP      = float(os.getenv("PICKUP_BUMP", 0.20))       # extra pickup prob at high traffic
PUTBACK_P        = float(os.getenv("PUTBACK_P", 0.10))         # chance of putback after a pickup
ALPHA_RATE       = float(os.getenv("ALPHA_RATE", 0.08))        # sleep shrink factor per events/min
TRAFFIC_WINDOW_MIN = int(os.getenv("TRAFFIC_WINDOW_MIN", 5))   # rolling window (minutes)

STORE_PARQUET = os.getenv("STORE_PARQUET", "/data/store_inventory_final.parquet")

# (NB: per ora NON aggiungiamo campi schema_version/store_id/shelf_id: li metteremo più avanti)

# =========================
# Stato condiviso: foot_traffic
# =========================
traffic_events = deque()
traffic_lock = threading.Lock()

# Clienti attivi (in negozio) => usati per associare pickup/putback
ACTIVE_TTL_SEC = int(os.getenv("ACTIVE_TTL_SEC", 2 * 60 * 60))  # sicurezza in caso di ritardi
active_customers = {}  # customer_id -> exit_dt (datetime)
active_lock = threading.Lock()

def now_utc() -> datetime:
    return datetime.utcnow()

def traffic_level_epm() -> float:
    """events per minute nella finestra scorrevole, basato su foot_traffic reali"""
    cutoff = now_utc() - timedelta(minutes=TRAFFIC_WINDOW_MIN)
    with traffic_lock:
        while traffic_events and traffic_events[0] < cutoff:
            traffic_events.popleft()
        return len(traffic_events) / max(1, TRAFFIC_WINDOW_MIN)

def register_customer_session(evt: dict):
    """Registra/aggiorna una sessione cliente 'in-store' a partire dall'evento foot_traffic."""
    try:
        cid = evt["customer_id"]
        exit_dt = pd.to_datetime(evt["exit_time"]).to_pydatetime()
    except Exception:
        return
    with active_lock:
        active_customers[cid] = exit_dt

def reap_inactive():
    """Ripulisce periodicamente i clienti ormai usciti (o stale)."""
    while True:
        now = now_utc()
        with active_lock:
            to_del = [cid for cid, dt in active_customers.items()
                      if dt < now or (now - dt).total_seconds() > ACTIVE_TTL_SEC]
            for cid in to_del:
                active_customers.pop(cid, None)
        time.sleep(5)

def pick_random_active_customer(rng: random.Random):
    """Ritorna un customer_id a caso tra quelli attivi, o None se il negozio è vuoto."""
    with active_lock:
        if not active_customers:
            return None
        return rng.choice(list(active_customers.keys()))

def foot_traffic_listener():
    """Consumer in background: ascolta foot_traffic, registra clienti e aggiorna il livello di traffico."""
    consumer = KafkaConsumer(
        TOPIC_FOOT,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id=os.getenv("FOOT_GROUP_ID", "shelf-producer-foot-listener"),
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    print(f"[shelf-producer] Listening to foot traffic topic: {TOPIC_FOOT}")
    for msg in consumer:
        evt = msg.value
        if evt.get("event_type") == "foot_traffic":
            register_customer_session(evt)  # segna il cliente come "in-store"
            with traffic_lock:
                traffic_events.append(now_utc())

def build_producer():
    last_err = None
    for attempt in range(1, 7):
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                linger_ms=10,
                retries=10,
                acks="all",
            )
            print("[shelf-producer] Connected to Kafka.")
            return p
        except NoBrokersAvailable as e:
            last_err = e
            print(f"[shelf-producer] Kafka not available (attempt {attempt}/6). Retrying in 3s…")
            time.sleep(3)
    raise RuntimeError(f"Could not connect to Kafka: {last_err}")

def main():
    # Carichiamo l'inventario una volta; manteniamo current_stock in RAM (nessun write su parquet qui).
    df = pd.read_parquet(STORE_PARQUET)
    for col in ("Item_Identifier", "current_stock", "Item_Weight"):
        if col not in df.columns:
            raise ValueError(f"Column '{col}' missing from {STORE_PARQUET}")

    # Thread: listener foot_traffic + reaper dei clienti scaduti
    threading.Thread(target=foot_traffic_listener, daemon=True).start()
    threading.Thread(target=reap_inactive, daemon=True).start()

    producer = build_producer()
    rng = random.Random()
    print("[shelf-producer] Started. Reacting to real foot_traffic events and assigning customers.")

    while True:
        # 1) Calcola intensità di traffico e deriva frequenza e probabilità di pickup
        tl = traffic_level_epm()  # events/min da foot_traffic reali
        sleep_secs = max(SHELF_SLEEP_MIN, SHELF_SLEEP_BASE / (1.0 + ALPHA_RATE * tl))
        pickup_p = min(0.98, max(0.0, PICKUP_BASE_P + PICKUP_BUMP * min(tl, 5.0) / 5.0))

        # 2) Se non ci sono clienti attivi, puoi: (a) saltare oppure (b) generare meno eventi.
        customer_id = pick_random_active_customer(rng)
        if customer_id is None:
            time.sleep(sleep_secs)
            continue

        # 3) Scegli un item a caso (vincolato all'inventario)
        row = df.sample(1).iloc[0]
        item_id = row["Item_Identifier"]
        cur_stock = int(row["current_stock"])
        item_weight = float(row["Item_Weight"])

        # 4) Genera pickup / eventuale putback
        if rng.random() < pickup_p and cur_stock > 0:
            # pickup
            df.loc[df["Item_Identifier"] == item_id, "current_stock"] = cur_stock - 1
            evt = {
                "event_type": "pickup",
                "customer_id": customer_id,                     # <<< collegamento al cliente
                "item_id": item_id,
                "weight": item_weight,
                "timestamp": datetime.utcnow().isoformat(),
                "traffic_level_epm": tl,
            }
            producer.send(TOPIC_SHELF, value=evt)
            print(f"[shelf-producer] Sent: {evt}")

            # putback occasionale poco dopo
            if rng.random() < PUTBACK_P:
                time.sleep(min(1.5, sleep_secs))
                # riportiamo lo stock a prima del pickup (cioè +1)
                df.loc[df["Item_Identifier"] == item_id, "current_stock"] = cur_stock
                evt2 = {
                    "event_type": "putback",
                    "customer_id": customer_id,                 # <<< stesso cliente
                    "item_id": item_id,
                    "weight": item_weight,
                    "timestamp": datetime.utcnow().isoformat(),
                    "traffic_level_epm": tl,
                }
                producer.send(TOPIC_SHELF, value=evt2)
                print(f"[shelf-producer] Sent: {evt2}")

        time.sleep(sleep_secs)

if __name__ == "__main__":
    main()
