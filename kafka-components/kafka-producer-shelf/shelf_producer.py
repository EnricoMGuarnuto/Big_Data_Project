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

# ---- Config ----
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

# ---- Shared state from foot_traffic ----
traffic_events = deque()
traffic_lock = threading.Lock()

def now_utc():
    return datetime.utcnow()

def traffic_level_epm():
    """events per minute in the rolling window, based on *actual* foot_traffic messages consumed"""
    cutoff = now_utc() - timedelta(minutes=TRAFFIC_WINDOW_MIN)
    with traffic_lock:
        while traffic_events and traffic_events[0] < cutoff:
            traffic_events.popleft()
        return len(traffic_events) / max(1, TRAFFIC_WINDOW_MIN)

def foot_traffic_listener():
    """Background consumer: listens to foot_traffic and records timestamps."""
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
    # load once; we keep current_stock in memory for realism (no writes to parquet here)
    df = pd.read_parquet(STORE_PARQUET)
    for col in ("Item_Identifier", "current_stock", "Item_Weight"):
        if col not in df.columns:
            raise ValueError(f"Column '{col}' missing from {STORE_PARQUET}")

    # start foot traffic listener
    t = threading.Thread(target=foot_traffic_listener, daemon=True)
    t.start()

    producer = build_producer()
    rng = random.Random()
    print("[shelf-producer] Started. Reacting to real foot_traffic events.")

    while True:
        tl = traffic_level_epm()  # events/min from real foot_traffic
        sleep_secs = max(SHELF_SLEEP_MIN, SHELF_SLEEP_BASE / (1.0 + ALPHA_RATE * tl))
        pickup_p = min(0.98, max(0.0, PICKUP_BASE_P + PICKUP_BUMP * min(tl, 5.0) / 5.0))

        row = df.sample(1).iloc[0]
        item_id = row["Item_Identifier"]
        cur_stock = int(row["current_stock"])
        item_weight = float(row["Item_Weight"])

        if rng.random() < pickup_p and cur_stock > 0:
            # pickup
            df.loc[df["Item_Identifier"] == item_id, "current_stock"] = cur_stock - 1
            evt = {
                "event_type": "pickup",
                "item_id": item_id,
                "weight": item_weight,
                "timestamp": datetime.utcnow().isoformat(),
                "traffic_level_epm": tl,
            }
            producer.send(TOPIC_SHELF, value=evt)
            print(f"[shelf-producer] Sent: {evt}")

            # occasional putback shortly after
            if rng.random() < PUTBACK_P:
                time.sleep(min(1.5, sleep_secs))
                df.loc[df["Item_Identifier"] == item_id, "current_stock"] = cur_stock
                evt2 = {
                    "event_type": "putback",
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
