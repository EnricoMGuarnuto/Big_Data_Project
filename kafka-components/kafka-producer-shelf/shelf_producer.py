import os
import json
import time
import random
import threading
from datetime import datetime, timedelta
from collections import defaultdict

import pandas as pd
import redis
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable

# ========================
# Config
# ========================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_SHELF = os.getenv("KAFKA_TOPIC_SHELF", "shelf_events")
TOPIC_FOOT = os.getenv("KAFKA_TOPIC_FOOT", "foot_traffic")
STORE_PARQUET = os.getenv("STORE_PARQUET", "/data/store_inventory_final.parquet")
SLEEP_SEC = float(os.getenv("SHELF_SLEEP", 1.0))
PUTBACK_PROB = float(os.getenv("PUTBACK_PROB", 0.15))
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

# ========================
# Stato
# ========================
active_customers = {}  # customer_id -> exit_time
scheduled_actions = defaultdict(list)
lock = threading.Lock()

# ========================
# Redis e utils
# ========================
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

def now_utc():
    return datetime.utcnow()

def sample_num_actions():
    r = random.random()
    if r < 0.2:
        return random.randint(3, 6)
    elif r < 0.7:
        return random.randint(7, 15)
    else:
        return random.randint(16, 30)

def generate_scheduled_actions(entry, exit):
    n_actions = sample_num_actions()
    duration = (exit - entry).total_seconds()
    start = entry + timedelta(seconds=60)
    end = exit - timedelta(seconds=30)
    if start >= end:
        return []
    timestamps = sorted([
        start + timedelta(seconds=random.uniform(0, (end - start).total_seconds()))
        for _ in range(n_actions)
    ])
    return [(ts, "pickup" if random.random() > PUTBACK_PROB else "putback") for ts in timestamps]

# ========================
# Kafka
# ========================
def build_producer():
    for attempt in range(6):
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                linger_ms=10,
                retries=10,
                acks="all",
            )
            print("[shelf] Connected to Kafka.")
            return p
        except NoBrokersAvailable:
            print(f"[shelf] Kafka not available (attempt {attempt+1}/6). Retrying...")
            time.sleep(3)
    raise RuntimeError("Kafka not reachable")

def foot_traffic_listener():
    consumer = KafkaConsumer(
        TOPIC_FOOT,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="shelf-foot-listener",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    for msg in consumer:
        evt = msg.value
        if evt.get("event_type") == "foot_traffic":
            cid = evt["customer_id"]
            exit_time = pd.to_datetime(evt["exit_time"]).to_pydatetime()
            entry_time = pd.to_datetime(evt["entry_time"]).to_pydatetime()
            with lock:
                active_customers[cid] = exit_time
                scheduled_actions[cid] = generate_scheduled_actions(entry_time, exit_time)

def reap_inactive():
    while True:
        now = now_utc()
        with lock:
            expired = [cid for cid, et in active_customers.items() if et < now]
            for cid in expired:
                active_customers.pop(cid, None)
                scheduled_actions.pop(cid, None)
        time.sleep(5)

# ========================
# Main loop
# ========================
def main():
    df = pd.read_parquet(STORE_PARQUET)
    required_cols = {"Item_Identifier", "Item_Weight", "Item_Visibility"}
    if not required_cols.issubset(df.columns):
        raise ValueError(f"Parquet must contain columns: {required_cols}")

    # Caricamento sconti da Redis
    def get_discount(item_id):
        try:
            d = float(r.get(f"discount:{item_id}") or 0.0)
            return max(0.0, min(d, 0.95))  # clamp tra 0 e 0.95
        except Exception:
            return 0.0

    df["discount"] = df["Item_Identifier"].map(get_discount)
    df["pick_score"] = df["Item_Visibility"] * (1 + df["discount"])
    pick_weights = df["pick_score"].tolist()

    rng = random.Random()
    producer = build_producer()

    threading.Thread(target=foot_traffic_listener, daemon=True).start()
    threading.Thread(target=reap_inactive, daemon=True).start()
    print("[shelf] Producer started.")

    while True:
        now = now_utc()
        executed = False

        with lock:
            for customer_id, actions in scheduled_actions.items():
                if not actions:
                    continue
                ts, action_type = actions[0]
                if ts <= now:
                    actions.pop(0)

                    idx = rng.choices(range(len(df)), weights=pick_weights, k=1)[0]
                    row = df.iloc[idx]
                    item_id = row["Item_Identifier"]
                    item_weight = float(row["Item_Weight"])
                    quantity = rng.choices([1, 2, 3], weights=[0.6, 0.3, 0.1])[0]
                    total_weight = round(item_weight * quantity, 3)

                    # Evento simulativo
                    sim_event = {
                        "event_type": action_type,
                        "customer_id": customer_id,
                        "item_id": item_id,
                        "weight": item_weight,
                        "quantity": quantity,
                        "timestamp": now.isoformat(),
                    }
                    producer.send(TOPIC_SHELF, value=sim_event)
                    print(f"[shelf] Sent {action_type.upper()}: {sim_event}")

                    # Evento realistico
                    weight_event = {
                        "event_type": "weight_change",
                        "customer_id": customer_id,
                        "item_id": item_id,
                        "delta_weight": (-1 if action_type == "pickup" else 1) * total_weight,
                        "timestamp": now.isoformat(),
                    }
                    producer.send(TOPIC_SHELF, value=weight_event)
                    print(f"[shelf] Sent WEIGHT_CHANGE: {weight_event}")

                    executed = True
                    break  # 1 azione per ciclo

        if not executed:
            time.sleep(SLEEP_SEC)

if __name__ == "__main__":
    main()
