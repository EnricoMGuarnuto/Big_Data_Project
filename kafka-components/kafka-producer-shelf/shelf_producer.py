import os
import json
import time
import random
import threading
import redis
import logging
import psycopg2
import pandas as pd
from kafka import KafkaConsumer
from datetime import datetime, timedelta, timezone
from collections import defaultdict

from simulated_time.redis_clock import RedisClock

# ========================
# Logging
# ========================
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s %(message)s')
log = logging.getLogger("shelf")

# ========================
# Config
# ========================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_SHELF = os.getenv("KAFKA_TOPIC_SHELF", "shelf_events")
TOPIC_FOOT  = os.getenv("KAFKA_TOPIC_FOOT", "foot_traffic")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB   = int(os.getenv("REDIS_DB", "0"))
REDIS_STREAM = os.getenv("REDIS_STREAM", "shelf_events")

STORE_PARQUET = os.getenv("STORE_PARQUET", "/data/store_inventory_final.parquet")
STORE_CSV_PATH = os.getenv("STORE_CSV_PATH", "/data/db_csv/store_inventory_final.csv")
DISCOUNT_PARQUET_PATH = os.getenv("DISCOUNT_PARQUET_PATH", "/data/all_discounts.parquet")
SLEEP_SEC = float(os.getenv("SHELF_IDLE_SLEEP", "0.01"))
PUTBACK_PROB = float(os.getenv("PUTBACK_PROB", 0.15))
STORE_FILE_RETRY_SECONDS = float(os.getenv("STORE_FILE_RETRY_SECONDS", "10"))

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "smart_shelf")
PG_USER = os.getenv("PG_USER", "bdt_user")
PG_PASS = os.getenv("PG_PASS", "bdt_password")
DAILY_DISCOUNT_TABLE = os.getenv("DAILY_DISCOUNT_TABLE", "analytics.daily_discounts")

# ========================
# Simulated Time
# ========================
clock = RedisClock(host=REDIS_HOST, port=REDIS_PORT)
def now():
    return clock.now()

# ========================
# State
# ========================
customer_carts = defaultdict(lambda: defaultdict(lambda: {"quantity": 0, "weight": 0.0}))
active_customers = {}
scheduled_actions = defaultdict(list)
lock = threading.Lock()

# ========================
# Utility functions
# ========================
def sample_num_actions():
    r = random.random()
    return random.randint(3, 6) if r < 0.2 else random.randint(7, 15) if r < 0.7 else random.randint(16, 30)

def generate_scheduled_actions(entry, exit):
    n = sample_num_actions()
    start = entry + timedelta(seconds=60)
    end = exit - timedelta(seconds=30)
    if start >= end:
        return []
    timestamps = sorted([
        start + timedelta(seconds=random.uniform(0, (end - start).total_seconds()))
        for _ in range(n)
    ])
    return [(ts, "pickup" if random.random() > PUTBACK_PROB else "putback") for ts in timestamps]

def load_discounts_from_parquet(path):
    try:
        df = pd.read_parquet(path)
        week_str = f"{now().isocalendar()[0]}-W{now().isocalendar()[1]:02}"
        df = df[df["week"] == week_str]
        return dict(zip(df["shelf_id"], df["discount"]))
    except Exception as e:
        log.warning(f"[shelf] Error loading weekly discounts: {e}")
        return {}

def load_daily_discounts():
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
        )
        cur = conn.cursor()
        cur.execute(f"SELECT shelf_id, discount FROM {DAILY_DISCOUNT_TABLE} WHERE discount_date = %s", (now().date(),))
        rows = cur.fetchall()
        cur.close(); conn.close()
        return {sid: float(d) for sid, d in rows}
    except Exception as e:
        log.warning(f"[shelf] Error loading daily discounts: {e}")
        return {}


def load_store_inventory() -> pd.DataFrame:
    required = {"shelf_id", "item_weight", "item_visibility"}

    while True:
        try:
            if os.path.exists(STORE_PARQUET):
                df = pd.read_parquet(STORE_PARQUET)
                src = STORE_PARQUET
            elif os.path.exists(STORE_CSV_PATH):
                df = pd.read_csv(STORE_CSV_PATH)
                src = STORE_CSV_PATH
            else:
                raise FileNotFoundError(
                    f"Neither STORE_PARQUET ({STORE_PARQUET}) nor STORE_CSV_PATH ({STORE_CSV_PATH}) exists"
                )

            missing = required - set(df.columns)
            if missing:
                raise ValueError(f"{src} missing required columns: {missing}")

            log.info(f"[shelf] Loaded inventory rows={len(df)} from {src}")
            return df
        except Exception as e:
            log.error(f"[shelf] Cannot load inventory dataset: {e}")
            log.error(
                "[shelf] Waiting for inventory file. "
                f"Expected parquet={STORE_PARQUET} or csv={STORE_CSV_PATH}. "
                f"Retry in {STORE_FILE_RETRY_SECONDS}s."
            )
            time.sleep(STORE_FILE_RETRY_SECONDS)

# ========================
# Kafka / Redis
# ========================
def build_redis():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    r.ping()
    return r

def emit(rconn, event):
    try:
        rconn.xadd(
            REDIS_STREAM,
            {"data": json.dumps(event), "key": event.get("customer_id", "")},
            maxlen=20000,
            approximate=True,
        )
    except Exception as e:
        log.warning(f"[shelf] Redis XADD failed: {e}")

# ========================
# Background threads
# ========================
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
            entry = datetime.fromisoformat(evt["entry_time"])
            exit = datetime.fromisoformat(evt["exit_time"])
            with lock:
                active_customers[cid] = exit
                scheduled_actions[cid] = generate_scheduled_actions(entry, exit)

def reap_inactive():
    while True:
        current = now()
        with lock:
            expired = [cid for cid, et in active_customers.items() if et < current]
            for cid in expired:
                active_customers.pop(cid, None)
                scheduled_actions.pop(cid, None)
                customer_carts.pop(cid, None)
        time.sleep(5)

# ========================
# Main Loop
# ========================
def main():
    df = load_store_inventory()
    weekly = load_discounts_from_parquet(DISCOUNT_PARQUET_PATH)
    daily = load_daily_discounts()

    def discount(sid):
        dw = float(weekly.get(sid, 0.0))
        dd = float(daily.get(sid, 0.0))
        return max(0.0, min(1 - (1 - dw) * (1 - dd), 0.95))

    df["discount"] = df["shelf_id"].map(discount)
    df["pick_score"] = df["item_visibility"] * (1 + df["discount"])
    pick_weights = df["pick_score"].tolist()

    rng = random.Random()
    rconn = build_redis()

    threading.Thread(target=foot_traffic_listener, daemon=True).start()
    threading.Thread(target=reap_inactive, daemon=True).start()
    log.info("[shelf] Producer started (SIMULATED TIME)")

    while True:
        current_time = now()
        executed = False

        with lock:
            for cid, actions in list(scheduled_actions.items()):
                if not actions:
                    continue
                ts, action = actions[0]
                if ts <= current_time:
                    actions.pop(0)

                    if action == "pickup":
                        idx = rng.choices(range(len(df)), weights=pick_weights, k=1)[0]
                        row = df.iloc[idx]
                        item_id = row["shelf_id"]
                        weight = float(row["item_weight"])
                        qty = rng.choices([1,2,3], [0.6,0.3,0.1])[0]
                        customer_carts[cid][item_id]["quantity"] += qty
                        customer_carts[cid][item_id]["weight"] = weight

                    else:
                        items = [(i,d) for i,d in customer_carts[cid].items() if d["quantity"]>0]
                        if not items:
                            continue
                        item_id, data = rng.choice(items)
                        weight = data["weight"]
                        qty = rng.randint(1, data["quantity"])
                        customer_carts[cid][item_id]["quantity"] -= qty

                    event = {
                        "event_type": action,
                        "customer_id": cid,
                        "item_id": item_id,
                        "quantity": qty,
                        "weight": weight,
                        "timestamp": current_time.isoformat(),
                    }
                    emit(rconn, event)

                    weight_evt = {
                        "event_type": "weight_change",
                        "customer_id": cid,
                        "item_id": item_id,
                        "delta_weight": (-1 if action == "pickup" else 1) * weight * qty,
                        "timestamp": current_time.isoformat(),
                    }
                    emit(rconn, weight_evt)

                    log.info(f"[shelf] {action.upper()} {cid} item={item_id} qty={qty}")
                    executed = True
                    break

        if not executed:
            time.sleep(SLEEP_SEC)

if __name__ == "__main__":
    main()
