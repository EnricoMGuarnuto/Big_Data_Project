import os
import json
import time
import threading
from datetime import datetime, timedelta

import pandas as pd
import redis
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable

# ========================
# ENV / Configuration
# ========================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_SHELF_EVENTS = os.getenv("TOPIC_SHELF_EVENTS", "shelf_events")
TOPIC_WAREHOUSE_EVENTS = os.getenv("TOPIC_WAREHOUSE_EVENTS", "warehouse_events")
TOPIC_ALERTS = os.getenv("TOPIC_ALERTS", "alerts")

# When shelf stock <= threshold, schedule a restock after RESTOCK_DELAY_SEC
RESTOCK_THRESHOLD   = int(os.getenv("RESTOCK_THRESHOLD", "10"))
RESTOCK_DELAY_SEC   = int(os.getenv("RESTOCK_DELAY_SEC", "60"))   # e.g. 1800 (=30 min) in prod
RESTOCK_MAX_CHUNK   = int(os.getenv("RESTOCK_MAX_CHUNK", "100000"))  # optional cap per restock action

# Expiry alerts (optional, sent on TOPIC_ALERTS)
EXPIRY_ALERT_DAYS   = int(os.getenv("EXPIRY_ALERT_DAYS", "3"))
EXPIRY_POLL_SEC     = int(os.getenv("EXPIRY_POLL_SEC", "15"))

# Data sources (read once at bootstrap)
STORE_PARQUET       = os.getenv("STORE_PARQUET", "/data/store_inventory_final.parquet")
WH_BATCHES_PARQUET  = os.getenv("WH_BATCHES_PARQUET", "/data/warehouse_batches.parquet")
STORE_BATCHES_PARQUET = os.getenv("STORE_BATCHES_PARQUET", "/data/store_batches.parquet")

# Redis
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB   = int(os.getenv("REDIS_DB", "0"))

# ========================
# Helpers
# ========================
def epoch(dt: datetime) -> int:
    return int(dt.timestamp())

def parse_dt(s: str) -> datetime:
    return pd.to_datetime(s).to_pydatetime()

# ========================
# Kafka
# ========================
def build_producer():
    last_err = None
    for attempt in range(1, 7):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                linger_ms=5,
                acks="all",
                retries=10,
            )
        except NoBrokersAvailable as e:
            last_err = e
            print(f"[restock-producer] Kafka not available (attempt {attempt}/6). Retrying in 3s…")
            time.sleep(3)
    raise RuntimeError(f"Could not connect to Kafka: {last_err}")

producer = build_producer()

consumer = KafkaConsumer(
    TOPIC_SHELF_EVENTS,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id=os.getenv("GROUP_ID", "shelf-restock-producer"),
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

# ========================
# Redis (state)
# ========================
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

def bootstrap_if_empty():
    """
    Initialize Redis state from parquet files (one-time).
    Keys used:
      stock:<item_id>   -> current shelf stock
      initial:<item_id> -> target shelf stock (refill goal)
      batch:<batch_id>  -> {item_id, expiry_ts, qty_wh, qty_store}
      wh:<item_id>      -> ZSET (score=expiry_ts, member=batch_id)
      store:<item_id>   -> ZSET (score=expiry_ts, member=batch_id)
      expiry:index      -> ZSET of all batches by expiry
    """
    if r.get("bootstrap:done"):
        print("[restock-producer] Redis already bootstrapped.")
        return

    print("[restock-producer] Bootstrapping Redis from parquet…")

    # Store inventory -> stock + initial
    store_df = pd.read_parquet(STORE_PARQUET)
    req_cols = {"Item_Identifier", "current_stock", "initial_stock"}
    if not req_cols.issubset(store_df.columns):
        raise ValueError(f"{STORE_PARQUET} missing columns: {req_cols - set(store_df.columns)}")

    for _, row in store_df.iterrows():
        item = row["Item_Identifier"]
        r.set(f"stock:{item}", int(row["current_stock"]))
        r.set(f"initial:{item}", int(row["initial_stock"]))

    # Warehouse batches
    wh = pd.read_parquet(WH_BATCHES_PARQUET)
    req_cols_wh = {"Batch_ID", "Item_Identifier", "Expiry_Date", "Batch_Quantity"}
    if not req_cols_wh.issubset(wh.columns):
        raise ValueError(f"{WH_BATCHES_PARQUET} missing columns: {req_cols_wh - set(wh.columns)}")

    for _, row in wh.iterrows():
        bid   = str(row["Batch_ID"])
        item  = row["Item_Identifier"]
        exp   = epoch(parse_dt(str(row["Expiry_Date"])))
        qty   = int(row["Batch_Quantity"])

        r.hset(f"batch:{bid}", mapping={
            "item_id": item,
            "expiry_ts": exp,
            "qty_wh": qty,
            "qty_store": 0
        })
        r.zadd(f"wh:{item}", {bid: exp})
        r.zadd("expiry:index", {bid: exp})

    # Store batches initial (if present)
    if os.path.exists(STORE_BATCHES_PARQUET):
        sb = pd.read_parquet(STORE_BATCHES_PARQUET)
        if {"Batch_ID", "Item_Identifier", "Expiry_Date", "Batch_Quantity"}.issubset(sb.columns):
            for _, row in sb.iterrows():
                bid   = str(row["Batch_ID"])
                item  = row["Item_Identifier"]
                exp   = epoch(parse_dt(str(row["Expiry_Date"])))
                qty   = int(row["Batch_Quantity"])

                if r.exists(f"batch:{bid}"):
                    r.hincrby(f"batch:{bid}", "qty_store", qty)
                else:
                    r.hset(f"batch:{bid}", mapping={
                        "item_id": item,
                        "expiry_ts": exp,
                        "qty_wh": 0,
                        "qty_store": qty
                    })
                    r.zadd("expiry:index", {bid: exp})
                r.zadd(f"store:{item}", {bid: exp})

    r.set("bootstrap:done", "1")
    print("[restock-producer] Bootstrap done.")

# ========================
# Restock logic
# ========================
def schedule_restock(item_id: str):
    """
    Set a TTL lock to avoid multiple schedules, emit an alert, and run perform_restock after delay.
    """
    lock_key = f"restock:lock:{item_id}"
    if r.setnx(lock_key, "1"):
        r.expire(lock_key, RESTOCK_DELAY_SEC)
        # notify alert channel that a restock is planned
        producer.send(TOPIC_ALERTS, {
            "event_type": "shelf_restock_alert",
            "item_id": item_id,
            "threshold": RESTOCK_THRESHOLD,
            "timestamp": datetime.utcnow().isoformat()
        })
        print(f"[restock-producer] Restock scheduled for {item_id} in {RESTOCK_DELAY_SEC}s")
        threading.Timer(RESTOCK_DELAY_SEC, perform_restock, args=(item_id,)).start()

# restock. Obiettivo: rifornire fino a initial_stock, prelevando dal warehouse i lotti con scadenza più vicina, anche multi-lotto se serve.
def perform_restock(item_id: str):
    """
    Refill shelf up to initial stock, pulling from warehouse batches in expiry order.
    Emits:
      - warehouse_events: warehouse_pick per batch used
      - shelf_events:     restock per batch moved to shelf
      - alerts:           warehouse_restock_required if warehouse supply is insufficient
    """
    try:
        current = int(r.get(f"stock:{item_id}") or 0)
        initial = int(r.get(f"initial:{item_id}") or current)
    except Exception:
        current, initial = 0, 0

    needed = max(0, initial - current)
    if needed == 0:  #  it means already at initial stock
        print(f"[restock-producer] {item_id}: at/above initial ({current}/{initial}). No action.")
        return

    # optional cap to avoid giant single moves
    needed = min(needed, RESTOCK_MAX_CHUNK)

    wh_key = f"wh:{item_id}"
    batch_ids = r.zrange(wh_key, 0, -1)  # all batches by earliest expiry first
    if not batch_ids:
        # no batches at all
        producer.send(TOPIC_ALERTS, {
            "event_type": "warehouse_restock_required",
            "item_id": item_id,
            "missing_quantity": needed,
            "reason": "no_warehouse_batches",
            "timestamp": datetime.utcnow().isoformat(),
        })
        print(f"[restock-producer] {item_id}: no warehouse batches; alert emitted.")
        return

    moved_total = 0
    now_iso = datetime.utcnow().isoformat()

    for bid in batch_ids:
        if needed <= 0:  # already filled the shelf
            break

        bkey = f"batch:{bid}"
        b = r.hgetall(bkey)
        if not b or b.get("item_id") != item_id:
            continue

        qty_wh = int(b.get("qty_wh", 0))
        exp_ts = int(b.get("expiry_ts", 0))
        if qty_wh <= 0:
            continue

        move = min(qty_wh, needed)

        # Transfer from warehouse -> store for that batch
        pipe = r.pipeline()
        pipe.hincrby(bkey, "qty_wh", -move)
        pipe.hincrby(bkey, "qty_store", move)
        pipe.execute()

        # index batch on store:<item_id>
        r.zadd(f"store:{item_id}", {bid: exp_ts})

        # increment shelf counter
        r.incrby(f"stock:{item_id}", move)

        # events
        producer.send(TOPIC_WAREHOUSE_EVENTS, {
            "event_type": "warehouse_pick",
            "item_id": item_id,
            "batch_id": bid,
            "quantity_picked": move,
            "timestamp": now_iso
        })
        producer.send(TOPIC_SHELF_EVENTS, {
            "event_type": "restock",
            "item_id": item_id,
            "batch_id": bid,
            "quantity": move,
            "timestamp": now_iso
        })

        moved_total += move
        needed -= move

    if needed > 0:  # still missing some stok, it means warehouse was insufficient
        producer.send(TOPIC_ALERTS, {
            "event_type": "warehouse_restock_required",
            "item_id": item_id,
            "missing_quantity": needed,
            "reason": "insufficient_warehouse_stock",
            "timestamp": datetime.utcnow().isoformat(),
        })
        print(f"[restock-producer] {item_id}: partial refill (+{moved_total}); missing {needed}. Alert emitted.")
    else:
        print(f"[restock-producer] {item_id}: fully refilled to initial (+{moved_total}).")

# ========================
# Expiry watcher (optional)
# ========================
# This periodically checks for batches that are about to expire and emits alerts.

def expiry_watcher():
    while True:
        horizon = epoch(datetime.utcnow() + timedelta(days=EXPIRY_ALERT_DAYS))
        due = r.zrangebyscore("expiry:index", 0, horizon)
        for bid in due:
            # emit alert once per batch
            if r.sadd("alerted:batches", bid):
                b = r.hgetall(f"batch:{bid}")
                if not b:
                    continue
                total_qty = int(b.get("qty_wh", 0)) + int(b.get("qty_store", 0))
                if total_qty <= 0:
                    continue
                producer.send(TOPIC_ALERTS, {
                    "event_type": "expiry_alert",
                    "batch_id": bid,
                    "item_id": b.get("item_id"),
                    "expires_at": datetime.utcfromtimestamp(int(b.get("expiry_ts", 0))).isoformat(),
                    "qty_wh": int(b.get("qty_wh", 0)),
                    "qty_store": int(b.get("qty_store", 0)),
                    "days_threshold": EXPIRY_ALERT_DAYS,
                    "timestamp": datetime.utcnow().isoformat()
                })
                print(f"[restock-producer] Expiry alert emitted for batch {bid}")
        time.sleep(EXPIRY_POLL_SEC)

# ========================
# Main loop: react to pickup/putback
# ========================
def main():
    bootstrap_if_empty()
    threading.Thread(target=expiry_watcher, daemon=True).start()

    print("[restock-producer] Listening to shelf_events (pickup/putback)…")
    for msg in consumer:
        evt = msg.value
        etype = evt.get("event_type")
        item_id = evt.get("item_id")
        if not item_id:
            continue

        if etype == "pickup":
            newv = r.decr(f"stock:{item_id}")
            if newv <= RESTOCK_THRESHOLD:
                schedule_restock(item_id)    # Quando scatta il timer del restock, verrà chiamata perform_restock() che fa il lavoro grosso (lotto per lotto, fino all’initial_stock).

        elif etype == "putback":
            r.incr(f"stock:{item_id}")
            # no special scheduling on putback

        # 'restock' events from other sources are ignored here

if __name__ == "__main__":
    main()
