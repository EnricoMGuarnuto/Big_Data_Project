import os
import json
import uuid
import time
import threading
import redis
import logging
import random
import psycopg2
from datetime import datetime, timedelta, timezone, date
from typing import Dict, DefaultDict, Optional, Deque, List, Tuple
from collections import defaultdict, deque

import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from kafka.admin import KafkaAdminClient, NewTopic

# ========================
# Logging
# ========================
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s %(message)s')
log = logging.getLogger("pos")

# ========================
# Config
# ========================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

FOOT_TOPIC   = os.getenv("FOOT_TOPIC", "foot_traffic")
SHELF_TOPIC  = os.getenv("SHELF_TOPIC", "shelf_events")
POS_TOPIC    = os.getenv("POS_TOPIC", "pos_transactions")

# Redis (buffer-before-Kafka)
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_STREAM = os.getenv("REDIS_STREAM", "pos_transactions")  # stream key

GROUP_ID_SHELF = os.getenv("GROUP_ID_SHELF", "pos-simulator-shelf")
GROUP_ID_FOOT  = os.getenv("GROUP_ID_FOOT", "pos-simulator-foot")

SHELF_AUTO_OFFSET_RESET = os.getenv("SHELF_AUTO_OFFSET_RESET", "latest")
FOOT_AUTO_OFFSET_RESET = os.getenv("FOOT_AUTO_OFFSET_RESET", "earliest")

# Consumer group stability / offset commits
CONSUMER_ENABLE_AUTO_COMMIT = os.getenv("CONSUMER_ENABLE_AUTO_COMMIT", "0") in ("1", "true", "True")
CONSUMER_COMMIT_EVERY_N = int(os.getenv("CONSUMER_COMMIT_EVERY_N", "200"))
CONSUMER_COMMIT_EVERY_S = float(os.getenv("CONSUMER_COMMIT_EVERY_S", "5"))
CONSUMER_SESSION_TIMEOUT_MS = int(os.getenv("CONSUMER_SESSION_TIMEOUT_MS", "30000"))
CONSUMER_HEARTBEAT_INTERVAL_MS = int(os.getenv("CONSUMER_HEARTBEAT_INTERVAL_MS", "10000"))
CONSUMER_MAX_POLL_INTERVAL_MS = int(os.getenv("CONSUMER_MAX_POLL_INTERVAL_MS", "600000"))

STORE_PARQUET = os.getenv("STORE_PARQUET", "/data/store_inventory_final.parquet")
DISCOUNT_PARQUET_PATH = os.getenv("DISCOUNT_PARQUET_PATH", "/data/all_discounts.parquet")
STORE_BATCHES_PARQUET = os.getenv("STORE_BATCHES_PARQUET", "/data/store_batches.parquet")

FORCE_CHECKOUT_IF_EMPTY = int(os.getenv("FORCE_CHECKOUT_IF_EMPTY", "0")) == 1
MAX_SESSION_AGE_SEC = int(os.getenv("MAX_SESSION_AGE_SEC", str(3 * 60 * 60)))

#postgres
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "smart_shelf")
PG_USER = os.getenv("PG_USER", "bdt_user")
PG_PASS = os.getenv("PG_PASS", "bdt_password")
DAILY_DISCOUNT_TABLE = os.getenv("DAILY_DISCOUNT_TABLE", "analytics.daily_discounts")


# Probability of choosing the batch with the closest expiry
PROB_EARLIEST_EXPIRY = float(os.getenv("PROB_EARLIEST_EXPIRY", "0.80"))



# Globals inizializzati in main()
producer: Optional[KafkaProducer] = None
rconn: Optional[redis.Redis] = None

# ========================
# Kafka topic ensure
# ========================
def ensure_topic(topic, bootstrap, partitions=3, rf=1, attempts=10, sleep_s=3):
    last = None
    for i in range(1, attempts + 1):
        admin = None
        try:
            admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="pos-init")
            if topic not in admin.list_topics():
                admin.create_topics([NewTopic(name=topic, num_partitions=partitions, replication_factor=rf)])
                log.info(f"[pos] created topic {topic}")
            else:
                log.info(f"[pos] topic {topic} already exists")
            return
        except NoBrokersAvailable as e:
            last = e
            log.warning(f"[pos] Kafka not ready (attempt {i}/{attempts}). Retry in {sleep_s}s…")
            time.sleep(sleep_s)
        except Exception as e:
            log.warning(f"[pos] topic check/create failed: {e}")
            return
        finally:
            if admin is not None:
                try:
                    admin.close()
                except Exception:
                    pass
    log.warning(f"[pos] impossible to create/verify topic {topic}: {last}")

ensure_topic(POS_TOPIC,   KAFKA_BROKER)
ensure_topic(SHELF_TOPIC, KAFKA_BROKER)
ensure_topic(FOOT_TOPIC,  KAFKA_BROKER)

# ========================
# Helpers
# ========================
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def parse_date(d) -> date:
    if isinstance(d, date):
        return d
    return datetime.strptime(str(d), "%Y-%m-%d").date()

def load_price_map_from_store(path: str) -> Dict[str, float]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Store parquet not found: {path}")
    df = pd.read_parquet(path)
    req = {"shelf_id", "item_price"}
    missing = req - set(df.columns)
    if missing:
        raise ValueError(f"{path} missing columns {missing} (need 'item_price').")
    df = df.dropna(subset=["shelf_id", "item_price"])
    return df.set_index("shelf_id")["item_price"].astype(float).to_dict()

def load_discounts_from_parquet(path: str) -> Dict[str, float]:
    if not os.path.exists(path):
        log.warning(f"[pos] Discounts file not found: {path}")
        return {}
    df = pd.read_parquet(path)
    today = datetime.today()
    week_str = f"{today.isocalendar().year}-W{today.isocalendar().week:02}"
    df = df[df["week"] == week_str]
    log.info(f"[pos] Loaded {len(df)} discounts for week {week_str}")
    return dict(zip(df["shelf_id"], df["discount"]))

def load_daily_discounts_from_pg() -> Dict[str, float]:
    """
    Returns a mapping shelf_id -> daily_discount for today from analytics.daily_discounts.
    """
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASS,
        )
        cur = conn.cursor()
        today = datetime.now().date()
        cur.execute(
            f"SELECT shelf_id, discount FROM {DAILY_DISCOUNT_TABLE} WHERE discount_date = %s",
            (today,),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()

        d = {
            sid: float(discount)
            for sid, discount in rows
            if sid is not None and discount is not None
        }
        log.info(f"[pos] Loaded {len(d)} daily discounts for {today}")
        return d
    except Exception as e:
        log.warning(f"[pos] ERROR while loading daily discounts from Postgres: {e}")
        return {}


# ========================
# Cart state + Batches per shelf
# ========================
carts: DefaultDict[str, DefaultDict[str, int]] = defaultdict(lambda: defaultdict(int))
carts_lock = threading.Lock()

# batch_state[shelf_id] = deque([{"batch_code", "expiry_date", "qty_store"}...]) ordinata per expiry asc
batch_state: Dict[str, Deque[Dict]] = {}
batches_lock = threading.Lock()

timers: Dict[str, threading.Timer] = {}
timers_lock = threading.Lock()
entries: Dict[str, datetime] = {}
entries_lock = threading.Lock()

def load_store_batches(path: str) -> Dict[str, Deque[Dict]]:
    """
    Bootstrap da store_batches.parquet
    Richieste colonne:
      shelf_id, batch_code, expiry_date, batch_quantity_store
    """
    if not os.path.exists(path):
        log.warning(f"[pos] store_batches parquet not found: {path}")
        return {}
    df = pd.read_parquet(path)
    required = {"shelf_id","batch_code","expiry_date","batch_quantity_store"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{path} missing columns: {missing}")
    df = df.copy()
    df["expiry_date"] = df["expiry_date"].apply(parse_date)

    state: Dict[str, Deque[Dict]] = {}
    for sid, g in df.groupby("shelf_id"):
        rows = []
        for _, r in g.iterrows():
            qty_store = int(r["batch_quantity_store"]) if pd.notna(r["batch_quantity_store"]) else 0
            if qty_store <= 0:
                continue
            rows.append({
                "batch_code": str(r["batch_code"]),
                "expiry_date": r["expiry_date"],
                "qty_store": qty_store
            })
        rows.sort(key=lambda x: x["expiry_date"])  # soonest expiry first
        state[str(sid)] = deque(rows)
    log.info(f"[pos] Loaded in-store batch FIFO for {len(state)} shelves.")
    return state

try:
    price_by_item = load_price_map_from_store(STORE_PARQUET)
    log.info(f"[pos] Prices loaded from {STORE_PARQUET}, {len(price_by_item)} items found.")
except Exception as e:
    log.error(f"[pos] ERROR while loading prices {STORE_PARQUET}: {e}")
    price_by_item = {}

weekly_discounts_by_item: Dict[str, float] = {}
daily_discounts_by_item: Dict[str, float] = {}

weekly_discounts_by_item.update(load_discounts_from_parquet(DISCOUNT_PARQUET_PATH))
daily_discounts_by_item.update(load_daily_discounts_from_pg())


# Bootstrap batches
try:
    batch_state = load_store_batches(STORE_BATCHES_PARQUET)
except Exception as e:
    log.error(f"[pos] ERROR loading store batches: {e}")
    batch_state = {}

# ========================
# IO builders
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
            log.info("[pos] Connected to Kafka.")
            return p
        except NoBrokersAvailable as e:
            last_err = e
            log.warning(f"[pos] Kafka not available (attempt {attempt}/6). Retry in 3s…")
            time.sleep(3)
    raise RuntimeError(f"Impossible to connect to Kafka: {last_err}")

def build_redis() -> redis.Redis:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    r.ping()
    log.info(f"[pos] Connected to Redis at {REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}")
    return r

# ========================
# Pricing / discount
# ========================
def _price_for(item_id: str) -> float:
    return float(price_by_item.get(item_id, 5.0))

def _discount_for(item_id: str) -> float:
    """
    Combines weekly + daily discounts multiplicatively:
      effective = 1 - (1 - dw) * (1 - dd)
    and clamps to [0, 0.95].
    """
    dw = float(weekly_discounts_by_item.get(item_id, 0.0) or 0.0)
    dd = float(daily_discounts_by_item.get(item_id, 0.0) or 0.0)
    effective = 1.0 - (1.0 - dw) * (1.0 - dd)
    return max(0.0, min(effective, 0.95))

# ========================
# Batch allocation (earliest-expiry bias)
# ========================
def _choose_batch_index(q: Deque[Dict]) -> int:
    """Returns the index to pick from: 0 with probability PROB_EARLIEST_EXPIRY, otherwise a random index among the others."""
    if not q:
        return -1
    if len(q) == 1:
        return 0
    r = random.random()
    if r < PROB_EARLIEST_EXPIRY:
        return 0
    # choose another batch (simple bias: uniform between 1..len-1)
    return random.randint(1, len(q)-1)

def _allocate_from_batches(shelf_id: str, qty: int) -> List[Tuple[str, int, date]]:
    """
    Subtracts qty from in-store batches for shelf_id while respecting:
      - earliest-expiry with probability PROB_EARLIEST_EXPIRY
      - when a batch runs out, move to the next one
    Returns a list of (batch_code, allocated_qty, expiry_date).
    """
    allocated: List[Tuple[str, int, date]] = []
    if qty <= 0:
        return allocated

    with batches_lock:
        q = batch_state.get(shelf_id)
        if not q or len(q) == 0:
            # no state → no batch allocation (caller can emit without batch_code)
            return allocated

        remaining = qty
        while remaining > 0 and q:
            # drop exhausted front batches
            while q and q[0]["qty_store"] <= 0:
                q.popleft()
            if not q:
                break

            idx = _choose_batch_index(q)
            if idx < 0:
                break

            # take a reference to the chosen batch
            # Deque non supporta accesso diretto efficiente a idx >0 per pop; usiamo lista temporanea
            tmp = list(q)
            b = tmp[idx]
            take = min(remaining, b["qty_store"])
            if take <= 0:
                # if that batch is empty, remove it
                if b["qty_store"] <= 0:
                    del tmp[idx]
                    q = deque(tmp)
                    batch_state[shelf_id] = q
                    continue
                else:
                    break

            # update quantity
            b["qty_store"] -= take
            remaining -= take

            allocated.append((b["batch_code"], take, b["expiry_date"]))

            # put back into the deque keeping expiry ordering (expiry doesn't change)
            tmp[idx] = b
            # remove batches with qty=0
            tmp = [x for x in tmp if x["qty_store"] > 0]
            # ensure ordering by expiry
            tmp.sort(key=lambda x: x["expiry_date"])
            q = deque(tmp)
            batch_state[shelf_id] = q

        return allocated

# ========================
# Checkout
# ========================
def emit_pos_transaction(customer_id: str, timestamp: datetime):
    global producer, rconn

    # snapshot carrello
    with carts_lock:
        items_map = dict(carts.get(customer_id, {}))

    if not items_map and not FORCE_CHECKOUT_IF_EMPTY:
        log.info(f"[pos] Checkout {customer_id}: cart empty → skip.")
        return

    # Build receipt lines, possibly split by batch
    tx_items = []
    for item_id, qty in items_map.items():
        if qty <= 0:
            continue

        # try allocating from batches
        allocations = _allocate_from_batches(item_id, int(qty))
        allocated_total = sum(a[1] for a in allocations)

        # if some units remain unallocated (missing state), sell them without batch_code
        unallocated = max(0, int(qty) - allocated_total)
        unit_price = round(_price_for(item_id), 2)
        discount   = round(_discount_for(item_id), 2)

        for batch_code, qalloc, exp in allocations:
            line_total = round(qalloc * unit_price * (1 - discount), 2)
            tx_items.append({
                "item_id": item_id,
                "batch_code": batch_code,
                "quantity": int(qalloc),
                "unit_price": unit_price,
                "discount": discount,
                "total_price": line_total,
                "expiry_date": exp.isoformat()
            })

        if unallocated > 0:
            line_total = round(unallocated * unit_price * (1 - discount), 2)
            tx_items.append({
                "item_id": item_id,
                "quantity": int(unallocated),
                "unit_price": unit_price,
                "discount": discount,
                "total_price": line_total
                # no batch_code/expiry when unallocated
            })

    if not tx_items and not FORCE_CHECKOUT_IF_EMPTY:
        log.info(f"[pos] Checkout {customer_id}: no valid item → skip.")
        return

    transaction = {
        "event_type": "pos_transaction",
        "transaction_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "timestamp": timestamp.isoformat(),
        "items": tx_items
    }

    # 1) Redis buffer first
    try:
        if rconn is not None:
            rconn.xadd(REDIS_STREAM, {"data": json.dumps(transaction)}, maxlen=20000, approximate=True)
    except Exception as e:
        log.warning(f"[pos] Redis XADD failed: {e}")

    # 2) Kafka
    producer.send(POS_TOPIC, value=transaction)
    log.info(f"[pos] POS transaction emitted: {transaction}")

    # cleanup
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
        log.info(f"[pos] Checkout for {customer_id} in {round(delay, 2)}s.")

# ========================
# Consumers
# ========================
def shelf_consumer_loop():
    consumer = KafkaConsumer(
        SHELF_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID_SHELF,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset=SHELF_AUTO_OFFSET_RESET,
        enable_auto_commit=CONSUMER_ENABLE_AUTO_COMMIT,
        session_timeout_ms=CONSUMER_SESSION_TIMEOUT_MS,
        heartbeat_interval_ms=CONSUMER_HEARTBEAT_INTERVAL_MS,
        max_poll_interval_ms=CONSUMER_MAX_POLL_INTERVAL_MS,
    )
    log.info(f"[pos] Listening shelf events on '{SHELF_TOPIC}'")

    processed = 0
    last_commit = time.time()
    for msg in consumer:
        try:
            evt = msg.value
            etype = evt.get("event_type")
            customer_id = evt.get("customer_id")
            item_id = evt.get("item_id")

            if etype not in ("pickup", "putback") or not customer_id or not item_id:
                continue

            qty = int(evt.get("quantity", 1))
            with carts_lock:
                if etype == "pickup":
                    carts[customer_id][item_id] += qty
                elif etype == "putback":
                    carts[customer_id][item_id] = max(0, carts[customer_id][item_id] - qty)
        finally:
            if not CONSUMER_ENABLE_AUTO_COMMIT:
                processed += 1
                now = time.time()
                if processed >= CONSUMER_COMMIT_EVERY_N or (now - last_commit) >= CONSUMER_COMMIT_EVERY_S:
                    try:
                        consumer.commit()
                    except Exception as e:
                        # Commit can fail during rebalances; we keep running to avoid killing the simulator.
                        log.warning(f"[pos] consumer commit failed for group {GROUP_ID_SHELF}: {e}")
                    processed = 0
                    last_commit = now

def foot_consumer_loop():
    consumer = KafkaConsumer(
        FOOT_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID_FOOT,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset=FOOT_AUTO_OFFSET_RESET,
        enable_auto_commit=CONSUMER_ENABLE_AUTO_COMMIT,
        session_timeout_ms=CONSUMER_SESSION_TIMEOUT_MS,
        heartbeat_interval_ms=CONSUMER_HEARTBEAT_INTERVAL_MS,
        max_poll_interval_ms=CONSUMER_MAX_POLL_INTERVAL_MS,
    )
    log.info(f"[pos] Listening foot traffic on '{FOOT_TOPIC}'")

    processed = 0
    last_commit = time.time()
    for msg in consumer:
        try:
            evt = msg.value
            if evt.get("event_type") != "foot_traffic":
                continue

            customer_id = evt.get("customer_id")
            entry_time  = datetime.fromisoformat(evt["entry_time"])
            exit_time   = datetime.fromisoformat(evt["exit_time"])

            with entries_lock:
                entries[customer_id] = entry_time

            schedule_checkout(customer_id, exit_time)
        finally:
            if not CONSUMER_ENABLE_AUTO_COMMIT:
                processed += 1
                now = time.time()
                if processed >= CONSUMER_COMMIT_EVERY_N or (now - last_commit) >= CONSUMER_COMMIT_EVERY_S:
                    try:
                        consumer.commit()
                    except Exception as e:
                        log.warning(f"[pos] consumer commit failed for group {GROUP_ID_FOOT}: {e}")
                    processed = 0
                    last_commit = now

def janitor_loop():
    while True:
        time.sleep(30)
        now = datetime.now(timezone.utc)
        stale = []
        with entries_lock:
            for cid, ent in list(entries.items()):
                if (now - ent).total_seconds() > MAX_SESSION_AGE_SEC:
                    stale.append(cid)
        for cid in stale:
            log.info(f"[pos] Janitor: forcing checkout for stale customer {cid}")
            emit_pos_transaction(cid, timestamp=now)

# ========================
# Main
# ========================
def main():
    global producer, rconn
    producer = build_producer()
    rconn = build_redis()

    threading.Thread(target=shelf_consumer_loop, daemon=True).start()
    threading.Thread(target=foot_consumer_loop, daemon=True).start()
    threading.Thread(target=janitor_loop, daemon=True).start()

    log.info("[pos] POS Producer started (building carts, scheduling checkouts).")
    while True:
        time.sleep(3600)

if __name__ == "__main__":
    main()
