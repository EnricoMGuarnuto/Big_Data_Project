# warehouse_executor.py
# Purpose: consume Spark plans (wh_restock_plan, wh_supplier_plan), apply FIFO on WH batches,
# emit wh_events (wh_out / wh_in) and CLOSE related alerts via Kafka-first (alert_status_change).

import os
import json
import uuid
import time
import logging
from datetime import datetime, timedelta, timezone, date
from collections import deque
from typing import Dict, Deque, Tuple, List, Optional

import pandas as pd
import redis
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

# =========================
# Env / Config
# =========================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

# Inputs (decided upstream by Spark)
PLAN_TOPIC = os.getenv("PLAN_TOPIC", "wh_restock_plan")                     # WH → store moves
SUPPLIER_PLAN_TOPIC = os.getenv("SUPPLIER_PLAN_TOPIC", "wh_supplier_plan")  # inbound from suppliers

# Outputs
EVENT_TOPIC = os.getenv("EVENT_TOPIC", "wh_events")                         # wh_in / wh_out (facts)
ALERTS_TOPIC = os.getenv("ALERTS_TOPIC", "alerts")                          # alert_status_change

# Bootstrap FIFO state from parquet (can be replaced by compacted topic later)
BATCHES_PARQUET = os.getenv("WH_BATCHES_PARQUET", "/data/warehouse_batches.parquet")

CLIENT_ID = os.getenv("CLIENT_ID", "warehouse-executor")

# Redis (buffer-before-Kafka)
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_STREAM = os.getenv("REDIS_STREAM", "wh_events")  # Redis Stream key

# Runtime
POLL_MS = int(os.getenv("POLL_MS", "300"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
MAX_SEEN_IDS = int(os.getenv("MAX_SEEN_IDS", "50000"))

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="[%(asctime)s] %(levelname)s %(message)s"
)
log = logging.getLogger("warehouse-executor")

# =========================
# Utils
# =========================
def now_utc() -> datetime:
    return datetime.utcnow().replace(tzinfo=timezone.utc)

def now_iso() -> str:
    return now_utc().isoformat()

# =========================
# IO Builders
# =========================
def build_consumer() -> KafkaConsumer:
    topics = [PLAN_TOPIC, SUPPLIER_PLAN_TOPIC]
    return KafkaConsumer(
        *topics,
        bootstrap_servers=KAFKA_BROKER,
        group_id="warehouse-executor",
        client_id=f"{CLIENT_ID}-consumer",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )

def build_producer() -> KafkaProducer:
    last = None
    for a in range(1, 7):
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                client_id=CLIENT_ID,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=10,
                linger_ms=10,
                max_in_flight_requests_per_connection=1,
                compression_type="gzip",
            )
            log.info("Connected to Kafka (producer).")
            return p
        except NoBrokersAvailable as e:
            last = e
            log.warning(f"Kafka not available ({a}/6). Retrying…")
            time.sleep(3)
    raise RuntimeError(f"Kafka unreachable: {last}")

def build_redis() -> redis.Redis:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    r.ping()
    log.info(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}")
    return r

def emit_event(rconn: redis.Redis, kprod: KafkaProducer, evt: dict):
    """Write to Redis Stream first (buffer), then to Kafka."""
    try:
        rconn.xadd(REDIS_STREAM, {"data": json.dumps(evt)}, maxlen=20000, approximate=True)
    except Exception as e:
        log.warning(f"Redis XADD failed: {e}")
    try:
        kprod.send(EVENT_TOPIC, value=evt)
    except Exception as e:
        log.warning(f"Kafka send failed: {e}")

# =========================
# FIFO State from Parquet
# =========================
def _parse_date(d) -> date:
    if isinstance(d, date):
        return d
    return datetime.strptime(str(d), "%Y-%m-%d").date()

def load_fifo(batches_path: str) -> Dict[str, Deque[dict]]:
    if not os.path.exists(batches_path):
        raise FileNotFoundError(f"warehouse_batches parquet not found: {batches_path}")
    bt = pd.read_parquet(batches_path).copy()

    required = {
        "shelf_id", "batch_code", "received_date", "expiry_date",
        "batch_quantity_warehouse", "batch_quantity_store"
    }
    missing = required - set(bt.columns)
    if missing:
        raise ValueError(f"warehouse_batches.parquet missing columns: {missing}")

    bt["received_date"] = bt["received_date"].apply(_parse_date)
    bt["expiry_date"] = bt["expiry_date"].apply(_parse_date)

    fifo: Dict[str, Deque[dict]] = {}
    for sid, g in bt.groupby("shelf_id"):
        rows = []
        for _, r in g.iterrows():
            rows.append({
                "batch_code": str(r["batch_code"]),
                "received_date": r["received_date"],
                "expiry_date": r["expiry_date"],
                "warehouse_qty": int(r["batch_quantity_warehouse"]),
                "store_qty": int(r["batch_quantity_store"]),
            })
        rows.sort(key=lambda x: (x["received_date"], x["expiry_date"]))
        fifo[str(sid)] = deque(rows)
    log.info(f"Loaded FIFO for {len(fifo)} shelves.")
    return fifo

def fifo_total(q: Deque[dict]) -> int:
    return sum(b["warehouse_qty"] for b in q)

def drain_fifo(q: Deque[dict], need: int) -> List[Tuple[str, int, dict]]:
    """Consume FIFO head to satisfy 'need'. Return [(batch_code, qty_moved, batch_meta_after), ...]."""
    moved = []
    remaining = int(need)
    while remaining > 0 and q:
        while q and q[0]["warehouse_qty"] <= 0:
            q.popleft()
        if not q:
            break
        h = q[0]
        take = min(remaining, h["warehouse_qty"])
        if take <= 0:
            break
        h["warehouse_qty"] -= take
        h["store_qty"] += take
        moved.append((h["batch_code"], take, h))
        remaining -= take
        if h["warehouse_qty"] <= 0:
            q.popleft()
    return moved

# =========================
# Idempotency (best-effort)
# =========================
class SeenIds:
    def __init__(self, cap: int):
        self.cap = cap
        self.set = set()
        self.order = deque()

    def add(self, key: str) -> bool:
        if key in self.set:
            return False
        self.set.add(key)
        self.order.append(key)
        if len(self.order) > self.cap:
            old = self.order.popleft()
            self.set.discard(old)
        return True

# =========================
# Alert closing (Kafka-first)
# =========================
def alert_status_to_kafka(kprod: KafkaProducer, alert_id: str, status: str, reason: str):
    """
    Publish an alert status change to the `alerts` topic.
    Another service/connector will persist it to Postgres.
    """
    evt = {
        "event_type": "alert_status_change",
        "alert_id": alert_id,
        "status": status,   # 'ack' | 'resolved'
        "timestamp": now_iso(),
        "reason": reason,
    }
    try:
        kprod.send(ALERTS_TOPIC, value=evt)
    except Exception as e:
        log.warning(f"Kafka send alert_status_change failed: {e}")

# =========================
# Handlers
# =========================
def _emit_wh_outs_for_need(
    sid: str, need: int, plan_id: Optional[str], reason: str, alert_id: Optional[str],
    fifo_map: Dict[str, Deque[dict]], rconn: redis.Redis, kprod: KafkaProducer
) -> int:
    """
    Execute a WH→store move with FIFO. Return total moved qty.
    Also emits alert status changes (Kafka-first) if alert_id is provided.
    """
    q = fifo_map.get(sid, deque())
    if need <= 0:
        return 0

    moved_total = 0
    if fifo_total(q) <= 0:
        evt = {
            "event_type": "wh_out",
            "event_id": str(uuid.uuid4()),
            "plan_id": plan_id,
            "shelf_id": sid,
            "batch_code": None,
            "qty": 0,
            "unit": "ea",
            "timestamp": now_iso(),
            "fifo": True,
            "reason": f"{reason}_warehouse_empty",
        }
        emit_event(rconn, kprod, evt)
        if alert_id:
            alert_status_to_kafka(kprod, alert_id, "ack", f"{reason}: planned but no stock in WH")
        log.info(f"[{reason}] warehouse empty for shelf {sid} (need={need})")
        return 0

    remaining = need
    while remaining > 0:
        moved = drain_fifo(q, remaining)
        if not moved:
            break
        for batch_code, qty, meta in moved:
            moved_total += qty
            evt = {
                "event_type": "wh_out",
                "event_id": str(uuid.uuid4()),
                "plan_id": plan_id,
                "shelf_id": sid,
                "batch_code": batch_code,
                "qty": int(qty),
                "unit": "ea",
                "timestamp": now_iso(),
                "fifo": True,
                "received_date": meta["received_date"].isoformat(),
                "expiry_date": meta["expiry_date"].isoformat(),
                "batch_quantity_warehouse_after": meta["warehouse_qty"],
                "batch_quantity_store_after": meta["store_qty"],
                "shelf_warehouse_qty_after": fifo_total(q),
                "reason": reason,
            }
            emit_event(rconn, kprod, evt)
            remaining -= qty

    fifo_map[sid] = q

    # Alert lifecycle via Kafka:
    if alert_id:
        if moved_total == 0:
            alert_status_to_kafka(kprod, alert_id, "ack", f"{reason}: planned but nothing moved")
        elif moved_total < need:
            alert_status_to_kafka(kprod, alert_id, "ack", f"{reason}: partial ({moved_total}/{need})")
        else:
            alert_status_to_kafka(kprod, alert_id, "resolved", f"{reason}: completed ({moved_total}/{need})")

    return moved_total

def handle_restock_plan(plan: dict, fifo_map: Dict[str, Deque[dict]],
                        rconn: redis.Redis, kprod: KafkaProducer):
    """
    Plan schema (minimal):
      { "plan_id": "...", "shelf_id": "...", "suggested_qty": 82, "alert_id": "..."? }
    """
    sid = plan.get("shelf_id")
    need = int(plan.get("suggested_qty", 0))
    pid = plan.get("plan_id")
    aid = plan.get("alert_id")

    if not sid or need <= 0:
        return

    # Immediately ACK the alert when plan is received (optional but common)
    if aid:
        alert_status_to_kafka(kprod, aid, "ack", "restock plan received")

    _emit_wh_outs_for_need(sid, need, pid, "plan_restock", aid, fifo_map, rconn, kprod)

def handle_supplier_plan(plan: dict, fifo_map: Dict[str, Deque[dict]],
                         rconn: redis.Redis, kprod: KafkaProducer):
    """
    Supplier plan schema:
      { "plan_id": "...", "shelf_id": "...", "qty": 120, "batch_code"?: "B-XYZ", "expiry_date"?: "YYYY-MM-DD", "alert_id"?: "..." }
    """
    sid = plan.get("shelf_id")
    if not sid:
        return
    qty = int(plan.get("qty", 0))
    if qty <= 0:
        return

    bcode = str(plan.get("batch_code") or f"B-{uuid.uuid4().hex[:6]}")
    rec = now_utc().date()
    exp = plan.get("expiry_date")
    exp_date = _parse_date(exp) if exp else (rec + timedelta(days=365))
    aid = plan.get("alert_id")

    q = fifo_map.get(sid, deque())
    q.append({
        "batch_code": bcode,
        "received_date": rec,
        "expiry_date": exp_date,
        "warehouse_qty": qty,
        "store_qty": 0
    })

    evt = {
        "event_type": "wh_in",
        "event_id": str(uuid.uuid4()),
        "plan_id": plan.get("plan_id"),
        "shelf_id": sid,
        "batch_code": bcode,
        "qty": qty,
        "unit": "ea",
        "timestamp": now_iso(),
        "fifo": True,
        "received_date": rec.isoformat(),
        "expiry_date": exp_date.isoformat(),
        "batch_quantity_warehouse_after": qty,
        "batch_quantity_store_after": 0,
        "shelf_warehouse_qty_after": fifo_total(q),
        "reason": "plan_inbound",
    }
    emit_event(rconn, kprod, evt)
    fifo_map[sid] = q

    # For supplier-driven replenishment, you can mark a related alert as resolved.
    if aid:
        alert_status_to_kafka(kprod, aid, "resolved", "supplier plan executed")

    log.info(f"[plan_inbound] wh_in shelf={sid} batch={bcode} qty={qty} total_wh={evt['shelf_warehouse_qty_after']}")

# =========================
# Main loop
# =========================
def main():
    fifo_map = load_fifo(BATCHES_PARQUET)   # initial snapshot; consider live updates from a compacted topic later
    consumer = build_consumer()
    producer = build_producer()
    rconn = build_redis()

    seen = SeenIds(MAX_SEEN_IDS)

    log.info("Warehouse executor started. Waiting for plans…")
    while True:
        polled = consumer.poll(timeout_ms=POLL_MS)
        if not polled:
            time.sleep(0.05)
            continue

        for tp, records in polled.items():
            topic = tp.topic
            for msg in records:
                try:
                    obj = msg.value
                    key = f"{topic}|{obj.get('plan_id') or obj.get('id') or json.dumps(obj, sort_keys=True)}"
                    if not seen.add(key):
                        continue

                    if topic == PLAN_TOPIC:
                        handle_restock_plan(obj, fifo_map, rconn, producer)
                    elif topic == SUPPLIER_PLAN_TOPIC:
                        handle_supplier_plan(obj, fifo_map, rconn, producer)
                except Exception as e:
                    log.exception(f"Error handling message from {topic}: {e}")

if __name__ == "__main__":
    main()
