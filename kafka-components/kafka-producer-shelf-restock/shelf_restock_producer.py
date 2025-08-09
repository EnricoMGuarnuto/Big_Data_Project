#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
shelf_restock_producer.py

Ascolta gli eventi di scaffale (pickup/putback/restock) e:
- Mantiene lo stato di stock a scaffale su Redis (per item)
- Quando stock <= soglia, schedula un restock (con lock TTL)
- Esegue il restock in modalità FEFO prelevando lotti dal magazzino (warehouse)
- Emette eventi su Kafka: warehouse_pick, restock, alerts (expiry / warehouse_restock_required)
- Bootstrap iniziale da file Parquet su Redis
- Idempotenza: deduplica event_id, operazioni critiche via pipeline atomiche
- Expiry watcher: allerta lotti in scadenza
- Dead-letter queue per eventi malformati
"""

import os
import json
import time
import signal
import logging
import threading
from typing import Optional, Dict, Any
from datetime import datetime, timedelta

import pandas as pd
import redis
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable, KafkaError

# ========================
# Config (env-first)
# ========================
KAFKA_BROKER              = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_SHELF_EVENTS        = os.getenv("TOPIC_SHELF_EVENTS", "shelf_events")
TOPIC_WAREHOUSE_EVENTS    = os.getenv("TOPIC_WAREHOUSE_EVENTS", "warehouse_events")
TOPIC_ALERTS              = os.getenv("TOPIC_ALERTS", "alerts")
TOPIC_DLQ                 = os.getenv("TOPIC_DLQ", "dlq_events")  # dead-letter

GROUP_ID                  = os.getenv("GROUP_ID", "shelf-restock-producer")

# restock
RESTOCK_THRESHOLD         = int(os.getenv("RESTOCK_THRESHOLD", "10"))
RESTOCK_DELAY_SEC         = int(os.getenv("RESTOCK_DELAY_SEC", "60"))
RESTOCK_MAX_CHUNK         = int(os.getenv("RESTOCK_MAX_CHUNK", "100000"))

# expiry
EXPIRY_ALERT_DAYS         = int(os.getenv("EXPIRY_ALERT_DAYS", "3"))
EXPIRY_POLL_SEC           = int(os.getenv("EXPIRY_POLL_SEC", "15"))

# bootstrap files
STORE_PARQUET             = os.getenv("STORE_PARQUET", "/data/store_inventory_final.parquet")
WH_BATCHES_PARQUET        = os.getenv("WH_BATCHES_PARQUET", "/data/warehouse_batches.parquet")
STORE_BATCHES_PARQUET     = os.getenv("STORE_BATCHES_PARQUET", "/data/store_batches.parquet")

# redis
REDIS_HOST                = os.getenv("REDIS_HOST", "redis")
REDIS_PORT                = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB                  = int(os.getenv("REDIS_DB", "0"))

# idempotenza
DEDUPE_TTL_SEC            = int(os.getenv("DEDUPE_TTL_SEC", "3600"))  # conserva gli event_id per 1h

# metrics (facoltative via log)
ENABLE_METRICS            = os.getenv("ENABLE_METRICS", "false").lower() in ("1","true","yes")

# logging
LOG_LEVEL                 = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='[%(asctime)s] %(levelname)s %(message)s'
)
log = logging.getLogger("shelf_restock_producer")

# ========================
# Helpers
# ========================
_shutdown = threading.Event()

def epoch(dt: datetime) -> int:
    return int(dt.timestamp())

def parse_dt(s: str) -> datetime:
    return pd.to_datetime(s).to_pydatetime()

def now_iso() -> str:
    return datetime.utcnow().isoformat()

def graceful_shutdown(signum, frame):
    log.info(f"Shutdown signal {signum} received, closing…")
    _shutdown.set()

signal.signal(signal.SIGINT, graceful_shutdown)
signal.signal(signal.SIGTERM, graceful_shutdown)

# ========================
# Kafka
# ========================
def build_producer() -> KafkaProducer:
    last_err = None
    for attempt in range(1, 7):
        try:
            prod = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                acks="all",
                retries=10,
                linger_ms=10,
                batch_size=16384,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            log.info("KafkaProducer connected.")
            return prod
        except NoBrokersAvailable as e:
            last_err = e
            log.warning(f"Kafka not available (attempt {attempt}/6). Retrying in 3s…")
            time.sleep(3)
    raise RuntimeError(f"Could not connect to Kafka: {last_err}")

def build_consumer() -> KafkaConsumer:
    cons = KafkaConsumer(
        TOPIC_SHELF_EVENTS,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        max_poll_interval_ms=300000,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    log.info(f"KafkaConsumer subscribed to {TOPIC_SHELF_EVENTS}")
    return cons

producer = build_producer()
consumer = build_consumer()

def send(topic: str, event: Dict[str, Any]):
    try:
        producer.send(topic, value=event)
        if ENABLE_METRICS:
            log.info(json.dumps({"metric":"kafka_send_ok","topic":topic,"etype":event.get("event_type")}))
    except KafkaError as e:
        log.error(f"Kafka send error to {topic}: {e}")
        if topic != TOPIC_DLQ:
            # Fallback DLQ
            try:
                producer.send(TOPIC_DLQ, value={"source_topic": topic, "payload": event, "error": str(e), "ts": now_iso()})
            except Exception as e2:
                log.error(f"DLQ send failed: {e2}")

# ========================
# Redis
# ========================
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

# Keyspace (schema):
# stock:<item_id>          -> int current shelf stock
# initial:<item_id>        -> int target shelf stock
# batch:<batch_id>         -> hash { item_id, expiry_ts, qty_wh, qty_store }
# wh:<item_id>             -> ZSET (score=expiry_ts, member=batch_id)
# store:<item_id>          -> ZSET (score=expiry_ts, member=batch_id)
# expiry:index             -> ZSET (score=expiry_ts, member=batch_id)
# dedupe:event             -> SET of event_id with TTL (via SETEX)
# bootstrap:done           -> "1"
# alerted:batches          -> SET of batches already alerted
# restock:lock:<item_id>   -> "1" (TTL = RESTOCK_DELAY_SEC)

def bootstrap_if_empty():
    if r.get("bootstrap:done"):
        log.info("Redis already bootstrapped.")
        return

    log.info("Bootstrapping Redis from parquet…")

    # Store inventory
    store_df = pd.read_parquet(STORE_PARQUET)
    req_cols = {"Item_Identifier", "current_stock", "initial_stock"}
    missing = req_cols - set(store_df.columns)
    if missing:
        raise ValueError(f"{STORE_PARQUET} missing columns: {missing}")

    pipe = r.pipeline()
    for _, row in store_df.iterrows():
        item = str(row["Item_Identifier"])
        pipe.set(f"stock:{item}", int(row["current_stock"]))
        pipe.set(f"initial:{item}", int(row["initial_stock"]))
    pipe.execute()

    # Warehouse batches
    wh = pd.read_parquet(WH_BATCHES_PARQUET)
    req_cols_wh = {"Batch_ID", "Item_Identifier", "Expiry_Date", "Batch_Quantity"}
    missing = req_cols_wh - set(wh.columns)
    if missing:
        raise ValueError(f"{WH_BATCHES_PARQUET} missing columns: {missing}")

    pipe = r.pipeline()
    for _, row in wh.iterrows():
        bid   = str(row["Batch_ID"])
        item  = str(row["Item_Identifier"])
        exp   = epoch(parse_dt(str(row["Expiry_Date"])))
        qty   = int(row["Batch_Quantity"])
        pipe.hset(f"batch:{bid}", mapping={
            "item_id": item,
            "expiry_ts": exp,
            "qty_wh": qty,
            "qty_store": 0
        })
        pipe.zadd(f"wh:{item}", {bid: exp})
        pipe.zadd("expiry:index", {bid: exp})
    pipe.execute()

    # Store batches (se presenti)
    if os.path.exists(STORE_BATCHES_PARQUET):
        sb = pd.read_parquet(STORE_BATCHES_PARQUET)
        if {"Batch_ID", "Item_Identifier", "Expiry_Date", "Batch_Quantity"}.issubset(sb.columns):
            pipe = r.pipeline()
            for _, row in sb.iterrows():
                bid   = str(row["Batch_ID"])
                item  = str(row["Item_Identifier"])
                exp   = epoch(parse_dt(str(row["Expiry_Date"])))
                qty   = int(row["Batch_Quantity"])

                if r.exists(f"batch:{bid}"):
                    pipe.hincrby(f"batch:{bid}", "qty_store", qty)
                else:
                    pipe.hset(f"batch:{bid}", mapping={
                        "item_id": item, "expiry_ts": exp,
                        "qty_wh": 0, "qty_store": qty
                    })
                    pipe.zadd("expiry:index", {bid: exp})
                pipe.zadd(f"store:{item}", {bid: exp})
            pipe.execute()

    r.set("bootstrap:done", "1")
    log.info("Bootstrap done.")

# ========================
# Restock core
# ========================
def schedule_restock(item_id: str):
    """ Imposta lock con TTL per evitare multi-scheduling e avvia timer """
    lock_key = f"restock:lock:{item_id}"
    if r.setnx(lock_key, "1"):
        r.expire(lock_key, RESTOCK_DELAY_SEC)
        send(TOPIC_ALERTS, {
            "event_type": "shelf_restock_alert",
            "item_id": item_id,
            "threshold": RESTOCK_THRESHOLD,
            "timestamp": now_iso()
        })
        log.info(f"Restock scheduled for {item_id} in {RESTOCK_DELAY_SEC}s")
        threading.Timer(RESTOCK_DELAY_SEC, perform_restock, args=(item_id,)).start()

def perform_restock(item_id: str):
    """
    Obiettivo: portare lo scaffale a initial_stock.
    - Si prendono lotti dal warehouse in ordine di scadenza (FEFO).
    - Emette eventi warehouse_pick e restock per i movimenti.
    - Se insufficiente, emette alert warehouse_restock_required.
    """
    try:
        current = int(r.get(f"stock:{item_id}") or 0)
        initial = int(r.get(f"initial:{item_id}") or current)
    except Exception:
        current, initial = 0, 0

    needed = max(0, initial - current)
    if needed == 0:
        log.info(f"{item_id}: at/above initial ({current}/{initial}). No action.")
        return

    needed = min(needed, RESTOCK_MAX_CHUNK)

    wh_key = f"wh:{item_id}"
    batch_ids = r.zrange(wh_key, 0, -1)  # earliest expiry first
    if not batch_ids:
        send(TOPIC_ALERTS, {
            "event_type": "warehouse_restock_required",
            "item_id": item_id,
            "missing_quantity": needed,
            "reason": "no_warehouse_batches",
            "timestamp": now_iso(),
        })
        log.warning(f"{item_id}: no warehouse batches; alert emitted.")
        return

    moved_total = 0
    ts = now_iso()

    for bid in batch_ids:
        if needed <= 0:
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

        pipe = r.pipeline()
        pipe.hincrby(bkey, "qty_wh", -move)
        pipe.hincrby(bkey, "qty_store", move)
        pipe.execute()

        r.zadd(f"store:{item_id}", {bid: exp_ts})
        r.incrby(f"stock:{item_id}", move)

        send(TOPIC_WAREHOUSE_EVENTS, {
            "event_type": "warehouse_pick",
            "item_id": item_id,
            "batch_id": bid,
            "quantity_picked": move,
            "timestamp": ts
        })
        send(TOPIC_SHELF_EVENTS, {
            "event_type": "restock",
            "item_id": item_id,
            "batch_id": bid,
            "quantity": move,
            "timestamp": ts
        })

        moved_total += move
        needed -= move

    if needed > 0:
        send(TOPIC_ALERTS, {
            "event_type": "warehouse_restock_required",
            "item_id": item_id,
            "missing_quantity": needed,
            "reason": "insufficient_warehouse_stock",
            "timestamp": now_iso(),
        })
        log.warning(f"{item_id}: partial refill (+{moved_total}); missing {needed}. Alert emitted.")
    else:
        log.info(f"{item_id}: fully refilled to initial (+{moved_total}).")

# ========================
# Expiry watcher
# ========================
def expiry_watcher():
    while not _shutdown.is_set():
        horizon = epoch(datetime.utcnow() + timedelta(days=EXPIRY_ALERT_DAYS))
        due = r.zrangebyscore("expiry:index", 0, horizon)
        for bid in due:
            if r.sadd("alerted:batches", bid):
                b = r.hgetall(f"batch:{bid}")
                if not b:
                    continue
                total_qty = int(b.get("qty_wh", 0)) + int(b.get("qty_store", 0))
                if total_qty <= 0:
                    continue
                send(TOPIC_ALERTS, {
                    "event_type": "expiry_alert",
                    "batch_id": bid,
                    "item_id": b.get("item_id"),
                    "expires_at": datetime.utcfromtimestamp(int(b.get("expiry_ts", 0))).isoformat(),
                    "qty_wh": int(b.get("qty_wh", 0)),
                    "qty_store": int(b.get("qty_store", 0)),
                    "days_threshold": EXPIRY_ALERT_DAYS,
                    "timestamp": now_iso()
                })
                log.info(f"Expiry alert emitted for batch {bid}")
        _shutdown.wait(EXPIRY_POLL_SEC)

# ========================
# Idempotenza
# ========================
def dedupe_event(event: Dict[str, Any]) -> bool:
    """
    Ritorna True se nuovo, False se già visto.
    Usa event_id se presente, altrimenti costruisce fingerprint semplice.
    """
    event_id = event.get("event_id")
    if not event_id:
        # hash minimale: etype|item|ts
        event_id = f"{event.get('event_type')}|{event.get('item_id')}|{event.get('timestamp')}"
    # SETNX con TTL via SET + NX + EX
    ok = r.set(f"dedupe:{event_id}", "1", ex=DEDUPE_TTL_SEC, nx=True)
    return bool(ok)

# ========================
# Event loop
# ========================
def handle_shelf_event(evt: Dict[str, Any]):
    etype = evt.get("event_type")
    item_id = evt.get("item_id")

    if not item_id or not etype:
        send(TOPIC_DLQ, {"reason":"missing_fields","payload":evt,"ts":now_iso()})
        log.warning(f"DLQ: missing fields in event {evt}")
        return

    if not dedupe_event(evt):
        # duplicato → ignora
        return

    if etype == "pickup":
        try:
            newv = r.decr(f"stock:{item_id}")
        except Exception:
            newv = int(r.get(f"stock:{item_id}") or 0) - 1
            r.set(f"stock:{item_id}", newv)

        if newv <= RESTOCK_THRESHOLD:
            schedule_restock(item_id)

    elif etype == "putback":
        r.incr(f"stock:{item_id}")
        # nessuna azione speciale

    elif etype == "restock":
        # potremmo riceverlo da altre sorgenti → best-effort: allineiamo stock se presente "quantity"
        qty = evt.get("quantity")
        if isinstance(qty, int) and qty > 0:
            r.incrby(f"stock:{item_id}", qty)

    else:
        # eventi non noti → DLQ
        send(TOPIC_DLQ, {"reason":"unknown_event_type","payload":evt,"ts":now_iso()})
        log.warning(f"DLQ: unknown event type {etype}")

def main():
    bootstrap_if_empty()
    # watcher scadenze
    threading.Thread(target=expiry_watcher, daemon=True).start()

    log.info("Listening to shelf_events (pickup/putback/restock)…")
    while not _shutdown.is_set():
        for msg in consumer:
            if _shutdown.is_set():
                break
            try:
                evt = msg.value
            except Exception as e:
                send(TOPIC_DLQ, {"reason":"json_deser_error","error":str(e),"ts":now_iso()})
                continue
            try:
                handle_shelf_event(evt)
            except Exception as e:
                send(TOPIC_DLQ, {"reason":"handler_exception","error":str(e),"payload":evt,"ts":now_iso()})
                log.exception("Error handling event")

    log.info("Flushing producer and closing…")
    try:
        producer.flush(5)
    except Exception:
        pass
    try:
        producer.close(5)
    except Exception:
        pass
    try:
        consumer.close()
    except Exception:
        pass
    log.info("Bye.")

if __name__ == "__main__":
    main()
