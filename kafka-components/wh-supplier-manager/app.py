import os
import time
import json
import uuid
import pandas as pd
from datetime import datetime, timedelta, timezone, date
from typing import Optional

# Import dal tuo modulo di tempo simulato
from simulated_time.clock import get_simulated_now

# Librerie per Delta Lake e Kafka
from deltalake import DeltaTable, write_deltalake
from confluent_kafka import Producer

# =========================
# Env / Config
# =========================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_WH_SUPPLIER_PLAN = os.getenv("TOPIC_WH_SUPPLIER_PLAN", "wh_supplier_plan")
TOPIC_WH_EVENTS        = os.getenv("TOPIC_WH_EVENTS", "wh_events")
TOPIC_ALERTS           = os.getenv("TOPIC_ALERTS", "alerts")

DELTA_ROOT = os.getenv("DELTA_ROOT", "/delta")
DL_SUPPLIER_PLAN_PATH = os.getenv("DL_SUPPLIER_PLAN_PATH", f"{DELTA_ROOT}/ops/wh_supplier_plan")
DL_ORDERS_PATH   = os.getenv("DL_ORDERS_PATH",   f"{DELTA_ROOT}/ops/wh_supplier_orders")
DL_RECEIPTS_PATH = os.getenv("DL_RECEIPTS_PATH", f"{DELTA_ROOT}/ops/wh_supplier_receipts")

CUTOFF_HOUR      = int(os.getenv("CUTOFF_HOUR", "12"))
DELIVERY_HOUR    = int(os.getenv("DELIVERY_HOUR", "8"))
DEFAULT_EXPIRY_DAYS = int(os.getenv("DEFAULT_EXPIRY_DAYS", "365"))

# Inizializza Producer Kafka
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

def now_utc() -> datetime:
    return get_simulated_now()

# =========================
# Helper Funzioni Temporali (Invariate)
# =========================
_LAST_CUTOFF_DATE: Optional[date] = None
_LAST_DELIVERY_DATE: Optional[date] = None

def _should_run_cutoff(ts: datetime) -> bool:
    global _LAST_CUTOFF_DATE
    if ts.weekday() not in (6, 1, 3): return False
    trigger_time = ts.replace(hour=CUTOFF_HOUR, minute=0, second=0)
    if ts < trigger_time or _LAST_CUTOFF_DATE == ts.date(): return False
    _LAST_CUTOFF_DATE = ts.date()
    return True

def _should_run_delivery(ts: datetime) -> bool:
    global _LAST_DELIVERY_DATE
    if ts.weekday() not in (0, 2, 4): return False
    trigger_time = ts.replace(hour=DELIVERY_HOUR, minute=0, second=0)
    if ts < trigger_time or _LAST_DELIVERY_DATE == ts.date(): return False
    _LAST_DELIVERY_DATE = ts.date()
    return True

def next_delivery_date(from_ts: datetime) -> date:
    d = from_ts.date()
    for i in range(0, 8):
        dd = d + timedelta(days=i)
        if dd.weekday() in (0, 2, 4):
            if i == 0 and from_ts.hour >= DELIVERY_HOUR: continue
            return dd
    return d + timedelta(days=7)

# =========================
# Logica di Business
# =========================

def do_cutoff(now_ts: datetime):
    print(f"[{now_ts}] Esecuzione CUTOFF...")
    try:
        dt_plan = DeltaTable(DL_SUPPLIER_PLAN_PATH)
        plans_df = dt_plan.to_pandas()
    except:
        print("Tabella piani non trovata.")
        return

    pending = plans_df[plans_df['status'] == 'pending'].copy()
    if pending.empty: return

    delivery = next_delivery_date(now_ts)
    
    # Crea Ordini
    orders = pending.groupby('shelf_id').agg({'suggested_qty': 'sum'}).reset_index()
    orders.rename(columns={'suggested_qty': 'total_qty'}, inplace=True)
    orders['order_id'] = [str(uuid.uuid4()) for _ in range(len(orders))]
    orders['delivery_date'] = pd.to_datetime(delivery).date()
    orders['status'] = 'issued'
    orders['cutoff_ts'] = now_ts
    orders['created_at'] = now_ts
    orders['updated_at'] = now_ts

    # Upsert su Delta (Usa append per semplicità, o merge se necessario)
    write_deltalake(DL_ORDERS_PATH, orders, mode="append")

    # Aggiorna Piani e manda a Kafka
    pending['status'] = 'issued'
    pending['updated_at'] = now_ts
    
    # Esegui il MERGE dei piani (per cambiare status in 'issued')
    dt_plan.merge(
        source=pending,
        predicate="t.supplier_plan_id = s.supplier_plan_id",
        source_alias="s", target_alias="t"
    ).when_matched_update(updates={"status": "s.status", "updated_at": "s.updated_at"}).execute()

    for _, row in pending.iterrows():
        msg = row.to_dict()
        producer.produce(TOPIC_WH_SUPPLIER_PLAN, key=str(row['shelf_id']), value=json.dumps(msg, default=str))
        
        # Invia Alert ACK
        alert = {"event_type": "alert_status_change", "shelf_id": str(row['shelf_id']), "status": "ack", "timestamp": str(now_ts)}
        producer.produce(TOPIC_ALERTS, value=json.dumps(alert))
    
    producer.flush()

def do_delivery(now_ts: datetime):
    print(f"[{now_ts}] Esecuzione DELIVERY...")
    today = now_ts.date()
    
    try:
        dt_orders = DeltaTable(DL_ORDERS_PATH)
        orders_df = dt_orders.to_pandas()
    except: return

    due = orders_df[(orders_df['delivery_date'] == today) & (orders_df['status'] == 'issued')].copy()
    if due.empty: return

    for _, row in due.iterrows():
        # 1. Evento WH_IN (Carico merce)
        event = {
            "event_type": "wh_in",
            "event_id": str(uuid.uuid4()),
            "shelf_id": row['shelf_id'],
            "qty": int(row['total_qty']),
            "timestamp": str(now_ts),
            "reason": "supplier_delivery",
            "expiry_date": str(today + timedelta(days=DEFAULT_EXPIRY_DAYS))
        }
        producer.produce(TOPIC_WH_EVENTS, value=json.dumps(event))

        # 2. Scrittura Ricevuta
        receipt = pd.DataFrame([{
            "receipt_id": str(uuid.uuid4()),
            "delivery_date": today,
            "shelf_id": row['shelf_id'],
            "received_qty": row['total_qty'],
            "created_at": now_ts
        }])
        write_deltalake(DL_RECEIPTS_PATH, receipt, mode="append")

    # 3. Aggiorna stato ordini a 'delivered' via Merge
    due['status'] = 'delivered'
    due['updated_at'] = now_ts
    dt_orders.merge(
        source=due,
        predicate="t.order_id = s.order_id",
        source_alias="s", target_alias="t"
    ).when_matched_update(updates={"status": "s.status", "updated_at": "s.updated_at"}).execute()

    producer.flush()

# =========================
# Main Loop
# =========================
if __name__ == "__main__":
    print("Warehouse Supplier Manager attivo (Polling ogni 10s)...")
    while True:
        try:
            ts = now_utc()
            if _should_run_cutoff(ts): do_cutoff(ts)
            if _should_run_delivery(ts): do_delivery(ts)
        except Exception as e:
            print(f"Errore nel loop: {e}")
        
        time.sleep(10)