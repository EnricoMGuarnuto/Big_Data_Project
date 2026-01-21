import os
import time
import json
import socket
import pandas as pd
from datetime import datetime
from typing import Optional

# Helper per tempo simulato e Delta
from simulated_time.redis_helpers import get_simulated_date, get_simulated_timestamp
from deltalake import DeltaTable, write_deltalake
from confluent_kafka import Producer

# =========================
# Env / Config
# =========================
KAFKA_BROKER     = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_ALERTS     = os.getenv("TOPIC_ALERTS", "alerts")
TOPIC_SHELF_STATE = os.getenv("TOPIC_SHELF_STATE", "shelf_state")
TOPIC_SHELF_BATCH_STATE = os.getenv("TOPIC_SHELF_BATCH_STATE", "shelf_batch_state")

DELTA_ROOT       = os.getenv("DELTA_ROOT", "/delta")
DL_SHELF_STATE   = os.getenv("DL_SHELF_STATE_PATH", f"{DELTA_ROOT}/cleansed/shelf_state")
DL_SHELF_BATCH   = os.getenv("DL_SHELF_BATCH_PATH", f"{DELTA_ROOT}/cleansed/shelf_batch_state")

REMOVE_MODE      = os.getenv("REMOVE_MODE", "next_day").lower()
POLL_SECONDS     = float(os.getenv("POLL_SECONDS", "1.0"))
KAFKA_WAIT_TIMEOUT_S  = int(os.getenv("KAFKA_WAIT_TIMEOUT_S", "180"))
KAFKA_WAIT_POLL_S     = float(os.getenv("KAFKA_WAIT_POLL_S", "2.0"))

def _parse_first_broker(brokers: str) -> tuple[str, int]:
    first = (brokers or "").split(",")[0].strip()
    if not first:
        return "kafka", 9092
    if "://" in first:
        first = first.split("://", 1)[1]
    host, _, port_s = first.partition(":")
    return host, int(port_s or "9092")


def wait_for_kafka() -> None:
    host, port = _parse_first_broker(KAFKA_BROKER)
    deadline = time.time() + KAFKA_WAIT_TIMEOUT_S
    attempt = 0
    while time.time() < deadline:
        attempt += 1
        try:
            with socket.create_connection((host, port), timeout=2.0):
                print(f"[removal_scheduler] Kafka ready at {host}:{port}")
                return
        except Exception as e:
            if attempt % 5 == 0:
                remaining = int(max(0, deadline - time.time()))
                print(f"[removal_scheduler] Waiting Kafka at {host}:{port}… ({remaining}s left) last_err={e}")
            time.sleep(KAFKA_WAIT_POLL_S)
    raise RuntimeError(f"Kafka not reachable at {host}:{port} after {KAFKA_WAIT_TIMEOUT_S}s")


def _producer() -> Producer:
    return Producer({"bootstrap.servers": KAFKA_BROKER})

def delta_ready() -> bool:
    return os.path.exists(DL_SHELF_STATE) and os.path.exists(DL_SHELF_BATCH)

def run_once(sim_date_str: str, sim_ts_str: str) -> None:
    print(f"[removal_scheduler] Controllo scadenze per la data simulata: {sim_date_str}")
    
    # 1) Caricamento tabelle con Delta-rs (Zero Spark!)
    dt_state = DeltaTable(DL_SHELF_STATE)
    dt_batch = DeltaTable(DL_SHELF_BATCH)
    
    df_state = dt_state.to_pandas()
    df_batch = dt_batch.to_pandas()

    today = pd.to_datetime(sim_date_str).date()

    # 2) Logica di rimozione
    if REMOVE_MODE == "same_day_evening":
        expired_mask = (df_batch['expiry_date'] <= today) & (df_batch['batch_quantity_store'] > 0)
    else:
        expired_mask = (df_batch['expiry_date'] < today) & (df_batch['batch_quantity_store'] > 0)

    expired_batches = df_batch[expired_mask].copy()

    if expired_batches.empty:
        print(f"[removal_scheduler] Nessun prodotto scaduto trovato per {sim_date_str}.")
        return

    # 3) Calcolo quantità da rimuovere
    removed_per_shelf = expired_batches.groupby('shelf_id')['batch_quantity_store'].sum().reset_index()
    removed_per_shelf.rename(columns={'batch_quantity_store': 'removed_qty'}, inplace=True)
    removed_qty_map = dict(zip(removed_per_shelf["shelf_id"].astype(str), removed_per_shelf["removed_qty"].astype(int)))

    # 4) Aggiornamento stato scaffali (Shelf State)
    new_state = df_state.merge(removed_per_shelf, on='shelf_id', how='inner')
    new_state['current_stock'] = (new_state['current_stock'] - new_state['removed_qty']).clip(lower=0)
    new_state['last_update_ts'] = pd.to_datetime(sim_ts_str)

    # Upsert su Delta Shelf State
    dt_state.merge(
        source=new_state[['shelf_id', 'current_stock', 'last_update_ts']],
        predicate="t.shelf_id = s.shelf_id",
        source_alias="s", target_alias="t"
    ).when_matched_update_all().execute()

    # 5) Aggiornamento lotti (Shelf Batch State)
    expired_batches['batch_quantity_store'] = 0
    expired_batches['last_update_ts'] = pd.to_datetime(sim_ts_str)

    dt_batch.merge(
        source=expired_batches[['shelf_id', 'batch_code', 'batch_quantity_store', 'last_update_ts']],
        predicate="t.shelf_id = s.shelf_id AND t.batch_code = s.batch_code",
        source_alias="s", target_alias="t"
    ).when_matched_update_all().execute()

    # 6) Notifiche Kafka
    kp = _producer()
    for _, row in new_state.iterrows():
        try:
            kp.produce(TOPIC_SHELF_STATE, key=str(row['shelf_id']), value=row.to_json())
        except Exception as e:
            print(f"[removal_scheduler] Kafka produce failed (shelf_state) shelf={row['shelf_id']}: {e}")
    
    for _, row in expired_batches.iterrows():
        key = f"{row['shelf_id']}::{row['batch_code']}"
        try:
            kp.produce(TOPIC_SHELF_BATCH_STATE, key=key, value=row.to_json())
        except Exception as e:
            print(f"[removal_scheduler] Kafka produce failed (shelf_batch_state) key={key}: {e}")
        
        # Alert rimozione
        alert = {
            "event_type": "expired_removal", "shelf_id": row['shelf_id'], 
            "location": "store", "suggested_qty": int(removed_qty_map.get(str(row["shelf_id"]), 0)),
            "created_at": sim_ts_str
        }
        try:
            kp.produce(TOPIC_ALERTS, value=json.dumps(alert))
        except Exception as e:
            print(f"[removal_scheduler] Kafka produce failed (alerts) shelf={row['shelf_id']}: {e}")

    try:
        kp.flush(30)
    except Exception as e:
        print(f"[removal_scheduler] Kafka flush failed: {e}")
    print(f"[removal_scheduler] Rimossi {len(expired_batches)} lotti scaduti.")

def main():
    wait_for_kafka()
    last_processed_day = None
    while True:
        if not delta_ready():
            time.sleep(POLL_SECONDS)
            continue

        sim_day = get_simulated_date()
        sim_ts  = get_simulated_timestamp()

        if sim_day != last_processed_day:
            try:
                run_once(sim_day, sim_ts)
                last_processed_day = sim_day
            except Exception as e:
                print(f"Errore: {e}")

        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main()
