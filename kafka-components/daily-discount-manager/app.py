import os
import time
import json
import socket
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional

from simulated_time.redis_helpers import get_simulated_date, get_simulated_timestamp
from deltalake import DeltaTable, write_deltalake
from confluent_kafka import Producer

# =========================
# Env / Config
# =========================
KAFKA_BROKER          = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_DAILY_DISCOUNTS = os.getenv("TOPIC_DAILY_DISCOUNTS", "daily_discounts")
TOPIC_ALERTS          = os.getenv("TOPIC_ALERTS", "alerts")

DELTA_ROOT            = os.getenv("DELTA_ROOT", "/delta")
DL_SHELF_BATCH_PATH   = os.getenv("DL_SHELF_BATCH_PATH", f"{DELTA_ROOT}/cleansed/shelf_batch_state")
DL_DAILY_DISC_PATH    = os.getenv("DL_DAILY_DISC_PATH", f"{DELTA_ROOT}/analytics/daily_discounts")

DISCOUNT_MIN          = float(os.getenv("DISCOUNT_MIN", "0.30"))
DISCOUNT_MAX          = float(os.getenv("DISCOUNT_MAX", "0.50"))
POLL_SECONDS          = float(os.getenv("POLL_SECONDS", "10.0"))

KAFKA_WAIT_TIMEOUT_S  = int(os.getenv("KAFKA_WAIT_TIMEOUT_S", "180"))
KAFKA_WAIT_POLL_S     = float(os.getenv("KAFKA_WAIT_POLL_S", "2.0"))
LOG_LEVEL             = os.getenv("LOG_LEVEL", "INFO").upper()


def log_info(msg: str) -> None:
    if LOG_LEVEL in ("INFO", "DEBUG"):
        print(msg)


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
                log_info(f"[daily_discount_manager] Kafka ready at {host}:{port}")
                return
        except Exception as e:
            if attempt % 5 == 0:
                remaining = int(max(0, deadline - time.time()))
                log_info(f"[daily_discount_manager] Waiting Kafka at {host}:{port}... ({remaining}s left) last_err={e}")
            time.sleep(KAFKA_WAIT_POLL_S)
    raise RuntimeError(f"Kafka not reachable at {host}:{port} after {KAFKA_WAIT_TIMEOUT_S}s")


def _producer() -> Producer:
    return Producer({"bootstrap.servers": KAFKA_BROKER})

def run_discount_job(sim_date_str, sim_ts_str):
    log_info(f"[daily_discount_manager] Starting discount calc for {sim_date_str}...")
    
    # 1) Lettura stato lotti
    try:
        dt_batch = DeltaTable(DL_SHELF_BATCH_PATH)
        df_batches = dt_batch.to_pandas()
    except Exception as e:
        print(f"[daily_discount_manager] Batch read failed: {e}")
        return

    today = pd.to_datetime(sim_date_str).date()
    tomorrow = today + timedelta(days=1)

    # 2) Filtro prodotti in scadenza (oggi o domani)
    mask = (df_batches['batch_quantity_store'] > 0) & \
           (df_batches['expiry_date'] >= today) & \
           (df_batches['expiry_date'] <= tomorrow)
    
    expiring = df_batches[mask][['shelf_id', 'expiry_date']].drop_duplicates()
    if expiring.empty:
        log_info("[daily_discount_manager] No products expiring today/tomorrow.")
        return

    # 3) Generazione candidati (shelf_id, discount_date)
    # Per chi scade oggi -> sconto oggi. Per chi scade domani -> sconto oggi E domani.
    candidates = []
    for _, row in expiring.iterrows():
        candidates.append({'shelf_id': row['shelf_id'], 'discount_date': today})
        if row['expiry_date'] == tomorrow:
            candidates.append({'shelf_id': row['shelf_id'], 'discount_date': tomorrow})
    
    df_candidates = pd.DataFrame(candidates).drop_duplicates()

    # 4) Caricamento sconti esistenti (se la tabella esiste)
    try:
        dt_disc = DeltaTable(DL_DAILY_DISC_PATH)
        df_existing = dt_disc.to_pandas()[['shelf_id', 'discount_date', 'discount']]
        df_existing.rename(columns={'discount': 'base_discount'}, inplace=True)
    except:
        df_existing = pd.DataFrame(columns=['shelf_id', 'discount_date', 'base_discount'])

    # 5) Calcolo nuovi sconti
    # Genera extra_discount random (passi da 0.10)
    steps = int(round((DISCOUNT_MAX - DISCOUNT_MIN) / 0.10)) + 1
    df_candidates['extra_discount'] = np.random.choice(
        np.round(np.arange(DISCOUNT_MIN, DISCOUNT_MAX + 0.01, 0.10), 2), 
        size=len(df_candidates)
    )

    df_final = df_candidates.merge(df_existing, on=['shelf_id', 'discount_date'], how='left')
    df_final['base_discount'] = df_final['base_discount'].fillna(0.0)
    
    # Formula composta: 1 - (1 - d1) * (1 - d2)
    df_final['discount'] = 1.0 - (1.0 - df_final['base_discount']) * (1.0 - df_final['extra_discount'])
    df_final['created_at'] = pd.to_datetime(sim_ts_str)

    # 6) Salvataggio Delta (Upsert)
    result_df = df_final[['shelf_id', 'discount_date', 'discount', 'created_at']]
    
    if not df_existing.empty:
        dt_disc.merge(
            source=result_df,
            predicate="t.shelf_id = s.shelf_id AND t.discount_date = s.discount_date",
            source_alias="s", target_alias="t"
        ).when_matched_update_all().when_not_matched_insert_all().execute()
    else:
        write_deltalake(DL_DAILY_DISC_PATH, result_df, mode="append")

    # 7) Invio Kafka
    kp = _producer()
    for _, row in result_df.iterrows():
        val = row.to_dict()
        try:
            kp.produce(TOPIC_DAILY_DISCOUNTS, key=str(row['shelf_id']), value=json.dumps(val, default=str))
        except Exception as e:
            print(f"[daily_discount_manager] Kafka produce failed (daily_discounts) shelf={row['shelf_id']}: {e}")
        
        # Alert opzionale
        alert = {
            "event_type": "near_expiry_discount", "shelf_id": row['shelf_id'],
            "location": "store", "created_at": sim_ts_str
        }
        try:
            kp.produce(TOPIC_ALERTS, value=json.dumps(alert))
        except Exception as e:
            print(f"[daily_discount_manager] Kafka produce failed (alerts) shelf={row['shelf_id']}: {e}")
    
    try:
        kp.flush(30)
    except Exception as e:
        print(f"[daily_discount_manager] Kafka flush failed: {e}")
    log_info(f"[daily_discount_manager] Updated discounts for {len(result_df)} shelves.")

if __name__ == "__main__":
    wait_for_kafka()
    last_day = None
    while True:
        sim_day = get_simulated_date()
        sim_ts = get_simulated_timestamp()
        
        if sim_day and sim_day != last_day:
            try:
                run_discount_job(sim_day, sim_ts)
                last_day = sim_day
            except Exception as e:
                print(f"[daily_discount_manager] Error while running discount job: {e}")
        
        time.sleep(POLL_SECONDS)
