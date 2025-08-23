import os
import json
import random
import time
import pandas as pd
from datetime import datetime
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable

# --- Config ---
KAFKA_BROKER   = os.getenv("KAFKA_BROKER", "kafka:9092")
DISCOUNT_TOPIC = os.getenv("DISCOUNT_TOPIC", "weekly_discounts")

INVENTORY_FILE = os.getenv("INVENTORY_FILE", "/data/store_inventory_final.parquet")
OUTPUT_JSON    = os.getenv("DISCOUNT_JSON_FILE", "/data/current_discounts.json")

K_ITEMS = int(os.getenv("DISCOUNT_K_ITEMS", 10))
DISCOUNT_MIN = float(os.getenv("DISCOUNT_MIN", 0.05))
DISCOUNT_MAX = float(os.getenv("DISCOUNT_MAX", 0.30))


# --- Kafka topic ensure ---
def ensure_topic(topic, bootstrap, partitions=1, rf=1, attempts=10, sleep_s=3):
    last = None
    for i in range(1, attempts+1):
        try:
            admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="discount-init")
            if topic not in admin.list_topics():
                admin.create_topics([NewTopic(name=topic, num_partitions=partitions, replication_factor=rf)])
                print(f"[discount] ✅ Creato topic {topic}")
            else:
                print(f"[discount] ℹ️ Topic {topic} già esistente")
            admin.close()
            return
        except NoBrokersAvailable as e:
            last = e
            print(f"[discount] ⚠️ Kafka non pronto (tentativo {i}/{attempts}). Retry fra {sleep_s}s…")
            time.sleep(sleep_s)
        except Exception as e:
            print(f"[discount] ⚠️ Errore creazione/verifica topic: {e}")
            return
    print(f"[discount] ❌ Impossibile creare/verificare topic {topic}: {last}")


# --- Kafka Producer ---
def build_producer():
    for attempt in range(10):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                linger_ms=10,
                retries=5
            )
        except Exception as e:
            print(f"[discount] ⚠️ Kafka non disponibile (tentativo {attempt+1}/10): {e}")
            time.sleep(5)
    raise RuntimeError("[discount] ❌ Impossibile connettersi a Kafka dopo 10 tentativi")


# --- Helpers ---
def get_current_year_week():
    today = datetime.utcnow()
    iso_year, iso_week, _ = today.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


def generate_discounts(week_id, item_ids):
    if not item_ids:
        print("[discount] ⚠️ Nessun item disponibile per generare sconti!")
        return None

    selected = random.sample(item_ids, min(K_ITEMS, len(item_ids)))
    discounts = [
        {
            "item_id": item,
            "discount": round(random.uniform(DISCOUNT_MIN, DISCOUNT_MAX), 2)
        }
        for item in selected
    ]
    return {
        "event_type": "weekly_discount",
        "week": week_id,
        "discounts": discounts,
        "created_at": datetime.utcnow().isoformat()
    }


def write_discounts_to_file(evt, path):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(evt["discounts"], f, indent=2)
        print(f"[discount] ✅ Salvato file sconti: {path}")
    except Exception as e:
        print(f"[discount] ⚠️ Errore scrivendo {path}: {e}")


# --- Main ---
def main():
    try:
        print("[discount] 🚀 Avvio weekly discount updater")

        ensure_topic(DISCOUNT_TOPIC, KAFKA_BROKER)

        if not os.path.exists(INVENTORY_FILE):
            raise FileNotFoundError(f"[discount] ❌ Inventory parquet non trovato: {INVENTORY_FILE}")

        df = pd.read_parquet(INVENTORY_FILE)

        # Usa shelf_id se presente, altrimenti fallback su Item_Identifier
        col_name = "shelf_id" if "shelf_id" in df.columns else "Item_Identifier"
        if col_name not in df.columns:
            raise ValueError("[discount] ❌ Inventory parquet manca colonna 'shelf_id' o 'Item_Identifier'")

        item_ids = sorted(set(df[col_name].astype(str)))
        print(f"[discount] 📦 Catalogo caricato: {len(item_ids)} articoli trovati")

        current_week = get_current_year_week()
        evt = generate_discounts(current_week, item_ids)
        if evt is None:
            return

        # Invia a Kafka
        producer = build_producer()
        producer.send(DISCOUNT_TOPIC, value=evt)
        producer.flush()
        producer.close()
        print(f"[discount] ✅ Inviato evento Kafka per settimana {current_week}")

        # Scrivi JSON per i producer shelf/pos
        write_discounts_to_file(evt, OUTPUT_JSON)

    except Exception as e:
        print(f"[discount] ❌ Errore in main: {e}")


# --- Loop settimanale ---
if __name__ == "__main__":
    while True:
        main()
        # ⬇️ per test puoi ridurre a 60 secondi
        time.sleep(60 * 60 * 24 * 7)  # ogni 7 giorni
