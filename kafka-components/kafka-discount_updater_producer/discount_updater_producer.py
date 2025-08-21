import os
import json
import time
import random
import pandas as pd
from datetime import datetime, timedelta
from kafka import KafkaProducer

# --- Config ---
DISCOUNT_TOPIC = os.getenv("DISCOUNT_TOPIC", "weekly_discounts")
INVENTORY_FILE = os.getenv("INVENTORY_FILE", "/data/store_inventory_final.parquet")
OUTPUT_JSON    = os.getenv("DISCOUNT_JSON_FILE", "/data/current_discounts.json")

K_ITEMS = int(os.getenv("DISCOUNT_K_ITEMS", 10))
DISCOUNT_MIN = float(os.getenv("DISCOUNT_MIN", 0.05))
DISCOUNT_MAX = float(os.getenv("DISCOUNT_MAX", 0.30))

def build_producer():
    return KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BROKER", "kafka:9092"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        linger_ms=10,
        retries=5
    )

def get_current_year_week():
    today = datetime.utcnow()
    iso_year, iso_week, _ = today.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"

def generate_discounts(week_id, item_ids):
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
    with open(path, "w") as f:
        json.dump(evt["discounts"], f, indent=2)
    print(f"[discount] Wrote discounts to JSON: {path}")

def main():
    print("[discount] Starting weekly discount updater")

    df = pd.read_parquet(INVENTORY_FILE)
    item_ids = sorted(set(df["Item_Identifier"].astype(str)))
    current_week = get_current_year_week()
    evt = generate_discounts(current_week, item_ids)

    # Send to Kafka
    producer = build_producer()
    producer.send(DISCOUNT_TOPIC, value=evt)
    producer.flush()
    producer.close()
    print(f"[discount] Sent Kafka event for week {current_week}")

    # Save to JSON
    write_discounts_to_file(evt, OUTPUT_JSON)

if __name__ == "__main__":
    main()
