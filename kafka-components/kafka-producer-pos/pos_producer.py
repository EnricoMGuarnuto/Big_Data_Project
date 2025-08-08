import os
import json
import uuid
import time
import random
import redis
import pandas as pd
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
FOOT_TOPIC = os.getenv("FOOT_TOPIC", "foot_traffic")
POS_TOPIC = os.getenv("POS_TOPIC", "pos_transactions")
GROUP_ID = "pos-simulator"

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

# Percorso file prezzi (parquet o csv)
PRICE_FILE = os.getenv("PRICE_FILE", "/data/item_prices.parquet")  # es. /data/item_prices.csv

# --- Helpers ---
def load_price_map(path: str) -> dict:
    ext = os.path.splitext(path)[1].lower()
    if ext == ".parquet":
        df = pd.read_parquet(path)
    elif ext == ".csv":
        df = pd.read_csv(path)
    else:
        raise ValueError(f"Formato file prezzi non supportato: {ext} (usa .parquet o .csv)")

    # si assumono colonne: Item_Identifier, price
    if "Item_Identifier" not in df.columns or "price" not in df.columns:
        raise ValueError("Il file prezzi deve avere le colonne 'Item_Identifier' e 'price'")

    df = df.dropna(subset=["Item_Identifier", "price"])
    return df.set_index("Item_Identifier")["price"].astype(float).to_dict()

# --- Client Redis solo per sconti ---
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# --- Kafka ---
consumer = KafkaConsumer(
    FOOT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id=GROUP_ID,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# --- Dati di base ---
inventory = pd.read_parquet("/data/store_inventory_final.parquet")
price_by_item = load_price_map(PRICE_FILE)

print("POS Producer (foot_traffic listener) started...")

for msg in consumer:
    event = msg.value
    if event.get("event_type") != "foot_traffic":
        continue

    customer_id = event["customer_id"]
    exit_time = datetime.fromisoformat(event["exit_time"])

    # attende l'uscita del cliente (se necessario)
    now = datetime.utcnow()
    delay = (exit_time - now).total_seconds()
    if delay > 0:
        time.sleep(delay)

    transaction_id = str(uuid.uuid4())
    timestamp = exit_time.isoformat()

    # campiona 1-5 articoli dall'inventario
    num_items = random.randint(1, 5)
    items = inventory.sample(n=num_items)

    transaction = {
        "event_type": "pos_transaction",
        "transaction_id": transaction_id,
        "customer_id": customer_id,
        "timestamp": timestamp,
        "items": []
    }

    for _, row in items.iterrows():
        item_id = row["Item_Identifier"]
        quantity = random.randint(1, 3)

        # Prezzo da file (fallback opzionale)
        unit_price = price_by_item.get(item_id)
        if unit_price is None:
            # fallback: se proprio manca, ultimo tentativo su Redis price o default
            unit_price = float(r.get(f"price:{item_id}") or 5.0)

        # Sconto da Redis (0..1)
        discount = float(r.get(f"discount:{item_id}") or 0.0)
        discount = max(0.0, min(discount, 0.95))  # per sicurezza, clamp

        total_price = round(quantity * unit_price * (1 - discount), 2)

        transaction["items"].append({
            "item_id": item_id,
            "quantity": quantity,
            "unit_price": round(float(unit_price), 2),
            "discount": round(discount, 2),
            "total_price": total_price
        })

    producer.send(POS_TOPIC, value=transaction)
    print(f"Generated POS transaction: {transaction}")
