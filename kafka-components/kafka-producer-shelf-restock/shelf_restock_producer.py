import os
import json
import pandas as pd
from datetime import datetime
from kafka import KafkaProducer
import time

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "shelf_events")
RESTOCK_THRESHOLD = int(os.getenv("RESTOCK_THRESHOLD", 10))
RESTOCK_QUANTITY = int(os.getenv("RESTOCK_QUANTITY", 20))
SLEEP = float(os.getenv("SLEEP", 10))  # seconds

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Load datasets
store_inventory = pd.read_parquet("/data/store_inventory_final.parquet")
warehouse_batches = pd.read_parquet("/data/warehouse_batches.parquet")

print("Shelf Restock Producer started...")

while True:
    for idx, row in store_inventory.iterrows():
        item_id = row["Item_Identifier"]
        current_stock = row["current_stock"]

        if current_stock <= RESTOCK_THRESHOLD:
            item_batches = warehouse_batches[warehouse_batches["Item_Identifier"] == item_id]
            item_batches = item_batches[item_batches["Batch_Quantity"] > 0].sort_values(by="Expiry_Date")

            if item_batches.empty:
                print(f"No warehouse stock available for Item {item_id}")
                continue

            batch_to_use = item_batches.iloc[0]
            batch_id = batch_to_use["Batch_ID"]
            available_quantity = batch_to_use["Batch_Quantity"]

            quantity_to_restock = min(RESTOCK_QUANTITY, available_quantity)

            warehouse_batches.loc[warehouse_batches["Batch_ID"] == batch_id, "Batch_Quantity"] -= quantity_to_restock

            event = {
                "event_type": "restock",
                "item_id": item_id,
                "batch_id": batch_id,
                "quantity": quantity_to_restock,
                "timestamp": datetime.utcnow().isoformat()
            }

            producer.send(TOPIC, value=event)
            print(f"Restock event sent: {event}")

            store_inventory.loc[store_inventory["Item_Identifier"] == item_id, "current_stock"] += quantity_to_restock

    warehouse_batches.to_parquet("/data/warehouse_batches.parquet", index=False)
    time.sleep(SLEEP)
