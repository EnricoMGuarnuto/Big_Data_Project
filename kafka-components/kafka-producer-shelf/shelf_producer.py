import os
import json
import time
import pandas as pd
import random
from kafka import KafkaProducer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "shelf_events")
SLEEP = float(os.getenv("SLEEP", 2))

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Load store inventory
store_inventory = pd.read_parquet("/data/store_inventory_final.parquet")

print("Shelf Sensors Producer started...")

while True:
    row = store_inventory.sample(1).iloc[0]
    item_id = row["Item_Identifier"]
    current_stock = row["current_stock"]
    item_weight = row["Item_Weight"]

    event_type = random.choices(["pickup", "putback"], weights=[0.9, 0.1])[0]

    if event_type == "pickup" and current_stock > 0:
        # Simulate pickup
        store_inventory.loc[store_inventory["Item_Identifier"] == item_id, "current_stock"] -= 1
        event = {
            "event_type": "pickup",
            "item_id": item_id,
            "weight": item_weight,
            "timestamp": time.time()
        }
        producer.send(TOPIC, value=event)
        print(f"Sent pickup event: {event}")

    elif event_type == "putback":
        # Simulate putback
        store_inventory.loc[store_inventory["Item_Identifier"] == item_id, "current_stock"] += 1
        event = {
            "event_type": "putback",
            "item_id": item_id,
            "weight": item_weight,
            "timestamp": time.time()
        }
        producer.send(TOPIC, value=event)
        print(f"Sent putback event: {event}")

    time.sleep(SLEEP)
