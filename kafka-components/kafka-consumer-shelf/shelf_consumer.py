import os
import json
import pandas as pd
from kafka import KafkaConsumer

# Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "shelf_events")

# Kafka Consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print(f"Consumer listening on topic: {TOPIC}")

# Load batches datasets
store_batches = pd.read_parquet("/data/store_batches.parquet")
warehouse_batches = pd.read_parquet("/data/warehouse_batches.parquet")

for message in consumer:
    event = message.value
    print(f"Received message: {event}")

    if event.get("event_type") == "restock":
        batch_id = event["batch_id"]
        quantity = event["quantity"]

        # Check if batch_id exists in store_batches
        if batch_id in store_batches["Batch_ID"].values:
            # Batch already exists → just increment quantity
            store_batches.loc[store_batches["Batch_ID"] == batch_id, "Batch_Quantity"] += quantity
            print(f"Updated existing batch {batch_id} with +{quantity} units.")
        else:
            # Batch not in store_batches → add it from warehouse_batches
            batch_row = warehouse_batches[warehouse_batches["Batch_ID"] == batch_id]

            if not batch_row.empty:
                new_entry = batch_row.copy()
                new_entry["Batch_Quantity"] = quantity  # Only add what's being taken for shelves
                store_batches = pd.concat([store_batches, new_entry], ignore_index=True)
                print(f"Added new batch {batch_id} to store_batches with {quantity} units.")
            else:
                print(f"Error: Batch {batch_id} not found in warehouse_batches.")

        # Save updated batches (overwrite for simplicity)
        store_batches.to_parquet("/data/store_batches.parquet", index=False)
