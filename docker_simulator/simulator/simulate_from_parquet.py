import pandas as pd
import random
import time
from datetime import datetime
from kafka import KafkaProducer
import json

# Carica i due dataset
store_df = pd.read_parquet("store_inventory.parquet")
warehouse_df = pd.read_parquet("warehouse_inventory.parquet")

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Simulatore eventi Kafka AVVIATO...\n")

try:
    while True:
        row = store_df.sample(1).iloc[0]
        sku = row["Item_Identifier"]
        initial_store = int(row["initial_stock"])
        current_store = int(row["current_stock"])

        warehouse_row = warehouse_df[warehouse_df["Item_Identifier"] == sku]
        if warehouse_row.empty:
            time.sleep(0.5)
            continue

        initial_warehouse = int(warehouse_row.iloc[0]["initial_stock"])
        current_warehouse = int(warehouse_row.iloc[0]["current_stock"])

        delta_store = initial_store - current_store
        delta_warehouse = initial_warehouse - current_warehouse

        timestamp = datetime.utcnow().isoformat()

        # Simula vendita
        if delta_store > 0:
            sales = random.randint(1, delta_store)
            store_df.loc[store_df["Item_Identifier"] == sku, "current_stock"] -= sales
            event = {"sku": sku, "qty_sold": sales, "store": "store_01", "timestamp": timestamp}
            producer.send("sales.events", event)
            print(f"[sales] {event}")

        # Simula trasferimento se lo store è sotto soglia e warehouse ha stock
        if current_store < 5 and current_warehouse > 0:
            qty = min(10, current_warehouse)  # al massimo 10 alla volta
            store_df.loc[store_df["Item_Identifier"] == sku, "current_stock"] += qty
            warehouse_df.loc[warehouse_df["Item_Identifier"] == sku, "current_stock"] -= qty
            event = {
                "sku": sku,
                "qty_moved": qty,
                "from": "warehouse",
                "to": "store_01",
                "timestamp": timestamp
            }
            producer.send("transfer.events", event)
            print(f"[transfer] {event}")

        # Simula spoilage
        if random.random() < 0.05:
            adjustment = random.randint(1, 3)
            store_df.loc[store_df["Item_Identifier"] == sku, "current_stock"] -= adjustment
            event = {
                "sku": sku,
                "qty_adjusted": adjustment,
                "reason": "spoilage",
                "location": "store_01",
                "timestamp": timestamp
            }
            producer.send("adjustment.events", event)
            print(f"[adjustment] {event}")

        time.sleep(random.uniform(1, 3))

except KeyboardInterrupt:
    print("Simulatore interrotto manualmente.")
