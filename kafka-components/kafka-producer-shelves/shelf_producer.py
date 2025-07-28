from kafka import KafkaProducer
import json
import pandas as pd
import random
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

df = pd.read_parquet('data/store_inventory.parquet')

while True:
    row = df.sample(1).iloc[0]
    event = {
        "event_type": "shelf_pick",
        "item_id": row["Item_Identifier"],
        "weight_removed": round(random.uniform(0.5, 1.5) * row["Item_Weight"], 2),
        "timestamp": datetime.utcnow().isoformat()
    }
    producer.send("shelf_events", value=event)
    print("Sent:", event)
    time.sleep(1)
