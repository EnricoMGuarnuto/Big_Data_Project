import os
from kafka import KafkaConsumer
import json

# === Config ===
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "foot_traffic")

# === Consumer ===
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print(f"✅ Consumer in ascolto sul topic: {TOPIC}")
for message in consumer:
    print(f"📥 Ricevuto: {message.value}")