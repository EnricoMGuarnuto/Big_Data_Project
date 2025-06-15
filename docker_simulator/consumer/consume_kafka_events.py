from kafka import KafkaConsumer
import json

TOPICS = ["sales.events", "transfer.events", "adjustment.events"]

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers="kafka:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="stock-monitor",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print(f"In ascolto sui topic: {TOPICS}...\n")

try:
    for message in consumer:
        topic = message.topic
        data = message.value
        print(f"[{topic}] {data}")
except KeyboardInterrupt:
    print("Interrotto manualmente")
