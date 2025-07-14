
import json
import time
import pandas as pd
from kafka import KafkaProducer

KAFKA_BROKER = 'localhost:9092'
TOPIC = 'pos_transactions'

def stream_pos_parquet(file_path):
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    df = pd.read_parquet(file_path)
    for _, row in df.iterrows():
        message = row.dropna().to_dict()
        producer.send(TOPIC, message)
        print(f"Sent to {TOPIC}: {message}")
        time.sleep(0.5)  # simulate streaming

    producer.flush()
    producer.close()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="Send POS transactions from Parquet to Kafka topic.")
    parser.add_argument('--file', type=str, required=True, help='Path to the POS Parquet file')
    args = parser.parse_args()

    stream_pos_parquet(args.file)
