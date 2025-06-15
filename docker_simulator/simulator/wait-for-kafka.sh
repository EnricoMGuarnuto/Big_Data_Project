#!/bin/bash
set -e

echo "Waiting for Kafka on kafka:9092..."

# loop finché kafka non risponde
while ! nc -z kafka 9092; do
  sleep 1
done

echo "Kafka is ready, starting the Python script..."
exec python simulate_from_parquet.py
