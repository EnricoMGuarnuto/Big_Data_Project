# kafka-producer-foot_traffic/Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App code
COPY foot_traffic_producer.py .

# Run only the producer (Kafka crea il topic se non c’è)
CMD ["python3", "-u", "foot_traffic_producer.py"]

