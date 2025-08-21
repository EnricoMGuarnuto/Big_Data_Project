# kafka-producer-foot_traffic/Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install tools needed to fetch Kafka CLI
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Kafka CLI (only for topic creation on startup)
RUN curl -fsSL https://archive.apache.org/dist/kafka/3.4.0/kafka_2.12-3.4.0.tgz -o /tmp/kafka.tgz \
    && tar -xzf /tmp/kafka.tgz -C /opt \
    && mv /opt/kafka_2.12-3.4.0 /opt/kafka \
    && rm /tmp/kafka.tgz
ENV PATH="/opt/kafka/bin:${PATH}"

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App code
COPY foot_traffic_producer.py .

# Run: ensure topic exists then start producer
CMD ["sh", "-c", "kafka-topics.sh --create --topic foot_traffic --bootstrap-server ${KAFKA_BROKER} --if-not-exists && python3 -u foot_traffic_producer.py"]
