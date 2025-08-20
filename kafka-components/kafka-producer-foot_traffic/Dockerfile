# Base con JRE + Ubuntu 22.04 (python3=3.10)
FROM eclipse-temurin:17-jre-jammy

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Kafka tools
RUN curl -fsSLO https://archive.apache.org/dist/kafka/3.4.0/kafka_2.12-3.4.0.tgz \
 && tar -xzf kafka_2.12-3.4.0.tgz \
 && mv kafka_2.12-3.4.0 /opt/kafka \
 && rm kafka_2.12-3.4.0.tgz
ENV PATH="/opt/kafka/bin:${PATH}"

COPY requirements.txt .
RUN python3 -m pip install --no-cache-dir -r requirements.txt

COPY foot_traffic_producer.py .

CMD ["sh", "-c", "kafka-topics.sh --create --topic foot_traffic --bootstrap-server ${KAFKA_BROKER} --if-not-exists && exec python3 -u foot_traffic_producer.py"]
