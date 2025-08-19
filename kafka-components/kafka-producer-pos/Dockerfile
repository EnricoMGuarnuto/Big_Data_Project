FROM eclipse-temurin:17-jre-focal

ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install Python and core dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Install Kafka tools
RUN curl -O https://archive.apache.org/dist/kafka/3.4.0/kafka_2.12-3.4.0.tgz \
    && tar -xzf kafka_2.12-3.4.0.tgz \
    && mv kafka_2.12-3.4.0 /opt/kafka \
    && rm kafka_2.12-3.4.0.tgz
ENV PATH="/opt/kafka/bin:${PATH}"

COPY requirements.txt .
RUN pip install -r requirements.txt

# code
COPY pos_producer.py .

# Start
CMD [ "sh", "-c", "kafka-topics.sh --create --topic pos_receipts --bootstrap-server ${KAFKA_BROKER} --if-not-exists && kafka-topics.sh --create --topic pos_transactions --bootstrap-server ${KAFKA_BROKER} --if-not-exists && python3 pos_producer.py" ]