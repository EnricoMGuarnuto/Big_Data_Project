FROM eclipse-temurin:17-jre-focal

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Install Python and core dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip tini ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

# Install Kafka tools
RUN curl -O https://archive.apache.org/dist/kafka/3.4.0/kafka_2.12-3.4.0.tgz \
    && tar -xzf kafka_2.12-3.4.0.tgz \
    && mv kafka_2.12-3.4.0 /opt/kafka \
    && rm kafka_2.12-3.4.0.tgz
ENV PATH="/opt/kafka/bin:${PATH}"

# Crea un utente non-root
ARG APP_USER=appuser
RUN useradd -m -u 10001 ${APP_USER}

# Installa dipendenze Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia lo script
COPY shelf_producer.py .

# Imposta permessi
RUN chown -R ${APP_USER}:${APP_USER} /app
USER ${APP_USER}

# Healthcheck aggiornato (rimosso import redis)
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python3 -c "import pandas, kafka, pyarrow; print('ok')" || exit 1

# Entrypoint con tini
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["sh", "-c", "kafka-topics.sh --create --topic shelf_events --bootstrap-server ${KAFKA_BROKER} --if-not-exists && python3 shelf_producer.py"]
