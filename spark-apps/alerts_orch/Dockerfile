FROM bitnami/spark:3.5.1

USER root

# Dipendenze di sistema (psycopg2)
RUN apt-get update --fix-missing && apt-get install -y --no-install-recommends \
    wget curl unzip libpq-dev build-essential \
 && rm -rf /var/lib/apt/lists/*

# Spark jars path
ENV SPARK_HOME=/opt/bitnami/spark
WORKDIR $SPARK_HOME/jars

# === Scarica librerie extra se ti servono anche qui (Postgres, Kafka) ===
RUN curl -L --retry 5 --retry-all-errors -o postgresql-42.6.0.jar \
        https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar && \
    curl -L --retry 5 --retry-all-errors -o kafka-clients-3.5.1.jar \
        https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar

# Torna in /app
WORKDIR /app

# Python deps
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copia il job orchestratore
COPY app.py /app/app.py

# Env di default (override da docker-compose)
ENV SPARK_APP_NAME="alerts-orch" \
    CHECKPOINT_DIR="/chk/alerts_orch" \
    LOW_STOCK_INTERVAL_SECS="10" \
    NEAR_EXP_INTERVAL_MINS="10" \
    PG_HOST="postgres" \
    PG_PORT="5432" \
    PG_DB="retaildb" \
    PG_USER="retail" \
    PG_PASS="retailpass" \
    KAFKA_BOOTSTRAP_SERVERS="kafka:9092" \
    TOPIC_SHELF="shelf_events" \
    EMIT_KAFKA_REFILL="true" \
    EMIT_REFILL_WEIGHT_CHANGE="false" \
    REFILL_UNIT_WEIGHT_DEFAULT_KG="0.25"

# Avvio come gli altri consumer Spark
ENTRYPOINT ["spark-submit", "/app/app.py"]
