FROM apache/spark:3.5.1-python3

USER root

# Optional utilities
RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir redis

WORKDIR /app

# Copy the Spark app
COPY app.py /app/app.py

# Postgres JDBC (not strictly needed here, but you use it elsewhere and it does no harm)
RUN curl -L --retry 5 -o /opt/spark/jars/postgresql-42.7.3.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

# Start: spark-submit with Delta + Kafka
CMD ["/opt/spark/bin/spark-submit", \
     "--packages", "io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3", \
     "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension", \
     "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog", \
     "/app/app.py"]
