FROM apache/spark:3.5.1-python3

USER root
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir redis

WORKDIR /app
COPY app.py /app/app.py

# Postgres JDBC non strettamente necessario qui; lo lascio fuori per ora
CMD ["/opt/spark/bin/spark-submit", \
     "--packages", "io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", \
     "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension", \
     "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog", \
     "/app/app.py"]
