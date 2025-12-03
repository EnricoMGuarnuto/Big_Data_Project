FROM bitnami/spark:3.5.1

USER root
# (opzionale) utilità
RUN apt-get update && apt-get install -y --no-install-recommends procps && rm -rf /var/lib/apt/lists/*

# App dir
WORKDIR /app
COPY app.py /app/app.py

# Non installiamo pyspark (già presente nella base bitnami); delta-spark non serve come wheel se usiamo --packages
CMD ["/opt/bitnami/spark/bin/spark-submit", \
     "--packages", "io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3", \
     "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension", \
     "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog", \
     "/app/app.py"]
