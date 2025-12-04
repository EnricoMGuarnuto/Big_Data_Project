FROM apache/spark:3.5.1-python3

USER root
# (opzionale) utilità
RUN apt-get update && apt-get install -y --no-install-recommends procps curl && rm -rf /var/lib/apt/lists/*

# App dir
WORKDIR /app
COPY app.py /app/app.py

# Ensure Postgres JDBC driver is available before Spark starts
RUN curl -L --retry 5 -o /opt/spark/jars/postgresql-42.7.3.jar \
        https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

# Non installiamo pyspark (già presente nella base Spark ufficiale); delta-spark non serve come wheel se usiamo --packages
CMD ["/opt/spark/bin/spark-submit", \
     "--packages", "io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3", \
     "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension", \
     "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog", \
     "/app/app.py"]
