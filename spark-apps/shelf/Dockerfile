FROM bitnami/spark:3.4.1

# Serve root per installare pacchetti
USER root

# Installa curl e ca-certificates, poi pulisci i layer APT
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

# JDBC driver Postgres (evita "No suitable driver")
RUN curl -fsSL \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar \
    -o /opt/bitnami/spark/jars/postgresql-42.7.3.jar

WORKDIR /app

# (facoltativo) requirements Python
COPY requirements.txt .
RUN if [ -s requirements.txt ]; then pip install --no-cache-dir -r requirements.txt; fi

# Codice Spark
COPY app.py /app/app.py

# Torna utente non-root (come da best practice Bitnami)
USER 1001

# Avvio con pacchetti per Kafka + S3A + AWS SDK
CMD ["spark-submit","--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262","--conf", "spark.sql.shuffle.partitions=2","--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem","--conf", "spark.hadoop.fs.s3a.path.style.access=true","--conf", "spark.hadoop.fs.s3a.endpoint=http://minio:9000","--conf", "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider","/app/app.py"]
