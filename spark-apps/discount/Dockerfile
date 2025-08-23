FROM bitnami/spark:3.5.1

USER root

# Install build deps for psycopg2
RUN apt-get update --fix-missing && apt-get install -y --no-install-recommends \
    wget curl unzip libpq-dev build-essential \
 && rm -rf /var/lib/apt/lists/*

# Spark jars path
ENV SPARK_HOME=/opt/bitnami/spark
WORKDIR $SPARK_HOME/jars

# === Scarica dipendenze necessarie ===
# Kafka + Hadoop AWS + AWS SDK (core+s3+dynamodb) + Postgres + Commons Pool2
RUN curl -L --retry 5 --retry-all-errors -o spark-sql-kafka-0-10_2.12-3.5.1.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    curl -L --retry 5 --retry-all-errors -o kafka-clients-3.5.1.jar \
        https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar && \
    curl -L --retry 5 --retry-all-errors -o spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    curl -L --retry 5 --retry-all-errors -o hadoop-aws-3.3.4.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L --retry 5 --retry-all-errors -o aws-java-sdk-core-1.12.767.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-core/1.12.767/aws-java-sdk-core-1.12.767.jar && \
    curl -L --retry 5 --retry-all-errors -o aws-java-sdk-s3-1.12.767.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-s3/1.12.767/aws-java-sdk-s3-1.12.767.jar && \
    curl -L --retry 5 --retry-all-errors -o aws-java-sdk-dynamodb-1.12.767.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-dynamodb/1.12.767/aws-java-sdk-dynamodb-1.12.767.jar && \
    curl -L --retry 5 --retry-all-errors -o postgresql-42.6.0.jar \
        https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar && \
    curl -L --retry 5 --retry-all-errors -o commons-pool2-2.11.1.jar \
        https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# Torna in /app
WORKDIR /app

# Copy requirements first (layer caching)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy consumer script
COPY app.py /app/app.py

# Entrypoint senza pacchetti remoti (già in jars/)
ENTRYPOINT ["spark-submit", "/app/app.py"]
