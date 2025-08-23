FROM bitnami/spark:3.4.1

USER root

# Install curl & ca-certificates
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

# Spark jars path
ENV SPARK_HOME=/opt/bitnami/spark
WORKDIR $SPARK_HOME/jars

# === Scarica dipendenze Spark/Kafka/Hadoop/AWS/Postgres ===
RUN curl -fsSL -o spark-sql-kafka-0-10_2.12-3.4.0.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.0/spark-sql-kafka-0-10_2.12-3.4.0.jar && \
    curl -fsSL -o kafka-clients-3.4.0.jar \
        https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar && \
    curl -fsSL -o spark-token-provider-kafka-0-10_2.12-3.4.0.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.4.0/spark-token-provider-kafka-0-10_2.12-3.4.0.jar && \
    curl -fsSL -o hadoop-aws-3.3.4.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fsSL -o aws-java-sdk-bundle-1.12.262.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -fsSL -o commons-pool2-2.11.1.jar \
        https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -fsSL -o postgresql-42.7.3.jar \
        https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

# Torna in /app
WORKDIR /app

# Python deps
COPY requirements.txt .
RUN if [ -s requirements.txt ]; then pip install --no-cache-dir -r requirements.txt; fi

# Allinea pyspark alla versione base image
RUN pip install pyspark==3.4.1

# Codice
COPY app.py /app/app.py

# Torna non-root
USER 1001

# Entrypoint: niente più --packages (sono già in jars/)
CMD ["spark-submit", "/app/app.py"]
