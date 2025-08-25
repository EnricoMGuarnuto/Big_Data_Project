FROM bitnami/spark:3.4.1

# Root per installazioni
USER root

# Installa curl e certificati (serve per scaricare i JAR)
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

# Path Spark jars
ENV SPARK_HOME=/opt/bitnami/spark
WORKDIR $SPARK_HOME/jars

# === Scarica JAR necessari ===
# Kafka + Postgres JDBC + Hadoop AWS (se scrivi anche su MinIO) + Commons Pool2
RUN curl -fsSL -o spark-sql-kafka-0-10_2.12-3.4.0.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.0/spark-sql-kafka-0-10_2.12-3.4.0.jar && \
    curl -fsSL -o kafka-clients-3.4.0.jar \
        https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar && \
    curl -fsSL -o spark-token-provider-kafka-0-10_2.12-3.4.0.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.4.0/spark-token-provider-kafka-0-10_2.12-3.4.0.jar && \
    curl -fsSL -o postgresql-42.7.3.jar \
        https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar && \
    curl -fsSL -o hadoop-aws-3.3.4.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fsSL -o aws-java-sdk-bundle-1.12.262.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -fsSL -o commons-pool2-2.11.1.jar \
        https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# Torna in /app per il codice
WORKDIR /app

# requirements Python (es. psycopg2-binary se fai anche JDBC manuale)
COPY requirements.txt .
RUN if [ -s requirements.txt ]; then pip install --no-cache-dir -r requirements.txt; fi

# PySpark (Bitnami include Spark, ma meglio allineare PyPI)
RUN pip install pyspark==3.4.1

# Copia il job Spark consumer
COPY app.py /app/app.py

# Torna utente non-root (Bitnami best practice)
USER 1001

# CMD: spark-submit senza --packages (i JAR sono già in $SPARK_HOME/jars)
CMD ["spark-submit", "/app/app.py"]
