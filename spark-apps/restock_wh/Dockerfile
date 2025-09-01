FROM bitnami/spark:3.4.1

# Root for installations
USER root

# Install curl and certificates (needed for JAR downloads)
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

# Spark jars path
ENV SPARK_HOME=/opt/bitnami/spark
WORKDIR $SPARK_HOME/jars

# === Download required JARs ===
# Kafka + Hadoop AWS + AWS SDK + Commons Pool2 + Postgres JDBC
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

# Back to /app for code
WORKDIR /app

# Python requirements (empty for now, but placeholder for psycopg2 or others)
COPY requirements.txt .
RUN if [ -s requirements.txt ]; then pip install --no-cache-dir -r requirements.txt; fi

# PySpark (Bitnami already has Spark, but ensure PyPI sync)
RUN pip install pyspark==3.4.1

# Copy Spark application
COPY app.py /app/app.py

# Back to non-root (Bitnami best practice)
USER 1001

# CMD clean: no --packages, jars are already in $SPARK_HOME/jars
CMD ["spark-submit", "/app/app.py"]
