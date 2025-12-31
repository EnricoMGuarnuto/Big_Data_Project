# spark-apps/deltalake/Dockerfile
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
      openjdk-21-jre-headless ca-certificates procps \
    && rm -rf /var/lib/apt/lists/*

# (su amd64 puoi omettere JAVA_HOME: Spark lo risolve da solo)
# ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
# ENV PATH="${PATH}:${JAVA_HOME}/bin"

RUN pip install --no-cache-dir pyspark==3.5.1 delta-spark==3.2.0

WORKDIR /app
COPY init_delta.py /app/init_delta.py

ENV DELTA_ROOT=/delta
ENV SPARK_WAREHOUSE_DIR=/tmp/spark-warehouse

CMD ["python","/app/init_delta.py"]
