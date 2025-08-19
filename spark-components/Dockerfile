FROM bitnami/spark:3.4
USER root
RUN apt-get update && apt-get install -y --no-install-recommends python3-pip \
    && rm -rf /var/lib/apt/lists/*
RUN pip3 install --no-cache-dir psycopg2-binary pandas pyarrow
WORKDIR /app
ENV PYTHONUNBUFFERED=1
