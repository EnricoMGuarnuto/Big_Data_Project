FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libpq-dev \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY discount_updater_producer.py /app/producer.py

# Entrypoint: esegue sempre in loop
CMD ["python", "-u", "producer.py"]
