FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends cron \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY discount_updater_producer.py .
COPY crontab.txt .
RUN chmod +x discount_updater_producer.py && crontab crontab.txt
VOLUME ["/data"]
CMD ["cron", "-f"]
