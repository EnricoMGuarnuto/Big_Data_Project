FROM python:3.11-slim

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install cron
RUN apt-get update && apt-get install -y cron

# Copy script + cron schedule
COPY discount_updater_producer.py .
COPY crontab.txt .

# Permessi
RUN chmod +x discount_updater_producer.py

# Register the crontab
RUN echo "" >> crontab.txt && crontab crontab.txt

# Create data dir if it doesn't exist
RUN mkdir -p /data

# Mount point
VOLUME ["/data"]

# Start cron in foreground
CMD ["cron", "-f"]
