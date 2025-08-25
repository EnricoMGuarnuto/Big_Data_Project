FROM python:3.11-slim

WORKDIR /app

# Install dependencies and cron
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libpq-dev cron \
 && rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the producer script
COPY discount_updater_producer.py /app/producer.py

# Copy the crontab file
COPY crontab.txt /etc/cron.d/discount-cron

# Apply permissions and register cron job
RUN chmod 0644 /etc/cron.d/discount-cron && \
    crontab /etc/cron.d/discount-cron

# Create log file
RUN touch /var/log/cron.log

# Launch cron in foreground
CMD cron && tail -f /var/log/cron.log

