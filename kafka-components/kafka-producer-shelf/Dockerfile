FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy producer script
COPY shelf_producer.py .

# Mount /data directory (comes from docker-compose volume)
VOLUME ["/app/data"]

CMD ["python", "shelf_producer.py"]
