# kafka-producer-wh_shelf/Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App code
COPY wh_shelf_producer.py .

# Run only the producer
CMD ["python","-u", "wh_shelf_producer.py"]