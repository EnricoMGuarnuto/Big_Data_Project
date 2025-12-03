FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY init.py .

ENV KAFKA_BROKER=kafka:9092
CMD ["python", "-u", "init.py"]

