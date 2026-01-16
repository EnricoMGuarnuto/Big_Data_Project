FROM python:3.10-slim

WORKDIR /app
COPY consumer_traffic/traffic_consumer.py .
COPY utils/ utils/
COPY consumer_traffic/requirements.txt .

RUN pip install -r requirements.txt

CMD ["python", "traffic_consumer.py"]