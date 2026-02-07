FROM python:3.10-slim

WORKDIR /app
COPY consumer_pos/pos_consumer.py .
COPY utils/ utils/
COPY consumer_pos/requirements.txt .

RUN pip install -r requirements.txt

CMD ["python", "pos_consumer.py"]