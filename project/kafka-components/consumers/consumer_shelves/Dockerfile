FROM python:3.10-slim

WORKDIR /app
COPY consumer_shelves/shelves_consumer.py .
COPY utils/ utils/
COPY consumer_shelves/requirements.txt .

RUN pip install -r requirements.txt

CMD ["python", "shelves_consumer.py"]