FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shelf_restock_producer.py .

CMD ["python", "shelf_restock_producer.py"]
