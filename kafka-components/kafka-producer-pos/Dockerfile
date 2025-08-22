FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY pos_producer.py .
VOLUME ["/data"]
CMD ["python","-u", "pos_producer.py"]
