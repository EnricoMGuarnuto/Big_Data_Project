FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    pandas \
    deltalake \
    confluent-kafka \
    pyarrow \
    redis

COPY app.py .
# Non serve il COPY se usi i volumi, ma male non fa per sicurezza
# COPY simulated_time/ ./simulated_time/

CMD ["python", "app.py"]