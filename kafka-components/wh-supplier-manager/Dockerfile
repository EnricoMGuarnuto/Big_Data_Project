FROM python:3.9-slim

WORKDIR /app

# Installiamo le librerie per Delta e Kafka
RUN pip install --no-cache-dir \
    pandas \
    deltalake \
    confluent-kafka \
    pyarrow

# Copiamo il codice e la tua cartella del tempo simulato
COPY app.py .

CMD ["python", "app.py"]