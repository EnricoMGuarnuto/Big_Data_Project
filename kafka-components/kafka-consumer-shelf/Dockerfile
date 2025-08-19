FROM python:3.11-slim
WORKDIR /app
# libpq-dev serve a psycopg2; gcc solo per compilare eventuali wheel
RUN apt-get update && apt-get install -y --no-install-recommends gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Copio tutto il contesto: evita “file not found” quando rinomini file
COPY . .
CMD ["python", "shelf_consumer.py"]
