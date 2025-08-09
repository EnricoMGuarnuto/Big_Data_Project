FROM python:3.10-slim

# Evita output buffered (log in tempo reale) e bytecode .pyc
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Dipendenze di runtime per wheels manylinux (pyarrow/pandas ok su slim)
# + tzdata solo per sicurezza sui timestamp (opzionale)
RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata \
 && rm -rf /var/lib/apt/lists/*

# Installa le dipendenze Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Codice applicativo
COPY shelf_restock_producer.py .

# Avvio
CMD ["python", "-u", "shelf_restock_producer.py"]
