# Image piccola, compatibile con psycopg2-binary
FROM python:3.11-slim

# Dipendenze di sistema utili (libpq per sicurezza) e certificati
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 ca-certificates && \
    rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    TZ=Europe/Rome

WORKDIR /app

# Install requirements prima per sfruttare la cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia lo script del producer
# Assicurati che il nome corrisponda al file nel tuo repo
COPY near_expiry_discount_producer.py .

# Utente non-root (opzionale ma consigliato)
RUN useradd -ms /bin/bash appuser
USER appuser

# Avvio in loop (lo script gestisce il retry)
CMD ["python", "-u", "near_expiry_discount_producer.py"]
