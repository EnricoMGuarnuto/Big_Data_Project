# Usare slim va benissimo
FROM python:3.10-slim

# 1) Evita buffer su stdout/stderr (log visibili subito)
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# 2) Installa tini per gestire correttamente i segnali (SIGTERM) nei container
#    + qualche util di base utile in debug
RUN apt-get update && apt-get install -y --no-install-recommends \
      tini ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

# 3) Crea un utente non-root (best practice)
ARG APP_USER=appuser
RUN useradd -m -u 10001 ${APP_USER}

WORKDIR /app

# 4) Installa dipendenze Python prima di copiare il codice (cache layer migliore)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5) Copia lo script
COPY shelf_producer.py .

# 6) Permessi
RUN chown -R ${APP_USER}:${APP_USER} /app
USER ${APP_USER}

# 7) Healthcheck semplice: il container risponde ed è in grado di importare le lib
HEALTHCHECK --interval=30s --timeout=5s --retries=3 CMD python -c "import pandas, kafka, pyarrow; print('ok')" || exit 1

# 8) Entrypoint con tini, poi il comando
ENTRYPOINT ["/usr/bin/tini","--"]
CMD ["python","shelf_producer.py"]
