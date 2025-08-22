  FROM python:3.11-slim
  ENV PIP_NO_CACHE_DIR=1 PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
  WORKDIR /app
  COPY requirements.txt .
  RUN apt-get update && apt-get install -y --no-install-recommends build-essential libpq-dev \
      && pip install -r requirements.txt \
      && apt-get purge -y build-essential \
      && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*
  COPY app.py .
  EXPOSE 8501
  CMD ["streamlit", "run", "app.py", "--server.runOnSave=true", "--server.address=0.0.0.0"]