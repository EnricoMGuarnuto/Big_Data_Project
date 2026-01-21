FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    pandas \
    psycopg2-binary \
    redis

COPY app.py .

CMD ["python", "app.py"]
