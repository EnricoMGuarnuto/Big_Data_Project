FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN apt-get update && apt-get install -y netcat-openbsd
RUN chmod +x wait-for-kafka.sh

CMD ["./wait-for-kafka.sh"]
