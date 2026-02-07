FROM python:3.10

WORKDIR /app
# COPY app.py .
COPY producer_traffic/foot_traffic.py .
COPY utils/ utils/
COPY producer_traffic/requirements.txt .

# RUN apt-get update && apt-get install -y gcc g++ build-essential
RUN pip install -r requirements.txt

CMD ["python", "-u", "foot_traffic.py"]