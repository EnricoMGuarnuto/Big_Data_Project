FROM python:3.10

WORKDIR /app
# COPY app.py .
COPY producer_pos/pos.py .
COPY utils/ utils/
COPY producer_pos/requirements.txt .

RUN pip install -r requirements.txt

CMD ["python", "-u", "pos.py"]