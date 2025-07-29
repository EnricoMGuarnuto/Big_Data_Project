FROM python:3.10

WORKDIR /app
# COPY app.py .
COPY producer_shelves/shelves.py .
COPY utils/ utils/
COPY producer_shelves/requirements.txt .

RUN pip install -r requirements.txt

CMD ["python", "-u", "shelves.py"]