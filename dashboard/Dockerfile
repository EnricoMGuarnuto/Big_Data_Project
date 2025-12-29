FROM python:3.11-slim

WORKDIR /app

COPY dashboard/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copia solo dashboard + assets necessari
COPY dashboard /app/dashboard
COPY images /app/images
COPY data /app/data
COPY delta /app/delta

EXPOSE 8501

ENV STREAMLIT_SERVER_PORT=8501
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0

CMD ["streamlit", "run", "dashboard/app.py"]
