FROM python:3.11-slim
WORKDIR /app
COPY init.py .
ENV CONNECT_URL=http://kafka-connect:8083
CMD ["python", "-u", "init.py"]
