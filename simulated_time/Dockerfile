FROM python:3.10-slim

WORKDIR /app

COPY simulated_time /app/simulated_time

RUN pip install redis

ENV PYTHONPATH=/app

CMD ["python", "-u", "simulated_time/sim_clock_driver.py"]
