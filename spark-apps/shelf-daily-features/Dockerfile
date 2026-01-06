FROM apache/spark:3.5.1

USER root

RUN pip install --no-cache-dir psycopg2-binary==2.9.9

# Metti il driver JDBC Postgres in ./jars/
COPY jars/postgresql-*.jar /opt/spark/jars/

WORKDIR /app
COPY app.py /app/app.py

USER 185

ENTRYPOINT ["/opt/spark/bin/spark-submit", "--master", "local[*]", "/app/app.py"]
