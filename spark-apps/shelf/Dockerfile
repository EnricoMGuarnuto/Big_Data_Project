# spark-apps/shelf/Dockerfile
FROM bitnami/spark:3.4.1

# Installa pacchetti Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Scarica JDBC driver PostgreSQL
ADD https://jdbc.postgresql.org/download/postgresql-42.7.3.jar /opt/bitnami/spark/jars/postgresql-42.7.3.jar


WORKDIR /app
COPY app.py /app/app.py

# Spark submit con pacchetti
CMD [ "spark-submit",  "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3","--conf", "spark.sql.shuffle.partitions=2","--conf", "spark.hadoop.fs.s3a.endpoint=http://minio:9000","--conf", "spark.hadoop.fs.s3a.path.style.access=true","--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem","/app/app.py"]
