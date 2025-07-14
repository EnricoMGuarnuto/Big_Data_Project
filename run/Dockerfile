FROM bitnami/spark:latest

USER root

# Crea directory per script Spark e JAR extra
RUN mkdir -p /opt/spark-apps /opt/spark-extra-jars

# Copia configurazione Spark
COPY ./conf/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf

# Copia gli script Python Spark
COPY ./spark-apps/ /opt/spark-apps/

# Copia i JAR extra se necessario (es: Kafka, Hadoop)
COPY ./jars/ /opt/spark-extra-jars/

# Imposta la directory di lavoro
WORKDIR /opt/bitnami/spark
