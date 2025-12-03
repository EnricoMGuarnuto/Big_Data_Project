FROM confluentinc/cp-kafka-connect:7.6.0
# Install the JDBC connector and bundle the Postgres driver so the connector
# is ready without manual mounts.
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.4 && \
    mkdir -p /usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib && \
    curl -fsSL -o /usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/postgresql-42.7.3.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar
