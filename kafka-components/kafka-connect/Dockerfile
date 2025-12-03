FROM confluentinc/kafka-connect-jdbc:10.7.4

# Scarica il driver Postgres (no mount manuale)
RUN curl -fsSL -o /usr/share/java/kafka-connect-jdbc/postgresql-42.7.3.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar
