FROM postgres:latest

WORKDIR /app

COPY ./*.sql /app/
COPY run_sql.sh /app/run_sql.sh

RUN chmod +x /app/run_sql.sh

ENTRYPOINT ["/app/run_sql.sh"]