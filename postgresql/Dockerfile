FROM postgres:latest

COPY make_db.sql /docker-entrypoint-initdb.d/

# Questo Dockerfile serve solo a bootstrap! Quando parte il container,
# Postgres caricherà automaticamente make_db.sql.
# TUTTO ciò che metti in /docker-entrypoint-initdb.d/ viene eseguito automaticamente alla prima inizializzazione del DB!
# per Bootstrap


