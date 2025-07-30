FROM postgres:latest

COPY make_db.sql/docker-entrypoint-initdb.d/
COPY inventory/data/store_inventory_final.csv
COPY inventory/data/warehouse_inventory_final.csv

# Questo Dockerfile serve solo a bootstrap! Quando parte il container,
# Postgres caricherà automaticamente make_db.sql e i CSV, se hai scritto la COPY come sopra.
# TUTTO ciò che metti in /docker-entrypoint-initdb.d/ viene eseguito automaticamente alla prima inizializzazione del DB!
# per Bootstrap


#da capire
