----prima lanciare l'infrastruttura di base:

ddocker compose --profile infra up -d


-----se non ancora lanciato, inizializzare, one-shot, il DB:

docker compose run --rm db-sql-jobs


-----avviare i produttori:

docker compose --profile stream up -d

------