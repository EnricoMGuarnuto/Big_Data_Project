# Shelf Daily Features (Kafka Python)

Daily job that computes and writes (UPSERT) the features for all `shelf_id` into:
`analytics.shelf_daily_features` (Postgres).

This version replaces the Spark app with a lightweight Python job (no Spark).

## Env vars
- PGHOST
- PGPORT (default 5432)
- PGDATABASE
- PGUSER
- PGPASSWORD
- DEFAULT_WH_REORDER_POINT_QTY (default 10)
- POLL_SECONDS (default 1.0)
- LOG_LEVEL (default INFO)

## Build
```
docker build -t shelf-daily-features .
```

## Run (example)
```
docker run --rm \
  -e PGHOST=postgres -e PGPORT=5432 -e PGDATABASE=smart_shelf \
  -e PGUSER=postgres -e PGPASSWORD=postgres \
  shelf-daily-features
```
