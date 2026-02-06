# Shelf Daily Features (Kafka Python)

Daily job that computes and writes (UPSERT) the features for all `shelf_id` into:
`analytics.shelf_daily_features` (Postgres).
It also appends the same daily snapshot to Delta at
`/delta/curated/features_store` (configurable).

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
- DELTA_ENABLED (default 1)
- DELTA_FEATURES_STORE_PATH (default /delta/curated/features_store)
- DELTA_WRITE_MAX_RETRIES (default 5)
- DELTA_WRITE_RETRY_BASE_S (default 0.8)

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
