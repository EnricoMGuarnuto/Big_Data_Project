# Shelf Daily Features (Spark)

Daily job that computes and writes (UPSERT) the features for all `shelf_id` into:
analytics.shelf_daily_features

## Env vars
- RUN_DATE=YYYY-MM-DD
- PGHOST
- PGPORT (default 5432)
- PGDATABASE
- PGUSER
- PGPASSWORD

## Dependencies
- Requires the Postgres JDBC driver in `./jars/` (e.g. `postgresql-42.7.3.jar`)

## Build
docker build -t shelf-daily-features .

## Run (example)
docker run --rm \
  -e RUN_DATE=2026-01-01 \
  -e PGHOST=postgres -e PGPORT=5432 -e PGDATABASE=smart_shelf \
  -e PGUSER=postgres -e PGPASSWORD=postgres \
  shelf-daily-features
