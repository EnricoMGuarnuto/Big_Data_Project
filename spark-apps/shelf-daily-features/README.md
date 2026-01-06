# Shelf Daily Features (Spark)

Job giornaliero che calcola e scrive (UPSERT) le feature per tutti gli shelf_id in:
analytics.shelf_daily_features

## Env vars
- RUN_DATE=YYYY-MM-DD
- PGHOST
- PGPORT (default 5432)
- PGDATABASE
- PGUSER
- PGPASSWORD

## Dipendenze
- Serve il JDBC driver Postgres in ./jars/ (es: postgresql-42.7.3.jar)

## Build
docker build -t shelf-daily-features .

## Run (esempio)
docker run --rm \
  -e RUN_DATE=2026-01-01 \
  -e PGHOST=postgres -e PGPORT=5432 -e PGDATABASE=smart_shelf \
  -e PGUSER=postgres -e PGPASSWORD=postgres \
  shelf-daily-features
