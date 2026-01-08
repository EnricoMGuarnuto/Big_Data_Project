# PostgreSQL setup

SQL scripts used to initialize the Postgres database, load reference snapshots,
and seed configuration tables. These scripts are intended to be run in order.

## Files
- `01_make_db.sql`: Creates schemas, enums, and all core tables
  (`config`, `state`, `ops`, `ref`, `analytics`).
- `02_load_ref_from_csv.sql`: Loads the snapshot tables in `ref.*` from CSVs.
- `03_load_config.sql`: Seeds `config.*` tables (policies, shelf profiles,
  batch catalog) using the latest snapshots.

## Expected inputs
`02_load_ref_from_csv.sql` assumes the following CSVs are mounted inside the
Postgres container at `/import/csv/db`:
- `store_inventory_final.csv`
- `store_batches.csv`
- `warehouse_inventory_final.csv`
- `warehouse_batches.csv`

These CSVs are produced by `data/create_db.py` and live in `data/db_csv/`.

## Run order
1. Run `01_make_db.sql`
2. Run `02_load_ref_from_csv.sql`
3. Run `03_load_config.sql`

See the root `docker-compose.yml` for how these scripts are applied in the
stack.
