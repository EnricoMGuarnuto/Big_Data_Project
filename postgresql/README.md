# PostgreSQL setup

SQL scripts used to initialize the Postgres database, load reference snapshots,
seed configuration tables, and prepare ML feature datasets. These scripts are
intended to be run in order.

## Files
- `01_make_db.sql`: Creates schemas, enums, and all core tables
  (`config`, `state`, `ops`, `ref`, `analytics`).
- `02_load_ref_from_csv.sql`: Loads the snapshot tables in `ref.*` from CSVs.
- `03_load_config.sql`: Seeds `config.*` tables (policies, shelf profiles,
  batch catalog) using the latest snapshots.
- `04_load_features_panel.sql`: Loads the synthetic ML feature panel into
  `analytics.shelf_daily_features` in postgres.
- `05_views.sql`: Defines analytics views used by training/inference.

## Schemas and core tables (high level)
- `config.*`: human/service-managed policies and catalog (e.g. `shelf_policies`,
  `wh_policies`, `shelf_profiles`, `batch_catalog`).
- `state.*`: compacted state mirrors (current shelf/warehouse stock + batches).
- `ops.*`: operational facts (alerts, restock plans, warehouse events, supplier
  plans).
- `ref.*`: immutable snapshots loaded from CSVs.
- `analytics.*`: feature panel and ML registry/logs (`shelf_daily_features`,
  `ml_models`, `ml_predictions_log`).

## ML-related views
`05_views.sql` prepares the datasets used by the ML services:
- `analytics.v_ml_features`: cleansed feature set with numeric casting and
  flags (derived from `analytics.shelf_daily_features`).
- `analytics.v_ml_train`: training dataset filtered to valid warehouse shelves.
- `analytics.v_train_demand_next7d`: helper view for 7-day demand target
  exploration.

## Expected inputs
`02_load_ref_from_csv.sql` expects these CSVs mounted in the Postgres container
at `/import/csv/db`:
- `store_inventory_final.csv`
- `store_batches.csv`
- `warehouse_inventory_final.csv`
- `warehouse_batches.csv`

`04_load_features_panel.sql` expects:
- `/import/csv/sim/synthetic_1y_panel.csv`

The reference CSVs are produced by `data/create_db.py` and live in
`data/db_csv/`. The ML panel CSV lives under `data/` (see `data/README.md`).

## Run order
1. Run `01_make_db.sql`
2. Run `02_load_ref_from_csv.sql`
3. Run `03_load_config.sql`
4. Run `04_load_features_panel.sql` (optional if you load features elsewhere)
5. Run `05_views.sql`

See the root `docker-compose.yml` for how these scripts are applied in the
stack.
