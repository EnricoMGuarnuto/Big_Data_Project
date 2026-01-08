# Inference Service

Loads the latest trained model and writes daily restock plans to Postgres
using simulated time. It runs once per simulated day at `RUN_HOUR:RUN_MINUTE`.

## What it does
- Reads features from `analytics.v_ml_features`
- Loads the latest model entry from `analytics.ml_models`
- Aligns inference features to the training feature list (if available)
- Writes planned quantities to `ops.wh_supplier_plan`
- Logs predictions in `analytics.ml_predictions_log`
- Optionally appends plans to Delta Lake

## Requirements
- Postgres with `analytics.v_ml_features`, `analytics.ml_models`,
  `ops.wh_supplier_plan`, and `analytics.ml_predictions_log`
- Redis + `sim-clock` (for simulated time)
- A volume mounted at `ARTIFACT_DIR` (default `/models`)
- Optional: Delta Lake path mounted at `DELTA_WHSUPPLIER_PLAN_PATH`

## Configuration (env vars)
- `PG_DSN` (default: `postgresql+psycopg2://postgres:example@db:5432/postgres`)
- `MODEL_NAME` (default: `xgb_batches_to_order`)
- `ARTIFACT_DIR` (default: `/models`)
- `RUN_HOUR` (default: `0`)
- `RUN_MINUTE` (default: `5`)
- `INFER_SQL` (default: query `analytics.v_ml_features` for `:today`)
- `DELTA_ENABLED` (default: `1`)
- `DELTA_WHSUPPLIER_PLAN_PATH` (default: `/delta/ops/wh_supplier_plan`)
- `FEATURE_COLUMNS_PATH` (default: `/models/<MODEL_NAME>_feature_columns.json`)
- `ALLOW_DISK_MODEL` (default: `1`, fallback to latest `/models/<MODEL_NAME>_*.json`)
- `HEARTBEAT_SECONDS` (default: `60`, log waiting status while idle)

## Run with Docker Compose
```bash
docker compose up -d postgres redis sim-clock training-service inference-service
```

## Notes
- If `analytics.ml_models` is empty, the service can fallback to the latest
  artifact found in `ARTIFACT_DIR` when `ALLOW_DISK_MODEL=1`.
