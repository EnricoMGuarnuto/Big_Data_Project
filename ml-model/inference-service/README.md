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

## Model logic (in words)
This service predicts **batches to order** for each shelf that needs
replenishment. It filters shelves using the inference view (e.g.,
`is_warehouse_alert = 1` and `warehouse_capacity > 0`). For each shelf, the
model outputs a non-negative integer `predicted_batches`. The service then
multiplies that by `standard_batch_size` to get `suggested_qty`, which is
persisted as a pending plan.

To keep inference consistent with training, categorical columns are one-hot
encoded and then aligned to the exact training feature list (stored during
training). Missing columns are filled with zero.

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

## Inference flow
1. Wait until simulated time reaches `RUN_HOUR:RUN_MINUTE`.
2. Load `analytics.v_ml_features` for `:today` (SQL is configurable).
3. Resolve the latest model from `analytics.ml_models` (fallback to disk if enabled).
4. One-hot encode categorical columns and align to training feature columns.
5. Predict `batches_to_order`, round to non-negative integers.
6. Compute `suggested_qty = predicted_batches * standard_batch_size`.
7. Upsert rows into `ops.wh_supplier_plan` and log to `analytics.ml_predictions_log`.
8. Optionally append plans to Delta Lake.

## Run with Docker Compose
```bash
docker compose up -d postgres redis sim-clock training-service inference-service
```

## Notes
- If `analytics.ml_models` is empty, the service can fallback to the latest
  artifact found in `ARTIFACT_DIR` when `ALLOW_DISK_MODEL=1`.
