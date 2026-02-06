# Inference Service

Loads the latest trained model and generates **pending supplier plans** aligned
to the `wh-supplier-manager` schedule. It triggers only around the configured
cutoff times (default: Sun/Tue/Thu 12:00 UTC, using simulated time).

## What it does
- Reads features from `analytics.v_ml_features`
- Loads the latest model entry from `analytics.ml_models`
- Aligns inference features to the training feature list (if available)
- Writes pending plans to `delta/ops/wh_supplier_plan` (upsert by `shelf_id`)
- Optionally publishes pending plans to Kafka topic `wh_supplier_plan`
- Logs predictions in `analytics.ml_predictions_log`
- Appends prediction logs to Delta at `delta/curated/predictions`

## Model logic (in words)
This service predicts **batches to order** for each shelf that needs
replenishment. It filters shelves using the inference view (e.g.,
`is_warehouse_alert = 1` and `warehouse_capacity > 0`). For each shelf, the
model outputs a non-negative integer `predicted_batches`. The service then
multiplies that by `standard_batch_size` to get `suggested_qty`, which is
persisted as a `pending` supplier plan.

To keep inference consistent with training, categorical columns are one-hot
encoded and then aligned to the exact training feature list (stored during
training). Missing columns are filled with zero.

## Requirements
- Postgres with `analytics.v_ml_features`, `analytics.ml_models`,
  and `analytics.ml_predictions_log`
- Redis + `sim-clock` (for simulated time)
- A volume mounted at `ARTIFACT_DIR` (default `/models`)
- A Delta Lake path mounted at `DELTA_WHSUPPLIER_PLAN_PATH` (default `/delta/ops/wh_supplier_plan`)
- Optional: Kafka broker (only if `KAFKA_ENABLED=1`)

## Configuration (env vars)
- `PG_DSN` (default: `postgresql+psycopg2://postgres:example@db:5432/postgres`)
- `MODEL_NAME` (default: `xgb_batches_to_order`)
- `ARTIFACT_DIR` (default: `/models`)
- `CUTOFF_DOWS` (default: `6,1,3` = Sun/Tue/Thu using `weekday()` Mon=0..Sun=6)
- `CUTOFF_HOUR` / `CUTOFF_MINUTE` (default: `12:00`)
- `PREDICT_LEAD_MINUTES` (default: `5`, trigger before cutoff)
- `INFER_SQL` (default: query `analytics.v_ml_features` for `:today`)
- `DELTA_ENABLED` (default: `1`)
- `DELTA_WHSUPPLIER_PLAN_PATH` (default: `/delta/ops/wh_supplier_plan`)
- `DELTA_PREDICTIONS_PATH` (default: `/delta/curated/predictions`)
- `FEATURE_COLUMNS_PATH` (default: `/models/<MODEL_NAME>_feature_columns.json`)
- `ALLOW_DISK_MODEL` (default: `1`, fallback to latest `/models/<MODEL_NAME>_*.json`)
- `HEARTBEAT_SECONDS` (default: `60`, log waiting status while idle)
- `KAFKA_ENABLED` (default: `0`)
- `KAFKA_BROKER` (default: `kafka:9092`)
- `TOPIC_WH_SUPPLIER_PLAN` (default: `wh_supplier_plan`)
- `REDIS_LAST_RUN_KEY` (default: `ml:<MODEL_NAME>:supplier_plan:last_run_day`)

## Inference flow
1. Wait until simulated time reaches the cutoff trigger window.
2. Load `analytics.v_ml_features` for `:today` (SQL is configurable; falls back to latest available date).
3. Resolve the latest model from `analytics.ml_models` (fallback to disk if enabled).
4. One-hot encode categorical columns and align to training feature columns.
5. Predict `batches_to_order`, round to non-negative integers.
6. Compute `suggested_qty = predicted_batches * standard_batch_size`.
7. Log predictions into `analytics.ml_predictions_log` and append to `delta/curated/predictions`.
8. Upsert pending plans into Delta (and optionally publish them to Kafka).

## Run with Docker Compose
```bash
docker compose up -d postgres redis sim-clock training-service inference-service
```

## Notes
- If `analytics.ml_models` is empty, the service can fallback to the latest
  artifact found in `ARTIFACT_DIR` when `ALLOW_DISK_MODEL=1`.
