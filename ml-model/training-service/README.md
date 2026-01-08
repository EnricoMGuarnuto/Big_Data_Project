# Training Service

Trains the XGBoost demand model from feature data in Postgres and registers
the resulting artifact. This service runs a single training pass on startup.

## What it does
- Reads features from `analytics.v_ml_train`
- One-hot encodes categorical columns and saves the training feature list
- Trains an XGBoost regressor with time-series CV
- Writes the model artifact to `ARTIFACT_DIR`
- Upserts metadata into `analytics.ml_models`

## Requirements
- Postgres with `analytics.v_ml_train` and `analytics.ml_models`
- Redis + `sim-clock` (for simulated time)
- A volume mounted at `ARTIFACT_DIR` (default `/models`)

## Configuration (env vars)
- `PG_DSN` (default: `postgresql+psycopg2://postgres:example@db:5432/postgres`)
- `MODEL_NAME` (default: `xgb_batches_to_order`)
- `ARTIFACT_DIR` (default: `/models`)
- `FEATURE_SQL` (default: `SELECT * FROM analytics.v_ml_train ORDER BY shelf_id, feature_date`)

## Output artifacts
- Model file: `/models/<MODEL_NAME>_<YYYYMMDD_HHMMSS>.json`
- Feature columns: `/models/<MODEL_NAME>_feature_columns.json`

## Run with Docker Compose
```bash
docker compose up -d postgres redis sim-clock training-service
```

## Notes
- The container exits after one training run. Restart the service when you want
  a new model version.
