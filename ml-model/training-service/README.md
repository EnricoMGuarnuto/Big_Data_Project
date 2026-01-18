# Training Service

Trains the XGBoost demand model from feature data in Postgres and registers
the resulting artifact. This service runs a single training pass on startup.

## What it does
- Reads features from `analytics.v_ml_train`
- One-hot encodes categorical columns and saves the training feature list
- Trains an XGBoost regressor with time-series CV
- Writes the model artifact to `ARTIFACT_DIR`
- Upserts metadata into `analytics.ml_models`

## Model logic 
The target is `batches_to_order`. The model is a gradient-boosted tree regressor
trained on historical, per-shelf features ordered by `feature_date`. Training
uses a time-series split (no shuffling) to avoid leakage from the future. The
best fold is chosen by lowest MAE, and the resulting model is saved as the
current version.

Categorical inputs are expanded with one-hot encoding (`item_category`,
`item_subcategory`). The full list of resulting feature columns is persisted to
disk so inference can align new data to the exact training feature space.

## Requirements
- Postgres with `analytics.v_ml_train` and `analytics.ml_models`
- Redis + `sim-clock` (for simulated time)
- A volume mounted at `ARTIFACT_DIR` (default `/models`)

## Configuration (env vars)
- `PG_DSN` (default: `postgresql+psycopg2://postgres:example@db:5432/postgres`)
- `MODEL_NAME` (default: `xgb_batches_to_order`)
- `ARTIFACT_DIR` (default: `/models`)
- `FEATURE_SQL` (default: `SELECT * FROM analytics.v_ml_train ORDER BY shelf_id, feature_date`)
- `RETRAIN_DAYS` (default: `0`, when > 0 skip training if the last `trained_at` is newer than this window)

## Training flow
1. Load `analytics.v_ml_train` (must include `batches_to_order`).
2. Drop non-feature columns (`batches_to_order`, `feature_date`, `shelf_id`).
3. One-hot encode categorical features and store the final column list.
4. Train with 5-fold `TimeSeriesSplit`, early stopping, MAE metric.
5. Save the best model under a simulated-time version.
6. Upsert `analytics.ml_models` with version, metrics, and artifact path.

## Output artifacts
- Model file: `/models/<MODEL_NAME>_<YYYYMMDD_HHMMSS>.json`
- Feature columns: `/models/<MODEL_NAME>_feature_columns.json`

## Run with Docker Compose
```bash
docker compose up -d postgres redis sim-clock training-service
```

## Notes
- The container exits after one training attempt. With `RETRAIN_DAYS>0`, it may
  exit immediately without training if the current model is still “fresh”.
