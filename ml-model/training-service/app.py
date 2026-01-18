import os
import json
import time
from datetime import timezone, timedelta

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_absolute_error
import xgboost as xgb

from simulated_time.redis_helpers import get_simulated_now

MODEL_NAME = os.getenv("MODEL_NAME", "xgb_batches_to_order")
ARTIFACT_DIR = os.getenv("ARTIFACT_DIR", "/models")
RETRAIN_DAYS = int(os.getenv("RETRAIN_DAYS", "0"))  # 0 = always retrain

PG_DSN = os.getenv(
    "PG_DSN",
    "postgresql+psycopg2://postgres:example@db:5432/postgres"
)

FEATURE_SQL = os.getenv(
    "FEATURE_SQL",
    "SELECT * FROM analytics.v_ml_train ORDER BY shelf_id, feature_date"
)

def ensure_utc(dt):
    """Ensure datetime is timezone-aware (UTC)."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def one_hot(df: pd.DataFrame) -> pd.DataFrame:
    cat_cols = ["item_category", "item_subcategory"]
    for c in cat_cols:
        if c in df.columns:
            df[c] = df[c].astype("category")
    df = pd.get_dummies(df, columns=[c for c in cat_cols if c in df.columns], drop_first=False)
    return df

def wait_for_db(engine, retries=20, delay_seconds=3):
    for attempt in range(1, retries + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return
        except OperationalError:
            if attempt == retries:
                raise
            print(f"[training] waiting for postgres ({attempt}/{retries})...")
            time.sleep(delay_seconds)

def should_retrain(engine, simulated_now):
    if RETRAIN_DAYS <= 0:
        return True

    q = text("""
        SELECT trained_at
        FROM analytics.ml_models
        WHERE model_name = :mn
        LIMIT 1
    """)
    try:
        with engine.connect() as conn:
            row = conn.execute(q, {"mn": MODEL_NAME}).fetchone()
    except Exception as e:
        print(f"[training] warning: cannot read ml_models (will retrain). error={e}")
        return True

    if not row or not row[0]:
        return True

    last_trained = ensure_utc(row[0])
    age = simulated_now - last_trained
    return age >= timedelta(days=RETRAIN_DAYS)

def train_once(engine):
    df = pd.read_sql(FEATURE_SQL, engine)
    if df.empty:
        print("[training] v_ml_train is empty, skipping training")
        return

    y = df["batches_to_order"].astype(int)
    drop_cols = ["batches_to_order", "feature_date", "shelf_id"]
    X = df.drop(columns=drop_cols)

    # 1) One-hot encoding (defines the final training feature space)
    X = one_hot(X)

    # 2) Save the exact feature columns used during training
    os.makedirs(ARTIFACT_DIR, exist_ok=True)
    feature_cols_path = os.path.join(ARTIFACT_DIR, f"{MODEL_NAME}_feature_columns.json")
    with open(feature_cols_path, "w") as f:
        json.dump(list(X.columns), f)
    print(f"[training] saved feature columns to {feature_cols_path} (n={len(X.columns)})")

    # 3) TimeSeries CV
    tscv = TimeSeriesSplit(n_splits=5)

    best_model = None
    best_mae = 1e18
    best_metrics = None

    # Keep memory lower vs default int64/float64.
    X_np = X.values.astype("float32", copy=False)
    y_np = y.values.astype("float32", copy=False)

    for fold, (tr, va) in enumerate(tscv.split(X_np), start=1):
        dtr = xgb.DMatrix(X_np[tr], label=y_np[tr])
        dva = xgb.DMatrix(X_np[va], label=y_np[va])

        params = {
            "objective": "reg:squarederror",
            "eval_metric": "mae",
            "tree_method": "hist",
            "max_depth": 8,
            "eta": 0.08,
            "subsample": 0.9,
            "colsample_bytree": 0.9,
            "min_child_weight": 2,
            "seed": 42,
        }

        model = xgb.train(
            params,
            dtr,
            num_boost_round=2000,
            evals=[(dva, "val")],
            early_stopping_rounds=50,
            verbose_eval=False
        )

        pred = model.predict(dva)
        mae = float(mean_absolute_error(y_np[va], pred))

        if mae < best_mae:
            best_mae = mae
            best_model = model
            best_metrics = {
                "mae": mae,
                "fold": fold,
                "features": int(X.shape[1])
            }

    # 4) Save model artifact versioned by simulated time
    simulated_now = ensure_utc(get_simulated_now())
    version = simulated_now.strftime("%Y%m%d_%H%M%S")

    artifact_path = os.path.join(ARTIFACT_DIR, f"{MODEL_NAME}_{version}.json")
    best_model.save_model(artifact_path)

    # 5) Upsert registry entry
    upsert_sql = text("""
        INSERT INTO analytics.ml_models(
            model_name,
            model_version,
            trained_at,
            metrics_json,
            artifact_path
        )
        VALUES (:mn, :mv, :ta, CAST(:mj AS jsonb), :ap)
        ON CONFLICT (model_name)
        DO UPDATE SET
          model_version = EXCLUDED.model_version,
          trained_at = EXCLUDED.trained_at,
          metrics_json = EXCLUDED.metrics_json,
          artifact_path = EXCLUDED.artifact_path;
    """)

    with engine.begin() as conn:
        conn.execute(upsert_sql, {
            "mn": MODEL_NAME,
            "mv": version,
            "ta": simulated_now,
            "mj": json.dumps(best_metrics),
            "ap": artifact_path
        })

    print(f"[training] saved {artifact_path} metrics={best_metrics}")

def main():
    engine = create_engine(PG_DSN, pool_pre_ping=True)
    wait_for_db(engine)
    simulated_now = ensure_utc(get_simulated_now())
    if not should_retrain(engine, simulated_now):
        print(f"[training] skip: model {MODEL_NAME} trained within last {RETRAIN_DAYS}d (sim_now={simulated_now.isoformat()})")
        return
    train_once(engine)

if __name__ == "__main__":
    main()
