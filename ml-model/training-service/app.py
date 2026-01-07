import os
import json
import time
from datetime import timedelta
from datetime import timezone


import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_absolute_error
import xgboost as xgb

from simulated_time.redis_helpers import get_simulated_now  # ✅

MODEL_NAME = os.getenv("MODEL_NAME", "xgb_batches_to_order")
ARTIFACT_DIR = os.getenv("ARTIFACT_DIR", "/models")
RETRAIN_DAYS = int(os.getenv("RETRAIN_DAYS", "7"))

PG_DSN = os.getenv(
    "PG_DSN",
    "postgresql+psycopg2://postgres:example@db:5432/postgres"
)

FEATURE_SQL = os.getenv(
    "FEATURE_SQL",
    "SELECT * FROM analytics.v_ml_train ORDER BY shelf_id, feature_date"
)

def one_hot(df: pd.DataFrame) -> pd.DataFrame:
    cat_cols = ["item_category", "item_subcategory"]
    for c in cat_cols:
        df[c] = df[c].astype("category")
    df = pd.get_dummies(df, columns=cat_cols, drop_first=False)
    return df

def train_once(engine):
    df = pd.read_sql(FEATURE_SQL, engine)

    y = df["batches_to_order"].astype(int)
    drop_cols = ["batches_to_order", "feature_date", "shelf_id"]
    X = df.drop(columns=drop_cols)

    X = one_hot(X)

    tscv = TimeSeriesSplit(n_splits=5)

    best_model = None
    best_mae = 1e18
    best_metrics = None

    X_np = X.values
    y_np = y.values

    for fold, (tr, va) in enumerate(tscv.split(X_np), start=1):
        dtr = xgb.DMatrix(X_np[tr], label=y_np[tr])
        dva = xgb.DMatrix(X_np[va], label=y_np[va])

        params = {
            "objective": "reg:squarederror",
            "eval_metric": "mae",
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

    os.makedirs(ARTIFACT_DIR, exist_ok=True)

    simulated_now = get_simulated_now()        # ✅ tempo simulato unico
    version = simulated_now.strftime("%Y%m%d_%H%M%S")
    artifact_path = os.path.join(
        ARTIFACT_DIR, f"{MODEL_NAME}_{version}.json"
    )
    best_model.save_model(artifact_path)

    upsert_sql = text("""
        INSERT INTO analytics.ml_models(
            model_name,
            model_version,
            trained_at,
            metrics_json,
            artifact_path
        )
        VALUES (:mn, :mv, :ta, :mj, :ap)
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
            "ta": simulated_now,          # ✅
            "mj": json.dumps(best_metrics),
            "ap": artifact_path
        })

    print(f"[training] saved {artifact_path} metrics={best_metrics}")

def main():
    engine = create_engine(PG_DSN)

    # train at startup
    train_once(engine)

    # retrain loop
    while True:
        time.sleep(60)

        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT trained_at FROM analytics.ml_models "
                    "WHERE model_name=:mn"
                ),
                {"mn": MODEL_NAME}
            ).fetchone()

        if row is None:
            train_once(engine)
            continue

        trained_at = row[0]              # timezone-aware (Postgres)
        now = get_simulated_now()         # naive

        now = now.replace(tzinfo=timezone.utc)

        if now - trained_at >= timedelta(days=RETRAIN_DAYS):
            train_once(engine)


if __name__ == "__main__":
    main()
