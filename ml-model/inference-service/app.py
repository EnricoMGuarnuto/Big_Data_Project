import os
import math
import time
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text
import xgboost as xgb

from simulated_time.redis_helpers import get_simulated_now  # ✅

PG_DSN = os.getenv(
    "PG_DSN",
    "postgresql+psycopg2://postgres:example@db:5432/postgres"
)
MODEL_NAME = os.getenv("MODEL_NAME", "xgb_batches_to_order")
ARTIFACT_DIR = os.getenv("ARTIFACT_DIR", "/models")

RUN_HOUR = int(os.getenv("RUN_HOUR", "0"))
RUN_MINUTE = int(os.getenv("RUN_MINUTE", "5"))

INFER_SQL = os.getenv(
    "INFER_SQL",
    "SELECT * FROM analytics.v_ml_infer_today ORDER BY shelf_id"
)

def get_latest_model(engine):
    row = engine.execute(
        text("SELECT model_version, artifact_path FROM analytics.ml_models WHERE model_name=:mn"),
        {"mn": MODEL_NAME}
    ).fetchone()
    if row is None:
        raise RuntimeError("No model in analytics.ml_models. Start training-service first.")
    return row[0], row[1]

def one_hot_like_training(df: pd.DataFrame) -> pd.DataFrame:
    cat_cols = ["item_category", "item_subcategory"]
    for c in cat_cols:
        df[c] = df[c].astype("category")
    df = pd.get_dummies(df, columns=cat_cols, drop_first=False)
    return df

def should_run_now():
    now = get_simulated_now()  # ✅ usa tempo simulato
    return now.hour == RUN_HOUR and now.minute == RUN_MINUTE

def main():
    engine = create_engine(PG_DSN)

    last_run_day = None

    while True:
        time.sleep(20)

        if not should_run_now():
            continue

        today = get_simulated_now().date()  # ✅ usa tempo simulato
        if last_run_day == today:
            continue  # già fatto oggi

        with engine.begin() as conn:
            df = pd.read_sql(INFER_SQL, conn)

            if df.empty:
                print("[inference] no shelves to infer today")
                last_run_day = today
                continue

            model_version, artifact_path = get_latest_model(conn)
            booster = xgb.Booster()
            booster.load_model(artifact_path)

            y_cols = ["batches_to_order", "feature_date", "shelf_id"]
            drop_cols = [c for c in y_cols if c in df.columns]
            X = df.drop(columns=drop_cols)
            X = one_hot_like_training(X)

            dmat = xgb.DMatrix(X.values)
            pred = booster.predict(dmat)

            pred_batches = [max(0, int(round(p))) for p in pred]

            insert_plan = text("""
                INSERT INTO ops.wh_supplier_plan(shelf_id, suggested_qty, standard_batch_size, status, created_at, updated_at)
                VALUES (:sid, :qty, :bs, 'pending', NOW(), NOW())
                ON CONFLICT DO NOTHING;
            """)

            log_pred = text("""
                INSERT INTO analytics.ml_predictions_log(feature_date, shelf_id, predicted_batches, suggested_qty, model_version)
                VALUES (:fd, :sid, :pb, :sq, :mv);
            """)

            for i, row in df.reset_index(drop=True).iterrows():
                sid = row["shelf_id"]
                bs = int(row["standard_batch_size"])
                pb = int(pred_batches[i])
                sq = int(pb * bs)

                if sq <= 0:
                    continue

                conn.execute(insert_plan, {"sid": sid, "qty": sq, "bs": bs})
                conn.execute(log_pred, {"fd": today, "sid": sid, "pb": pb, "sq": sq, "mv": model_version})

        print(f"[inference] wrote pending plans for {today}")
        last_run_day = today

if __name__ == "__main__":
    main()
