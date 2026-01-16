import glob
import os
import re
import time
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import create_engine, text
import xgboost as xgb

from simulated_time.redis_helpers import get_simulated_now  # tempo simulato


# -----------------------------
# ENV / CONFIG
# -----------------------------
PG_DSN = os.getenv(
    "PG_DSN",
    "postgresql+psycopg2://bdt_user:bdt_password@postgres:5432/smart_shelf"
)
MODEL_NAME = os.getenv("MODEL_NAME", "xgb_batches_to_order")
ARTIFACT_DIR = os.getenv("ARTIFACT_DIR", "/models")
ALLOW_DISK_MODEL = os.getenv("ALLOW_DISK_MODEL", "1") == "1"

RUN_HOUR = int(os.getenv("RUN_HOUR", "0"))
RUN_MINUTE = int(os.getenv("RUN_MINUTE", "5"))
HEARTBEAT_SECONDS = int(os.getenv("HEARTBEAT_SECONDS", "60"))

INFER_SQL = os.getenv(
    "INFER_SQL",
    """
    SELECT *
    FROM analytics.v_ml_features
    WHERE feature_date = :today
      AND is_warehouse_alert = 1
      AND warehouse_capacity > 0
    ORDER BY shelf_id
    """
)

DELTA_ENABLED = os.getenv("DELTA_ENABLED", "1") == "1"
DELTA_WHSUPPLIER_PLAN_PATH = os.getenv(
    "DELTA_WHSUPPLIER_PLAN_PATH",
    "/delta/ops/wh_supplier_plan"
)

FEATURE_COLUMNS_PATH = os.getenv(
    "FEATURE_COLUMNS_PATH",
    os.path.join(ARTIFACT_DIR, f"{MODEL_NAME}_feature_columns.json")
)


# -----------------------------
# HELPERS
# -----------------------------
def ensure_utc(dt):
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _latest_model_from_disk():
    pattern = os.path.join(ARTIFACT_DIR, f"{MODEL_NAME}_*.json")
    candidates = []

    for path in sorted(glob.glob(pattern)):
        fname = os.path.basename(path)
        match = re.match(rf"^{re.escape(MODEL_NAME)}_(\d{{8}}_\d{{6}})\.json$", fname)
        if not match:
            continue
        version = match.group(1)
        candidates.append((version, path))

    if not candidates:
        return None, None

    version, path = sorted(candidates)[-1]
    return version, path


def get_latest_model(conn):
    row = conn.execute(
        text("""
            SELECT model_version, artifact_path
            FROM analytics.ml_models
            WHERE model_name = :mn
            ORDER BY trained_at DESC
            LIMIT 1
        """),
        {"mn": MODEL_NAME}
    ).fetchone()

    if row is not None and row[1] and os.path.exists(row[1]):
        return row[0], row[1]

    if not ALLOW_DISK_MODEL:
        return None, None

    return _latest_model_from_disk()


def parse_model_version_ts(version: str):
    try:
        return datetime.strptime(version, "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def ensure_model_registry(conn, model_version: str, artifact_path: str, trained_at):
    conn.execute(
        text("""
            INSERT INTO analytics.ml_models(
                model_name, model_version, trained_at, artifact_path
            )
            VALUES (:mn, :mv, :ta, :ap)
            ON CONFLICT (model_name) DO NOTHING;
        """),
        {"mn": MODEL_NAME, "mv": model_version, "ta": trained_at, "ap": artifact_path}
    )

def get_latest_feature_date(conn, today):
    row = conn.execute(
        text("""
            SELECT MAX(feature_date)
            FROM analytics.v_ml_features
            WHERE feature_date <= :today
        """),
        {"today": today}
    ).fetchone()
    return row[0] if row and row[0] else None


def one_hot_like_training(df: pd.DataFrame) -> pd.DataFrame:
    cat_cols = ["item_category", "item_subcategory"]
    for c in cat_cols:
        if c in df.columns:
            df[c] = df[c].astype("category")
    df = pd.get_dummies(df, columns=[c for c in cat_cols if c in df.columns], drop_first=False)
    return df


def align_columns(X: pd.DataFrame, training_cols: list) -> pd.DataFrame:
    return X.reindex(columns=training_cols, fill_value=0)


def load_training_feature_columns():
    try:
        import json
        if os.path.exists(FEATURE_COLUMNS_PATH):
            with open(FEATURE_COLUMNS_PATH, "r") as f:
                return json.load(f)
    except Exception:
        pass
    return None


def should_run_now(sim_now):
    run_dt = sim_now.replace(hour=RUN_HOUR, minute=RUN_MINUTE, second=0, microsecond=0)
    return sim_now >= run_dt


def write_delta_plans(plans_df: pd.DataFrame):
    if not DELTA_ENABLED or plans_df.empty:
        return

    try:
        from deltalake import DeltaTable
        from deltalake.writer import write_deltalake
    except Exception as e:
        print(f"[delta] deltalake not available -> skip delta write. error={e}")
        return

    try:
        target_df = plans_df
        if os.path.exists(DELTA_WHSUPPLIER_PLAN_PATH):
            try:
                table = DeltaTable(DELTA_WHSUPPLIER_PLAN_PATH)
                fields = [f.name for f in table.schema().fields]
                if fields:
                    target_df = plans_df.copy()
                    for col in fields:
                        if col not in target_df.columns:
                            target_df[col] = None
                    target_df = target_df[fields]
            except Exception as e:
                print(f"[delta] failed to read existing schema, using input df. error={e}")
        write_deltalake(
            DELTA_WHSUPPLIER_PLAN_PATH,
            target_df,
            mode="append"
        )
        print(f"[delta] appended {len(target_df)} rows to {DELTA_WHSUPPLIER_PLAN_PATH}")
    except Exception as e:
        print(f"[delta] failed to write delta plans: {e}")


# -----------------------------
# MAIN
# -----------------------------
def main():
    engine = create_engine(PG_DSN)

    last_run_day = None
    training_cols = load_training_feature_columns()
    last_heartbeat = 0.0

    print(
        f"[inference] starting with RUN_HOUR={RUN_HOUR} RUN_MINUTE={RUN_MINUTE} "
        f"HEARTBEAT_SECONDS={HEARTBEAT_SECONDS}"
    )

    while True:
        time.sleep(20)

        sim_now = ensure_utc(get_simulated_now())
        if not should_run_now(sim_now):
            if HEARTBEAT_SECONDS > 0 and time.time() - last_heartbeat >= HEARTBEAT_SECONDS:
                run_dt = sim_now.replace(hour=RUN_HOUR, minute=RUN_MINUTE, second=0, microsecond=0)
                print(f"[inference] waiting for {run_dt.isoformat()} (sim_now={sim_now.isoformat()})")
                last_heartbeat = time.time()
            continue

        today = sim_now.date()
        if last_run_day == today:
            continue

        with engine.begin() as conn:
            feature_date = today
            df = pd.read_sql(text(INFER_SQL), conn, params={"today": feature_date})
            if df.empty:
                feature_date = get_latest_feature_date(conn, today)
                if feature_date:
                    print(f"[inference] no features for {today}, using latest {feature_date}")
                    df = pd.read_sql(text(INFER_SQL), conn, params={"today": feature_date})

            if df.empty:
                print(f"[inference] no shelves to infer on {today}")
                last_run_day = today
                continue

            model_version, artifact_path = get_latest_model(conn)
            if not model_version or not artifact_path:
                print("[inference] no model available yet (db empty or artifact missing).")
                continue

            trained_at = parse_model_version_ts(model_version) or sim_now
            ensure_model_registry(conn, model_version, artifact_path, trained_at)

            booster = xgb.Booster()
            booster.load_model(artifact_path)

            drop_cols = [c for c in ["batches_to_order", "feature_date", "shelf_id"] if c in df.columns]
            X = df.drop(columns=drop_cols)
            X = one_hot_like_training(X)

            if training_cols is not None:
                X = align_columns(X, training_cols)

            dmat = xgb.DMatrix(X.values)
            pred = booster.predict(dmat)
            pred_batches = [max(0, int(round(p))) for p in pred]

            upsert_plan_sql = text("""
                INSERT INTO ops.wh_supplier_plan(
                    shelf_id, plan_date, suggested_qty, standard_batch_size, status, created_at, updated_at
                )
                VALUES (:sid, :pd, :qty, :bs, 'pending', :now_ts, :now_ts)
                ON CONFLICT (shelf_id, plan_date)
                DO UPDATE SET
                    suggested_qty = EXCLUDED.suggested_qty,
                    standard_batch_size = EXCLUDED.standard_batch_size,
                    status = 'pending',
                    updated_at = EXCLUDED.updated_at;
            """)

            log_pred_sql = text("""
                INSERT INTO analytics.ml_predictions_log(
                    model_name, feature_date, shelf_id, predicted_batches, suggested_qty, model_version
                )
                VALUES (:mn, :fd, :sid, :pb, :sq, :mv)
                ON CONFLICT (model_name, feature_date, shelf_id, model_version)
                DO NOTHING;
            """)

            delta_rows = []

            for i, row in df.reset_index(drop=True).iterrows():
                sid = str(row["shelf_id"]).strip()
                bs = int(row.get("standard_batch_size", 1))
                pb = int(pred_batches[i])
                sq = int(pb * bs)

                conn.execute(log_pred_sql, {
                    "mn": MODEL_NAME,
                    "fd": feature_date,
                    "sid": sid,
                    "pb": pb,
                    "sq": sq,
                    "mv": model_version
                })

                if sq <= 0:
                    continue

                conn.execute(upsert_plan_sql, {
                    "sid": sid,
                    "pd": feature_date,
                    "qty": sq,
                    "bs": bs,
                    "now_ts": sim_now
                })

                delta_rows.append({
                    "shelf_id": sid,
                    "plan_date": str(feature_date),
                    "suggested_qty": sq,
                    "standard_batch_size": bs,
                    "status": "pending",
                    "model_name": MODEL_NAME,
                    "model_version": model_version,
                    "created_at": sim_now.isoformat(),
                    "updated_at": sim_now.isoformat()
                })

        write_delta_plans(pd.DataFrame(delta_rows))

        if feature_date != today:
            print(f"[inference] wrote pending plans for {feature_date} (sim day={today}, rows={len(delta_rows)})")
        else:
            print(f"[inference] wrote pending plans for {today} (rows={len(delta_rows)})")
        last_run_day = today


if __name__ == "__main__":
    main()
