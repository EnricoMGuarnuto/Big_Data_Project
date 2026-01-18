import gc
import glob
import json
import os
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from itertools import chain
from typing import Iterable, Optional, Tuple

from simulated_time.redis_helpers import get_simulated_now


# -----------------------------
# ENV / CONFIG
# -----------------------------
PG_DSN = os.getenv(
    "PG_DSN",
    "postgresql+psycopg2://bdt_user:bdt_password@postgres:5432/smart_shelf",
)

MODEL_NAME = os.getenv("MODEL_NAME", "xgb_batches_to_order")
ARTIFACT_DIR = os.getenv("ARTIFACT_DIR", "/models")
ALLOW_DISK_MODEL = os.getenv("ALLOW_DISK_MODEL", "1") == "1"

# Scheduling: align with wh-supplier-manager cutoff (Sun/Tue/Thu 12:00 UTC).
# We trigger slightly BEFORE cutoff to ensure pending plans exist in time.
#
# weekday(): Mon=0..Sun=6
CUTOFF_DOWS = os.getenv("CUTOFF_DOWS", "6,1,3")  # Sun, Tue, Thu
CUTOFF_HOUR = int(os.getenv("CUTOFF_HOUR", os.getenv("RUN_HOUR", "12")))
CUTOFF_MINUTE = int(os.getenv("CUTOFF_MINUTE", os.getenv("RUN_MINUTE", "0")))
PREDICT_LEAD_MINUTES = int(os.getenv("PREDICT_LEAD_MINUTES", "5"))

SLEEP_SECONDS = int(os.getenv("SLEEP_SECONDS", "20"))
HEARTBEAT_SECONDS = int(os.getenv("HEARTBEAT_SECONDS", "60"))
INFER_CHUNK_ROWS = int(os.getenv("INFER_CHUNK_ROWS", "2000"))

INFER_SQL = os.getenv(
    "INFER_SQL",
    """
    SELECT *
    FROM analytics.v_ml_features
    WHERE feature_date = :today
      AND is_warehouse_alert = 1
      AND warehouse_capacity > 0
    ORDER BY shelf_id
    """,
)

DELTA_ENABLED = os.getenv("DELTA_ENABLED", "1") == "1"
DELTA_WHSUPPLIER_PLAN_PATH = os.getenv(
    "DELTA_WHSUPPLIER_PLAN_PATH",
    "/delta/ops/wh_supplier_plan",
)
DELTA_WRITE_MAX_RETRIES = int(os.getenv("DELTA_WRITE_MAX_RETRIES", "5"))
DELTA_WRITE_RETRY_BASE_S = float(os.getenv("DELTA_WRITE_RETRY_BASE_S", "0.8"))

FEATURE_COLUMNS_PATH = os.getenv(
    "FEATURE_COLUMNS_PATH",
    os.path.join(ARTIFACT_DIR, f"{MODEL_NAME}_feature_columns.json"),
)

KAFKA_ENABLED = os.getenv("KAFKA_ENABLED", "0") == "1"
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_WH_SUPPLIER_PLAN = os.getenv("TOPIC_WH_SUPPLIER_PLAN", "wh_supplier_plan")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_LAST_RUN_KEY = os.getenv("REDIS_LAST_RUN_KEY", f"ml:{MODEL_NAME}:supplier_plan:last_run_day")


# -----------------------------
# TIME / SCHEDULING
# -----------------------------
def ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_dows(value: str) -> Tuple[int, ...]:
    dows = []
    for part in (value or "").split(","):
        part = part.strip()
        if not part:
            continue
        dows.append(int(part))
    return tuple(sorted(set(dows)))


_CUTOFF_DOWS = _parse_dows(CUTOFF_DOWS)


def cutoff_dt(sim_now: datetime) -> datetime:
    return sim_now.replace(hour=CUTOFF_HOUR, minute=CUTOFF_MINUTE, second=0, microsecond=0)


def should_generate_plans(sim_now: datetime) -> bool:
    if sim_now.weekday() not in _CUTOFF_DOWS:
        return False
    trigger = cutoff_dt(sim_now) - timedelta(minutes=max(0, PREDICT_LEAD_MINUTES))
    return sim_now >= trigger


def _redis_client():
    import redis

    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def _get_last_run_day() -> Optional[str]:
    try:
        return _redis_client().get(REDIS_LAST_RUN_KEY)
    except Exception as e:
        print(f"[inference] warning: cannot read redis last-run key: {e}")
        return None


def _set_last_run_day(day_iso: str) -> None:
    try:
        _redis_client().set(REDIS_LAST_RUN_KEY, day_iso)
    except Exception as e:
        print(f"[inference] warning: cannot write redis last-run key: {e}")


# -----------------------------
# MODEL RESOLUTION
# -----------------------------
def _latest_model_from_disk() -> Tuple[Optional[str], Optional[str]]:
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


def get_latest_model(conn) -> Tuple[Optional[str], Optional[str]]:
    from sqlalchemy import text

    row = conn.execute(
        text(
            """
            SELECT model_version, artifact_path
            FROM analytics.ml_models
            WHERE model_name = :mn
            ORDER BY trained_at DESC
            LIMIT 1
            """
        ),
        {"mn": MODEL_NAME},
    ).fetchone()

    if row is not None and row[1] and os.path.exists(row[1]):
        return row[0], row[1]

    if not ALLOW_DISK_MODEL:
        return None, None

    return _latest_model_from_disk()


def parse_model_version_ts(version: str) -> Optional[datetime]:
    try:
        return datetime.strptime(version, "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def ensure_model_registry(conn, model_version: str, artifact_path: str, trained_at: datetime) -> None:
    from sqlalchemy import text

    conn.execute(
        text(
            """
            INSERT INTO analytics.ml_models(model_name, model_version, trained_at, artifact_path)
            VALUES (:mn, :mv, :ta, :ap)
            ON CONFLICT (model_name) DO NOTHING;
            """
        ),
        {"mn": MODEL_NAME, "mv": model_version, "ta": trained_at, "ap": artifact_path},
    )


# -----------------------------
# FEATURES
# -----------------------------
def get_latest_feature_date(conn, today):
    from sqlalchemy import text

    row = conn.execute(
        text(
            """
            SELECT MAX(feature_date)
            FROM analytics.v_ml_features
            WHERE feature_date <= :today
            """
        ),
        {"today": today},
    ).fetchone()
    return row[0] if row and row[0] else None


def load_training_feature_columns() -> Optional[list]:
    try:
        if os.path.exists(FEATURE_COLUMNS_PATH):
            with open(FEATURE_COLUMNS_PATH, "r") as f:
                return json.load(f)
    except Exception as e:
        print(f"[inference] warning: failed to load feature columns: {e}")
    return None


def one_hot_like_training(df):
    import pandas as pd

    cat_cols = ["item_category", "item_subcategory"]
    for c in cat_cols:
        if c in df.columns:
            df[c] = df[c].astype("category")
    df = pd.get_dummies(df, columns=[c for c in cat_cols if c in df.columns], drop_first=False)
    return df


def align_columns(X, training_cols: list):
    return X.reindex(columns=training_cols, fill_value=0)


def iter_infer_frames(conn, feature_date, chunksize: int) -> Iterable:
    import pandas as pd
    from sqlalchemy import text

    if chunksize and chunksize > 0:
        yield from pd.read_sql(text(INFER_SQL), conn, params={"today": feature_date}, chunksize=chunksize)
        return
    yield pd.read_sql(text(INFER_SQL), conn, params={"today": feature_date})


# -----------------------------
# OUTPUTS (Postgres log, Kafka plan, Delta mirror)
# -----------------------------
def insert_prediction_logs(conn, rows: list) -> None:
    if not rows:
        return

    from sqlalchemy import text

    log_pred_sql = text(
        """
        INSERT INTO analytics.ml_predictions_log(
            model_name, feature_date, shelf_id, predicted_batches, suggested_qty, model_version
        )
        VALUES (:mn, :fd, :sid, :pb, :sq, :mv)
        ON CONFLICT (model_name, feature_date, shelf_id, model_version)
        DO NOTHING;
        """
    )
    conn.execute(log_pred_sql, rows)


def publish_kafka_plans(plans: list) -> None:
    if not KAFKA_ENABLED or not plans:
        return

    try:
        from kafka import KafkaProducer
    except Exception as e:
        print(f"[inference] kafka-python not available -> skip kafka publish. error={e}")
        return

    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
    try:
        for plan in plans:
            key = plan["shelf_id"].encode("utf-8")
            value = json.dumps(plan, default=str).encode("utf-8")
            producer.send(TOPIC_WH_SUPPLIER_PLAN, key=key, value=value)
        producer.flush(timeout=30)
        print(f"[inference] published {len(plans)} plans to kafka topic={TOPIC_WH_SUPPLIER_PLAN}")
    finally:
        try:
            producer.close(timeout=5)
        except Exception:
            pass


def upsert_delta_plans_by_shelf_id(plans_df):
    if not DELTA_ENABLED:
        return
    if plans_df is None or plans_df.empty:
        return

    try:
        import pandas as pd
        from deltalake import DeltaTable
        from deltalake.writer import write_deltalake
    except Exception as e:
        print(f"[delta] deltalake not available -> skip delta write. error={e}")
        return

    plans_df = plans_df.copy()
    plans_df["shelf_id"] = plans_df["shelf_id"].astype(str).str.strip()

    # Ensure stable column set: preserve existing schema columns if present.
    if os.path.exists(DELTA_WHSUPPLIER_PLAN_PATH):
        try:
            table = DeltaTable(DELTA_WHSUPPLIER_PLAN_PATH)
            existing_fields = [f.name for f in table.schema().fields]
        except Exception as e:
            print(f"[delta] failed to read schema, will overwrite using input schema. error={e}")
            existing_fields = []
    else:
        existing_fields = []

    if existing_fields:
        for col in existing_fields:
            if col not in plans_df.columns:
                plans_df[col] = None
        for col in plans_df.columns:
            if col not in existing_fields:
                existing_fields.append(col)
        plans_df = plans_df[existing_fields]

    # Delta-rs merge API is version-dependent; implement a safe overwrite-based upsert:
    # - read existing table (small: 1 row per shelf_id)
    # - drop rows for shelves we are updating
    # - append new rows
    # - overwrite table
    merged = plans_df
    if os.path.exists(DELTA_WHSUPPLIER_PLAN_PATH):
        try:
            dt = DeltaTable(DELTA_WHSUPPLIER_PLAN_PATH)
            if hasattr(dt, "to_pandas"):
                existing = dt.to_pandas()
            elif hasattr(dt, "to_pyarrow_table"):
                existing = dt.to_pyarrow_table().to_pandas()
            else:
                raise RuntimeError("DeltaTable cannot be materialized to pandas")
            if not existing.empty and "shelf_id" in existing.columns:
                existing["shelf_id"] = existing["shelf_id"].astype(str).str.strip()
                keep = existing[~existing["shelf_id"].isin(set(plans_df["shelf_id"].tolist()))]
                merged = pd.concat([keep, plans_df], ignore_index=True)
        except Exception as e:
            print(f"[delta] failed to merge existing plans, overwriting with input only. error={e}")

    last = None
    for attempt in range(1, DELTA_WRITE_MAX_RETRIES + 1):
        try:
            write_deltalake(DELTA_WHSUPPLIER_PLAN_PATH, merged, mode="overwrite")
            print(f"[delta] upserted {len(plans_df)} shelves into {DELTA_WHSUPPLIER_PLAN_PATH}")
            return
        except Exception as e:
            last = e
            sleep_s = DELTA_WRITE_RETRY_BASE_S * attempt
            print(f"[delta] write failed ({attempt}/{DELTA_WRITE_MAX_RETRIES}) -> sleep {sleep_s}s: {e}")
            time.sleep(sleep_s)
    raise last


# -----------------------------
# MAIN (single cutoff run)
# -----------------------------
def run_cutoff_once(engine, sim_now: datetime, training_cols: Optional[list]) -> bool:
    import pandas as pd
    import xgboost as xgb

    today = sim_now.date()
    with engine.begin() as conn:
        feature_date = today
        frames_iter = iter_infer_frames(conn, feature_date, chunksize=INFER_CHUNK_ROWS)
        first = next(iter(frames_iter), None)

        if first is None or first.empty:
            feature_date = get_latest_feature_date(conn, today)
            if feature_date:
                print(f"[inference] no features for {today}, using latest {feature_date}")
                frames_iter = iter_infer_frames(conn, feature_date, chunksize=INFER_CHUNK_ROWS)
                first = next(iter(frames_iter), None)

        if first is None or first.empty:
            print(f"[inference] no shelves to infer on {today}")
            return True  # nothing to do today; treat as completed

        model_version, artifact_path = get_latest_model(conn)
        if not model_version or not artifact_path:
            print("[inference] no model available yet (db empty or artifact missing).")
            return False

        trained_at = parse_model_version_ts(model_version) or sim_now
        ensure_model_registry(conn, model_version, artifact_path, trained_at)

        booster = xgb.Booster()
        booster.load_model(artifact_path)

        prediction_log_rows = []
        plan_rows = []

        for df in chain([first], frames_iter):
            if df.empty:
                continue

            df = df.reset_index(drop=True)
            drop_cols = [c for c in ["batches_to_order", "feature_date", "shelf_id"] if c in df.columns]
            X = df.drop(columns=drop_cols)
            X = one_hot_like_training(X)

            if training_cols is not None:
                X = align_columns(X, training_cols)

            dmat = xgb.DMatrix(X.values)
            pred = booster.predict(dmat)
            pred_batches = [max(0, int(round(p))) for p in pred]

            for i, row in df.iterrows():
                shelf_id = str(row["shelf_id"]).strip()
                batch_size = int(row.get("standard_batch_size", 1) or 1)
                predicted_batches = int(pred_batches[i])
                suggested_qty = int(predicted_batches * batch_size)

                prediction_log_rows.append(
                    {
                        "mn": MODEL_NAME,
                        "fd": feature_date,
                        "sid": shelf_id,
                        "pb": predicted_batches,
                        "sq": suggested_qty,
                        "mv": model_version,
                    }
                )

                if suggested_qty <= 0:
                    continue

                # Deterministic UUID per (cutoff day, model version, shelf) to keep retries idempotent.
                stable_key = f"{MODEL_NAME}:{model_version}:{today.isoformat()}:{shelf_id}"
                supplier_plan_id = str(uuid.uuid5(uuid.NAMESPACE_URL, stable_key))

                plan_rows.append(
                    {
                        "supplier_plan_id": supplier_plan_id,
                        "shelf_id": shelf_id,
                        "suggested_qty": suggested_qty,
                        "standard_batch_size": batch_size,
                        "status": "pending",
                        "created_at": sim_now.isoformat(),
                        "updated_at": sim_now.isoformat(),
                    }
                )

        insert_prediction_logs(conn, prediction_log_rows)

    # Publish/update plans outside the transaction (side effects).
    publish_kafka_plans(plan_rows)

    delta_df = pd.DataFrame(plan_rows)
    # Ensure timestamps are real timestamps for Delta
    if not delta_df.empty:
        delta_df["created_at"] = pd.to_datetime(delta_df["created_at"], utc=True, errors="coerce")
        delta_df["updated_at"] = pd.to_datetime(delta_df["updated_at"], utc=True, errors="coerce")
    upsert_delta_plans_by_shelf_id(delta_df)

    print(
        f"[inference] cutoff run done: sim_day={today} feature_date={feature_date} "
        f"plans={len(plan_rows)} (pending), predictions_logged={len(prediction_log_rows)}"
    )
    return True


def main():
    # Keep idle memory low: heavy imports (pandas/xgboost/deltalake) happen only at run time.
    from sqlalchemy import create_engine

    engine = create_engine(PG_DSN)
    training_cols = load_training_feature_columns()
    last_heartbeat = 0.0

    print(
        "[inference] starting with "
        f"CUTOFF_DOWS={_CUTOFF_DOWS} CUTOFF_HOUR={CUTOFF_HOUR} CUTOFF_MINUTE={CUTOFF_MINUTE} "
        f"PREDICT_LEAD_MINUTES={PREDICT_LEAD_MINUTES} SLEEP_SECONDS={SLEEP_SECONDS} "
        f"HEARTBEAT_SECONDS={HEARTBEAT_SECONDS} KAFKA_ENABLED={int(KAFKA_ENABLED)}"
    )

    while True:
        time.sleep(max(1, SLEEP_SECONDS))

        sim_now = ensure_utc(get_simulated_now())
        if not should_generate_plans(sim_now):
            if HEARTBEAT_SECONDS > 0 and time.time() - last_heartbeat >= HEARTBEAT_SECONDS:
                run_at = cutoff_dt(sim_now) - timedelta(minutes=max(0, PREDICT_LEAD_MINUTES))
                print(f"[inference] waiting for >= {run_at.isoformat()} (sim_now={sim_now.isoformat()})")
                last_heartbeat = time.time()
            continue

        today = sim_now.date().isoformat()
        if _get_last_run_day() == today:
            continue

        ran = run_cutoff_once(engine, sim_now, training_cols)
        if ran:
            _set_last_run_day(today)
        gc.collect()


if __name__ == "__main__":
    main()
