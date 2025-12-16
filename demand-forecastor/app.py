import os
import json
import psycopg2
import pandas as pd
from datetime import date, timedelta
from psycopg2.extras import execute_values
from sklearn.linear_model import SGDRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.exceptions import NotFittedError
from joblib import dump, load

# --------------------
# Config
# --------------------
DB = {
    "host": os.getenv("PG_HOST", "postgres"),
    "port": int(os.getenv("PG_PORT", "5432")),
    "dbname": os.getenv("PG_DB", "retaildb"),
    "user": os.getenv("PG_USER", "retail"),
    "password": os.getenv("PG_PASS", "retailpass"),
}
MODEL_DIR = os.getenv("MODEL_DIR", "/app/model")
os.makedirs(MODEL_DIR, exist_ok=True)
MODEL_PATH  = os.path.join(MODEL_DIR, "sgd_model.joblib")
SCALER_PATH = os.path.join(MODEL_DIR, "scaler.joblib")
MODEL_VERSION = "sgd_v1"

HORIZON_DAYS = int(os.getenv("HORIZON_DAYS", "3"))
FEATURE_COLS = [
    "sales_1d", "sales_3d", "sales_7d", "sales_1w_ago",
    "traffic_1d", "traffic_3d",
    "is_discounted", "is_weekend"
]

# --------------------
# DB helpers
# --------------------
def conn():
    return psycopg2.connect(**DB)

def refresh_feature_table():
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT refresh_feature_demand_forecast();")

def load_features_for_day(d: date) -> pd.DataFrame:
    q = """
        SELECT *
        FROM feature_demand_forecast
        WHERE business_date = %s
    """
    with conn() as c:
        return pd.read_sql(q, c, params=[d])

def load_training_hist(cutoff: date) -> pd.DataFrame:
    """
    Labeled history up to cutoff (excluded).
    We use future_sales_3d as the target y.
    """
    q = """
        SELECT *
        FROM feature_demand_forecast
        WHERE business_date <= %s
          AND future_sales_3d IS NOT NULL
    """
    with conn() as c:
        return pd.read_sql(q, c, params=[cutoff])

def insert_predictions(rows):
    # rows: list of tuples matching columns below
    sql = """
        INSERT INTO demand_predictions
        (prediction_date, shelf_id, horizon_days, y_hat, model_version, features_json)
        VALUES %s
    """
    with conn() as c, c.cursor() as cur:
        execute_values(cur, sql, rows)

def matured_predictions_ids(cutoff_date: date):
    """
    Returns prediction_id whose horizon has elapsed
    (prediction_date + horizon_days <= cutoff_date) and y_true is NULL.
    """
    q = """
        SELECT prediction_id
        FROM demand_predictions
        WHERE y_true IS NULL
          AND (prediction_date + (horizon_days || ' days')::interval) <= %s::date
    """
    with conn() as c:
        df = pd.read_sql(q, c, params=[cutoff_date])
    return df["prediction_id"].tolist()

def fetch_labels_for(pred_ids):
    if not pred_ids:
        return pd.DataFrame(columns=["prediction_id", "y_true"])
    ids_tuple = tuple(pred_ids)
    q = """
        SELECT v.prediction_id, v.y_true
        FROM v_prediction_labels v
        WHERE v.prediction_id IN %s
    """
    with conn() as c:
        return pd.read_sql(q, c, params=[ids_tuple])

def fetch_prediction_rows(pred_ids):
    if not pred_ids:
        return pd.DataFrame(columns=["prediction_id","prediction_date","shelf_id","y_hat","features_json"])
    ids_tuple = tuple(pred_ids)
    q = """
        SELECT prediction_id, prediction_date, shelf_id, y_hat, features_json
        FROM demand_predictions
        WHERE prediction_id IN %s
        ORDER BY prediction_id
    """
    with conn() as c:
        return pd.read_sql(q, c, params=[ids_tuple])

def update_outcomes(rows):
    """
    rows: list of (y_true, mae, mape, prediction_id)
    """
    sql = """
        UPDATE demand_predictions
        SET y_true = %s, mae = %s, mape = %s, updated_at = now()
        WHERE prediction_id = %s
    """
    with conn() as c, c.cursor() as cur:
        cur.executemany(sql, rows)

# --------------------
# Model helpers
# --------------------
def load_or_init_model():
    if os.path.exists(MODEL_PATH) and os.path.exists(SCALER_PATH):
        model  = load(MODEL_PATH)
        scaler = load(SCALER_PATH)
        return model, scaler, True
    # Initialize incremental model
    model = SGDRegressor(loss="squared_error", penalty="l2", alpha=1e-4, random_state=42)
    scaler = StandardScaler(with_mean=True, with_std=True)
    return model, scaler, False

def save_model(model, scaler):
    dump(model, MODEL_PATH)
    dump(scaler, SCALER_PATH)

def to_X(df: pd.DataFrame) -> pd.DataFrame:
    X = df[FEATURE_COLS].copy()
    for col in ["is_discounted", "is_weekend"]:
        X[col] = X[col].astype(int)
    return X

def warm_start_if_needed(model, scaler, is_fitted: bool, today: date):
    """
    If the model is not fitted yet, bootstrap it from history
    (all feature_demand_forecast rows labeled up to today - HORIZON_DAYS).
    """
    if is_fitted:
        return model, scaler, True

    cutoff = today - timedelta(days=HORIZON_DAYS)  # ensure labels are available
    hist = load_training_hist(cutoff)
    if hist.empty:
        # No history → initialize “blindly”: no training; predict will use fallback.
        return model, scaler, False

    X = to_X(hist)
    y = hist["future_sales_3d"].astype(float).values

    # scaler + initial fit
    scaler.partial_fit(X.values)
    Xs = scaler.transform(X.values)
    model.partial_fit(Xs, y)

    save_model(model, scaler)
    return model, scaler, True

# --------------------
# Pipeline steps
# --------------------
def step_refresh_and_predict(today: date):
    # 1) refresh features
    refresh_feature_table()
    # 2) get today's features
    df = load_features_for_day(today)
    if df.empty:
        print(f"[{today}] No features available, skipping predictions.")
        return

    # load/init model+scaler (+ warm start if needed)
    model, scaler, has_files = load_or_init_model()
    model, scaler, fitted = warm_start_if_needed(model, scaler, has_files, today)

    X = to_X(df)

    # Keep the scaler updated daily (drift-aware)
    scaler.partial_fit(X.values)
    Xs = scaler.transform(X.values)

    # If still not fitted (no history), use a simple fallback
    y_hat = None
    if not fitted:
        # Fallback: weighted average of historical sales (simple example)
        y_hat = (
            0.5 * df["sales_3d"].fillna(0).values / 3.0 +
            0.3 * df["sales_7d"].fillna(0).values / 7.0 +
            0.2 * df["sales_1d"].fillna(0).values
        )
        y_hat = y_hat.clip(min=0.0)
    else:
        try:
            y_hat = model.predict(Xs)
        except NotFittedError:
            # last-resort defense (should not happen): use fallback
            y_hat = df["sales_3d"].fillna(0).values / 3.0

    # salva predictions con snapshot delle feature
    rows = []
    for (_, r), y in zip(df.iterrows(), y_hat):
        feat_snapshot = {k: (int(r[k]) if k in ("is_discounted","is_weekend") else float(r[k])) for k in FEATURE_COLS}
        rows.append((
            today,               # prediction_date
            r["shelf_id"],
            HORIZON_DAYS,        # horizon_days
            float(max(0.0, y)),  # y_hat is non-negative
            MODEL_VERSION,
            json.dumps(feat_snapshot)
        ))
    insert_predictions(rows)
    # persist scaler (and model if it was warm-started)
    save_model(model, scaler)
    print(f"[{today}] Stored {len(rows)} predictions.")

def step_label_and_online_learn(cutoff_date: date, epsilon=1e-6):
    """
    cutoff_date: today -> label all predictions whose window is completed up to today.
    """
    pred_ids = matured_predictions_ids(cutoff_date)
    if not pred_ids:
        print(f"[{cutoff_date}] No matured predictions to label.")
        return

    labels = fetch_labels_for(pred_ids)              # prediction_id, y_true
    preds  = fetch_prediction_rows(pred_ids)         # + features_json, y_hat

    if labels.empty:
        print(f"[{cutoff_date}] No labels found (check data).")
        return

    df = preds.merge(labels, on="prediction_id", how="left")
    df = df.dropna(subset=["y_true"])
    if df.empty:
        print(f"[{cutoff_date}] No rows with y_true available.")
        return

    # load/init model & scaler
    model, scaler, _ = load_or_init_model()

    # build X from stored snapshots
    X_list, y_list = [], []
    outcomes = []  # rows for update_outcomes
    for _, r in df.iterrows():
        f = json.loads(r["features_json"])
        x = [f[col] for col in FEATURE_COLS]
        y_true = float(r["y_true"])
        y_hat  = float(r["y_hat"])
        mae = abs(y_true - y_hat)
        mape = abs(y_true - y_hat) / (abs(y_true) + epsilon)

        X_list.append(x)
        y_list.append(y_true)
        outcomes.append((y_true, mae, mape, int(r["prediction_id"])))

    X = pd.DataFrame(X_list, columns=FEATURE_COLS)
    # update scaler & transform
    scaler.partial_fit(X.values)
    Xs = scaler.transform(X.values)

    # ONLINE LEARNING
    model.partial_fit(Xs, y_list)

    # persist & update outcomes
    save_model(model, scaler)
    update_outcomes(outcomes)
    print(f"[{cutoff_date}] Online-learned on {len(X_list)} samples. Updated outcomes.")

# --------------------
# Main (daily)
# --------------------
if __name__ == "__main__":
    today = date.today()

    # 1) refresh + predict for today
    step_refresh_and_predict(today)

    # 2) label and update model for predictions with a completed window
    cutoff = today
    step_label_and_online_learn(cutoff)
