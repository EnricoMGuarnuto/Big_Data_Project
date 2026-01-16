from __future__ import annotations

import os
import logging
from dataclasses import dataclass
from datetime import datetime, timezone, date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from PIL import Image

# Optional deps
try:
    import yaml
except Exception:
    yaml = None

try:
    from sqlalchemy import create_engine, text
except Exception:
    create_engine = None
    text = None

try:
    from deltalake import DeltaTable
except Exception:
    DeltaTable = None

try:
    from streamlit_plotly_events import plotly_events
except Exception:
    plotly_events = None

try:
    from streamlit_autorefresh import st_autorefresh
except Exception:
    st_autorefresh = None

# Simulated time (optional)
SIM_TIME_OK = False
try:
    from simulated_time.redis_helpers import get_simulated_now as _get_simulated_now
    get_simulated_now = _get_simulated_now
    SIM_TIME_OK = True
except Exception:
    try:
        import socket

        def _redis_get_sim_now(host: str, port: int, key: str = "sim:now") -> Optional[str]:
            cmd = f"*2\r\n$3\r\nGET\r\n${len(key)}\r\n{key}\r\n"
            with socket.create_connection((host, port), timeout=1.5) as sock:
                sock.sendall(cmd.encode("ascii"))
                data = sock.recv(4096)
            if not data:
                return None
            if data.startswith(b"$-1"):
                return None
            if not data.startswith(b"$"):
                raise RuntimeError(f"Unexpected Redis reply: {data[:100]!r}")
            header, rest = data.split(b"\r\n", 1)
            length = int(header[1:].decode("ascii"))
            return rest[:length].decode("utf-8")

        def get_simulated_now():
            host = os.getenv("REDIS_HOST", "redis")
            port = int(os.getenv("REDIS_PORT", "6379"))
            s = _redis_get_sim_now(host, port)
            if not s:
                raise RuntimeError(
                    "Redis sim:now not initialized yet. Start sim-clock "
                    "(e.g. `docker compose up -d sim-clock`)."
                )
            try:
                dt = datetime.fromisoformat(s)
            except ValueError as exc:
                raise RuntimeError(
                    f"Invalid sim:now value: {s}. Restart sim-clock to reset it."
                ) from exc
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt

        SIM_TIME_OK = True
    except Exception:
        SIM_TIME_OK = False


log = logging.getLogger("dashboard")

# -----------------------------
# PATHS & ENV
# -----------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_LAYOUT_YAML = PROJECT_ROOT / "dashboard" / "store_layout.yaml"
DEFAULT_INVENTORY_CSV = PROJECT_ROOT / "data" / "db_csv" / "store_inventory_final.csv"
DEFAULT_DELTA_SUPPLIER_PLAN = PROJECT_ROOT / "delta" / "ops" / "wh_supplier_plan"
DEFAULT_DELTA_SUPPLIER_ORDERS = PROJECT_ROOT / "delta" / "ops" / "wh_supplier_orders"
DEFAULT_DELTA_SHELF_STATE = PROJECT_ROOT / "delta" / "cleansed" / "shelf_state"
DEFAULT_DELTA_WH_STATE = PROJECT_ROOT / "delta" / "cleansed" / "wh_state"
DEFAULT_DELTA_SHELF_BATCH_STATE = PROJECT_ROOT / "delta" / "cleansed" / "shelf_batch_state"
DEFAULT_DELTA_WH_BATCH_STATE = PROJECT_ROOT / "delta" / "cleansed" / "wh_batch_state"
DEFAULT_DELTA_WH_EVENTS_RAW = PROJECT_ROOT / "delta" / "raw" / "wh_events"
DEFAULT_DELTA_FOOT_TRAFFIC = PROJECT_ROOT / "delta" / "raw" / "foot_traffic"

def _env_path(*keys: str, default: Path) -> Path:
    for k in keys:
        v = os.getenv(k)
        if v:
            return Path(v)
    return default

# supports both the new names and the ones from the previous compose
LAYOUT_YAML_PATH = _env_path("DASH_LAYOUT_YAML", "LAYOUT_YAML", "LAYOUT_YAML_PATH", default=DEFAULT_LAYOUT_YAML)
INVENTORY_CSV_PATH = _env_path("INVENTORY_CSV_PATH", "INVENTORY_CSV", default=DEFAULT_INVENTORY_CSV)

DELTA_SUPPLIER_PLAN_PATH = _env_path("DELTA_SUPPLIER_PLAN_PATH", default=DEFAULT_DELTA_SUPPLIER_PLAN)
DELTA_SUPPLIER_ORDERS_PATH = _env_path("DELTA_SUPPLIER_ORDERS_PATH", default=DEFAULT_DELTA_SUPPLIER_ORDERS)
DELTA_SHELF_STATE_PATH = _env_path("DL_SHELF_STATE_PATH", "DELTA_SHELF_STATE_PATH", default=DEFAULT_DELTA_SHELF_STATE)
DELTA_WH_STATE_PATH = _env_path("DL_WH_STATE_PATH", "DELTA_WH_STATE_PATH", default=DEFAULT_DELTA_WH_STATE)
DELTA_SHELF_BATCH_STATE_PATH = _env_path("DL_SHELF_BATCH_PATH", "DELTA_SHELF_BATCH_STATE_PATH", default=DEFAULT_DELTA_SHELF_BATCH_STATE)
DELTA_WH_BATCH_STATE_PATH = _env_path("DL_WH_BATCH_PATH", "DELTA_WH_BATCH_STATE_PATH", default=DEFAULT_DELTA_WH_BATCH_STATE)
DELTA_WH_EVENTS_RAW_PATH = _env_path("DL_WH_EVENTS_RAW", "DELTA_WH_EVENTS_RAW_PATH", default=DEFAULT_DELTA_WH_EVENTS_RAW)
DELTA_FOOT_TRAFFIC_PATH = _env_path("DL_FOOT_TRAFFIC_PATH", "DELTA_FOOT_TRAFFIC_PATH", default=DEFAULT_DELTA_FOOT_TRAFFIC)

ALERTS_TABLE_NAME = os.getenv("ALERTS_TABLE", "ops.alerts").strip()
SUPPLIER_PLAN_TABLE_NAME = os.getenv("SUPPLIER_PLAN_TABLE", "ops.wh_supplier_plan").strip()

AUTOREFRESH_MS = int(os.getenv("DASH_AUTOREFRESH_MS", os.getenv("DASH_REFRESH_MS", "1500")))

# -----------------------------
# DATA STRUCTURES
# -----------------------------
@dataclass
class Zone:
    id: str
    category: str
    label: str
    x0: float
    y0: float
    x1: float
    y1: float

    def contains(self, x: float, y: float) -> bool:
        return (self.x0 <= x <= self.x1) and (self.y0 <= y <= self.y1)

    def polygon(self) -> Tuple[List[float], List[float]]:
        xs = [self.x0, self.x1, self.x1, self.x0, self.x0]
        ys = [self.y0, self.y0, self.y1, self.y1, self.y0]
        return xs, ys


# -----------------------------
# HELPERS
# -----------------------------
def normalize_cat(s: Any) -> str:
    if s is None:
        return ""
    return str(s).strip().upper()

def normalize_location(s: Any) -> str:
    if s is None:
        return ""
    return str(s).strip().lower()

def must_have(dep_ok: bool, msg: str):
    if not dep_ok:
        st.error(msg)
        st.stop()

def now_utc_ts() -> pd.Timestamp:
    # dashboard usa simulated time se disponibile, altrimenti UTC reale
    if SIM_TIME_OK:
        dt = get_simulated_now()
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return pd.Timestamp(dt)
    return pd.Timestamp.utcnow().tz_localize("UTC")

def render_simulated_time() -> None:
    if not SIM_TIME_OK:
        st.info("Simulated time unavailable (redis helper not installed).")
        return
    try:
        dt = get_simulated_now()
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        st.metric("Simulated time (UTC)", dt.isoformat())
    except Exception as e:
        st.warning(f"Simulated time error: {e}")

def split_alerts_by_location(alerts_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if alerts_df.empty or "location" not in alerts_df.columns:
        return alerts_df, alerts_df.iloc[0:0]
    loc = alerts_df["location"].fillna("").map(normalize_location)
    store_mask = loc.str.contains("store") | loc.str.contains("shop")
    wh_mask = loc.str.contains("warehouse") | loc.str.startswith("wh")
    store_alerts = alerts_df[store_mask].copy()
    wh_alerts = alerts_df[wh_mask].copy()
    return store_alerts, wh_alerts


# -----------------------------
# LOAD LAYOUT (YAML)
# -----------------------------
@st.cache_data(show_spinner=False)
def _load_layout_cached(layout_path_str: str) -> Tuple[str, List[Dict[str, Any]], str]:
    if yaml is None:
        raise RuntimeError("pyyaml not installed. Add 'pyyaml' to requirements.")
    layout_path = Path(layout_path_str)
    if not layout_path.exists():
        raise FileNotFoundError(f"Layout YAML not found: {layout_path}")

    data = yaml.safe_load(layout_path.read_text())
    coord_type = str(data.get("coord_type", "normalized")).strip().lower()
    if coord_type != "normalized":
        raise ValueError("Only coord_type=normalized (0..1) is supported.")

    img_rel = str(data["image"]["path"])
    zones_payload: List[Dict[str, Any]] = []

    for z in data.get("zones", []):
        b = z["bounds"]
        zones_payload.append({
            "id": str(z["id"]),
            "category": normalize_cat(z["category"]),
            "label": str(z.get("label", z["id"])),
            "x0": float(b["x0"]),
            "y0": float(b["y0"]),
            "x1": float(b["x1"]),
            "y1": float(b["y1"]),
        })

    return img_rel, zones_payload, coord_type


def load_layout(layout_path: Path) -> Tuple[Path, List[Zone], str]:
    img_rel, zones_payload, coord_type = _load_layout_cached(str(layout_path.resolve()))
    img_path = (layout_path.parent / img_rel).resolve()
    zones: List[Zone] = [Zone(**zp) for zp in zones_payload]
    return img_path, zones, coord_type


# -----------------------------
# LOAD INVENTORY CSV
# -----------------------------
@st.cache_data(show_spinner=False)
def load_inventory(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(f"Inventory CSV not found: {csv_path}")
    df = pd.read_csv(csv_path)
    for col in ["shelf_id", "item_category", "item_subcategory"]:
        if col not in df.columns:
            raise ValueError(f"CSV is missing '{col}'. Columns: {list(df.columns)}")

    df["shelf_id"] = df["shelf_id"].astype(str).str.strip()
    df["item_category"] = df["item_category"].astype(str).map(normalize_cat)
    df["item_subcategory"] = df["item_subcategory"].astype(str).str.strip()
    return df

def build_shelf_maps(inv: pd.DataFrame) -> Tuple[Dict[str, str], Dict[str, str]]:
    shelf_to_cat = dict(zip(inv["shelf_id"], inv["item_category"]))
    shelf_to_sub = dict(zip(inv["shelf_id"], inv["item_subcategory"]))
    return shelf_to_cat, shelf_to_sub


# -----------------------------
# POSTGRES
# -----------------------------
def postgres_url_from_env() -> str:
    host = os.getenv("POSTGRES_HOST", "postgres")  # this is the docker service name
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", os.getenv("POSTGRES_DATABASE", "smart_shelf"))
    user = os.getenv("POSTGRES_USER", "bdt_user")
    pwd = os.getenv("POSTGRES_PASSWORD", "bdt_password")
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"

@st.cache_resource(show_spinner=False)
def get_pg_engine():
    must_have(create_engine is not None, "Manca SQLAlchemy. Installa sqlalchemy + psycopg2-binary.")
    return create_engine(postgres_url_from_env(), pool_pre_ping=True)

@st.cache_data(ttl=2, show_spinner=False)
def fetch_alerts() -> pd.DataFrame:
    eng = get_pg_engine()
    q = text(f"""
        SELECT
            alert_id,
            event_type,
            shelf_id,
            location,
            severity,
            current_stock,
            max_stock,
            target_pct,
            suggested_qty,
            status,
            created_at,
            updated_at
        FROM {ALERTS_TABLE_NAME}
        ORDER BY created_at DESC
        LIMIT 5000
    """)
    with eng.connect() as c:
        df = pd.read_sql(q, c)
    if "shelf_id" in df.columns:
        df["shelf_id"] = df["shelf_id"].astype(str).str.strip()
    if "status" in df.columns:
        df["status"] = df["status"].astype(str).str.strip().str.lower()
    return df

@st.cache_data(ttl=5, show_spinner=False)
def fetch_supplier_plan_pg(limit: int = 2000) -> pd.DataFrame:
    eng = get_pg_engine()
    q = text(f"""
        SELECT
            supplier_plan_id,
            shelf_id,
            plan_date,
            suggested_qty,
            standard_batch_size,
            status,
            created_at,
            updated_at
        FROM {SUPPLIER_PLAN_TABLE_NAME}
        ORDER BY plan_date DESC, updated_at DESC
        LIMIT {int(limit)}
    """)
    with eng.connect() as c:
        df = pd.read_sql(q, c)
    if "shelf_id" in df.columns:
        df["shelf_id"] = df["shelf_id"].astype(str).str.strip()
    if "status" in df.columns:
        df["status"] = df["status"].astype(str).str.strip().str.lower()
    if "plan_date" in df.columns:
        df["plan_date"] = pd.to_datetime(df["plan_date"], errors="coerce").dt.date
    return df

@st.cache_data(ttl=5, show_spinner=False)
def fetch_store_foot_traffic(target_date: date) -> Tuple[Optional[date], Optional[int]]:
    eng = get_pg_engine()
    q = text("""
        SELECT feature_date, COALESCE(SUM(people_count), 0) AS people_count
        FROM analytics.shelf_daily_features
        WHERE feature_date = :day
        GROUP BY feature_date
    """)
    with eng.connect() as c:
        df = pd.read_sql(q, c, params={"day": target_date})
    if not df.empty:
        return pd.to_datetime(df.loc[0, "feature_date"], errors="coerce").date(), int(df.loc[0, "people_count"])

    q_latest = text("""
        SELECT MAX(feature_date) AS feature_date
        FROM analytics.shelf_daily_features
        WHERE feature_date <= :day
    """)
    with eng.connect() as c:
        df_latest = pd.read_sql(q_latest, c, params={"day": target_date})
    if df_latest.empty or pd.isna(df_latest.loc[0, "feature_date"]):
        return None, None

    latest_date = pd.to_datetime(df_latest.loc[0, "feature_date"], errors="coerce").date()
    with eng.connect() as c:
        df_latest_sum = pd.read_sql(q, c, params={"day": latest_date})
    if df_latest_sum.empty:
        return latest_date, 0
    return latest_date, int(df_latest_sum.loc[0, "people_count"])

def compute_live_foot_traffic(now_ts: pd.Timestamp) -> Optional[int]:
    df = fetch_foot_traffic_raw(DELTA_FOOT_TRAFFIC_PATH)
    if df.empty or "entry_time" not in df.columns:
        return None
    df = df[df["entry_time"].notna()].copy()
    if df.empty:
        return None
    cutoff = now_ts - pd.Timedelta(days=1)
    df = df[df["entry_time"] >= cutoff]
    if df.empty:
        return 0
    if "exit_time" in df.columns:
        exit_ts = df["exit_time"]
        active = df[(df["entry_time"] <= now_ts) & (exit_ts.isna() | (exit_ts > now_ts))]
    else:
        active = df[df["entry_time"] <= now_ts]
    return int(len(active))

def active_alerts(alerts_df: pd.DataFrame) -> pd.DataFrame:
    if alerts_df.empty or "status" not in alerts_df.columns:
        return alerts_df
    return alerts_df[alerts_df["status"].isin(["open", "active", "pending"])].copy()

@st.cache_data(ttl=5, show_spinner=False)
def fetch_pg_table(table_name: str, limit: int = 2000) -> pd.DataFrame:
    allow = {"shelf_state", "wh_state", "shelf_batch_state", "wh_batch_state"}
    raw_name = table_name.strip()
    base_name = raw_name.split(".")[-1]
    if base_name not in allow:
        raise ValueError(f"Tabella non permessa: {table_name}")
    raise RuntimeError("fetch_pg_table is deprecated; use Delta paths in fetch_state_delta.")

# ML registry + predictions
@st.cache_data(ttl=5, show_spinner=False)
def fetch_ml_model_registry() -> pd.DataFrame:
    eng = get_pg_engine()
    q = text("""
        SELECT model_name, model_version, trained_at, metrics_json, artifact_path
        FROM analytics.ml_models
        ORDER BY trained_at DESC
        LIMIT 20
    """)
    with eng.connect() as c:
        return pd.read_sql(q, c)

@st.cache_data(ttl=5, show_spinner=False)
def fetch_ml_predictions(limit: int = 8000) -> pd.DataFrame:
    eng = get_pg_engine()
    q = text(f"""
        SELECT model_name, feature_date, shelf_id, predicted_batches, suggested_qty, model_version, created_at
        FROM analytics.ml_predictions_log
        ORDER BY feature_date DESC, created_at DESC
        LIMIT {int(limit)}
    """)
    with eng.connect() as c:
        df = pd.read_sql(q, c)
    if not df.empty:
        df["shelf_id"] = df["shelf_id"].astype(str).str.strip()
        df["model_name"] = df["model_name"].astype(str).str.strip()
        df["feature_date"] = pd.to_datetime(df["feature_date"], errors="coerce").dt.date
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
    return df


# -----------------------------
# DELTA HELPERS
# -----------------------------
def _pick_timestamp_column(df: pd.DataFrame) -> Optional[str]:
    for col in ["event_ts", "event_time", "timestamp", "ts", "created_at", "time"]:
        if col in df.columns:
            return col
    return None

def _to_datetime(series: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(series):
        return series
    if pd.api.types.is_numeric_dtype(series):
        s = series.dropna()
        if s.empty:
            return pd.to_datetime(series, unit="s", errors="coerce", utc=True)
        v = float(s.iloc[-1])
        unit = "ms" if v > 10_000_000_000 else "s"
        return pd.to_datetime(series, unit=unit, errors="coerce", utc=True)
    return pd.to_datetime(series, errors="coerce", utc=True)

def _is_delta_table(delta_path: Path) -> bool:
    log_dir = delta_path / "_delta_log"
    if not log_dir.exists() or not log_dir.is_dir():
        return False
    for ext in (".json", ".checkpoint"):
        if any(log_dir.glob(f"*{ext}")):
            return True
    return False

@st.cache_data(ttl=10, show_spinner=False)
def fetch_supplier_orders(delta_path: Path) -> pd.DataFrame:
    if DeltaTable is None or not delta_path.exists() or not _is_delta_table(delta_path):
        return pd.DataFrame()
    try:
        dt = DeltaTable(str(delta_path))
    except Exception as e:
        log.warning("Delta table not available at %s: %s", delta_path, e)
        return pd.DataFrame()
    df = dt.to_pandas()
    if df.empty:
        return df
    if "delivery_date" in df.columns:
        df["delivery_date"] = pd.to_datetime(df["delivery_date"], errors="coerce").dt.date
    for col in ["cutoff_ts", "delivery_ts"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
    if "status" in df.columns:
        df["status"] = df["status"].astype(str).str.strip().str.lower()
    return df

@st.cache_data(ttl=10, show_spinner=False)
def fetch_supplier_plan_delta(delta_path: Path) -> pd.DataFrame:
    if DeltaTable is None or not delta_path.exists() or not _is_delta_table(delta_path):
        return pd.DataFrame()
    try:
        dt = DeltaTable(str(delta_path))
    except Exception as e:
        log.warning("Delta table not available at %s: %s", delta_path, e)
        return pd.DataFrame()

    df = dt.to_pandas()
    if df.empty:
        return df

    if "shelf_id" in df.columns:
        df["shelf_id"] = df["shelf_id"].astype(str).str.strip()
    if "status" in df.columns:
        df["status"] = df["status"].astype(str).str.strip().str.lower()
    for col in ["created_at", "updated_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
    if "plan_date" in df.columns:
        df["plan_date"] = pd.to_datetime(df["plan_date"], errors="coerce").dt.date
    return df

@st.cache_data(ttl=10, show_spinner=False)
def fetch_wh_events_raw(delta_path: Path) -> pd.DataFrame:
    if DeltaTable is None or not delta_path.exists() or not _is_delta_table(delta_path):
        return pd.DataFrame()
    try:
        dt = DeltaTable(str(delta_path))
    except Exception as e:
        log.warning("Delta table not available at %s: %s", delta_path, e)
        return pd.DataFrame()

    df = dt.to_pandas()
    if df.empty:
        return df

    if "event_type" in df.columns:
        df["event_type"] = df["event_type"].astype(str).str.strip().str.lower()
    if "shelf_id" in df.columns:
        df["shelf_id"] = df["shelf_id"].astype(str).str.strip()
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    if "received_date" in df.columns:
        df["received_date"] = pd.to_datetime(df["received_date"], errors="coerce").dt.date
    return df

@st.cache_data(ttl=5, show_spinner=False)
def fetch_foot_traffic_raw(delta_path: Path) -> pd.DataFrame:
    if DeltaTable is None or not delta_path.exists() or not _is_delta_table(delta_path):
        return pd.DataFrame()
    try:
        dt = DeltaTable(str(delta_path))
    except Exception as e:
        log.warning("Delta table not available at %s: %s", delta_path, e)
        return pd.DataFrame()

    df = dt.to_pandas()
    if df.empty:
        return df

    if "entry_time" in df.columns:
        df["entry_time"] = pd.to_datetime(df["entry_time"], errors="coerce", utc=True)
    if "exit_time" in df.columns:
        df["exit_time"] = pd.to_datetime(df["exit_time"], errors="coerce", utc=True)
    return df

@st.cache_data(ttl=5, show_spinner=False)
def fetch_state_delta(table_name: str, limit: int = 2000) -> pd.DataFrame:
    allow = {
        "shelf_state": DELTA_SHELF_STATE_PATH,
        "wh_state": DELTA_WH_STATE_PATH,
        "shelf_batch_state": DELTA_SHELF_BATCH_STATE_PATH,
        "wh_batch_state": DELTA_WH_BATCH_STATE_PATH,
    }
    raw_name = table_name.strip()
    base_name = raw_name.split(".")[-1]
    if base_name not in allow:
        raise ValueError(f"Tabella non permessa: {table_name}")

    delta_path = allow[base_name]
    if DeltaTable is None or not delta_path.exists() or not _is_delta_table(delta_path):
        return pd.DataFrame()
    try:
        dt = DeltaTable(str(delta_path))
    except Exception as e:
        log.warning("Delta table not available at %s: %s", delta_path, e)
        return pd.DataFrame()

    df = dt.to_pandas()
    if df.empty:
        return df

    ts_col = _pick_timestamp_column(df)
    if ts_col is not None:
        df["_ts"] = _to_datetime(df[ts_col])
        df = df.sort_values("_ts", ascending=False).drop(columns=["_ts"])
    elif df.columns.tolist():
        try:
            df = df.sort_values(df.columns[0], ascending=False)
        except Exception:
            pass

    limit = int(limit)
    if limit > 0:
        df = df.head(limit)
    return df

def next_supplier_order_date(orders_df: pd.DataFrame) -> Optional[Tuple[Any, str]]:
    if orders_df.empty:
        return None
    df = orders_df.copy()
    if "status" in df.columns:
        df["status"] = df["status"].astype(str).str.strip().str.lower()
        df = df[~df["status"].isin(["canceled", "cancelled"])]
    if df.empty:
        return None

    if "delivery_ts" in df.columns:
        df["delivery_ts"] = pd.to_datetime(df["delivery_ts"], errors="coerce", utc=True)
        if df["delivery_ts"].notna().any():
            df = df[df["delivery_ts"].notna()]
            now = now_utc_ts()
            future = df[df["delivery_ts"] >= now]
            if future.empty:
                return None
            return future["delivery_ts"].min(), "delivery_ts"

    if "cutoff_ts" in df.columns:
        df["cutoff_ts"] = pd.to_datetime(df["cutoff_ts"], errors="coerce", utc=True)
        if df["cutoff_ts"].notna().any():
            df = df[df["cutoff_ts"].notna()]
            now = now_utc_ts()
            future = df[df["cutoff_ts"] >= now]
            if future.empty:
                return None
            return future["cutoff_ts"].min(), "cutoff_ts"

    if "delivery_date" in df.columns:
        df["delivery_date"] = pd.to_datetime(df["delivery_date"], errors="coerce").dt.date
        df = df[df["delivery_date"].notna()]
        if df.empty:
            return None
        today = now_utc_ts().date()
        df = df[df["delivery_date"] >= today]
        if df.empty:
            return None
        return df["delivery_date"].min(), "delivery_date"

    return None

def last_supplier_order_date(orders_df: pd.DataFrame) -> Optional[Tuple[Any, str]]:
    if orders_df.empty:
        return None
    df = orders_df.copy()
    if "status" in df.columns:
        df["status"] = df["status"].astype(str).str.strip().str.lower()
        df = df[~df["status"].isin(["canceled", "cancelled"])]
    if df.empty:
        return None

    if "delivery_ts" in df.columns:
        df["delivery_ts"] = pd.to_datetime(df["delivery_ts"], errors="coerce", utc=True)
        if df["delivery_ts"].notna().any():
            df = df[df["delivery_ts"].notna()]
            return df["delivery_ts"].max(), "delivery_ts"

    if "cutoff_ts" in df.columns:
        df["cutoff_ts"] = pd.to_datetime(df["cutoff_ts"], errors="coerce", utc=True)
        if df["cutoff_ts"].notna().any():
            df = df[df["cutoff_ts"].notna()]
            return df["cutoff_ts"].max(), "cutoff_ts"

    if "delivery_date" in df.columns:
        df["delivery_date"] = pd.to_datetime(df["delivery_date"], errors="coerce").dt.date
        df = df[df["delivery_date"].notna()]
        if df.empty:
            return None
        return df["delivery_date"].max(), "delivery_date"

    return None


# -----------------------------
# MAP FIGURE
# -----------------------------
def build_map_figure(map_img: Image.Image, zones: List[Zone], red_categories: set[str]) -> go.Figure:
    fig = go.Figure()
    fig.add_layout_image(
        dict(
            source=map_img,
            xref="x",
            yref="y",
            x=0,
            y=0,
            sizex=1,
            sizey=1,
            sizing="stretch",
            layer="below",
            opacity=1,
        )
    )
    fig.update_xaxes(range=[0, 1], showgrid=False, zeroline=False, visible=False)
    fig.update_yaxes(range=[1, 0], showgrid=False, zeroline=False, visible=False)

    for z in zones:
        cat = normalize_cat(z.category)
        is_red = cat in red_categories
        fillcolor = "rgba(255,0,0,0.25)" if is_red else "rgba(0,0,0,0.0)"
        line_color = "rgba(255,0,0,0.9)" if is_red else "rgba(0,0,0,0.0)"
        line_width = 3 if is_red else 1

        xs, ys = z.polygon()
        fig.add_trace(
            go.Scatter(
                x=xs,
                y=ys,
                mode="lines",
                fill="toself",
                fillcolor=fillcolor,
                line=dict(width=line_width, color=line_color),
                hoverinfo="skip",
                name=z.label,
                customdata=[{"zone_id": z.id, "category": cat, "label": z.label}] * len(xs),
                showlegend=False,
            )
        )

    fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), height=820)
    return fig

def find_zone_at_point(zones: List[Zone], x: float, y: float) -> Optional[Zone]:
    for z in zones:
        if z.contains(x, y):
            return z
    return None


# -----------------------------
# STREAMLIT APP
# -----------------------------
def main():
    st.set_page_config(page_title="Store Dashboard", layout="wide")

    must_have(yaml is not None, "Installa 'pyyaml' nei requirements.")
    must_have(plotly_events is not None, "Installa 'streamlit-plotly-events' nei requirements.")

    if st_autorefresh is not None:
        st_autorefresh(interval=AUTOREFRESH_MS, key="autorefresh")

    map_img_path, zones, _ = load_layout(LAYOUT_YAML_PATH)
    inv = load_inventory(INVENTORY_CSV_PATH)
    shelf_to_cat, shelf_to_sub = build_shelf_maps(inv)

    st.title("Supermarket Live Dashboard")
    render_simulated_time()

    st.markdown("**Current foot traffic**")
    try:
        sim_day = now_utc_ts().date()
        live_count = compute_live_foot_traffic(now_utc_ts())
        if live_count is not None:
            st.metric("People count (live)", int(live_count))
        else:
            ft_date, ft_count = fetch_store_foot_traffic(sim_day)
            if ft_date is None:
                st.info("Foot traffic data not available.")
            else:
                st.metric("People count (latest)", int(ft_count))
                if ft_date != sim_day:
                    st.caption(f"Latest available date: {ft_date}")
    except Exception as e:
        st.warning(f"Foot traffic error: {e}")

    tabs = st.tabs(["Map Live", "States", "Supplier Plan", "ML Model", "Alerts (raw)", "Debug"])

    # TAB 0: MAP LIVE
    with tabs[0]:
        col_left, col_right = st.columns([1.15, 0.85], gap="large")

        alerts_df = fetch_alerts()
        act_alerts = active_alerts(alerts_df)
        store_alerts, wh_alerts = split_alerts_by_location(act_alerts)

        red_cats = set()
        if not store_alerts.empty and "shelf_id" in store_alerts.columns:
            tmp = store_alerts.copy()
            tmp["item_category"] = tmp["shelf_id"].map(shelf_to_cat).map(normalize_cat)
            red_cats = set(tmp["item_category"].dropna().unique().tolist())

        with col_left:
            st.subheader("Live map (red=store alerts)")
            if not map_img_path.exists():
                st.error(f"Map image not found: {map_img_path}")
                st.stop()

            map_img = Image.open(map_img_path).convert("RGB")
            fig = build_map_figure(map_img, zones, red_cats)

            events = plotly_events(
                fig,
                click_event=True,
                hover_event=False,
                select_event=False,
                override_height=820,
                key="map_click",
            )

        with col_right:
            st.subheader("Store details")
            st.metric("Active alerts (store)", int(len(store_alerts)))
            st.metric("Active alerts (warehouse)", int(len(wh_alerts)))

            st.markdown("**Warehouse summary (chart)**")
            if wh_alerts.empty:
                st.info("No active warehouse alerts.")
            else:
                chart_col = "severity" if "severity" in wh_alerts.columns else ("event_type" if "event_type" in wh_alerts.columns else None)
                if chart_col is None:
                    st.info("Missing severity/event_type columns.")
                else:
                    wh_counts = (
                        wh_alerts[chart_col]
                        .fillna("unknown")
                        .astype(str)
                        .str.strip()
                        .str.lower()
                        .value_counts()
                        .sort_index()
                    )
                    fig_wh = go.Figure(data=[go.Bar(x=wh_counts.index.tolist(), y=wh_counts.values.tolist())])
                    fig_wh.update_layout(height=260, margin=dict(l=10, r=10, t=10, b=10), xaxis_title=chart_col, yaxis_title="Count")
                    st.plotly_chart(fig_wh, use_container_width=True)

            clicked_zone: Optional[Zone] = None
            clicked_cat: Optional[str] = None

            if events:
                e0 = events[0]
                cd = e0.get("customdata")
                if isinstance(cd, dict) and "category" in cd:
                    clicked_cat = normalize_cat(cd.get("category"))
                    for z in zones:
                        if normalize_cat(z.category) == clicked_cat:
                            clicked_zone = z
                            break

                if clicked_zone is None:
                    try:
                        x = float(e0.get("x"))
                        y = float(e0.get("y"))
                        clicked_zone = find_zone_at_point(zones, x, y)
                        if clicked_zone:
                            clicked_cat = normalize_cat(clicked_zone.category)
                    except Exception:
                        pass

            if clicked_cat:
                st.markdown(f"### Selected section: **{clicked_cat}**")
                sub = store_alerts.copy()
                if not sub.empty and "shelf_id" in sub.columns:
                    sub["item_category"] = sub["shelf_id"].map(shelf_to_cat).map(normalize_cat)
                    sub["item_subcategory"] = sub["shelf_id"].map(shelf_to_sub)
                    sub = sub[sub["item_category"] == clicked_cat].copy()

                if sub.empty:
                    st.info("No active store alerts in this section.")
                else:
                    st.error(f"Active store alerts in this section: {len(sub)}")
                    shelf_list = sorted(sub["shelf_id"].dropna().unique().tolist())
                    st.markdown("**Shelf in alert:**")
                    st.write(shelf_list)
                    cols = ["alert_id", "event_type", "shelf_id", "severity", "current_stock", "max_stock", "target_pct", "suggested_qty", "status", "created_at"]
                    cols = [c for c in cols if c in sub.columns]
                    st.markdown("**Alert details (latest):**")
                    st.dataframe(sub[cols].head(200), use_container_width=True)
            else:
                st.caption("Click a map section to see alerts in that area.")

    # TAB 1: STATES
    with tabs[1]:
        st.subheader("States (Delta)")
        left, right = st.columns([0.35, 0.65], gap="large")

        with left:
            table = st.selectbox("Choose state table", ["shelf_state", "wh_state", "shelf_batch_state", "wh_batch_state"], index=0)
            categories = ["(all)"] + sorted(inv["item_category"].dropna().unique().tolist())
            sel_cat = st.selectbox("Category", categories, index=0)
            subcats = ["(all)"]
            if sel_cat != "(all)":
                subcats += sorted(inv.loc[inv["item_category"] == sel_cat, "item_subcategory"].dropna().unique().tolist())
            else:
                subcats += sorted(inv["item_subcategory"].dropna().unique().tolist())
            sel_sub = st.selectbox("Subcategory", subcats, index=0)

            shelves = ["(all)"]
            filt = inv.copy()
            if sel_cat != "(all)":
                filt = filt[filt["item_category"] == sel_cat]
            if sel_sub != "(all)":
                filt = filt[filt["item_subcategory"] == sel_sub]
            shelves += sorted(filt["shelf_id"].dropna().unique().tolist())
            sel_shelf = st.selectbox("Shelf ID", shelves, index=0)
            limit = st.slider("Max rows", 100, 5000, 1500, step=100)

        with right:
            try:
                df_state = fetch_state_delta(table, limit=limit)
            except Exception as e:
                st.error(f"Error fetching {table}: {e}")
                df_state = pd.DataFrame()

            if df_state.empty:
                st.info("No data.")
            else:
                show = df_state.copy()
                if "shelf_id" in show.columns and sel_shelf != "(all)":
                    show["shelf_id"] = show["shelf_id"].astype(str).str.strip()
                    show = show[show["shelf_id"] == sel_shelf]

                if sel_cat != "(all)":
                    for c in ["item_category", "category"]:
                        if c in show.columns:
                            show[c] = show[c].map(normalize_cat)
                            show = show[show[c] == sel_cat]
                            break

                if sel_sub != "(all)":
                    for c in ["item_subcategory", "subcategory"]:
                        if c in show.columns:
                            show[c] = show[c].astype(str).str.strip()
                            show = show[show[c] == sel_sub]
                            break

                st.dataframe(show, use_container_width=True)

    # TAB 2: SUPPLIER PLAN
    with tabs[2]:
        st.subheader("WH Supplier Plan")

        orders_df = fetch_supplier_orders(DELTA_SUPPLIER_ORDERS_PATH)
        next_order = next_supplier_order_date(orders_df)
        if next_order is not None:
            next_value, source = next_order
            st.info(f"Next supplier order ({source}): {next_value}")
        else:
            st.info("No supplier order planned (or delta not available).")
            last_order = last_supplier_order_date(orders_df)
            if last_order is not None:
                last_value, last_source = last_order
                st.caption(f"Last supplier order ({last_source}): {last_value}")

        st.markdown("**Warehouse restock status**")
        events_df = fetch_wh_events_raw(DELTA_WH_EVENTS_RAW_PATH)
        if events_df.empty or "event_type" not in events_df.columns:
            st.info("No restock events found (wh_events delta not available).")
        else:
            restocks = events_df[events_df["event_type"] == "wh_in"].copy()
            if restocks.empty:
                st.info("No restock events found.")
            else:
                if "timestamp" in restocks.columns and restocks["timestamp"].notna().any():
                    restocks = restocks.sort_values("timestamp")
                    last_restock_ts = restocks["timestamp"].dropna().max()
                    last_restock_day = last_restock_ts.date()
                    day_df = restocks[restocks["timestamp"].dt.date == last_restock_day].copy()
                elif "received_date" in restocks.columns and restocks["received_date"].notna().any():
                    last_restock_day = restocks["received_date"].dropna().max()
                    last_restock_ts = None
                    day_df = restocks[restocks["received_date"] == last_restock_day].copy()
                else:
                    last_restock_day = None
                    last_restock_ts = None
                    day_df = restocks.copy()

                total_qty = int(day_df["qty"].sum()) if "qty" in day_df.columns and not day_df.empty else 0
                shelves_count = int(day_df["shelf_id"].nunique()) if "shelf_id" in day_df.columns and not day_df.empty else 0

                k1, k2, k3 = st.columns(3)
                k1.metric("Last restock day", str(last_restock_day) if last_restock_day else "n/a")
                k2.metric("Last restock time", str(last_restock_ts) if last_restock_ts else "n/a")
                k3.metric("Items restocked (day)", total_qty)
                st.caption(f"Shelves restocked (day): {shelves_count}")

                if not day_df.empty:
                    if "shelf_id" in day_df.columns:
                        day_df["item_category"] = day_df["shelf_id"].map(shelf_to_cat)
                        day_df["item_subcategory"] = day_df["shelf_id"].map(shelf_to_sub)
                    show_cols = [
                        c for c in [
                            "shelf_id", "item_category", "item_subcategory",
                            "qty", "unit", "received_date", "timestamp", "batch_code", "reason"
                        ] if c in day_df.columns
                    ]
                    st.markdown("**Restocked products (last restock day):**")
                    if show_cols:
                        st.dataframe(day_df[show_cols].sort_values(show_cols[0]).head(3000), use_container_width=True)
                    else:
                        st.info("Restock event columns not available.")

        left, right = st.columns([0.35, 0.65], gap="large")
        with left:
            limit = st.slider("Max rows", 100, 5000, 1500, step=100, key="supplier_plan_limit")
            status_filter = st.selectbox("Status", ["(all)", "pending", "issued", "completed", "canceled"], index=0, key="supplier_plan_status")

        df_plan = fetch_supplier_plan_delta(DELTA_SUPPLIER_PLAN_PATH)
        data_source = "delta"
        if df_plan.empty:
            try:
                df_plan = fetch_supplier_plan_pg(limit=limit)
                if not df_plan.empty:
                    data_source = "postgres"
            except Exception as e:
                st.error(f"Error fetching {SUPPLIER_PLAN_TABLE_NAME}: {e}")
                df_plan = pd.DataFrame()

        if status_filter != "(all)" and "status" in df_plan.columns:
            df_plan = df_plan[df_plan["status"] == status_filter]
        if "plan_date" in df_plan.columns:
            df_plan = df_plan.sort_values(["plan_date"], ascending=False)
        elif "created_at" in df_plan.columns:
            df_plan = df_plan.sort_values(["created_at"], ascending=False)
        df_plan = df_plan.head(limit)

        with right:
            if df_plan.empty:
                st.info("No supplier plan found.")
            else:
                st.caption(f"Data source: {data_source}")
                st.dataframe(df_plan, use_container_width=True)

    # TAB 3: ML MODEL
    with tabs[3]:
        st.subheader("ML Model")

        reg = fetch_ml_model_registry()
        preds = fetch_ml_predictions(limit=12000)

        if reg.empty:
            st.warning("No model registered in analytics.ml_models.")
        else:
            latest = reg.iloc[0]
            c1, c2, c3 = st.columns(3)
            c1.metric("Model name", str(latest["model_name"]))
            c2.metric("Model version", str(latest["model_version"]))
            c3.metric("Trained at", str(latest["trained_at"]))
            st.markdown("**Metrics (latest):**")
            st.json(latest["metrics_json"])
            st.caption(f"Artifact: {latest['artifact_path']}")
            with st.expander("Model registry (last 20)"):
                st.dataframe(reg, use_container_width=True)

        st.divider()

        if preds.empty:
            st.info("No predictions found in analytics.ml_predictions_log.")
        else:
            latest_feature_date = max(preds["feature_date"])
            st.caption(f"Latest feature_date in predictions: {latest_feature_date}")

            model_names = ["(all)"] + sorted(preds["model_name"].dropna().unique().tolist())
            sel_model = st.selectbox("Model", model_names, index=0)

            all_shelves = ["(all)"] + sorted(preds["shelf_id"].dropna().unique().tolist())
            sel_shelf = st.selectbox("Shelf ID", all_shelves, index=0)

            dmin = min(preds["feature_date"])
            dmax = max(preds["feature_date"])
            date_range = st.date_input("Date range", value=(dmin, dmax))

            show = preds.copy()
            if sel_model != "(all)":
                show = show[show["model_name"] == sel_model]
            if isinstance(date_range, tuple) and len(date_range) == 2:
                a, b = date_range
                show = show[(show["feature_date"] >= a) & (show["feature_date"] <= b)]
            if sel_shelf != "(all)":
                show = show[show["shelf_id"] == sel_shelf]

            k1, k2, k3 = st.columns(3)
            k1.metric("Predictions rows", int(len(show)))
            k2.metric("Distinct shelves", int(show["shelf_id"].nunique()))
            k3.metric("Total suggested qty", int(show["suggested_qty"].sum()))

            if sel_shelf != "(all)" and not show.empty:
                g = show.sort_values("feature_date")
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=g["feature_date"], y=g["predicted_batches"], mode="lines+markers", name="predicted_batches"))
                fig.add_trace(go.Scatter(x=g["feature_date"], y=g["suggested_qty"], mode="lines+markers", name="suggested_qty"))
                fig.update_layout(height=320, margin=dict(l=10, r=10, t=10, b=10), xaxis_title="feature_date")
                st.plotly_chart(fig, use_container_width=True)

            st.markdown("**Predictions (filtered):**")
            st.dataframe(show.head(3000), use_container_width=True)

    # TAB 4: ALERTS RAW
    with tabs[4]:
        st.subheader("Alerts (Postgres table)")
        df = fetch_alerts()
        if df.empty:
            st.info("No alerts found.")
        else:
            st.dataframe(df, use_container_width=True)

    # TAB 5: DEBUG
    with tabs[5]:
        st.subheader("Debug / Config")
        st.write("PROJECT_ROOT:", str(PROJECT_ROOT))
        st.write("LAYOUT_YAML_PATH:", str(LAYOUT_YAML_PATH))
        st.write("INVENTORY_CSV_PATH:", str(INVENTORY_CSV_PATH))
        st.write("DELTA_SUPPLIER_PLAN_PATH:", str(DELTA_SUPPLIER_PLAN_PATH))
        st.write("DELTA_SUPPLIER_ORDERS_PATH:", str(DELTA_SUPPLIER_ORDERS_PATH))
        st.write("ALERTS_TABLE_NAME:", ALERTS_TABLE_NAME)
        st.write("SUPPLIER_PLAN_TABLE_NAME:", SUPPLIER_PLAN_TABLE_NAME)
        st.write("AUTOREFRESH_MS:", AUTOREFRESH_MS)
        st.write("SIM_TIME_OK:", SIM_TIME_OK)
        if SIM_TIME_OK:
            st.write("SIM_NOW_UTC:", now_utc_ts().isoformat())

        st.markdown("**Categorie presenti nel CSV (item_category):**")
        st.write(sorted(inv["item_category"].dropna().unique().tolist()))

        st.markdown("**Zone nel layout.yaml:**")
        st.write([{"id": z.id, "category": z.category, "bounds": (z.x0, z.y0, z.x1, z.y1)} for z in zones])


if __name__ == "__main__":
    main()
