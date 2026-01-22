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

# Optional Arrow helpers (used to make Delta reads cheaper)
try:
    import pyarrow as pa
    import pyarrow.dataset as ds
except Exception:
    pa = None
    ds = None

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

def _env_int(*keys: str, default: int) -> int:
    for k in keys:
        v = os.getenv(k)
        if v is None:
            continue
        try:
            return int(str(v).strip())
        except Exception:
            continue
    return int(default)

def _env_bool(*keys: str, default: bool) -> bool:
    truthy = {"1", "true", "t", "yes", "y", "on"}
    falsy = {"0", "false", "f", "no", "n", "off"}
    for k in keys:
        v = os.getenv(k)
        if v is None:
            continue
        s = str(v).strip().lower()
        if s in truthy:
            return True
        if s in falsy:
            return False
    return bool(default)

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

AUTOREFRESH_MS = _env_int("DASH_AUTOREFRESH_MS", "DASH_REFRESH_MS", default=0)
AUTOREFRESH_ENABLED_DEFAULT = _env_bool("DASH_AUTOREFRESH_ENABLED", default=(AUTOREFRESH_MS > 0))

# Defaults to reduce load: override via env vars if you want more “live” behavior.
DEFAULT_ALERTS_LIMIT = _env_int("DASH_ALERTS_LIMIT", default=1000)
DEFAULT_ML_PREDICTIONS_LIMIT = _env_int("DASH_ML_PREDICTIONS_LIMIT", default=3000)

TTL_ALERTS_S = _env_int("DASH_TTL_ALERTS_S", default=30)
TTL_SUPPLIER_PLAN_S = _env_int("DASH_TTL_SUPPLIER_PLAN_S", default=30)
TTL_FOOT_TRAFFIC_S = _env_int("DASH_TTL_FOOT_TRAFFIC_S", default=30)
TTL_DELTA_STATE_S = _env_int("DASH_TTL_DELTA_STATE_S", default=20)
TTL_DELTA_OPS_S = _env_int("DASH_TTL_DELTA_OPS_S", default=30)
TTL_ML_S = _env_int("DASH_TTL_ML_S", default=60)

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

@st.cache_data(ttl=TTL_ALERTS_S, show_spinner=False)
def fetch_alerts(limit: int = DEFAULT_ALERTS_LIMIT) -> pd.DataFrame:
    eng = get_pg_engine()
    q = text(f"""
        SELECT
            alert_id,
            event_type,
            shelf_id,
            location,
            current_stock,
            max_stock,
            target_pct,
            suggested_qty,
            status,
            created_at,
            updated_at
        FROM {ALERTS_TABLE_NAME}
        ORDER BY created_at DESC
        LIMIT :limit
    """)
    with eng.connect() as c:
        df = pd.read_sql(q, c, params={"limit": int(limit)})
    if "alert_id" in df.columns:
        df["alert_id"] = df["alert_id"].astype(str)
    if "shelf_id" in df.columns:
        df["shelf_id"] = df["shelf_id"].astype(str).str.strip()
    if "status" in df.columns:
        df["status"] = df["status"].astype(str).str.strip().str.lower()
    return df

@st.cache_data(ttl=TTL_SUPPLIER_PLAN_S, show_spinner=False)
def fetch_supplier_plan_pg(limit: int = 2000) -> pd.DataFrame:
    eng = get_pg_engine()
    q = text(f"""
        SELECT
            supplier_plan_id,
            shelf_id,
            suggested_qty,
            standard_batch_size,
            status,
            created_at,
            updated_at
        FROM {SUPPLIER_PLAN_TABLE_NAME}
        ORDER BY updated_at DESC
        LIMIT :limit
    """)
    with eng.connect() as c:
        df = pd.read_sql(q, c, params={"limit": int(limit)})
    if "shelf_id" in df.columns:
        df["shelf_id"] = df["shelf_id"].astype(str).str.strip()
    if "status" in df.columns:
        df["status"] = df["status"].astype(str).str.strip().str.lower()
    for col in ["created_at", "updated_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
    return df

@st.cache_data(ttl=60, show_spinner=False)
def fetch_standard_batch_sizes_pg() -> pd.DataFrame:
    eng = get_pg_engine()
    candidates = [
        ("ref.store_batches_snapshot", "standard_batch_size"),
        ("analytics.shelf_daily_features", "standard_batch_size"),
        ("ref.batches_snapshot", "standard_batch_size"),
        ("config.batch_catalog", "standard_batch_size"),
    ]
    last_err: Optional[Exception] = None
    for table, col in candidates:
        try:
            q = text(f"""
                SELECT shelf_id, MAX({col})::int AS standard_batch_size
                FROM {table}
                WHERE shelf_id IS NOT NULL
                GROUP BY shelf_id
            """)
            with eng.connect() as c:
                df = pd.read_sql(q, c)
            if not df.empty:
                df["shelf_id"] = df["shelf_id"].astype(str).str.strip()
                df["standard_batch_size"] = pd.to_numeric(df["standard_batch_size"], errors="coerce").astype("Int64")
                return df
        except Exception as e:
            last_err = e
            continue
    if last_err is not None:
        log.warning("Cannot fetch standard_batch_size mapping from Postgres: %s", last_err)
    return pd.DataFrame(columns=["shelf_id", "standard_batch_size"])

@st.cache_data(ttl=TTL_FOOT_TRAFFIC_S, show_spinner=False)
def fetch_store_foot_traffic(target_date: date) -> Tuple[Optional[date], Optional[int]]:
    eng = get_pg_engine()
    q = text("""
        SELECT feature_date, COALESCE(MAX(people_count), 0) AS people_count
        FROM analytics.shelf_daily_features
        WHERE feature_date = :day
        GROUP BY feature_date
    """)
    with eng.connect() as c:
        df = pd.read_sql(q, c, params={"day": target_date})
    if not df.empty:
        return pd.to_datetime(df.loc[0, "feature_date"], errors="coerce").date(), int(df.loc[0, "people_count"])
    return None, None

def compute_live_foot_traffic(now_ts: pd.Timestamp) -> Optional[int]:
    # Optimized path: scan only needed columns and (when possible) push down a 1-day cutoff.
    try:
        if DeltaTable is None or ds is None or pa is None:
            raise RuntimeError("Delta/Arrow not available")
        delta_path = DELTA_FOOT_TRAFFIC_PATH
        if not delta_path.exists() or not _is_delta_table(delta_path):
            return None

        dt = DeltaTable(str(delta_path))
        dataset = dt.to_pyarrow_dataset()
        names = set(dataset.schema.names)
        if "entry_time" not in names:
            return None

        cutoff = now_ts - pd.Timedelta(days=1)
        filter_expr = None
        try:
            entry_field = dataset.schema.field("entry_time")
            if pa.types.is_timestamp(entry_field.type) or pa.types.is_date(entry_field.type):
                cutoff_dt = cutoff.to_pydatetime()
                now_dt = now_ts.to_pydatetime()
                filter_expr = (ds.field("entry_time") >= cutoff_dt) & (ds.field("entry_time") <= now_dt)
        except Exception:
            filter_expr = None

        cols = [c for c in ["entry_time", "exit_time"] if c in names]
        scanner = dataset.scanner(columns=cols, filter=filter_expr, batch_size=65536)

        active = 0
        for batch in scanner.to_batches():
            pdf = batch.to_pandas()
            if pdf.empty or "entry_time" not in pdf.columns:
                continue
            pdf = pdf[pdf["entry_time"].notna()].copy()
            if pdf.empty:
                continue

            pdf["entry_time"] = pd.to_datetime(pdf["entry_time"], errors="coerce", utc=True)
            pdf = pdf[pdf["entry_time"].notna()]
            if pdf.empty:
                continue

            pdf = pdf[pdf["entry_time"] >= cutoff]
            if pdf.empty:
                continue

            if "exit_time" in pdf.columns:
                pdf["exit_time"] = pd.to_datetime(pdf["exit_time"], errors="coerce", utc=True)
                exit_ts = pdf["exit_time"]
                active += int(((pdf["entry_time"] <= now_ts) & (exit_ts.isna() | (exit_ts > now_ts))).sum())
            else:
                active += int((pdf["entry_time"] <= now_ts).sum())
        return int(active)
    except Exception:
        # Fallback path (older behavior).
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
@st.cache_data(ttl=TTL_ML_S, show_spinner=False)
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

@st.cache_data(ttl=TTL_ML_S, show_spinner=False)
def fetch_ml_predictions(limit: int = DEFAULT_ML_PREDICTIONS_LIMIT) -> pd.DataFrame:
    eng = get_pg_engine()
    q = text("""
        SELECT model_name, feature_date, shelf_id, predicted_batches, suggested_qty, model_version, created_at
        FROM analytics.ml_predictions_log
        ORDER BY feature_date DESC, created_at DESC
        LIMIT :limit
    """)
    with eng.connect() as c:
        df = pd.read_sql(q, c, params={"limit": int(limit)})
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

def _delta_to_pandas(
    delta_path: Path,
    *,
    columns: Optional[List[str]] = None,
    filter_expr=None,
    max_rows: Optional[int] = None,
) -> pd.DataFrame:
    if DeltaTable is None or ds is None or pa is None:
        raise RuntimeError("Delta/Arrow not available")
    if not delta_path.exists() or not _is_delta_table(delta_path):
        return pd.DataFrame()

    dt = DeltaTable(str(delta_path))
    dataset = dt.to_pyarrow_dataset()
    use_columns = None
    if columns:
        available = set(dataset.schema.names)
        use_columns = [c for c in columns if c in available]
        if not use_columns:
            use_columns = None

    scanner = dataset.scanner(columns=use_columns, filter=filter_expr, batch_size=65536)
    batches = []
    rows = 0
    for batch in scanner.to_batches():
        batches.append(batch)
        rows += int(batch.num_rows)
        if max_rows is not None and rows >= int(max_rows):
            break
    if not batches:
        return pd.DataFrame()
    table = pa.Table.from_batches(batches)
    if max_rows is not None:
        table = table.slice(0, int(max_rows))
    return table.to_pandas()

@st.cache_data(ttl=TTL_DELTA_OPS_S, show_spinner=False)
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

@st.cache_data(ttl=TTL_DELTA_OPS_S, show_spinner=False)
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

@st.cache_data(ttl=TTL_DELTA_OPS_S, show_spinner=False)
def fetch_wh_events_raw(delta_path: Path, event_type: Optional[str] = None) -> pd.DataFrame:
    if DeltaTable is None or not delta_path.exists() or not _is_delta_table(delta_path):
        return pd.DataFrame()
    try:
        if ds is not None and pa is not None:
            filter_expr = None
            try:
                if event_type is not None:
                    filter_expr = ds.field("event_type") == str(event_type).strip().lower()
            except Exception:
                filter_expr = None

            df = _delta_to_pandas(
                delta_path,
                columns=[
                    "event_id",
                    "event_type",
                    "shelf_id",
                    "batch_code",
                    "qty",
                    "unit",
                    "timestamp",
                    "received_date",
                    "reason",
                ],
                filter_expr=filter_expr,
            )
        else:
            dt = DeltaTable(str(delta_path))
            df = dt.to_pandas()
    except Exception as e:
        log.warning("Delta table not available at %s: %s", delta_path, e)
        return pd.DataFrame()
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

    # De-duplicate events for display: multiple writers/replays can create duplicates.
    if "event_id" in df.columns:
        ids = df["event_id"].astype(str)
        mask = ids.notna() & (ids != "") & (ids != "None") & (ids != "nan")
        if mask.any():
            df = df.loc[mask].drop_duplicates(subset=["event_id"], keep="last").copy()
        else:
            subset = [c for c in ["event_type", "shelf_id", "batch_code", "qty", "timestamp"] if c in df.columns]
            if subset:
                df = df.drop_duplicates(subset=subset, keep="last").copy()
    else:
        subset = [c for c in ["event_type", "shelf_id", "batch_code", "qty", "timestamp"] if c in df.columns]
        if subset:
            df = df.drop_duplicates(subset=subset, keep="last").copy()
    return df

@st.cache_data(ttl=TTL_FOOT_TRAFFIC_S, show_spinner=False)
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

@st.cache_data(ttl=TTL_DELTA_STATE_S, show_spinner=False)
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

    with st.sidebar:
        st.header("Performance")

        if st_autorefresh is None:
            st.caption("Auto-refresh disabled (streamlit-autorefresh not installed).")
            enable_autorefresh = False
        else:
            enable_autorefresh = st.checkbox("Auto-refresh", value=AUTOREFRESH_ENABLED_DEFAULT)

        interval_s_default = max(1, int(AUTOREFRESH_MS / 1000)) if AUTOREFRESH_MS > 0 else 10
        interval_s = st.number_input(
            "Refresh interval (seconds)",
            min_value=1,
            max_value=300,
            value=int(interval_s_default),
            step=1,
            disabled=not enable_autorefresh,
        )

        alerts_limit = st.number_input(
            "Alerts max rows",
            min_value=100,
            max_value=5000,
            value=int(DEFAULT_ALERTS_LIMIT),
            step=100,
        )

        ml_preds_limit = st.number_input(
            "ML predictions max rows",
            min_value=200,
            max_value=20000,
            value=int(DEFAULT_ML_PREDICTIONS_LIMIT),
            step=200,
        )

        compute_live_ft = st.checkbox(
            "Compute live foot traffic (Delta)",
            value=_env_bool("DASH_LIVE_FOOT_TRAFFIC", "DASH_COMPUTE_LIVE_FOOT_TRAFFIC", default=True),
        )

        if st.button("Refresh now"):
            st.cache_data.clear()
            st.cache_resource.clear()
            st.rerun()

    if enable_autorefresh and st_autorefresh is not None:
        st_autorefresh(interval=int(interval_s) * 1000, key="autorefresh")

    map_img_path, zones, _ = load_layout(LAYOUT_YAML_PATH)
    inv = load_inventory(INVENTORY_CSV_PATH)
    shelf_to_cat, shelf_to_sub = build_shelf_maps(inv)

    st.title("Supermarket Live Dashboard")
    render_simulated_time()

    pages = ["Map Live", "States", "Supplier Plan", "ML Model", "Alerts (raw)", "Debug"]
    page = st.radio("View", pages, horizontal=True)

    # PAGE: MAP LIVE
    if page == "Map Live":
        col_left, col_right = st.columns([1.15, 0.85], gap="large")

        alerts_df = fetch_alerts(limit=int(alerts_limit))
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
            st.markdown("**Current foot traffic**")
            try:
                sim_day = now_utc_ts().date()
                live_count = compute_live_foot_traffic(now_utc_ts()) if compute_live_ft else None
                if live_count is None:
                    ft_date, ft_count = fetch_store_foot_traffic(sim_day)
                    if ft_date is None:
                        st.info("Foot traffic data not available.")
                    else:
                        st.metric("People count (latest)", int(ft_count))
                        if ft_date != sim_day:
                            st.caption(f"Latest available date: {ft_date}")
                else:
                    st.metric("People count (live)", int(live_count))
            except Exception as e:
                st.warning(f"Foot traffic error: {e}")

            st.metric("Active alerts (store)", int(len(store_alerts)))
            st.metric("Active alerts (warehouse)", int(len(wh_alerts)))

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
                    cols = ["alert_id", "event_type", "shelf_id", "current_stock", "max_stock", "target_pct", "suggested_qty", "status", "created_at"]
                    cols = [c for c in cols if c in sub.columns]
                    st.markdown("**Alert details (latest):**")
                    st.dataframe(sub[cols].head(200), use_container_width=True)
            else:
                st.caption("Click a map section to see alerts in that area.")

    # PAGE: STATES
    elif page == "States":
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
            sel_shelf = st.selectbox("Shelf ID", shelves, index=0, key="shelf_id_selection_plan")
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

    # PAGE: SUPPLIER PLAN
    elif page == "Supplier Plan":
        st.subheader("WH Supplier Plan")

        orders_df = fetch_supplier_orders(DELTA_SUPPLIER_ORDERS_PATH)
        next_order = next_supplier_order_date(orders_df)
        if next_order is not None:
            next_value, source = next_order
            st.success(f"WH supplier order planned ({source}): {next_value}")
        else:
            st.info("No supplier order planned (or delta not available).")
            last_order = last_supplier_order_date(orders_df)
            if last_order is not None:
                last_value, last_source = last_order
                st.caption(f"Last supplier order ({last_source}): {last_value}")

        st.markdown("**Warehouse restock status**")
        events_df = fetch_wh_events_raw(DELTA_WH_EVENTS_RAW_PATH, event_type="wh_in")
        if events_df.empty or "event_type" not in events_df.columns:
            st.info("No restock events found (wh_events delta not available).")
        else:
            restocks = events_df.copy()
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
                # Backfill standard_batch_size for display if upstream services didn't populate it.
                if "shelf_id" in df_plan.columns:
                    if "standard_batch_size" not in df_plan.columns:
                        df_plan["standard_batch_size"] = pd.NA
                    s = df_plan["standard_batch_size"]
                    s_num = pd.to_numeric(s, errors="coerce")
                    # Treat 0/negative values as missing (older writers used 0 for nulls).
                    s_num = s_num.where(s_num.isna() | (s_num > 0), pd.NA)
                    if s_num.isna().mean() >= 0.5:
                        try:
                            sizes = fetch_standard_batch_sizes_pg()
                            if not sizes.empty:
                                m = dict(zip(sizes["shelf_id"], sizes["standard_batch_size"]))
                                df_plan["standard_batch_size"] = (
                                    s_num.fillna(df_plan["shelf_id"].astype(str).str.strip().map(m))
                                    .astype("Int64")
                                )
                        except Exception as e:
                            log.warning("standard_batch_size backfill failed: %s", e)
                st.caption(f"Data source: {data_source}")
                st.dataframe(df_plan, use_container_width=True)

    # PAGE: ML MODEL
    elif page == "ML Model":
        st.subheader("ML Model")

        reg = fetch_ml_model_registry()
        preds = fetch_ml_predictions(limit=int(ml_preds_limit))

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
            sel_shelf = st.selectbox("Shelf ID", all_shelves, index=0, key="shelf_id_selection_ml")

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

    # PAGE: ALERTS RAW
    elif page == "Alerts (raw)":
        st.subheader("Alerts (Postgres table)")
        df = fetch_alerts(limit=int(alerts_limit))
        if df.empty:
            st.info("No alerts found.")
        else:
            st.dataframe(df, use_container_width=True)

    # PAGE: DEBUG
    elif page == "Debug":
        st.subheader("Debug / Config")
        st.write("PROJECT_ROOT:", str(PROJECT_ROOT))
        st.write("LAYOUT_YAML_PATH:", str(LAYOUT_YAML_PATH))
        st.write("INVENTORY_CSV_PATH:", str(INVENTORY_CSV_PATH))
        st.write("DELTA_SUPPLIER_PLAN_PATH:", str(DELTA_SUPPLIER_PLAN_PATH))
        st.write("DELTA_SUPPLIER_ORDERS_PATH:", str(DELTA_SUPPLIER_ORDERS_PATH))
        st.write("ALERTS_TABLE_NAME:", ALERTS_TABLE_NAME)
        st.write("SUPPLIER_PLAN_TABLE_NAME:", SUPPLIER_PLAN_TABLE_NAME)
        st.write("AUTOREFRESH_MS (env default):", AUTOREFRESH_MS)
        st.write("TTL_ALERTS_S:", TTL_ALERTS_S)
        st.write("TTL_DELTA_STATE_S:", TTL_DELTA_STATE_S)
        st.write("TTL_DELTA_OPS_S:", TTL_DELTA_OPS_S)
        st.write("TTL_ML_S:", TTL_ML_S)
        st.write("SIM_TIME_OK:", SIM_TIME_OK)
        if SIM_TIME_OK:
            st.write("SIM_NOW_UTC:", now_utc_ts().isoformat())

        st.markdown("**Categorie presenti nel CSV (item_category):**")
        st.write(sorted(inv["item_category"].dropna().unique().tolist()))

        st.markdown("**Zone nel layout.yaml:**")
        st.write([{"id": z.id, "category": z.category, "bounds": (z.x0, z.y0, z.x1, z.y1)} for z in zones])


if __name__ == "__main__":
    main()
