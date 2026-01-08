from __future__ import annotations

import os
import time
import logging
from dataclasses import dataclass
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
try:
    from simulated_time.redis_helpers import get_simulated_now
    from datetime import timezone
    SIM_TIME_OK = True
except Exception:
    SIM_TIME_OK = False
    timezone = None


log = logging.getLogger("dashboard")

# -----------------------------
# PATHS & ENV
# -----------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_LAYOUT_YAML = PROJECT_ROOT / "dashboard" / "store_layout.yaml"
DEFAULT_INVENTORY_CSV = PROJECT_ROOT / "data" / "db_csv" / "store_inventory_final.csv"
DEFAULT_DELTA_SHELF_EVENTS = PROJECT_ROOT / "delta" / "raw" / "shelf_events"
DEFAULT_DELTA_SUPPLIER_PLAN = PROJECT_ROOT / "delta" / "ops" / "wh_supplier_plan"
DEFAULT_DELTA_SUPPLIER_ORDERS = PROJECT_ROOT / "delta" / "ops" / "wh_supplier_orders"

def _env_path(*keys: str, default: Path) -> Path:
    for k in keys:
        v = os.getenv(k)
        if v:
            return Path(v)
    return default

# supporta sia i nomi “nuovi” che quelli che avevi nel compose precedente
LAYOUT_YAML_PATH = _env_path("DASH_LAYOUT_YAML", "LAYOUT_YAML", "LAYOUT_YAML_PATH", default=DEFAULT_LAYOUT_YAML)
INVENTORY_CSV_PATH = _env_path("INVENTORY_CSV_PATH", "INVENTORY_CSV", default=DEFAULT_INVENTORY_CSV)

DELTA_SHELF_EVENTS_PATH = _env_path("DELTA_SHELF_EVENTS_PATH", "DELTA_EVENTS_PATH", default=DEFAULT_DELTA_SHELF_EVENTS)
DELTA_SUPPLIER_PLAN_PATH = _env_path("DELTA_SUPPLIER_PLAN_PATH", default=DEFAULT_DELTA_SUPPLIER_PLAN)
DELTA_SUPPLIER_ORDERS_PATH = _env_path("DELTA_SUPPLIER_ORDERS_PATH", default=DEFAULT_DELTA_SUPPLIER_ORDERS)

ALERTS_TABLE_NAME = os.getenv("ALERTS_TABLE", "ops.alerts").strip()
SUPPLIER_PLAN_TABLE_NAME = os.getenv("SUPPLIER_PLAN_TABLE", "ops.wh_supplier_plan").strip()
STATE_SCHEMA = os.getenv("STATE_SCHEMA", "state").strip()

AUTOREFRESH_MS = int(os.getenv("DASH_AUTOREFRESH_MS", os.getenv("DASH_REFRESH_MS", "1500")))
WEIGHT_EVENT_LOOKBACK_SEC = int(os.getenv("WEIGHT_EVENT_LOOKBACK_SEC", "8"))
WEIGHT_EVENT_TYPE = os.getenv("WEIGHT_EVENT_TYPE", "weight_change").strip()

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
        raise RuntimeError("pyyaml non installato. Aggiungi 'pyyaml' ai requirements.")
    layout_path = Path(layout_path_str)
    if not layout_path.exists():
        raise FileNotFoundError(f"Layout YAML non trovato: {layout_path}")

    data = yaml.safe_load(layout_path.read_text())
    coord_type = str(data.get("coord_type", "normalized")).strip().lower()
    if coord_type != "normalized":
        raise ValueError("Supporto solo coord_type=normalized (0..1).")

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
        raise FileNotFoundError(f"Inventory CSV non trovato: {csv_path}")
    df = pd.read_csv(csv_path)
    for col in ["shelf_id", "item_category", "item_subcategory"]:
        if col not in df.columns:
            raise ValueError(f"Nel CSV manca '{col}'. Colonne: {list(df.columns)}")

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
    host = os.getenv("POSTGRES_HOST", "postgres")  # questo è il nome servizio docker
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
    table_ref = raw_name if "." in raw_name else (f"{STATE_SCHEMA}.{base_name}" if STATE_SCHEMA else base_name)
    eng = get_pg_engine()
    q = text(f"SELECT * FROM {table_ref} ORDER BY 1 DESC LIMIT {int(limit)}")
    with eng.connect() as c:
        return pd.read_sql(q, c)

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

@st.cache_data(ttl=1, show_spinner=False)
def fetch_recent_weight_events(delta_path: Path, lookback_sec: int) -> pd.DataFrame:
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

    et_col = "event_type" if "event_type" in df.columns else ("type" if "type" in df.columns else None)
    if et_col is None:
        return pd.DataFrame()

    df[et_col] = df[et_col].astype(str).str.strip().str.lower()
    df = df[df[et_col] == WEIGHT_EVENT_TYPE.lower()].copy()
    if df.empty:
        return df

    ts_col = _pick_timestamp_column(df)
    if ts_col is None:
        return pd.DataFrame()

    df["_ts"] = _to_datetime(df[ts_col])
    now = now_utc_ts()
    cutoff = now - pd.Timedelta(seconds=int(lookback_sec))
    df = df[df["_ts"] >= cutoff].copy()
    return df

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

def next_supplier_order_date(orders_df: pd.DataFrame) -> Optional[Tuple[Any, str]]:
    if orders_df.empty:
        return None
    df = orders_df.copy()
    if "status" in df.columns:
        df["status"] = df["status"].astype(str).str.strip().str.lower()
        df = df[~df["status"].isin(["canceled", "cancelled"])]
    if df.empty:
        return None

    if "cutoff_ts" in df.columns:
        df["cutoff_ts"] = pd.to_datetime(df["cutoff_ts"], errors="coerce", utc=True)
        df = df[df["cutoff_ts"].notna()]
        if df.empty:
            return None
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


# -----------------------------
# MAP FIGURE
# -----------------------------
def build_map_figure(map_img: Image.Image, zones: List[Zone], red_categories: set[str], green_categories: set[str], blink_on: bool) -> go.Figure:
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
        is_green = (cat in green_categories) and blink_on

        fillcolor = "rgba(255,0,0,0.25)" if is_red else ("rgba(0,255,0,0.18)" if is_green else "rgba(0,0,0,0.0)")
        line_color = "rgba(255,0,0,0.9)" if is_red else ("rgba(0,255,0,0.95)" if is_green else "rgba(0,0,0,0.0)")
        line_width = 3 if (is_red or is_green) else 1

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

    st.title("📍 Supermarket Live Dashboard")

    if SIM_TIME_OK:
        st.caption(f"Simulated time (UTC): {now_utc_ts().isoformat()}")

    tabs = st.tabs(["🗺️ Map Live", "📦 States", "🚚 Supplier Plan", "🤖 ML Model", "🚨 Alerts (raw)", "⚙️ Debug"])

    blink_on = (int(time.time() * 2) % 2 == 0)

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

        weight_df = fetch_recent_weight_events(DELTA_SHELF_EVENTS_PATH, WEIGHT_EVENT_LOOKBACK_SEC)
        green_cats = set()
        if not weight_df.empty and "shelf_id" in weight_df.columns:
            tmpw = weight_df.copy()
            tmpw["item_category"] = tmpw["shelf_id"].map(shelf_to_cat).map(normalize_cat)
            green_cats = set(tmpw["item_category"].dropna().unique().tolist())

        with col_left:
            st.subheader("Mappa live (rosso=alert store, verde=weight_change recente)")
            if not map_img_path.exists():
                st.error(f"Immagine mappa non trovata: {map_img_path}")
                st.stop()

            map_img = Image.open(map_img_path).convert("RGB")
            fig = build_map_figure(map_img, zones, red_cats, green_cats, blink_on=blink_on)

            events = plotly_events(
                fig,
                click_event=True,
                hover_event=False,
                select_event=False,
                override_height=820,
                key="map_click",
            )

        with col_right:
            st.subheader("Dettagli store")
            st.metric("Alerts attivi (store)", int(len(store_alerts)))
            st.metric("Alerts attivi (warehouse)", int(len(wh_alerts)))
            st.metric("Weight events recenti", int(len(weight_df)))

            st.markdown("**Riepilogo warehouse (grafico)**")
            if wh_alerts.empty:
                st.info("Nessun alert warehouse attivo.")
            else:
                chart_col = "severity" if "severity" in wh_alerts.columns else ("event_type" if "event_type" in wh_alerts.columns else None)
                if chart_col is None:
                    st.info("Mancano colonne severity/event_type.")
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
                st.markdown(f"### Sezione selezionata: **{clicked_cat}**")
                sub = store_alerts.copy()
                if not sub.empty and "shelf_id" in sub.columns:
                    sub["item_category"] = sub["shelf_id"].map(shelf_to_cat).map(normalize_cat)
                    sub["item_subcategory"] = sub["shelf_id"].map(shelf_to_sub)
                    sub = sub[sub["item_category"] == clicked_cat].copy()

                if sub.empty:
                    st.info("Nessun alert store attivo in questa sezione.")
                else:
                    st.error(f"Alert store attivi nella sezione: {len(sub)}")
                    shelf_list = sorted(sub["shelf_id"].dropna().unique().tolist())
                    st.markdown("**Shelf in alert:**")
                    st.write(shelf_list)
                    cols = ["alert_id", "event_type", "shelf_id", "severity", "current_stock", "max_stock", "target_pct", "suggested_qty", "status", "created_at"]
                    cols = [c for c in cols if c in sub.columns]
                    st.markdown("**Dettagli alerts (ultimi):**")
                    st.dataframe(sub[cols].head(200), use_container_width=True)
            else:
                st.caption("Clicca su una sezione della mappa per vedere gli alert di quella zona.")

            if DeltaTable is None:
                st.warning("DeltaTable (deltalake) non installato: weight_change non mostrati.")

    # TAB 1: STATES
    with tabs[1]:
        st.subheader("States (Postgres)")
        left, right = st.columns([0.35, 0.65], gap="large")

        with left:
            table = st.selectbox("Scegli tabella state", ["shelf_state", "wh_state", "shelf_batch_state", "wh_batch_state"], index=0)
            categories = ["(tutte)"] + sorted(inv["item_category"].dropna().unique().tolist())
            sel_cat = st.selectbox("Categoria", categories, index=0)
            subcats = ["(tutte)"]
            if sel_cat != "(tutte)":
                subcats += sorted(inv.loc[inv["item_category"] == sel_cat, "item_subcategory"].dropna().unique().tolist())
            else:
                subcats += sorted(inv["item_subcategory"].dropna().unique().tolist())
            sel_sub = st.selectbox("Subcategoria", subcats, index=0)

            shelves = ["(tutte)"]
            filt = inv.copy()
            if sel_cat != "(tutte)":
                filt = filt[filt["item_category"] == sel_cat]
            if sel_sub != "(tutte)":
                filt = filt[filt["item_subcategory"] == sel_sub]
            shelves += sorted(filt["shelf_id"].dropna().unique().tolist())
            sel_shelf = st.selectbox("Shelf ID", shelves, index=0)
            limit = st.slider("Righe max", 100, 5000, 1500, step=100)

        with right:
            try:
                df_state = fetch_pg_table(table, limit=limit)
            except Exception as e:
                st.error(f"Errore fetch {table}: {e}")
                df_state = pd.DataFrame()

            if df_state.empty:
                st.info("Nessun dato.")
            else:
                show = df_state.copy()
                if "shelf_id" in show.columns and sel_shelf != "(tutte)":
                    show["shelf_id"] = show["shelf_id"].astype(str).str.strip()
                    show = show[show["shelf_id"] == sel_shelf]

                if sel_cat != "(tutte)":
                    for c in ["item_category", "category"]:
                        if c in show.columns:
                            show[c] = show[c].map(normalize_cat)
                            show = show[show[c] == sel_cat]
                            break

                if sel_sub != "(tutte)":
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
            st.info(f"Prossimo ordine supplier ({source}): {next_value}")
        else:
            st.info("Nessun ordine fornitore pianificato (o delta non disponibile).")

        left, right = st.columns([0.35, 0.65], gap="large")
        with left:
            limit = st.slider("Righe max", 100, 5000, 1500, step=100, key="supplier_plan_limit")
            status_filter = st.selectbox("Status", ["(tutti)", "pending", "issued", "completed", "canceled"], index=0, key="supplier_plan_status")

        df_plan = fetch_supplier_plan_delta(DELTA_SUPPLIER_PLAN_PATH)
        data_source = "delta"
        if df_plan.empty:
            try:
                df_plan = fetch_supplier_plan_pg(limit=limit)
                if not df_plan.empty:
                    data_source = "postgres"
            except Exception as e:
                st.error(f"Errore fetch {SUPPLIER_PLAN_TABLE_NAME}: {e}")
                df_plan = pd.DataFrame()

        if status_filter != "(tutti)" and "status" in df_plan.columns:
            df_plan = df_plan[df_plan["status"] == status_filter]
        if "plan_date" in df_plan.columns:
            df_plan = df_plan.sort_values(["plan_date"], ascending=False)
        elif "created_at" in df_plan.columns:
            df_plan = df_plan.sort_values(["created_at"], ascending=False)
        df_plan = df_plan.head(limit)

        with right:
            if df_plan.empty:
                st.info("Nessun piano supplier trovato.")
            else:
                st.caption(f"Fonte dati: {data_source}")
                st.dataframe(df_plan, use_container_width=True)

    # TAB 3: ML MODEL
    with tabs[3]:
        st.subheader("🤖 ML Model")

        reg = fetch_ml_model_registry()
        preds = fetch_ml_predictions(limit=12000)

        if reg.empty:
            st.warning("Nessun modello registrato in analytics.ml_models.")
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
            st.info("Nessuna predizione trovata in analytics.ml_predictions_log.")
        else:
            model_names = ["(tutti)"] + sorted(preds["model_name"].dropna().unique().tolist())
            sel_model = st.selectbox("Model", model_names, index=0)

            all_shelves = ["(tutte)"] + sorted(preds["shelf_id"].dropna().unique().tolist())
            sel_shelf = st.selectbox("Shelf ID", all_shelves, index=0)

            dmin = min(preds["feature_date"])
            dmax = max(preds["feature_date"])
            date_range = st.date_input("Intervallo date", value=(dmin, dmax))

            show = preds.copy()
            if sel_model != "(tutti)":
                show = show[show["model_name"] == sel_model]
            if isinstance(date_range, tuple) and len(date_range) == 2:
                a, b = date_range
                show = show[(show["feature_date"] >= a) & (show["feature_date"] <= b)]
            if sel_shelf != "(tutte)":
                show = show[show["shelf_id"] == sel_shelf]

            k1, k2, k3 = st.columns(3)
            k1.metric("Predictions rows", int(len(show)))
            k2.metric("Distinct shelves", int(show["shelf_id"].nunique()))
            k3.metric("Total suggested qty", int(show["suggested_qty"].sum()))

            if sel_shelf != "(tutte)" and not show.empty:
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
        st.subheader("Alerts (tabella Postgres)")
        df = fetch_alerts()
        if df.empty:
            st.info("Nessun alert trovato.")
        else:
            st.dataframe(df, use_container_width=True)

    # TAB 5: DEBUG
    with tabs[5]:
        st.subheader("Debug / Config")
        st.write("PROJECT_ROOT:", str(PROJECT_ROOT))
        st.write("LAYOUT_YAML_PATH:", str(LAYOUT_YAML_PATH))
        st.write("INVENTORY_CSV_PATH:", str(INVENTORY_CSV_PATH))
        st.write("DELTA_SHELF_EVENTS_PATH:", str(DELTA_SHELF_EVENTS_PATH))
        st.write("DELTA_SUPPLIER_PLAN_PATH:", str(DELTA_SUPPLIER_PLAN_PATH))
        st.write("DELTA_SUPPLIER_ORDERS_PATH:", str(DELTA_SUPPLIER_ORDERS_PATH))
        st.write("ALERTS_TABLE_NAME:", ALERTS_TABLE_NAME)
        st.write("SUPPLIER_PLAN_TABLE_NAME:", SUPPLIER_PLAN_TABLE_NAME)
        st.write("AUTOREFRESH_MS:", AUTOREFRESH_MS)
        st.write("WEIGHT_EVENT_LOOKBACK_SEC:", WEIGHT_EVENT_LOOKBACK_SEC)
        st.write("WEIGHT_EVENT_TYPE:", WEIGHT_EVENT_TYPE)
        st.write("SIM_TIME_OK:", SIM_TIME_OK)
        if SIM_TIME_OK:
            st.write("SIM_NOW_UTC:", now_utc_ts().isoformat())

        st.markdown("**Categorie presenti nel CSV (item_category):**")
        st.write(sorted(inv["item_category"].dropna().unique().tolist()))

        st.markdown("**Zone nel layout.yaml:**")
        st.write([{"id": z.id, "category": z.category, "bounds": (z.x0, z.y0, z.x1, z.y1)} for z in zones])


if __name__ == "__main__":
    main()
