# dashboard/app.py
from __future__ import annotations

import os
import time
from dataclasses import dataclass
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from PIL import Image
from pathlib import Path
from typing import Tuple, List, Dict, Any

# Optional deps
try:
    import yaml  # pyyaml
except Exception:
    yaml = None

try:
    from sqlalchemy import create_engine, text
except Exception:
    create_engine = None
    text = None

try:
    # delta-rs
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


# -----------------------------
# PATHS & ENV
# -----------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # repo root (dashboard/..)

DEFAULT_LAYOUT_YAML = PROJECT_ROOT / "dashboard" / "store_layout.yaml"
DEFAULT_INVENTORY_CSV = PROJECT_ROOT / "data" / "db_csv" / "store_inventory_final.csv"
DEFAULT_DELTA_SHELF_EVENTS = PROJECT_ROOT / "delta" / "raw" / "shelf_events"

# se preferisci, puoi sovrascrivere da .env / docker-compose
LAYOUT_YAML_PATH = Path(os.getenv("DASH_LAYOUT_YAML", str(DEFAULT_LAYOUT_YAML)))
INVENTORY_CSV_PATH = Path(os.getenv("INVENTORY_CSV_PATH", str(DEFAULT_INVENTORY_CSV)))
DELTA_SHELF_EVENTS_PATH = Path(os.getenv("DELTA_SHELF_EVENTS_PATH", str(DEFAULT_DELTA_SHELF_EVENTS)))

# tabella alerts su Postgres (nome fisso)
ALERTS_TABLE_NAME = os.getenv("ALERTS_TABLE", "ops.alerts").strip()
# schema di default per le tabelle "state.*"
STATE_SCHEMA = os.getenv("STATE_SCHEMA", "state").strip()

# live refresh
AUTOREFRESH_MS = int(os.getenv("DASH_AUTOREFRESH_MS", "1500"))  # 1.5s
WEIGHT_EVENT_LOOKBACK_SEC = int(os.getenv("WEIGHT_EVENT_LOOKBACK_SEC", "8"))

# weight event type label (se nel tuo delta è diverso, cambialo qui)
WEIGHT_EVENT_TYPE = os.getenv("WEIGHT_EVENT_TYPE", "weight_change")

log = logging.getLogger("dashboard")

# -----------------------------
# DATA STRUCTURES
# -----------------------------
@dataclass
class Zone:
    id: str
    category: str   # es: "BEVERAGES"
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


def must_have(dep_ok: bool, msg: str):
    if not dep_ok:
        st.error(msg)
        st.stop()


# -----------------------------
# LOAD LAYOUT (YAML)
# -----------------------------
@st.cache_data(show_spinner=False)
def _load_layout_cached(layout_path_str: str) -> Tuple[str, List[Dict[str, Any]], str]:
    """
    Cache-safe: ritorna SOLO tipi semplici (str/list/dict/float).
    Returns: (img_rel_path, zones_payload, coord_type)
    """
    if yaml is None:
        raise RuntimeError("pyyaml non installato. Aggiungi 'pyyaml' ai requirements.")

    layout_path = Path(layout_path_str)
    if not layout_path.exists():
        raise FileNotFoundError(f"Layout YAML non trovato: {layout_path}")

    data = yaml.safe_load(layout_path.read_text())

    coord_type = str(data.get("coord_type", "normalized")).strip().lower()
    if coord_type != "normalized":
        raise ValueError("In questo app.py supporto solo coord_type: normalized (0..1).")

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


def load_layout(layout_path: Path) -> Tuple[Path, List["Zone"], str]:
    """
    Wrapper NON cachata: ricrea i Zone (oggetti Python) ogni run.
    """
    img_rel, zones_payload, coord_type = _load_layout_cached(str(layout_path.resolve()))
    img_path = (layout_path.parent / img_rel).resolve()

    zones: List[Zone] = [Zone(**zp) for zp in zones_payload]
    return img_path, zones, coord_type



# -----------------------------
# LOAD INVENTORY CSV (shelf_id -> category/subcategory)
# -----------------------------
@st.cache_data(show_spinner=False)
def load_inventory(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(f"Inventory CSV non trovato: {csv_path}")
    df = pd.read_csv(csv_path)
    # attesi: shelf_id, item_category, item_subcategory
    for col in ["shelf_id", "item_category", "item_subcategory"]:
        if col not in df.columns:
            raise ValueError(f"Nel CSV manca la colonna '{col}'. Colonne presenti: {list(df.columns)}")

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
    """
    Usa le env standard:
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
    """
    host = os.getenv("POSTGRES_HOST", "postgresql")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "postgres")
    user = os.getenv("POSTGRES_USER", "postgres")
    pwd = os.getenv("POSTGRES_PASSWORD", "postgres")
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"


@st.cache_resource(show_spinner=False)
def get_pg_engine():
    must_have(create_engine is not None, "Manca SQLAlchemy. Aggiungi 'sqlalchemy' e 'psycopg2-binary' ai requirements.")
    return create_engine(postgres_url_from_env(), pool_pre_ping=True)


@st.cache_data(ttl=2, show_spinner=False)
def fetch_alerts() -> pd.DataFrame:
    """
    Tabella alerts (schema che mi hai dato):
    alert_id, event_type, shelf_id, location, severity, current_stock, max_stock,
    target_pct, suggested_qty, status, created_at, updated_at
    """
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
    # normalizza
    if "shelf_id" in df.columns:
        df["shelf_id"] = df["shelf_id"].astype(str).str.strip()
    if "status" in df.columns:
        df["status"] = df["status"].astype(str).str.strip().str.lower()
    return df


def active_alerts(alerts_df: pd.DataFrame) -> pd.DataFrame:
    if alerts_df.empty:
        return alerts_df
    if "status" not in alerts_df.columns:
        return alerts_df
    # considera attivi: status != "resolved"
    return alerts_df[alerts_df["status"].isin(["open", "active", "pending"])].copy()


@st.cache_data(ttl=5, show_spinner=False)
def fetch_pg_table(table_name: str, limit: int = 2000) -> pd.DataFrame:
    """
    Per gli states (shelf_state, wh_state, shelf_batch_state, wh_batch_state)
    """
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


# -----------------------------
# DELTA (shelf_events)
# -----------------------------
def _pick_timestamp_column(df: pd.DataFrame) -> Optional[str]:
    for col in ["event_ts", "event_time", "timestamp", "ts", "created_at", "time"]:
        if col in df.columns:
            return col
    return None


def _to_datetime(series: pd.Series) -> pd.Series:
    # prova a convertire robustamente
    if pd.api.types.is_datetime64_any_dtype(series):
        return series
    # epoch?
    if pd.api.types.is_numeric_dtype(series):
        # euristica: ms vs s
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
    """
    Legge Delta raw shelf_events e filtra gli eventi weight_change recenti.
    Richiede deltalake (delta-rs).
    """
    if DeltaTable is None:
        # niente delta-rs: ritorna vuoto
        return pd.DataFrame()

    if not delta_path.exists():
        # se non esiste path, vuoto
        return pd.DataFrame()
    if not _is_delta_table(delta_path):
        # cartella esiste ma non e' una delta table valida
        return pd.DataFrame()

    try:
        dt = DeltaTable(str(delta_path))
    except Exception as e:
        # delta-rs alza TableNotFoundError se manca _delta_log
        log.warning("Delta table not available at %s: %s", delta_path, e)
        return pd.DataFrame()
    df = dt.to_pandas()  # per un progetto demo va bene; se cresce, ottimizziamo con predicate pushdown

    if df.empty:
        return df

    # colonne attese minime
    if "shelf_id" in df.columns:
        df["shelf_id"] = df["shelf_id"].astype(str).str.strip()

    # event_type
    et_col = "event_type" if "event_type" in df.columns else ("type" if "type" in df.columns else None)
    if et_col is None:
        return pd.DataFrame()

    df[et_col] = df[et_col].astype(str).str.strip().str.lower()
    df = df[df[et_col] == WEIGHT_EVENT_TYPE.lower()].copy()
    if df.empty:
        return df

    # timestamp
    ts_col = _pick_timestamp_column(df)
    if ts_col is None:
        return pd.DataFrame()

    df["_ts"] = _to_datetime(df[ts_col])
    now = pd.Timestamp.utcnow()
    cutoff = now - pd.Timedelta(seconds=int(lookback_sec))
    df = df[df["_ts"] >= cutoff].copy()

    return df


# -----------------------------
# MAP FIGURE (Plotly)
# -----------------------------
def build_map_figure(
    map_img: Image.Image,
    zones: List[Zone],
    red_categories: set[str],
    green_categories: set[str],
    blink_on: bool,
) -> go.Figure:
    """
    Crea una figura Plotly con immagine di sfondo e zone cliccabili.
    Coordinate assi: x,y in [0..1] (normalized)
    """
    fig = go.Figure()

    # background image
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

    # assi
    fig.update_xaxes(range=[0, 1], showgrid=False, zeroline=False, visible=False)
    fig.update_yaxes(range=[1, 0], showgrid=False, zeroline=False, visible=False)  # invert y (top=0)

    # zone overlays
    for z in zones:
        cat = normalize_cat(z.category)
        is_red = cat in red_categories
        is_green = (cat in green_categories) and blink_on

        # Stile: se red -> riempimento rosso trasparente.
        # se green -> bordo verde e (blink) fill leggero
        # altrimenti invisibile ma cliccabile
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

    fig.update_layout(
        margin=dict(l=0, r=0, t=0, b=0),
        height=820,
    )
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

    must_have(yaml is not None, "Installa 'pyyaml' nei requirements per leggere il layout.yaml.")
    must_have(plotly_events is not None, "Installa 'streamlit-plotly-events' nei requirements per gestire il click sulla mappa.")

    # autorefresh per “quasi realtime”
    if st_autorefresh is not None:
        st_autorefresh(interval=AUTOREFRESH_MS, key="autorefresh")

    # load base assets
    map_img_path, zones, coord_type = load_layout(LAYOUT_YAML_PATH)
    inv = load_inventory(INVENTORY_CSV_PATH)
    shelf_to_cat, shelf_to_sub = build_shelf_maps(inv)

    st.title("📍 Supermarket Live Dashboard")

    tabs = st.tabs(["🗺️ Map Live", "📦 States", "🚨 Alerts (raw)", "⚙️ Debug"])

    # blinking logic
    blink_on = (int(time.time() * 2) % 2 == 0)

    # -----------------------------
    # TAB 0: MAP LIVE
    # -----------------------------
    with tabs[0]:
        col_left, col_right = st.columns([1.15, 0.85], gap="large")

        alerts_df = fetch_alerts()
        act_alerts = active_alerts(alerts_df)

        # categorie in alert (rosso)
        red_cats = set()
        if not act_alerts.empty and "shelf_id" in act_alerts.columns:
            tmp = act_alerts.copy()
            tmp["item_category"] = tmp["shelf_id"].map(shelf_to_cat).map(normalize_cat)
            red_cats = set(tmp["item_category"].dropna().unique().tolist())

        # eventi weight_change recenti (verde blink)
        weight_df = fetch_recent_weight_events(DELTA_SHELF_EVENTS_PATH, WEIGHT_EVENT_LOOKBACK_SEC)
        green_cats = set()
        if not weight_df.empty and "shelf_id" in weight_df.columns:
            tmpw = weight_df.copy()
            tmpw["item_category"] = tmpw["shelf_id"].map(shelf_to_cat).map(normalize_cat)
            green_cats = set(tmpw["item_category"].dropna().unique().tolist())

        with col_left:
            st.subheader("Mappa live (rosso = alert attivo, verde = weight_change recente)")
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
            st.subheader("Dettagli")
            st.metric("Alerts attivi", int(len(act_alerts)))
            st.metric("Weight events recenti", int(len(weight_df)))

            clicked_zone: Optional[Zone] = None
            clicked_cat: Optional[str] = None

            if events:
                e0 = events[0]

                # 1) via customdata (consigliato)
                cd = e0.get("customdata")
                if isinstance(cd, dict) and "category" in cd:
                    clicked_cat = normalize_cat(cd.get("category"))
                    # trova la zone associata
                    for z in zones:
                        if normalize_cat(z.category) == clicked_cat:
                            clicked_zone = z
                            break

                # 2) fallback via x,y
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

                sub = act_alerts.copy()
                if not sub.empty and "shelf_id" in sub.columns:
                    sub["item_category"] = sub["shelf_id"].map(shelf_to_cat).map(normalize_cat)
                    sub["item_subcategory"] = sub["shelf_id"].map(shelf_to_sub)
                    sub = sub[sub["item_category"] == clicked_cat].copy()

                if sub.empty:
                    st.info("Nessun alert attivo in questa sezione.")
                else:
                    st.error(f"Alert attivi nella sezione: {len(sub)}")

                    shelf_list = sorted(sub["shelf_id"].dropna().unique().tolist())
                    st.markdown("**Shelf in alert:**")
                    st.write(shelf_list)

                    cols = [
                        "alert_id", "event_type", "shelf_id", "severity",
                        "current_stock", "max_stock", "target_pct", "suggested_qty",
                        "status", "created_at"
                    ]
                    cols = [c for c in cols if c in sub.columns]
                    st.markdown("**Dettagli alerts (ultimi):**")
                    st.dataframe(sub[cols].head(200), use_container_width=True)
            else:
                st.caption("Clicca su una sezione della mappa per vedere gli alert di quella zona.")

            # warning delta
            if DeltaTable is None:
                st.warning("DeltaTable (deltalake) non installato: i weight_change recenti non saranno mostrati.")

    # -----------------------------
    # TAB 1: STATES
    # -----------------------------
    with tabs[1]:
        st.subheader("States (Postgres)")

        left, right = st.columns([0.35, 0.65], gap="large")

        with left:
            table = st.selectbox(
                "Scegli tabella state",
                ["shelf_state", "wh_state", "shelf_batch_state", "wh_batch_state"],
                index=0,
            )

            # filtri da inventario
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
                st.error(f"Errore fetch tabella {table}: {e}")
                df_state = pd.DataFrame()

            if df_state.empty:
                st.info("Nessun dato.")
            else:
                # prova a filtrare se ci sono colonne compatibili (shelf_id/category/subcategory)
                show = df_state.copy()
                # se la tabella ha shelf_id
                if "shelf_id" in show.columns and sel_shelf != "(tutte)":
                    show["shelf_id"] = show["shelf_id"].astype(str).str.strip()
                    show = show[show["shelf_id"] == sel_shelf]

                # se la tabella ha category/subcategory
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

    # -----------------------------
    # TAB 2: ALERTS RAW
    # -----------------------------
    with tabs[2]:
        st.subheader("Alerts (tabella Postgres)")
        df = fetch_alerts()
        if df.empty:
            st.info("Nessun alert trovato.")
        else:
            st.dataframe(df, use_container_width=True)

    # -----------------------------
    # TAB 3: DEBUG
    # -----------------------------
    with tabs[3]:
        st.subheader("Debug / Config")
        st.write("PROJECT_ROOT:", str(PROJECT_ROOT))
        st.write("LAYOUT_YAML_PATH:", str(LAYOUT_YAML_PATH))
        st.write("INVENTORY_CSV_PATH:", str(INVENTORY_CSV_PATH))
        st.write("DELTA_SHELF_EVENTS_PATH:", str(DELTA_SHELF_EVENTS_PATH))
        st.write("ALERTS_TABLE_NAME:", ALERTS_TABLE_NAME)
        st.write("AUTOREFRESH_MS:", AUTOREFRESH_MS)
        st.write("WEIGHT_EVENT_LOOKBACK_SEC:", WEIGHT_EVENT_LOOKBACK_SEC)
        st.write("WEIGHT_EVENT_TYPE:", WEIGHT_EVENT_TYPE)

        st.markdown("**Categorie presenti nel CSV (item_category):**")
        st.write(sorted(inv["item_category"].dropna().unique().tolist()))

        st.markdown("**Zone nel layout.yaml:**")
        st.write([{"id": z.id, "category": z.category, "bounds": (z.x0, z.y0, z.x1, z.y1)} for z in zones])


if __name__ == "__main__":
    main()
