"""
Streamlit dashboard MVP for the BDT retail project
"""

import os
import time
import pytz
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import plotly.graph_objects as go
import math

# -----------------------------
# Config & helpers
# -----------------------------
st.set_page_config(page_title="Retail Ops – Shelf Intelligence", page_icon="🛒", layout="wide")

TZ = os.getenv("TZ", "Europe/Rome")
LOCAL_TZ = pytz.timezone(TZ)

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "retaildb")
PG_USER = os.getenv("PG_USER", "retail")
PG_PASS = os.getenv("PG_PASS", "retailpass")

@st.cache_resource(show_spinner=False)
def get_engine():
    url = f"postgresql+psycopg://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    engine = create_engine(url, pool_pre_ping=True)
    return engine

def q(engine, sql, params=None):
    with engine.begin() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})

def exec_sql(engine, sql, params=None):
    with engine.begin() as conn:
        conn.execute(text(sql), params or {})

# -----------------------------
# UI: sidebar controls
# -----------------------------
with st.sidebar:
    st.title("🛒 Retail Ops")
    st.caption("Shelf sensors, stock and alerts")
    refresh_ms = st.slider("Auto-refresh (seconds)", 2, 60, 5) * 1000
    lookback_min = st.slider("Shelf events: lookback (minutes)", 5, 240, 30)
    top_n = st.slider("Top rows per table", 10, 200, 50)
    st.divider()
    st.caption("Store map controls")
    severity_sel = st.multiselect(
        "Mostra scaffali",
        options=["critical","near","ok","unknown"],
        default=["critical","near"],
        help="Filtra per stato stock rispetto alla soglia"
    )
    page_size = st.slider("Scaffali per pagina (mappa)", 100, 1000, 500, step=50)
    search_id = st.text_input("Cerca shelf_id", placeholder="es. A12-034", help="Mostra e evidenzia lo scaffale specifico")
    st.caption("Optional store layout (CSV: shelf_id,x,y,w,h,label)")
    layout_file = st.file_uploader("Upload layout.csv", type=["csv"], help="Coordinates in arbitrary units; origin at top-left")
    st.divider()
    st.caption("Actions")
    if st.button("Run near-expiry check now"):
        try:
            exec_sql(get_engine(), "SELECT check_near_expiry();")
            st.success("check_near_expiry() executed")
        except Exception as e:
            st.error(f"Failed: {e}")

# Auto refresh
try:
    st.autorefresh(interval=refresh_ms, key="auto-refresh")
except Exception:
    pass

engine = get_engine()

# -----------------------------
# KPI row
# -----------------------------
col1, col2, col3, col4 = st.columns(4)

kpi_alerts = q(engine, "SELECT COUNT(*) AS open_alerts FROM alerts WHERE status='OPEN';")
col1.metric("Open alerts", int(kpi_alerts.loc[0, 'open_alerts']))

kpi_today = q(engine, """
    SELECT 
      SUM(CASE WHEN event_type='pickup'  THEN 1 ELSE 0 END) AS pickups,
      SUM(CASE WHEN event_type='putback' THEN 1 ELSE 0 END) AS putbacks,
      SUM(CASE WHEN event_type='restock' THEN 1 ELSE 0 END) AS restocks
    FROM shelf_events
    WHERE event_time::date = current_date;
""")
col2.metric("Pickups (today)", int(kpi_today.loc[0, 'pickups'] or 0))
col3.metric("Putbacks (today)", int(kpi_today.loc[0, 'putbacks'] or 0))
col4.metric("Restocks (today)", int(kpi_today.loc[0, 'restocks'] or 0))

st.divider()

# -----------------------------
# Store map – realistic aisle view
# -----------------------------
st.subheader("Store layout - zone & aisles")

layout_df, map_df, plan = None, None, pd.DataFrame()

# Carica layout.csv se fornito
if layout_file is not None:
    try:
        layout_df = pd.read_csv(layout_file)
        layout_df["shelf_id"] = layout_df["shelf_id"].astype(str).str.strip().str.upper()
        st.write("📂 Colonne layout_df:", layout_df.columns.tolist())
        st.write("➡️ shelf_id layout_df (prime 10):", layout_df["shelf_id"].head(10).tolist())
    except Exception as e:
        st.warning(f"Invalid CSV: {e}")

# Carica la tabella shelf_id/status dal DB
try:
    map_df = q(engine, "SELECT shelf_id, status FROM shelf_status;")
    map_df["shelf_id"] = map_df["shelf_id"].astype(str).str.strip().str.upper()
    st.write("📂 Colonne map_df:", map_df.columns.tolist())
    st.write("➡️ shelf_id map_df (prime 10):", map_df["shelf_id"].head(10).tolist())
except Exception as e:
    st.warning(f"⚠️ Nessuna tabella shelf_status trovata: {e}")

# Merge layout + stato scaffali
if layout_df is not None and map_df is not None:
    plan = layout_df.merge(map_df, on="shelf_id", how="inner")
    st.write("📊 Plan preview:", plan.head())
    st.write(f"✅ Match trovati: {len(plan)} su {len(layout_df)} layout e {len(map_df)} map_df")

# Zone summary
if plan is not None and not plan.empty:
    zone_summary = (
        plan.groupby("zone")["status"]
            .value_counts()
            .unstack(fill_value=0)
            .reset_index()
    )
    for col in ["critical","near","ok","unknown"]:
        if col not in zone_summary.columns:
            zone_summary[col] = 0
    zone_summary["total"] = zone_summary[["critical","near","ok","unknown"]].sum(axis=1)

    zones = zone_summary["zone"].tolist()
    page_choice = st.selectbox("Pagina/Area", ["Overview"] + zones, index=0)
else:
    st.warning("⚠️ Nessun layout disponibile per generare la mappa (CSV mancante o tabella vuota).")
    zone_summary = pd.DataFrame(columns=["zone","critical","near","ok","unknown","total"])
    page_choice = "Overview"

# Vista overview o corsia
if page_choice == "Overview":
    ov = zone_summary.sort_values(["critical","near","total"], ascending=[False, False, False])
    ov_display = ov.assign(alert=lambda df: df.apply(lambda r: "🔴" if r["critical"]>0 else ("🟠" if r["near"]>0 else "🟢"), axis=1))[["zone","alert","critical","near","ok","unknown","total"]]
    st.dataframe(ov_display, use_container_width=True, hide_index=True)
    st.info("Seleziona una zona dall'elenco sopra per entrare nella mappa.")
else:
    plan_z = plan[plan["zone"] == page_choice].copy()
    if severity_sel:
        plan_z = plan_z[plan_z["status"].isin(severity_sel) | plan_z["shelf_id"].eq(search_id)]

    aisles = sorted(plan_z["aisle"].dropna().unique().tolist())
    if not aisles:
        st.warning("Nessuna corsia trovata in questa zona")
    else:
        aisle_sel = st.selectbox("Corsia", aisles)
        plan_a = plan_z[plan_z["aisle"] == aisle_sel].reset_index(drop=True)

        fig = go.Figure()
        for i, row in plan_a.iterrows():
            x0, y0 = i*1.2, 0
            x1, y1 = x0 + 1, 2
            fig.add_shape(
                type="rect", x0=x0, y0=y0, x1=x1, y1=y1,
                fillcolor=row.get("color", "gray"), line=dict(color="black", width=1)
            )
            fig.add_annotation(
                x=(x0+x1)/2, y=1,
                text=row["shelf_id"],
                showarrow=False,
                font=dict(size=10, color="white")
            )

        fig.update_xaxes(visible=False)
        fig.update_yaxes(visible=False)
        fig.update_layout(height=250, title=f"Zona: {page_choice} – Corsia {aisle_sel} ({len(plan_a)} scaffali)")
        st.plotly_chart(fig, use_container_width=True, theme="streamlit")

# -----------------------------
# Live shelf events
# -----------------------------
st.subheader("Live shelf stream")
live_sql = """
SELECT 
  se.event_time AT TIME ZONE 'UTC' AT TIME ZONE :tz AS event_time_local,
  se.shelf_id, se.event_type, se.qty_est, se.weight_change,
  i.item_id, c.category_name, se.meta
FROM shelf_events se
JOIN items i ON i.item_id = se.item_id
JOIN categories c ON c.category_id = i.category_id
WHERE se.event_time >= now() - (:mins || ' minutes')::interval
ORDER BY se.event_time DESC
LIMIT :lim;
"""
live_df = q(engine, live_sql, {"mins": lookback_min, "lim": top_n, "tz": TZ})
st.dataframe(live_df, use_container_width=True, hide_index=True)

# -----------------------------
# (resto codice: low stock, refill, alerts, POS...)
# -----------------------------
# ⚠️ resto uguale a quello che avevi, non lo riscrivo per intero per brevità


# -----------------------------
# Low stock focus (instore)
# -----------------------------
st.subheader("Low stock focus – instore")
low_sql = """
WITH 
  instore AS (
    SELECT l.location_id FROM locations l WHERE l.location='instore'
  ),
  thr AS (
    SELECT 
      pi.item_id,
      pi.location_id,
      COALESCE(
        (SELECT low_stock_threshold FROM inventory_thresholds t 
           WHERE t.scope='item' AND t.item_id=pi.item_id AND (t.location_id=pi.location_id OR t.location_id IS NULL)
           ORDER BY t.location_id NULLS LAST LIMIT 1),
        (SELECT low_stock_threshold FROM inventory_thresholds t 
           WHERE t.scope='category' AND t.category_id=i.category_id AND (t.location_id=pi.location_id OR t.location_id IS NULL)
           ORDER BY t.location_id NULLS LAST LIMIT 1),
        (SELECT low_stock_threshold FROM inventory_thresholds t 
           WHERE t.scope='global' AND (t.location_id=pi.location_id OR t.location_id IS NULL)
           ORDER BY t.location_id NULLS LAST LIMIT 1),
        0
      ) AS low_thr,
      (SELECT safety_stock FROM inventory_thresholds t 
         WHERE t.scope='item' AND t.item_id=pi.item_id AND (t.location_id=pi.location_id OR t.location_id IS NULL)
         ORDER BY t.location_id NULLS LAST LIMIT 1) AS safety_stock
    FROM product_inventory pi
    JOIN items i ON i.item_id=pi.item_id
    WHERE pi.location_id = (SELECT location_id FROM instore)
  )
SELECT 
  i.shelf_id,
  c.category_name,
  pi.current_stock,
  COALESCE(pi.price, 0) AS price,
  COALESCE(thr.low_thr, 0) AS low_thr,
  GREATEST(0, COALESCE(thr.low_thr,0) - COALESCE(pi.current_stock,0)) AS gap,
  COALESCE(thr.safety_stock, 0) AS safety_stock,
  i.item_id
FROM product_inventory pi
JOIN items i ON i.item_id=pi.item_id
JOIN categories c ON c.category_id=i.category_id
JOIN thr ON thr.item_id=pi.item_id AND thr.location_id=pi.location_id
WHERE pi.location_id = (SELECT location_id FROM instore)
ORDER BY gap DESC, pi.current_stock ASC
LIMIT :lim;
"""
low_df = q(engine, low_sql, {"lim": top_n})

c1, c2 = st.columns([2,1])
c1.dataframe(low_df, use_container_width=True, hide_index=True)

try:
    import plotly.express as px
    fig = px.bar(low_df.head(20), x="shelf_id", y="gap", hover_data=["current_stock", "low_thr", "safety_stock", "category_name"], title="Top gaps vs low-stock threshold")
    fig.update_layout(height=360, xaxis_title="shelf", yaxis_title="gap to threshold")
    c2.plotly_chart(fig, use_container_width=True, theme="streamlit")
except Exception as e:
    c2.info(f"Plot unavailable: {e}")

# -----------------------------
# Simple refill suggestions (warehouse -> instore)
# -----------------------------
st.subheader("Refill suggestions (heuristic)")
refill_sql = """
WITH instore AS (SELECT location_id FROM locations WHERE location='instore'),
     wh AS (SELECT location_id FROM locations WHERE location='warehouse'),
     thr AS (
       SELECT 
         pi.item_id,
         pi.location_id,
         COALESCE(
           (SELECT low_stock_threshold FROM inventory_thresholds t 
            WHERE t.scope='item' AND t.item_id=pi.item_id AND (t.location_id=pi.location_id OR t.location_id IS NULL)
            ORDER BY t.location_id NULLS LAST LIMIT 1),
           (SELECT low_stock_threshold FROM inventory_thresholds t 
            WHERE t.scope='category' AND t.category_id=i.category_id AND (t.location_id=pi.location_id OR t.location_id IS NULL)
            ORDER BY t.location_id NULLS LAST LIMIT 1),
           (SELECT low_stock_threshold FROM inventory_thresholds t 
            WHERE t.scope='global' AND (t.location_id=pi.location_id OR t.location_id IS NULL)
            ORDER BY t.location_id NULLS LAST LIMIT 1),
           0
         ) AS low_thr,
         (SELECT safety_stock FROM inventory_thresholds t 
            WHERE t.scope='item' AND t.item_id=pi.item_id AND (t.location_id=pi.location_id OR t.location_id IS NULL)
            ORDER BY t.location_id NULLS LAST LIMIT 1) AS safety_stock
       FROM product_inventory pi
       JOIN items i ON i.item_id=pi.item_id
       WHERE pi.location_id=(SELECT location_id FROM instore)
     )
SELECT 
  i.shelf_id,
  c.category_name,
  st.current_stock AS stock_instore,
  whi.current_stock AS stock_wh,
  COALESCE(thr.low_thr,0) AS low_thr,
  GREATEST(0, COALESCE(thr.low_thr,0) - COALESCE(st.current_stock,0)) AS gap,
  CEIL(GREATEST(0, COALESCE(thr.low_thr,0) + COALESCE(thr.safety_stock,0) - COALESCE(st.current_stock,0)))::int AS suggested_refill_qty
FROM thr
JOIN product_inventory st  ON st.item_id=thr.item_id AND st.location_id=(SELECT location_id FROM instore)
JOIN product_inventory whi ON whi.item_id=thr.item_id AND whi.location_id=(SELECT location_id FROM wh)
JOIN items i ON i.item_id=thr.item_id
JOIN categories c ON c.category_id=i.category_id
WHERE (COALESCE(st.current_stock,0) < COALESCE(thr.low_thr,0))
  AND whi.current_stock > 0
ORDER BY gap DESC, stock_wh DESC
LIMIT :lim;
"""
refill_df = q(engine, refill_sql, {"lim": top_n})

st.dataframe(refill_df, use_container_width=True, hide_index=True)

# -----------------------------
# Alerts
# -----------------------------
st.subheader("Alerts")
alerts_sql = """
SELECT 
  rule_key, severity, status, item_id, location_id, batch_id,
  message, value_num,
  opened_at AT TIME ZONE 'UTC' AT TIME ZONE :tz AS opened_at,
  last_seen_at AT TIME ZONE 'UTC' AT TIME ZONE :tz AS last_seen_at
FROM alerts
WHERE status='OPEN'
ORDER BY severity DESC, last_seen_at DESC
LIMIT :lim;
"""
alerts_df = q(engine, alerts_sql, {"lim": top_n, "tz": TZ})
st.dataframe(alerts_df, use_container_width=True, hide_index=True)

# -----------------------------
# Receipts snapshot (optional POS sanity)
# -----------------------------
st.subheader("Receipts (today)")
receipts_sql = """
SELECT 
  r.receipt_id, r.transaction_id, r.business_date, r.closed_at,
  r.total_gross, r.total_net, r.total_tax, r.status,
  COALESCE(SUM(rl.quantity),0) AS total_lines
FROM receipts r
LEFT JOIN receipt_lines rl ON rl.receipt_id=r.receipt_id
WHERE r.business_date = current_date
GROUP BY r.receipt_id
ORDER BY r.closed_at DESC
LIMIT :lim;
"""
receipts_df = q(engine, receipts_sql, {"lim": top_n})
st.dataframe(receipts_df, use_container_width=True, hide_index=True)

st.caption("© BDT 2025 – demo dashboard. Data refresh is automatic.")


# -----------------------------
# POS analytics (from Spark pipeline)
# -----------------------------
st.subheader("POS – vendite in tempo reale")

# Ultimi scontrini
pos_sql = """
SELECT 
  r.receipt_id,
  r.transaction_id,
  r.business_date,
  r.closed_at,
  r.total_gross,
  r.total_net,
  r.total_tax,
  r.status,
  COALESCE(SUM(rl.quantity),0) AS total_lines,
  COALESCE(SUM(rl.line_total),0) AS total_sales
FROM receipts r
LEFT JOIN receipt_lines rl ON rl.receipt_id = r.receipt_id
WHERE r.business_date = current_date
GROUP BY r.receipt_id
ORDER BY r.closed_at DESC
LIMIT :lim;
"""
pos_df = q(engine, pos_sql, {"lim": top_n})
st.dataframe(pos_df, use_container_width=True, hide_index=True)

# KPI incassi giornalieri
kpi_pos = q(engine, """
SELECT 
  COALESCE(SUM(total_gross),0) AS gross,
  COALESCE(SUM(total_net),0)   AS net,
  COUNT(*) AS receipts
FROM receipts
WHERE business_date = current_date;
""")
c1, c2, c3 = st.columns(3)
c1.metric("Scontrini oggi", int(kpi_pos.loc[0,"receipts"]))
c2.metric("Incasso lordo", f"€{kpi_pos.loc[0,'gross']:.2f}")
c3.metric("Incasso netto", f"€{kpi_pos.loc[0,'net']:.2f}")

# Top categorie vendute
try:
    sales_cat = q(engine, """
    SELECT 
      c.category_name,
      SUM(rl.quantity) AS qty,
      SUM(rl.line_total) AS sales
    FROM receipt_lines rl
    JOIN items i ON i.item_id = rl.item_id
    JOIN categories c ON c.category_id = i.category_id
    JOIN receipts r ON r.receipt_id = rl.receipt_id
    WHERE r.business_date = current_date
    GROUP BY c.category_name
    ORDER BY sales DESC
    LIMIT 10;
    """)
    import plotly.express as px
    fig = px.bar(sales_cat, x="category_name", y="sales", title="Top categorie vendute (oggi)")
    fig.update_layout(height=360, xaxis_title="Categoria", yaxis_title="€ vendite")
    st.plotly_chart(fig, use_container_width=True, theme="streamlit")
except Exception as e:
    st.info(f"Nessuna vendita disponibile: {e}")

# Stream events raw (debug)
st.subheader("POS raw stream events (Spark → Postgres)")
events_sql = """
SELECT event_id, source, event_ts, left(payload::text,200) AS payload
FROM stream_events
WHERE source='pos.sales'
ORDER BY event_ts DESC
LIMIT :lim;
"""
events_df = q(engine, events_sql, {"lim": top_n})
st.dataframe(events_df, use_container_width=True, hide_index=True)
