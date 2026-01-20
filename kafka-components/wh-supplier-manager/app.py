import json
import os
import socket
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Optional, Tuple

import pandas as pd
from confluent_kafka import Producer
from deltalake import DeltaTable, write_deltalake

from simulated_time.clock import get_simulated_now

# =========================
# Env / Config
# =========================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_WH_SUPPLIER_PLAN = os.getenv("TOPIC_WH_SUPPLIER_PLAN", "wh_supplier_plan")  # compacted (key shelf_id)
TOPIC_WH_EVENTS = os.getenv("TOPIC_WH_EVENTS", "wh_events")  # append-only

DELTA_ROOT = os.getenv("DELTA_ROOT", "/delta")
DL_SUPPLIER_PLAN_PATH = os.getenv("DL_SUPPLIER_PLAN_PATH", f"{DELTA_ROOT}/ops/wh_supplier_plan")
DL_ORDERS_PATH = os.getenv("DL_ORDERS_PATH", f"{DELTA_ROOT}/ops/wh_supplier_orders")
DL_RECEIPTS_PATH = os.getenv("DL_RECEIPTS_PATH", f"{DELTA_ROOT}/ops/wh_supplier_receipts")
DL_WH_EVENTS_RAW = os.getenv("DL_WH_EVENTS_RAW", f"{DELTA_ROOT}/raw/wh_events")

POLL_SECONDS = float(os.getenv("POLL_SECONDS", "10"))
MIRROR_WH_EVENTS_DELTA = os.getenv("MIRROR_WH_EVENTS_DELTA", "1") in ("1", "true", "True")
KAFKA_WAIT_TIMEOUT_S = int(os.getenv("KAFKA_WAIT_TIMEOUT_S", "180"))
KAFKA_WAIT_POLL_S = float(os.getenv("KAFKA_WAIT_POLL_S", "2.0"))

# schedule (UTC, simulated time)
CUTOFF_DOWS = os.getenv("CUTOFF_DOWS", "6,1,3")  # Sun, Tue, Thu
CUTOFF_HOUR = int(os.getenv("CUTOFF_HOUR", "12"))
CUTOFF_MINUTE = int(os.getenv("CUTOFF_MINUTE", "0"))

DELIVERY_DOWS = os.getenv("DELIVERY_DOWS", "0,2,4")  # Mon, Wed, Fri
DELIVERY_HOUR = int(os.getenv("DELIVERY_HOUR", "8"))
DELIVERY_MINUTE = int(os.getenv("DELIVERY_MINUTE", "0"))

DEFAULT_EXPIRY_DAYS = int(os.getenv("DEFAULT_EXPIRY_DAYS", "365"))

DELTA_WRITE_MAX_RETRIES = int(os.getenv("DELTA_WRITE_MAX_RETRIES", "5"))
DELTA_WRITE_RETRY_BASE_S = float(os.getenv("DELTA_WRITE_RETRY_BASE_S", "0.8"))


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
_DELIVERY_DOWS = _parse_dows(DELIVERY_DOWS)


def now_utc() -> datetime:
    return ensure_utc(get_simulated_now())


def cutoff_dt(ts: datetime) -> datetime:
    return ts.replace(hour=CUTOFF_HOUR, minute=CUTOFF_MINUTE, second=0, microsecond=0)


def delivery_dt(d: date) -> datetime:
    return datetime.combine(d, datetime.min.time()).replace(
        tzinfo=timezone.utc, hour=DELIVERY_HOUR, minute=DELIVERY_MINUTE
    )


def is_cutoff_moment(ts: datetime) -> bool:
    return (ts.weekday() in _CUTOFF_DOWS) and ts.hour == CUTOFF_HOUR and ts.minute == CUTOFF_MINUTE


def is_delivery_moment(ts: datetime) -> bool:
    return (ts.weekday() in _DELIVERY_DOWS) and ts.hour == DELIVERY_HOUR and ts.minute == DELIVERY_MINUTE


def next_delivery_date(from_ts: datetime) -> date:
    d = from_ts.date()
    for i in range(0, 8):
        dd = d + timedelta(days=i)
        if dd.weekday() not in _DELIVERY_DOWS:
            continue
        if i == 0 and from_ts >= delivery_dt(dd):
            continue
        return dd
    return d + timedelta(days=7)


def _is_delta_table(path: str) -> bool:
    log_dir = Path(path) / "_delta_log"
    if not log_dir.exists() or not log_dir.is_dir():
        return False
    return any(log_dir.glob("*.json")) or any(log_dir.glob("*.checkpoint"))


def _read_delta(path: str) -> pd.DataFrame:
    if not _is_delta_table(path):
        return pd.DataFrame()
    try:
        return DeltaTable(path).to_pandas()
    except Exception as e:
        print(f"[wh-supplier-manager] warn: failed to read delta table at {path}: {e}")
        return pd.DataFrame()


def _delta_schema_fields(path: str) -> list[str]:
    if not _is_delta_table(path):
        return []
    try:
        dt = DeltaTable(path)
        if hasattr(dt, "schema"):
            s = dt.schema()
            if hasattr(s, "fields"):
                return [getattr(f, "name", str(f)) for f in s.fields]
        if hasattr(dt, "to_pyarrow_table"):
            return list(dt.to_pyarrow_table().schema.names)
        if hasattr(dt, "to_pandas"):
            return list(dt.to_pandas().columns)
        return []
    except Exception:
        return []


def _align_df_to_delta_schema(path: str, df: pd.DataFrame) -> pd.DataFrame:
    """
    Delta-rs is strict about the incoming schema matching the existing table schema.
    Align outgoing DataFrames to the target Delta schema and apply small compatibility
    mappings to prevent "Cannot cast schema" failures when tables were bootstrapped
    with older column sets.
    """
    if df is None or df.empty:
        return df

    fields = _delta_schema_fields(path)
    if not fields:
        return df

    out = df.copy()

    # Compatibility mappings for historical schema variants.
    if "suggested_qty" in fields and "suggested_qty" not in out.columns and "total_qty" in out.columns:
        out["suggested_qty"] = out["total_qty"]
    if "total_qty" in fields and "total_qty" not in out.columns and "suggested_qty" in out.columns:
        out["total_qty"] = out["suggested_qty"]

    if "qty_received" in fields and "qty_received" not in out.columns and "received_qty" in out.columns:
        out["qty_received"] = out["received_qty"]
    if "received_qty" in fields and "received_qty" not in out.columns and "qty_received" in out.columns:
        out["received_qty"] = out["qty_received"]

    for col in fields:
        if col not in out.columns:
            out[col] = None

    extra = [c for c in out.columns if c not in fields]
    if extra:
        out = out.drop(columns=extra)

    return out[fields]


def _write_delta(path: str, df: pd.DataFrame, mode: str) -> None:
    last = None
    for attempt in range(1, DELTA_WRITE_MAX_RETRIES + 1):
        try:
            aligned = _align_df_to_delta_schema(path, df)
            write_deltalake(path, aligned, mode=mode)
            return
        except Exception as e:
            last = e
            sleep_s = DELTA_WRITE_RETRY_BASE_S * attempt
            print(f"[wh-supplier-manager] delta write failed ({attempt}/{DELTA_WRITE_MAX_RETRIES}) -> sleep {sleep_s}s: {e}")
            time.sleep(sleep_s)
    raise last


def _normalize_plan_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    if "shelf_id" in df.columns:
        df["shelf_id"] = df["shelf_id"].astype(str).str.strip()
    if "supplier_plan_id" in df.columns:
        df["supplier_plan_id"] = df["supplier_plan_id"].astype(str).str.strip()
    if "status" in df.columns:
        df["status"] = df["status"].astype(str).str.strip().str.lower()
    for col in ["created_at", "updated_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
    if "suggested_qty" in df.columns:
        df["suggested_qty"] = pd.to_numeric(df["suggested_qty"], errors="coerce").fillna(0).astype("int64")
    if "standard_batch_size" in df.columns:
        s = pd.to_numeric(df["standard_batch_size"], errors="coerce")
        s = s.where(s.notna() & (s > 0), pd.NA)
        # Keep nullable ints so missing sizes stay null (avoid turning null into 0).
        df["standard_batch_size"] = s.astype("Int64")
    return df


def _normalize_orders_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    if "shelf_id" in df.columns:
        df["shelf_id"] = df["shelf_id"].astype(str).str.strip()
    if "status" in df.columns:
        df["status"] = df["status"].astype(str).str.strip().str.lower()
    if "delivery_date" in df.columns:
        df["delivery_date"] = pd.to_datetime(df["delivery_date"], errors="coerce").dt.date

    # Support older schemas where qty was stored as suggested_qty.
    if "total_qty" not in df.columns and "suggested_qty" in df.columns:
        df["total_qty"] = df["suggested_qty"]
    if "suggested_qty" not in df.columns and "total_qty" in df.columns:
        df["suggested_qty"] = df["total_qty"]

    for col in ["cutoff_ts", "delivery_ts", "created_at", "updated_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
    for col in ["total_qty", "suggested_qty", "standard_batch_size"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("int64")
    return df


def _normalize_receipts_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    if "shelf_id" in df.columns:
        df["shelf_id"] = df["shelf_id"].astype(str).str.strip()
    if "delivery_date" in df.columns:
        df["delivery_date"] = pd.to_datetime(df["delivery_date"], errors="coerce").dt.date

    # Support older schemas where qty was stored as qty_received.
    if "received_qty" not in df.columns and "qty_received" in df.columns:
        df["received_qty"] = df["qty_received"]
    if "qty_received" not in df.columns and "received_qty" in df.columns:
        df["qty_received"] = df["received_qty"]

    for col in ["created_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
    for col in ["received_qty", "qty_received"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("int64")
    return df


def _normalize_events_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    for col in ["event_type", "event_id", "shelf_id", "batch_code", "unit", "reason"]:
        if col in df.columns:
            s = df[col]
            df[col] = s.where(s.isna(), s.astype(str).str.strip())
    if "plan_id" in df.columns:
        # keep None/null as null (do not stringify)
        s = df["plan_id"]
        df["plan_id"] = s.where(s.isna(), s.astype(str).str.strip())
    if "event_type" in df.columns:
        df["event_type"] = df["event_type"].str.lower()
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    for col in ["qty", "batch_quantity_warehouse_after", "batch_quantity_store_after", "shelf_warehouse_qty_after"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "fifo" in df.columns:
        df["fifo"] = df["fifo"].astype("boolean")
    return df


def _upsert_by_keys(existing: pd.DataFrame, incoming: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    if incoming.empty:
        return existing
    if existing.empty:
        return incoming

    e = existing.copy()
    i = incoming.copy()

    e["_k"] = e[keys].astype(str).agg("|".join, axis=1)
    i["_k"] = i[keys].astype(str).agg("|".join, axis=1)

    keep = e[~e["_k"].isin(set(i["_k"].tolist()))].drop(columns=["_k"])
    merged = pd.concat([keep, i.drop(columns=["_k"])], ignore_index=True)
    return merged


def _producer() -> Producer:
    return Producer({"bootstrap.servers": KAFKA_BROKER})

def _parse_first_broker(brokers: str) -> tuple[str, int]:
    first = (brokers or "").split(",")[0].strip()
    if not first:
        return "kafka", 9092
    if "://" in first:
        first = first.split("://", 1)[1]
    host, _, port_s = first.partition(":")
    return host, int(port_s or "9092")


def wait_for_kafka() -> None:
    host, port = _parse_first_broker(KAFKA_BROKER)
    deadline = time.time() + KAFKA_WAIT_TIMEOUT_S
    attempt = 0
    while time.time() < deadline:
        attempt += 1
        try:
            with socket.create_connection((host, port), timeout=2.0):
                print(f"[wh-supplier-manager] Kafka ready at {host}:{port}")
                return
        except Exception as e:
            if attempt % 5 == 0:
                remaining = int(max(0, deadline - time.time()))
                print(f"[wh-supplier-manager] Waiting Kafka at {host}:{port}… ({remaining}s left) last_err={e}")
            time.sleep(KAFKA_WAIT_POLL_S)
    raise RuntimeError(f"Kafka not reachable at {host}:{port} after {KAFKA_WAIT_TIMEOUT_S}s")


def publish_plan_updates(plans: pd.DataFrame) -> None:
    if plans.empty:
        return
    if "shelf_id" not in plans.columns:
        return

    def _is_na(v) -> bool:
        try:
            return v is None or pd.isna(v)
        except Exception:
            return v is None

    def _int_or_zero(v) -> int:
        if _is_na(v):
            return 0
        try:
            return int(v)
        except Exception:
            try:
                return int(float(v))
            except Exception:
                return 0

    def _int_or_none(v) -> Optional[int]:
        if _is_na(v):
            return None
        out = _int_or_zero(v)
        return out if out > 0 else None

    p = _producer()
    try:
        for _, row in plans.iterrows():
            msg = {
                "supplier_plan_id": str(row.get("supplier_plan_id") or ""),
                "shelf_id": str(row.get("shelf_id") or "").strip(),
                "suggested_qty": _int_or_zero(row.get("suggested_qty")),
                "standard_batch_size": _int_or_none(row.get("standard_batch_size")),
                "status": str(row.get("status") or "").strip().lower(),
                "created_at": (
                    row.get("created_at").isoformat()
                    if isinstance(row.get("created_at"), (pd.Timestamp, datetime)) and pd.notna(row.get("created_at"))
                    else None
                ),
                "updated_at": (
                    row.get("updated_at").isoformat()
                    if isinstance(row.get("updated_at"), (pd.Timestamp, datetime)) and pd.notna(row.get("updated_at"))
                    else None
                ),
            }
            key = msg["shelf_id"].encode("utf-8")
            p.produce(TOPIC_WH_SUPPLIER_PLAN, key=key, value=json.dumps(msg, default=str).encode("utf-8"))
        p.flush(30)
    finally:
        try:
            p.flush(5)
        except Exception:
            pass


def publish_wh_events(events: pd.DataFrame) -> None:
    if events.empty:
        return

    def _jsonable(v):
        if v is None:
            return None
        try:
            if pd.isna(v):
                return None
        except Exception:
            pass
        if isinstance(v, (pd.Timestamp, datetime)):
            return pd.Timestamp(v).to_pydatetime().isoformat()
        if isinstance(v, date):
            return v.isoformat()
        if isinstance(v, (int, float, bool, str)):
            return v
        return str(v)

    p = _producer()
    try:
        for _, row in events.iterrows():
            msg = {k: _jsonable(v) for k, v in row.to_dict().items()}
            p.produce(TOPIC_WH_EVENTS, key=None, value=json.dumps(msg).encode("utf-8"))
        p.flush(30)
    finally:
        try:
            p.flush(5)
        except Exception:
            pass


def _emit_and_mirror_wh_events(events: pd.DataFrame) -> None:
    if events.empty:
        return
    events = _normalize_events_df(events)
    publish_wh_events(events)
    if not MIRROR_WH_EVENTS_DELTA:
        return

    # Mirror to Delta as append-only to avoid clobbering other writers (e.g. wh-aggregator).
    if _is_delta_table(DL_WH_EVENTS_RAW):
        existing = _read_delta(DL_WH_EVENTS_RAW)
        if not existing.empty and "event_id" in existing.columns:
            existing_ids = set(existing["event_id"].astype(str).tolist())
            to_append = events[~events["event_id"].astype(str).isin(existing_ids)].copy()
        else:
            to_append = events
        if to_append.empty:
            return
        _write_delta(DL_WH_EVENTS_RAW, to_append, mode="append")
    else:
        _write_delta(DL_WH_EVENTS_RAW, events, mode="overwrite")


def do_cutoff(now_ts: datetime) -> None:
    print(f"[wh-supplier-manager] cutoff tick at {now_ts.isoformat()}")

    plans = _normalize_plan_df(_read_delta(DL_SUPPLIER_PLAN_PATH))
    if plans.empty:
        return
    pending = plans[plans["status"] == "pending"].copy()
    if pending.empty:
        return

    delivery = next_delivery_date(now_ts)
    delivery_ts = delivery_dt(delivery)

    # Orders ledger: 1 row per (delivery_date, shelf_id)
    agg = {"suggested_qty": "sum"}
    if "standard_batch_size" in pending.columns:
        agg["standard_batch_size"] = "max"
    orders_new = pending.groupby("shelf_id", as_index=False).agg(agg)
    orders_new = orders_new.rename(columns={"suggested_qty": "total_qty"})
    # Back-compat: older Delta schemas use suggested_qty instead of total_qty.
    orders_new["suggested_qty"] = orders_new["total_qty"]
    orders_new["order_id"] = [
        str(uuid.uuid5(uuid.NAMESPACE_URL, f"wh_supplier_order:{delivery.isoformat()}:{sid}"))
        for sid in orders_new["shelf_id"].astype(str).tolist()
    ]
    orders_new["delivery_date"] = delivery
    orders_new["status"] = "issued"
    orders_new["cutoff_ts"] = pd.Timestamp(now_ts)
    orders_new["delivery_ts"] = pd.Timestamp(delivery_ts)
    orders_new["created_at"] = pd.Timestamp(now_ts)
    orders_new["updated_at"] = pd.Timestamp(now_ts)

    orders_existing = _normalize_orders_df(_read_delta(DL_ORDERS_PATH))
    orders_merged = _upsert_by_keys(orders_existing, _normalize_orders_df(orders_new), keys=["delivery_date", "shelf_id"])
    _write_delta(DL_ORDERS_PATH, orders_merged, mode="overwrite")

    # Update plan statuses: pending -> issued for involved shelves.
    shelves = set(orders_new["shelf_id"].astype(str).tolist())
    updates = plans[plans["shelf_id"].isin(shelves) & (plans["status"] == "pending")].copy()
    if updates.empty:
        return

    updates["status"] = "issued"
    updates["updated_at"] = pd.Timestamp(now_ts)
    plans_updated = plans.copy()
    plans_updated["_k"] = plans_updated["shelf_id"].astype(str)
    updates["_k"] = updates["shelf_id"].astype(str)
    keep = plans_updated[~plans_updated["_k"].isin(set(updates["_k"].tolist()))].drop(columns=["_k"])
    plans_final = pd.concat([keep, updates.drop(columns=["_k"])], ignore_index=True)
    _write_delta(DL_SUPPLIER_PLAN_PATH, plans_final, mode="overwrite")

    publish_plan_updates(updates)
    print(f"[wh-supplier-manager] cutoff done: orders={len(orders_new)} plans_issued={len(updates)} delivery={delivery.isoformat()}")


def do_delivery(now_ts: datetime) -> None:
    print(f"[wh-supplier-manager] delivery tick at {now_ts.isoformat()}")

    today = now_ts.date()
    orders = _normalize_orders_df(_read_delta(DL_ORDERS_PATH))
    if orders.empty:
        return

    due = orders[(orders["delivery_date"] == today) & (orders["status"] == "issued")].copy()
    if due.empty:
        return

    receipts_existing = _normalize_receipts_df(_read_delta(DL_RECEIPTS_PATH))
    already = set()
    if not receipts_existing.empty and {"delivery_date", "shelf_id"}.issubset(receipts_existing.columns):
        already = set(
            receipts_existing[["delivery_date", "shelf_id"]]
            .astype(str)
            .agg("|".join, axis=1)
            .tolist()
        )
    due["_k"] = due[["delivery_date", "shelf_id"]].astype(str).agg("|".join, axis=1)
    due = due[~due["_k"].isin(already)].drop(columns=["_k"])
    if due.empty:
        return

    expiry_date = today + timedelta(days=DEFAULT_EXPIRY_DAYS)

    # Emit wh_in events (1 event per shelf). Keep schema aligned with init_delta schema.
    events = due[["shelf_id", "total_qty"]].copy()
    events["event_type"] = "wh_in"
    events["event_id"] = [
        str(uuid.uuid5(uuid.NAMESPACE_URL, f"wh_in:{today.isoformat()}:{sid}"))
        for sid in events["shelf_id"].astype(str).tolist()
    ]
    events["plan_id"] = None
    events["batch_code"] = [
        f"SUP-{today.strftime('%Y%m%d')}-{sid}-1"
        for sid in events["shelf_id"].astype(str).tolist()
    ]
    events["qty"] = events["total_qty"].astype("int64")
    events["unit"] = "ea"
    events["timestamp"] = pd.Timestamp(now_ts)
    events["fifo"] = True
    events["received_date"] = today.isoformat()
    events["expiry_date"] = expiry_date.isoformat()
    events["batch_quantity_warehouse_after"] = None
    events["batch_quantity_store_after"] = None
    events["shelf_warehouse_qty_after"] = None
    events["reason"] = "supplier_delivery"
    events = events[
        [
            "event_type",
            "event_id",
            "plan_id",
            "shelf_id",
            "batch_code",
            "qty",
            "unit",
            "timestamp",
            "fifo",
            "received_date",
            "expiry_date",
            "batch_quantity_warehouse_after",
            "batch_quantity_store_after",
            "shelf_warehouse_qty_after",
            "reason",
        ]
    ]

    _emit_and_mirror_wh_events(events)

    # Receipts ledger: 1 row per (delivery_date, shelf_id) (idempotent upsert).
    receipts_new = due[["delivery_date", "shelf_id", "total_qty"]].copy()
    receipts_new["receipt_id"] = [
        str(uuid.uuid5(uuid.NAMESPACE_URL, f"wh_supplier_receipt:{today.isoformat()}:{sid}"))
        for sid in receipts_new["shelf_id"].astype(str).tolist()
    ]
    receipts_new = receipts_new.rename(columns={"total_qty": "received_qty"})
    receipts_new["created_at"] = pd.Timestamp(now_ts)
    receipts_new = _normalize_receipts_df(receipts_new)
    receipts_merged = _upsert_by_keys(receipts_existing, receipts_new, keys=["delivery_date", "shelf_id"])
    _write_delta(DL_RECEIPTS_PATH, receipts_merged, mode="overwrite")

    # Update orders to delivered (idempotent) while preserving cutoff_ts/created_at.
    orders2 = orders.copy()
    orders2["delivery_date"] = pd.to_datetime(orders2["delivery_date"], errors="coerce").dt.date
    orders2["shelf_id"] = orders2["shelf_id"].astype(str).str.strip()
    delivered_shelves = set(due["shelf_id"].astype(str).tolist())
    mask = (orders2["delivery_date"] == today) & (orders2["shelf_id"].isin(delivered_shelves))
    orders2.loc[mask, "status"] = "delivered"
    orders2.loc[mask, "delivery_ts"] = pd.Timestamp(now_ts)
    orders2.loc[mask, "updated_at"] = pd.Timestamp(now_ts)
    _write_delta(DL_ORDERS_PATH, _normalize_orders_df(orders2), mode="overwrite")

    # Mark plans completed for shelves delivered today (issued -> completed).
    plans = _normalize_plan_df(_read_delta(DL_SUPPLIER_PLAN_PATH))
    if not plans.empty:
        shelves_today = set(due["shelf_id"].astype(str).tolist())
        updates = plans[plans["shelf_id"].isin(shelves_today) & (plans["status"] == "issued")].copy()
        if not updates.empty:
            updates["status"] = "completed"
            updates["updated_at"] = pd.Timestamp(now_ts)
            plans_updated = plans.copy()
            plans_updated["_k"] = plans_updated["shelf_id"].astype(str)
            updates["_k"] = updates["shelf_id"].astype(str)
            keep = plans_updated[~plans_updated["_k"].isin(set(updates["_k"].tolist()))].drop(columns=["_k"])
            plans_final = pd.concat([keep, updates.drop(columns=["_k"])], ignore_index=True)
            _write_delta(DL_SUPPLIER_PLAN_PATH, plans_final, mode="overwrite")
            publish_plan_updates(updates)

    print(f"[wh-supplier-manager] delivery done: wh_in={len(events)} receipts={len(receipts_new)} shelves={len(due)}")


def main() -> None:
    print(
        "[wh-supplier-manager] starting with "
        f"cutoff_dows={_CUTOFF_DOWS} cutoff={CUTOFF_HOUR:02d}:{CUTOFF_MINUTE:02d} "
        f"delivery_dows={_DELIVERY_DOWS} delivery={DELIVERY_HOUR:02d}:{DELIVERY_MINUTE:02d} "
        f"poll={POLL_SECONDS}s"
    )

    wait_for_kafka()

    last_cutoff_day: Optional[date] = None
    last_delivery_day: Optional[date] = None

    while True:
        time.sleep(max(1.0, POLL_SECONDS))
        ts = now_utc()

        try:
            # If simulated time jumps over the exact minute, still run once per day after the scheduled time.
            if (ts.weekday() in _CUTOFF_DOWS) and (ts >= cutoff_dt(ts)) and (last_cutoff_day != ts.date()):
                do_cutoff(ts)
                last_cutoff_day = ts.date()
        except Exception as e:
            print(f"[wh-supplier-manager] error during cutoff: {e}")

        try:
            trigger_time = ts.replace(hour=DELIVERY_HOUR, minute=DELIVERY_MINUTE, second=0, microsecond=0)
            if (ts.weekday() in _DELIVERY_DOWS) and (ts >= trigger_time) and (last_delivery_day != ts.date()):
                do_delivery(ts)
                last_delivery_day = ts.date()
        except Exception as e:
            print(f"[wh-supplier-manager] error during delivery: {e}")


if __name__ == "__main__":
    main()
