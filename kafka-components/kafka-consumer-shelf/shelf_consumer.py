import os
import json
import pandas as pd
from pathlib import Path
from kafka import KafkaConsumer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_SHELF = os.getenv("TOPIC_SHELF_EVENTS", "shelf_events")
TOPIC_WH = os.getenv("TOPIC_WAREHOUSE_EVENTS", "warehouse_events")

# input bootstrap parquet (to get batch meta when needed)
WAREHOUSE_BOOTSTRAP = os.getenv("WH_BATCHES_PARQUET", "/data/warehouse_batches.parquet")

# materialized outputs (do NOT overwrite base parquet)
OUT_DIR = Path(os.getenv("OUT_DIR", "/data/materialized"))
OUT_DIR.mkdir(parents=True, exist_ok=True)
STORE_OUT = OUT_DIR / "store_batches_state.parquet"
WH_OUT = OUT_DIR / "warehouse_batches_state.parquet"

# Load bootstrap warehouse meta (Batch_ID, Item_Identifier, Expiry_Date)
wh_boot = pd.read_parquet(WAREHOUSE_BOOTSTRAP)[["Batch_ID","Item_Identifier","Expiry_Date"]].drop_duplicates()

# Initialize state files if missing
if STORE_OUT.exists():
    store_state = pd.read_parquet(STORE_OUT)
else:
    store_state = pd.DataFrame(columns=["Batch_ID","Item_Identifier","Expiry_Date","Batch_Quantity"])

if WH_OUT.exists():
    wh_state = pd.read_parquet(WH_OUT)
    # ensure consistent dtypes
    if "Batch_Quantity" not in wh_state.columns:
        wh_state["Batch_Quantity"] = 0
else:
    # start from bootstrap quantities = 0 (we’ll only reflect picks here)
    wh_state = wh_boot.merge(
        pd.DataFrame(columns=["Batch_ID","Batch_Quantity"]),
        on="Batch_ID", how="left"
    )
    wh_state["Batch_Quantity"] = 0

consumer = KafkaConsumer(
    TOPIC_SHELF, TOPIC_WH,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id=os.getenv("GROUP_ID","shelf-consumer-materializer")
)

print(f"[shelf-consumer] Listening on: {TOPIC_SHELF}, {TOPIC_WH}")

def upsert_store_batch(batch_id: str, item_id: str, qty: int):
    global store_state
    # find batch meta
    meta = wh_boot[wh_boot["Batch_ID"] == batch_id]
    if meta.empty:
        # batch not in bootstrap (shouldn’t happen in this toy flow), skip
        return
    expiry = meta.iloc[0]["Expiry_Date"]
    item_meta = meta.iloc[0]["Item_Identifier"]

    if (store_state["Batch_ID"] == batch_id).any():
        store_state.loc[store_state["Batch_ID"] == batch_id, "Batch_Quantity"] += qty
    else:
        new_row = pd.DataFrame([{
            "Batch_ID": batch_id,
            "Item_Identifier": item_meta,
            "Expiry_Date": expiry,
            "Batch_Quantity": qty
        }])
        store_state = pd.concat([store_state, new_row], ignore_index=True)

def dec_wh_batch(batch_id: str, qty: int):
    global wh_state
    if (wh_state["Batch_ID"] == batch_id).any():
        wh_state.loc[wh_state["Batch_ID"] == batch_id, "Batch_Quantity"] -= qty
    else:
        # initialize record with negative picked qty (we only track deltas here)
        meta = wh_boot[wh_boot["Batch_ID"] == batch_id]
        if meta.empty:
            return
        new_row = meta.copy()
        new_row["Batch_Quantity"] = -qty
        wh_state = pd.concat([wh_state, new_row], ignore_index=True)

for message in consumer:
    evt = message.value
    etype = evt.get("event_type")

    if etype == "restock":
        batch_id = evt.get("batch_id")
        item_id = evt.get("item_id")
        qty = int(evt.get("quantity", 0))
        if batch_id and qty > 0:
            upsert_store_batch(batch_id, item_id, qty)
            store_state.to_parquet(STORE_OUT, index=False)
            print(f"[shelf-consumer] store batch {batch_id} +{qty}")

    elif etype == "warehouse_pick":
        batch_id = evt.get("batch_id")
        qty = int(evt.get("quantity_picked", 0))
        if batch_id and qty > 0:
            dec_wh_batch(batch_id, qty)
            wh_state.to_parquet(WH_OUT, index=False)
            print(f"[shelf-consumer] warehouse batch {batch_id} -{qty}")

    # ignore pickup/putback here (they're sensor-level events)
