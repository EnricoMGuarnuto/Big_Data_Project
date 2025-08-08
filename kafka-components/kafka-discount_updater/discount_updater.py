import os
import time
import random
import redis
import pandas as pd

# --- Config ---
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

# file prezzi: parquet o csv con colonne [Item_Identifier, Price]
PRICE_FILE = os.getenv("PRICE_FILE", "/data/prices.parquet")

# inventario per lista item (se già incluso nel file prezzi non serve, ma lo uso come fallback)
INVENTORY_FILE = os.getenv("INVENTORY_FILE", "/data/store_inventory_final.parquet")

# intervallo aggiornamento sconti (sec)
SLEEP = float(os.getenv("DISCOUNT_UPDATE_INTERVAL", 604800))  # 7 giorni in secondi
# quanti item scontare a giro
K_ITEMS = int(os.getenv("DISCOUNT_K_ITEMS", 10))

# range sconto [min, max] (0-1)
DISCOUNT_MIN = float(os.getenv("DISCOUNT_MIN", 0.05))
DISCOUNT_MAX = float(os.getenv("DISCOUNT_MAX", 0.30))

# --- Redis ---
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def load_prices_df(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv":
        df = pd.read_csv(path)
    else:
        # default: parquet
        df = pd.read_parquet(path)
    # normalizza nomi colonne
    cols = {c.lower(): c for c in df.columns}
    # mappa case-insensitive
    id_col = cols.get("item_identifier") or cols.get("item_id") or cols.get("sku")
    price_col = cols.get("price") or cols.get("unit_price")
    if not id_col or not price_col:
        raise ValueError(
            f"Nel file prezzi servono colonne Item_Identifier e Price (o equivalenti). Colonne trovate: {list(df.columns)}"
        )
    out = df[[id_col, price_col]].rename(columns={id_col: "Item_Identifier", price_col: "Price"})
    # cast
    out["Item_Identifier"] = out["Item_Identifier"].astype(str)
    out["Price"] = out["Price"].astype(float)
    return out

# --- Carica prezzi e pubblicali (una tantum) ---
prices = load_prices_df(PRICE_FILE)
# se serve la lista item per gli sconti:
try:
    inventory = pd.read_parquet(INVENTORY_FILE)
    item_ids = sorted(set(inventory["Item_Identifier"].astype(str)))
except Exception:
    # fallback: usa gli item presenti nel listino prezzi
    item_ids = sorted(set(prices["Item_Identifier"]))

# scrivi i prezzi in Redis (overwrite all'avvio, così sono sempre allineati al file)
for _, row in prices.iterrows():
    r.set(f"price:{row['Item_Identifier']}", round(float(row["Price"]), 2))

print(f"Discount updater started. Prezzi caricati: {len(prices)}. Item per sconto: {len(item_ids)}")

# --- Loop: aggiorna SOLO gli sconti ---
while True:
    k = min(K_ITEMS, len(item_ids))
    discounted_items = set(random.sample(item_ids, k=k)) if k > 0 else set()
    pipe = r.pipeline()
    for item_id in item_ids:
        if item_id in discounted_items:
            discount = round(random.uniform(DISCOUNT_MIN, DISCOUNT_MAX), 2)
        else:
            discount = 0.0
        pipe.set(f"discount:{item_id}", discount)
    pipe.execute()
    print(f"Aggiornati sconti per {len(discounted_items)} item (range {DISCOUNT_MIN}-{DISCOUNT_MAX}).")
    time.sleep(SLEEP)
