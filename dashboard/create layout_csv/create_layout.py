# tools/make_layout.py
import pandas as pd
import math
from pathlib import Path

# ======= CONFIG =======
INPUT_CSV  = "data/store_inventory_final.csv"   # il tuo file sorgente
OUTPUT_CSV = "layout.csv"

ITEMS_PER_AISLE = 30

ORDER_BY_COL = "shelf_id"

TILE_W = 1.0
TILE_H = 0.7
GAP_X  = 0.2
GAP_Y  = 0.15
AISLE_GAP_X = 0.6
ZONE_GAP_X  = 2.0

ZONE_MAP = {
    "🥬 Produce": [
        "Fruits and Vegetables",
    ],
    "🥶 Fresh & Fridge": [
        "Frozen Foods", "Dairy", "Meat", "Seafood", "Ready Meals",
    ],
    "🥖 Bakery": [
        "Breads",
    ],
    "🥣 Breakfast & Starchy": [
        "Breakfast", "Starchy Foods",
    ],
    "🥫 Pantry": [
        "Canned", "Baking Goods", "Spices and Condiments",
    ],
    "🍫 Snacks & Sweets": [
        "Snack Foods", "Sweets",
    ],
    "🥤 Drinks": [
        "Soft Drinks", "Hard Drinks", "Hot Beverages",
    ],
    "👶 Care & Family": [
        "Health and Hygiene", "Baby Products", "Pet Products",
    ],
    "🌾 Gluten Free": [
        "Gluten Free",
    ],
}

ZONE_ORDER = [
    "🥬 Produce",
    "🥶 Fresh & Fridge",
    "🥖 Bakery",
    "🥣 Breakfast & Starchy",
    "🥫 Pantry",
    "🍫 Snacks & Sweets",
    "🥤 Drinks",
    "👶 Care & Family",
    "🌾 Gluten Free",
    "📦 Other",
]

def normalize(s: str) -> str:
    return (s or "").strip().casefold()

CAT2ZONE = {}
for zone, cats in ZONE_MAP.items():
    for c in cats:
        CAT2ZONE[normalize(c)] = zone

# ======= LOAD =======
usecols = ["shelf_id", "item_category"]
df = pd.read_csv(INPUT_CSV, usecols=usecols)

df = df.dropna(subset=["shelf_id"]).copy()
df["shelf_id"] = df["shelf_id"].astype(str)
df["item_category"] = df["item_category"].astype(str)

df = df.sort_values(["shelf_id"]).drop_duplicates(subset=["shelf_id"], keep="first")

df["zone"] = df["item_category"].map(lambda x: CAT2ZONE.get(normalize(x), "📦 Other"))

# ======= LAYOUT =======
rows = []
x_offset = 0.0

def zone_sort_key(z):
    return ZONE_ORDER.index(z) if z in ZONE_ORDER else len(ZONE_ORDER)

for zone in sorted(df["zone"].unique(), key=zone_sort_key):
    g = df[df["zone"] == zone].sort_values(ORDER_BY_COL).reset_index(drop=True)
    n = len(g)
    if n == 0:
        continue

    aisles = math.ceil(n / ITEMS_PER_AISLE)

    for i, row in g.iterrows():
        aisle_idx = i // ITEMS_PER_AISLE
        pos_in_aisle = i % ITEMS_PER_AISLE

        x = x_offset + aisle_idx * (TILE_W + GAP_X + AISLE_GAP_X)
        y = pos_in_aisle * (TILE_H + GAP_Y)

        rows.append({
            "shelf_id": row["shelf_id"],
            "x": round(x, 4),
            "y": round(y, 4),
            "w": TILE_W,
            "h": TILE_H,
            "label": f"{row['shelf_id']} ({row['item_category']})",
            "zone": zone,
            "aisle": str(aisle_idx + 1),
        })

    # spazio tra zone
    x_offset += aisles * (TILE_W + GAP_X + AISLE_GAP_X) + ZONE_GAP_X

layout = pd.DataFrame(rows)

# ======= SAVE =======
if layout.empty:
    raise ValueError("⚠️ Layout vuoto! Controlla se le categorie in store_inventory_final.csv corrispondono a quelle in ZONE_MAP.")

layout.to_csv(OUTPUT_CSV, index=False)
print(f"✅ Creato {OUTPUT_CSV} con {len(layout)} righe.")
print(layout.groupby('zone').size().sort_values(ascending=False).to_string())
