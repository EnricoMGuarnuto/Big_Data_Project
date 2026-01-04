from pathlib import Path
import numpy as np
import pandas as pd


def _load_inventory(inventory_path: str) -> pd.DataFrame:
    inventory_path = str(inventory_path)
    if inventory_path.endswith(".parquet"):
        return pd.read_parquet(inventory_path)
    elif inventory_path.endswith(".csv"):
        return pd.read_csv(inventory_path)
    else:
        raise ValueError("inventory_path must be .parquet or .csv")


def generate_weekly_discounts(
    inventory_path: str = "data/store_inventory_final.parquet",
    out_path: str = "data/all_discounts.parquet",
    start_date: str = "2025-01-01",
    years: int = 3,
    discounts_per_week: int | None = None,
    discount_range: tuple[float, float] = (0.05, 0.35),  # 5% - 35%
    seed: int = 42,
) -> pd.DataFrame:
    inv = _load_inventory(inventory_path).copy()

    required_cols = {"shelf_id", "item_price"}
    missing = required_cols - set(inv.columns)
    if missing:
        raise ValueError(f"Missing columns in inventory: {missing}")

    inv["shelf_id"] = inv["shelf_id"].astype(str)
    inv["item_price"] = pd.to_numeric(inv["item_price"], errors="coerce")

    shelf_ids = inv["shelf_id"].dropna().unique().tolist()
    if not shelf_ids:
        raise ValueError("No shelf_id found in inventory.")

    price_map = inv.drop_duplicates("shelf_id").set_index("shelf_id")["item_price"].to_dict()

    n_products = len(shelf_ids)
    if discounts_per_week is None:
        # default: 5% dei prodotti a settimana (min 1)
        discounts_per_week = max(1, int(round(n_products * 0.05)))

    if discounts_per_week > n_products:
        discounts_per_week = n_products

    rng = np.random.default_rng(seed)

    # allinea start_date al lunedì della sua settimana
    start = pd.to_datetime(start_date)
    start_monday = start - pd.Timedelta(days=start.weekday())

    n_weeks = years * 52
    week_starts = pd.date_range(start_monday, periods=n_weeks, freq="W-MON")

    # rotazione: ordine casuale + puntatore
    order = rng.permutation(shelf_ids).tolist()
    ptr = 0

    rows = []
    for week_idx, ws in enumerate(week_starts):
        we = ws + pd.Timedelta(days=6)

        chosen = []
        while len(chosen) < discounts_per_week:
            remaining = len(order) - ptr
            take = min(discounts_per_week - len(chosen), remaining)
            chosen.extend(order[ptr:ptr + take])
            ptr += take

            if ptr >= len(order):
                order = rng.permutation(shelf_ids).tolist()
                ptr = 0

        # sconto random per prodotto scelto
        discount_pcts = rng.uniform(discount_range[0], discount_range[1], size=len(chosen))

        for sid, dp in zip(chosen, discount_pcts):
            base_price = float(price_map.get(sid, np.nan))
            discounted_price = round(base_price * (1 - float(dp)), 2) if pd.notna(base_price) else np.nan

            rows.append({
                "week_index": int(week_idx),
                "week_start": ws.normalize(),
                "week_end": we.normalize(),
                "shelf_id": sid,
                "discount_pct": float(round(dp, 4)),   # es. 0.1532
                "original_price": base_price,
                "discounted_price": discounted_price,
            })

    discounts_df = pd.DataFrame(rows)

    # salva parquet
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    discounts_df.to_parquet(out_path, index=False)

    return discounts_df


if __name__ == "__main__":
    df = generate_weekly_discounts(
        inventory_path="data/store_inventory_final.parquet",
        out_path="data/all_discounts.parquet",
        start_date="2025-01-01",
        years=3,
        discounts_per_week=60,           # <-- QUI scegli "un tot" fisso
        discount_range=(0.05, 0.35),
        seed=42,
    )
    print(f"Saved discounts: {len(df):,} rows -> data/all_discounts.parquet")
    print(df.head())
