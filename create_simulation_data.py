import os
import math
import numpy as np
import pandas as pd
from datetime import date, timedelta

# ============================================================
# CONFIG
# ============================================================
SEED = 42
np.random.seed(SEED)

START_DATE = date(2025, 1, 1)
N_DAYS = 365

# Giorni inbound (fornitore -> warehouse) FISSI: Martedì=1, Venerdì=4
WH_INBOUND_DOW = {1, 4}

# Categorie deperibili
PERISHABLE_CATEGORIES = {'FRUITS AND VEGETABLES', 'REFRIGERATED', 'FROZEN', 'MEAT', 'FISH'}

# Policy "corrente" (per generare alert/refill coerenti)
SHELF_THRESHOLD_PCT = 0.18      # alert shelf se stock < 18% capacità
SHELF_TARGET_PCT = 0.80         # refill shelf fino all'80%
WH_COVER_DAYS_REORDER = 10      # reorder point: giorni copertura
WH_ORDER_COVER_DAYS = 14        # quando ordini, punti a ~14 giorni copertura

# Promo sintetiche
PROMO_PROB = 0.08
PROMO_LEVELS = [0.10, 0.15, 0.20, 0.25]
PROMO_UPLIFT = 1.35

# Domanda: moltiplicatori categoria
CATEGORY_DEMAND_MULT = {
    "BEVERAGES": 1.15,
    "BEERS": 1.05,
    "WINES": 0.85,
    "SPIRITS AND APERITIFS": 0.55,
    "FRUITS AND VEGETABLES": 1.20,
    "REFRIGERATED": 1.00,
    "FROZEN": 0.75,
    "MEAT": 0.85,
    "FISH": 0.60,
    "BATH AND PERSONAL CARE": 0.50,
    "HOUSEHOLD GOODS": 0.45,
    "LAUNDRY PRODUCTS": 0.40,
    "HOUSE CLEANING PRODUCTS": 0.45,
    "BABY PRODUCTS": 0.35,
    "SWEETS AND BREAKFAST": 0.95,
    "PASTA AND RICE": 0.80,
    "FLOURS AND SUGAR": 0.60,
    "HOT BEVERAGES": 0.55,
    "SAUCES": 0.55,
    "PASTA SAUCES": 0.65,
    "SPICES": 0.35,
    "VINEGAR AND OIL": 0.45,
    "SAVORY SNACKS": 0.90,
    "PET PRODUCTS": 0.50,
    "CANNED GOODS": 0.70,
    "LONG-LIFE DAIRY": 0.75,
    "LONG-LIFE FRUIT": 0.55,
    "GLUTEN FREE": 0.40,
    "BREAD AND BREADSTICKS": 0.70
}
DEFAULT_MULT = 0.60


# ============================================================
# HELPERS: stagionalità
# ============================================================
def seasonality_multiplier(d: date) -> float:
    if d.month in (6, 7, 8):
        return 1.10
    if d.month == 12:
        return 1.15
    return 1.0

def weekend_multiplier(dow: int) -> float:
    return 1.10 if dow in (5, 6) else 1.0


# ============================================================
# HELPERS: batch FEFO
# batches: list of dict {batch_code(str), expiry_date(date), qty(int)}
# ============================================================
def sort_batches(batches):
    batches.sort(key=lambda b: (b["expiry_date"], b["batch_code"]))

def total_qty(batches) -> int:
    return int(sum(b["qty"] for b in batches))

def expire_batches(batches, today: date) -> int:
    expired = 0
    keep = []
    for b in batches:
        if b["expiry_date"] < today and b["qty"] > 0:
            expired += int(b["qty"])
        else:
            keep.append(b)
    batches[:] = keep
    sort_batches(batches)
    return int(expired)

def consume_fefo(batches, qty_needed: int) -> int:
    if qty_needed <= 0:
        return 0
    sort_batches(batches)
    consumed = 0
    i = 0
    while qty_needed > 0 and i < len(batches):
        take = min(int(batches[i]["qty"]), int(qty_needed))
        batches[i]["qty"] -= take
        qty_needed -= take
        consumed += take
        if batches[i]["qty"] <= 0:
            i += 1
        else:
            break
    batches[:] = [b for b in batches if b["qty"] > 0]
    sort_batches(batches)
    return int(consumed)

def transfer_fefo(src_batches, dst_batches, qty: int, new_batch_code_fn) -> int:
    if qty <= 0:
        return 0
    moved = 0
    sort_batches(src_batches)

    while qty > 0 and src_batches:
        b = src_batches[0]
        take = min(int(b["qty"]), int(qty))
        b["qty"] -= take
        qty -= take
        moved += take

        dst_batches.append({
            "batch_code": new_batch_code_fn(),
            "expiry_date": b["expiry_date"],
            "qty": int(take)
        })

        if b["qty"] <= 0:
            src_batches.pop(0)

    src_batches[:] = [b for b in src_batches if b["qty"] > 0]
    sort_batches(src_batches)
    sort_batches(dst_batches)
    return int(moved)


# ============================================================
# HELPERS: discount fixed loader
# ============================================================


def load_fixed_discounts(all_discounts_path: str) -> pd.DataFrame:
    """
    Carica all_discounts.parquet in formato SETTIMANALE:
      week_start (epoch ms), week_end (epoch ms), shelf_id, discount_pct, ...
    e lo espande a formato GIORNALIERO:
      shelf_id | discount_date | discount
    """
    df = pd.read_parquet(all_discounts_path)

    required = {"week_start", "week_end", "shelf_id", "discount_pct"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"all_discounts.parquet: mancano colonne {missing}. Colonne presenti: {list(df.columns)}")

    # Converti epoch ms -> date (UTC)
    df = df.copy()
    df["week_start_date"] = pd.to_datetime(df["week_start"], unit="ms", utc=True).dt.date
    df["week_end_date"]   = pd.to_datetime(df["week_end"],   unit="ms", utc=True).dt.date

    # Normalizza discount_pct a [0,1] se qualcuno ha messo 20 invece di 0.20
    df["discount_pct"] = df["discount_pct"].astype(float)
    df.loc[df["discount_pct"] > 1.0, "discount_pct"] = df.loc[df["discount_pct"] > 1.0, "discount_pct"] / 100.0

    out_rows = []
    for _, r in df.iterrows():
        start = r["week_start_date"]
        end = r["week_end_date"]

        # Alcuni dataset hanno week_end “inclusivo” (diff=6), altri “esclusivo” (diff=7).
        delta = (end - start).days

        if delta == 6:
            # inclusivo: start..end (7 giorni)
            dates = pd.date_range(start, end, freq="D")
        elif delta == 7:
            # esclusivo: start..(end-1) (7 giorni)
            dates = pd.date_range(start, end - timedelta(days=1), freq="D")
        else:
            # fallback: prova inclusivo (meglio di rompere tutto)
            dates = pd.date_range(start, end, freq="D")

        for d in dates:
            out_rows.append({
                "shelf_id": r["shelf_id"],
                "discount_date": d.date(),
                "discount": float(r["discount_pct"])
            })

    out = pd.DataFrame(out_rows)

    # Se ci sono sovrapposizioni (stesso shelf e stesso giorno), tieni lo sconto massimo
    out = out.groupby(["shelf_id", "discount_date"], as_index=False)["discount"].max()
    return out



# ============================================================
# HELPERS: expiry sampling (coerente col tuo generatore)
# ============================================================
def sample_expiry_days(is_perishable: bool, batch_index_hint: int = 0) -> int:
    if is_perishable:
        max_days = 15
        min_days = min(max_days, 2 + batch_index_hint)
        return int(np.random.randint(min_days, max_days + 1))
    min_days = 365 + 100 * batch_index_hint
    max_days = min_days + 365
    return int(np.random.randint(min_days, max_days + 1))


# ============================================================
# Demand model
# ============================================================
def base_daily_demand(row) -> float:
    cap = int(row["shelf_capacity"])
    price = float(row["item_price"])
    cat = row["item_category"]

    mult = CATEGORY_DEMAND_MULT.get(cat, DEFAULT_MULT)
    cap_component = 0.03 * cap
    price_factor = 1.0 / (1.0 + 0.10 * max(price - 2.0, 0.0))

    return max(0.2, cap_component * price_factor * mult)

def quantize_to_batch(qty: int, batch_size: int) -> int:
    if qty <= 0:
        return 0
    return int(math.ceil(qty / batch_size)) * batch_size


# ============================================================
# MAIN: simulate
# ============================================================
def simulate_year(
    store_inv_path="data/store_inventory_final.parquet",
    wh_inv_path="data/warehouse_inventory_final.parquet",
    store_batches_path="data/store_batches.parquet",
    wh_batches_path="data/warehouse_batches.parquet",
    all_discounts_path="data/all_discounts.parquet",
    out_dir="data/sim_out",
    discount_combine_mode="max",   # "max" oppure "add"
):
    os.makedirs(out_dir, exist_ok=True)

    # ---------- Load snapshots ----------
    store_inv = pd.read_parquet(store_inv_path)
    wh_inv = pd.read_parquet(wh_inv_path)
    store_batches_df = pd.read_parquet(store_batches_path)
    wh_batches_df = pd.read_parquet(wh_batches_path)

    # rinomina coerente
    store_inv = store_inv.rename(columns={"maximum_stock": "shelf_capacity", "current_stock": "stock_shelf"})
    wh_inv = wh_inv.rename(columns={"maximum_stock": "warehouse_capacity", "current_stock": "stock_warehouse"})

    # master prodotti (shelf_id dai dataset)
    products = store_inv[[
        "shelf_id", "aisle", "item_weight", "shelf_weight",
        "item_category", "item_subcategory", "item_visibility",
        "shelf_capacity", "stock_shelf", "item_price"
    ]].merge(
        wh_inv[["shelf_id", "warehouse_capacity", "stock_warehouse"]],
        on="shelf_id",
        how="left"
    )

    products["warehouse_capacity"] = products["warehouse_capacity"].fillna(0).astype(int)
    products["stock_warehouse"] = products["stock_warehouse"].fillna(0).astype(int)

    # standard_batch_size dai parquet batches
    batch_size_map = (
        pd.concat([
            store_batches_df[["shelf_id", "standard_batch_size"]],
            wh_batches_df[["shelf_id", "standard_batch_size"]],
        ], axis=0)
        .dropna()
        .groupby("shelf_id")["standard_batch_size"].max()
        .astype(int)
        .to_dict()
    )
    products["standard_batch_size"] = products["shelf_id"].map(batch_size_map).fillna(24).astype(int)

    # ---------- Build batch state in-memory ----------
    def build_batches(df, qty_col):
        out = {}
        for sid, g in df.groupby("shelf_id"):
            batches = []
            for _, r in g.iterrows():
                q = int(r[qty_col])
                if q <= 0:
                    continue
                batches.append({
                    "batch_code": str(r["batch_code"]),
                    "expiry_date": pd.to_datetime(r["expiry_date"]).date(),
                    "qty": int(q)
                })
            sort_batches(batches)
            out[sid] = batches
        return out

    shelf_batches = build_batches(store_batches_df, "batch_quantity_store")
    wh_batches = build_batches(wh_batches_df, "batch_quantity_warehouse")

    # assicurati che esistano le chiavi
    for sid in products["shelf_id"].tolist():
        shelf_batches.setdefault(sid, [])
        wh_batches.setdefault(sid, [])

    # ---------- Calendar ----------
    all_dates = [START_DATE + timedelta(days=i) for i in range(N_DAYS)]

    # fixed discounts
    fixed_df = load_fixed_discounts(all_discounts_path)
    fixed_lookup = {(r["shelf_id"], r["discount_date"]): float(r["discount"]) for _, r in fixed_df.iterrows()}

    # promo synthetic
    promo_map = {}
    for sid in products["shelf_id"].tolist():
        arr = np.zeros(N_DAYS, dtype=float)
        promo_days = (np.random.rand(N_DAYS) < PROMO_PROB)
        arr[promo_days] = np.random.choice(PROMO_LEVELS, size=int(promo_days.sum()))
        promo_map[sid] = arr

    # esporta analytics.daily_discounts usando sconto effettivo
    dd_rows = []
    shelf_ids = products["shelf_id"].tolist()

    for t, d in enumerate(all_dates):
        for sid in shelf_ids:
            promo = float(promo_map[sid][t])
            fixed = float(fixed_lookup.get((sid, d), 0.0))

            if discount_combine_mode == "add":
                eff = min(0.90, promo + fixed)
            else:
                eff = max(promo, fixed)

            if eff > 0:
                dd_rows.append({
                    "shelf_id": sid,
                    "discount_date": d.isoformat(),
                    "discount": eff
                })
    pd.DataFrame(dd_rows).to_csv(os.path.join(out_dir, "analytics_daily_discounts.csv"), index=False)

    # ---------- Demand EWMA ----------
    demand_ewma = {r["shelf_id"]: base_daily_demand(r) for _, r in products.iterrows()}

    # ---------- Pending supplier orders (PIECES) ----------
    pending_supplier_orders = {sid: 0 for sid in products["shelf_id"].tolist()}

    # batch code generator
    batch_counter = 10_000_000
    def new_batch_code():
        nonlocal batch_counter
        batch_counter += 1
        return f"B{batch_counter:07d}"

    # rolling alerts history
    shelf_alert_hist = {sid: [] for sid in products["shelf_id"].tolist()}
    wh_alert_hist = {sid: [] for sid in products["shelf_id"].tolist()}

    rows = []

    # ============================================================
    # SIM LOOP
    # ============================================================
    for t in range(N_DAYS):
        d = all_dates[t]
        dow = d.weekday()
        is_weekend = 1 if dow in (5, 6) else 0
        warehouse_inbound_day = 1 if dow in WH_INBOUND_DOW else 0

        # traffico (unico PV)
        base_traffic = 1100 + 120 * is_weekend
        people_count = int(max(250, np.random.normal(base_traffic * seasonality_multiplier(d), 70)))

        # ---------- 1) inbound supplier -> warehouse (solo Mar/Ven) ----------
        if warehouse_inbound_day == 1:
            for _, pr in products.iterrows():
                sid = pr["shelf_id"]
                cat = pr["item_category"]
                is_perishable = cat in PERISHABLE_CATEGORIES
                order_qty = int(pending_supplier_orders[sid])
                if order_qty <= 0:
                    continue

                shelf_cap = int(pr["shelf_capacity"])
                wh_cap = int(pr["warehouse_capacity"])

                # Perishables: consegna diretta allo shelf (altrimenti muoiono)
                if is_perishable or wh_cap <= 0:
                    # rispetta capacità shelf (spazio disponibile)
                    shelf_now = total_qty(shelf_batches[sid])
                    free_shelf = max(0, shelf_cap - shelf_now)
                    delivered = min(order_qty, free_shelf)
                    if delivered > 0:
                        exp_days = sample_expiry_days(True, 0)
                        shelf_batches[sid].append({
                            "batch_code": new_batch_code(),
                            "expiry_date": d + timedelta(days=exp_days),
                            "qty": int(delivered)
                        })
                        sort_batches(shelf_batches[sid])
                    pending_supplier_orders[sid] = order_qty - delivered
                    continue

                # Non-perishables: consegna in warehouse (rispetta capacità)
                wh_now = total_qty(wh_batches[sid])
                free_wh = max(0, wh_cap - wh_now)
                delivered = min(order_qty, free_wh)
                if delivered > 0:
                    exp_days = sample_expiry_days(False, 0)
                    wh_batches[sid].append({
                        "batch_code": new_batch_code(),
                        "expiry_date": d + timedelta(days=exp_days),
                        "qty": int(delivered)
                    })
                    sort_batches(wh_batches[sid])
                pending_supplier_orders[sid] = order_qty - delivered

        # ---------- 2) per ogni SKU: scadenze, vendite, refill, decisione ordine ----------
        for _, pr in products.iterrows():
            sid = pr["shelf_id"]
            cat = pr["item_category"]
            sub = pr["item_subcategory"]
            is_perishable = cat in PERISHABLE_CATEGORIES

            shelf_cap = int(pr["shelf_capacity"])
            wh_cap = int(pr["warehouse_capacity"])
            batch_size = int(pr["standard_batch_size"])
            price = float(pr["item_price"])

            # scadenze
            expired_shelf = expire_batches(shelf_batches[sid], d)
            expired_wh = expire_batches(wh_batches[sid], d)

            stock_shelf_before = total_qty(shelf_batches[sid])
            stock_wh_before = total_qty(wh_batches[sid])

            # sconto effettivo (fixed + promo)
            promo = float(promo_map[sid][t])
            fixed = float(fixed_lookup.get((sid, d), 0.0))
            if discount_combine_mode == "add":
                discount_today = min(0.90, promo + fixed)
            else:
                discount_today = max(promo, fixed)

            is_discounted = 1 if discount_today > 0 else 0

            # next 7d effettivo
            look_end = min(N_DAYS, t + 7)
            is_discounted_next_7d = 0
            for tt in range(t, look_end):
                dd = all_dates[tt]
                promo2 = float(promo_map[sid][tt])
                fixed2 = float(fixed_lookup.get((sid, dd), 0.0))
                eff2 = (min(0.90, promo2 + fixed2) if discount_combine_mode == "add" else max(promo2, fixed2))
                if eff2 > 0:
                    is_discounted_next_7d = 1
                    break

            # domanda -> vendite (censurate)
            lam = float(demand_ewma[sid])
            lam *= seasonality_multiplier(d) * weekend_multiplier(dow)
            lam *= (1.0 + 0.00035 * (people_count - 1100))
            if is_discounted:
                lam *= PROMO_UPLIFT

            demand = int(np.random.poisson(max(lam, 0.1)))
            sales_qty = min(demand, stock_shelf_before)
            consume_fefo(shelf_batches[sid], sales_qty)
            stock_shelf_after_sales = total_qty(shelf_batches[sid])

            # aggiorna ewma su vendite osservate
            demand_ewma[sid] = 0.90 * demand_ewma[sid] + 0.10 * sales_qty

            # soglie
            shelf_threshold_qty = int(math.ceil(SHELF_THRESHOLD_PCT * shelf_cap))
            wh_reorder_point_qty = int(math.ceil(demand_ewma[sid] * WH_COVER_DAYS_REORDER))

            is_shelf_alert = 1 if stock_shelf_after_sales < shelf_threshold_qty else 0
            is_warehouse_alert = 0
            if (not is_perishable) and wh_cap > 0:
                is_warehouse_alert = 1 if stock_wh_before < wh_reorder_point_qty else 0

            # refill shelf IMMEDIATO: se shelf alert, sposta da WH->SHELF nello stesso giorno
            moved_wh_to_shelf = 0
            refill_day = 0
            if (not is_perishable) and wh_cap > 0 and is_shelf_alert == 1:
                target_level = int(math.ceil(SHELF_TARGET_PCT * shelf_cap))
                need = max(0, target_level - stock_shelf_after_sales)
                movable = min(need, total_qty(wh_batches[sid]))
                if movable > 0:
                    moved_wh_to_shelf = transfer_fefo(
                        wh_batches[sid], shelf_batches[sid], movable, new_batch_code_fn=new_batch_code
                    )
                    refill_day = 1

            # decisione ordine fornitore: OGNI GIORNO, ma entra SOLO Mar/Ven (pending)
            batches_to_order = 0
            if (not is_perishable) and wh_cap > 0:
                wh_now = total_qty(wh_batches[sid])
                wh_future = wh_now + int(pending_supplier_orders[sid])
                if wh_future < wh_reorder_point_qty:
                    desired = int(math.ceil(demand_ewma[sid] * WH_ORDER_COVER_DAYS))
                    gap = max(0, desired - wh_future)
                    gap_q = quantize_to_batch(gap, batch_size)

                    free_future = max(0, wh_cap - wh_future)
                    add_qty = min(gap_q, free_future)

                    if add_qty > 0:
                        pending_supplier_orders[sid] += int(add_qty)
                        batches_to_order = int(add_qty // batch_size)

            # expiry features shelf
            exp_days_list = [(b["expiry_date"] - d).days for b in shelf_batches[sid] if b["qty"] > 0]
            min_expiration_days = int(min(exp_days_list)) if exp_days_list else 9999
            avg_expiration_days = float(np.mean(exp_days_list)) if exp_days_list else 9999.0
            qty_expiring_next_7d = int(sum(
                b["qty"] for b in shelf_batches[sid]
                if d <= b["expiry_date"] <= (d + timedelta(days=6))
            ))

            # rolling alerts last 30d
            shelf_alert_hist[sid].append(is_shelf_alert)
            wh_alert_hist[sid].append(is_warehouse_alert)
            a30_shelf = int(sum(shelf_alert_hist[sid][-30:-1])) if len(shelf_alert_hist[sid]) > 1 else 0
            a30_wh = int(sum(wh_alert_hist[sid][-30:-1])) if len(wh_alert_hist[sid]) > 1 else 0

            # salva riga
            curr_shelf = total_qty(shelf_batches[sid])
            curr_wh = total_qty(wh_batches[sid])

            rows.append({
                "shelf_id": sid,
                "item_category": cat,
                "item_subcategory": sub,

                "feature_date": d.isoformat(),
                "day_of_week": int(dow),
                "is_weekend": int(is_weekend),

                "warehouse_inbound_day": int(warehouse_inbound_day),
                "refill_day": int(refill_day),

                "item_price": float(price),
                "discount": float(discount_today),
                "is_discounted": int(is_discounted),
                "is_discounted_next_7d": int(is_discounted_next_7d),

                "people_count": int(people_count),
                "sales_qty": int(sales_qty),
                "stockout_events": int(1 if demand > stock_shelf_before else 0),

                "shelf_capacity": int(shelf_cap),
                "current_stock_shelf": int(curr_shelf),
                "shelf_fill_ratio": float(curr_shelf / shelf_cap) if shelf_cap > 0 else 0.0,

                "warehouse_capacity": int(wh_cap),
                "current_stock_warehouse": int(curr_wh),
                "warehouse_fill_ratio": float(curr_wh / wh_cap) if wh_cap > 0 else 0.0,

                "pending_supplier_qty": int(pending_supplier_orders[sid]),
                "standard_batch_size": int(batch_size),

                "min_expiration_days": int(min_expiration_days),
                "avg_expiration_days": float(avg_expiration_days),
                "qty_expiring_next_7d": int(qty_expiring_next_7d),

                "alerts_last_30d_shelf": int(a30_shelf),
                "alerts_last_30d_wh": int(a30_wh),

                "is_shelf_alert": int(is_shelf_alert),
                "is_warehouse_alert": int(is_warehouse_alert),

                # target (euristica) in batch
                "batches_to_order": int(batches_to_order),

                # debug
                "expired_qty_shelf": int(expired_shelf),
                "expired_qty_wh": int(expired_wh),
                "moved_wh_to_shelf": int(moved_wh_to_shelf),
                "shelf_threshold_qty": int(shelf_threshold_qty),
                "wh_reorder_point_qty": int(wh_reorder_point_qty),
            })

    panel = pd.DataFrame(rows)
    panel["feature_date"] = pd.to_datetime(panel["feature_date"])
    panel = panel.sort_values(["shelf_id", "feature_date"]).reset_index(drop=True)

    # Lag sales
    panel["sales_last_1d"] = panel.groupby("shelf_id")["sales_qty"].shift(1).fillna(0).astype(int)
    panel["sales_last_7d"] = (
        panel.groupby("shelf_id")["sales_qty"]
             .shift(1).rolling(7).sum().reset_index(level=0, drop=True)
             .fillna(0).astype(int)
    )
    panel["sales_last_14d"] = (
        panel.groupby("shelf_id")["sales_qty"]
             .shift(1).rolling(14).sum().reset_index(level=0, drop=True)
             .fillna(0).astype(int)
    )

    # Label next day stockout (schema analytics.features_store)
    panel["label_next_day_stockout"] = (
        panel.groupby("shelf_id")["stockout_events"].shift(-1).fillna(0).astype(int).astype(bool)
    )

    # Export
    panel_out = os.path.join(out_dir, "synthetic_1y_panel.csv")
    panel.to_csv(panel_out, index=False)

    features_store = panel[[
        "feature_date", "shelf_id", "people_count", "sales_qty", "stockout_events", "label_next_day_stockout"
    ]].rename(columns={"people_count": "foot_traffic"})

    feat_out = os.path.join(out_dir, "analytics_features_store.csv")
    features_store.to_csv(feat_out, index=False)

    print("Saved:", panel_out)
    print("Saved:", feat_out)
    print("Saved:", os.path.join(out_dir, "analytics_daily_discounts.csv"))

    return panel, features_store


if __name__ == "__main__":
    simulate_year()
