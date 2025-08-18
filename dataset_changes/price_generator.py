#!/usr/bin/env python3
import os
import argparse
import numpy as np
import pandas as pd

def main():
    ap = argparse.ArgumentParser(description="Aggiunge/rigenera la colonna 'price' nel CSV di store inventory (in-place).")
    ap.add_argument("--input", default=os.environ.get("STORE_FILE", "/dataset_changes/store_inventory_final.csv"),
                    help="Percorso al CSV di inventario (default: dataset_changes/store_inventory_final.csv)")
    ap.add_argument("--seed", type=int, default=int(os.environ.get("SEED", "42")),
                    help="Seed RNG per prezzi riproducibili (default: 42)")
    ap.add_argument("--min-price", type=float, default=float(os.environ.get("MIN_PRICE", "1.0")),
                    help="Prezzo minimo (default: 1.0)")
    ap.add_argument("--max-price", type=float, default=float(os.environ.get("MAX_PRICE", "25.0")),
                    help="Prezzo massimo (default: 25.0)")
    ap.add_argument("--overwrite", action="store_true",
                    help="Se 'price' esiste già, la rigenera.")
    ap.add_argument("--delimiter", default=os.environ.get("CSV_DELIMITER", ","),
                    help="Delimitatore CSV (default: ',').")
    args = ap.parse_args()

    in_path = args.input
    if not os.path.exists(in_path):
        raise FileNotFoundError(f"CSV non trovato: {in_path}")

    df = pd.read_csv(in_path, sep=args.delimiter)

    if "shelf_id" not in df.columns:
        raise ValueError("Il CSV deve contenere la colonna 'shelf_id'.")

    if "price" in df.columns and not args.overwrite:
        print("[add-prices] La colonna 'price' esiste già. Usa --overwrite per rigenerarla. Nessuna modifica.")
        return

    # Prezzo riproducibile per ITEM (stesso prezzo per stesso shelf_id)
    rng = np.random.default_rng(args.seed)
    uniques = df["shelf_id"].astype(str).unique()
    prices = np.round(rng.uniform(low=args.min_price, high=args.max_price, size=len(uniques)), 2)
    price_map = dict(zip(uniques, prices))

    df["price"] = df["shelf_id"].astype(str).map(price_map).astype(float)

    df.to_csv(in_path, index=False, sep=args.delimiter)
    print(f"[add-prices] Colonna 'price' scritta in {in_path}. righe={len(df)}, item_unici={len(uniques)}, seed={args.seed}")

if __name__ == "__main__":
    main()
