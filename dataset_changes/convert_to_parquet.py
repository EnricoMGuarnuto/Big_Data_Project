import pandas as pd

# Lista dei file CSV
csv_files = [
    'store_inventory_final.csv',
    'warehouse_inventory_final.csv',
    'warehouse_batches.csv',
    'store_batches.csv'
]

# Convertili tutti
for file in csv_files:
    df = pd.read_csv(file)
    parquet_file = file.replace('.csv', '.parquet')
    df.to_parquet(parquet_file, index=False)
    print(f"Converted {file} -> {parquet_file}")
