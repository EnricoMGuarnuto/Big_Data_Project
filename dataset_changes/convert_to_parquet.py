import pandas as pd

# Lista dei file CSV
# csv_files = [
#    'dataset_changes/store_inventory_final.csv',
#    'dataset_changes/warehouse_inventory_final.csv',
#    'dataset_changes/warehouse_batches.csv',
#    'dataset_changes/store_batches.csv'
#]

csv_files= ['data/store_inventory_final.csv']
# Convertili tutti
for file in csv_files:
    df = pd.read_csv(file)
    parquet_file = file.replace('.csv', '.parquet')
    df.to_parquet(parquet_file, index=False)
    print(f"Converted {file} -> {parquet_file}")
