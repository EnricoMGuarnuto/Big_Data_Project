import pandas as pd
import numpy as np

# Load store inventory CSV
store_df = pd.read_csv('dataset_changes/store_inventory_final.csv')

# Create warehouse inventory by increasing initial_stock
warehouse_df = store_df.copy()

# Apply a random increase between 10% and 50%
warehouse_df['initial_stock'] = (warehouse_df['initial_stock'] * np.random.uniform(1.1, 1.5, size=len(warehouse_df))).round()

# Set current_stock = initial_stock
warehouse_df['current_stock'] = warehouse_df['initial_stock']

# save to csv
warehouse_df.to_csv('dataset_changes/warehouse_inventory_final.csv', index=False)

