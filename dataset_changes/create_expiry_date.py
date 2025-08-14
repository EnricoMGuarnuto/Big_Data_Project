import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# Load both warehouse and store inventory
warehouse_df_input = pd.read_csv('dataset_changes/warehouse_inventory_final.csv')
store_df_input = pd.read_csv('dataset_changes/store_inventory_final.csv')

# Expiry ranges
expiry_ranges = {
    'Fruits and Vegetables': (2, 7),
    'Dairy': (5, 15),
    'Meat': (5, 15),
    'Seafood': (5, 15),
    'Breads': (3, 10),
    'Frozen Foods': (10, 30),
    'Breakfast': (20, 60),
    'Snack Foods': (20, 60),
    'Sweets': (20, 60),
    'Canned': (90, 365),
    'Soft Drinks': (90, 365),
    'Hard Drinks': (90, 365),
    'Household': (180, 730),
    'Health and Hygiene': (180, 730),
    'Spices and Condiments': (180, 720),
    'Pasta and Rice': (180, 720),
    'Ready Meals': (30, 90),
    'Gluten Free': (60, 180),
    'Hot Beverages': (90, 365),
    'Pet Products': (90, 365),
    'Baby Products': (60, 180),
    'Starchy Foods': (60, 180),
    'Baking Goods': (60, 180)
}

warehouse_batches = []
store_batches = []

# Merge on Item_Identifier to ensure alignment
merged_df = pd.merge(
    warehouse_df_input[['Item_Identifier', 'Item_Type', 'current_stock']],
    store_df_input[['Item_Identifier', 'current_stock']],
    on='Item_Identifier',
    suffixes=('_warehouse', '_store')
)

# Generate batches
for _, row in merged_df.iterrows():
    item_id = row['Item_Identifier']
    item_type = row['Item_Type']
    stock_warehouse = int(row['current_stock_warehouse'])
    stock_store = int(row['current_stock_store'])

    # Randomly choose number of batches (1 or 2)
    num_batches = random.choice([1, 2])

    # Split warehouse and store stock across same batch IDs
    warehouse_parts = np.random.multinomial(stock_warehouse, np.random.dirichlet(np.ones(num_batches), size=1)[0])
    store_parts = np.random.multinomial(stock_store, np.random.dirichlet(np.ones(num_batches), size=1)[0])

    # Define expiry range
    min_days, max_days = expiry_ranges.get(item_type, (60, 180))

    for i in range(num_batches):
        batch_id = f'BATCH_{item_id}_{i+1}'
        warehouse_q = warehouse_parts[i]
        store_q = store_parts[i]
        batch_total = warehouse_q + store_q

        expiry_date = datetime.now() + timedelta(days=random.randint(min_days, max_days))
        received_days_ago = random.randint(0, 10)
        received_date = datetime.now() - timedelta(days=received_days_ago)

        # Warehouse row
        warehouse_batches.append({
            'Item_Identifier': item_id,
            'Batch_ID': batch_id,
            'Received_Date': received_date.strftime('%Y-%m-%d'),
            'Expiry_Date': expiry_date.strftime('%Y-%m-%d'),
            'Batch_Quantity_Total': batch_total,
            'Batch_Quantity_Warehouse': warehouse_q,
            'Batch_Quantity_Store': 0,
            'Location': 'Warehouse'
        })

        # Store row
        store_batches.append({
            'Item_Identifier': item_id,
            'Batch_ID': batch_id,
            'Received_Date': datetime.now().strftime('%Y-%m-%d'),
            'Expiry_Date': expiry_date.strftime('%Y-%m-%d'),
            'Batch_Quantity_Total': batch_total,
            'Batch_Quantity_Warehouse': 0,
            'Batch_Quantity_Store': store_q,
            'Location': 'In-Store'
        })

# Convert to DataFrames
warehouse_batches_df = pd.DataFrame(warehouse_batches)
store_batches_df = pd.DataFrame(store_batches)

# Save to CSV
warehouse_batches_df.to_csv('dataset_changes/warehouse_batches.csv', index=False)
store_batches_df.to_csv('dataset_changes/store_batches.csv', index=False)

print("✅ Batches saved with correct quantity splits based on current_stock.")
