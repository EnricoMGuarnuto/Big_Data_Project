import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta


###### WAREHOUSE #######

# Load your main dataset (update the path as needed)
df = pd.read_csv('dataset_changes/warehouse_inventory_final.csv')

# Define expiry ranges (in days from today) for each Item_Type
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

# Prepare list to collect all batches
warehouse_batches = []

# Loop through each item in the dataset
for _, row in df.iterrows():
    item_id = row['Item_Identifier']
    item_type = row['Item_Type']
    
    # Get expiry range for this type (fallback if not found)
    min_days, max_days = expiry_ranges.get(item_type, (60, 180))
    
    # Decide how many batches to create (1–3)
    num_batches = random.choice([1, 2, 3])
    
    for i in range(num_batches):
        expiry_date = datetime.now() + timedelta(days=random.randint(min_days, max_days))
        received_days_ago = random.randint(0, 10)
        received_date = datetime.now() - timedelta(days=received_days_ago)
        batch_quantity = random.randint(10, 100)
        batch_id = f'BATCH_{item_id}_{i+1}'
        
        warehouse_batches.append({
            'Item_Identifier': item_id,
            'Batch_ID': batch_id,
            'Received_Date': received_date.strftime('%Y-%m-%d'),
            'Expiry_Date': expiry_date.strftime('%Y-%m-%d'),
            'Batch_Quantity': batch_quantity,
            'Location': 'Warehouse'
        })

# Convert to DataFrame
warehouse_df = pd.DataFrame(warehouse_batches)

# Save to CSV
warehouse_df.to_csv('dataset_changes/warehouse_batches.csv', index=False)
print("Warehouse batch dataset saved to 'warehouse_batches.csv'")




####### IN STORE #######

import pandas as pd
import random
from datetime import datetime, timedelta

# Load warehouse batches
warehouse_df = pd.read_csv('dataset_changes/warehouse_batches.csv')

# Container for store batches
store_batches = []

# Group by product
grouped = warehouse_df.groupby('Item_Identifier')

for item_id, group in grouped:
    # Get batch with earliest expiry
    group['Expiry_Date'] = pd.to_datetime(group['Expiry_Date'])
    group_sorted = group.sort_values('Expiry_Date')
    
    # Probabilistic choice: 70% prendo lo stesso lotto con expiry min, 30% simulo uno più vecchio
    if random.random() < 0.7:
        batch_row = group_sorted.iloc[0]
        expiry_date = batch_row['Expiry_Date']
    else:
        # Simulo lotto vecchio con expiry prima del min attuale
        earliest = group_sorted['Expiry_Date'].min()
        expiry_date = earliest - timedelta(days=random.randint(1, 5))  # prodotto già quasi scaduto

    # Simulo una quantità rifornita realistica
    store_quantity = random.randint(5, 50)

    # Creo ID batch separato
    store_batches.append({
        'Item_Identifier': item_id,
        'Batch_ID': f'STOREBATCH_{item_id}',
        'Received_Date': datetime.now().strftime('%Y-%m-%d'),
        'Expiry_Date': expiry_date.strftime('%Y-%m-%d'),
        'Batch_Quantity': store_quantity,
        'Location': 'In-Store'
    })

# Crea DataFrame e salva
store_df = pd.DataFrame(store_batches)
store_df.to_csv('dataset_changes/store_batches.csv', index=False)
print("Store batch dataset saved to 'store_batches.csv'")
