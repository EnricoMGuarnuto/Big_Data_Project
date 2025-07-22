import pandas as pd
import numpy as np
import random
import string
from datetime import datetime
from uuid import uuid4

# Load dataset
df = pd.read_csv('store_inventory.csv') 

# Remove the 'Others' category ---
df = df[df['Item_Type'] != 'Others']

# Define additional rows to add to existing categories (realistic estimates)
additional_items = {
    'Fruits and Vegetables': 0,     # very common
    'Snack Foods': 52,
    'Frozen Foods': 75,
    'Dairy': 28,
    'Canned': 125,
    'Baking Goods': 64,
    'Health and Hygiene': 43,
    'Soft Drinks': 51,
    'Meat': 48,
    'Breads': 36,
    'Hard Drinks': 28,
    'Starchy Foods': 33,
    'Breakfast': 24,
    'Seafood': 18
}

# Define newly introduced categories from earlier
new_categories = {
    'Spices and Condiments': 87,
    'Ready Meals': 63,
    'Pet Products': 103,
    'Baby Products': 97,
    'Hot Beverages': 74,
    'Gluten Free': 39,
    'Sweets': 92
}

# Generate new rows for both added and newly introduced categories
new_rows = []

# Add rows to existing categories
for category, count in additional_items.items():
    for _ in range(count):
        new_rows.append({
            'Item_Identifier': str(uuid4()),
            'Item_Type': category
        })

# Add rows for new categories
for category, count in new_categories.items():
    for _ in range(count):
        new_rows.append({
            'Item_Identifier': str(uuid4()),
            'Item_Type': category
        })

# Create DataFrame for all new rows
df_new = pd.DataFrame(new_rows)

# Combine original with new data
df = pd.concat([df, df_new], ignore_index=True)

# Count unique items per category
item_counts = df.groupby('Item_Type')['Item_Identifier'].nunique().sort_values(ascending=False)

# Display the result
print(item_counts)

############ Weights update #############

df['Item_Weight'] = df['Item_Weight'] * 1000

# Define realistic weights per Item_Type (in grams)
# These are fixed weights typically found in a supermarket
weight_mapping = {
    'Fruits and Vegetables': [200, 300, 500, 1000],   # change !!!
    'Snack Foods': [30, 50, 75, 100],
    'Household': [500, 750, 1000, 1500],
    'Frozen Foods': [400, 500, 600, 1000, 1500],
    'Dairy': [200, 500, 1000],
    'Canned': [400, 800],
    'Baking Goods': [250, 500, 1000],
    'Health and Hygiene': [100, 250, 500],
    'Soft Drinks': [330, 500, 1000, 1500],
    'Meat': [300, 500, 1000],
    'Breads': [250, 500, 800],
    'Hard Drinks': [500, 700, 1000],
    'Starchy Foods': [400, 500, 1000],
    'Breakfast': [200, 300, 400],
    'Seafood': [300, 500, 700],
    'Spices and Condiments': [50, 100, 200],
    'Ready Meals': [350, 400, 500],
    'Pet Products': [500, 1000, 1500],
    'Baby Products': [200, 400, 800],
    'Hot Beverages': [100, 250, 500],
    'Gluten Free': [200, 400, 500],
    'Sweets': [50, 100, 150]
}

# Assign a new weight to every row based on Item_Type
def assign_new_weight(row):
    options = weight_mapping.get(row['Item_Type'])
    if options:
        return np.random.choice(options)
    else:
        return np.random.randint(200, 1000)  # fallback

df['Item_Weight'] = df.apply(assign_new_weight, axis=1)


########## ID #########

# Define 2-letter prefixes per Item_Type
prefix_map = {
    'Fruits and Vegetables': 'FV',
    'Snack Foods': 'SF',
    'Household': 'HO',
    'Frozen Foods': 'FR',
    'Dairy': 'DA',
    'Canned': 'CA',
    'Baking Goods': 'BA',
    'Health and Hygiene': 'HH',
    'Soft Drinks': 'SD',
    'Meat': 'ME',
    'Breads': 'BR',
    'Hard Drinks': 'HD',
    'Starchy Foods': 'ST',
    'Breakfast': 'BK',
    'Seafood': 'SE',
    'Pasta and Rice': 'PR',
    'Spices and Condiments': 'SC',
    'Ready Meals': 'RM',
    'Pet Products': 'PP',
    'Baby Products': 'BP',
    'Hot Beverages': 'HB',
    'Gluten Free': 'GF',
    'Sweets': 'SW'
}

# Set to track used IDs
used_ids = set()

# Function to generate ID based on Item_Type
def generate_custom_id(item_type):
    prefix = prefix_map.get(item_type, 'XX')  # fallback in case of unexpected category
    while True:
        middle = random.choice(string.ascii_uppercase)
        digits = ''.join(random.choices(string.digits, k=2))
        new_id = prefix + middle + digits
        if new_id not in used_ids:
            used_ids.add(new_id)
            return new_id

# Apply the function to all rows
df['Item_Identifier'] = df['Item_Type'].apply(generate_custom_id)



########## item visibility ##########

# Define visibility ranges per Item_Type
visibility_ranges = {
    'Fruits and Vegetables': (0.05, 0.12),
    'Snack Foods': (0.08, 0.20),
    'Household': (0.01, 0.06),
    'Frozen Foods': (0.04, 0.10),
    'Dairy': (0.05, 0.10),
    'Canned': (0.02, 0.06),
    'Baking Goods': (0.03, 0.08),
    'Health and Hygiene': (0.01, 0.04),
    'Soft Drinks': (0.08, 0.18),
    'Meat': (0.04, 0.09),
    'Breads': (0.06, 0.12),
    'Hard Drinks': (0.02, 0.06),
    'Starchy Foods': (0.04, 0.08),
    'Breakfast': (0.03, 0.07),
    'Seafood': (0.03, 0.07),
    'Pasta and Rice': (0.04, 0.08),
    'Spices and Condiments': (0.01, 0.04),
    'Ready Meals': (0.04, 0.09),
    'Pet Products': (0.01, 0.03),
    'Baby Products': (0.01, 0.03),
    'Hot Beverages': (0.02, 0.05),
    'Gluten Free': (0.01, 0.03),
    'Sweets': (0.05, 0.10)
}

# Function to assign random visibility per type
def generate_visibility(row):
    if pd.isna(row['Item_Visibility']):
        min_v, max_v = visibility_ranges.get(row['Item_Type'], (0.03, 0.08))
        return round(np.random.uniform(min_v, max_v), 6)
    return row['Item_Visibility']

# Apply to the column
df['Item_Visibility'] = df.apply(generate_visibility, axis=1)


########## initial and current stock ############

# 1. Set current_stock equal to initial_stock for all rows
df['current_stock'] = df['initial_stock']

# 2. Identify rows with missing initial_stock (i.e., new rows)
missing_stock_mask = df['initial_stock'].isna()

# 3. Generate realistic stock levels for new rows
# Let's assume typical shelf quantities between 20 and 200 units
df.loc[missing_stock_mask, 'initial_stock'] = np.random.randint(20, 200, size=missing_stock_mask.sum())
df.loc[missing_stock_mask, 'current_stock'] = df.loc[missing_stock_mask, 'initial_stock']

# 4. Add timestamp column (same for all rows)
current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
df['time_stamp'] = current_timestamp




















# Save dataset 
df.to_csv('dataset_changes/store_inventory_final.csv', index=False)
