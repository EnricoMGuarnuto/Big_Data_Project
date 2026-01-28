# Dataset Generators (Prompt-driven)

This folder contains the **dataset generators** used in the Big Data Technologies project.

The generator `create_db.py` produces a **synthetic supermarket retail dataset** (inventory + batches) in **Parquet** format, starting from a product hierarchy (categories/subcategories) and applying realistic rules for:
- store shelf capacity and current stock
- warehouse capacity aligned to batch sizes
- perishable vs non-perishable handling
- batch-level traceability (received/expiry dates)

The general list of categories that we used was provided by Conad in Scandiano, RE.

## What this script produces

Running `create_db.py` generates **four interconnected datasets** under `data/`:

- `data/store_inventory_final.parquet`  
  Snapshot of **in-store** stock per `shelf_id` (with `item_visibility`).

- `data/warehouse_inventory_final.parquet`  
  Snapshot of **warehouse** stock per `shelf_id` (batch-aligned capacity; perishables typically `current_stock = 0`).

- `data/store_batches.parquet`  
  Batch-level stock in-store, with FIFO-friendly `received_date` and expiry dates.

- `data/warehouse_batches.parquet`  
  Batch-level stock in the warehouse, stored in **full batches only** for non-perishables.

> Join key across all datasets: **`shelf_id`**.

Additional generators in this folder:

- `create_discounts.py`: creates weekly discount schedules and saves `data/all_discounts.parquet`.
- `create_simulation_data.py`: generates 1 year of synthetic sales/events and writes outputs under `data/sim_out/`.


## Output structure

```
data/
├── store_inventory_final.parquet
├── warehouse_inventory_final.parquet
├── store_batches.parquet
├── warehouse_batches.parquet
└── db_csv/
    ├── store_inventory_final.csv
    ├── warehouse_inventory_final.csv
    ├── store_batches.csv
    └── warehouse_batches.csv
```

CSV files are exported only for creating the postgres databases.

## Notes on scaling

The generator can **downscale** the number of SKUs per subcategory (keeping all categories/subcategories) via:

- `scale_factor=0.15` (example used in the script)
- `min_per_subcategory=1`

This was useful to keep the dataset manageable while preserving the structure.

---

# Prompt used to generate the dataset generator (ChatGPT)

For this project, the dataset generator code was produced by asking ChatGPT to implement the specification below.

```text
# create_db – Synthetic Retail Inventory & Batches Generator

I need your help with a Big Data Technology project.  
I have a detailed list of products and categories for a supermarket (beverages, canned goods, refrigerated, frozen, meat, fish, household goods, etc.), with a specific number of item types for each subcategory.

I need you to **generate Python code using Pandas and NumPy** to create **four interconnected datasets saved as Parquet files**, following these rules.

Please use `np.random.seed` for reproducibility and ensure all category and subcategory names are in English.  
The code should be **self-contained and ready to run**, creating the files under a `data/` directory.

---

## 1. Product catalog (internal helper, not necessarily saved)

First, create an internal **product catalog** (a DataFrame, not necessarily saved to disk) that enumerates all products based on the provided category/subcategory list.

Each product in the catalog must have:

- **`shelf_id`**  
  String, unique per product, encoding category + subcategory + progressive number (e.g. `"BEAWAT1"`).  
  This is the **primary key** that links all datasets.

- **`aisle`**  
  Integer, representing the aisle where the product is located in-store (fixed per macro category).

- **`item_category`**  
  High-level category (e.g. `"BEVERAGES"`, `"CANNED GOODS"`, `"FRUITS AND VEGETABLES"`, `"REFRIGERATED"`, `"MEAT"`, etc.).

- **`item_subcategory`**  
  More granular subcategory (e.g. `"Water"`, `"Yogurt"`, `"Beef"`).

- **`item_weight`**  
  Net weight / volume of a single unit (e.g. 500, 1000, etc.), in grams or millilitres depending on product type.  
  Drawn from either discrete `weight_options` or a numeric `weight_range` per subcategory.

- **`item_price`**  
  Unit price in euros, drawn uniformly from a sensible `price_range` specific to each subcategory.

- **`standard_batch_size`**  
  Integer representing the *logical* standard size (in units/pieces) of a batch for that product.  
  It must be:
  - Coherent with the product type (e.g., beverages often multiples of 6, fresh products smaller batches, heavy items smaller batches, etc.).
  - Used later to constrain warehouse capacity and batch generation.

This catalog is then used to generate both **inventory** and **batches** datasets.

---

## 2. Store Inventory  
**Output file:** `data/store_inventory_final.parquet`

This dataset represents the stock **on the supermarket shelves** (in-store).  
Each row must correspond to one `shelf_id` from the product catalog.

Columns:

- **`shelf_id`**  
  Unique product identifier, as in the product catalog.

- **`aisle`**  
  Integer aisle number where the shelf is located.

- **`item_weight`**  
  Weight/volume of a single item, copied from the catalog.

- **`shelf_weight`**  
  Total potential weight of the shelf **at maximum capacity**, computed as  
  `shelf_weight = item_weight * maximum_stock`.

- **`item_category`**  
  Product macro category.

- **`item_subcategory`**  
  Product subcategory.

- **`item_visibility`**  
  Float in \[0.05, 0.20], representing the fraction of customer foot traffic that “sees” this shelf (synthetic visibility score; only defined for in-store).

- **`maximum_stock`**  
  Integer, the **maximum number of units** that fit on the store shelf for that `shelf_id`.  
  - Drawn from a `store_max_stock_range` per subcategory.  
  - Must be at least `standard_batch_size` (so that one full batch fits).

- **`current_stock`**  
  Integer, the **current number of units** on the shelf.  
  - For **non-perishable / long-shelf-life products** (e.g. canned goods, cleaning products, long-life fruit, many grocery items):  
    `current_stock` is a random percentage of `maximum_stock`, drawn from a subcategory-specific `current_stock_percent` range (e.g. 70–100%).  
  - For **perishable categories** (e.g. fruits and vegetables, refrigerated, frozen, meat, fish), use a specific rule per subcategory (e.g. `current_stock_probabilities`) to simulate just-in-time replenishment and stock-outs.  
  - Always ensure `0 < current_stock <= maximum_stock` when capacity is non-zero (if a rule would give 0 but capacity exists, force at least some minimal stock).

- **`item_price`**  
  Unit price in euros, copied from the product catalog.

- **`time_stamp`**  
  String timestamp of when the inventory snapshot is generated, in the format  
  `"YYYY-MM-DD HH:MM:SS"`.


> It is kept only in the internal DataFrame for batch and warehouse logic.

---

## 3. Warehouse Inventory  
**Output file:** `data/warehouse_inventory_final.parquet`

This dataset represents the **central/backshelf warehouse stock** for the same products.  
It must reuse the **same `shelf_id`, `aisle`, `item_weight`, `item_price`, `item_category`, and `item_subcategory`** as the store inventory.

Columns:

- `shelf_id`  
- `aisle`  
- `item_weight`  
- `shelf_weight`  
- `item_category`  
- `item_subcategory`  
- `standard_batch_size`  
- `maximum_stock`  
- `current_stock`  
- `item_price`  
- `time_stamp`  

(Only `item_visibility` is **not** present in the warehouse inventory.)

Semantics and constraints:

- **`standard_batch_size`**  
  Same value as in the product catalog. Here it is **saved** in the parquet file because warehouse logic is batch-driven.

- **`maximum_stock` (warehouse)**  
  Integer, **always a multiple of `standard_batch_size`** and represents total warehouse capacity in units.  

  Rules:
  - For categories flagged to have `warehouse_max_stock_equal_store`:  
    - Start from store `maximum_stock`, ensure at least `2 * standard_batch_size`, then round **up** to a multiple of `standard_batch_size`.
  - For categories with a `warehouse_max_stock_range`:  
    - Draw `base_max` in that range, ensure at least `2 * standard_batch_size`, then round **down** to a multiple of `standard_batch_size`. If rounding down breaks the 2-batch rule, fix it.
  - Otherwise (fallback):  
    - Use a fraction of store `maximum_stock` (e.g. 10–40%), ensure at least `2 * standard_batch_size`, then round to a multiple of `standard_batch_size`.

- **`current_stock` (warehouse)**  
  Integer.  
  - For **perishable products** (where we use `current_stock_probabilities` in the hierarchy):  
    `current_stock = 0` → they are not stored in the central warehouse.  
  - For **non-perishables**:  
    `current_stock = n_full_batches * standard_batch_size`, with:
    - `n_full_batches` randomly chosen in a small range (e.g. 1–3).
    - `n_full_batches * standard_batch_size <= maximum_stock`.  
    Warehouse stock is thus **in full batches only** (no partial batch).

- **`shelf_weight`**  
  Computed as `item_weight * maximum_stock`.

- **`time_stamp`**  
  Same format and semantics as in the store inventory: `"YYYY-MM-DD HH:MM:SS"`.

---

## 4. Store Batches  
**Output file:** `data/store_batches.parquet`

This dataset tracks **product batches in the store**.  
Each row is a batch, associated with one `shelf_id`.

Columns:

- **`shelf_id`**  
  Links the batch to a product in store/warehouse inventories.

- **`batch_code`**  
  String batch identifier (e.g. `"B-1234"`).

- **`item_category`**  
  Category of the product (copied from inventory).

- **`item_subcategory`**  
  Subcategory of the product (copied from inventory).

- **`standard_batch_size`**  
  From the product catalog / inventory, reused here to keep batch semantics explicit.

- **`received_date`**  
  Date (string `"YYYY-MM-DD"`) when this batch arrived in-store.  
  Batches with lower index are **older**, and must have earlier `received_date`.

- **`expiry_date`**  
  Date (string `"YYYY-MM-DD"`) when this batch expires.  
  - For **perishable products**: short shelf life (e.g. 2–15 days), older batches expire earlier.  
  - For **non-perishable products**: long shelf life (e.g. 1–3 years), again with older batches expiring earlier.

- **`batch_quantity_total`**  
  Integer, total units in this batch (still available at the time of the snapshot).  
  Must satisfy:
  - `0 < batch_quantity_total <= standard_batch_size`  
  - For each `shelf_id`, the sum over all its batches equals the corresponding `current_stock` from the **store inventory**.

- **`batch_quantity_store`**  
  Integer, number of units of this batch that are currently **physically in-store**.  
  - In the **store batches dataset**, set  
    `batch_quantity_store = batch_quantity_total`.

- **`batch_quantity_warehouse`**  
  Integer, number of units for this batch in the warehouse.  
  - In the **store batches dataset**, this must always be `0`.

- **`location`**  
  String location tag, must be exactly `"in-store"` for all rows in this dataset.

- **`time_stamp`**  
  String `"YYYY-MM-DD HH:MM:SS"` representing when this batch record was generated.

For each `shelf_id` in the **store inventory**, create one or more batches (typically 1–2, but more are allowed) such that:

\[
\sum_{\text{batches of that shelf}} batch\_quantity\_store = current\_stock_{\text{store}}
\]

---

## 5. Warehouse Batches  
**Output file:** `data/warehouse_batches.parquet`

This dataset mirrors the store batches, but for the **warehouse**.

Columns (same schema as store batches):

- `shelf_id`  
- `batch_code`  
- `item_category`  
- `item_subcategory`  
- `standard_batch_size`  
- `received_date`  
- `expiry_date`  
- `batch_quantity_total`  
- `batch_quantity_store`  
- `batch_quantity_warehouse`  
- `location`  
- `time_stamp`  

Differences in semantics:

- **`batch_quantity_store`**  
  Must always be `0` in this dataset.

- **`batch_quantity_warehouse`**  
  Holds the actual batch quantity for the warehouse. For each `shelf_id`:

\[
\sum_{\text{batches of that shelf}} batch\_quantity\_warehouse = current\_stock_{\text{warehouse}}
\]

- **`location`**  
  Must always be `"warehouse"`.

All other fields (`shelf_id`, `batch_code`, `item_category`, `item_subcategory`, `standard_batch_size`, `received_date`, `expiry_date`, `batch_quantity_total`, `time_stamp`) follow the same logic as in the store batches dataset, with expiry logic consistent between store and warehouse for the same product type.

---

## 6. General requirements

- Use **Pandas** and **NumPy** only.
- Use `np.random.seed(42)` for reproducibility.
- Save all four datasets in **Parquet format** under a `data/` folder with these exact filenames:
  - `data/store_inventory_final.parquet`
  - `data/warehouse_inventory_final.parquet`
  - `data/store_batches.parquet`
  - `data/warehouse_batches.parquet`
- Ensure:
  - `item_weight` and `item_price` are consistent across **all** datasets for the same `shelf_id`.
  - `shelf_id` is the **join key** across catalog, inventories, and batches.
  - Stock and batch quantities respect all constraints described above:
    - Sum of batches = current stock for each `shelf_id` and location.
    - `batch_quantity_total <= standard_batch_size`.
    - Warehouse stock in **full batches only** for non-perishables.
    - Perishable items mostly managed only in-store, with warehouse stock often 0.

## Product List

This is a comprehensive list of all products, organized by category and subcategory. The number next to each subcategory indicates the number of different item types (e.g., different brands or sizes) that should be included.

### BEVERAGES
•⁠  ⁠Water: 53
•⁠  ⁠Fruit-based Soft Drinks: 59
•⁠  ⁠Cola Soft Drinks: 37
•⁠  ⁠Iced Tea: 31
•⁠  ⁠Energy Drinks: 42
•⁠  ⁠Flavored Water: 10
•⁠  ⁠Juices: 81
•⁠  ⁠Syrups: 12

### SPIRITS AND APERITIFS
•⁠  ⁠Non-alcoholic Aperitifs: 8
•⁠  ⁠Alcoholic Aperitifs: 31
•⁠  ⁠Liqueurs: 32
•⁠  ⁠Grappa: 30
•⁠  ⁠Cognac and Brandy: 8
•⁠  ⁠Whisky: 9
•⁠  ⁠Amaro: 21
•⁠  ⁠Gin: 12
•⁠  ⁠Vodka: 10
•⁠  ⁠Rum: 11
•⁠  ⁠Ethyl Alcohol: 4

### BEERS
•⁠  ⁠Standard: 62
•⁠  ⁠Premium: 41
•⁠  ⁠Imported: 59
•⁠  ⁠Non-alcoholic: 4
•⁠  ⁠Flavored: 3

### WINES
•⁠  ⁠Brick Wines: 19
•⁠  ⁠White Wines: 117
•⁠  ⁠Sparkling White Wines: 38
•⁠  ⁠Red Wines: 156
•⁠  ⁠Sparkling Red Wines: 73
•⁠  ⁠Rosé Wines: 17
•⁠  ⁠Sparkling Rosé Wines: 12
•⁠  ⁠Sparkling Wines: 22
•⁠  ⁠Dessert Wines: 5

### LONG-LIFE DAIRY
•⁠  ⁠Long-life Milk: 59
•⁠  ⁠Plant-based Drinks: 40
•⁠  ⁠Cream and Béchamel: 25
•⁠  ⁠Condensed Milk: 1

### HOUSEHOLD GOODS
•⁠  ⁠Paper Towels: 14
•⁠  ⁠Tissues: 21
•⁠  ⁠Napkins: 20
•⁠  ⁠Film-aluminum-parchment paper: 21
•⁠  ⁠Baking Trays: 16
•⁠  ⁠Freezer Bags: 13
•⁠  ⁠Toothpicks and similar: 10
•⁠  ⁠Paper tableware: 11

### BATH AND PERSONAL CARE
•⁠  ⁠Wipes: 11
•⁠  ⁠Sanitary pads and liners: 55
•⁠  ⁠Adult diapers: 30
•⁠  ⁠Cotton: 8
•⁠  ⁠Cotton swabs: 4
•⁠  ⁠Toilet paper: 12
•⁠  ⁠Bar soaps: 13
•⁠  ⁠Liquid soaps: 32
•⁠  ⁠Intimate soaps: 19
•⁠  ⁠Toothpastes: 57
•⁠  ⁠Mouthwashes: 14
•⁠  ⁠Toothbrushes and accessories: 51
•⁠  ⁠Shampoos: 81
•⁠  ⁠Conditioners and after-shampoos: 42
•⁠  ⁠Hair dyes: 34
•⁠  ⁠Hairsprays and gels: 26
•⁠  ⁠Deodorants and talc: 85
•⁠  ⁠Bath and shower gels: 92
•⁠  ⁠Body treatments: 35
•⁠  ⁠Face creams and masks: 23
•⁠  ⁠Cleansing and makeup remover waters: 29
•⁠  ⁠Acetone: 2
•⁠  ⁠Pre-shave: 17
•⁠  ⁠Razors and men's accessories: 39
•⁠  ⁠Razors and women's accessories: 33
•⁠  ⁠Aftershave: 15
•⁠  ⁠Band-aids and pharmaceuticals: 14
•⁠  ⁠Trash bags: 18

### LAUNDRY PRODUCTS
•⁠  ⁠Hand wash: 7
•⁠  ⁠Liquid detergent: 42
•⁠  ⁠Powder and sheet detergent: 20
•⁠  ⁠Descalers: 11
•⁠  ⁠Fabric softeners: 36
•⁠  ⁠Laundry scents: 19
•⁠  ⁠Bleaches: 28
•⁠  ⁠Stain removers: 19

### HOUSE CLEANING PRODUCTS
•⁠  ⁠Bleach: 15
•⁠  ⁠Dish detergents: 38
•⁠  ⁠Dishwasher detergents: 43
•⁠  ⁠Dishwasher salt: 2
•⁠  ⁠Ironing products: 4
•⁠  ⁠Bathroom cleaners: 26
•⁠  ⁠Drain cleaners: 8
•⁠  ⁠Air fresheners: 21
•⁠  ⁠Anti-limescale: 7
•⁠  ⁠Detergents and disinfectants: 31
•⁠  ⁠Glass and furniture cleaners: 28
•⁠  ⁠Insecticides: 34
•⁠  ⁠Gloves: 25
•⁠  ⁠Sponges and cloths: 41

### BABY PRODUCTS
•⁠  ⁠Diapers: 42
•⁠  ⁠Cookies: 7
•⁠  ⁠Pasta and creams: 30
•⁠  ⁠Baby formula: 23
•⁠  ⁠Homogenized foods: 81
•⁠  ⁠Care and hygiene: 29

### FRUITS AND VEGETABLES
•⁠  ⁠Apples: 7
•⁠  ⁠Pears: 5
•⁠  ⁠Cherries: 3
•⁠  ⁠Strawberries: 2
•⁠  ⁠Apricots: 3
•⁠  ⁠Plums: 3
•⁠  ⁠Yellow Peaches: 4
•⁠  ⁠Grapes: 3
•⁠  ⁠Bananas: 3
•⁠  ⁠Kiwi: 2
•⁠  ⁠Pineapples: 2
•⁠  ⁠Avocados: 2
•⁠  ⁠Watermelons: 1
•⁠  ⁠Melons: 2
•⁠  ⁠Berries: 6
•⁠  ⁠Potatoes: 5
•⁠  ⁠Onions: 4
•⁠  ⁠Spring onions: 3
•⁠  ⁠Carrots: 4
•⁠  ⁠Radishes: 2
•⁠  ⁠Green beans: 2
•⁠  ⁠Tomatoes: 10
•⁠  ⁠Cucumbers: 1
•⁠  ⁠Peppers: 3
•⁠  ⁠Eggplants: 1
•⁠  ⁠Zucchini: 2
•⁠  ⁠Fennels: 2
•⁠  ⁠Celery: 2
•⁠  ⁠Cabbages: 4
•⁠  ⁠Broccoli: 2
•⁠  ⁠Pumpkins: 2
•⁠  ⁠Beets and chicory: 2
•⁠  ⁠Lettuce: 3
•⁠  ⁠Radicchio: 1
•⁠  ⁠Oranges: 3
•⁠  ⁠Mandarins: 3
•⁠  ⁠Grapefruits: 1
•⁠  ⁠Ready-to-eat raw vegetables: 37
•⁠  ⁠Ready-to-eat cooked vegetables: 10
•⁠  ⁠Soups and side dishes: 39
•⁠  ⁠Garlic and chilies: 4
•⁠  ⁠Leaf herbs: 14

### LONG-LIFE FRUIT
•⁠  ⁠Canned fruit: 11
•⁠  ⁠Fruit desserts: 16
•⁠  ⁠Dried fruit: 21
•⁠  ⁠Nuts: 32
•⁠  ⁠Other snacks: 43
•⁠  ⁠Dried legumes: 20
•⁠  ⁠Seeds: 7

### SWEETS AND BREAKFAST
•⁠  ⁠Snack cakes: 97
•⁠  ⁠Ready-made cakes: 19
•⁠  ⁠Cookies: 114
•⁠  ⁠Industrial pastries: 63
•⁠  ⁠Snacks and bars: 71
•⁠  ⁠Sweeteners: 12
•⁠  ⁠Candies: 58
•⁠  ⁠Flavors and decorations: 31
•⁠  ⁠Cocoa powder: 5
•⁠  ⁠Puddings: 13
•⁠  ⁠Spreads: 25
•⁠  ⁠Chocolate bars: 32
•⁠  ⁠Chocolates: 27
•⁠  ⁠Chocolate eggs: 10
•⁠  ⁠Cereals: 38
•⁠  ⁠Rusks: 36
•⁠  ⁠Milk modifiers and chocolate preparations: 12
•⁠  ⁠Jams: 52
•⁠  ⁠Honey: 11

### FLOURS AND SUGAR
•⁠  ⁠Sugar: 13
•⁠  ⁠Flour: 44
•⁠  ⁠Corn flour and polenta: 18
•⁠  ⁠Yeast: 21
•⁠  ⁠Salt: 14

### PASTA AND RICE
•⁠  ⁠Rice: 43
•⁠  ⁠Spelt and whole wheat pasta: 23
•⁠  ⁠Cereals and others: 18
•⁠  ⁠Pasta: 98
•⁠  ⁠Egg pasta: 47
•⁠  ⁠Soup and risotto mixes: 45
•⁠  ⁠Stock cubes and broths: 31

### GLUTEN FREE
•⁠  ⁠Pasta: 21
•⁠  ⁠Baked goods: 4
•⁠  ⁠Cookies: 18
•⁠  ⁠Rice cakes: 4
•⁠  ⁠Savory snacks: 13
•⁠  ⁠Sweet snacks: 23

### BREAD AND BREADSTICKS
•⁠  ⁠Breadsticks: 29
•⁠  ⁠Flatbreads: 7
•⁠  ⁠Rice cakes: 29
•⁠  ⁠Packaged bread: 53
•⁠  ⁠Long-life flatbreads: 13
•⁠  ⁠Breadcrumbs: 3
•⁠  ⁠Pizza and bread mixes: 8
•⁠  ⁠Crackers: 25
•⁠  ⁠Taralli: 15

### HOT BEVERAGES
•⁠  ⁠Ground coffee: 28
•⁠  ⁠Decaf ground coffee: 6
•⁠  ⁠Coffee pods and capsules: 46
•⁠  ⁠Coffee beans: 10
•⁠  ⁠Instant coffee: 21
•⁠  ⁠Barley coffee: 10
•⁠  ⁠Tea bags: 32
•⁠  ⁠Decaf tea bags: 8
•⁠  ⁠Instant chamomile: 6
•⁠  ⁠Chamomile tea bags: 8
•⁠  ⁠Other infusions: 34

### SAUCES
•⁠  ⁠Mayonnaise: 21
•⁠  ⁠Ketchup: 13
•⁠  ⁠Mustard: 8
•⁠  ⁠Savory spreads: 14
•⁠  ⁠Other sauces: 30

### PASTA SAUCES
•⁠  ⁠Ready-made sauces: 60
•⁠  ⁠Tomato puree: 30
•⁠  ⁠Tomato pulp: 21

### SPICES
•⁠  ⁠Spices: 19
•⁠  ⁠Herbs: 21
•⁠  ⁠Flavorings: 16

### VINEGAR AND OIL
•⁠  ⁠White vinegar: 10
•⁠  ⁠Balsamic vinegar: 9
•⁠  ⁠Vinegar-condiments: 12
•⁠  ⁠Olive oil: 46
•⁠  ⁠Corn oil: 5
•⁠  ⁠Peanut oil: 4
•⁠  ⁠Soybean oil: 2
•⁠  ⁠Sunflower oil: 6
•⁠  ⁠Frying oil: 4

### SAVORY SNACKS
•⁠  ⁠Chips and similar: 64
•⁠  ⁠Popcorn: 8
•⁠  ⁠Toasted snacks: 18

### PET PRODUCTS
•⁠  ⁠Wet cat food: 57
•⁠  ⁠Dry cat food: 32
•⁠  ⁠Cat snacks: 15
•⁠  ⁠Wet dog food: 41
•⁠  ⁠Dry dog food: 30
•⁠  ⁠Dog snacks: 25
•⁠  ⁠Dog hygiene and care: 7
•⁠  ⁠Cat litter: 10

### CANNED GOODS
•⁠  ⁠Peeled tomatoes: 7
•⁠  ⁠Peas: 9
•⁠  ⁠Beans: 16
•⁠  ⁠Corn: 9
•⁠  ⁠Other legumes and vegetables: 23
•⁠  ⁠Vegetables in oil: 41
•⁠  ⁠Vegetables in vinegar: 12
•⁠  ⁠Sweet and sour vegetables: 8
•⁠  ⁠Olives: 32
•⁠  ⁠Condiriso: 11
•⁠  ⁠Capers: 8
•⁠  ⁠Dried mushrooms: 5
•⁠  ⁠Canned meat: 12
•⁠  ⁠Tuna in oil: 51
•⁠  ⁠Tuna in water: 14
•⁠  ⁠Tuna with other condiments: 20
•⁠  ⁠Mackerel and sardines: 22
•⁠  ⁠Anchovies and similar: 20
•⁠  ⁠Other fish products: 10

### REFRIGERATED 
•⁠  ⁠Yogurt: 60
•⁠  ⁠Fresh Milk: 15
•⁠  ⁠Eggs: 10
•⁠  ⁠Fresh Pasta: 45
•⁠  ⁠Butter and Margarine: 20
•⁠  ⁠Cheese and Cured Meats: 120
•⁠  ⁠Pies and Pizza dough: 25
•⁠  ⁠Fresh Juices: 15

### FROZEN 
•⁠  ⁠Frozen Vegetables: 40
•⁠  ⁠Frozen Fish: 25
•⁠  ⁠Frozen Potatoes: 10
•⁠  ⁠Ice Creams: 50
•⁠  ⁠Pizzas: 30
•⁠  ⁠Frozen Ready Meals: 35

### MEAT
•⁠  ⁠Beef: 40
•⁠  ⁠Pork: 30
•⁠  ⁠Chicken: 50
•⁠  ⁠Sausages and Burgers: 25
•⁠  ⁠Cold Cuts: 70

### FISH 
•⁠  ⁠Fresh Fish: 30
•⁠  ⁠Shellfish: 15
•⁠  ⁠Smoked Salmon: 10

```
