# data - Synthetic retail datasets

This folder contains the generated inventory, batch, and discount datasets used by
the project pipelines (Kafka producers, dashboard, Postgres loaders, Spark apps).

## Main datasets (Parquet)
- `store_inventory_final.parquet`: Store shelf snapshot, one row per `shelf_id`
  with `aisle`, `item_category`, `item_subcategory`, `item_weight`, `item_price`,
  `item_visibility`, `maximum_stock`, `current_stock`, `shelf_weight`,
  `time_stamp`.
- `warehouse_inventory_final.parquet`: Warehouse snapshot with the same product
  attributes plus `standard_batch_size` and no `item_visibility`.
- `store_batches.parquet`: Batch-level store stock. Quantities sum to the store
  inventory `current_stock`; `location` is `in-store`; `batch_quantity_warehouse`
  is `0`.
- `warehouse_batches.parquet`: Batch-level warehouse stock. Quantities sum to the
  warehouse inventory `current_stock`; `location` is `warehouse`;
  `batch_quantity_store` is `0`.
- `all_discounts.parquet`: Weekly discounts per `shelf_id` (`discount`, `week`),
  used by the shelf and POS producers.

## Common keys
- `shelf_id` is the join key across inventories, batches, and discounts.
- `batch_code` identifies a batch inside the batch datasets.
- `time_stamp` records the snapshot time for inventories and batches.

## CSV exports (data/db_csv)
These mirror the parquet files for Postgres loading and quick inspection.
- `data/db_csv/store_inventory_final.csv`
- `data/db_csv/warehouse_inventory_final.csv`
- `data/db_csv/store_batches.csv`
- `data/db_csv/warehouse_batches.csv`

## Scripts
- `data/create_db.py`: Generates the synthetic catalog, inventories, and batches
  and writes both parquet and CSV outputs (seeded with `np.random.seed(42)`).
- `data/controllo_univocità.py`: Quick check for duplicate `shelf_id` values in
  the store inventory CSV.

## Regenerate data
```bash
python data/create_db.py
```
This overwrites the parquet files in `data/` and CSVs in `data/db_csv/`.
