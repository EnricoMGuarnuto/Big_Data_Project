-- load_ref_from_csv.sql
-- Assumes CSVs are mounted for the Postgres server at /import/csv
-- snapshot_ts è omesso: usa il DEFAULT NOW() definito nella DDL.

-- ============================
-- STORE INVENTORY SNAPSHOT
-- ============================
COPY ref.store_inventory_snapshot (
  shelf_id,
  aisle,
  item_weight,
  shelf_weight,
  item_category,
  item_subcategory,
  item_visibility,
  maximum_stock,
  current_stock,
  item_price
)
FROM '/import/csv/store_inventory_final.csv'
CSV HEADER;

-- ============================
-- STORE BATCHES SNAPSHOT
-- ===========================
COPY ref.store_batches_snapshot (
  shelf_id,
  batch_code,
  item_category,
  item_subcategory,
  standard_batch_size,
  received_date,
  expiry_date,
  batch_quantity_total,
  batch_quantity_store,
  batch_quantity_warehouse,
  location
)
FROM '/import/csv/store_batches.csv'
CSV HEADER;

-- ============================
-- WAREHOUSE INVENTORY SNAPSHOT
-- ============================
COPY ref.warehouse_inventory_snapshot (
  shelf_id,
  aisle,
  item_weight,
  shelf_weight,
  item_category,
  item_subcategory,
  maximum_stock,
  current_stock,
  item_price
)
FROM '/import/csv/warehouse_inventory_final.csv'
CSV HEADER;

-- ============================
-- WAREHOUSE BATCHES SNAPSHOT
-- ============================
COPY ref.warehouse_batches_snapshot (
  shelf_id,
  batch_code,
  item_category,
  item_subcategory,
  standard_batch_size,
  received_date,
  expiry_date,
  batch_quantity_total,
  batch_quantity_store,
  batch_quantity_warehouse,
  location
)
FROM '/import/csv/warehouse_batches.csv'
CSV HEADER;