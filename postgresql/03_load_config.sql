-- ============================
-- 1) SHELF PROFILES (STORE)
-- ============================
WITH latest_store_snapshot AS (
  SELECT MAX(snapshot_ts) AS snapshot_ts
  FROM ref.store_inventory_snapshot
)
INSERT INTO config.shelf_profiles (shelf_id, item_weight)
SELECT
  s.shelf_id,
  MAX(s.item_weight) AS item_weight
FROM ref.store_inventory_snapshot s
JOIN latest_store_snapshot l
  ON s.snapshot_ts = l.snapshot_ts
GROUP BY s.shelf_id
ON CONFLICT (shelf_id) DO UPDATE
SET
  item_weight = EXCLUDED.item_weight,
  updated_at  = NOW();


-- ============================
-- 2) BATCH CATALOG
-- ============================
WITH all_batches AS (
  SELECT snapshot_ts, shelf_id, batch_code, expiry_date, standard_batch_size
  FROM ref.store_batches_snapshot
  UNION ALL
  SELECT snapshot_ts, shelf_id, batch_code, expiry_date, standard_batch_size
  FROM ref.warehouse_batches_snapshot
),
latest_batches AS (
  SELECT MAX(snapshot_ts) AS snapshot_ts
  FROM all_batches
)
INSERT INTO config.batch_catalog (
  batch_code,
  shelf_id,
  expiry_date,
  standard_batch_size
)
SELECT DISTINCT ON (b.batch_code)
  b.batch_code,
  b.shelf_id,
  b.expiry_date,
  b.standard_batch_size
FROM all_batches b
JOIN latest_batches l
  ON b.snapshot_ts = l.snapshot_ts
ORDER BY
  b.batch_code,
  b.snapshot_ts DESC
ON CONFLICT (batch_code) DO UPDATE
SET
  shelf_id            = EXCLUDED.shelf_id,
  expiry_date         = EXCLUDED.expiry_date,
  standard_batch_size = EXCLUDED.standard_batch_size,
  updated_at          = NOW();



-- ============================
-- 3) SHELF POLICIES (STORE)
-- ============================

-- fallback global
INSERT INTO config.shelf_policies (
  shelf_id, item_category, item_subcategory,
  threshold_pct, min_qty, active, notes
)
SELECT
  NULL, NULL, NULL,
  30.00, 1, TRUE,
  'Global default shelf policy (auto-created)'
WHERE NOT EXISTS (
  SELECT 1
  FROM config.shelf_policies p
  WHERE p.shelf_id IS NULL
    AND p.item_category IS NULL
    AND p.item_subcategory IS NULL
);

WITH latest_store_snapshot AS (
  SELECT MAX(snapshot_ts) AS snapshot_ts
  FROM ref.store_inventory_snapshot
),
store_rows AS (
  SELECT s.*
  FROM ref.store_inventory_snapshot s
  JOIN latest_store_snapshot l
    ON s.snapshot_ts = l.snapshot_ts
)

INSERT INTO config.shelf_policies (
  shelf_id,
  item_category,
  item_subcategory,
  threshold_pct,
  min_qty,
  active,
  notes
)
SELECT
  s.shelf_id,
  s.item_category,
  s.item_subcategory,
  30.00                        AS threshold_pct,   -- 30% default
  1                            AS min_qty,         -- minimo assoluto
  TRUE                         AS active,
  'Per-shelf policy auto-generated from latest store snapshot' AS notes
FROM store_rows s
WHERE NOT EXISTS (
  SELECT 1
  FROM config.shelf_policies p
  WHERE p.shelf_id = s.shelf_id
);


-- ============================
-- 4) WAREHOUSE POLICIES
-- ============================

WITH latest_wh_snapshot AS (
  SELECT MAX(snapshot_ts) AS snapshot_ts
  FROM ref.warehouse_inventory_snapshot
)
INSERT INTO config.wh_policies (
  shelf_id,
  item_category,
  item_subcategory,
  reorder_point_qty,
  active,
  notes
)
SELECT
  w.shelf_id,
  w.item_category,
  w.item_subcategory,
  GREATEST(
    1,
    (COALESCE(w.maximum_stock, w.current_stock) * 0.05)::int
  ) AS reorder_point_qty,
  TRUE AS active,
  'Auto-generated reorder point ≈5% of maximum_stock' AS notes
FROM ref.warehouse_inventory_snapshot w
JOIN latest_wh_snapshot l
  ON w.snapshot_ts = l.snapshot_ts
WHERE NOT EXISTS (
  SELECT 1
  FROM config.wh_policies p
  WHERE p.shelf_id = w.shelf_id
);
