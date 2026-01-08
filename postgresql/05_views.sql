CREATE OR REPLACE VIEW analytics.v_train_demand_next7d AS
SELECT
  a.shelf_id,
  a.feature_date,

  a.item_category,
  a.item_subcategory,
  a.day_of_week,
  a.is_weekend,
  a.refill_day,

  a.item_price,
  a.discount,
  a.is_discounted,
  a.is_discounted_next_7d,

  a.people_count,

  a.sales_last_1d,
  a.sales_last_7d,
  a.sales_last_14d,

  a.current_stock_shelf,
  a.shelf_capacity,
  a.current_stock_warehouse,
  a.warehouse_capacity,

  a.standard_batch_size,

  a.min_expiration_days,
  a.avg_expiration_days,
  a.qty_expiring_next_7d,

  a.alerts_last_30d_shelf,
  a.alerts_last_30d_wh,

  (
    SELECT COALESCE(SUM(b.sales_qty), 0)
    FROM analytics.shelf_daily_features b
    WHERE b.shelf_id = a.shelf_id
      AND b.feature_date > a.feature_date
      AND b.feature_date <= a.feature_date + INTERVAL '7 days'
  )::int AS y_sales_next_7d

FROM analytics.shelf_daily_features a
WHERE a.feature_date <= (DATE '2025-12-31' - INTERVAL '7 days');


-- Feature set per ML (pulito + cast a numerico)
CREATE OR REPLACE VIEW analytics.v_ml_features AS
SELECT
  shelf_id,
  feature_date,

  item_category,
  item_subcategory,

  day_of_week,
  CASE WHEN is_weekend THEN 1 ELSE 0 END AS is_weekend,

  CASE WHEN refill_day THEN 1 ELSE 0 END AS refill_day,

  item_price::float AS item_price,
  COALESCE(discount, 0)::float AS discount,
  CASE WHEN is_discounted THEN 1 ELSE 0 END AS is_discounted,
  CASE WHEN is_discounted_next_7d THEN 1 ELSE 0 END AS is_discounted_next_7d,

  COALESCE(people_count, 0) AS people_count,

  COALESCE(sales_last_1d, 0) AS sales_last_1d,
  COALESCE(sales_last_7d, 0) AS sales_last_7d,
  COALESCE(sales_last_14d, 0) AS sales_last_14d,

  COALESCE(stockout_events, 0) AS stockout_events,

  COALESCE(shelf_capacity, 0) AS shelf_capacity,
  COALESCE(current_stock_shelf, 0) AS current_stock_shelf,
  COALESCE(shelf_fill_ratio, 0)::float AS shelf_fill_ratio,
  COALESCE(shelf_threshold_qty, 0) AS shelf_threshold_qty,
  COALESCE(expired_qty_shelf, 0) AS expired_qty_shelf,
  COALESCE(alerts_last_30d_shelf, 0) AS alerts_last_30d_shelf,
  CASE WHEN is_shelf_alert THEN 1 ELSE 0 END AS is_shelf_alert,

  COALESCE(warehouse_capacity, 0) AS warehouse_capacity,
  COALESCE(current_stock_warehouse, 0) AS current_stock_warehouse,
  COALESCE(warehouse_fill_ratio, 0)::float AS warehouse_fill_ratio,
  COALESCE(wh_reorder_point_qty, 0) AS wh_reorder_point_qty,
  COALESCE(pending_supplier_qty, 0) AS pending_supplier_qty,
  COALESCE(expired_qty_wh, 0) AS expired_qty_wh,
  COALESCE(alerts_last_30d_wh, 0) AS alerts_last_30d_wh,
  CASE WHEN is_warehouse_alert THEN 1 ELSE 0 END AS is_warehouse_alert,

  COALESCE(moved_wh_to_shelf, 0) AS moved_wh_to_shelf,

  COALESCE(standard_batch_size, 1) AS standard_batch_size,
  COALESCE(min_expiration_days, 9999) AS min_expiration_days,
  COALESCE(avg_expiration_days, 9999)::float AS avg_expiration_days,
  COALESCE(qty_expiring_next_7d, 0) AS qty_expiring_next_7d,

  -- target (solo per training)
  COALESCE(batches_to_order, 0) AS batches_to_order
FROM analytics.shelf_daily_features;


-- Dataset training: esclude righe “future” e roba senza warehouse
CREATE OR REPLACE VIEW analytics.v_ml_train AS
SELECT *
FROM analytics.v_ml_features
WHERE warehouse_capacity > 0
  AND feature_date IS NOT NULL;


-- Inference giornaliera: solo shelf in warehouse alert (o vuoi anche shelf alert? scegli tu)
-- CREATE OR REPLACE VIEW analytics.v_ml_infer_today AS
-- SELECT *
-- FROM analytics.v_ml_features
-- WHERE feature_date = CURRENT_DATE
  -- AND is_warehouse_alert = 1
  -- AND warehouse_capacity > 0;
