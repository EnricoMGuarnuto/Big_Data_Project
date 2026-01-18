-- ============================================
-- Smart Shelf Restocking System – Database DDL
-- Schemas: config, state, ops, ref, analytics
-- ============================================

-- Extensions (optional but useful)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- -------------------------
-- SCHEMAS
-- -------------------------
CREATE SCHEMA IF NOT EXISTS config;   -- alert rules, shelf profiles, batch catalog
CREATE SCHEMA IF NOT EXISTS state; -- wh_state, shelf_state, shelf_batch_state, wh_batch_state
CREATE SCHEMA IF NOT EXISTS ops;  -- alerts, shelf_restock_plan, wh_events, pos_transactions, wh_supplier_plan
CREATE SCHEMA IF NOT EXISTS ref;  -- snapshots from parquet (store_inventory, warehouse_inventory, batches)
CREATE SCHEMA IF NOT EXISTS analytics;

-- -------------------------
-- ENUMS 
-- -------------------------
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'severity_level') THEN
    CREATE TYPE severity_level AS ENUM ('low','medium','high','critical');
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'wh_event_type') THEN
    CREATE TYPE wh_event_type AS ENUM ('wh_in','wh_out');
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'alert_status') THEN
    CREATE TYPE alert_status AS ENUM ('open','ack','resolved','dismissed');
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'plan_status') THEN
    CREATE TYPE plan_status AS ENUM ('pending','issued','completed','canceled');
  END IF;
END$$;

-- ======================================================
-- CONFIG / RULES (owned and edited by humans or services)
-- ======================================================

-- Shelf policies managed in the UI and synced to Kafka `shelf_policies`
CREATE TABLE IF NOT EXISTS config.shelf_policies (
  policy_id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  -- Scope: by shelf, category, or global
  shelf_id          TEXT NULL,
  item_category     TEXT NULL,
  item_subcategory  TEXT NULL,
  -- Thresholds / logic
  threshold_pct     NUMERIC(5,2) NULL,       -- e.g. trigger when stock < threshold% of max
  min_qty           INTEGER NULL,            -- absolute floor
  -- target_pct        NUMERIC(5,2) NULL,       -- target refill level (e.g. 80)
  -- hysteresis_pct    NUMERIC(5,2) NULL,       -- to avoid alert flapping
  severity          severity_level NOT NULL DEFAULT 'medium',
  active            BOOLEAN NOT NULL DEFAULT TRUE,
  notes             TEXT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_shelf_policies_scope
  ON config.shelf_policies (shelf_id, item_category, item_subcategory)
  WHERE active = TRUE;


CREATE TABLE IF NOT EXISTS config.wh_policies (
  policy_id        UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  shelf_id         TEXT NULL,
  item_category    TEXT NULL,
  item_subcategory TEXT NULL,

  reorder_point_qty  INTEGER NULL,       -- or threshold in absolute units

  active           BOOLEAN NOT NULL DEFAULT TRUE,
  notes            TEXT NULL,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);




-- Optional reference of shelf profiles/metadata (for planner)
CREATE TABLE IF NOT EXISTS config.shelf_profiles (
  shelf_id          TEXT PRIMARY KEY,
  item_weight       NUMERIC(10,3) NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- batch catalog (optional reference data for batches)
CREATE TABLE IF NOT EXISTS config.batch_catalog (
  batch_code          TEXT PRIMARY KEY,
  shelf_id            TEXT NULL,
  expiry_date         DATE NULL,
  standard_batch_size INTEGER NULL,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ======================================================
-- STATE (compacted mirrors you can also materialize from Spark)
-- These tables mirror your compacted Kafka topics for convenient querying.
-- Primary keys reflect the compaction keys.
-- ======================================================

-- Current shelf state (from shelf_events aggregation)
CREATE TABLE IF NOT EXISTS state.shelf_state (
  shelf_id        TEXT PRIMARY KEY,
  category        TEXT NULL,
  subcategory     TEXT NULL,
  current_stock   INTEGER NOT NULL,
  shelf_weight    NUMERIC(12,3) NULL,
  last_update_ts  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Current warehouse state (from wh_events aggregation)
CREATE TABLE IF NOT EXISTS state.wh_state (
  shelf_id         TEXT PRIMARY KEY,
  category         TEXT NULL,
  subcategory      TEXT NULL,
  wh_current_stock INTEGER NOT NULL,
  last_update_ts   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- In-store batches (per batch; mirrors `shelf_batch_state`)
CREATE TABLE IF NOT EXISTS state.shelf_batch_state (
  shelf_id                    TEXT NOT NULL,
  batch_code                  TEXT NOT NULL,
  category                   TEXT NULL,
  subcategory                TEXT NULL,
  received_date               DATE NOT NULL,
  expiry_date                 DATE NOT NULL,
  batch_quantity_store        INTEGER NOT NULL DEFAULT 0,
  last_update_ts              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (shelf_id, batch_code)
);

CREATE INDEX IF NOT EXISTS ix_shelf_batch_state_expiry
  ON state.shelf_batch_state (expiry_date);

-- Warehouse batches (per batch; mirrors `wh_batch_state`)
CREATE TABLE IF NOT EXISTS state.wh_batch_state (
  shelf_id                    TEXT NOT NULL,
  batch_code                  TEXT NOT NULL,
  category                   TEXT NULL,
  subcategory                TEXT NULL,
  received_date               DATE NOT NULL,
  expiry_date                 DATE NOT NULL,
  batch_quantity_warehouse    INTEGER NOT NULL DEFAULT 0,
  batch_quantity_store        INTEGER NOT NULL DEFAULT 0,
  last_update_ts              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (shelf_id, batch_code)
);

CREATE INDEX IF NOT EXISTS ix_wh_batch_state_expiry
  ON state.wh_batch_state (expiry_date);

-- ======================================================
-- OPS: operational facts (plans, events, alerts)
-- These are append-only (or status-updated) tables fed by your services.
-- ======================================================

-- Shelf restock plan (Spark output, also mirrored on Kafka `shelf_restock_plan`)
CREATE TABLE IF NOT EXISTS ops.shelf_restock_plan (
  plan_id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  shelf_id           TEXT NOT NULL,
  suggested_qty      INTEGER NOT NULL CHECK (suggested_qty >= 0),
  status             plan_status NOT NULL DEFAULT 'pending',
  created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_shelf_restock_plan_status
  ON ops.shelf_restock_plan (status, created_at);

-- Warehouse events (executor output, also on Kafka `wh_events`)
CREATE TABLE IF NOT EXISTS ops.wh_events (
  event_id                       UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  event_type                     wh_event_type NOT NULL,   -- 'wh_in' | 'wh_out'
  shelf_id                       TEXT NOT NULL,
  batch_code                     TEXT NULL,
  qty                            INTEGER NOT NULL CHECK (qty >= 0),
  timestamp                      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  plan_id                        UUID NULL,                -- link back to plan or alert
  reason                         TEXT NULL,                -- 'plan_restock', 'alert_restock', 'plan_inbound', ...
  -- Snapshots after the event (useful for fast UI consistency)
  received_date                  DATE NULL,
  expiry_date                    DATE NULL,
  batch_quantity_warehouse_after INTEGER NULL,
  batch_quantity_store_after     INTEGER NULL,
  shelf_warehouse_qty_after      INTEGER NULL
);

CREATE INDEX IF NOT EXISTS ix_wh_events_time
  ON ops.wh_events (timestamp);

CREATE INDEX IF NOT EXISTS ix_wh_events_plan
  ON ops.wh_events (plan_id);

CREATE INDEX IF NOT EXISTS ix_wh_events_shelf
  ON ops.wh_events (shelf_id, timestamp);

-- Supplier restock plan (compacted plan state; lifecycle: pending/issued/completed)
CREATE TABLE IF NOT EXISTS ops.wh_supplier_plan (
  supplier_plan_id TEXT PRIMARY KEY,          -- UUID from planner (Kafka compaction key is shelf_id)
  shelf_id TEXT NOT NULL,
  suggested_qty INTEGER NOT NULL CHECK (suggested_qty >= 0),
  standard_batch_size INTEGER NULL,
  status TEXT NOT NULL DEFAULT 'pending',     -- pending/issued/completed/canceled
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_wh_supplier_plan_shelf
  ON ops.wh_supplier_plan(shelf_id);

CREATE INDEX IF NOT EXISTS ix_wh_supplier_plan_status
  ON ops.wh_supplier_plan(status);


-- Alerts (Alert Engine output, also on Kafka `alerts`)
CREATE TABLE IF NOT EXISTS ops.alerts (
  alert_id        UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  event_type      TEXT NOT NULL,            -- e.g., 'refill_request','near_expiry', 'supplier_request'
  shelf_id        TEXT NULL,
  location        TEXT NULL,            -- 'store' | 'warehouse' 
  severity        severity_level NOT NULL DEFAULT 'medium',
  -- payload (optional, for refill_request)
  current_stock   INTEGER NULL,
  max_stock       INTEGER NULL,
  target_pct      NUMERIC(5,2) NULL,
  suggested_qty   INTEGER NULL,
  -- lifecycle
  status          alert_status NOT NULL DEFAULT 'open',
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_alerts_time
  ON ops.alerts (created_at);

CREATE INDEX IF NOT EXISTS ix_alerts_status
  ON ops.alerts (status, created_at);

-- Optional: POS transactions mirror (for UI/analytics; source Kafka `pos_transactions`)
CREATE TABLE IF NOT EXISTS ops.pos_transactions (
  transaction_id   UUID PRIMARY KEY,
  customer_id      TEXT NULL,
  timestamp        TIMESTAMPTZ NOT NULL,
  -- denormalized totals (optional)
  total_items      INTEGER NULL,
  total_amount     NUMERIC(12,2) NULL --price
);

-- POS line items (supports batch_code for FIFO auditing)
CREATE TABLE IF NOT EXISTS ops.pos_transaction_items (
  transaction_id   UUID NOT NULL REFERENCES ops.pos_transactions(transaction_id) ON DELETE CASCADE,
  line_no          INTEGER NOT NULL,
  shelf_id         TEXT NOT NULL,
  quantity         INTEGER NOT NULL CHECK (quantity > 0),
  unit_price       NUMERIC(10,2) NOT NULL,
  discount         NUMERIC(5,2) NOT NULL DEFAULT 0.00,
  total_price      NUMERIC(12,2) NOT NULL,
  batch_code       TEXT NULL,
  expiry_date      DATE NULL,
  PRIMARY KEY (transaction_id, line_no)
);

CREATE INDEX IF NOT EXISTS ix_pos_items_shelf
  ON ops.pos_transaction_items (shelf_id);

-- ======================================================
-- REF / SNAPSHOTS (bootstrap from Parquet for demos)
-- These mirror your initial parquet files for quick reference or reproducibility.
-- ======================================================

-- Store inventory snapshot (from store_inventory_final.parquet)
CREATE TABLE IF NOT EXISTS ref.store_inventory_snapshot (
  snapshot_ts      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  shelf_id         TEXT NOT NULL,
  aisle            INTEGER NULL,
  item_weight      NUMERIC(10,3) NULL,
  shelf_weight     NUMERIC(12,3) NULL,
  item_category    TEXT NULL,
  item_subcategory TEXT NULL,
  item_visibility  NUMERIC(10,6) NULL,
  maximum_stock    INTEGER NULL,
  current_stock    INTEGER NULL,
  item_price       NUMERIC(10,2) NULL,
  PRIMARY KEY (snapshot_ts, shelf_id)
);

-- Store batches snapshot (from store_batched.parquet)
CREATE TABLE IF NOT EXISTS ref.store_batches_snapshot (
  snapshot_ts               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  shelf_id                  TEXT NOT NULL,
  batch_code                TEXT NOT NULL,
  item_category             TEXT NULL,
  item_subcategory          TEXT NULL,
  standard_batch_size       INTEGER NULL,
  received_date             DATE NULL,
  expiry_date               DATE NULL,
  batch_quantity_total      INTEGER NULL,
  batch_quantity_store      INTEGER NULL,
  batch_quantity_warehouse  INTEGER NULL,
  location                  TEXT NULL,   -- 'in-store' or 'warehouse'
  PRIMARY KEY (snapshot_ts, shelf_id, batch_code)
);

-- Warehouse inventory snapshot (from warehouse_inventory_final.parquet)
CREATE TABLE IF NOT EXISTS ref.warehouse_inventory_snapshot (
  snapshot_ts      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  shelf_id         TEXT NOT NULL,
  aisle            INTEGER NULL,
  item_weight      NUMERIC(10,3) NULL,
  shelf_weight     NUMERIC(12,3) NULL,
  item_category    TEXT NULL,
  item_subcategory TEXT NULL,
  maximum_stock    INTEGER NULL,
  current_stock    INTEGER NULL,
  item_price       NUMERIC(10,2) NULL,
  PRIMARY KEY (snapshot_ts, shelf_id)
);

-- Warehouse batches snapshot (from warehouse_batches.parquet)
CREATE TABLE IF NOT EXISTS ref.warehouse_batches_snapshot (
  snapshot_ts               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  shelf_id                  TEXT NOT NULL,
  batch_code                TEXT NOT NULL,
  item_category             TEXT NULL,
  item_subcategory          TEXT NULL,
  standard_batch_size       INTEGER NULL,
  received_date             DATE NULL,
  expiry_date               DATE NULL,
  batch_quantity_total      INTEGER NULL,
  batch_quantity_store      INTEGER NULL,
  batch_quantity_warehouse  INTEGER NULL,
  location                  TEXT NULL,   -- 'warehouse'
  PRIMARY KEY (snapshot_ts, shelf_id, batch_code)
);

-- ======================================================
-- ANALYTICS (discounts, features, etc.)
-- ======================================================

-- Daily discounts (Spark output; also on Kafka `daily_discounts`)
CREATE TABLE IF NOT EXISTS analytics.daily_discounts (
  shelf_id       TEXT NOT NULL,
  discount_date  DATE NOT NULL,               -- es. '2025-12-12'
  discount       NUMERIC(5,2) NOT NULL DEFAULT 0.00,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (shelf_id, discount_date)
);

-- Placeholder for curated features (for forecasting/ML)
CREATE TABLE IF NOT EXISTS analytics.features_store (
  feature_date  DATE NOT NULL,
  shelf_id      TEXT NOT NULL,
  foot_traffic  INTEGER NULL,
  sales_qty     INTEGER NULL,
  stockout_events INTEGER NULL,
  label_next_day_stockout BOOLEAN NULL,
  PRIMARY KEY (feature_date, shelf_id)
);

-- ======================================================
-- ANALYTICS / FEATURES (bootstrap from CSV, then append-only)
-- ======================================================

CREATE TABLE IF NOT EXISTS analytics.shelf_daily_features (
  shelf_id TEXT NOT NULL,
  feature_date DATE NOT NULL,

  item_category TEXT,
  item_subcategory TEXT,

  day_of_week SMALLINT,
  is_weekend BOOLEAN,

  warehouse_inbound_day BOOLEAN,
  refill_day BOOLEAN,

  item_price NUMERIC(10,2),
  discount NUMERIC(5,2),
  is_discounted BOOLEAN,
  is_discounted_next_7d BOOLEAN,

  people_count INTEGER,
  sales_qty INTEGER,
  sales_last_1d INTEGER,
  sales_last_7d INTEGER,
  sales_last_14d INTEGER,

  stockout_events INTEGER,

  shelf_capacity INTEGER,
  current_stock_shelf INTEGER,
  shelf_fill_ratio NUMERIC(5,4),
  shelf_threshold_qty INTEGER,
  expired_qty_shelf INTEGER,
  alerts_last_30d_shelf INTEGER,
  is_shelf_alert BOOLEAN,

  warehouse_capacity INTEGER,
  current_stock_warehouse INTEGER,
  warehouse_fill_ratio NUMERIC(5,4),
  wh_reorder_point_qty INTEGER,
  pending_supplier_qty INTEGER,
  expired_qty_wh INTEGER,
  alerts_last_30d_wh INTEGER,
  is_warehouse_alert BOOLEAN,
  moved_wh_to_shelf INTEGER,

  standard_batch_size INTEGER,
  min_expiration_days INTEGER,
  avg_expiration_days NUMERIC(6,2),
  qty_expiring_next_7d INTEGER,
  batches_to_order INTEGER,

  label_next_day_stockout BOOLEAN,

  snapshot_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  PRIMARY KEY (shelf_id, feature_date)
);

-- registry modelli (necessaria a training + inference)
CREATE TABLE IF NOT EXISTS analytics.ml_models (
  model_name    TEXT PRIMARY KEY,
  model_version TEXT NOT NULL,
  trained_at    TIMESTAMPTZ NOT NULL,
  metrics_json  JSONB,
  artifact_path TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_ml_models_name
  ON analytics.ml_models (model_name);

-- log predizioni (necessaria se vuoi tracciare cosa ha predetto il modello)
-- analytics.ml_predictions_log
CREATE TABLE IF NOT EXISTS analytics.ml_predictions_log (
  model_name        TEXT NOT NULL,
  feature_date      DATE NOT NULL,
  shelf_id          TEXT NOT NULL,
  predicted_batches INTEGER NOT NULL,
  suggested_qty     INTEGER NOT NULL,
  model_version     TEXT NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  PRIMARY KEY (model_name, feature_date, shelf_id, model_version),

  CONSTRAINT fk_ml_pred_model
    FOREIGN KEY (model_name)
    REFERENCES analytics.ml_models(model_name)
    ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_ml_pred_log_date
  ON analytics.ml_predictions_log (feature_date);

CREATE INDEX IF NOT EXISTS ix_ml_pred_log_shelf_date
  ON analytics.ml_predictions_log (shelf_id, feature_date DESC);

CREATE INDEX IF NOT EXISTS ix_ml_pred_log_modelver_date
  ON analytics.ml_predictions_log (model_name, model_version, feature_date DESC);




-- ======================================================
-- HOUSEKEEPING TRIGGERS (auto-update updated_at)
-- ======================================================
-- CREATE OR REPLACE FUNCTION set_updated_at()
-- RETURNS TRIGGER AS $$
-- BEGIN
--   NEW.updated_at = NOW();
--   RETURN NEW;
-- END;
-- $$ LANGUAGE plpgsql;

-- DO $$
-- BEGIN
--   IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'tr_alert_rules_set_updated_at') THEN
--     CREATE TRIGGER tr_alert_rules_set_updated_at
--     BEFORE UPDATE ON config.alert_rules
--     FOR EACH ROW EXECUTE FUNCTION set_updated_at();
--   END IF;

--   IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'tr_shelf_restock_plan_set_updated_at') THEN
--     CREATE TRIGGER tr_shelf_restock_plan_set_updated_at
--     BEFORE UPDATE ON ops.shelf_restock_plan
--     FOR EACH ROW EXECUTE FUNCTION set_updated_at();
--   END IF;

--   IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'tr_alerts_set_updated_at') THEN
--     CREATE TRIGGER tr_alerts_set_updated_at
--     BEFORE UPDATE ON ops.alerts
--     FOR EACH ROW EXECUTE FUNCTION set_updated_at();
--   END IF;

--   IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'tr_shelf_profiles_set_updated_at') THEN
--     CREATE TRIGGER tr_shelf_profiles_set_updated_at
--     BEFORE UPDATE ON config.shelf_profiles
--     FOR EACH ROW EXECUTE FUNCTION set_updated_at();
--   END IF;
-- END$$;
