-- =====================================================================
-- MAKE DB - Schema + bootstrap iniziale (CSV -> Postgres)
-- =====================================================================

\set ON_ERROR_STOP on
BEGIN;

-- ---------- anagrafiche ----------
CREATE TABLE IF NOT EXISTS locations (
  location_id SERIAL PRIMARY KEY,
  location    TEXT NOT NULL UNIQUE,
  CONSTRAINT chk_location_type CHECK (location IN ('instore', 'warehouse'))
);

CREATE TABLE IF NOT EXISTS categories (
  category_id   SERIAL PRIMARY KEY,
  category_name TEXT  NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS items (
  item_id     BIGSERIAL PRIMARY KEY,
  shelf_id    VARCHAR(32) NOT NULL,
  category_id INT        NOT NULL REFERENCES categories(category_id),
  CONSTRAINT uq_items_shelf UNIQUE (shelf_id)
);

CREATE TABLE IF NOT EXISTS sensor_balance (
  item_id       BIGINT      NOT NULL,
  location_id   INT         NOT NULL,
  pending_delta INT         NOT NULL DEFAULT 0,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (item_id, location_id)
);

CREATE TABLE IF NOT EXISTS product_inventory (
  item_id         BIGINT NOT NULL REFERENCES items(item_id),
  location_id     INT    NOT NULL REFERENCES locations(location_id),
  item_weight     FLOAT,
  shelf_weight    FLOAT,
  item_visibility FLOAT,
  initial_stock   INT,
  current_stock   INT,
  price           NUMERIC(12,4),
  PRIMARY KEY (item_id, location_id)
);

CREATE TABLE IF NOT EXISTS batches (
  batch_id      BIGSERIAL PRIMARY KEY,
  item_id       BIGINT NOT NULL REFERENCES items(item_id),
  batch_code    VARCHAR(30) NOT NULL,
  received_date DATE NOT NULL,
  expiry_date   DATE,
  CONSTRAINT uq_batches UNIQUE (item_id, batch_code),
  CONSTRAINT chk_expiry_after_received CHECK (expiry_date IS NULL OR expiry_date >= received_date)
);

CREATE TABLE IF NOT EXISTS batch_inventory (
  batch_id    BIGINT NOT NULL REFERENCES batches(batch_id) ON DELETE CASCADE,
  location_id INT    NOT NULL REFERENCES locations(location_id),
  quantity    INT    NOT NULL,
  PRIMARY KEY (batch_id, location_id),
  CONSTRAINT chk_qty_nonneg CHECK (quantity >= 0)
);

CREATE TABLE IF NOT EXISTS shelf_events (
  event_id      TEXT PRIMARY KEY,
  item_id       BIGINT      NOT NULL REFERENCES items(item_id),
  shelf_id      VARCHAR(32) NOT NULL,
  event_type    TEXT        NOT NULL CHECK (event_type IN ('pickup','putback','restock','sensor_noise')),
  qty_est       INT,
  weight_change DOUBLE PRECISION,
  event_time    TIMESTAMPTZ NOT NULL,
  meta          JSONB
);

CREATE TABLE IF NOT EXISTS receipts (
  receipt_id     BIGSERIAL PRIMARY KEY,
  transaction_id TEXT UNIQUE NOT NULL,
  customer_id    TEXT,
  business_date  DATE        NOT NULL,
  closed_at      TIMESTAMPTZ NOT NULL,
  total_net      NUMERIC(12,2) NOT NULL DEFAULT 0,
  total_tax      NUMERIC(12,2) NOT NULL DEFAULT 0,
  total_gross    NUMERIC(12,2) NOT NULL DEFAULT 0,
  status         TEXT          NOT NULL DEFAULT 'CLOSED'
);

CREATE TABLE IF NOT EXISTS receipt_lines (
  receipt_line_id BIGSERIAL PRIMARY KEY,
  receipt_id      BIGINT NOT NULL REFERENCES receipts(receipt_id) ON DELETE CASCADE,
  shelf_id        VARCHAR(32) NOT NULL,
  quantity        INT NOT NULL,
  unit_price      NUMERIC(12,4) NOT NULL,
  discount        NUMERIC(12,4) NOT NULL DEFAULT 0,
  total_price     NUMERIC(14,4) NOT NULL
);

CREATE TABLE IF NOT EXISTS stream_events (
  event_id  TEXT PRIMARY KEY,
  source    TEXT        NOT NULL,
  event_ts  TIMESTAMPTZ NOT NULL,
  payload   JSONB       NOT NULL,
  ingest_ts TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_stream_events_source_ts ON stream_events(source, event_ts);

CREATE TABLE IF NOT EXISTS inventory_ledger (
  ledger_id   BIGSERIAL PRIMARY KEY,
  event_id    TEXT NOT NULL UNIQUE,
  event_ts    TIMESTAMPTZ NOT NULL,
  item_id     BIGINT NOT NULL,
  location_id INT    NOT NULL REFERENCES locations(location_id),
  delta_qty   INT    NOT NULL,
  reason      TEXT   NOT NULL,
  batch_id    BIGINT,
  meta        JSONB,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ledger_item_loc_ts ON inventory_ledger(item_id, location_id, event_ts);

CREATE TABLE IF NOT EXISTS alerts (
  alert_id     BIGSERIAL PRIMARY KEY,
  rule_key     TEXT        NOT NULL,
  severity     TEXT        NOT NULL DEFAULT 'WARN',
  status       TEXT        NOT NULL DEFAULT 'OPEN',
  item_id      BIGINT,
  location_id  INT,
  batch_id     BIGINT,
  opened_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  resolved_at  TIMESTAMPTZ,
  message      TEXT,
  value_num    NUMERIC,
  meta         JSONB
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_alert_open
  ON alerts(rule_key, COALESCE(item_id,0), COALESCE(location_id,0), COALESCE(batch_id,0))
  WHERE status='OPEN';

CREATE TABLE IF NOT EXISTS inventory_thresholds (
  threshold_id        SERIAL PRIMARY KEY,
  scope               TEXT NOT NULL CHECK (scope IN ('item','category','global')),
  item_id             BIGINT,
  category_id         INT,
  location_id         INT REFERENCES locations(location_id),
  low_stock_threshold INT,
  safety_stock        INT,
  near_expiry_days    INT,
  UNIQUE(scope, item_id, category_id, location_id)
);

-- ---------- historical weekly discounts ----------
CREATE TABLE IF NOT EXISTS discount_history (
  item_id     BIGINT NOT NULL REFERENCES items(item_id),
  week        TEXT   NOT NULL,               -- esempio: '2025-W34'
  discount    NUMERIC(5,4) NOT NULL,         -- esempio: 0.15
  created_at  TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (item_id, week)
);

-- =====================================================================
-- ML Dataset - Previsione refill warehouse
-- =====================================================================

CREATE TABLE IF NOT EXISTS ml_refill_dataset (
  item_id           BIGINT NOT NULL REFERENCES items(item_id),
  location_id       INT    NOT NULL REFERENCES locations(location_id),
  business_date     DATE   NOT NULL,

  -- Calendario
  weekday           TEXT   NOT NULL,
  is_weekend        BOOLEAN NOT NULL,
  is_holiday        BOOLEAN DEFAULT FALSE,

  -- Stock e vendite
  current_stock     INT,
  sold_qty          INT,
  refill_qty        INT,
  rolling_avg_7d    NUMERIC,

  -- Prezzi e sconti
  mean_price        NUMERIC(10,4),
  discount_applied  BOOLEAN DEFAULT FALSE,
  discount_rate     NUMERIC(5,4),

  -- Audit
  created_at        TIMESTAMPTZ DEFAULT now(),

  PRIMARY KEY (item_id, location_id, business_date)
);

-- =====================================================================
-- DIMENSIONE DATE - Calendar table per ML / analisi
-- =====================================================================

CREATE TABLE IF NOT EXISTS dim_date (
  date             DATE PRIMARY KEY,
  year             INT NOT NULL,
  month            INT NOT NULL,
  month_name       TEXT NOT NULL,
  day              INT NOT NULL,
  weekday          TEXT NOT NULL,
  weekday_index    INT NOT NULL,
  is_weekend       BOOLEAN NOT NULL,
  is_holiday       BOOLEAN DEFAULT FALSE,
  week_of_year     INT NOT NULL,
  quarter          INT NOT NULL,
  season           TEXT
);

CREATE INDEX IF NOT EXISTS idx_items_shelf    ON items(shelf_id);
CREATE INDEX IF NOT EXISTS idx_pi_item_loc    ON product_inventory(item_id, location_id);
CREATE INDEX IF NOT EXISTS idx_batches_item   ON batches(item_id);
CREATE INDEX IF NOT EXISTS idx_batches_expiry ON batches(expiry_date);
CREATE INDEX IF NOT EXISTS idx_batchinv_loc   ON batch_inventory(location_id);

-- ---------- parametri path ----------
\set store_inventory '/data/store_inventory_final.csv'
\set wh_inventory    '/data/warehouse_inventory_final.csv'
\set store_batches   '/data/store_batches.csv'
\set wh_batches      '/data/warehouse_batches.csv'

-- ---------- guard rails ----------
DO $$
DECLARE
  n_items BIGINT; n_categories BIGINT; n_inventory BIGINT; n_batches BIGINT; n_batch_inventory BIGINT;
BEGIN
  SELECT COUNT(*) INTO n_items           FROM items;
  SELECT COUNT(*) INTO n_categories      FROM categories;
  SELECT COUNT(*) INTO n_inventory       FROM product_inventory;
  SELECT COUNT(*) INTO n_batches         FROM batches;
  SELECT COUNT(*) INTO n_batch_inventory FROM batch_inventory;
  IF n_items > 0 OR n_categories > 0 OR n_inventory > 0 OR n_batches > 0 OR n_batch_inventory > 0 THEN
    RAISE EXCEPTION 'dati già presenti (items=%, categories=%, inventory=%, batches=%, batch_inventory=%). Interrompo per sicurezza.',
      n_items, n_categories, n_inventory, n_batches, n_batch_inventory;
  END IF;
END$$;

-- ---------- seed ----------
INSERT INTO locations (location) VALUES ('instore'), ('warehouse')
ON CONFLICT (location) DO NOTHING;

-- ---------- staging ----------
CREATE TEMP TABLE staging_inventory_raw (
  shelf_id TEXT,
  item_category TEXT,
  item_weight NUMERIC,
  shelf_weight NUMERIC,
  item_visibility NUMERIC,
  initial_stock NUMERIC,
  current_stock NUMERIC,
  time_stamp TIMESTAMP NULL,
  price NUMERIC NULL
) ON COMMIT DROP;

CREATE TEMP TABLE staging_inventory (
  shelf_id TEXT,
  item_category TEXT,
  item_weight NUMERIC,
  shelf_weight NUMERIC,
  item_visibility NUMERIC,
  initial_stock NUMERIC,
  current_stock NUMERIC,
  time_stamp TIMESTAMP NULL,
  price NUMERIC NULL,
  location TEXT
) ON COMMIT DROP;

CREATE TEMP TABLE staging_batches_raw (
  shelf_id TEXT,
  batch_code VARCHAR(30),
  received_date DATE,
  expiry_date DATE,
  batch_quantity_total NUMERIC NULL,
  batch_quantity_warehouse NUMERIC NULL,
  batch_quantity_store NUMERIC NULL,
  location TEXT NULL
) ON COMMIT DROP;

CREATE TEMP TABLE staging_batches (
  shelf_id TEXT,
  batch_code VARCHAR(30),
  received_date DATE,
  expiry_date DATE,
  quantity NUMERIC,
  location TEXT
) ON COMMIT DROP;

-- ---------- COPY + arricchimento (INVENTORY) ----------
-- STORE
COPY staging_inventory_raw
  (shelf_id, item_category, item_weight, shelf_weight, item_visibility, initial_stock, current_stock, time_stamp, price)
FROM :'store_inventory' WITH (FORMAT csv, HEADER true);

INSERT INTO staging_inventory
  (shelf_id, item_category, item_weight, shelf_weight, item_visibility, initial_stock, current_stock, time_stamp, price, location)
SELECT shelf_id, item_category, item_weight, shelf_weight, item_visibility,
       initial_stock, current_stock, time_stamp, price, 'instore'::text
FROM staging_inventory_raw;

-- WAREHOUSE
TRUNCATE staging_inventory_raw;

COPY staging_inventory_raw
  (shelf_id, item_category, item_weight, shelf_weight, item_visibility, initial_stock, current_stock, time_stamp)
FROM :'wh_inventory' WITH (FORMAT csv, HEADER true);

INSERT INTO staging_inventory
  (shelf_id, item_category, item_weight, shelf_weight, item_visibility, initial_stock, current_stock, time_stamp, price, location)
SELECT shelf_id, item_category, item_weight, shelf_weight, item_visibility,
       initial_stock, current_stock, time_stamp, NULL::numeric, 'warehouse'::text
FROM staging_inventory_raw;

-- ---------- normalizzazione da INVENTORY ----------
INSERT INTO categories (category_name)
SELECT DISTINCT si.item_category
FROM staging_inventory si
WHERE si.item_category IS NOT NULL AND si.item_category <> ''
ON CONFLICT (category_name) DO NOTHING;

INSERT INTO items (shelf_id, category_id)
SELECT DISTINCT si.shelf_id, c.category_id
FROM staging_inventory si
JOIN categories c ON c.category_name = si.item_category
WHERE si.shelf_id IS NOT NULL AND si.shelf_id <> ''
ON CONFLICT (shelf_id) DO NOTHING;

-- ---------- product_inventory ----------
INSERT INTO product_inventory (
  item_id, location_id, item_weight, shelf_weight, item_visibility, initial_stock, current_stock, price
)
SELECT
  i.item_id,
  l.location_id,
  si.item_weight,
  si.shelf_weight,
  si.item_visibility,
  ROUND(si.initial_stock)::int,
  ROUND(si.current_stock)::int,
  CASE WHEN l.location='instore' THEN si.price ELSE NULL END
FROM staging_inventory si
JOIN items i     ON i.shelf_id = si.shelf_id
JOIN locations l ON l.location  = si.location
WHERE si.shelf_id IS NOT NULL AND si.shelf_id <> ''
ON CONFLICT (item_id, location_id) DO NOTHING;

-- ---------- COPY + arricchimento (BATCHES) ----------
TRUNCATE staging_batches_raw;

-- append STORE
COPY staging_batches_raw
  (shelf_id,batch_code,received_date,expiry_date,batch_quantity_total,batch_quantity_warehouse,batch_quantity_store,location)
FROM :'store_batches' WITH (FORMAT csv, HEADER true);

-- append WAREHOUSE (senza truncate!)
COPY staging_batches_raw
  (shelf_id,batch_code,received_date,expiry_date,batch_quantity_total,batch_quantity_warehouse,batch_quantity_store,location)
FROM :'wh_batches' WITH (FORMAT csv, HEADER true);

-- normalizza per location → staging_batches
TRUNCATE staging_batches;

INSERT INTO staging_batches
  (shelf_id, batch_code, received_date, expiry_date, quantity, location)
SELECT shelf_id, batch_code, received_date, expiry_date,
       COALESCE(batch_quantity_store, batch_quantity_total, 0) AS quantity,
       'instore'::text
FROM staging_batches_raw
WHERE location = 'instore' AND COALESCE(batch_quantity_store, batch_quantity_total, 0) > 0;

INSERT INTO staging_batches
  (shelf_id, batch_code, received_date, expiry_date, quantity, location)
SELECT shelf_id, batch_code, received_date, expiry_date,
       COALESCE(batch_quantity_warehouse, batch_quantity_total, 0) AS quantity,
       'warehouse'::text
FROM staging_batches_raw
WHERE location = 'warehouse' AND COALESCE(batch_quantity_warehouse, batch_quantity_total, 0) > 0;

-- dimensione lotti
INSERT INTO batches (item_id, batch_code, received_date, expiry_date)
SELECT DISTINCT i.item_id, sb.batch_code, sb.received_date, sb.expiry_date
FROM staging_batches sb
JOIN items i ON i.shelf_id = sb.shelf_id
WHERE sb.batch_code IS NOT NULL AND sb.batch_code <> ''
ON CONFLICT (item_id, batch_code) DO NOTHING;

-- quantità per location
INSERT INTO batch_inventory (batch_id, location_id, quantity)
SELECT b.batch_id, l.location_id, ROUND(sb.quantity)::int
FROM staging_batches sb
JOIN items     i ON i.shelf_id = sb.shelf_id
JOIN batches   b ON b.item_id = i.item_id AND b.batch_code = sb.batch_code
JOIN locations l ON l.location = sb.location
WHERE sb.quantity > 0
ON CONFLICT (batch_id, location_id) DO NOTHING;

-- ---------- soglie ----------
INSERT INTO inventory_thresholds (scope, location_id, low_stock_threshold, safety_stock, near_expiry_days)
SELECT 'global', l.location_id,
       CASE WHEN l.location='instore'   THEN 5  ELSE 20 END,
       CASE WHEN l.location='instore'   THEN 2  ELSE 5  END,
       CASE WHEN l.location='instore'   THEN 3  ELSE 7  END
FROM locations l
WHERE l.location IN ('instore','warehouse')
ON CONFLICT (scope, item_id, category_id, location_id) DO NOTHING;

INSERT INTO inventory_thresholds (scope, category_id, location_id, low_stock_threshold, safety_stock, near_expiry_days)
SELECT 'category', c.category_id, l.location_id,
       CASE WHEN l.location='instore' THEN 8 ELSE 30 END,
       CASE WHEN l.location='instore' THEN 3 ELSE 8  END,
       CASE WHEN l.location='instore' THEN 2 ELSE 5  END
FROM categories c
CROSS JOIN locations l
WHERE lower(c.category_name) ~ '^(fresh|fresco|perish|deperib|dairy|lattic|meat|carne|fish|pesce|produce|frutta|verdura|bakery|pane|pasticc)'
ON CONFLICT (scope, item_id, category_id, location_id) DO NOTHING;

INSERT INTO inventory_thresholds (scope, item_id, location_id, low_stock_threshold, safety_stock, near_expiry_days)
SELECT 'item', pi.item_id, pi.location_id,
       CASE
         WHEN COALESCE(pi.initial_stock, pi.current_stock, 0) <= 0 THEN
           CASE WHEN l.location='instore' THEN 5 ELSE 20 END
         ELSE
           CASE
             WHEN l.location='instore' THEN GREATEST(1, LEAST(25, CEIL(COALESCE(pi.initial_stock, pi.current_stock)::numeric * 0.10))::int)
             ELSE                          GREATEST(1, LEAST(50, CEIL(COALESCE(pi.initial_stock, pi.current_stock)::numeric * 0.10))::int)
           END
       END,
       NULL::int, NULL::int
FROM product_inventory pi
JOIN locations l ON l.location_id = pi.location_id
ON CONFLICT (scope, item_id, category_id, location_id) DO NOTHING;

ANALYZE;
COMMIT;

-- NOTE: dopo il bootstrap, tutte le modifiche passano dai job Kafka->DB (funzioni in db_management.sql)
