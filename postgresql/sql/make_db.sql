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

CREATE TABLE shelf_status (
    shelf_id TEXT PRIMARY KEY,
    status TEXT
);


-- =====================================================================
-- >>> NUOVE TABELLE PER FOOT TRAFFIC <<<
-- =====================================================================

-- Eventi “grezzi” idempotenti (utile anche per audit)
CREATE TABLE IF NOT EXISTS foot_traffic_events (
  event_id   TEXT PRIMARY KEY,                              -- generato dal consumer Spark
  event_type TEXT NOT NULL CHECK (event_type IN ('entry','exit')),
  event_time TIMESTAMPTZ NOT NULL,
  weekday    TEXT,                                          -- opzionale, dal producer
  time_slot  TEXT,                                          -- es. "07:00–09:59"
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ft_events_time ON foot_traffic_events(event_time);

-- Tabella per dashboard: contatore live nel tempo (come richiesto)
CREATE TABLE IF NOT EXISTS foot_traffic_counter (
  id                    BIGSERIAL PRIMARY KEY,
  event_type            TEXT NOT NULL CHECK (event_type IN ('entry','exit')),
  event_time            TIMESTAMPTZ NOT NULL,
  current_foot_traffic  INT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ft_counter_time ON foot_traffic_counter(event_time);

-- Aggregato per timeslot (giorno x timeslot)
CREATE TABLE IF NOT EXISTS foot_traffic_timeslot_agg (
  business_date DATE NOT NULL,
  weekday       TEXT,
  time_slot     TEXT NOT NULL,
  total_entries INT  NOT NULL DEFAULT 0,
  total_exits   INT  NOT NULL DEFAULT 0,
  net_traffic   INT  NOT NULL DEFAULT 0,
  PRIMARY KEY (business_date, time_slot)
);

-- Stato “live” (singola riga) per occupazione attuale
CREATE TABLE IF NOT EXISTS foot_traffic_state (
  id          SMALLINT PRIMARY KEY DEFAULT 1,
  current_cnt INT NOT NULL DEFAULT 0,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO foot_traffic_state(id, current_cnt)
VALUES (1, 0) ON CONFLICT (id) DO NOTHING;

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

-- ---------- seed ----------
INSERT INTO locations (location) VALUES ('instore'), ('warehouse')
ON CONFLICT (location) DO NOTHING;

-- ---------- parametri path ----------
\set store_inventory '/data/store_inventory_final.csv'
\set wh_inventory    '/data/warehouse_inventory_final.csv'
\set store_batches   '/data/store_batches.csv'
\set wh_batches      '/data/warehouse_batches.csv'

-- ---------- soft guard rails: log-only ----------
DO $$
DECLARE
  n_items BIGINT; n_categories BIGINT; n_inventory BIGINT; n_batches BIGINT; n_batch_inventory BIGINT;
BEGIN
  SELECT COUNT(*) INTO n_items           FROM items;
  SELECT COUNT(*) INTO n_categories      FROM categories;
  SELECT COUNT(*) INTO n_inventory       FROM product_inventory;
  SELECT COUNT(*) INTO n_batches         FROM batches;
  SELECT COUNT(*) INTO n_batch_inventory FROM batch_inventory;

  IF n_items = 0 AND n_categories = 0 AND n_inventory = 0 AND n_batches = 0 AND n_batch_inventory = 0 THEN
    RAISE NOTICE 'Bootstrap: DB vuoto → carico i CSV.';
  ELSE
    RAISE NOTICE 'Bootstrap saltato: dati già presenti (items=%, categories=%, inventory=%, batches=%, batch_inventory=%).',
      n_items, n_categories, n_inventory, n_batches, n_batch_inventory;
  END IF;
END$$;

-- ---------- calcolo :do_bootstrap (1 se DB vuoto) ----------
WITH c AS (
  SELECT
    (SELECT COUNT(*) FROM items)            AS n_items,
    (SELECT COUNT(*) FROM categories)       AS n_categories,
    (SELECT COUNT(*) FROM product_inventory) AS n_inventory,
    (SELECT COUNT(*) FROM batches)          AS n_batches,
    (SELECT COUNT(*) FROM batch_inventory)  AS n_batch_inventory
)
SELECT CASE WHEN n_items=0 AND n_categories=0 AND n_inventory=0 AND n_batches=0 AND n_batch_inventory=0
            THEN 1 ELSE 0 END AS do_bootstrap
FROM c
\gset

\if :do_bootstrap
  \echo '>>> Eseguo bootstrap CSV...'

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
-- ============================================================
-- (1) Fallback GLOBAL per instore/warehouse (opzionale ma consigliato)
-- ============================================================
WITH loc AS (
  SELECT location_id, location
  FROM locations
  WHERE location IN ('instore','warehouse')
)
INSERT INTO inventory_thresholds(scope, location_id, low_stock_threshold, safety_stock, near_expiry_days)
SELECT 'global', l.location_id,
       CASE WHEN l.location='instore' THEN 5 ELSE 20 END,
       CASE WHEN l.location='instore' THEN 2 ELSE 5  END,
       CASE WHEN l.location='instore' THEN 3 ELSE 7  END
FROM loc l;

-- ============================================================
-- (2) ITEM + LOCATION: soglie da baseline (CSV) e near‑expiry da lotti
--     baseline = GREATEST(initial_stock, current_stock)
--     instore:  low = ceil(15%) clamp 2..30 ; safety = ceil(7%)  clamp 1..15
--     wh:       low = ceil(25%) clamp 10..100; safety = ceil(10%) clamp 5..50
--     near-expiry: life_days = avg(expiry - received) (pesata su qty)
--       instore:  ceil(life*0.20) clamp 2..14 (fallback 3)
--       wh:       ceil(life*0.35) clamp 5..30 (fallback 7)
-- ============================================================
WITH
loc AS (
  SELECT
    MAX(CASE WHEN location='instore'  THEN location_id END) AS instore_id,
    MAX(CASE WHEN location='warehouse' THEN location_id END) AS warehouse_id
  FROM locations
),
base AS (
  SELECT
    pi.item_id,
    pi.location_id,
    GREATEST(COALESCE(pi.initial_stock,0), COALESCE(pi.current_stock,0))::int AS baseline
  FROM product_inventory pi
),
life AS (
  SELECT
    b.item_id,
    CASE WHEN SUM(COALESCE(bi.quantity,0)) > 0
         THEN ROUND( SUM( GREATEST(1, (b.expiry_date - b.received_date)) * COALESCE(bi.quantity,0) )
                     / SUM(COALESCE(bi.quantity,0)) )::int
         ELSE NULL END AS life_days
  FROM batches b
  JOIN batch_inventory bi ON bi.batch_id = b.batch_id
  WHERE b.expiry_date IS NOT NULL
  GROUP BY b.item_id
),
thr AS (
  SELECT
    b.item_id,
    b.location_id,
    b.baseline,
    CASE
      WHEN b.location_id = (SELECT instore_id FROM loc) THEN LEAST( GREATEST(2,  CEIL(b.baseline * 0.15)::int), 30 )
      ELSE                                                LEAST( GREATEST(10, CEIL(b.baseline * 0.25)::int), 100)
    END AS low_thr,
    CASE
      WHEN b.location_id = (SELECT instore_id FROM loc) THEN LEAST( GREATEST(1,  CEIL(b.baseline * 0.07)::int), 15 )
      ELSE                                                LEAST( GREATEST(5,  CEIL(b.baseline * 0.10)::int), 50 )
    END AS safety_thr
  FROM base b
),
near AS (
  SELECT
    b.item_id,
    b.location_id,
    CASE
      WHEN b.location_id = (SELECT instore_id FROM loc)
        THEN LEAST( GREATEST(2,  CEIL(COALESCE(l.life_days, 3) * 0.20)::int), 14 )
      ELSE LEAST( GREATEST(5,  CEIL(COALESCE(l.life_days, 7) * 0.35)::int), 30 )
    END AS near_expiry_days
  FROM base b
  LEFT JOIN life l ON l.item_id = b.item_id
)
INSERT INTO inventory_thresholds(scope, item_id, location_id, low_stock_threshold, safety_stock, near_expiry_days)
SELECT
  'item'::text,
  t.item_id,
  t.location_id,
  NULLIF(t.low_thr,    0),
  NULLIF(t.safety_thr, 0),
  n.near_expiry_days
FROM thr t
JOIN near n ON n.item_id = t.item_id AND n.location_id = t.location_id;
-- ============================================================
-- (3) POPOLA shelf_status in base alle nuove soglie (solo INSTORE)
--     Regole:
--       - critical: current_stock <= 0  OR  current_stock <= low_thr
--       - near:     current_stock <= low_thr + GREATEST(safety_thr, 1)
--       - ok:       altrimenti
--     Fallback soglie: ITEM -> GLOBAL (stesso location_id o NULL)
-- ============================================================
WITH per_shelf AS (
  SELECT
      i.shelf_id,
      pi.current_stock,
      COALESCE(
        (SELECT t.low_stock_threshold
           FROM inventory_thresholds t
          WHERE t.scope = 'item'
            AND t.item_id = pi.item_id
            AND (t.location_id = pi.location_id OR t.location_id IS NULL)
          ORDER BY t.location_id NULLS LAST
          LIMIT 1),
        (SELECT t.low_stock_threshold
           FROM inventory_thresholds t
          WHERE t.scope = 'global'
            AND (t.location_id = pi.location_id OR t.location_id IS NULL)
          ORDER BY t.location_id NULLS LAST
          LIMIT 1),
        0
      ) AS low_thr,
      COALESCE(
        (SELECT t.safety_stock
           FROM inventory_thresholds t
          WHERE t.scope = 'item'
            AND t.item_id = pi.item_id
            AND (t.location_id = pi.location_id OR t.location_id IS NULL)
          ORDER BY t.location_id NULLS LAST
          LIMIT 1),
        (SELECT t.safety_stock
           FROM inventory_thresholds t
          WHERE t.scope = 'global'
            AND (t.location_id = pi.location_id OR t.location_id IS NULL)
          ORDER BY t.location_id NULLS LAST
          LIMIT 1),
        0
      ) AS safety_thr
  FROM product_inventory pi
  JOIN items     i ON i.item_id = pi.item_id
  JOIN locations l ON l.location_id = pi.location_id
  WHERE l.location = 'instore'
)
INSERT INTO shelf_status (shelf_id, status)
SELECT
  shelf_id,
  CASE
    WHEN current_stock <= 0 THEN 'critical'
    WHEN current_stock <= low_thr THEN 'critical'
    WHEN current_stock <= (low_thr + GREATEST(safety_thr, 1)) THEN 'near'
    ELSE 'ok'
  END AS status
FROM per_shelf
ON CONFLICT (shelf_id) DO UPDATE
SET status = EXCLUDED.status;

\else
  \echo '>>> Bootstrap CSV saltato (DB non vuoto).'
\endif

ANALYZE;
COMMIT;

-- NOTE: dopo il bootstrap, tutte le modifiche passano dai job Kafka->DB (funzioni in db_management.sql)
