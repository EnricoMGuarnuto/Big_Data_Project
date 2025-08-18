-- supporting tables
CREATE TABLE IF NOT EXISTS locations (
  location_id SERIAL PRIMARY KEY,
  location TEXT NOT NULL UNIQUE,
  CONSTRAINT chk_location_type CHECK (location IN ('instore', 'warehouse'))
);

CREATE TABLE IF NOT EXISTS categories (
  category_id SERIAL PRIMARY KEY,
  category_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS items (
  item_id BIGSERIAL PRIMARY KEY,
  shelf_id VARCHAR(5) NOT NULL,
  category_id INT NOT NULL REFERENCES categories(category_id)
);

-- Accumula l’effetto netto dei sensori (pickup/putback) non ancora consolidato dal POS
CREATE TABLE IF NOT EXISTS sensor_balance (
  item_id       bigint      NOT NULL,
  location_id   int         NOT NULL,
  pending_delta int         NOT NULL DEFAULT 0,   -- pickup<0, putback>0
  updated_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (item_id, location_id)
);

-- main inventory table
CREATE TABLE IF NOT EXISTS product_inventory (
  item_id BIGINT NOT NULL REFERENCES items(item_id),
  location_id INT NOT NULL REFERENCES locations(location_id),
  item_weight FLOAT,
  shelf_weight FLOAT,
  item_visibility FLOAT,
  initial_stock INT,
  current_stock INT,
  PRIMARY KEY (item_id, location_id)
);

-- main batch tables
CREATE TABLE IF NOT EXISTS batches (
  batch_id BIGSERIAL PRIMARY KEY,
  item_id BIGINT NOT NULL REFERENCES items(item_id),
  batch_code VARCHAR(30) NOT NULL,
  received_date DATE NOT NULL,
  expiry_date DATE,
  CONSTRAINT uq_batches UNIQUE (item_id, batch_code),
  CONSTRAINT chk_expiry_after_received CHECK (expiry_date IS NULL OR expiry_date >= received_date)
);

CREATE TABLE IF NOT EXISTS batch_inventory (
  batch_id BIGINT NOT NULL REFERENCES batches(batch_id) ON DELETE CASCADE,
  location_id INT NOT NULL REFERENCES locations(location_id),
  quantity INT NOT NULL,
  PRIMARY KEY (batch_id, location_id),
  CONSTRAINT chk_qty_nonneg CHECK (quantity >= 0)
);

'''
CREATE TABLE IF NOT EXISTS refill_history (
    refill_id SERIAL PRIMARY KEY,
    item_id BIGINT NOT NULL REFERENCES items(item_id),
    shelf_id VARCHAR(5) NOT NULL,
    refill_time TIMESTAMP DEFAULT NOW(),
    quantity INT
);
-- Tabella eventi di refill per shelf
'''

'''
CREATE TABLE IF NOT EXISTS shelf_events (
    event_id SERIAL PRIMARY KEY,
    item_id BIGINT NOT NULL REFERENCES items(item_id),
    shelf_id VARCHAR(5) NOT NULL,
    event_type TEXT, -- "pickup (item preso da shelf)", "rimesso giù (item rimesso a posto)", "restock (item rifornito)"
    weight_change FLOAT, -- variazione di peso (positivo o negativo)
    event_time TIMESTAMP
    constraint chk_event_type CHECK (event_type IN ('pickup', 'putback', 'restock'));
);
-- Ogni volta che il sensore registra una variazione di peso generi un record qui.
-- Il consumer aggiornerà lo status in base a quanto succede dopo (acquisto o restituzione).
'''

CREATE TABLE IF NOT EXISTS receipts (
  receipt_id        bigserial primary key,
  business_date     date not null,                         -- data fiscale
  opened_at         timestamptz not null default now(),
  closed_at         timestamptz,
  -- register_id       ,                           -- cassa
  -- totali "fotografati" alla chiusura (riduce ricalcoli)
  total_net         numeric(12,2) not null default 0,    -- totale netto (senza tasse)
  total_tax         numeric(12,2) not null default 0,    -- totale tasse applicate (IVA)
  total_gross       numeric(12,2) not null default 0,    -- totale lordo (con tasse)
  status            text not null default 'OPEN'        -- e.g., OPEN, CLOSED, VOIDED
);
-- Ogni volta che viene effettuata una vendita, registra qui.

CREATE TABLE IF NOT EXISTS receipt_lines (
  receipt_line_id   bigserial primary key,
  receipt_id        bigint not null references receipts(receipt_id) on delete cascade,
  product_id        bigint not null references items(item_id),
  batch_id          bigint not null references batches(batch_id) 
  shelf_id          VARCHAR(5) not null,                   -- shelf_id del prodotto                        
  unit_price        numeric(12,4) not null,                  -- prezzo lordo unitario
  line_discount     numeric(12,2) not null default 0,        -- sconto riga (+ = sconto)
  tax_code          text not null default 'IVA',
  tax_rate          numeric(5,2) not null,                   -- percentuale di aliquota applicata alla riga
  -- calcoli deterministici come colonne generate (PG 12+)
  line_net          numeric(14,4) generated always as (unit_price - line_discount) stored,
  line_tax          numeric(14,4) generated always as (round((unit_price - line_discount) * tax_rate / 100.0, 4)) stored,
  line_gross        numeric(14,4) generated always as ( (unit_price - line_discount)
                                                       + round((unit_price - line_discount) * tax_rate / 100.0, 4) ) stored,
  constraint chk_prices_nonneg check (unit_price >= 0 and line_discount >= 0)
);

-- create index idx_lines_receipt on receipt_lines(receipt_id);


-- Eventi base (idempotenza: chiave (event_id, source))
CREATE TABLE IF NOT EXISTS stream_events (
  event_id        TEXT PRIMARY KEY,
  source          TEXT NOT NULL,         -- 'pos.sales', 'shelf.sensors', ...
  event_ts        TIMESTAMPTZ NOT NULL,
  payload         JSONB NOT NULL,
  ingest_ts       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Uniche “facilitanti” per tipi frequenti (opzionale)
CREATE INDEX IF NOT EXISTS idx_stream_events_source_ts ON stream_events(source, event_ts);

-- Libro mastro movimenti (verità storica)
CREATE TABLE IF NOT EXISTS inventory_ledger (
  ledger_id       BIGSERIAL PRIMARY KEY,
  event_id        TEXT NOT NULL UNIQUE,  -- garanti idempotenza end-to-end
  event_ts        TIMESTAMPTZ NOT NULL,
  item_id         BIGINT NOT NULL,
  location_id     INT NOT NULL REFERENCES locations(location_id),
  delta_qty       INT NOT NULL,          -- +carico / -scarico
  reason          TEXT NOT NULL,         -- 'sale','refill','receipt','adjust','sensor-sync'
  batch_id        BIGINT,                -- se applicato a lotto specifico
  meta            JSONB,                 -- es. scontrino, sensore, operatore
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ledger_item_loc_ts ON inventory_ledger(item_id, location_id, event_ts);

-- dà storia completa, sicurezza contro i duplicati e la possibilità di ricostruire/analizzare tutto



-- Dove finisce ogni alert (stato OPEN/RESOLVED)
CREATE TABLE IF NOT EXISTS alerts (
  alert_id     bigserial PRIMARY KEY,
  rule_key     text        NOT NULL,            -- es. 'low_stock','near_expiry','pipeline_lag','sensor_mismatch'
  severity     text        NOT NULL DEFAULT 'WARN',
  status       text        NOT NULL DEFAULT 'OPEN',   -- OPEN, ACK, RESOLVED
  item_id      bigint,
  location_id  int,
  batch_id     bigint,
  opened_at    timestamptz NOT NULL DEFAULT now(),
  last_seen_at timestamptz NOT NULL DEFAULT now(),
  resolved_at  timestamptz,
  message      text,
  value_num    numeric,
  meta         jsonb
);

-- Evita duplicati: un solo OPEN per stessa (rule,item,location,batch)
CREATE UNIQUE INDEX IF NOT EXISTS uq_alert_open
ON alerts(rule_key, coalesce(item_id,0), coalesce(location_id,0), coalesce(batch_id,0))
WHERE status='OPEN';

-- Soglie/regole (per item, categoria o globali)
CREATE TABLE IF NOT EXISTS inventory_thresholds (
  threshold_id serial PRIMARY KEY,
  scope        text NOT NULL CHECK (scope IN ('item','category','global')),
  item_id      bigint,
  category_id  int,
  location_id  int REFERENCES locations(location_id),   -- NULL = tutte le location
  low_stock_threshold int,          -- es. 5 pezzi
  safety_stock       int,           -- opzionale
  near_expiry_days   int,           -- es. 3 giorni
  UNIQUE(scope, item_id, category_id, location_id)
);

-- indici per ottimizzare le query
CREATE INDEX IF NOT EXISTS idx_items_shelf ON items(shelf_id);
CREATE INDEX IF NOT EXISTS idx_pi_item_loc ON product_inventory(item_id, location_id);
CREATE INDEX IF NOT EXISTS idx_batches_item ON batches(item_id);
CREATE INDEX IF NOT EXISTS idx_batches_expiry ON batches(expiry_date);
CREATE INDEX IF NOT EXISTS idx_batchinv_loc ON batch_inventory(location_id);

-- =====================================================================
-- popolamento iniziale delle tabelle (ONE-SHOT BOOTSTRAP)
-- Carica i 4 CSV iniziali e popola lo schema.
-- Se tabelle non sono vuote -> ABORT (per evitare sovrascritture).
-- Questo script è pensato per essere lanciato una sola volta dopo la creazione dello schema.
-- Qua si possono aggiungere anche script di inizializzazione per altre tabelle!!!!!!
-- =====================================================================

\set ON_ERROR_STOP on

BEGIN;

-- ---------------------------------------------------------------------
-- 0) Parametri percorsi CSV (adatta i path se necessario)
-- ---------------------------------------------------------------------
\set store_inventory '/data/store_inventory_final.csv'
\set wh_inventory    '/data/warehouse_inventory_final.csv'
\set store_batches   '/data/store_batches.csv'
\set wh_batches      '/data/warehouse_batches.csv'

-- ---------------------------------------------------------------------
-- 1) Guard rails: abort se i target hanno già dati
--    (evita di rilanciare per errore questo script dopo l'avvio)
-- ---------------------------------------------------------------------
DO $$
DECLARE
  n_items           bigint;
  n_categories      bigint;
  n_inventory       bigint;
  n_batches         bigint;
  n_batch_inventory bigint;
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

-- ---------------------------------------------------------------------
-- 2) Seed minimo di supporto
-- ---------------------------------------------------------------------
INSERT INTO locations (location) VALUES ('instore'), ('warehouse')
ON CONFLICT (location) DO NOTHING;

-- ---------------------------------------------------------------------
-- 3) Staging temporanee
-- ---------------------------------------------------------------------
CREATE TEMP TABLE staging_inventory_raw (
  shelf_id TEXT,
  item_category TEXT,
  item_weight FLOAT,
  shelf_weight FLOAT,
  item_visibility FLOAT,
  initial_stock INT,
  current_stock INT
) ON COMMIT DROP;

CREATE TEMP TABLE staging_inventory (
  shelf_id TEXT,
  item_category TEXT,
  item_weight FLOAT,
  shelf_weight FLOAT,
  item_visibility FLOAT,
  initial_stock INT,
  current_stock INT,
  location TEXT
) ON COMMIT DROP;

CREATE TEMP TABLE staging_batches_raw (
  shelf_id TEXT,
  batch_code VARCHAR(30),
  received_date DATE,
  expiry_date DATE,
  quantity INT
) ON COMMIT DROP;

CREATE TEMP TABLE staging_batches (
  shelf_id TEXT,
  batch_code VARCHAR(30),
  received_date DATE,
  expiry_date DATE,
  quantity INT,
  location TEXT
) ON COMMIT DROP;

-- ---------------------------------------------------------------------
-- 4) COPY nei RAW + arricchimento con location
-- ---------------------------------------------------------------------
-- INVENTORY - STORE
COPY staging_inventory_raw FROM :'store_inventory' DELIMITER ',' CSV HEADER;
INSERT INTO staging_inventory
SELECT shelf_id, item_category, item_weight, shelf_weight, item_visibility, initial_stock, current_stock,
       'instore'::text
FROM staging_inventory_raw;

-- INVENTORY - WAREHOUSE
TRUNCATE staging_inventory_raw;
COPY staging_inventory_raw FROM :'wh_inventory' DELIMITER ',' CSV HEADER;
INSERT INTO staging_inventory
SELECT shelf_id, item_category, item_weight, shelf_weight, item_visibility, initial_stock, current_stock,
       'warehouse'::text
FROM staging_inventory_raw;

-- BATCHES - STORE
COPY staging_batches_raw FROM :'store_batches' DELIMITER ',' CSV HEADER;
INSERT INTO staging_batches
SELECT shelf_id, batch_code, received_date, expiry_date, quantity,
       'instore'::text
FROM staging_batches_raw;

-- BATCHES - WAREHOUSE
TRUNCATE staging_batches_raw;
COPY staging_batches_raw FROM :'wh_batches' DELIMITER ',' CSV HEADER;
INSERT INTO staging_batches
SELECT shelf_id, batch_code, received_date, expiry_date, quantity,
       'warehouse'::text
FROM staging_batches_raw;

-- ---------------------------------------------------------------------
-- 5) Normalizzazione: categories, items  (solo insert, no update)
-- ---------------------------------------------------------------------
INSERT INTO categories (category_name)
SELECT DISTINCT si.item_category
FROM staging_inventory si
WHERE si.item_category IS NOT NULL AND si.item_category <> ''
ON CONFLICT (category_name) DO NOTHING;

INSERT INTO items (shelf_id, category_id)
SELECT DISTINCT
  si.shelf_id, c.category_id
FROM staging_inventory si
JOIN categories c ON c.category_name = si.item_category
WHERE si.shelf_id IS NOT NULL AND si.shelf_id <> ''
ON CONFLICT (item_name) DO NOTHING;

-- ---------------------------------------------------------------------
-- 6) product_inventory  (solo insert, evita upsert che aggiornano)
-- ---------------------------------------------------------------------
INSERT INTO product_inventory (
  item_id, location_id, item_weight, shelf_weight, item_visibility, initial_stock, current_stock
)
SELECT
  i.item_id,
  l.location_id,
  si.item_weight,
  si.shelf_weight,
  si.item_visibility,
  si.initial_stock,
  si.current_stock
FROM staging_inventory si
JOIN items i     ON i.shelf_id = si.shelf_id
JOIN locations l ON l.location  = si.location
WHERE si.shelf_id IS NOT NULL AND si.shelf_id <> ''
ON CONFLICT (item_id, location_id) DO NOTHING;

-- ---------------------------------------------------------------------
-- 7) batches & batch_inventory  (solo insert)
-- ---------------------------------------------------------------------
INSERT INTO batches (item_id, batch_code, received_date, expiry_date)
SELECT DISTINCT
  i.item_id,
  sb.batch_code,
  sb.received_date,
  sb.expiry_date
FROM staging_batches sb
JOIN items i ON i.shelf_id = sb.shelf_id
WHERE sb.batch_code IS NOT NULL AND sb.batch_code <> ''
ON CONFLICT (item_id, batch_code) DO NOTHING;

INSERT INTO batch_inventory (batch_id, location_id, quantity)
SELECT
  b.batch_id,
  l.location_id,
  sb.quantity
FROM staging_batches sb
JOIN items     i ON i.shelf_id = sb.shelf_id
JOIN batches   b ON b.item_id = i.item_id AND b.batch_code = sb.batch_code
JOIN locations l ON l.location = sb.location
ON CONFLICT (batch_id, location_id) DO NOTHING;

-- ---------------------------------------------------------------------
--8) allerts & thresholds
-- ---------------------------------------------------------------------
-- soglie GLOBALI per location (default: instore più conservativo)
INSERT INTO inventory_thresholds (scope, location_id, low_stock_threshold, safety_stock, near_expiry_days)
SELECT 'global', l.location_id,
       CASE WHEN l.location='instore'   THEN 5  ELSE 20 END AS low_thr,
       CASE WHEN l.location='instore'   THEN 2  ELSE 5  END AS safety,
       CASE WHEN l.location='instore'   THEN 3  ELSE 7  END AS near_exp
FROM locations l
WHERE l.location IN ('instore','warehouse')
ON CONFLICT (scope, item_id, category_id, location_id) DO NOTHING;

-- soglie per CATEGORIE deperibili (se presenti): più aggressive
--    (adatta i pattern ai tuoi nomi di categoria reali)
INSERT INTO inventory_thresholds (scope, category_id, location_id, low_stock_threshold, safety_stock, near_expiry_days)
SELECT 'category', c.category_id, l.location_id,
       CASE WHEN l.location='instore' THEN 8 ELSE 30 END AS low_thr,
       CASE WHEN l.location='instore' THEN 3 ELSE 8  END AS safety,
       CASE WHEN l.location='instore' THEN 2 ELSE 5  END AS near_exp
FROM categories c
CROSS JOIN locations l
WHERE lower(c.category_name) ~ '^(fresh|fresco|perish|deperib|dairy|lattic|meat|carne|fish|pesce|produce|frutta|verdura|bakery|pane|pasticc)'
  AND l.location IN ('instore','warehouse')
ON CONFLICT (scope, item_id, category_id, location_id) DO NOTHING;

-- soglie per ITEM calcolate dai livelli iniziali (per location)
--    low_stock_threshold ~ 10% dello stock iniziale (min 1, max 25 per instore; max 50 per warehouse)
INSERT INTO inventory_thresholds (scope, item_id, location_id, low_stock_threshold, safety_stock, near_expiry_days)
SELECT 'item',
       pi.item_id,
       pi.location_id,
       CASE
         WHEN COALESCE(pi.initial_stock, pi.current_stock, 0) <= 0 THEN
           CASE WHEN l.location='instore' THEN 5 ELSE 20 END
         ELSE
           CASE
             WHEN l.location='instore' THEN GREATEST(1, LEAST(25, CEIL(COALESCE(pi.initial_stock, pi.current_stock)::numeric * 0.10))::int)
             ELSE                          GREATEST(1, LEAST(50, CEIL(COALESCE(pi.initial_stock, pi.current_stock)::numeric * 0.10))::int)
           END
       END AS low_thr,
       NULL::int  AS safety_stock,         -- lascia NULL per ereditare dal livello superiore
       NULL::int  AS near_expiry_days      -- idem
FROM product_inventory pi
JOIN locations l ON l.location_id = pi.location_id
ON CONFLICT (scope, item_id, category_id, location_id) DO NOTHING;


-- ---------------------------------------------------------------------
-- 9) Statistiche
-- ---------------------------------------------------------------------
ANALYZE;

COMMIT;

-- NOTE:
-- Dopo l’avvio, tutti i cambiamenti (stock, lotti, movimenti) devono passare
-- dai job Kafka/Spark -> NON rilanciare questo script.
