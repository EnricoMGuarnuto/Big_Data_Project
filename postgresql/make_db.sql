CREATE TABLE IF NOT EXISTS products_instore (
    shelf_id VARCHAR(5),
    item_weight FLOAT,
    shelf_weight FLOAT,
    visibility FLOAT,
    category TEXT,
    item_mrp FLOAT,
    initial_stock INT,
    current_stock INT
);


CREATE TABLE IF NOT EXISTS products_warehouse (
    shelf_id VARCHAR(5),
    item_weight FLOAT,
    visibility FLOAT,
    category TEXT,
    item_mrp FLOAT,
    initial_stock INT,
    current_stock INT
);

-- Tabelle per la gestione dei lotti (batches) in store e warehouse
CREATE TABLE IF NOT EXISTS staging_batches (
    shelf_id VARCHAR(5),
    batch_code VARCHAR(30),
    received_date DATE,
    expiry_date DATE,
    quantity INT,
    location TEXT -- 'instore' or 'warehouse'
    constraint chk_location_type CHECK (location IN ('instore', 'warehouse')) -- contraint per limitare i valori
);

CREATE TABLE IF NOT EXISTS items (
  item_id bigserial primary key,
  shelf_id  VARCHAR(5) not null unique
);

create table if not exists locations (
  location_id      serial primary key,
  location    text not null unique
  constraint chk_location_type CHECK (location IN ('instore', 'warehouse'))
);

CREATE TABLE IF NOT EXISTS categories (
  category_id SERIAL PRIMARY KEY,
  category_name TEXT NOT NULL UNIQUE
);


create table if not exists batches (
  batch_id             bigserial primary key,
  item_id              bigint not null references items(item_id),
  batch_code           varchar(30)   not null  -- Batch_ID del CSV
  received_date        date   not null,
  expiry_date          date,
  constraint uq_batches unique (item_id, batch_code)
  constraint chk_expiry_after_received CHECK (expiry_date IS NULL OR expiry_date >= received_date)

);

-- create index if not exists idx_batches_expiry on batches(expiry_date);

create table if not exists batch_inventory (
  batch_id     bigint not null references batches(batch_id) on delete cascade,
  location_id  int    not null references locations(location_id),
  quantity     int    not null,
  constraint pk_batch_inventory primary key (batch_id, location_id),
  constraint chk_qty_nonneg check (quantity >= 0)
);
-- create index if not exists idx_batch_inventory_loc on batch_inventory(location_id);


'''
-- Tabella di domanda prevista per prodotto (popolata da Spark/ML periodicamente)
-- Da vedere 

CREATE TABLE IF NOT EXISTS forecast_demand (
   shelf_id INT,
   daily_sale INT,
   daily_traffic INT,
   updated_at TIMESTAMP
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



COPY products_instore(product_id,item_weight, visibility, category, item_mrp, initial_stock, current_stock) 
FROM '/data/store_inventory_final.csv'
DELIMITER ','
CSV HEADER;

COPY products_warehouse(product_id,item_weight, visibility, category, item_mrp, initial_stock, current_stock) 
FROM '/data/warehouse_inventory_final.csv'
DELIMITER ','
CSV HEADER;

COPY staging_batches(shelf_id, batch_code, received_date, expiry_date, quantity, location)
FROM '/data/store_batches.csv'
DELIMITER ','
CSV HEADER;

COPY staging_batches(shelf_id, batch_code, received_date, expiry_date, quantity, location)
FROM '/data/warehouse_batches.csv'
DELIMITER ','
CSV HEADER;