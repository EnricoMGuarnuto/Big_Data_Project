CREATE TABLE IF NOT EXISTS products_instore (
    shelf_id SERIAL PRIMARY KEY,
    item_weight FLOAT,
    shelf_weight FLOAT,
    visibility FLOAT,
    category TEXT,
    item_mrp FLOAT,
    initial_stock INT,
    current_stock INT
);


CREATE TABLE IF NOT EXISTS products_warehouse (
    shelf_id SERIAL PRIMARY KEY,
    item_weight FLOAT,
    visibility FLOAT,
    category TEXT,
    item_mrp FLOAT,
    initial_stock INT,
    current_stock INT
);

-- CREATE TABLE IF NOT EXISTS batch (
--     batch_id SERIAL PRIMARY KEY, 
--     product_id INT,
--     quantity_instore INT,
--     quantity_warehouse INT,
--     expiry_date DATE, -- data di scadenza del lotto
--     received_date DATE
-- );

CREATE TABLE store_batches (
    product_id INT,
    batch_id serial primary key,
    quantity INT,
    expiry_date DATE,
    received_date DATE
);

CREATE TABLE warehouse_batches (
    product_id INT,
    batch_id serial primary key,
    quantity INT,
    expiry_date DATE,
    received_date DATE
);

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
    shelf_id INT,
    refill_time TIMESTAMP DEFAULT NOW(),
    quantity INT
);
-- Tabella eventi di refill per shelf

CREATE TABLE IF NOT EXISTS shelf_events (
    event_id SERIAL PRIMARY KEY,
    shelf_id INT,
    event_type TEXT, -- "pickup (item preso da shelf)", "rimesso giù (item rimesso a posto)", "restock (item rifornito)"
    weight_change FLOAT, -- variazione di peso (positivo o negativo)
    event_time TIMESTAMP
    --da togliere:
    --status_event TEXT DEFAULT 'pending', -- "pending", "confirmed", "returned", "expired"
    --stock_updated BOOL DEFAULT FALSE -- per evitare doppio decremento dello stock
);
-- Ogni volta che il sensore registra una variazione di peso generi un record qui.
-- Il consumer aggiornerà lo status in base a quanto succede dopo (acquisto o restituzione).

create table receipts (
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

create table receipt_lines (
  receipt_line_id   bigserial primary key,
  receipt_id        bigint not null references receipts(receipt_id) on delete cascade,
  product_id        bigint not null references products_instore(shelf_id), 
  batch_id          text not null references batch(batch_id),                        
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

create index idx_lines_receipt on receipt_lines(receipt_id);

COPY products_instore(product_id,item_weight, visibility, category, item_mrp, initial_stock, current_stock) 
FROM '/data/store_inventory_final.csv'
DELIMITER ','
CSV HEADER;

COPY products_warehouse(product_id,item_weight, visibility, category, item_mrp, initial_stock, current_stock) 
FROM '/data/warehouse_inventory_final.csv'
DELIMITER ','
CSV HEADER;

COPY store_batches
FROM '/data/store_batches.csv'
DELIMITER ','
CSV HEADER;

COPY warehouse_batches
FROM '/data/warehouse_batches.csv'
DELIMITER ','
CSV HEADER;