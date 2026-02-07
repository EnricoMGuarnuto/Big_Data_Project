CREATE TABLE IF NOT EXISTS products_instore (
    product_id SERIAL PRIMARY KEY,
    item_weight FLOAT,
    visibility FLOAT,
    category TEXT,
    item_mrp FLOAT,
    initial_stock INT,
    current_stock INT
);

CREATE TABLE IF NOT EXISTS products_warehouse (
    product_id SERIAL PRIMARY KEY,
    item_weight FLOAT,
    visibility FLOAT,
    category TEXT,
    item_mrp FLOAT,
    initial_stock INT,
    current_stock INT
);

CREATE TABLE IF NOT EXISTS alerts (
    alert_id SERIAL PRIMARY KEY,
    product_id INT REFERENCES products_instore(product_id) ON DELETE CASCADE,
    alert_type TEXT,
    alert_message TEXT -- altro da fare e da capire come gestirli?
);

CREATE TABLE IF NOT EXISTS refill_history (
    refill_id SERIAL PRIMARY KEY,
    product_id INT,
    refill_time TIMESTAMP DEFAULT NOW(),
    quantity INT
);


CREATE TABLE IF NOT EXISTS shelf_events (
    event_id SERIAL PRIMARY KEY,
    product_id INT,
    event_type TEXT, -- "pickup" o "replace"
    event_time TIMESTAMP,
    status TEXT DEFAULT 'pending', -- "pending", "confirmed", "returned", "expired"
    stock_updated BOOL DEFAULT FALSE -- per evitare doppio decremento dello stock
);
-- Ogni volta che il sensore registra una variazione di peso generi un record qui.
-- Il consumer aggiornerà lo status in base a quanto succede dopo (acquisto o restituzione).

CREATE TABLE IF NOT EXISTS sales_history (
    sale_id SERIAL PRIMARY KEY,
    product_id INT,
    quantity INT,
    sale_time TIMESTAMP,
    transaction_id TEXT
);
-- Ogni volta che viene effettuata una vendita, registra qui.

CREATE TABLE IF NOT EXISTS pos_staging (
    transaction_id TEXT,
    product_id INT,
    quantity INT,
    sale_time TIMESTAMP DEFAULT NOW(),
    processed BOOL DEFAULT FALSE
);
-- Staging table per le transazioni POS, da processare periodicamente.

CREATE TABLE IF NOT EXISTS foot_traffic_events (
    event_id SERIAL PRIMARY KEY,
    entry_time TIMESTAMP,
    customer_id TEXT
);
-- utile per analytics successive e per calcolare il traffico in store.


--CREATE TABLE IF NOT EXISTS refill_history (
    -- da fare?
--);


COPY products_instore(product_id,item_weight, visibility, category, item_mrp, initial_stock, current_stock) 
FROM '/data/store_inventory_final.csv'
DELIMITER ','
CSV HEADER;

COPY products_warehouse(product_id,item_weight, visibility, category, item_mrp, initial_stock, current_stock) 
FROM '/data/warehouse_inventory_final.csv'
DELIMITER ','
CSV HEADER;