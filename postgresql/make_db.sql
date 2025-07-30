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

CREATE TABLE IF NOT EXISTS sales_history (
    sale_id SERIAL PRIMARY KEY,
    quantity INT,
    sale_time TIMESTAMP
);
-- Ogni volta che viene effettuata una vendita, registra qui.



COPY products_instore(product_id,item_weight, visibility, category, item_mrp, initial_stock, current_stock) 
FROM '/data/store_inventory_final.csv'
DELIMITER ','
CSV HEADER;

COPY products_warehouse(product_id,item_weight, visibility, category, item_mrp, initial_stock, current_stock) 
FROM '/data/warehouse_inventory_final.csv'
DELIMITER ','
CSV HEADER;