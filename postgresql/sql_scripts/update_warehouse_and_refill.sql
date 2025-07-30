-- Trova prodotti da refillare (current_stock=4)
WITH to_refill AS (
  SELECT 
    p.product_id,
    p.initial_stock - p.current_stock AS qty_to_refill
  FROM products_instore p
  WHERE p.current_stock = 4
    AND p.initial_stock > p.current_stock
)
-- 1. Diminuisci warehouse
UPDATE products_warehouse w
SET current_stock = GREATEST(current_stock - t.qty_to_refill, 0)
FROM to_refill t
WHERE w.product_id = t.product_id;

-- 2. Aggiorna scaffale (porta current_stock=initial_stock)
UPDATE products_instore s
SET current_stock = initial_stock
FROM to_refill t
WHERE s.product_id = t.product_id;

-- 3. Scrivi refill_history
INSERT INTO refill_history (product_id, refill_time, quantity)
SELECT product_id, NOW(), qty_to_refill FROM to_refill;

-- 4. Chiudi alert
UPDATE allerts
SET alert_message = 'Refill effettuato', alert_type = 'REFILLED'
WHERE product_id IN (SELECT product_id FROM to_refill)
  AND alert_type = 'LOW_STOCK'
  AND alert_message = 'Scaffale prossimo a esaurimento!';

-- 5. ALERT per warehouse sotto soglia
INSERT INTO allerts (product_id, alert_type, alert_message)
SELECT product_id, 'LOW_WAREHOUSE', 'Magazzino prossimo a esaurimento!'
FROM products_warehouse
WHERE current_stock <= 20
  AND NOT EXISTS (
    SELECT 1 FROM allerts
    WHERE product_id = products_warehouse.product_id
      AND alert_type = 'LOW_WAREHOUSE'
      AND alert_message = 'Magazzino prossimo a esaurimento!'
  );

-- 6. ORDINA lotto se sotto soglia
-- In questo esempio, ordini la quantità necessaria per coprire 14 giorni di domanda prevista.
WITH to_order AS (
  SELECT 
    w.product_id,
    GREATEST(0,
      (f.daily_demand * 14) - w.current_stock
    ) AS order_qty
  FROM products_warehouse w
  JOIN forecast_demand f ON w.product_id = f.product_id
  WHERE w.current_stock <= 20
)
UPDATE products_warehouse w
SET current_stock = w.current_stock + t.order_qty,
    initial_stock = w.initial_stock + t.order_qty
FROM to_order t
WHERE w.product_id = t.product_id
  AND t.order_qty > 0;

-- 7. (Opzionale) Scrivi refill_history_warehouse
INSERT INTO refill_history (product_id, refill_time, quantity)
SELECT product_id, NOW(), order_qty FROM to_order;

-- 8. (Opzionale) Chiudi l'alert appena rifornito
UPDATE allerts
SET alert_message = 'Warehouse refill effettuato', alert_type = 'WAREHOUSE_REFILLED'
WHERE product_id IN (SELECT product_id FROM to_order)
  AND alert_type = 'LOW_WAREHOUSE'
  AND alert_message = 'Magazzino prossimo a esaurimento!';