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
