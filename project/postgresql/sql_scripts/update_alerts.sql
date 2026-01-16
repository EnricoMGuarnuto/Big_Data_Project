-- Genera un alert se non esiste già uno "open" per quel prodotto
INSERT INTO alerts (product_id, alert_type, alert_message)
SELECT product_id, 'LOW_STOCK', 'Scaffale prossimo a esaurimento!'
FROM products_instore
WHERE current_stock = 4
  AND NOT EXISTS (
      SELECT 1 FROM alerts 
      WHERE product_id = products_instore.product_id AND alert_type = 'LOW_STOCK' AND alert_message = 'Scaffale prossimo a esaurimento!'
  );