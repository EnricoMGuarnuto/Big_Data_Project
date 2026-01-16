-- 1. Per ogni riga di pos_staging NON ancora processata:
--    - Decrementa lo stock in-store
--    - Inserisce nella sales_history
--    - Marca come processed

DO $$
DECLARE
    rec RECORD;
BEGIN
    FOR rec IN 
        SELECT * FROM pos_staging WHERE processed IS FALSE
    LOOP
        -- Decrementa lo stock (non va sotto zero)
        UPDATE products_instore
        SET current_stock = GREATEST(current_stock - rec.quantity, 0)
        WHERE product_id = rec.product_id;

        -- Inserisci nella storia
        INSERT INTO sales_history (product_id, quantity, sale_time, transaction_id)
        VALUES (rec.product_id, rec.quantity, rec.sale_time, rec.transaction_id);

        -- Marca come processato
        UPDATE pos_staging
        SET processed = TRUE
        WHERE transaction_id = rec.transaction_id AND product_id = rec.product_id;
    END LOOP;
END $$;

-- Marca come "confirmed" i pickup corrispondenti in shelf_events
UPDATE shelf_events
SET status = 'confirmed'
WHERE product_id IN (SELECT product_id FROM pos_staging WHERE processed IS TRUE)
  AND status = 'pending';

