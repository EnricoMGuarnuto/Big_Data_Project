'''
pickup (prelievo item dallo scaffale, evento iniziale)

replace (item rimesso sullo scaffale)

expired (non restituito dopo 1 minuto)

confirmed (effettivamente acquistato)

returned (rimesso sullo scaffale entro 1 minuto)

Lo script va lanciato periodicamente (ogni minuto) e aggiorna sia la tabella di staging degli eventi che lo stock in-store.
'''


-- 1. MARCA COME "returned" tutti i pickup che hanno una replace SUCCESSIVA e ancora status pending
UPDATE shelf_events AS pickup
SET status = 'returned'
FROM shelf_events AS repl
WHERE pickup.product_id = repl.product_id
  AND pickup.status = 'pending'
  AND pickup.event_type = 'pickup'
  AND repl.event_type = 'replace'
  AND repl.event_time > pickup.event_time
  AND repl.status IS DISTINCT FROM 'returned';

-- 2. MARCA COME "expired" tutti i pickup PENDING che hanno più di 1 minuto e non hanno un replace SUCCESSIVO
UPDATE shelf_events AS pickup
SET status = 'expired'
WHERE pickup.status = 'pending'
  AND pickup.event_type = 'pickup'
  AND pickup.event_time < NOW() - INTERVAL '1 minute'
  AND NOT EXISTS (
      SELECT 1 FROM shelf_events AS repl
      WHERE repl.product_id = pickup.product_id
        AND repl.event_type = 'replace'
        AND repl.event_time > pickup.event_time
  );
  -- questo expired sarà creato da variazioni shelves guidate da expiring dates.

-- 3. DECREMENTA LO STOCK IN-STORE per tutti i pickup ora "expired"
UPDATE products_instore AS p
SET current_stock = current_stock - 1
FROM shelf_events AS se
WHERE se.product_id = p.product_id
  AND se.status = 'expired'
  AND se.stock_updated IS DISTINCT FROM TRUE;

-- 4. MARCA GLI EVENTI "expired" come già aggiornati (per evitare doppio decremento)
UPDATE shelf_events
SET stock_updated = TRUE
WHERE status = 'expired'
  AND stock_updated IS DISTINCT FROM TRUE;

-- 5. (OPZIONALE) MARCA COME "confirmed" i pickup che hanno una corrispondenza in POS (logica da implementare nel job consumer/python: qui puoi solo preparare la colonna!)

-- NB: accertati che la colonna "stock_updated" sia BOOL in shelf_events (default FALSE/null)
ALTER TABLE shelf_events 
ADD COLUMN IF NOT EXISTS stock_updated BOOL DEFAULT FALSE;

-- (FACOLTATIVO) Pulisci gli eventi troppo vecchi (>1 giorno) per evitare accumulo
DELETE FROM shelf_events
WHERE event_time < NOW() - INTERVAL '1 day'
  AND status IN ('returned', 'expired', 'confirmed');

