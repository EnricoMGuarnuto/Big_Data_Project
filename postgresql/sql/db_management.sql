/* ===============================================================
   DB MANAGEMENT — Funzioni dominio: FEFO, sale/refill, sensori, alert
   =============================================================== */

-- Helper: id location da testo
CREATE OR REPLACE FUNCTION _get_location_id(loc TEXT)
RETURNS INT LANGUAGE sql AS $$
  SELECT location_id FROM locations WHERE location = loc
$$;

-- Garantisce la riga in product_inventory
CREATE OR REPLACE FUNCTION _ensure_pi_row(p_item_id BIGINT, p_loc_id INT)
RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO product_inventory(item_id, location_id, current_stock)
  VALUES (p_item_id, p_loc_id, 0)
  ON CONFLICT (item_id, location_id) DO NOTHING;
END$$;

/* =====================
   SHADOW: STOCK "LIVE"
   ===================== */

CREATE OR REPLACE FUNCTION _update_sensor_balance(p_item_id BIGINT, p_loc_id INT, p_delta INT)
RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO sensor_balance(item_id, location_id, pending_delta)
  VALUES (p_item_id, p_loc_id, p_delta)
  ON CONFLICT (item_id, location_id)
  DO UPDATE SET pending_delta = sensor_balance.pending_delta + EXCLUDED.pending_delta,
                updated_at    = now();
END$$;

CREATE OR REPLACE FUNCTION _get_live_on_hand(p_item_id BIGINT, p_loc_id INT)
RETURNS INT LANGUAGE sql AS $$
  SELECT COALESCE((SELECT current_stock FROM product_inventory
                    WHERE item_id=p_item_id AND location_id=p_loc_id),0)
       + COALESCE((SELECT pending_delta FROM sensor_balance
                    WHERE item_id=p_item_id AND location_id=p_loc_id),0)
$$;

-- Valuta alert low_stock su base LIVE (crossing up/down)
CREATE OR REPLACE FUNCTION _evaluate_low_stock_live(
  p_item_id BIGINT, p_loc_id INT, p_prev_live INT, p_new_live INT
) RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE v_thr INT;
BEGIN
  v_thr := _get_low_stock_threshold(p_item_id, p_loc_id);
  IF v_thr IS NULL OR v_thr <= 0 THEN RETURN; END IF;

  IF (COALESCE(p_prev_live, v_thr+1) > v_thr) AND (p_new_live <= v_thr) THEN
    PERFORM raise_alert(
      'low_stock','WARN', p_item_id, p_loc_id, NULL,
      format('LIVE: stock basso %s (soglia %s)', p_new_live, v_thr),
      p_new_live, jsonb_build_object('live',true,'threshold',v_thr)
    );
  END IF;

  IF (COALESCE(p_prev_live, v_thr+1) <= v_thr) AND (p_new_live > v_thr) THEN
    PERFORM resolve_alert('low_stock', p_item_id, p_loc_id, NULL);
  END IF;
END$$;

/* =====================
   FEFO PRIMITIVES
   ===================== */

-- Decremento FEFO su UNA location. Ritorna residuo non coperto da lotti.
CREATE OR REPLACE FUNCTION _fefo_decrement(
  p_item_id   BIGINT,
  p_loc_id    INT,
  p_qty       INT,
  p_event_id  TEXT,
  p_event_ts  TIMESTAMPTZ,
  p_reason    TEXT,
  p_meta      JSONB
) RETURNS INT
LANGUAGE plpgsql AS $$
DECLARE
  v_left INT := p_qty;
  v_row  RECORD;
BEGIN
  PERFORM _ensure_pi_row(p_item_id, p_loc_id);

  FOR v_row IN
    SELECT b.batch_id, bi.quantity, b.expiry_date, b.received_date
    FROM batches b
    JOIN batch_inventory bi ON bi.batch_id = b.batch_id
    WHERE b.item_id = p_item_id
      AND bi.location_id = p_loc_id
      AND bi.quantity > 0
    ORDER BY b.expiry_date NULLS LAST, b.received_date
  LOOP
    EXIT WHEN v_left <= 0;

    IF v_row.quantity >= v_left THEN
      UPDATE batch_inventory
         SET quantity = quantity - v_left
       WHERE batch_id = v_row.batch_id AND location_id = p_loc_id;

      INSERT INTO inventory_ledger(event_id,event_ts,item_id,location_id,delta_qty,reason,batch_id,meta)
      VALUES (p_event_id || '-' || v_row.batch_id, p_event_ts, p_item_id, p_loc_id, -v_left, p_reason, v_row.batch_id, p_meta);
      v_left := 0;

    ELSE
      UPDATE batch_inventory
         SET quantity = 0
       WHERE batch_id = v_row.batch_id AND location_id = p_loc_id;

      INSERT INTO inventory_ledger(event_id,event_ts,item_id,location_id,delta_qty,reason,batch_id,meta)
      VALUES (p_event_id || '-' || v_row.batch_id, p_event_ts, p_item_id, p_loc_id, -v_row.quantity, p_reason, v_row.batch_id, p_meta);
      v_left := v_left - v_row.quantity;
    END IF;
  END LOOP;

  RETURN v_left;
END$$;

-- Transfer FEFO tra due location (from → to). Ritorna qty effettivamente trasferita.
CREATE OR REPLACE FUNCTION _fefo_transfer(
  p_item_id   BIGINT,
  p_from_loc  INT,
  p_to_loc    INT,
  p_qty       INT,
  p_event_id  TEXT,
  p_event_ts  TIMESTAMPTZ,
  p_reason    TEXT,
  p_meta      JSONB
) RETURNS INT
LANGUAGE plpgsql AS $$
DECLARE
  v_left    INT := p_qty;
  v_moved   INT := 0;
  v_row     RECORD;
  v_q       INT;
  v_meta2   JSONB := COALESCE(p_meta,'{}'::jsonb) || jsonb_build_object('base_event_id', p_event_id);
  v_wh_reason TEXT := p_reason || '_wh';
  v_st_reason TEXT := p_reason || '_st';
BEGIN
  PERFORM _ensure_pi_row(p_item_id, p_from_loc);
  PERFORM _ensure_pi_row(p_item_id, p_to_loc);

  FOR v_row IN
    SELECT b.batch_id, bi.quantity, b.expiry_date, b.received_date
    FROM batches b
    JOIN batch_inventory bi ON bi.batch_id = b.batch_id
    WHERE b.item_id = p_item_id
      AND bi.location_id = p_from_loc
      AND bi.quantity > 0
    ORDER BY b.expiry_date NULLS LAST, b.received_date
  LOOP
    EXIT WHEN v_left <= 0;

    v_q := LEAST(v_left, v_row.quantity);

    -- decremento a from_loc
    UPDATE batch_inventory
       SET quantity = quantity - v_q
     WHERE batch_id = v_row.batch_id AND location_id = p_from_loc;

    -- incremento a to_loc
    INSERT INTO batch_inventory(batch_id, location_id, quantity)
    VALUES (v_row.batch_id, p_to_loc, v_q)
    ON CONFLICT (batch_id, location_id) DO UPDATE
      SET quantity = batch_inventory.quantity + EXCLUDED.quantity;

    -- ledger: due movimenti speculari sullo stesso batch
    INSERT INTO inventory_ledger(event_id,event_ts,item_id,location_id,delta_qty,reason,batch_id,meta)
    VALUES (p_event_id || '-' || v_row.batch_id || '-wh', p_event_ts, p_item_id, p_from_loc, -v_q, v_wh_reason, v_row.batch_id, v_meta2);

    INSERT INTO inventory_ledger(event_id,event_ts,item_id,location_id,delta_qty,reason,batch_id,meta)
    VALUES (p_event_id || '-' || v_row.batch_id || '-st', p_event_ts, p_item_id, p_to_loc,  +v_q, v_st_reason, v_row.batch_id, v_meta2);

    v_left  := v_left - v_q;
    v_moved := v_moved + v_q;
  END LOOP;

  -- aggiorna stock prodotto aggregato (solo quanto davvero mosso)
  IF v_moved > 0 THEN
    UPDATE product_inventory
       SET current_stock = current_stock - v_moved
     WHERE item_id = p_item_id AND location_id = p_from_loc;

    UPDATE product_inventory
       SET current_stock = current_stock + v_moved
     WHERE item_id = p_item_id AND location_id = p_to_loc;
  END IF;

  RETURN v_moved;
END$$;

/* =====================
   DOMAIN FUNCTIONS
   ===================== */

CREATE OR REPLACE FUNCTION apply_pos_transaction(
    p_transaction JSONB
) RETURNS VOID AS $$
DECLARE
    v_receipt_id BIGINT;
    v_total_net NUMERIC(12,2) := 0;
    v_total_tax NUMERIC(12,2) := 0;
    v_total_gross NUMERIC(12,2) := 0;
    v_business_date DATE;
    v_item JSONB;
    v_unit_price NUMERIC(12,4);
    v_discount NUMERIC(12,4);
    v_qty INT;
    v_total_line NUMERIC(14,4);
BEGIN
    -- business_date con fallback a oggi
    v_business_date := COALESCE(
        (p_transaction->>'timestamp')::timestamptz::date,
        CURRENT_DATE
    );

    -- Inserisci/aggiorna header receipt
    INSERT INTO receipts(transaction_id, customer_id, business_date, closed_at, status)
    VALUES (
        p_transaction->>'transaction_id',
        p_transaction->>'customer_id',
        v_business_date,
        (p_transaction->>'timestamp')::timestamptz,
        'CLOSED'
    )
    ON CONFLICT (transaction_id) DO UPDATE
      SET customer_id   = EXCLUDED.customer_id,
          business_date = EXCLUDED.business_date,
          closed_at     = EXCLUDED.closed_at,
          status        = 'CLOSED'
    RETURNING receipt_id INTO v_receipt_id;

    -- cancella righe vecchie (idempotenza)
    DELETE FROM receipt_lines WHERE receipt_id = v_receipt_id;

    -- loop sugli items
    FOR v_item IN SELECT jsonb_array_elements(p_transaction->'items')
    LOOP
        v_qty        := COALESCE((v_item->>'quantity')::int,0);
        v_unit_price := COALESCE((v_item->>'unit_price')::numeric,0);
        v_discount   := COALESCE((v_item->>'discount')::numeric,0);
        v_total_line := COALESCE((v_item->>'total_price')::numeric, v_qty*(v_unit_price-v_discount));

        -- inserisci riga
        INSERT INTO receipt_lines(
            receipt_id, shelf_id, quantity,
            unit_price, discount, total_price
        )
        VALUES (
            v_receipt_id,
            v_item->>'item_id',
            v_qty,
            v_unit_price,
            v_discount,
            v_total_line
        );

        -- aggiorna totali
        v_total_net   := v_total_net + (v_qty * (v_unit_price - v_discount));
        v_total_gross := v_total_gross + v_total_line;
    END LOOP;

    -- supponiamo IVA = 22%
    v_total_tax := v_total_gross - v_total_net;

    -- aggiorna header
    UPDATE receipts
    SET total_net   = v_total_net,
        total_tax   = v_total_tax,
        total_gross = v_total_gross
    WHERE receipt_id = v_receipt_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION apply_sale_event(
  p_event_id TEXT,
  p_event_ts TIMESTAMPTZ,
  p_shelf_id TEXT,
  p_qty      INT,
  p_meta     JSONB DEFAULT '{}'
) RETURNS VOID
LANGUAGE plpgsql AS $$
DECLARE
  v_item_id   BIGINT;
  v_loc_id    INT := _get_location_id('instore');
  v_left      INT;
  v_pending   INT;
  v_prev_live INT; 
  v_new_live  INT;
BEGIN
  -- idempotenza: se già presente, esci
  IF EXISTS (SELECT 1 FROM inventory_ledger WHERE event_id = p_event_id) THEN
    RETURN;
  END IF;

  -- trova item
  SELECT item_id INTO v_item_id FROM items WHERE shelf_id = p_shelf_id;
  IF v_item_id IS NULL THEN
    RAISE EXCEPTION 'Unknown shelf_id %', p_shelf_id;
  END IF;

  PERFORM _ensure_pi_row(v_item_id, v_loc_id);

  -- 1) scala stock ufficiale
  UPDATE product_inventory
     SET current_stock = current_stock - p_qty
   WHERE item_id = v_item_id AND location_id = v_loc_id;

  -- 2) Lotti FEFO (instore)
  v_left := _fefo_decrement(v_item_id, v_loc_id, p_qty, p_event_id, p_event_ts, 'sale', p_meta);

  -- se avanzano quantità non coperte → log overdraw (idempotente)
  IF v_left > 0 THEN
    INSERT INTO inventory_ledger(event_id,event_ts,item_id,location_id,delta_qty,reason,meta)
    VALUES (p_event_id||'-over', p_event_ts, v_item_id, v_loc_id, -v_left, 'sale_overdraw',
            jsonb_build_object('missing_qty',v_left)::jsonb || COALESCE(p_meta,'{}'::jsonb))
    ON CONFLICT (event_id) DO NOTHING;
  END IF;

  -- 3) Compensazione LIVE
  v_prev_live := _get_live_on_hand(v_item_id, v_loc_id);

  SELECT COALESCE(pending_delta,0) INTO v_pending
  FROM sensor_balance
  WHERE item_id=v_item_id AND location_id=v_loc_id;

  IF COALESCE(v_pending,0) < 0 THEN
    PERFORM _update_sensor_balance(v_item_id, v_loc_id, LEAST(p_qty, -v_pending));
  END IF;

  v_new_live := _get_live_on_hand(v_item_id, v_loc_id);
  PERFORM _evaluate_low_stock_live(v_item_id, v_loc_id, v_prev_live, v_new_live);
END;
$$;


-- Ricezione lotto a warehouse
CREATE OR REPLACE FUNCTION apply_receipt_event(
  p_event_id   TEXT,
  p_event_ts   TIMESTAMPTZ,
  p_shelf_id   TEXT,
  p_batch_code TEXT,
  p_received   DATE,
  p_expiry     DATE,
  p_qty        INT,
  p_meta       JSONB DEFAULT '{}'
) RETURNS VOID
LANGUAGE plpgsql AS $$
DECLARE
  v_item_id  BIGINT;
  v_loc_id   INT := _get_location_id('warehouse');
  v_batch_id BIGINT;
BEGIN
  IF EXISTS (SELECT 1 FROM inventory_ledger WHERE event_id = p_event_id) THEN RETURN; END IF;

  SELECT item_id INTO v_item_id FROM items WHERE shelf_id = p_shelf_id;
  IF v_item_id IS NULL THEN RAISE EXCEPTION 'Unknown shelf_id %', p_shelf_id; END IF;

  PERFORM _ensure_pi_row(v_item_id, v_loc_id);

  INSERT INTO batches(item_id, batch_code, received_date, expiry_date)
  VALUES (v_item_id, p_batch_code, p_received, p_expiry)
  ON CONFLICT (item_id, batch_code) DO NOTHING;

  SELECT batch_id INTO v_batch_id FROM batches WHERE item_id=v_item_id AND batch_code=p_batch_code;

  INSERT INTO batch_inventory(batch_id, location_id, quantity)
  VALUES (v_batch_id, v_loc_id, p_qty)
  ON CONFLICT (batch_id, location_id) DO UPDATE
    SET quantity = batch_inventory.quantity + EXCLUDED.quantity;

  INSERT INTO inventory_ledger(event_id, event_ts, item_id, location_id, delta_qty, reason, batch_id, meta)
  VALUES (p_event_id, p_event_ts, v_item_id, v_loc_id, p_qty, 'receipt', v_batch_id, p_meta);

  UPDATE product_inventory
     SET current_stock = current_stock + p_qty
   WHERE item_id = v_item_id AND location_id = v_loc_id;
END$$;

-- Refill FEFO: warehouse → instore
CREATE OR REPLACE FUNCTION apply_refill_event(
  p_event_id TEXT,
  p_event_ts TIMESTAMPTZ,
  p_shelf_id TEXT,
  p_qty      INT,
  p_meta     JSONB DEFAULT '{}'
) RETURNS VOID
LANGUAGE plpgsql AS $$
DECLARE
  v_item_id   BIGINT;
  v_wh        INT := _get_location_id('warehouse');
  v_st        INT := _get_location_id('instore');
  v_moved     INT := 0;
  v_meta2     JSONB := COALESCE(p_meta,'{}'::jsonb) || jsonb_build_object('base_event_id', p_event_id);
  v_prev_live INT; 
  v_new_live  INT;
BEGIN
  IF p_qty IS NULL OR p_qty <= 0 THEN RAISE EXCEPTION 'Refill qty must be > 0 (got %)', p_qty; END IF;

  -- idempotenza refill: già eseguito?
  IF EXISTS (
    SELECT 1 FROM inventory_ledger
     WHERE meta ? 'base_event_id'
       AND meta->>'base_event_id' = p_event_id
       AND reason LIKE 'refill_transfer%'
  ) THEN RETURN; END IF;

  SELECT item_id INTO v_item_id FROM items WHERE shelf_id = p_shelf_id;
  IF v_item_id IS NULL THEN RAISE EXCEPTION 'Unknown shelf_id %', p_shelf_id; END IF;

  v_meta2 := v_meta2 || jsonb_build_object('shelf_id', p_shelf_id);

  -- LIVE pre-movimento
  v_prev_live := _get_live_on_hand(v_item_id, v_st);

  -- Transfer FEFO lotto-per-lotto + ledger + product_inventory
  v_moved := _fefo_transfer(v_item_id, v_wh, v_st, p_qty, p_event_id, p_event_ts, 'refill_transfer', v_meta2);

  -- Evento “umano” per BI/UI
  INSERT INTO shelf_events(event_id, item_id, shelf_id, event_type, qty_est, weight_change, event_time, meta)
  VALUES (p_event_id, v_item_id, p_shelf_id, 'restock', v_moved, NULL, p_event_ts,
          v_meta2 || jsonb_build_object('requested_qty', p_qty, 'moved_qty', v_moved))
  ON CONFLICT DO NOTHING;

  -- LIVE post-movimento → alert
  v_new_live := _get_live_on_hand(v_item_id, v_st);
  PERFORM _evaluate_low_stock_live(v_item_id, v_st, v_prev_live, v_new_live);

  -- best effort: chiudi low_stock su ufficiale
  PERFORM resolve_alert('low_stock', v_item_id, v_st, NULL);
END$$;

-- Evento sensore: converte delta peso in qty stimata e aggiorna shadow LIVE
-- NB: l’unica fonte del peso unitario è product_inventory(item_weight) per la location ‘instore’.
CREATE OR REPLACE FUNCTION apply_shelf_weight_event(
  p_event_id       TEXT,
  p_event_ts       TIMESTAMPTZ,
  p_shelf_id       TEXT,
  p_weight_change  DOUBLE PRECISION,             -- stessa unità di item_weight
  p_is_refill      BOOLEAN DEFAULT false,        -- se true → solo log
  p_meta           JSONB    DEFAULT '{}'::jsonb,
  p_noise_tol      DOUBLE PRECISION DEFAULT 0.25 -- tolleranza come frazione dell'unità
) RETURNS VOID
LANGUAGE plpgsql AS $$
DECLARE
  v_item_id     BIGINT;
  v_st          INT := _get_location_id('instore');
  v_unit_w      DOUBLE PRECISION;
  v_ratio       DOUBLE PRECISION;
  v_qty         INT;              -- pickup<0 / putback>0
  v_event_type  TEXT;
  v_is_refill   BOOLEAN := p_is_refill;
  v_meta_enrich JSONB;
  v_prev_live   INT;
  v_new_live    INT;
BEGIN
  SELECT it.item_id
    INTO v_item_id
  FROM items it
  WHERE it.shelf_id = p_shelf_id;

  IF v_item_id IS NULL THEN
    RAISE EXCEPTION 'Missing item for shelf %', p_shelf_id;
  END IF;

  -- peso unitario dalla location instore
  SELECT pi.item_weight
    INTO v_unit_w
  FROM product_inventory pi
  WHERE pi.item_id = v_item_id
    AND pi.location_id = v_st;

  IF v_unit_w IS NULL OR v_unit_w <= 0 THEN
    RAISE EXCEPTION 'Missing unit weight for shelf % (item_id=%)', p_shelf_id, v_item_id;
  END IF;

  -- consentire override via meta
  IF (p_meta ? 'is_refill') THEN
    v_is_refill := COALESCE((p_meta->>'is_refill')::BOOLEAN, v_is_refill);
  END IF;

  v_ratio := p_weight_change / v_unit_w;
  v_qty := CASE
             WHEN ABS(v_ratio - ROUND(v_ratio)) <= p_noise_tol
               THEN ROUND(v_ratio)::INT
             ELSE 0
           END;

  v_event_type := CASE
                    WHEN v_qty > 0 THEN CASE WHEN v_is_refill THEN 'restock' ELSE 'putback' END
                    WHEN v_qty < 0 THEN 'pickup'
                    ELSE 'sensor_noise'
                  END;

  v_meta_enrich := COALESCE(p_meta,'{}'::jsonb)
                   || jsonb_build_object('unit_weight',v_unit_w,
                                         'ratio',v_ratio,
                                         'is_refill',v_is_refill);

  -- Log sempre (idempotente su event_id)
  INSERT INTO shelf_events(event_id,item_id,shelf_id,event_type,qty_est,weight_change,event_time,meta)
  VALUES (p_event_id, v_item_id, p_shelf_id, v_event_type, v_qty, p_weight_change, p_event_ts, v_meta_enrich)
  ON CONFLICT DO NOTHING;

  -- Se refill o quantizzazione fallita -> solo log
  IF v_is_refill OR v_qty = 0 THEN
    RETURN;
  END IF;

  -- Shadow LIVE: aggiorna bilancio sensori e valuta alert
  v_prev_live := _get_live_on_hand(v_item_id, v_st);
  PERFORM _update_sensor_balance(v_item_id, v_st, v_qty);
  v_new_live := _get_live_on_hand(v_item_id, v_st);
  PERFORM _evaluate_low_stock_live(v_item_id, v_st, v_prev_live, v_new_live);
END$$;

-- =====================================================================
-- OVERLOAD 1: accetta NUMERIC per i reali e delega alla firma canonica
--    (risolve errori tipo: function ... (numeric, numeric) does not exist)
-- =====================================================================
CREATE OR REPLACE FUNCTION apply_shelf_weight_event(
  p_event_id       TEXT,
  p_event_ts       TIMESTAMPTZ,
  p_shelf_id       TEXT,
  p_weight_change  NUMERIC,
  p_is_refill      BOOLEAN DEFAULT false,
  p_meta           JSONB    DEFAULT '{}'::jsonb,
  p_noise_tol      NUMERIC  DEFAULT 0.25
) RETURNS VOID
LANGUAGE plpgsql AS $$
BEGIN
  PERFORM apply_shelf_weight_event(
    p_event_id,
    p_event_ts,
    p_shelf_id,
    p_weight_change::DOUBLE PRECISION,
    p_is_refill,
    p_meta,
    p_noise_tol::DOUBLE PRECISION
  );
END$$;

/* =====================
   ALERTING & THRESHOLDS
   ===================== */

-- Apre/aggiorna alert OPEN idempotente + NOTIFY (gestione senza ON CONFLICT su indice parziale)
CREATE OR REPLACE FUNCTION raise_alert(
  p_rule_key   TEXT, 
  p_severity   TEXT, 
  p_item_id    BIGINT, 
  p_location_id INT, 
  p_batch_id   BIGINT,
  p_message    TEXT, 
  p_value      NUMERIC, 
  p_meta       JSONB DEFAULT '{}'
) RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
  -- Primo tentativo: UPDATE se esiste già un OPEN equivalente
  UPDATE alerts
     SET last_seen_at = now(),
         message     = p_message,
         value_num   = p_value,
         meta        = p_meta
   WHERE status='OPEN'
     AND rule_key = p_rule_key
     AND COALESCE(item_id,0)     = COALESCE(p_item_id,0)
     AND COALESCE(location_id,0) = COALESCE(p_location_id,0)
     AND COALESCE(batch_id,0)    = COALESCE(p_batch_id,0);

  IF NOT FOUND THEN
    BEGIN
      INSERT INTO alerts(rule_key,severity,status,item_id,location_id,batch_id,message,value_num,meta)
      VALUES (p_rule_key,COALESCE(p_severity,'WARN'),'OPEN',p_item_id,p_location_id,p_batch_id,p_message,p_value,p_meta);
    EXCEPTION WHEN unique_violation THEN
      -- Race: un altro processo ha appena inserito l'OPEN → riprova con UPDATE
      UPDATE alerts
         SET last_seen_at = now(),
             message     = p_message,
             value_num   = p_value,
             meta        = p_meta
       WHERE status='OPEN'
         AND rule_key = p_rule_key
         AND COALESCE(item_id,0)     = COALESCE(p_item_id,0)
         AND COALESCE(location_id,0) = COALESCE(p_location_id,0)
         AND COALESCE(batch_id,0)    = COALESCE(p_batch_id,0);
    END;
  END IF;

  PERFORM pg_notify('alerts',
    json_build_object(
      'rule',p_rule_key,'severity',p_severity,'item_id',p_item_id,
      'location_id',p_location_id,'batch_id',p_batch_id,'message',p_message,'value',p_value
    )::TEXT);
END$$;

-- Chiude alert OPEN
CREATE OR REPLACE FUNCTION resolve_alert(
  p_rule_key   TEXT, 
  p_item_id    BIGINT, 
  p_location_id INT, 
  p_batch_id   BIGINT
) RETURNS INT LANGUAGE plpgsql AS $$
DECLARE v_count INT;
BEGIN
  UPDATE alerts
     SET status='RESOLVED', resolved_at=now()
   WHERE status='OPEN'
     AND rule_key=p_rule_key
     AND COALESCE(item_id,0)=COALESCE(p_item_id,0)
     AND COALESCE(location_id,0)=COALESCE(p_location_id,0)
     AND COALESCE(batch_id,0)=COALESCE(p_batch_id,0);
  GET DIAGNOSTICS v_count = ROW_COUNT;
  RETURN v_count;
END$$;

-- Soglia low stock: precedenza item>categoria>globale (per location)
CREATE OR REPLACE FUNCTION _get_low_stock_threshold(p_item_id BIGINT, p_location_id INT)
RETURNS INT LANGUAGE sql AS $$
  WITH cat AS (SELECT category_id FROM items WHERE item_id=p_item_id)
  SELECT COALESCE(
    (SELECT low_stock_threshold FROM inventory_thresholds 
      WHERE scope='item' AND item_id=p_item_id AND (location_id=p_location_id OR location_id IS NULL)
      ORDER BY location_id NULLS LAST LIMIT 1),
    (SELECT low_stock_threshold FROM inventory_thresholds 
      WHERE scope='category' AND category_id=(SELECT category_id FROM cat) AND (location_id=p_location_id OR location_id IS NULL)
      ORDER BY location_id NULLS LAST LIMIT 1),
    (SELECT low_stock_threshold FROM inventory_thresholds 
      WHERE scope='global' AND (location_id=p_location_id OR location_id IS NULL)
      ORDER BY location_id NULLS LAST LIMIT 1),
    0
  );
$$;

-- Trigger: low stock su update dello stock ufficiale
CREATE OR REPLACE FUNCTION trg_low_stock_alert()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE v_thr INT; v_rule TEXT := 'low_stock';
BEGIN
  v_thr := _get_low_stock_threshold(NEW.item_id, NEW.location_id);
  IF v_thr IS NULL OR v_thr <= 0 THEN RETURN NEW; END IF;

  IF (COALESCE(OLD.current_stock, v_thr+1) > v_thr) AND (NEW.current_stock <= v_thr) THEN
    PERFORM raise_alert(
      v_rule,'WARN', NEW.item_id, NEW.location_id, NULL,
      format('Stock basso: %s pezzi (soglia %s)', NEW.current_stock, v_thr),
      NEW.current_stock, jsonb_build_object('soglia',v_thr)
    );
  END IF;

  IF (OLD.current_stock <= v_thr) AND (NEW.current_stock > v_thr) THEN
    PERFORM resolve_alert(v_rule, NEW.item_id, NEW.location_id, NULL);
  END IF;

  RETURN NEW;
END$$;

DROP TRIGGER IF EXISTS t_low_stock_alert ON product_inventory;
CREATE TRIGGER t_low_stock_alert
AFTER UPDATE OF current_stock ON product_inventory
FOR EACH ROW EXECUTE FUNCTION trg_low_stock_alert();

/* =====================
   NEAR-EXPIRY BATCH CHECK
   ===================== */

CREATE OR REPLACE FUNCTION check_near_expiry()
RETURNS INT LANGUAGE plpgsql AS $$
DECLARE v_count INT := 0;
BEGIN
  WITH thr AS (
    SELECT b.batch_id, b.item_id,
           COALESCE(
             (SELECT near_expiry_days FROM inventory_thresholds t WHERE t.scope='item' AND t.item_id=b.item_id LIMIT 1),
             (SELECT near_expiry_days FROM inventory_thresholds t WHERE t.scope='category' AND t.category_id=(SELECT category_id FROM items WHERE item_id=b.item_id) LIMIT 1),
             (SELECT near_expiry_days FROM inventory_thresholds t WHERE t.scope='global' LIMIT 1),
             3
           ) AS days_thr
    FROM batches b
  )
  SELECT COUNT(*) FROM (
    SELECT bi.batch_id, bi.location_id, b.item_id, b.expiry_date, t.days_thr
    FROM batch_inventory bi
    JOIN batches b ON b.batch_id=bi.batch_id
    JOIN thr t ON t.batch_id=b.batch_id
    WHERE bi.quantity > 0
      AND b.expiry_date IS NOT NULL
      AND b.expiry_date <= (current_date + (t.days_thr || ' days')::interval)
  ) q
  INTO v_count;

  INSERT INTO alerts(rule_key,severity,status,item_id,location_id,batch_id,message,value_num,meta)
  SELECT 'near_expiry','WARN','OPEN', b.item_id, bi.location_id, b.batch_id,
         format('Lotto in scadenza (%s) qty=%s', b.expiry_date, bi.quantity),
         bi.quantity,
         jsonb_build_object('expiry',b.expiry_date,'days_thr',t.days_thr)
  FROM batch_inventory bi
  JOIN batches b ON b.batch_id=bi.batch_id
  JOIN thr t ON t.batch_id=b.batch_id
  WHERE bi.quantity > 0
    AND b.expiry_date IS NOT NULL
    AND b.expiry_date <= (current_date + (t.days_thr || ' days')::interval)
  ON CONFLICT DO NOTHING;

  -- Aggiorna/refresh se già aperti (idemp.)
  UPDATE alerts a
     SET last_seen_at=now(),
         value_num = bi.quantity,
         message   = format('Lotto in scadenza (%s) qty=%s', b.expiry_date, bi.quantity),
         meta      = jsonb_build_object('expiry',b.expiry_date,'days_thr',t.days_thr)
  FROM batch_inventory bi
  JOIN batches b ON b.batch_id=bi.batch_id
  JOIN thr t ON t.batch_id=b.batch_id
  WHERE a.status='OPEN'
    AND a.rule_key='near_expiry'
    AND a.batch_id=b.batch_id
    AND a.location_id=bi.location_id
    AND bi.quantity>0
    AND b.expiry_date IS NOT NULL
    AND b.expiry_date <= (current_date + (t.days_thr || ' days')::interval);

  -- Chiudi quelli che non sono più in near-expiry
  UPDATE alerts a
     SET status='RESOLVED', resolved_at=now()
   WHERE a.status='OPEN'
     AND a.rule_key='near_expiry'
     AND NOT EXISTS (
       SELECT 1
       FROM batch_inventory bi
       JOIN batches b ON b.batch_id=bi.batch_id
       JOIN thr t ON t.batch_id=b.batch_id
       WHERE a.batch_id=b.batch_id
         AND a.location_id=bi.location_id
         AND bi.quantity>0
         AND b.expiry_date IS NOT NULL
         AND b.expiry_date <= (current_date + (t.days_thr || ' days')::interval)
     );

  RETURN v_count;
END$$;

/* =======================================================================
   FOOT TRAFFIC — funzioni idempotenti per eventi entry/exit + aggregazioni
   ======================================================================= */

-- Applica un evento foot_traffic (ENTRY/EXIT) in modo idempotente
-- Assunzione: il consumer Spark genera un event_id unico per ogni record
CREATE OR REPLACE FUNCTION apply_foot_traffic_event(
  p_event_id   TEXT,
  p_event_ts   TIMESTAMPTZ,
  p_event_type TEXT,
  p_weekday    TEXT,
  p_time_slot  TEXT
) RETURNS VOID
LANGUAGE plpgsql AS $$
DECLARE
  v_delta    INT;
  v_curr     INT;
  v_new      INT;
  v_date     DATE := p_event_ts::date;
BEGIN
  -- Validazione tipo evento
  IF p_event_type NOT IN ('entry','exit') THEN
    RAISE EXCEPTION 'event_type non valido: %', p_event_type;
  END IF;

  -- Idempotenza: se già presente su eventi grezzi → esci
  IF EXISTS (SELECT 1 FROM foot_traffic_events WHERE event_id = p_event_id) THEN
    RETURN;
  END IF;

  -- Inserisci su tabella eventi grezzi (audit)
  INSERT INTO foot_traffic_events(event_id, event_type, event_time, weekday, time_slot)
  VALUES (p_event_id, p_event_type, p_event_ts, p_weekday, p_time_slot);

  -- Delta contatore
  v_delta := CASE WHEN p_event_type='entry' THEN 1 ELSE -1 END;

  -- Stato live corrente
  SELECT current_cnt INTO v_curr FROM foot_traffic_state WHERE id=1 FOR UPDATE;
  IF v_curr IS NULL THEN
    v_curr := 0;
  END IF;
  v_new := GREATEST(0, v_curr + v_delta); -- non andare sotto zero

  -- Persisti stato live
  UPDATE foot_traffic_state
     SET current_cnt = v_new, updated_at = now()
   WHERE id = 1;

  -- Append su tabella per dashboard (come richiesto)
  INSERT INTO foot_traffic_counter(event_type, event_time, current_foot_traffic)
  VALUES (p_event_type, p_event_ts, v_new);

  -- Aggiorna aggregato timeslot (upsert)
  INSERT INTO foot_traffic_timeslot_agg(business_date, weekday, time_slot, total_entries, total_exits, net_traffic)
  VALUES (
    v_date,
    COALESCE(p_weekday, TO_CHAR(v_date, 'Day')),
    COALESCE(p_time_slot, 'n/a'),
    CASE WHEN p_event_type='entry' THEN 1 ELSE 0 END,
    CASE WHEN p_event_type='exit'  THEN 1 ELSE 0 END,
    CASE WHEN p_event_type='entry' THEN 1 ELSE -1 END
  )
  ON CONFLICT (business_date, time_slot) DO UPDATE
    SET total_entries = foot_traffic_timeslot_agg.total_entries + EXCLUDED.total_entries,
        total_exits   = foot_traffic_timeslot_agg.total_exits   + EXCLUDED.total_exits,
        net_traffic   = foot_traffic_timeslot_agg.net_traffic   + EXCLUDED.net_traffic,
        weekday       = COALESCE(EXCLUDED.weekday, foot_traffic_timeslot_agg.weekday);
END$$;

-- (Opzionale) Ricalcolo “full” del contatore/aggregati a partire dalla tabella grezza
-- Utile per manutenzione o backfill.
CREATE OR REPLACE FUNCTION rebuild_foot_traffic_from_raw()
RETURNS VOID
LANGUAGE plpgsql AS $$
DECLARE
  r RECORD;
  v_curr INT := 0;
BEGIN
  TRUNCATE foot_traffic_counter;
  TRUNCATE foot_traffic_timeslot_agg;

  FOR r IN
    SELECT * FROM foot_traffic_events ORDER BY event_time ASC
  LOOP
    v_curr := GREATEST(0, v_curr + CASE WHEN r.event_type='entry' THEN 1 ELSE -1 END);
    INSERT INTO foot_traffic_counter(event_type, event_time, current_foot_traffic)
    VALUES (r.event_type, r.event_time, v_curr);

    INSERT INTO foot_traffic_timeslot_agg(business_date, weekday, time_slot, total_entries, total_exits, net_traffic)
    VALUES (r.event_time::date, r.weekday, COALESCE(r.time_slot,'n/a'),
            CASE WHEN r.event_type='entry' THEN 1 ELSE 0 END,
            CASE WHEN r.event_type='exit'  THEN 1 ELSE 0 END,
            CASE WHEN r.event_type='entry' THEN 1 ELSE -1 END)
    ON CONFLICT (business_date, time_slot) DO UPDATE
      SET total_entries = foot_traffic_timeslot_agg.total_entries + EXCLUDED.total_entries,
          total_exits   = foot_traffic_timeslot_agg.total_exits   + EXCLUDED.total_exits,
          net_traffic   = foot_traffic_timeslot_agg.net_traffic   + EXCLUDED.net_traffic,
          weekday       = COALESCE(EXCLUDED.weekday, foot_traffic_timeslot_agg.weekday);
  END LOOP;

  UPDATE foot_traffic_state SET current_cnt = v_curr, updated_at = now() WHERE id=1;
END$$;

