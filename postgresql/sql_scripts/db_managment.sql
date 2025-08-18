/* ================================================
   INVENTORY FUNCTIONS — FEFO + LIVE SENSOR BALANCE
   Coerenza tra stock ufficiale (POS/receipt/refill) e stock “live” (sensori)
   ================================================ */

-- Helper: risolve location per testo
CREATE OR REPLACE FUNCTION _get_location_id(loc TEXT)
RETURNS INT LANGUAGE sql AS $$
  SELECT location_id FROM locations WHERE location = loc
$$;

-- Garantisce riga in product_inventory (0 se non esiste)
CREATE OR REPLACE FUNCTION _ensure_pi_row(p_item_id bigint, p_loc_id int)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO product_inventory(item_id, location_id, current_stock)
  VALUES (p_item_id, p_loc_id, 0)
  ON CONFLICT (item_id, location_id) DO NOTHING;
END$$;

/* =====================
   SHADOW: STOCK "LIVE"
   ===================== */

CREATE OR REPLACE FUNCTION _update_sensor_balance(p_item_id bigint, p_loc_id int, p_delta int)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO sensor_balance(item_id, location_id, pending_delta)
  VALUES (p_item_id, p_loc_id, p_delta)
  ON CONFLICT (item_id, location_id)
  DO UPDATE SET pending_delta = sensor_balance.pending_delta + EXCLUDED.pending_delta,
                updated_at    = now();
END$$;

CREATE OR REPLACE FUNCTION _get_live_on_hand(p_item_id bigint, p_loc_id int)
RETURNS int LANGUAGE sql AS $$
  SELECT COALESCE((SELECT current_stock FROM product_inventory
                    WHERE item_id=p_item_id AND location_id=p_loc_id),0)
       + COALESCE((SELECT pending_delta FROM sensor_balance
                    WHERE item_id=p_item_id AND location_id=p_loc_id),0)
$$;

-- Valuta alert low_stock su base LIVE (crossing up/down)
CREATE OR REPLACE FUNCTION _evaluate_low_stock_live(
  p_item_id bigint, p_loc_id int, p_prev_live int, p_new_live int
) RETURNS void LANGUAGE plpgsql AS $$
DECLARE v_thr int;
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
  p_item_id   bigint,
  p_loc_id    int,
  p_qty       int,
  p_event_id  text,
  p_event_ts  timestamptz,
  p_reason    text,
  p_meta      jsonb
) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
  v_left int := p_qty;
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
  p_item_id   bigint,
  p_from_loc  int,
  p_to_loc    int,
  p_qty       int,
  p_event_id  text,
  p_event_ts  timestamptz,
  p_reason    text,
  p_meta      jsonb
) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
  v_left    int := p_qty;
  v_moved   int := 0;
  v_row     RECORD;
  v_q       int;
  v_meta    jsonb := COALESCE(p_meta,'{}'::jsonb) || jsonb_build_object('base_event_id', p_event_id);
  v_wh_reason text := p_reason || '_wh';
  v_st_reason text := p_reason || '_st';
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
    VALUES (p_event_id || '-' || v_row.batch_id || '-wh', p_event_ts, p_item_id, p_from_loc, -v_q, v_wh_reason, v_row.batch_id, v_meta);

    INSERT INTO inventory_ledger(event_id,event_ts,item_id,location_id,delta_qty,reason,batch_id,meta)
    VALUES (p_event_id || '-' || v_row.batch_id || '-st', p_event_ts, p_item_id, p_to_loc,  +v_q, v_st_reason, v_row.batch_id, v_meta);

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

-- Vendita POS (instore): scala stock ufficiale + lotti FEFO; compensa shadow sensori
CREATE OR REPLACE FUNCTION apply_sale_event(
  p_event_id TEXT,
  p_event_ts TIMESTAMPTZ,
  p_shelf_id TEXT,
  p_qty INT,
  p_meta JSONB DEFAULT '{}'
) RETURNS VOID
LANGUAGE plpgsql AS $$
DECLARE
  v_item_id  BIGINT;
  v_loc_id   INT := _get_location_id('instore');
  v_left     INT;
  v_pending  INT;
  v_prev_live INT; v_new_live INT;
BEGIN
  IF EXISTS (SELECT 1 FROM inventory_ledger WHERE event_id = p_event_id) THEN RETURN; END IF;

  SELECT item_id INTO v_item_id FROM items WHERE shelf_id = p_shelf_id;
  IF v_item_id IS NULL THEN RAISE EXCEPTION 'Unknown shelf_id %', p_shelf_id; END IF;

  PERFORM _ensure_pi_row(v_item_id, v_loc_id);

  -- 1) Stock ufficiale aggregato
  UPDATE product_inventory
     SET current_stock = current_stock - p_qty
   WHERE item_id = v_item_id AND location_id = v_loc_id;

  -- 2) Lotti FEFO (instore)
  v_left := _fefo_decrement(v_item_id, v_loc_id, p_qty, p_event_id, p_event_ts, 'sale', p_meta);
  IF v_left > 0 THEN
    INSERT INTO inventory_ledger(event_id,event_ts,item_id,location_id,delta_qty,reason,meta)
    VALUES (p_event_id||'-over', p_event_ts, v_item_id, v_loc_id, -v_left, 'sale_overdraw',
            jsonb_build_object('missing_qty',v_left)::jsonb || COALESCE(p_meta,'{}'::jsonb));
  END IF;

  -- 3) Compensazione LIVE: riduci pickups pendenti fino a p_qty
  v_prev_live := _get_live_on_hand(v_item_id, v_loc_id);
  SELECT COALESCE(pending_delta,0) INTO v_pending FROM sensor_balance WHERE item_id=v_item_id AND location_id=v_loc_id;
  IF COALESCE(v_pending,0) < 0 THEN
    PERFORM _update_sensor_balance(v_item_id, v_loc_id, LEAST(p_qty, -v_pending));
  END IF;
  v_new_live := _get_live_on_hand(v_item_id, v_loc_id);
  PERFORM _evaluate_low_stock_live(v_item_id, v_loc_id, v_prev_live, v_new_live);
END$$;

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
  ON CONFLICT (batch_id, location_id) DO UPDATE SET quantity = batch_inventory.quantity + EXCLUDED.quantity;

  INSERT INTO inventory_ledger(event_id, event_ts, item_id, location_id, delta_qty, reason, batch_id, meta)
  VALUES (p_event_id, p_event_ts, v_item_id, v_loc_id, p_qty, 'receipt', v_batch_id, p_meta);

  UPDATE product_inventory SET current_stock = current_stock + p_qty
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
  v_item_id BIGINT;
  v_wh INT := _get_location_id('warehouse');
  v_st INT := _get_location_id('instore');
  v_moved INT := 0;
  v_meta JSONB := COALESCE(p_meta,'{}'::jsonb) || jsonb_build_object('base_event_id', p_event_id);
  v_prev_live INT; v_new_live INT;
BEGIN
  IF p_qty IS NULL OR p_qty <= 0 THEN RAISE EXCEPTION 'Refill qty must be > 0 (got %)', p_qty; END IF;

  -- idempotenza refill: se esiste ledger con base_event_id + reason refill_transfer*
  IF EXISTS (
    SELECT 1 FROM inventory_ledger
     WHERE meta ? 'base_event_id'
       AND meta->>'base_event_id' = p_event_id
       AND reason LIKE 'refill_transfer%'
  ) THEN RETURN; END IF;

  SELECT item_id INTO v_item_id FROM items WHERE shelf_id = p_shelf_id;
  IF v_item_id IS NULL THEN RAISE EXCEPTION 'Unknown shelf_id %', p_shelf_id; END IF;

  v_meta := v_meta || jsonb_build_object('shelf_id', p_shelf_id);

  -- LIVE pre-movimento
  v_prev_live := _get_live_on_hand(v_item_id, v_st);

  -- Transfer FEFO lotto-per-lotto + ledger + product_inventory
  v_moved := _fefo_transfer(v_item_id, v_wh, v_st, p_qty, p_event_id, p_event_ts, 'refill_transfer', v_meta);

  -- Evento "umano" per BI
  INSERT INTO shelf_events(event_id, item_id, shelf_id, event_type, qty_est, weight_change, event_time, meta)
  VALUES (p_event_id, v_item_id, p_shelf_id, 'restock', v_moved, NULL, p_event_ts,
          v_meta || jsonb_build_object('requested_qty', p_qty, 'moved_qty', v_moved))
  ON CONFLICT DO NOTHING;

  -- LIVE post-movimento → alert
  v_new_live := _get_live_on_hand(v_item_id, v_st);
  PERFORM _evaluate_low_stock_live(v_item_id, v_st, v_prev_live, v_new_live);

  -- best effort: chiudi low_stock su ufficiale
  PERFORM resolve_alert('low_stock', v_item_id, v_st, NULL);
END$$;

-- Evento sensore di peso: SOLO LOG + shadow live; durante refill: solo log
CREATE OR REPLACE FUNCTION apply_shelf_weight_event(
  p_event_id       text,
  p_event_ts       timestamptz,
  p_shelf_id       text,
  p_weight_change  double precision,             -- stessa unità di unit_weight
  p_is_refill      boolean DEFAULT false,        -- se true → log only
  p_meta           jsonb    DEFAULT '{}'::jsonb,
  p_noise_tol      double precision DEFAULT 0.25 -- frazione dell'unità (0.25 = 25%)
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
  v_item_id    bigint;
  v_st         int := _get_location_id('instore');
  v_unit_w     double precision;
  v_ratio      double precision;
  v_qty        int;              -- pickup<0 / putback>0
  v_event_type text;
  v_is_refill  boolean := p_is_refill;
  v_meta_enrich jsonb;
  v_prev_live int; v_new_live int;
BEGIN
  SELECT it.item_id,
         COALESCE(it.unit_weight,
           (SELECT pi.item_weight FROM product_inventory pi
             WHERE pi.item_id=it.item_id AND pi.location_id=v_st))
    INTO v_item_id, v_unit_w
  FROM items it WHERE it.shelf_id=p_shelf_id;

  IF v_item_id IS NULL OR v_unit_w IS NULL OR v_unit_w<=0 THEN
    RAISE EXCEPTION 'Missing item or unit_weight for shelf %', p_shelf_id;
  END IF;

  IF (p_meta ? 'is_refill') THEN
    v_is_refill := COALESCE((p_meta->>'is_refill')::boolean, v_is_refill);
  END IF;

  v_ratio := p_weight_change / v_unit_w;
  v_qty := CASE WHEN abs(v_ratio - round(v_ratio)) <= p_noise_tol
                THEN round(v_ratio)::int ELSE 0 END;

  v_event_type := CASE
    WHEN v_qty > 0 THEN CASE WHEN v_is_refill THEN 'restock' ELSE 'putback' END
    WHEN v_qty < 0 THEN 'pickup'
    ELSE 'sensor_noise' END;

  v_meta_enrich := COALESCE(p_meta,'{}'::jsonb)
                   || jsonb_build_object('unit_weight',v_unit_w,'ratio',v_ratio,'is_refill',v_is_refill);

  -- log sempre
  INSERT INTO shelf_events(event_id,item_id,shelf_id,event_type,qty_est,weight_change,event_time,meta)
  VALUES (p_event_id,v_item_id,p_shelf_id,v_event_type,v_qty,p_weight_change,p_event_ts,v_meta_enrich)
  ON CONFLICT DO NOTHING;

  -- durante refill o qty=0 → solo log
  IF v_is_refill OR v_qty=0 THEN RETURN; END IF;

  -- SHADOW LIVE: aggiorna bilancio sensori e valuta alert
  v_prev_live := _get_live_on_hand(v_item_id, v_st);
  PERFORM _update_sensor_balance(v_item_id, v_st, v_qty);
  v_new_live := _get_live_on_hand(v_item_id, v_st);
  PERFORM _evaluate_low_stock_live(v_item_id, v_st, v_prev_live, v_new_live);
END$$;

/* =====================
   ALERTING & THRESHOLDS
   ===================== */

-- Apre/aggiorna alert OPEN idempotente + NOTIFY
CREATE OR REPLACE FUNCTION raise_alert(
  p_rule_key text, p_severity text, p_item_id bigint, p_location_id int, p_batch_id bigint,
  p_message text, p_value numeric, p_meta jsonb DEFAULT '{}'
) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO alerts(rule_key,severity,status,item_id,location_id,batch_id,message,value_num,meta)
  VALUES (p_rule_key,coalesce(p_severity,'WARN'),'OPEN',p_item_id,p_location_id,p_batch_id,p_message,p_value,p_meta)
  ON CONFLICT ON CONSTRAINT uq_alert_open DO
    UPDATE SET last_seen_at=now(), message=EXCLUDED.message, value_num=EXCLUDED.value_num, meta=EXCLUDED.meta;

  PERFORM pg_notify('alerts',
    json_build_object(
      'rule',p_rule_key,'severity',p_severity,'item_id',p_item_id,
      'location_id',p_location_id,'batch_id',p_batch_id,'message',p_message,'value',p_value
    )::text);
END$$;

-- Chiude alert OPEN
CREATE OR REPLACE FUNCTION resolve_alert(
  p_rule_key text, p_item_id bigint, p_location_id int, p_batch_id bigint
) RETURNS int LANGUAGE plpgsql AS $$
DECLARE v_count int;
BEGIN
  UPDATE alerts
     SET status='RESOLVED', resolved_at=now()
   WHERE status='OPEN'
     AND rule_key=p_rule_key
     AND coalesce(item_id,0)=coalesce(p_item_id,0)
     AND coalesce(location_id,0)=coalesce(p_location_id,0)
     AND coalesce(batch_id,0)=coalesce(p_batch_id,0);
  GET DIAGNOSTICS v_count = ROW_COUNT;
  RETURN v_count;
END$$;

-- Soglia low stock: precedenza item>categoria>globale (per location)
CREATE OR REPLACE FUNCTION _get_low_stock_threshold(p_item_id bigint, p_location_id int)
RETURNS int LANGUAGE sql AS $$
  WITH cat AS (SELECT category_id FROM items WHERE item_id=p_item_id)
  SELECT COALESCE(
    (SELECT low_stock_threshold FROM inventory_thresholds WHERE scope='item' AND item_id=p_item_id AND (location_id=p_location_id OR location_id IS NULL) ORDER BY location_id NULLS LAST LIMIT 1),
    (SELECT low_stock_threshold FROM inventory_thresholds WHERE scope='category' AND category_id=(SELECT category_id FROM cat) AND (location_id=p_location_id OR location_id IS NULL) ORDER BY location_id NULLS LAST LIMIT 1),
    (SELECT low_stock_threshold FROM inventory_thresholds WHERE scope='global' AND (location_id=p_location_id OR location_id IS NULL) ORDER BY location_id NULLS LAST LIMIT 1),
    0
  );
$$;

-- Trigger: low stock su update stock ufficiale
CREATE OR REPLACE FUNCTION trg_low_stock_alert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE v_thr int; v_rule text := 'low_stock';
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
RETURNS int LANGUAGE plpgsql AS $$
DECLARE v_count int := 0;
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
  ON CONFLICT ON CONSTRAINT uq_alert_open DO
    UPDATE SET last_seen_at=now(), value_num=EXCLUDED.value_num, message=EXCLUDED.message, meta=EXCLUDED.meta;

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
         AND b.expiry_date <= (current_date + (t.days_thr || ' days')::interval)
     );

  RETURN v_count;
END$$;
