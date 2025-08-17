-- Helper: risolve location per testo
CREATE OR REPLACE FUNCTION _get_location_id(loc TEXT)
RETURNS INT LANGUAGE sql AS $$
  SELECT location_id FROM locations WHERE location = loc
$$;

-- Alloca/scarica per FEFO (prima scadenza)
CREATE OR REPLACE FUNCTION _fefo_decrement(
  p_item_id bigint, p_loc_id int, p_qty int, p_event_id text, p_event_ts timestamptz, p_reason text, p_meta jsonb
) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE v_left int := p_qty; v_row RECORD;
BEGIN
  FOR v_row IN
    SELECT b.batch_id, bi.quantity, b.expiry_date, b.received_date
    FROM batches b
    JOIN batch_inventory bi ON bi.batch_id=b.batch_id
    WHERE b.item_id=p_item_id AND bi.location_id=p_loc_id AND bi.quantity>0
    ORDER BY b.expiry_date NULLS LAST, b.received_date
  LOOP
    EXIT WHEN v_left<=0;
    IF v_row.quantity >= v_left THEN
      UPDATE batch_inventory SET quantity = quantity - v_left
       WHERE batch_id=v_row.batch_id AND location_id=p_loc_id;
      INSERT INTO inventory_ledger(event_id,event_ts,item_id,location_id,delta_qty,reason,batch_id,meta)
      VALUES (p_event_id||'-'||v_row.batch_id,p_event_ts,p_item_id,p_loc_id,-v_left,p_reason,v_row.batch_id,p_meta);
      v_left := 0;
    ELSE
      UPDATE batch_inventory SET quantity = 0
       WHERE batch_id=v_row.batch_id AND location_id=p_loc_id;
      INSERT INTO inventory_ledger(event_id,event_ts,item_id,location_id,delta_qty,reason,batch_id,meta)
      VALUES (p_event_id||'-'||v_row.batch_id,p_event_ts,p_item_id,p_loc_id,-v_row.quantity,p_reason,v_row.batch_id,p_meta);
      v_left := v_left - v_row.quantity;
    END IF;
  END LOOP;
  RETURN v_left; -- >0 se è andato sotto scorta lottizzata
END$$;


-- Applica una vendita: scala stock instore e scala lotti per FEFO
CREATE OR REPLACE FUNCTION apply_sale_event(
  p_event_id TEXT,
  p_event_ts TIMESTAMPTZ,
  p_shelf_id TEXT,
  p_qty INT,
  p_meta JSONB DEFAULT '{}'
) RETURNS VOID
LANGUAGE plpgsql AS $$
DECLARE
  v_item_id BIGINT;
  v_loc_id  INT := _get_location_id('instore');
  v_left    INT := p_qty;
  v_batch   RECORD;
BEGIN
  -- idempotenza: se l’evento è già nel ledger -> esci
  IF EXISTS (SELECT 1 FROM inventory_ledger WHERE event_id = p_event_id) THEN
    RETURN;
  END IF;

  SELECT item_id INTO v_item_id FROM items WHERE shelf_id = p_shelf_id;
  IF v_item_id IS NULL THEN
    RAISE EXCEPTION 'Unknown shelf_id %', p_shelf_id;
  END IF;

  -- scala stock a livello di prodotto
  UPDATE product_inventory
     SET current_stock = current_stock - p_qty
   WHERE item_id = v_item_id AND location_id = v_loc_id;

  -- FEFO sui lotti (instore)
  v_left := _fefo_decrement(v_item_id, v_loc, p_qty, p_event_id, p_event_ts, 'sale', p_meta);
  -- se manca copertura lotti, traccia residuo
  IF v_left > 0 THEN
    INSERT INTO inventory_ledger(event_id,event_ts,item_id,location_id,delta_qty,reason,meta)
    VALUES (p_event_id||'-over', p_event_ts, v_item_id, v_loc, -v_left, 'sale_overdraw', jsonb_build_object('missing_qty',v_left)::jsonb || p_meta);
  END IF;
END$$;

-- Ricezione lotto (warehouse): crea batch se manca, incrementa batch_inventory e product_inventory
-- occhio alle tempistiche: tempi diversi tra ordine e ricezione
CREATE OR REPLACE FUNCTION apply_receipt_event(
  p_event_id TEXT,
  p_event_ts TIMESTAMPTZ,
  p_shelf_id TEXT,
  p_batch_code TEXT,
  p_received DATE,
  p_expiry DATE,
  p_qty INT,
  p_meta JSONB DEFAULT '{}'
) RETURNS VOID
LANGUAGE plpgsql AS $$
DECLARE
  v_item_id BIGINT;
  v_loc_id  INT := _get_location_id('warehouse');
  v_batch_id BIGINT;
BEGIN
  IF EXISTS (SELECT 1 FROM inventory_ledger WHERE event_id = p_event_id) THEN
    RETURN;
  END IF;
  SELECT item_id INTO v_item_id FROM items WHERE shelf_id = p_shelf_id;
  IF v_item_id IS NULL THEN
    RAISE EXCEPTION 'Unknown shelf_id %', p_shelf_id;
  END IF;

  INSERT INTO batches(item_id, batch_code, received_date, expiry_date)
  VALUES (v_item_id, p_batch_code, p_received, p_expiry)
  ON CONFLICT (item_id, batch_code) DO NOTHING;

  SELECT batch_id INTO v_batch_id FROM batches WHERE item_id = v_item_id AND batch_code = p_batch_code;

  INSERT INTO batch_inventory(batch_id, location_id, quantity)
  VALUES (v_batch_id, v_loc_id, p_qty)
  ON CONFLICT (batch_id, location_id) DO UPDATE SET quantity = batch_inventory.quantity + EXCLUDED.quantity;

  INSERT INTO inventory_ledger(event_id, event_ts, item_id, location_id, delta_qty, reason, batch_id, meta)
  VALUES (p_event_id, p_event_ts, v_item_id, v_loc_id, p_qty, 'receipt', v_batch_id, p_meta);

  -- allinea product_inventory
  INSERT INTO product_inventory(item_id, location_id, current_stock)
  VALUES (v_item_id, v_loc_id, p_qty)
  ON CONFLICT (item_id, location_id) DO UPDATE SET current_stock = product_inventory.current_stock + EXCLUDED.current_stock;
END$$;

-- Refill store: sposta quantità da warehouse a instore (senza cambiare total)
CREATE OR REPLACE FUNCTION apply_refill_event(
  p_event_id TEXT,
  p_event_ts TIMESTAMPTZ,
  p_shelf_id TEXT,
  p_qty INT,
  p_meta JSONB DEFAULT '{}'
) RETURNS VOID
LANGUAGE plpgsql AS $$
DECLARE
  v_item_id BIGINT;
  v_wh INT := _get_location_id('warehouse');
  v_st INT := _get_location_id('instore');
BEGIN
  IF EXISTS (SELECT 1 FROM inventory_ledger WHERE event_id = p_event_id) THEN
    RETURN;
  END IF;
  SELECT item_id INTO v_item_id FROM items WHERE shelf_id = p_shelf_id;
  IF v_item_id IS NULL THEN
    RAISE EXCEPTION 'Unknown shelf_id %', p_shelf_id;
  END IF;

  -- decrementa stock warehouse
  UPDATE product_inventory SET current_stock = current_stock - p_qty
   WHERE item_id = v_item_id AND location_id = v_wh;

  INSERT INTO inventory_ledger(event_id, event_ts, item_id, location_id, delta_qty, reason, meta)
  VALUES (p_event_id||'-wh', p_event_ts, v_item_id, v_wh, -p_qty, 'refill', p_meta);

  -- incremento stock in store
  INSERT INTO product_inventory(item_id, location_id, current_stock)
  VALUES (v_item_id, v_st, p_qty)
  ON CONFLICT (item_id, location_id) DO UPDATE SET current_stock = product_inventory.current_stock + EXCLUDED.current_stock;

  INSERT INTO inventory_ledger(event_id, event_ts, item_id, location_id, delta_qty, reason, meta)
  VALUES (p_event_id||'-st', p_event_ts, v_item_id, v_st, +p_qty, 'refill', p_meta);

  -- (opzionale) FEFO anche per movimento lotti: sottrai da warehouse.batch_inventory (per FEFO) e aggiungi a instore.batch_inventory
  -- -> implementabile come nella apply_sale_event ma “trasferendo” sui due location_id.
END$$;

-- Evento di peso da sensore: applica pickup/restock in base al peso
-- DA CAPIRE
CREATE OR REPLACE FUNCTION apply_shelf_weight_event(
  p_event_id text, p_event_ts timestamptz, p_shelf_id text, p_weight_change double precision, p_meta jsonb DEFAULT '{}'
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE v_item_id bigint; v_st int := _loc_id('instore'); v_item_w double precision; v_qty int;
BEGIN
  IF EXISTS (SELECT 1 FROM inventory_ledger WHERE event_id=p_event_id) THEN RETURN; END IF;

  SELECT item_id, (SELECT item_weight FROM product_inventory pi WHERE pi.item_id=items.item_id AND pi.location_id=v_st)
    INTO v_item_id, v_item_w
  FROM items WHERE shelf_id=p_shelf_id;

  IF v_item_id IS NULL OR v_item_w IS NULL OR v_item_w <= 0 THEN
    RAISE EXCEPTION 'Missing item or item_weight for shelf %', p_shelf_id;
  END IF;

  v_qty := FLOOR(p_weight_change / v_item_w);
  IF v_qty = 0 THEN
    -- registra solo evento sensore
    INSERT INTO shelf_events(item_id,shelf_id,event_type,weight_change,event_time)
    VALUES (v_item_id,p_shelf_id,'pickup',p_weight_change,p_event_ts);
    RETURN;
  END IF;

  UPDATE product_inventory SET current_stock = current_stock + v_qty
   WHERE item_id=v_item_id AND location_id=v_st;

  INSERT INTO shelf_events(item_id,shelf_id,event_type,weight_change,event_time)
  VALUES (v_item_id,p_shelf_id, CASE WHEN v_qty<0 THEN 'pickup' ELSE 'restock' END, p_weight_change, p_event_ts);

  INSERT INTO inventory_ledger(event_id,event_ts,item_id,location_id,delta_qty,reason,meta)
  VALUES (p_event_id,p_event_ts,v_item_id,v_st,v_qty,'sensor', p_meta || jsonb_build_object('weight_change',p_weight_change,'qty_est',v_qty));
END$$;


-- Alza/aggiorna un alert OPEN (idempotente grazie all’indice parziale)
CREATE OR REPLACE FUNCTION raise_alert(
  p_rule_key text, p_severity text, p_item_id bigint, p_location_id int, p_batch_id bigint,
  p_message text, p_value numeric, p_meta jsonb DEFAULT '{}'
) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO alerts(rule_key,severity,status,item_id,location_id,batch_id,message,value_num,meta)
  VALUES (p_rule_key,coalesce(p_severity,'WARN'),'OPEN',p_item_id,p_location_id,p_batch_id,p_message,p_value,p_meta)
  ON CONFLICT ON CONSTRAINT uq_alert_open DO
    UPDATE SET last_seen_at=now(), message=EXCLUDED.message, value_num=EXCLUDED.value_num, meta=EXCLUDED.meta;

  -- Notifica in tempo reale (un listener esterno inoltra su Slack/Telegram)
  PERFORM pg_notify('alerts',
    json_build_object(
      'rule',p_rule_key,'severity',p_severity,'item_id',p_item_id,
      'location_id',p_location_id,'batch_id',p_batch_id,'message',p_message,'value',p_value
    )::text);
END$$;

-- Chiude (RESOLVED) tutti gli alert OPEN per la stessa chiave/entità
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

-- Per ricevere gli alert: un microservizio (o uno script) apre una connessione Postgres con LISTEN alerts; e inoltra il payload JSON su Slack/Telegram/email.

-- Recupera soglia: item > categoria > globale
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

CREATE OR REPLACE FUNCTION trg_low_stock_alert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE v_thr int; v_rule text := 'low_stock';
BEGIN
  v_thr := _get_low_stock_threshold(NEW.item_id, NEW.location_id);
  IF v_thr IS NULL OR v_thr <= 0 THEN
    RETURN NEW;
  END IF;

  -- crossing-down: sopra soglia -> sotto soglia
  IF (COALESCE(OLD.current_stock, v_thr+1) > v_thr) AND (NEW.current_stock <= v_thr) THEN
    PERFORM raise_alert(
      v_rule,'WARN', NEW.item_id, NEW.location_id, NULL,
      format('Stock basso: %s pezzi (soglia %s)', NEW.current_stock, v_thr),
      NEW.current_stock, jsonb_build_object('soglia',v_thr)
    );
  END IF;

  -- crossing-up: torna sopra soglia -> chiudi
  IF (OLD.current_stock <= v_thr) AND (NEW.current_stock > v_thr) THEN
    PERFORM resolve_alert(v_rule, NEW.item_id, NEW.location_id, NULL);
  END IF;

  RETURN NEW;
END$$;

DROP TRIGGER IF EXISTS t_low_stock_alert ON product_inventory;
CREATE TRIGGER t_low_stock_alert
AFTER UPDATE OF current_stock ON product_inventory
FOR EACH ROW EXECUTE FUNCTION trg_low_stock_alert();


-- da chiamare periodicamente (spark periodico) per verificare lotti in scadenza
CREATE OR REPLACE FUNCTION check_near_expiry()
RETURNS int LANGUAGE plpgsql AS $$
DECLARE v_count int := 0;
BEGIN
  WITH thr AS (
    -- calcola days prioritizzando item>category>global
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

  -- apri/aggiorna per ciascun batch a rischio
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

  -- chiudi quelli che non sono più in scadenza
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
