"""
alerts_orch/app.py — Orchestratore alert & refill.

Funzioni:
  • Aggiorna/apre/chiude alert low_stock (da shelf_status).
  • Applica refill set-based per low_stock.
  • Apre/aggiorna/chiude alert near_expiry (da batches/batch_inventory).
  • Eventuale emissione refill events su Kafka.
"""

from __future__ import annotations
import os, sys, json
from dataclasses import dataclass
from typing import Optional
from datetime import datetime, time

import psycopg2
from psycopg2.extras import DictCursor
from pyspark.sql import SparkSession, DataFrame

# Kafka opzionale
try:
    from kafka import KafkaProducer
    _KAFKA_AVAILABLE = True
except Exception:
    _KAFKA_AVAILABLE = False

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def env(key: str, default: Optional[str] = None) -> str:
    v = os.getenv(key)
    return v if v is not None else (default if default is not None else "")

SPARK_APP_NAME = env("SPARK_APP_NAME", "alerts-orch")
CHECKPOINT_DIR = env("CHECKPOINT_DIR", "/chk/alerts_orch")
LOW_STOCK_INTERVAL_SECS = int(env("LOW_STOCK_INTERVAL_SECS", "10"))
NEAR_EXP_INTERVAL_MINS = int(env("NEAR_EXP_INTERVAL_MINS", "10"))

PG_HOST = env("PG_HOST", "postgres")
PG_PORT = env("PG_PORT", "5432")
PG_DB   = env("PG_DB",   "retaildb")
PG_USER = env("PG_USER", "retail")
PG_PASS = env("PG_PASS", "retailpass")

DSN = f"host={PG_HOST} port={PG_PORT} dbname={PG_DB} user={PG_USER} password={PG_PASS}"

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", os.getenv("KAFKA_BROKER", "kafka:9092"))
TOPIC_SHELF     = os.getenv("TOPIC_SHELF", os.getenv("KAFKA_TOPIC_SHELF", "shelf_events"))
EMIT_KAFKA_REFILL = os.getenv("EMIT_KAFKA_REFILL", "true").lower() in {"1","true","yes"}
EMIT_REFILL_WEIGHT_CHANGE = os.getenv("EMIT_REFILL_WEIGHT_CHANGE", "false").lower() in {"1","true","yes"}
REFILL_UNIT_WEIGHT_DEFAULT_KG = float(os.getenv("REFILL_UNIT_WEIGHT_DEFAULT_KG", "0.25"))

# 👇 NEW: locations parametrizzate
ALERT_LOCATIONS = [s.strip() for s in env("ALERT_LOCATIONS", "instore").split(",") if s.strip()]

# ---------------------------------------------------------------------------
# DB accessor
# ---------------------------------------------------------------------------

@dataclass
class DB:
    dsn: str

    def connect(self):
        return psycopg2.connect(self.dsn)

    def ping(self):
        with psycopg2.connect(self.dsn) as c, c.cursor() as cur:
            cur.execute("SELECT 1;")

    # -------- LOW STOCK ALERTS (open/refresh/close)
    def process_low_stock_alerts(self, conn):
        with conn.cursor() as cur:
            cur.execute(f"""
            WITH locs AS (
              SELECT location_id FROM locations WHERE location = ANY(%s)
            )
            INSERT INTO alerts(rule_key,status,item_id,location_id,opened_at,last_seen_at,meta)
            SELECT 'low_stock','OPEN',i.item_id,pi.location_id,now(),now(),
                   jsonb_build_object('shelf_id',ss.shelf_id)
            FROM shelf_status ss
            JOIN items i ON i.shelf_id=ss.shelf_id
            JOIN product_inventory pi ON pi.item_id=i.item_id
            WHERE pi.location_id IN (SELECT location_id FROM locs)
              AND ss.status='critical'
              AND NOT EXISTS (
                SELECT 1 FROM alerts a
                WHERE a.item_id=i.item_id AND a.location_id=pi.location_id
                  AND a.rule_key='low_stock' AND a.status='OPEN'
              );
            """, (ALERT_LOCATIONS,))

            # Refresh
            cur.execute("""
            UPDATE alerts a
            SET last_seen_at=now()
            FROM shelf_status ss
            JOIN items i ON i.shelf_id=ss.shelf_id
            WHERE a.item_id=i.item_id AND a.rule_key='low_stock' AND a.status='OPEN';
            """)

            # Close
            cur.execute("""
            UPDATE alerts a
            SET status='RESOLVED',resolved_at=now(),
                meta=COALESCE(a.meta,'{}'::jsonb)||jsonb_build_object('resolved_reason','stock_ok')
            FROM shelf_status ss
            JOIN items i ON i.shelf_id=ss.shelf_id
            WHERE a.item_id=i.item_id
              AND a.rule_key='low_stock' AND a.status='OPEN'
              AND ss.status='ok';
            """)

    # -------- LOW STOCK REFILLS (rimane su instore)
    def process_low_stock_refills_setbased(self, conn):
        sql = """
        WITH instore AS (
          SELECT location_id FROM locations WHERE location='instore'
        ), warehouse AS (
          SELECT location_id FROM locations WHERE location='warehouse'
        ), candidates AS (
          SELECT a.alert_id,a.item_id,i.shelf_id,a.location_id,
                 _get_low_stock_threshold(a.item_id,a.location_id) AS thr,
                 COALESCE((SELECT safety_stock FROM inventory_thresholds t
                           WHERE t.scope='item' AND t.item_id=a.item_id
                             AND (t.location_id=a.location_id OR t.location_id IS NULL)
                           ORDER BY t.location_id NULLS LAST LIMIT 1),0) AS safety,
                 _get_live_on_hand(a.item_id,a.location_id) AS live,
                 COALESCE((SELECT current_stock FROM product_inventory pi
                           WHERE pi.item_id=a.item_id
                             AND pi.location_id=(SELECT location_id FROM warehouse)),0) AS wh_available
          FROM alerts a
          JOIN items i ON i.item_id=a.item_id
          WHERE a.status='OPEN' AND a.rule_key='low_stock'
            AND a.location_id IN (SELECT location_id FROM instore)
            AND COALESCE((a.meta->>'refill_emitted')::boolean,false) IS NOT TRUE
        ), qty_calc AS (
          SELECT c.*, GREATEST(1,GREATEST(c.safety,CEIL(NULLIF(c.thr,0)/2.0))) AS buf,
                 GREATEST(c.thr+GREATEST(c.safety,CEIL(NULLIF(c.thr,0)/2.0)),c.thr+1) AS target,
                 GREATEST(0,(GREATEST(c.thr+GREATEST(c.safety,CEIL(NULLIF(c.thr,0)/2.0)),c.thr+1)-GREATEST(c.live,0))) AS need
          FROM candidates c
        ), final_sel AS (
          SELECT q.alert_id,q.item_id,q.shelf_id,LEAST(q.need,GREATEST(0,q.wh_available))::int AS qty
          FROM qty_calc q
          WHERE LEAST(q.need,GREATEST(0,q.wh_available))::int > 0
        ), prepared AS (
          SELECT f.alert_id,f.item_id,f.shelf_id,f.qty,
                 'refill-'||f.item_id||'-'||substr(md5(random()::text||clock_timestamp()::text),1,12) AS event_id,
                 jsonb_build_object('triggered_by','low_stock','alert_id',f.alert_id,
                                    'recommended_qty',f.qty,'is_refill',true) AS meta
          FROM final_sel f
        )
        SELECT * FROM prepared;
        """
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(sql)
            return [dict(r) for r in cur.fetchall()]

    # -------- NEAR EXPIRY ALERTS
    def process_near_expiry_alerts(self, conn) -> None:
        with conn.cursor() as cur:
            cur.execute("""
                WITH locs AS (
                  SELECT location_id FROM locations WHERE location = ANY(%s)
                ),
                item_min_expire AS (
                  SELECT b.item_id, bi.location_id, MIN(b.expiry_date)::date AS min_expiry
                  FROM batch_inventory bi
                  JOIN batches b ON b.batch_id = bi.batch_id
                  WHERE bi.quantity > 0
                    AND bi.location_id IN (SELECT location_id FROM locs)
                    AND b.expiry_date IS NOT NULL
                  GROUP BY b.item_id, bi.location_id
                ),
                thr AS (
                  SELECT i.item_id, i.location_id,
                         i.min_expiry,
                         COALESCE(
                           (SELECT t.near_expiry_days FROM inventory_thresholds t
                              WHERE t.scope='item' AND t.item_id=i.item_id
                                AND (t.location_id=i.location_id OR t.location_id IS NULL)
                              ORDER BY t.location_id NULLS LAST LIMIT 1),
                           (SELECT t.near_expiry_days FROM inventory_thresholds t
                              WHERE t.scope='global' AND (t.location_id=i.location_id OR t.location_id IS NULL)
                              ORDER BY t.location_id NULLS LAST LIMIT 1),
                           3
                         ) AS near_thr
                  FROM item_min_expire i
                ),
                candidates AS (
                  SELECT t.item_id, t.location_id, t.min_expiry,
                         GREATEST(0, (t.min_expiry - CURRENT_DATE))::int AS days_left,
                         t.near_thr
                  FROM thr t
                  WHERE (t.min_expiry - CURRENT_DATE) <= t.near_thr
                ),
                updated AS (
                  UPDATE alerts a
                     SET last_seen_at = now(),
                         meta = COALESCE(a.meta,'{}'::jsonb) || jsonb_build_object(
                                  'min_expiry', c.min_expiry,
                                  'days_left',  c.days_left,
                                  'threshold',  c.near_thr
                                )
                  FROM candidates c
                  WHERE a.rule_key='near_expiry' AND a.status='OPEN'
                    AND a.item_id=c.item_id AND a.location_id=c.location_id
                  RETURNING a.alert_id
                )
                INSERT INTO alerts(rule_key, status, item_id, location_id, opened_at, last_seen_at, meta)
                SELECT 'near_expiry','OPEN', c.item_id, c.location_id, now(), now(),
                       jsonb_build_object('min_expiry', c.min_expiry, 'days_left', c.days_left, 'threshold', c.near_thr)
                FROM candidates c
                WHERE NOT EXISTS (
                  SELECT 1 FROM alerts a
                   WHERE a.rule_key='near_expiry' AND a.status='OPEN'
                     AND a.item_id=c.item_id AND a.location_id=c.location_id
                );
            """, (ALERT_LOCATIONS,))

        with conn.cursor() as cur:
            cur.execute("""
                WITH locs AS (
                  SELECT location_id FROM locations WHERE location = ANY(%s)
                ),
                still_near AS (
                  SELECT a.item_id, a.location_id
                  FROM alerts a
                  JOIN batch_inventory bi ON bi.location_id=a.location_id AND bi.quantity>0
                  JOIN batches b ON b.batch_id = bi.batch_id AND b.item_id=a.item_id AND b.expiry_date IS NOT NULL
                  WHERE a.rule_key='near_expiry' AND a.status='OPEN'
                    AND a.location_id IN (SELECT location_id FROM locs)
                  GROUP BY a.item_id, a.location_id
                  HAVING MIN(b.expiry_date)::date - CURRENT_DATE
                         <= COALESCE(
                              (SELECT t.near_expiry_days FROM inventory_thresholds t
                                WHERE t.scope='item' AND t.item_id=a.item_id
                                  AND (t.location_id=a.location_id OR t.location_id IS NULL)
                                ORDER BY t.location_id NULLS LAST LIMIT 1),
                              (SELECT t.near_expiry_days FROM inventory_thresholds t
                                WHERE t.scope='global' AND (t.location_id=a.location_id OR t.location_id IS NULL)
                                ORDER BY t.location_id NULLS LAST LIMIT 1),
                              3
                            )
                )
                UPDATE alerts a
                   SET status='RESOLVED',
                       resolved_at=now(),
                       meta = COALESCE(a.meta,'{}'::jsonb) || jsonb_build_object('resolved_reason','expiry_ok')
                 WHERE a.rule_key='near_expiry' AND a.status='OPEN'
                   AND a.location_id IN (SELECT location_id FROM locs)
                   AND (a.item_id, a.location_id) NOT IN (SELECT item_id, location_id FROM still_near);
            """, (ALERT_LOCATIONS,))

# ---------------------------------------------------------------------------
# Spark plumbing
# ---------------------------------------------------------------------------

def build_spark():
    return (SparkSession.builder.appName(SPARK_APP_NAME)
            .config("spark.sql.session.timeZone","UTC")
            .config("spark.sql.streaming.metadata.compression","false")
            .getOrCreate())

db = DB(DSN)

# -------- Job wrappers
def _run_low_stock_alerts(_: DataFrame, __: int):
    conn = db.connect()
    try:
        db.process_low_stock_alerts(conn); conn.commit()
        print(f"[alerts-orch] ✅ low_stock alerts refreshed (locations={ALERT_LOCATIONS})")
    except Exception as e:
        conn.rollback(); print(f"[alerts-orch] ❌ low_stock alerts error: {e}"); raise
    finally:
        conn.close()

def _run_low_stock_refills(_: DataFrame, __: int):
    conn = db.connect()
    try:
        rows = db.process_low_stock_refills_setbased(conn); conn.commit()
        print(f"[alerts-orch] ✅ low_stock refills processed; prepared={len(rows)}")
    except Exception as e:
        conn.rollback(); print(f"[alerts-orch] ❌ low_stock refill error: {e}"); raise
    finally:
        conn.close()
    # Kafka emit opzionale
    if EMIT_KAFKA_REFILL and rows:
        if not _KAFKA_AVAILABLE: print("[alerts-orch] ⚠ kafka not avail"); return
        try:
            prod = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                linger_ms=10, acks="all", retries=5)
        except Exception as e: print(f"[alerts-orch] ⚠ Kafka init fail: {e}"); return
        from datetime import datetime, timezone
        now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
        for r in rows:
            shelf_id=r["shelf_id"]; qty=int(r["qty"]); eid=r["event_id"]
            evt={"event_type":"putback","event_id":eid,"shelf_id":shelf_id,"item_id":shelf_id,
                 "quantity":qty,"timestamp":now_iso,"is_refill":True}
            prod.send(TOPIC_SHELF,value=evt)
        prod.flush(5); prod.close()
        print(f"[alerts-orch] ▶ emitted {len(rows)} refill putbacks")

def _run_near_expiry(_: DataFrame, __: int):
    conn = db.connect()
    try:
        db.process_near_expiry_alerts(conn); conn.commit()
        print(f"[alerts-orch] ✅ near_expiry processed (locations={ALERT_LOCATIONS})")
    except Exception as e:
        conn.rollback(); print(f"[alerts-orch] ❌ near_expiry error: {e}"); raise
    finally:
        conn.close()

def _run_trash_expiry(_: DataFrame, __: int):
    conn = db.connect()
    try:
        now_ts = datetime.utcnow()
        closing_time = time(22, 0) 
        with conn.cursor() as cur:
            cur.execute("SELECT trash_expiry(%s, %s);", (now_ts, closing_time))
        conn.commit()
        print(f"[alerts-orch] ✅ trash_expiry executed at {now_ts}")
    except Exception as e:
        conn.rollback()
        print(f"[alerts-orch] ❌ trash_expiry error: {e}")
        raise
    finally:
        conn.close()


# -------- Main
def main():
    try: db.ping(); print("[alerts-orch] ✅ DB reachable")
    except Exception as e: print(f"[alerts-orch] ❌ DB connect fail: {e}"); sys.exit(2)

    spark = build_spark()
    clock = (spark.readStream.format("rate").option("rowsPerSecond",1).load())

    q_alerts = (clock.writeStream.foreachBatch(_run_low_stock_alerts)
                .trigger(processingTime=f"{LOW_STOCK_INTERVAL_SECS} seconds")
                .option("checkpointLocation",os.path.join(CHECKPOINT_DIR,"low_stock_alerts")).start())

    q_refills = (clock.writeStream.foreachBatch(_run_low_stock_refills)
                 .trigger(processingTime=f"{LOW_STOCK_INTERVAL_SECS} seconds")
                 .option("checkpointLocation",os.path.join(CHECKPOINT_DIR,"low_stock_refills")).start())

    q_expiry = (clock.writeStream.foreachBatch(_run_near_expiry)
                .trigger(processingTime=f"{NEAR_EXP_INTERVAL_MINS} minutes")
                .option("checkpointLocation",os.path.join(CHECKPOINT_DIR,"near_expiry")).start())
    
    q_trash = (clock.writeStream.foreachBatch(_run_trash_expiry)
               .trigger(processingTime="60 seconds") 
               .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "trash_expiry"))
               .start())

    print(f"[alerts-orch] ▶ running with low_stock_interval={LOW_STOCK_INTERVAL_SECS}s, near_expiry_interval={NEAR_EXP_INTERVAL_MINS}m, trash_expiry=active, locations={ALERT_LOCATIONS}")
    spark.streams.awaitAnyTermination()

if __name__=="__main__":
    main()
