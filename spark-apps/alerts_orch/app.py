"""
app.py (alerts_orch) — Entry point Spark per refill low_stock (set-based) e
alert near_expiry, allineato allo stile/ambiente di spark-shelf.

Caratteristiche:
  • Solo Postgres (niente lettura Kafka qui), due stream timer separati.
  • Refill low_stock in SQL **set-based** (una query fa tutto).
  • Near-expiry: open/refresh/resolve alert secondo inventory_thresholds.
  • Env e default coerenti con app Spark esistente (PG_* e /chk paths).

Env richieste (default come spark-shelf):
  PG_HOST=postgres
  PG_PORT=5432
  PG_DB=retaildb
  PG_USER=retail
  PG_PASS=retailpass
  CHECKPOINT_DIR=/chk/alerts_orch
  SPARK_APP_NAME=alerts-orch
  LOW_STOCK_INTERVAL_SECS=10   # "caldo"
  NEAR_EXP_INTERVAL_MINS=10    # "lento"

Dipendenze: pyspark, psycopg2-binary, kafka-python (opzionale per emit refill)
"""
from __future__ import annotations

import os
import sys
import json
from dataclasses import dataclass
from typing import Optional

import psycopg2
from psycopg2.extras import DictCursor
from pyspark.sql import SparkSession, DataFrame

# Kafka opzionale per emettere PUTBACK is_refill=true
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

try:
    LOW_STOCK_INTERVAL_SECS = int(env("LOW_STOCK_INTERVAL_SECS", "10"))
except ValueError:
    LOW_STOCK_INTERVAL_SECS = 10
try:
    NEAR_EXP_INTERVAL_MINS = int(env("NEAR_EXP_INTERVAL_MINS", "10"))
except ValueError:
    NEAR_EXP_INTERVAL_MINS = 10

PG_HOST = env("PG_HOST", "postgres")
PG_PORT = env("PG_PORT", "5432")
PG_DB   = env("PG_DB",   "retaildb")
PG_USER = env("PG_USER", "retail")
PG_PASS = env("PG_PASS", "retailpass")

DSN = f"host={PG_HOST} port={PG_PORT} dbname={PG_DB} user={PG_USER} password={PG_PASS}"

# Kafka env
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", os.getenv("KAFKA_BROKER", "kafka:9092"))
TOPIC_SHELF     = os.getenv("TOPIC_SHELF", os.getenv("KAFKA_TOPIC_SHELF", "shelf_events"))
EMIT_KAFKA_REFILL = os.getenv("EMIT_KAFKA_REFILL", "true").lower() in {"1","true","yes"}
EMIT_REFILL_WEIGHT_CHANGE = os.getenv("EMIT_REFILL_WEIGHT_CHANGE", "false").lower() in {"1","true","yes"}
REFILL_UNIT_WEIGHT_DEFAULT_KG = float(os.getenv("REFILL_UNIT_WEIGHT_DEFAULT_KG", "0.25"))

# ---------------------------------------------------------------------------
# DB accessors
# ---------------------------------------------------------------------------

@dataclass
class DB:
    dsn: str

    def connect(self):
        conn = psycopg2.connect(self.dsn)
        with conn.cursor() as cur:
            try:
                cur.execute("SET LOCAL statement_timeout TO '10s';")
            except Exception:
                pass
        return conn

    def ping(self) -> None:
        with psycopg2.connect(self.dsn) as c, c.cursor() as cur:
            cur.execute("SELECT 1;")

    def process_low_stock_refills_setbased(self, conn):
        """Esegue refill set-based e restituisce i record utili per eventuale emit Kafka."""
        sql = """
        WITH instore AS (
          SELECT location_id FROM locations WHERE location='instore'
        ), warehouse AS (
          SELECT location_id FROM locations WHERE location='warehouse'
        ), candidates AS (
          SELECT
            a.alert_id,
            a.item_id,
            i.shelf_id,
            a.location_id AS store_loc_id,
            _get_low_stock_threshold(a.item_id, a.location_id) AS thr,
            COALESCE((
               SELECT safety_stock FROM inventory_thresholds t
                WHERE t.scope='item' AND t.item_id=a.item_id AND (t.location_id=a.location_id OR t.location_id IS NULL)
                ORDER BY t.location_id NULLS LAST LIMIT 1
            ), (
               SELECT safety_stock FROM inventory_thresholds t
                WHERE t.scope='global' AND (t.location_id=a.location_id OR t.location_id IS NULL)
                ORDER BY t.location_id NULLS LAST LIMIT 1
            ), 0) AS safety,
            _get_live_on_hand(a.item_id, a.location_id) AS live,
            COALESCE((SELECT current_stock FROM product_inventory pi
                      WHERE pi.item_id=a.item_id AND pi.location_id=(SELECT location_id FROM warehouse)),0) AS wh_available
          FROM alerts a
          JOIN items i ON i.item_id = a.item_id
          WHERE a.status='OPEN' AND a.rule_key='low_stock'
            AND a.location_id IN (SELECT location_id FROM instore)
            AND COALESCE((a.meta->>'refill_emitted')::boolean, false) IS NOT TRUE
        ), qty_calc AS (
          SELECT
            c.*, 
            GREATEST(1, GREATEST(c.safety, CEIL(NULLIF(c.thr,0)/2.0)))              AS buf,
            GREATEST(c.thr + GREATEST(c.safety, CEIL(NULLIF(c.thr,0)/2.0)), c.thr+1) AS target,
            GREATEST(0, (GREATEST(c.thr + GREATEST(c.safety, CEIL(NULLIF(c.thr,0)/2.0)), c.thr+1) - GREATEST(c.live,0))) AS need
          FROM candidates c
        ), final_sel AS (
          SELECT
            q.alert_id,
            q.item_id,
            q.shelf_id,
            LEAST(q.need, GREATEST(0,q.wh_available))::int AS qty
          FROM qty_calc q
          WHERE LEAST(q.need, GREATEST(0,q.wh_available))::int > 0
        ), prepared AS (
          SELECT
            f.alert_id,
            f.item_id,
            f.shelf_id,
            f.qty,
            'refill-'||f.item_id||'-'||substr(md5(random()::text||clock_timestamp()::text),1,12) AS event_id,
            jsonb_build_object(
              'triggered_by','low_stock',
              'alert_id', f.alert_id,
              'recommended_qty', f.qty,
              'is_refill', true
            ) AS meta
          FROM final_sel f
        ), applied AS (
          SELECT apply_refill_event(p.event_id, now(), p.shelf_id, p.qty, p.meta)
          FROM prepared p
        ), updated AS (
          UPDATE alerts a
             SET meta = COALESCE(a.meta,'{}'::jsonb) || jsonb_build_object(
                          'refill_emitted', true,
                          'refill_event_id', p.event_id,
                          'refill_qty', p.qty,
                          'refill_emitted_at', now()
                        ),
                 last_seen_at = now()
          FROM prepared p
          WHERE a.alert_id = p.alert_id
        )
        SELECT p.event_id, p.item_id, p.shelf_id, p.qty
        FROM prepared p;
        """
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            return [dict(r) for r in rows]

    def process_near_expiry_alerts(self, conn) -> None:
        # OPEN/REFRESH
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH instore AS (
                  SELECT location_id FROM locations WHERE location='instore'
                ),
                item_min_expire AS (
                  SELECT b.item_id, bi.location_id, MIN(b.expiry_date)::date AS min_expiry
                  FROM batch_inventory bi
                  JOIN batches b ON b.batch_id = bi.batch_id
                  WHERE bi.quantity > 0
                    AND bi.location_id IN (SELECT location_id FROM instore)
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
                              WHERE t.scope='global'
                                AND (t.location_id=i.location_id OR t.location_id IS NULL)
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
                SELECT 'near_expiry', 'OPEN', c.item_id, c.location_id, now(), now(),
                       jsonb_build_object('min_expiry', c.min_expiry, 'days_left', c.days_left, 'threshold', c.near_thr)
                FROM candidates c
                WHERE NOT EXISTS (
                  SELECT 1 FROM alerts a
                   WHERE a.rule_key='near_expiry' AND a.status='OPEN'
                     AND a.item_id=c.item_id AND a.location_id=c.location_id
                );
                """
            )
        # RESOLVE
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH instore AS (
                  SELECT location_id FROM locations WHERE location='instore'
                ),
                still_near AS (
                  SELECT a.item_id, a.location_id
                  FROM alerts a
                  JOIN batch_inventory bi ON bi.location_id=a.location_id AND bi.quantity>0
                  JOIN batches b ON b.batch_id = bi.batch_id AND b.item_id=a.item_id AND b.expiry_date IS NOT NULL
                  WHERE a.rule_key='near_expiry' AND a.status='OPEN'
                    AND a.location_id IN (SELECT location_id FROM instore)
                  GROUP BY a.item_id, a.location_id
                  HAVING MIN(b.expiry_date)::date - CURRENT_DATE
                         <= COALESCE(
                              (SELECT t.near_expiry_days FROM inventory_thresholds t
                               WHERE t.scope='item' AND t.item_id=a.item_id
                                 AND (t.location_id=a.location_id OR t.location_id IS NULL)
                               ORDER BY t.location_id NULLS LAST LIMIT 1),
                              (SELECT t.near_expiry_days FROM inventory_thresholds t
                               WHERE t.scope='global'
                                 AND (t.location_id=a.location_id OR t.location_id IS NULL)
                               ORDER BY t.location_id NULLS LAST LIMIT 1),
                              3
                            )
                )
                UPDATE alerts a
                   SET status='RESOLVED',
                       resolved_at=now(),
                       meta = COALESCE(a.meta,'{}'::jsonb) || jsonb_build_object('resolved_reason','expiry_ok')
                 WHERE a.rule_key='near_expiry' AND a.status='OPEN'
                   AND a.location_id IN (SELECT location_id FROM instore)
                   AND (a.item_id, a.location_id) NOT IN (SELECT item_id, location_id FROM still_near);
                """
            )

# ---------------------------------------------------------------------------
# Spark plumbing
# ---------------------------------------------------------------------------

def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.streaming.metadata.compression", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

db = DB(DSN)

# === callback functions (low_stock, near_expiry) ===

def _run_low_stock_refills(_: DataFrame, __: int) -> None:
    conn = db.connect()
    try:
        prepared_rows = db.process_low_stock_refills_setbased(conn) or []
        conn.commit()
        print(f"[alerts-orch] ✅ low_stock processed; prepared={len(prepared_rows)}")
    except Exception as e:
        conn.rollback()
        print(f"[alerts-orch] ❌ low_stock batch error: {e}")
        raise
    finally:
        conn.close()

def _run_near_expiry_check(_: DataFrame, __: int) -> None:
    conn = db.connect()
    try:
        db.process_near_expiry_alerts(conn)
        conn.commit()
        print(f"[alerts-orch] ✅ near_expiry processed")
    except Exception as e:
        conn.rollback()
        print(f"[alerts-orch] ❌ near_expiry batch error: {e}")
        raise
    finally:
        conn.close()

def main() -> None:
    try:
        db.ping()
        print("[alerts-orch] ✅ DB reachable")
    except Exception as e:
        print(f"[alerts-orch] ❌ DB connection failed: {e}")
        sys.exit(2)

    spark = build_spark()

    # timers
    clock_refill = spark.readStream.format("rate").option("rowsPerSecond", 1).load()
    clock_expiry = spark.readStream.format("rate").option("rowsPerSecond", 1).load()

    q_refills = (
        clock_refill.writeStream
        .foreachBatch(_run_low_stock_refills)
        .outputMode("update")
        .trigger(processingTime=f"{LOW_STOCK_INTERVAL_SECS} seconds")
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "refills"))
        .start()
    )

    q_expiry = (
        clock_expiry.writeStream
        .foreachBatch(_run_near_expiry_check)
        .outputMode("update")
        .trigger(processingTime=f"{NEAR_EXP_INTERVAL_MINS} minutes")
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "near_expiry"))
        .start()
    )

    print(f"[alerts-orch] ▶ running with LOW_STOCK_INTERVAL_SECS={LOW_STOCK_INTERVAL_SECS}, NEAR_EXP_INTERVAL_MINS={NEAR_EXP_INTERVAL_MINS}")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
