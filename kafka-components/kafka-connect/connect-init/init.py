import os
import time
import json
import urllib.request

CONNECT_URL = os.getenv("CONNECT_URL", "http://kafka-connect:8083")
BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "smart_shelf")
PG_USER = os.getenv("PG_USER", "bdt_user")
PG_PASS = os.getenv("PG_PASS", "bdt_password")

def wait_for_connect():
    url = f"{CONNECT_URL}/connectors"
    for i in range(60):
        try:
            with urllib.request.urlopen(url, timeout=3) as r:
                if r.status == 200:
                    print("[connect-init] Kafka Connect ready.")
                    return
        except Exception:
            pass
        print("[connect-init] Waiting Kafka Connect…")
        time.sleep(2)
    raise RuntimeError("Kafka Connect not reachable")

def upsert_connector(name, config):
    url = f"{CONNECT_URL}/connectors/{name}/config"
    data = json.dumps(config).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="PUT", headers={"Content-Type":"application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            body = r.read().decode()
            print(f"[connect-init] Upserted {name}: {r.status} {body}")
    except urllib.error.HTTPError as e:
        print(f"[connect-init] HTTPError {name}: {e.code} {e.read().decode()}")
        raise
    except Exception as e:
        print(f"[connect-init] Error {name}: {e}")
        raise

def pg_jdbc_url():
    return f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

# ---------- SOURCE (Postgres -> Kafka) -----------
def source_alert_rules():
    # Produce on topic "alert_rules" (compacted metadata)
    # Use table.whitelist + RegexRouter to strip the schema prefix ("config.")
    return {
      "name": "pg-source-alert-rules",
      "config": {
        "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
        "connection.url": pg_jdbc_url(),
        "connection.user": PG_USER,
        "connection.password": PG_PASS,
        "table.whitelist": "config.alert_rules",
        "mode": "timestamp",
        "timestamp.column.name": "updated_at",
        "topic.prefix": "config.",       # creates "config.alert_rules"
        "poll.interval.ms": "10000",
        "numeric.mapping": "best_fit",
        "validate.non.null": "false",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "transforms": "R,Key",
        "transforms.R.type": "org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.R.regex": "config\\.(.*)",
        "transforms.R.replacement": "$1",                 # -> "alert_rules"
        "transforms.Key.type": "org.apache.kafka.connect.transforms.ValueToKey",
        "transforms.Key.fields": "rule_id"                # key = rule_id (UUID string)
      }
    }

def source_batch_catalog():
    return {
      "name": "pg-source-batch-catalog",
      "config": {
        "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
        "connection.url": pg_jdbc_url(),
        "connection.user": PG_USER,
        "connection.password": PG_PASS,
        "table.whitelist": "config.batch_catalog",
        "mode": "timestamp",
        "timestamp.column.name": "updated_at",
        "topic.prefix": "config.",
        "poll.interval.ms": "10000",
        "numeric.mapping": "best_fit",
        "validate.non.null": "false",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "transforms": "R,Key",
        "transforms.R.type": "org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.R.regex": "config\\.(.*)",
        "transforms.R.replacement": "$1",                 # -> "batch_catalog"
        "transforms.Key.type": "org.apache.kafka.connect.transforms.ValueToKey",
        "transforms.Key.fields": "batch_code"
      }
    }

def source_shelf_profiles():
    return {
      "name": "pg-source-shelf-profiles",
      "config": {
        "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
        "connection.url": pg_jdbc_url(),
        "connection.user": PG_USER,
        "connection.password": PG_PASS,
        "table.whitelist": "config.shelf_profiles",
        "mode": "timestamp",
        "timestamp.column.name": "updated_at",
        "topic.prefix": "config.",
        "poll.interval.ms": "10000",
        "numeric.mapping": "best_fit",
        "validate.non.null": "false",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "transforms": "R,Key",
        "transforms.R.type": "org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.R.regex": "config\\.(.*)",
        "transforms.R.replacement": "$1",                 # -> "shelf_profiles"
        "transforms.Key.type": "org.apache.kafka.connect.transforms.ValueToKey",
        "transforms.Key.fields": "shelf_id"
      }
    }

# ---------- SINK (Kafka -> Postgres) -------------
def sink_upsert(topic, table, pk_fields):
    """Generic upsert on PK fields coming from record_value (json)."""
    return {
      "name": f"pg-sink-{topic}",
      "config": {
        "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
        "connection.url": pg_jdbc_url(),
        "connection.user": PG_USER,
        "connection.password": PG_PASS,
        "auto.create": "false",
        "auto.evolve": "false",
        "delete.enabled": "false",
        "insert.mode": "upsert",
        "pk.mode": "record_value",
        "pk.fields": ",".join(pk_fields),
        "table.name.format": table,
        "topics": topic,
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "batch.size": "3000"
      }
    }

def sink_insert(topic, table):
    """Append-only mode (e.g., wh_events)."""
    return {
      "name": f"pg-sink-{topic}",
      "config": {
        "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
        "connection.url": pg_jdbc_url(),
        "connection.user": PG_USER,
        "connection.password": PG_PASS,
        "auto.create": "false",
        "auto.evolve": "false",
        "insert.mode": "insert",
        "pk.mode": "none",
        "table.name.format": table,
        "topics": topic,
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "batch.size": "3000"
      }
    }

def main():
    wait_for_connect()

    # Sources (Postgres -> Kafka, compacted metadata)
    for src in [source_alert_rules(), source_batch_catalog(), source_shelf_profiles()]:
        upsert_connector(src["name"], src["config"])

    # Sinks (Kafka -> Postgres)
    sinks = [
        # STATE compacted
        sink_upsert("shelf_state",          "state.shelf_state",          ["shelf_id"]),
        sink_upsert("wh_state",             "state.wh_state",             ["shelf_id"]),
        sink_upsert("product_total_state",  "state.product_total_state",  ["shelf_id"]),
        sink_upsert("shelf_batch_state",    "state.shelf_batch_state",    ["shelf_id","batch_code"]),
        sink_upsert("wh_batch_state",       "state.wh_batch_state",       ["shelf_id","batch_code"]),
        sink_upsert("daily_discounts",      "analytics.daily_discounts",  ["shelf_id","week"]),
        sink_upsert("wh_restock_plan",      "ops.shelf_restock_plan",     ["plan_id"]),
        # Append-only
        sink_insert("wh_events",            "ops.wh_events"),
        sink_insert("pos_transactions",     "ops.pos_transactions"),
        sink_insert("alerts",             "ops.alerts")
    ]
    for s in sinks:
        upsert_connector(s["name"], s["config"])

    print("[connect-init] All connectors upserted.")

if __name__ == "__main__":
    main()
