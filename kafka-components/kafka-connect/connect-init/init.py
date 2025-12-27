import json
import os
import time
import urllib.error
import urllib.request

CONNECT_URL = os.getenv("CONNECT_URL", "http://kafka-connect:8083")

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "smart_shelf")
PG_USER = os.getenv("PG_USER", "bdt_user")
PG_PASS = os.getenv("PG_PASS", "bdt_password")

# Enable/disable specific connector groups (defaults chosen to avoid clashes with Spark sinks)
ENABLE_SOURCES = os.getenv("CONNECT_INIT_ENABLE_SOURCES", "1") in ("1", "true", "True")
ENABLE_SINKS = os.getenv("CONNECT_INIT_ENABLE_SINKS", "1") in ("1", "true", "True")

# Spark `alerts-sink` already writes to Postgres by default (WRITE_TO_PG=1), so keep this off unless you want Connect to own it.
ENABLE_ALERTS_SINK = os.getenv("CONNECT_INIT_ENABLE_ALERTS_SINK", "0") in ("1", "true", "True")

# POS events contain nested arrays; JDBC sink cannot map them to the normalized tables in this repo.
ENABLE_POS_SINK = os.getenv("CONNECT_INIT_ENABLE_POS_SINK", "0") in ("1", "true", "True")


def _pg_jdbc_url() -> str:
    # stringtype=unspecified lets Postgres implicitly cast strings to enum/uuid/date columns
    return f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}?stringtype=unspecified"


def wait_for_connect(timeout_s: int = 180) -> None:
    url = f"{CONNECT_URL}/connectors"
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=3) as r:
                if r.status == 200:
                    print("[connect-init] Kafka Connect ready.")
                    return
        except Exception:
            pass
        print("[connect-init] Waiting Kafka Connect…")
        time.sleep(2)
    raise RuntimeError(f"Kafka Connect not reachable at {CONNECT_URL}")


def upsert_connector(name: str, config: dict) -> None:
    url = f"{CONNECT_URL}/connectors/{name}/config"
    data = json.dumps(config).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method="PUT",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            body = r.read().decode()
            print(f"[connect-init] Upserted {name}: {r.status} {body}")
    except urllib.error.HTTPError as e:
        print(f"[connect-init] HTTPError {name}: {e.code} {e.read().decode()}")
        raise
    except Exception as e:
        print(f"[connect-init] Error {name}: {e}")
        raise


# ---------- SOURCE (Postgres -> Kafka) ----------
def source_table_to_topic(name: str, table: str, topic: str, key_fields: list[str]) -> dict:
    # Produce to topic "<topic>" via topic.prefix + RegexRouter (strip schema prefix).
    # Key is derived from record value fields (compacted topics benefit from stable keys).
    return {
        "name": name,
        "config": {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "connection.url": _pg_jdbc_url(),
            "connection.user": PG_USER,
            "connection.password": PG_PASS,
            "table.whitelist": table,
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
            "transforms.R.replacement": "$1",
            "transforms.Key.type": "org.apache.kafka.connect.transforms.ValueToKey",
            "transforms.Key.fields": ",".join(key_fields),
        },
    }


# ---------- SINK (Kafka -> Postgres) ----------
def sink_upsert(name: str, topics: str, table: str, pk_fields: list[str], fields_whitelist: list[str] | None = None) -> dict:
    cfg: dict[str, str] = {
        "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
        "connection.url": _pg_jdbc_url(),
        "connection.user": PG_USER,
        "connection.password": PG_PASS,
        "auto.create": "false",
        "auto.evolve": "false",
        "delete.enabled": "false",
        "insert.mode": "upsert",
        "pk.mode": "record_value",
        "pk.fields": ",".join(pk_fields),
        "table.name.format": table,
        "topics": topics,
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "batch.size": "3000",
    }
    if fields_whitelist:
        cfg["fields.whitelist"] = ",".join(fields_whitelist)
    return {"name": name, "config": cfg}


def sink_insert(name: str, topics: str, table: str, fields_whitelist: list[str] | None = None) -> dict:
    cfg: dict[str, str] = {
        "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
        "connection.url": _pg_jdbc_url(),
        "connection.user": PG_USER,
        "connection.password": PG_PASS,
        "auto.create": "false",
        "auto.evolve": "false",
        "insert.mode": "insert",
        "pk.mode": "none",
        "table.name.format": table,
        "topics": topics,
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "batch.size": "3000",
    }
    if fields_whitelist:
        cfg["fields.whitelist"] = ",".join(fields_whitelist)
    return {"name": name, "config": cfg}


def main() -> None:
    wait_for_connect()

    if ENABLE_SOURCES:
        # Needed by shelf apps (weights per shelf_id)
        sources = [
            source_table_to_topic(
                name="pg-source-shelf-profiles",
                table="config.shelf_profiles",
                topic="shelf_profiles",
                key_fields=["shelf_id"],
            ),
            source_table_to_topic(
                name="pg-source-batch-catalog",
                table="config.batch_catalog",
                topic="batch_catalog",
                key_fields=["batch_code"],
            ),
        ]
        for src in sources:
            upsert_connector(src["name"], src["config"])

    if ENABLE_SINKS:
        sinks: list[dict] = [
            sink_upsert(
                name="pg-sink-shelf-state",
                topics="shelf_state",
                table="state.shelf_state",
                pk_fields=["shelf_id"],
                fields_whitelist=["shelf_id", "current_stock", "shelf_weight", "last_update_ts"],
            ),
            sink_upsert(
                name="pg-sink-wh-state",
                topics="wh_state",
                table="state.wh_state",
                pk_fields=["shelf_id"],
                fields_whitelist=["shelf_id", "wh_current_stock", "last_update_ts"],
            ),
            sink_upsert(
                name="pg-sink-shelf-batch-state",
                topics="shelf_batch_state",
                table="state.shelf_batch_state",
                pk_fields=["shelf_id", "batch_code"],
                fields_whitelist=[
                    "shelf_id",
                    "batch_code",
                    "received_date",
                    "expiry_date",
                    "batch_quantity_store",
                    "last_update_ts",
                ],
            ),
            sink_upsert(
                name="pg-sink-wh-batch-state",
                topics="wh_batch_state",
                table="state.wh_batch_state",
                pk_fields=["shelf_id", "batch_code"],
                fields_whitelist=[
                    "shelf_id",
                    "batch_code",
                    "received_date",
                    "expiry_date",
                    "batch_quantity_warehouse",
                    "batch_quantity_store",
                    "last_update_ts",
                ],
            ),
            sink_upsert(
                name="pg-sink-daily-discounts",
                topics="daily_discounts",
                table="analytics.daily_discounts",
                pk_fields=["shelf_id", "discount_date"],
                fields_whitelist=["shelf_id", "discount_date", "discount", "created_at"],
            ),
            sink_upsert(
                name="pg-sink-shelf-restock-plan",
                topics="shelf_restock_plan",
                table="ops.shelf_restock_plan",
                pk_fields=["plan_id"],
                fields_whitelist=["plan_id", "shelf_id", "suggested_qty", "status", "created_at", "updated_at"],
            ),
            sink_upsert(
                name="pg-sink-wh-supplier-plan",
                topics="wh_supplier_plan",
                table="ops.wh_supplier_plan",
                pk_fields=["supplier_plan_id"],
                fields_whitelist=[
                    "supplier_plan_id",
                    "shelf_id",
                    "suggested_qty",
                    "standard_batch_size",
                    "status",
                    "created_at",
                    "updated_at",
                ],
            ),
            sink_insert(
                name="pg-sink-wh-events",
                topics="wh_events",
                table="ops.wh_events",
                fields_whitelist=[
                    "event_id",
                    "event_type",
                    "shelf_id",
                    "batch_code",
                    "qty",
                    "timestamp",
                    "plan_id",
                    "received_date",
                    "expiry_date",
                    "reason",
                ],
            ),
        ]

        if ENABLE_ALERTS_SINK:
            sinks.append(
                sink_insert(
                    name="pg-sink-alerts",
                    topics="alerts",
                    table="ops.alerts",
                    fields_whitelist=[
                        "event_type",
                        "shelf_id",
                        "location",
                        "severity",
                        "current_stock",
                        "max_stock",
                        "target_pct",
                        "suggested_qty",
                        "status",
                        "created_at",
                        "updated_at",
                    ],
                )
            )

        if ENABLE_POS_SINK:
            sinks.append(
                sink_insert(
                    name="pg-sink-pos-transactions",
                    topics="pos_transactions",
                    table="ops.pos_transactions",
                )
            )

        for s in sinks:
            upsert_connector(s["name"], s["config"])

    print("[connect-init] Done.")


if __name__ == "__main__":
    main()
