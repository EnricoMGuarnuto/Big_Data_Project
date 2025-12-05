import os
import time
from typing import Dict, List
from kafka.admin import KafkaAdminClient, NewTopic, ConfigResource, ConfigResourceType
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

# --- General parameters ---
DEFAULT_PARTITIONS = int(os.getenv("DEFAULT_PARTITIONS", "3"))
DEFAULT_RF = int(os.getenv("DEFAULT_RF", "1"))

# Base retention for append-only (7 days)
APPEND_RETENTION_MS = int(os.getenv("APPEND_RETENTION_MS", str(7 * 24 * 60 * 60 * 1000)))
# Base retention for compacted + delete (only older segments)
COMPACT_DELETE_RETENTION_MS = int(os.getenv("COMPACT_DELETE_RETENTION_MS", str(24 * 60 * 60 * 1000)))

# --- Topic groups ---

# Append-only (event streams)
APPEND_ONLY: Dict[str, Dict[str, str]] = {
    "shelf_events": {},
    "pos_transactions": {},
    "foot_traffic": {},
    "wh_events": {},
    "alerts": {},
}

# Compacted (last value per key) – STATE + RULES/METADATA
COMPACTED: Dict[str, Dict[str, str]] = {
    # STATE topics
    "shelf_state": {},
    "wh_state": {},
    "shelf_batch_state": {},
    "wh_batch_state": {},
    "daily_discounts": {},         # Discount Manager output (last per key)
    "shelf_restock_plan": {},         # planner: latest plan per shelf_id
    "wh_supplier_plan": {},         # planner: latest plan per batch_id::expiry_date

    # Metadata / rules topics
    "shelf_policies": {},
    "wh_policies": {},
    "batch_catalog": {},
    "shelf_profiles": {},
}

# Base config shared by both profiles
BASE_APPEND_CFG = {
    "cleanup.policy": "delete",
    "retention.ms": str(APPEND_RETENTION_MS),
    "segment.bytes": str(256 * 1024 * 1024),  # 256MB
}

BASE_COMPACTED_CFG = {
    "cleanup.policy": "compact",
    "min.cleanable.dirty.ratio": "0.01",
    "segment.ms": str(60 * 60 * 1000),                # 1h
    "delete.retention.ms": str(COMPACT_DELETE_RETENTION_MS),
    "segment.bytes": str(256 * 1024 * 1024),
}

def build_admin() -> KafkaAdminClient:
    last = None
    for attempt in range(1, 11):
        try:
            return KafkaAdminClient(bootstrap_servers=BROKER, client_id="kafka-init")
        except (NoBrokersAvailable, Exception) as e:
            last = e
            print(f"[init] Broker non disponibile ({attempt}/10). Riprovo tra 3s… ({e})")
            time.sleep(3)
    raise RuntimeError(f"Kafka non raggiungibile: {last}")

def ensure_topics(admin: KafkaAdminClient, topics_cfg: Dict[str, Dict[str, str]],
                  partitions: int, rf: int, base_cfg: Dict[str, str]) -> None:
    existing = set(admin.list_topics())
    to_create: List[NewTopic] = []
    for name, overrides in topics_cfg.items():
        cfg = base_cfg.copy()
        cfg.update(overrides or {})
        if name not in existing:
            to_create.append(NewTopic(
                name=name,
                num_partitions=partitions,
                replication_factor=rf,
                topic_configs=cfg
            ))
        else:
            # try to align config when topic already exists
            try:
                res = ConfigResource(ConfigResourceType.TOPIC, name, configs=cfg)
                admin.alter_configs([res])
                print(f"[init] Aggiornata config topic esistente: {name}")
            except Exception as e:
                print(f"[init] Warning: impossibile aggiornare config per {name}: {e}")

    if to_create:
        try:
            admin.create_topics(to_create, validate_only=False, timeout_ms=15000)
            print(f"[init] Creati {len(to_create)} topic: {[t.topic for t in to_create]}")
        except TopicAlreadyExistsError:
            print("[init] Alcuni topic esistevano già (race condition), ok.")
        except Exception as e:
            print(f"[init] Errore nella creazione dei topic: {e}")

def main():
    admin = None
    try:
        admin = build_admin()
        print(f"[init] Connesso a {BROKER}")

        # Append-only
        ensure_topics(admin, APPEND_ONLY, DEFAULT_PARTITIONS, DEFAULT_RF, BASE_APPEND_CFG)
        # Compacted
        ensure_topics(admin, COMPACTED, DEFAULT_PARTITIONS, DEFAULT_RF, BASE_COMPACTED_CFG)

        print("[init] Done.")
    finally:
        if admin is not None:
            try:
                admin.close()
            except Exception:
                pass

if __name__ == "__main__":
    main()
