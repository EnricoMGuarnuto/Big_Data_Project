# Smart Shelf — Big Data Project

![Logo](images/logo.png)

Sistema end‑to‑end (simulato) per **monitorare stock e lotti** di un supermercato, generare **eventi in tempo reale** (foot traffic, interazioni con gli scaffali, transazioni POS) e applicare **logiche di alerting e replenishment** tramite **Kafka + Spark Structured Streaming**, con persistenza su **Delta Lake** e **PostgreSQL**.

> Questo repository è pensato per essere eseguito principalmente via `docker compose`.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
  - [Data Pipeline](#data-pipeline)
  - [Data Flow](#data-flow)
- [Tech Stack](#tech-stack)
- [Repository Structure](#repository-structure)
- [Data & Topics](#data--topics)
- [Simulated Time](#simulated-time)
- [How to Run](#how-to-run)
- [Spark Apps](#spark-apps)
- [Kafka Connect](#kafka-connect)
- [PostgreSQL Schemas](#postgresql-schemas)
- [Streamlit Dashboard](#streamlit-dashboard)
- [ML / Demand Forecasting](#ml--demand-forecasting)
- [Performance](#performance)
- [Troubleshooting](#troubleshooting)

---

## Project Overview

L’idea è simulare un sistema “Smart Shelf” dove:

- **Sensori / eventi** (simulati) producono stream di dati: ingressi/uscite clienti, pickup/putback/weight change sugli scaffali, transazioni POS, movimenti warehouse.
- **Kafka** è lo streaming bus, con topic **append‑only** (eventi) e **compacted** (stati/metadati).
- **Spark Structured Streaming** costruisce e mantiene gli **stati** (es. `shelf_state`, `wh_state`, batch states), genera **alert** e **piani di restock**.
- **Delta Lake (filesystem)** è il data lake (raw/cleansed/ops/analytics).
- **PostgreSQL** mantiene snapshot di riferimento, config/policy e materializzazioni utili per query/serving.
- **Kafka Connect (JDBC)** sincronizza Postgres ↔ Kafka per metadata e state tables.
- **Simulated time** (Redis) fornisce un clock condiviso per demo deterministiche.
- **Streamlit dashboard** visualizza mappa store, alert, stati, piani supplier e output ML.
- **ML services** (training/inference) generano previsioni di replenishment e le pubblicano su Postgres/Delta.

---

## Architecture

### Data Pipeline

![Data Pipeline](images/data_pipeline.png)

### Data Flow

![Data Flow](images/data_flow.png)

> Nota: i file `data_pipeline.png` e `data_flow.png` sono referenziati direttamente dal README; sostituiscili con le immagini finali mantenendo gli stessi nomi.

---

## Tech Stack

| Technology | Role/Purpose |
|---|---|
| Python | Main language (dataset generation, Kafka producers, services/apps) |
| Docker / Docker Compose | Containerization and orchestration of the whole stack |
| Apache Kafka | Message broker for real-time streaming across components |
| Kafka Connect (JDBC) | PostgreSQL ↔ Kafka synchronization (metadata sources + state/ops sinks) |
| Redis Streams | “Before Kafka” buffer for producers (resilience/backpressure) |
| Apache Spark 3.5 (Structured Streaming) | Streaming processing, aggregations, alerting, and flow orchestration |
| Delta Lake | Filesystem-based data lake (`./delta`) for raw/cleansed/ops/analytics layers + checkpoints |
| PostgreSQL 16 | Database for snapshots (`ref`), config/policy (`config`), state (`state`), ops (`ops`) and analytics |

### Technologies and Justification

#### Core Technologies

- **Python — Main programming language**
  -> Chosen for its flexibility and rich data ecosystem; used across the project for data generation, Kafka producers, Spark job logic, ML services, and the Streamlit dashboard.
- **Docker / Docker Compose — Containerization and orchestration**
  -> Chosen to ensure reproducible environments, isolate dependencies (Kafka, Postgres, Spark, etc.), and start/stop the entire stack with a single `docker compose` command.

#### Real-Time Streaming & Ingestion

- **Apache Kafka — Distributed event streaming platform**
  -> Chosen as the central event bus to decouple producers and consumers, support high-throughput ingestion, and model both append-only event streams and compacted state/metadata topics.
- **Apache ZooKeeper — Kafka coordination (single-broker setup)**
  -> Chosen because the Kafka distribution used in this project relies on ZooKeeper for broker metadata/coordination in a lightweight, single-node setup.
- **Kafka Connect + JDBC Connector — Data integration layer**
  -> Chosen to move data between PostgreSQL and Kafka without writing custom ingestion services: sourcing configuration/reference tables into Kafka and sinking compacted state tables back to Postgres for serving/querying.
- **Redis Streams — In-memory buffer before Kafka**
  -> Chosen to absorb bursts and provide a simple backpressure layer for the simulated producers, reducing coupling to Kafka availability and smoothing event publication.
- **kafka-python — Kafka client for Python**
  -> Chosen as a lightweight, widely-used client library to publish/consume Kafka messages from Python components (producers and some utility jobs) with minimal overhead.
- **redis-py — Redis client for Python**
  -> Chosen to interact with Redis Streams from Python components (simulated clock + producers) in a simple, reliable way.

#### Stream Processing & Lakehouse Storage

- **Apache Spark (Structured Streaming) — Stateful stream processing**
  -> Chosen for scalable micro-batch streaming, stateful aggregations, windowing, and fault-tolerance via checkpoints; used to build shelf/warehouse states, generate alerts, and compute replenishment/supplier plans.
- **Delta Lake (`delta-spark` + `deltalake`) — ACID tables on files**
  -> Chosen to persist raw/cleansed/ops/analytics layers with ACID guarantees, schema evolution support, and reliable streaming sinks/checkpoints on a local filesystem (`./delta`).
- **Apache Arrow / Parquet (`pyarrow`) — Columnar storage and I/O**
  -> Chosen for fast, efficient Parquet read/write for bootstrap datasets and interoperability across Spark, Delta, and Python analytics.

#### Databases & Data Access

- **PostgreSQL 16 — Relational database**
  -> Chosen for reliability and strong SQL support; used for reference snapshots, configuration/policies, materialized state mirrors, ops tables, and analytics that are easy to query/serve.
- **psycopg2 + SQLAlchemy — Postgres connectivity**
  -> Chosen to provide robust database access from Python services and the dashboard (connections, queries, inserts/updates) with mature tooling and good integration.

#### Visualization

- **Streamlit — Interactive dashboard**
  -> Chosen to quickly build a data-driven UI to inspect pipeline outputs (states/alerts/analytics) without the overhead of a separate frontend stack.
- **Plotly — Interactive charts**
  -> Chosen for rich, interactive plots embedded in Streamlit to explore metrics and trends.

#### Machine Learning

- **scikit-learn — Baseline ML + preprocessing**
  -> Chosen for standardized preprocessing utilities and simple, dependable baseline models used in the demand-forecasting component.
- **XGBoost — Gradient-boosted trees**
  -> Chosen for strong performance on tabular data and efficient training/inference for demand forecasting.
- **joblib — Model persistence**
  -> Chosen to serialize trained models and reload them reliably in inference/retraining workflows.
- **pandas + NumPy — Data wrangling**
  -> Chosen for feature engineering, dataset manipulation, and “glue” logic throughout generators, services, and analytics tasks.

#### Supporting Libraries

- **PyYAML — Configuration parsing**
  -> Chosen to keep simple, readable configuration files for the dashboard and utilities without hardcoding parameters.
- **Pillow — Image handling**
  -> Chosen for lightweight image loading/manipulation in the Streamlit dashboard (assets, logos, and plots exports if needed).
- **streamlit-autorefresh / streamlit-plotly-events — Dashboard UX helpers**
  -> Chosen to improve interactivity (auto-refresh, click/selection events) when exploring real-time-ish outputs.

---

## Repository Structure

```text
.
├── docker-compose.yml                 # Stack completa (Kafka/Redis/Postgres/Kafka Connect/Spark apps/ML/dashboard)
├── conf/                              # Spark config (spark-defaults.conf)
├── data/                              # Dataset sintetici + generator + CSV per bootstrap Postgres
│   ├── create_db.py
│   └── db_csv/                        # CSV importati da Postgres in init
├── delta/                             # Delta Lake locale (raw/cleansed/ops/analytics + checkpoint)
├── dashboard/                         # Streamlit dashboard (mappa, alert, stati, ML)
│   ├── app.py
│   ├── store_layout.yaml
│   └── assets/
├── images/                            # Logo + diagrammi + screenshot dashboard
├── kafka-components/                  # Componenti Kafka (topic, connect, producer)
│   ├── kafka-init/                    # Crea topic (append-only + compacted)
│   ├── kafka-connect/                 # Kafka Connect + init connector JDBC
│   ├── kafka-producer-foot_traffic/   # Producer foot traffic (session + realistico)
│   ├── kafka-producer-shelf/          # Producer shelf events (pickup/putback/weight_change)
│   └── kafka-producer-pos/            # Producer POS transactions (da foot traffic + shelf events)
├── ml-model/                          # ML services (training + inference)
│   ├── training-service/              # Train XGBoost + registry su Postgres
│   └── inference-service/             # Inference giornaliera + wh_supplier_plan
├── models/                            # Artifact ML (modelli + feature columns)
├── postgresql/                        # DDL + init SQL (ref/config/state/ops/analytics + views)
│   ├── 01_make_db.sql
│   ├── 02_load_ref_from_csv.sql
│   ├── 03_load_config.sql
│   └── 05_views.sql
├── simulated_time/                    # Simulated clock condiviso (Redis)
├── spark-apps/                        # Spark Structured Streaming jobs
│   ├── deltalake/                     # Bootstrap tabelle Delta “vuote”
│   ├── foot-traffic-raw-sink/         # Kafka foot_traffic -> Delta raw
│   ├── shelf-aggregator/              # shelf_events -> raw + shelf_state (Delta + Kafka compacted)
│   ├── batch-state-updater/           # pos_transactions -> shelf_batch_state (Delta + Kafka compacted)
│   ├── shelf-alert-engine/            # shelf_state -> alerts + shelf_restock_plan
│   ├── shelf-restock-manager/         # shelf_restock_plan -> wh_events (FIFO picking)
│   ├── shelf-refill-bridge/           # wh_events (wh_out) -> shelf_events (delay)
│   ├── wh-aggregator/                 # wh_events -> wh_state
│   ├── wh-batch-state-updater/        # wh_events -> wh_batch_state (+ mirror store batches)
│   ├── wh-alert-engine/               # wh_state -> alerts + wh_supplier_plan
│   ├── wh-supplier-manager/           # Ordini/receipts supplier + wh_in events
│   ├── daily-discount-manager/        # near-expiry -> daily_discounts
│   ├── removal-scheduler/             # Rimozione lotti scaduti + alerts opzionali
│   ├── shelf-daily-features/          # Feature giornaliere per ML
│   └── alerts-sink/                   # alerts -> Delta (+ opzionale Postgres)
```

---

## Data & Topics

### Dataset (bootstrap)

I file principali (generati) sono in `data/`:

- `data/store_inventory_final.parquet`
- `data/warehouse_inventory_final.parquet`
- `data/store_batches.parquet`
- `data/warehouse_batches.parquet`
- `data/all_discounts.parquet`

Per bootstrap del DB vengono creati anche i CSV in `data/db_csv/` (utilizzati dagli script in `postgresql/`).

Per rigenerare tutto:

```bash
python3 data/create_db.py
```

### Kafka topics

Creati da `kafka-components/kafka-init/init.py`.

- **Append‑only (event streams)**: `shelf_events`, `pos_transactions`, `foot_traffic`, `wh_events`, `alerts`
- **Compacted (state/metadata)**: `shelf_state`, `wh_state`, `shelf_batch_state`, `wh_batch_state`, `daily_discounts`, `shelf_restock_plan`, `wh_supplier_plan`, `shelf_policies`, `wh_policies`, `batch_catalog`, `shelf_profiles`

### Delta Lake layout (paths principali)

- `delta/raw/`: `foot_traffic`, `shelf_events`, `wh_events`
- `delta/cleansed/`: `shelf_state`, `shelf_batch_state`, `wh_state`, `wh_batch_state`
- `delta/ops/`: `alerts`, `shelf_restock_plan`, `wh_supplier_plan`, `wh_supplier_orders`, `wh_supplier_receipts`
- `delta/analytics/`: `daily_discounts`

---

## Simulated Time

Il progetto include un **clock simulato** condiviso che scrive su Redis:

- chiavi: `sim:now` (timestamp) e `sim:today` (data).
- usato da producer e job Spark per mantenere una timeline coerente durante demo/test.

Configurazione (env vars principali):

- `USE_SIMULATED_TIME` (default `0`, usa `1` per attivare il clock)
- `TIME_MULTIPLIER` (default `1.0`)
- `STEP_SECONDS` (default `1`)
- `SIM_DAYS` (default `365`, da `SIM_START` definito in `simulated_time/config.py`)

Avvio:

```bash
docker compose up -d redis sim-clock
```

---

## How to Run

### Prerequisiti

- Docker + Docker Compose
- Connessione Internet durante la build (alcune immagini scaricano dipendenze/JAR)

### Avvio

```bash
docker compose up -d --build
```

Servizi principali esposti:

- Postgres: `localhost:5432`
- Kafka Connect: `localhost:8083`

Credenziali Postgres (default da `docker-compose.yml`):

- DB: `smart_shelf`
- User: `bdt_user`
- Password: `bdt_password`

Per verificare lo stato:

```bash
docker compose ps
docker compose logs -f kafka-init connect-init spark-init-delta
```

### Dashboard & ML (opzionali)

- Dashboard Streamlit: `http://localhost:8501` (servizio `dashboard`).
- Pipeline ML: `shelf-daily-features` → `training-service` → `inference-service`.

Esempi (dopo aver avviato l'infrastruttura base):

```bash
docker compose up -d dashboard
docker compose up -d sim-clock shelf-daily-features training-service inference-service
```

### Stop

```bash
docker compose down
```

---

## Spark Apps

I job Spark sono in `spark-apps/` e girano come container dedicati (base `apache/spark:3.5.1-python3`).

Principali componenti (vedi i README dentro ogni cartella):

- `spark-apps/deltalake/`: inizializza le tabelle Delta “vuote” (raw/cleansed/ops/analytics).
- `spark-apps/foot-traffic-raw-sink/`: Kafka `foot_traffic` → Delta RAW.
- `spark-apps/shelf-aggregator/`: Kafka `shelf_events` (+ `shelf_profiles`) → Delta RAW + Delta `shelf_state` + topic compatto `shelf_state`.
- `spark-apps/batch-state-updater/`: Kafka `pos_transactions` → Delta RAW + Delta `shelf_batch_state` + topic compatto.
- `spark-apps/wh-aggregator/`: `wh_events` → Delta `wh_state` + topic compatto.
- `spark-apps/wh-batch-state-updater/`: `wh_events` → Delta `wh_batch_state` (+ mirror store batches) + topic compatti.
- `spark-apps/shelf-alert-engine/`: `shelf_state` (+ policy + batch mirror) → `alerts` + `shelf_restock_plan` (Kafka + Delta).
- `spark-apps/wh-alert-engine/`: `wh_state` (+ policy + batch mirror) → `alerts` + `wh_supplier_plan` (Kafka + Delta).
- `spark-apps/wh-supplier-manager/`: `wh_supplier_plan` → ordini/receipts + `wh_events` (wh_in).
- `spark-apps/shelf-restock-manager/`: `shelf_restock_plan` → `wh_events` (picking FIFO).
- `spark-apps/shelf-refill-bridge/`: `wh_events` (wh_out) → `shelf_events` (putback/weight_change) con delay.
- `spark-apps/daily-discount-manager/`: trova lotti in scadenza e pubblica `daily_discounts`.
- `spark-apps/removal-scheduler/`: rimuove lotti scaduti dallo shelf e aggiorna gli stati.
- `spark-apps/shelf-daily-features/`: calcola le feature giornaliere per il modello ML.
- `spark-apps/alerts-sink/`: `alerts` → Delta (+ opzionale Postgres).

Nota: per i consumer Structured Streaming puoi impostare `MAX_OFFSETS_PER_TRIGGER` per evitare micro-batch troppo grandi al primo avvio con `STARTING_OFFSETS=earliest`.

---

## Kafka Connect

I connector vengono registrati da `connect-init` (`kafka-components/kafka-connect/connect-init/init.py`) e il container poi termina.

- **Postgres → Kafka (source, compacted metadata)**: `batch_catalog`, `shelf_profiles`
- **Kafka → Postgres (sink)**:
  - upsert: `shelf_state`, `wh_state`, `shelf_batch_state`, `wh_batch_state`, `daily_discounts`, `shelf_restock_plan`, `wh_supplier_plan`
  - insert append‑only: `wh_events`

Nota: per evitare doppioni, di default non viene creato il sink `alerts` (lo fa già `alerts-sink` con `WRITE_TO_PG=1`) e non viene creato il sink `pos_transactions` (eventi annidati non mappabili direttamente su `ops.pos_transactions`/`ops.pos_transaction_items` con JDBC sink).

---

## PostgreSQL Schemas

Inizializzazione in `postgresql/01_make_db.sql` con schemi:

- `ref`: snapshot importati da CSV (inventory e batches).
- `config`: policy e metadata (es. `shelf_policies`, `wh_policies`).
- `state`: materializzazioni degli stati (mirror dei topic compacted).
- `ops`: eventi operativi, alert, piani.
- `analytics`: tabelle analitiche (es. `daily_discounts`).

I CSV vengono caricati da `postgresql/02_load_ref_from_csv.sql` e le policy di default vengono popolate da `postgresql/03_load_config.sql`.

Tabelle/viste analytics rilevanti:

- `analytics.shelf_daily_features`: feature engineering giornaliero (input ML).
- `analytics.v_ml_features`, `analytics.v_ml_train`: viste per inference/training.
- `analytics.ml_models`, `analytics.ml_predictions_log`: registry modelli e log predizioni.

---

## Streamlit Dashboard

Dashboard Streamlit in `dashboard/app.py` che visualizza:

- mappa store con overlay alert (rosso) e weight_change recenti (verde).
- stati shelf/warehouse con filtri per categoria, sottocategoria e shelf.
- piani supplier (Delta o Postgres) + prossime consegne.
- registry ML + predizioni (grafici e tabelle).
- tab alert raw e debug config.

### Data sources

- Inventory CSV: `data/db_csv/store_inventory_final.csv`
- Layout + immagine: `dashboard/store_layout.yaml`, `dashboard/assets/supermarket_map.png`
- Postgres (read-only): `ops.alerts`, `state.shelf_state`, `state.wh_state`, `state.shelf_batch_state`, `state.wh_batch_state`, `ops.wh_supplier_plan`, `analytics.ml_models`, `analytics.ml_predictions_log`
- Delta (opzionale): `delta/raw/shelf_events`, `delta/ops/wh_supplier_plan`, `delta/ops/wh_supplier_orders`

### Config (env vars)

- Layout & paths: `DASH_LAYOUT_YAML`/`LAYOUT_YAML`, `INVENTORY_CSV_PATH`/`INVENTORY_CSV`, `DELTA_SHELF_EVENTS_PATH`/`DELTA_EVENTS_PATH`, `DELTA_SUPPLIER_PLAN_PATH`, `DELTA_SUPPLIER_ORDERS_PATH`
- Postgres: `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `ALERTS_TABLE`, `SUPPLIER_PLAN_TABLE`, `STATE_SCHEMA`
- Refresh & eventi: `DASH_AUTOREFRESH_MS`/`DASH_REFRESH_MS`, `WEIGHT_EVENT_LOOKBACK_SEC`, `WEIGHT_EVENT_TYPE`

### Run

- Docker: `docker compose up -d dashboard` (porta `8501`)
- Local: `pip install -r dashboard/requirements.txt` + `streamlit run dashboard/app.py`

### Dashboard Screenshots (placeholder)

![Dashboard - Map Live](images/dashboard_map_live.png)

![Dashboard - Supplier Plan](images/dashboard_supplier_plan.png)

![Dashboard - ML](images/dashboard_ml.png)

![Dashboard - Alerts](images/dashboard_alerts.png)

> Sostituisci questi file con gli screenshot reali (o aggiorna i link).

---

## ML / Demand Forecasting

La pipeline ML è composta da:

1. **Feature engineering giornaliero** – job `spark-apps/shelf-daily-features` che popola `analytics.shelf_daily_features`.
2. **Views ML** – `analytics.v_ml_features` e `analytics.v_ml_train` (definite in `postgresql/05_views.sql`).
3. **Training** – servizio `ml-model/training-service` (XGBoost + time-series CV), con output in `models/` e registry in `analytics.ml_models`.
4. **Inference** – servizio `ml-model/inference-service` che genera piani giornalieri in `ops.wh_supplier_plan` e log in `analytics.ml_predictions_log` (opzionale mirror Delta).

### Feature set (alto livello)

- anagrafiche: categoria/sottocategoria.
- calendario: `day_of_week`, `is_weekend`, `refill_day`.
- pricing/discount: prezzo, sconto, flag sconti correnti/futuri.
- domanda: `people_count`, vendite ultime 1/7/14 giornate.
- stock shelf: capacita, fill ratio, soglia, scaduti, alert recenti.
- stock warehouse: capacita, fill ratio, reorder point, pending supplier, scaduti, alert recenti.
- scadenze e batch: `standard_batch_size`, `min/avg_expiration_days`, `qty_expiring_next_7d`.
- target training: `batches_to_order` (solo training).

### Output principali

- `analytics.shelf_daily_features`
- `analytics.ml_models`
- `analytics.ml_predictions_log`
- `ops.wh_supplier_plan` (+ `delta/ops/wh_supplier_plan`)

### Config (env vars)

- Training: `PG_DSN`, `MODEL_NAME`, `ARTIFACT_DIR`, `FEATURE_SQL`
- Inference: `PG_DSN`, `MODEL_NAME`, `RUN_HOUR`, `RUN_MINUTE`, `DELTA_ENABLED`, `DELTA_WHSUPPLIER_PLAN_PATH`, `FEATURE_COLUMNS_PATH`, `ALLOW_DISK_MODEL`, `HEARTBEAT_SECONDS`

### Run

```bash
docker compose up -d sim-clock shelf-daily-features training-service inference-service
```

Note:
- `training-service` fa una singola run e poi termina.
- `inference-service` gira una volta al giorno (simulato) in base a `RUN_HOUR`/`RUN_MINUTE`.

---

## Performance

### Avviare solo ciò che serve

Il modo più efficace per ridurre CPU/RAM è non lanciare tutta la pipeline insieme. Esempi:

```bash
# solo infrastruttura
docker compose up -d --build zookeeper kafka redis postgres

# aggiungi producer e un sottoinsieme di job Spark
docker compose up -d --build kafka-producer-shelf spark-shelf-aggregator
```

### Ridurre carico dei producer e dei consumer

- Producer: aumenta `SLEEP` o diminuisci `TIME_SCALE` nei servizi `kafka-producer-*`.
- Spark streaming: usa `MAX_OFFSETS_PER_TRIGGER` (già previsto in vari servizi) per evitare micro-batch enormi a `STARTING_OFFSETS=earliest`.

---

## Troubleshooting

- Se vuoi rilanciare solo l’inizializzazione dei connector: `docker compose run --rm connect-init`.
- Se `kafka-init` o `connect-init` falliscono, controllare i log con `docker compose logs -f kafka-init connect-init`.
- Se Spark non parte durante la build, verificare l’accesso a Internet (download JAR/`--packages`).
- Se Postgres non carica i CSV, verificare che `data/db_csv/*.csv` esistano e che il volume `./data/db_csv:/import/csv/db:ro` sia montato correttamente.
