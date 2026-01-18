# Smart Shelf — Big Data Project

![Logo](images/logo.png)

End-to-end (simulated) system to **monitor stock and batches** for a supermarket, generate **real-time events** (foot traffic, shelf interactions, POS transactions), and apply **alerting and replenishment logic** via **Kafka + Spark Structured Streaming**, with persistence on **Delta Lake** and **PostgreSQL**.

> This repository is meant to be run primarily via `docker compose`.

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

The idea is to simulate a "Smart Shelf" system where:

- **Sensors / events** (simulated) produce data streams: customer entries/exits, pickup/putback/weight change on shelves, POS transactions, warehouse movements.
- **Kafka** is the streaming bus, with **append-only** topics (events) and **compacted** topics (state/metadata).
- **Spark Structured Streaming** builds and maintains **state** (e.g. `shelf_state`, `wh_state`, batch states), generates **alerts** and **restock plans**.
- **Delta Lake (filesystem)** is the data lake (raw/cleansed/ops/analytics).
- **PostgreSQL** stores reference snapshots, config/policy, and materializations useful for queries/serving.
- **Kafka Connect (JDBC)** synchronizes Postgres ↔ Kafka for metadata and state tables.
- **Simulated time** (Redis) provides a shared clock for deterministic demos.
- **Streamlit dashboard** displays the store map, alerts, states, supplier plans, and ML outputs.
- **ML services** (training/inference) generate replenishment forecasts and publish them to Postgres/Delta.

Note on workload: the system is computationally intensive primarily because it relies on end-to-end simulation of data that, in a production setting, would be obtained from retailer APIs (live operational streams) or from pre-existing, non-simulated datasets captured from real stores. Since such sources were not available for this project, we generated synthetic event and inventory/batch data to drive and validate the full pipeline under realistic operating assumptions.

---

## Architecture

### Data Pipeline

![Data Pipeline](images/data_pipeline.png)

### Data Flow

![Data Flow](images/data_flow.png)

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
├── docker-compose.yml                 # Full stack (Kafka/Redis/Postgres/Kafka Connect/Spark apps/ML dashboard)
├── conf/                              # Spark config (spark-defaults.conf)
├── data/                              # Synthetic datasets + generator + CSVs for Postgres bootstrap
│   ├── create_db.py
│   └── db_csv/                        # CSVs imported by Postgres during init
├── delta/                             # Local Delta Lake (raw/cleansed/ops/analytics + checkpoint)
├── dashboard/                         # Streamlit dashboard (map, alerts, state, ML)
│   ├── app.py
│   ├── store_layout.yaml
│   └── assets/
├── images/                            # Logo + diagrams + dashboard screenshots
├── kafka-components/                  # Kafka components (topics, connect, producers)
│   ├── kafka-init/                    # Create topics (append-only + compacted)
│   ├── kafka-connect/                 # Kafka Connect + JDBC connector init
│   ├── kafka-producer-foot_traffic/   # Foot traffic producer (session + realistic)
│   ├── kafka-producer-shelf/          # Producer shelf events (pickup/putback/weight_change)
│   └── kafka-producer-pos/            # POS transactions producer (from foot traffic + shelf events)
├── ml-model/                          # ML services (training + inference)
│   ├── training-service/              # Train XGBoost, register in Postgres, write artifacts to /models
│   └── inference-service/             # Cutoff-time inference (Sun/Tue/Thu), write pending wh_supplier_plan, log predictions
├── models/                            # Model artifacts + feature columns (mounted to /models)
├── postgresql/                        # DDL + init SQL (ref/config/state/ops/analytics + views)
│   ├── 01_make_db.sql
│   ├── 02_load_ref_from_csv.sql
│   ├── 03_load_config.sql
│   └── 05_views.sql
├── simulated_time/                    # Shared simulated clock (Redis)
├── spark-apps/                        # Spark Structured Streaming jobs
│   ├── deltalake/                     # Bootstrap empty Delta tables
│   ├── foot-traffic-raw-sink/         # Kafka foot_traffic -> Delta raw
│   ├── shelf-aggregator/              # shelf_events -> raw + shelf_state (Delta + Kafka compacted)
│   ├── batch-state-updater/           # pos_transactions -> shelf_batch_state (Delta + Kafka compacted)
│   ├── shelf-alert-engine/            # shelf_state -> alerts + shelf_restock_plan
│   ├── shelf-restock-manager/         # shelf_restock_plan -> wh_events (FIFO picking)
│   ├── shelf-refill-bridge/           # wh_events (wh_out) -> shelf_events (delay)
│   ├── wh-aggregator/                 # wh_events -> wh_state
│   ├── wh-batch-state-updater/        # wh_events -> wh_batch_state (+ mirror store batches)
│   ├── wh-alert-engine/               # wh_state -> alerts + wh_supplier_plan
│   ├── wh-supplier-manager/           # Supplier orders/receipts + wh_in events
│   ├── daily-discount-manager/        # near-expiry -> daily_discounts
│   ├── removal-scheduler/             # Expired batch removal + optional alerts
│   ├── shelf-daily-features/          # Daily features for ML
│   └── alerts-sink/                   # alerts -> Delta (+ optional Postgres)
```

---

## Data & Topics

### Dataset (bootstrap)

The main (generated) files are in `data/`:

- `data/store_inventory_final.parquet`
- `data/warehouse_inventory_final.parquet`
- `data/store_batches.parquet`
- `data/warehouse_batches.parquet`
- `data/all_discounts.parquet`

For DB bootstrap, CSVs are also created in `data/db_csv/` (used by scripts in `postgresql/`).

To regenerate everything:

```bash
python3 data/create_db.py
```

### Kafka topics

Created by `kafka-components/kafka-init/init.py`.

- **Append‑only (event streams)**: `shelf_events`, `pos_transactions`, `foot_traffic`, `wh_events`, `alerts`
- **Compacted (state/metadata)**: `shelf_state`, `wh_state`, `shelf_batch_state`, `wh_batch_state`, `daily_discounts`, `shelf_restock_plan`, `wh_supplier_plan`, `shelf_policies`, `wh_policies`, `batch_catalog`, `shelf_profiles`

### Delta Lake layout (main paths)

- `delta/raw/`: `foot_traffic`, `shelf_events`, `wh_events`
- `delta/cleansed/`: `shelf_state`, `shelf_batch_state`, `wh_state`, `wh_batch_state`
- `delta/ops/`: `alerts`, `shelf_restock_plan`, `wh_supplier_plan`, `wh_supplier_orders`, `wh_supplier_receipts`
- `delta/analytics/`: `daily_discounts`

---

## Simulated Time

The project includes a shared **simulated clock** that writes to Redis:

- keys: `sim:now` (timestamp) and `sim:today` (date).
- used by producers and Spark jobs to keep a consistent timeline during demos/tests.

Configuration (main env vars):

- `USE_SIMULATED_TIME` (default `0`, use `1` to enable the clock)
- `TIME_MULTIPLIER` (default `1.0`)
- `STEP_SECONDS` (default `1`)
- `SIM_DAYS` (default `365`, from `SIM_START` defined in `simulated_time/config.py`)

Start:

```bash
docker compose up -d redis sim-clock
```

---

## How to Run

### Prerequisites

- Docker + Docker Compose
- Internet connection during build (some images download dependencies/JARs)

### Start

```bash
docker compose up -d --build
```

Main services exposed:

- Postgres: `localhost:5432`
- Kafka Connect: `localhost:8083`

Postgres credentials (defaults from `docker-compose.yml`):

- DB: `smart_shelf`
- User: `bdt_user`
- Password: `bdt_password`

To check status:

```bash
docker compose ps
docker compose logs -f kafka-init connect-init spark-init-delta
```

### Dashboard & ML (optional)

- Streamlit dashboard: `http://localhost:8501` (service `dashboard`).
- ML pipeline: `shelf-daily-features` → `training-service` → `inference-service`.

Examples (after starting the base infrastructure):

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

Spark jobs live in `spark-apps/` and run as dedicated containers (base `apache/spark:3.5.1-python3`).

Main components (see the README in each folder):

- `spark-apps/deltalake/`: initialize empty Delta tables (raw/cleansed/ops/analytics).
- `spark-apps/foot-traffic-raw-sink/`: Kafka `foot_traffic` → Delta raw.
- `spark-apps/shelf-aggregator/`: Kafka `shelf_events` (+ `shelf_profiles`) → Delta raw + Delta `shelf_state` + compacted `shelf_state` topic.
- `spark-apps/batch-state-updater/`: Kafka `pos_transactions` → Delta raw + Delta `shelf_batch_state` + compacted topic.
- `spark-apps/wh-aggregator/`: `wh_events` → Delta `wh_state` + compacted topic.
- `spark-apps/wh-batch-state-updater/`: `wh_events` → Delta `wh_batch_state` (+ mirror store batches) + compacted topics.
- `spark-apps/shelf-alert-engine/`: `shelf_state` (+ policy + batch mirror) → `alerts` + `shelf_restock_plan` (Kafka + Delta).
- `spark-apps/wh-alert-engine/`: `wh_state` (+ policy + batch mirror) → `alerts` + `wh_supplier_plan` (Kafka + Delta).
- `spark-apps/wh-supplier-manager/`: `wh_supplier_plan` → orders/receipts + `wh_events` (wh_in).
- `spark-apps/shelf-restock-manager/`: `shelf_restock_plan` → `wh_events` (FIFO picking).
- `spark-apps/shelf-refill-bridge/`: `wh_events` (wh_out) → `shelf_events` (putback/weight_change) with delay.
- `spark-apps/daily-discount-manager/`: find batches near expiry and publish `daily_discounts`.
- `spark-apps/removal-scheduler/`: remove expired batches from the shelf and update state.
- `spark-apps/shelf-daily-features/`: compute daily features for the ML model.
- `spark-apps/alerts-sink/`: `alerts` → Delta (+ optional Postgres).

Note: for Structured Streaming consumers you can set `MAX_OFFSETS_PER_TRIGGER` to avoid huge micro-batches on the first run with `STARTING_OFFSETS=earliest`.

---

## Kafka Connect

Connectors are registered by `connect-init` (`kafka-components/kafka-connect/connect-init/init.py`), and the container then exits.

- **Postgres → Kafka (source, compacted metadata)**: `batch_catalog`, `shelf_profiles`
- **Kafka → Postgres (sink)**:
  - upsert: `shelf_state`, `wh_state`, `shelf_batch_state`, `wh_batch_state`, `daily_discounts`, `shelf_restock_plan`, `wh_supplier_plan`
  - insert append‑only: `wh_events`

Note: to avoid duplicates, by default the `alerts` sink is not created (it is already handled by `alerts-sink` with `WRITE_TO_PG=1`), and the `pos_transactions` sink is not created (nested events not directly mappable to `ops.pos_transactions`/`ops.pos_transaction_items` with the JDBC sink).

---

## PostgreSQL Schemas

Initialization in `postgresql/01_make_db.sql` with schemas:

- `ref`: snapshots imported from CSVs (inventory and batches).
- `config`: policies and metadata (e.g. `shelf_policies`, `wh_policies`).
- `state`: state materializations (mirrors of compacted topics).
- `ops`: operational events, alerts, plans.
- `analytics`: analytics tables (e.g. `daily_discounts`).

CSVs are loaded by `postgresql/02_load_ref_from_csv.sql`, and default policies are populated by `postgresql/03_load_config.sql`.

Relevant analytics tables/views:

- `analytics.shelf_daily_features`: daily feature engineering (ML inputs).
- `analytics.v_ml_features`, `analytics.v_ml_train`: views for inference/training.
- `analytics.ml_models`, `analytics.ml_predictions_log`: model registry and prediction logs.

---

## Streamlit Dashboard

Streamlit dashboard in `dashboard/app.py` that shows:

- interactive store map with clickable zones and alert overlay.
- shelf/warehouse state tables from Delta with filters by category, subcategory, and shelf.
- supplier plan view with next delivery date (Delta first, Postgres fallback).
- ML model registry and prediction explorer from Postgres.
- raw alerts table plus debug/config panel.
- uses simulated time when available; otherwise falls back to UTC.

### Data sources

- Inventory + layout: `data/db_csv/store_inventory_final.csv`, `dashboard/store_layout.yaml`, `dashboard/assets/supermarket_map.png`
- Delta: `delta/cleansed/shelf_state`, `delta/cleansed/wh_state`, `delta/cleansed/shelf_batch_state`, `delta/cleansed/wh_batch_state`, `delta/ops/wh_supplier_plan`, `delta/ops/wh_supplier_orders`
- Postgres (read-only): `ops.alerts`, `ops.wh_supplier_plan` (fallback), `analytics.ml_models`, `analytics.ml_predictions_log`

### Config (env vars)

- Layout & inventory: `DASH_LAYOUT_YAML`/`LAYOUT_YAML`/`LAYOUT_YAML_PATH`, `INVENTORY_CSV_PATH`/`INVENTORY_CSV`
- Delta paths: `DELTA_SUPPLIER_PLAN_PATH`, `DELTA_SUPPLIER_ORDERS_PATH`, `DL_SHELF_STATE_PATH`/`DELTA_SHELF_STATE_PATH`, `DL_WH_STATE_PATH`/`DELTA_WH_STATE_PATH`, `DL_SHELF_BATCH_PATH`/`DELTA_SHELF_BATCH_STATE_PATH`, `DL_WH_BATCH_PATH`/`DELTA_WH_BATCH_STATE_PATH`
- Postgres: `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`/`POSTGRES_DATABASE`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `ALERTS_TABLE`, `SUPPLIER_PLAN_TABLE`
- Refresh: `DASH_AUTOREFRESH_MS`/`DASH_REFRESH_MS`

### Run

- Docker: `docker compose up -d dashboard` (port `8501`)
- Local: `pip install -r dashboard/requirements.txt` + `streamlit run dashboard/app.py`

### Results (dashboard)

The dashboard consolidates real-time-ish operations (alerts, states, supplier plans) with ML outputs to show how the pipeline behaves end-to-end during a simulated run.

Screenshots (placeholders):

![Dashboard - Map Live](images/dashboard_map_live.png)

![Dashboard - Supplier Plan](images/dashboard_supplier_plan.png)

![Dashboard - ML](images/dashboard_ml.png)

![Dashboard - Alerts](images/dashboard_alerts.png)

Video (placeholder):
- `videos/dashboard_demo.mp4`

> Replace these files with real screenshots and videos (or update the links).

---

## ML / Demand Forecasting

The ML pipeline consists of:

1. **Daily feature engineering** – job `spark-apps/shelf-daily-features` that populates `analytics.shelf_daily_features`.
2. **ML views** – `analytics.v_ml_features` and `analytics.v_ml_train` (defined in `postgresql/05_views.sql`).
3. **Training** – `ml-model/training-service` service (XGBoost + time-series CV), writes model artifacts to `models/` and registers metadata in `analytics.ml_models`.
4. **Inference** – `ml-model/inference-service` service that loads the latest model, generates pending supplier plans around cutoff times (Sun/Tue/Thu 12:00 UTC), and logs predictions in `analytics.ml_predictions_log` (Delta mirror + optional Kafka publish).

### Feature set (high level)

- master data: category/subcategory.
- calendar: `day_of_week`, `is_weekend`, `refill_day`.
- pricing/discount: price, discount, current/future discount flags.
- demand: `people_count`, sales in the last 1/7/14 days.
- shelf stock: capacity, fill ratio, threshold, expired, recent alerts.
- warehouse stock: capacity, fill ratio, reorder point, pending supplier, expired, recent alerts.
- expirations and batches: `standard_batch_size`, `min/avg_expiration_days`, `qty_expiring_next_7d`.
- training target: `batches_to_order` (training only).

### Main outputs

- `analytics.shelf_daily_features`
- `analytics.ml_models`
- `analytics.ml_predictions_log`
- `ops.wh_supplier_plan` (+ `delta/ops/wh_supplier_plan`)
- `models/<MODEL_NAME>_<YYYYMMDD_HHMMSS>.json`
- `models/<MODEL_NAME>_feature_columns.json`

The `models/` directory is mounted to `/models` in Docker Compose for both training and inference.

### Config (env vars)

- Training: `PG_DSN`, `MODEL_NAME`, `ARTIFACT_DIR`, `FEATURE_SQL`
- Inference: `PG_DSN`, `MODEL_NAME`, `CUTOFF_DOWS`, `CUTOFF_HOUR`, `CUTOFF_MINUTE`, `PREDICT_LEAD_MINUTES`, `DELTA_ENABLED`, `DELTA_WHSUPPLIER_PLAN_PATH`, `FEATURE_COLUMNS_PATH`, `ALLOW_DISK_MODEL`, `HEARTBEAT_SECONDS`, `KAFKA_ENABLED`

### Run

```bash
docker compose up -d sim-clock shelf-daily-features training-service inference-service
```

Note:
- `training-service` runs once and then exits (may skip if `RETRAIN_DAYS` is set and the model is still fresh).
- `inference-service` stays idle and triggers only around the configured cutoff schedule (see `CUTOFF_*` env vars).

---

## Performance

### Resource footprint

This project runs multiple stateful services (Kafka, Postgres, Redis, Spark streaming jobs, optional Kafka Connect, ML services, and a Streamlit UI). Running the full stack is CPU- and RAM-intensive, and the first run can be particularly heavy when Spark jobs start from `STARTING_OFFSETS=earliest` (they may backfill large portions of the topics).

### Run a minimal subset

Prefer starting only what you need for the task you are validating:

```bash
# Base infrastructure (fastest minimal core)
docker compose up -d zookeeper kafka redis postgres

# One-shot initializers (run when you need to (re)create topics / Delta layout)
docker compose up -d kafka-init spark-init-delta

# Optional: Kafka Connect + connector registration
docker compose up -d kafka-connect
docker compose run --rm connect-init
```

Then add only the producers / Spark apps you want to observe (e.g., shelf events + aggregation, or alerts, or supplier planning).

### Tuning knobs (simulation and Spark)

- Producers:
  - `kafka-producer-foot-traffic`, `kafka-producer-pos`: tune `SLEEP` and `TIME_SCALE`.
  - `kafka-producer-shelf`: tune `SHELF_SLEEP`.
- Simulated clock: tune `TIME_MULTIPLIER` and `STEP_SECONDS` (see `.env` and `simulated_time/README.md`).
- Spark consumers:
  - Prefer `STARTING_OFFSETS=latest` for iterative development; use `earliest` only when you intentionally want a full replay.
  - Reduce `MAX_OFFSETS_PER_TRIGGER` if you see memory pressure or very large micro-batches on startup.

### Disk usage (Delta + checkpoints)

The `delta/` directory (tables + `_checkpoints/`) can grow quickly during streaming runs. Checkpoint paths encode the streaming progress; removing checkpoints forces a replay from Kafka offsets and should be done only when you explicitly want to reset consumer state.

---

## Troubleshooting

- **Startup order / bootstrap dependencies**
  - If a Spark job has `BOOTSTRAP_FROM_PG=1`, ensure Postgres is up before starting it (otherwise the JDBC read will fail). Re-run the job with `docker compose up -d <service>` once Postgres is ready.
- **One-shot init containers**
  - `kafka-init`, `connect-init`, `spark-init-delta`, and `training-service` are designed to exit after completing their work. Re-run them explicitly when needed (e.g., `docker compose run --rm connect-init`).
- **Kafka Connect connectors not created**
  - Check `docker compose logs -f kafka-connect connect-init`, then re-run `docker compose run --rm connect-init`.
- **Spark job fails at startup**
  - Inspect logs with `docker compose logs -f <spark-service-name>`.
  - If the failure happens during dependency resolution (`--packages`), verify network access during image build/run.
  - If the failure happens due to replay volume, switch `STARTING_OFFSETS` to `latest` or lower `MAX_OFFSETS_PER_TRIGGER`.
- **Postgres does not load snapshot CSVs**
  - Verify that `data/db_csv/*.csv` exist and that the `./data/db_csv:/import/csv/db:ro` volume is mounted.
  - If you regenerated CSVs after the first Postgres initialization, recreate the Postgres volume (data is persisted in `postgres-data`).
- **Dashboard shows empty Delta tables**
  - Delta reads require the `deltalake` dependency; if it is missing, the dashboard will silently fall back to “no Delta data”.
  - Confirm the Delta paths exist under `delta/` and match the env vars used by `dashboard/app.py` (see the Dashboard section above).
- **Reset to a clean state (destructive)**
  - `docker compose down -v` removes named volumes (`kafka-data`, `redis-data`, `postgres-data`) and starts the stack from scratch on next `up`.
