# WH Supplier Manager

## What it does
- Automates the warehouse → store replenishment flow based on `wh_supplier_plan` Delta mirrors.
- At scheduled cutoff times (Sun/Tue/Thu 12:00 UTC) groups pending plans into delivery orders, writes them into Delta, and publishes the updated plan statuses back to Kafka.
- At delivery times (Mon/Wed/Fri 08:00 UTC) emits synthetic `wh_in` events, records receipts, updates orders to `delivered`, and marks the related plans as `completed`.
- Keeps append-only mirrors for warehouse events plus idempotent ledgers for orders and receipts in Delta.

## Job flow
1. **Bootstrap** – Ensure Delta tables for orders and receipts exist (empty tables are created if missing).
2. **Cutoff ticks** – When `is_cutoff_moment` is true, aggregate pending plans by `shelf_id`, write/merge into `DL_ORDERS_PATH`, and publish plan updates via `TOPIC_WH_SUPPLIER_PLAN`.
3. **Delivery ticks** – When `is_delivery_moment` is true, read issued orders for today, skip already received shelves, explode quantities into batches, emit `wh_in` events to Kafka/Delta, upsert receipts, set orders to `delivered`, and close the issued plans.
4. **Streaming trigger** – A Spark rate source drives the scheduler every `TICK_MINUTES`, ensuring both cutoff and delivery logic run idempotently.

## Key configuration
- `TOPIC_WH_SUPPLIER_PLAN`, `TOPIC_WH_EVENTS`, `KAFKA_BROKER`: Kafka topics for plans and warehouse events.
- `DL_SUPPLIER_PLAN_PATH`, `DL_ORDERS_PATH`, `DL_RECEIPTS_PATH`, `DL_WH_EVENTS_RAW`: Delta inputs/outputs.
- `CUTOFF_HOUR/MINUTE`, `DELIVERY_HOUR/MINUTE`, `TICK_MINUTES`: scheduling knobs.
- `DEFAULT_EXPIRY_DAYS`: shelf-life applied to generated batches.
- Simulated time: uses Redis `sim:now`/`sim:today` via `simulated_time`; start `sim-clock` for deterministic runs.
