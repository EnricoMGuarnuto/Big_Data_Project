# Shelf Refill Bridge

## What it does
- Listens to warehouse `wh_events` (specifically `wh_out` produced by the restock manager) and replays them as delayed `shelf_events`.
- Introduces a configurable delay so that restocks appear on shelves only after goods arrive.
- Persists pending events in Delta to survive restarts and resumes emitting them once their `available_at` timestamp is reached.

## Spark job steps
1. **Load shelf profiles** – Read the compacted `TOPIC_SHELF_PROFILES` topic, keep the latest message per shelf, and use it to compute `weight_change` events.
2. **Streaming read** – Consume `TOPIC_WH_EVENTS`, parse JSON, and keep only valid `wh_out` records, enriching them with `available_at = timestamp + DELAY_MINUTES`.
3. **ForeachBatch logic**
   - Split events between due now vs. still pending; merge with any Delta-stored pending events not yet emitted.
   - For due events, create `putback` and `weight_change` shelf events, serialize them to JSON, and emit them to `TOPIC_SHELF_EVENTS`.
   - Upsert the remaining pending events into the Delta table at `PENDING_PATH` so they can be retried in future batches.

## Key configuration
- `DELAY_MINUTES`: minutes to wait before re-emitting the warehouse events to shelves.
- `PENDING_PATH`, `CKP`: Delta storage for pending events and structured streaming checkpoint.
- Kafka topics: `TOPIC_WH_EVENTS`, `TOPIC_SHELF_EVENTS`, `TOPIC_SHELF_PROFILES`.
- Simulated time: uses Redis `sim:now`/`sim:today` via `simulated_time`; start `sim-clock` for deterministic runs.
