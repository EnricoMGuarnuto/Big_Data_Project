# Shelf Restock Manager

## What it does
- Consumes the compacted `shelf_restock_plan` topic and turns mature plans into warehouse pick tickets (`wh_events`).
- Allocates inventory by FIFO based on the Delta mirror of warehouse batches, ensuring each plan draws from the oldest lot first.
- Updates the Delta restock plan mirror and optionally acknowledges related alerts once work orders are issued.

## Spark job steps
1. **Streaming source** – Read the `TOPIC_RESTOCK` topic, parse plans, and keep only pending entries older than `PLAN_DELAY_SEC` to avoid immediate re-triggers.
2. **Inventory join** – Load the warehouse batch state from `DL_WH_BATCH_PATH` and join against candidate plans per shelf.
3. **FIFO allocation** – Use window functions ordered by `received_date`, `expiry_date`, and `batch_code` to compute how much each batch can contribute to the requested quantity.
4. **Emit warehouse events** – Build `wh_out` payloads (including plan ID, batch code, and timestamps) and send them to `TOPIC_WH_EVENTS`.
5. **State updates** – Mark the consumed plans as `issued` inside the Delta mirror (`DL_RESTOCK_PATH`) and optionally emit alert acknowledgements to `TOPIC_ALERTS`.

## Key configuration
- Kafka topics: `TOPIC_RESTOCK`, `TOPIC_WH_EVENTS`, `TOPIC_ALERTS`.
- Delta inputs/outputs: `DL_WH_BATCH_PATH`, `DL_RESTOCK_PATH`, `CKP_ROOT`.
- Operational tuning: `PLAN_DELAY_SEC` ensures alerts have time to self-heal before triggering a pick.
- Simulated time: uses Redis `sim:now`/`sim:today` via `simulated_time`; start `sim-clock` for deterministic runs.
