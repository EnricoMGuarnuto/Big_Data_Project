# Daily Discount Manager

## What it does
- Reads the latest shelf batch state (`/delta/cleansed/shelf_batch_state`) to find products expiring today or tomorrow.
- Generates daily discount suggestions per `shelf_id`/date, mirrors them into Delta (`/delta/analytics/daily_discounts`) and publishes them to the Kafka topic `daily_discounts`.
- Optionally emits `near_expiry_discount` alerts on the standard `alerts` topic so that the monitoring layer can inform operators.

## Job flow
1. Load `shelf_batch_state` from Delta and keep only lotti disponibili (`batch_quantity_store > 0`) with expiry between today and tomorrow.
2. Build the `(shelf_id, discount_date)` pairs that must be discounted (if expiry is tomorrow we schedule both today and tomorrow).
3. Merge with existing discounts (if any) and assign a new random extra discount within `[DISCOUNT_MIN, DISCOUNT_MAX]`, preserving previous values to avoid jumps.
4. Publish the JSON payloads to Kafka (`TOPIC_DAILY_DISCOUNTS`), mirroring the exact rows into Delta with an upsert on `(shelf_id, discount_date)`.
5. Emit optional alert messages so downstream dashboards know which shelves received an extra discount.

## Key configuration
- `DL_SHELF_BATCH_PATH`, `DL_DAILY_DISC_PATH`: Delta sources/destinations.
- `TOPIC_DAILY_DISCOUNTS`, `TOPIC_ALERTS`, `KAFKA_BROKER`: Kafka wiring.
- `DISCOUNT_MIN`, `DISCOUNT_MAX`: percentage range for the additional markdown.

