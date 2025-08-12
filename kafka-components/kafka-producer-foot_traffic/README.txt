FOOT-TRAFFIC KAFKA PRODUCER
This component simulates in-store customer arrivals and streams them to Kafka as JSON events. It respects empirically observed time-of-day preferences (weights per weekday and slot) and is configurable, reproducible, and simple to operate.

WHAT IT DOES (SUMMARY)

Chooses how many customers “today” based on the weekday’s overall popularity and (optionally) a small random variation.

Splits today’s total across six time slots using your per-slot weights. Integer counts are computed with Largest Remainder Allocation to avoid rounding bias.

Starts “from now onward”: if you launch mid-day, past slots are dropped and the current slot is scaled by its remaining time.

Samples entry timestamps uniformly inside each slot’s interval and emits events in real time as those timestamps become due.

For each entry, draws a visit duration and computes an exit time.

Sends one JSON event per entry to a Kafka topic (default: foot_traffic).

TIME MODEL

All timestamps are in UTC (ISO 8601 with timezone).

Slot grid (UTC): 00:00–06:59, 07:00–09:59, 10:00–13:59, 14:00–16:59, 17:00–19:59, 20:00–23:59.

Real-time clock: the code checks the wall clock. TIME_SCALE only shortens the sleep between checks (more frequent polling); it does not alter event timestamps.

If you need local time (e.g., Europe/Rome), convert using zoneinfo before combining dates and times.

ARRIVAL GENERATION LOGIC

Daily total:

Let W_d be the 6 weights for weekday d, S_d = sum(W_d), and S̄ = average of S_d across all weekdays.

If DAILY_CUSTOMERS is set, that fixed number is used for every day.

Otherwise, mean_total = BASE_DAILY_CUSTOMERS * (S_d / S̄).

Optional day-to-day variation: multiply by a random factor in [1 − DAILY_VARIATION_PCT, 1 + DAILY_VARIATION_PCT] unless disabled.

Per-slot allocation (integers):

quota_i = mean_total * (W_d[i] / S_d).

Take the floor of each quota and distribute the remaining units to the largest fractional remainders (Largest Remainder Allocation).

Mid-day start (“from now”):

Past slots are set to zero.

The current slot is scaled by the fraction of time remaining in that slot.

Future slots stay unchanged.

Entry times inside a slot:

For each slot, sample “count” uniform timestamps in [slot_start, slot_end] (or [now, slot_end] for the current slot), then sort.

Event emission:

A loop compares the next scheduled entry to the current time; when entry_time ≤ now, the event is emitted to Kafka.

Visit duration:

Mixed distribution in minutes: 35% 10–29, 39% 30–44, 26% 45–75.

exit_time = entry_time + duration.

MESSAGE SCHEMA (JSON)

Produced to the Kafka topic (key = customer_id):

{
"event_type": "foot_traffic",
"customer_id": "UUID",
"entry_time": "2025-08-12T17:52:24.442703+00:00",
"exit_time": "2025-08-12T18:31:24.442703+00:00",
"trip_duration_minutes": 39,
"weekday": "Tuesday",
"time_slot": "17:00–19:59"
}

Notes:

weekday is the English weekday name via a fixed mapping from datetime.weekday() (locale-independent).

time_slot is the human-readable label from the configured grid.

CONFIGURATION (ENVIRONMENT VARIABLES)

KAFKA_BROKER
Default: kafka:9092
Kafka bootstrap server(s). Inside Docker Compose use the service name; from host, expose a host listener.

KAFKA_TOPIC
Default: foot_traffic
Kafka topic for events.

SLEEP
Default: 0.5
Base loop sleep (seconds). Larger values reduce log volume.

TIME_SCALE
Default: 1.0
Divides SLEEP (higher value = more frequent polling). Does not change timestamps.

DEFAULT_DAILY_CUSTOMERS
Default: 100
Legacy default; BASE_DAILY_CUSTOMERS uses this if not set.

BASE_DAILY_CUSTOMERS
Default: value of DEFAULT_DAILY_CUSTOMERS
Mean daily total for an “average” weekday; scaled by weekday popularity.

DAILY_CUSTOMERS
Default: unset
If set, forces a fixed total every day (bypasses weekday scaling and variation).

DAILY_VARIATION_PCT
Default: 0.10
Day-to-day variation magnitude. Example 0.10 means ±10%. Set to 0 to disable.

DISABLE_DAILY_VARIATION
Default: 0 (false)
If 1/true, disables variation regardless of DAILY_VARIATION_PCT.

SEED
Default: unset
Seed for Python’s random module to make runs reproducible.

MAX_RETRIES
Default: 6
Startup attempts to connect to Kafka.

RETRY_BACKOFF_SECONDS
Default: 5.0
Backoff between connection attempts.

STARTUP, LOGGING, SHUTDOWN

On startup, the producer connects to Kafka with retries and backoff.

A daily plan is computed and logged, for example:
[Tuesday 2025-08-12] Planned total (from now): 29; per slot: [0, 0, 0, 8, 12, 9]

Each emitted event is logged at INFO level.

On SIGINT/SIGTERM the producer flushes and closes gracefully.

QUICK START (STANDALONE)

Ensure Kafka is reachable at localhost:9092 (or override KAFKA_BROKER).

Example:
export KAFKA_BROKER=localhost:9092
export KAFKA_TOPIC=foot_traffic
export DAILY_CUSTOMERS=20
export DISABLE_DAILY_VARIATION=1
python foot_traffic_producer.py

Tip: increase SLEEP (e.g., 1.0) if you want fewer log lines during testing.

ASSUMPTIONS AND LIMITATIONS

Uniform arrivals within each slot. If a Poisson process or time-varying rates are required, replace the sampler accordingly.

UTC timestamps by default. For local-time operations, convert with zoneinfo and ensure slot boundaries reflect local time.

Real-time clock only. To compress simulation time (e.g., 24h simulated in 1h real), introduce a virtual clock and scale timestamps consistently.

Single-process producer. Running multiple instances would require coordination (partitioning and deduplication policy).