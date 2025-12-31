# Simulated time

Utility module + service to provide a shared “simulated clock” for the
pipeline. It publishes the current simulated timestamp into Redis so producers
and Spark jobs can use a consistent timeline during demos/tests.

## How it works
- `sim_clock_driver.py` advances time from `SIM_START` to `SIM_END` using the
  step size and multiplier.
- Each tick writes:
  - `sim:now` (ISO timestamp)
  - `sim:today` (ISO date)
- Client code reads the clock via `redis_helpers.py` or `RedisClock`.

## Files
- `sim_clock_driver.py`: main loop that writes the simulated time to Redis.
- `config.py`: clock configuration (start/end, step, multiplier).
- `redis_helpers.py`: helpers to read `sim:now` / `sim:today` with friendly errors.
- `redis_clock.py`: simple `RedisClock` class used by producers.
- `shared_clock.py`: time generator utilities (simulated + real time).
- `clock.py`: convenience export of `get_simulated_now`.
- `Dockerfile`: container for the sim-clock service.

## Configuration (env vars)
Read from `.env` or the environment:
- `USE_SIMULATED_TIME` (default `0`) must be `1` to run the clock.
- `TIME_MULTIPLIER` (default `1.0`) speeds up or slows down time.
- `STEP_SECONDS` (default `1`) tick size in seconds.
- `SIM_DAYS` (default `365`) total simulated duration from `SIM_START`.

## Run locally
```bash
docker compose up -d redis sim-clock
```
Or run directly (requires Redis running):
```bash
pip install redis
USE_SIMULATED_TIME=1 python simulated_time/sim_clock_driver.py
```
