# simulated_time/redis_helpers.py

import redis
from datetime import datetime, timezone

REDIS_HOST = "redis"
REDIS_PORT = 6379
SIM_NOW_KEY = "sim:now"
SIM_TODAY_KEY = "sim:today"

def _redis_client():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def _get_value(key: str):
    try:
        r = _redis_client()
        return r.get(key)
    except redis.RedisError as exc:
        raise RuntimeError(
            "Redis not reachable for simulated time. Start redis and sim-clock "
            "(e.g. `docker compose up -d redis sim-clock`)."
        ) from exc

def get_simulated_date():
    value = _get_value(SIM_TODAY_KEY)
    if not value:
        raise RuntimeError(
            "Redis sim:today not initialized yet. Start sim-clock "
            "(e.g. `docker compose up -d sim-clock`)."
        )
    return value

def get_simulated_timestamp():
    value = _get_value(SIM_NOW_KEY)
    if not value:
        raise RuntimeError(
            "Redis sim:now not initialized yet. Start sim-clock "
            "(e.g. `docker compose up -d sim-clock`)."
        )
    return value

def get_simulated_now():
    s = get_simulated_timestamp()
    if not s:
        raise RuntimeError(
            "Redis sim:now not initialized yet. Start sim-clock "
            "(e.g. `docker compose up -d sim-clock`)."
        )
    try:
        dt = datetime.fromisoformat(s)
    except ValueError as exc:
        raise RuntimeError(
            f"Invalid sim:now value: {s}. Restart sim-clock to reset it."
        ) from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt
