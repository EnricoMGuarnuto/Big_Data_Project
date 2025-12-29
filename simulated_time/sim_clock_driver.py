import time
import redis
from datetime import timezone

from simulated_time.config import (
    USE_SIMULATED_TIME,
    SIM_START,
    SIM_END,
    STEP_SECONDS,
    TIME_MULTIPLIER,
)
from simulated_time.shared_clock import simulated_time_generator


REDIS_HOST = "redis"
REDIS_PORT = 6379
REDIS_KEY  = "sim:now"

def main():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    if not USE_SIMULATED_TIME:
        raise RuntimeError("sim-clock-driver requires USE_SIMULATED_TIME=1")

    clock = simulated_time_generator(
        SIM_START, SIM_END, STEP_SECONDS, TIME_MULTIPLIER
    )

    for t in clock:
        # ISO UTC
        r.set(REDIS_KEY, t.isoformat())
        r.set("sim:today", t.date().isoformat())
        time.sleep(0.001)  # tiny sleep to avoid busy loop

if __name__ == "__main__":
    main()
