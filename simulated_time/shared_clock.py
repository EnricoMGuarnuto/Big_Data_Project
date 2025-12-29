import time
from datetime import datetime, timedelta, timezone

def simulated_time_generator(start: datetime, end: datetime, step_seconds: int, time_multiplier: float):
    current = start.replace(tzinfo=timezone.utc)  # ✅ forza UTC
    end = end.replace(tzinfo=timezone.utc)        # ✅ forza UTC
    step = timedelta(seconds=step_seconds)

    while current <= end:
        yield current
        current += step
        time.sleep(step_seconds / time_multiplier)

def real_time_generator(step_seconds):
    while True:
        yield datetime.now(timezone.utc)  # ✅ anche qui: usa UTC
        time.sleep(step_seconds)
