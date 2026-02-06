
import os
import time
import uuid
import random
import json
import logging
import signal
import redis
from typing import List, Optional
from datetime import datetime, timedelta, date, time as dtime, timezone
from simulated_time.redis_clock import RedisClock

# ============================================================
# Config
# ============================================================
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_STREAM = os.getenv("REDIS_STREAM", "foot_traffic")

DEFAULT_DAILY_CUSTOMERS = int(os.getenv("DEFAULT_DAILY_CUSTOMERS", 1000))
BASE_DAILY_CUSTOMERS = int(os.getenv("BASE_DAILY_CUSTOMERS", DEFAULT_DAILY_CUSTOMERS))
DAILY_CUSTOMERS = os.getenv("DAILY_CUSTOMERS")
TRAFFIC_MULTIPLIER = float(os.getenv("TRAFFIC_MULTIPLIER", "0.8"))

DAILY_VARIATION_PCT = float(os.getenv("DAILY_VARIATION_PCT", 0.10))
DISABLE_DAILY_VARIATION = os.getenv("DISABLE_DAILY_VARIATION", "0") in ("1", "true", "True")

SEED = os.getenv("SEED")
if SEED is not None:
    try:
        random.seed(int(SEED))
    except ValueError:
        random.seed(SEED)

time_slots = [
    ("00:00", "06:59"),
    ("07:00", "09:59"),
    ("10:00", "13:59"),
    ("14:00", "16:59"),
    ("17:00", "19:59"),
    ("20:00", "23:59"),
]

schedule = {
    "Sunday":    [22, 21, 23, 19, 16, 13],
    "Monday":    [16, 17, 20, 17, 13, 10],
    "Tuesday":   [12, 15, 21, 17, 14, 9],
    "Wednesday": [15, 16, 25, 22, 17, 12],
    "Thursday":  [14, 14, 19, 21, 18, 10],
    "Friday":    [19, 17, 25, 24, 23, 15],
    "Saturday":  [22, 24, 27, 22, 20, 18]
}

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s')
log = logging.getLogger(__name__)

stop = False
def _handle_stop(sig, frame):
    global stop
    stop = True
signal.signal(signal.SIGTERM, _handle_stop)
signal.signal(signal.SIGINT, _handle_stop)

WEEKDAYS_ORDER = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
SCHEDULE_BY_IDX = [schedule[d] for d in WEEKDAYS_ORDER]
DAY_WEIGHTS_SUM = [sum(w) for w in SCHEDULE_BY_IDX]
AVG_DAY_WEIGHT = sum(DAY_WEIGHTS_SUM) / len(DAY_WEIGHTS_SUM)

def parse_hhmm(s: str) -> dtime:
    return datetime.strptime(s, "%H:%M").time()

TIME_SLOTS_STR = time_slots
TIME_SLOTS = [(parse_hhmm(s), parse_hhmm(e)) for (s, e) in TIME_SLOTS_STR]

def slot_bounds(day: date, slot_idx: int):
    s, e = TIME_SLOTS[slot_idx]
    start_dt = datetime.combine(day, s, tzinfo=timezone.utc)
    end_dt   = datetime.combine(day, e, tzinfo=timezone.utc)
    return start_dt, end_dt

def get_slot_index_for(dt: datetime):
    t = dt.time()
    for i, (s, e) in enumerate(TIME_SLOTS):
        if s <= t <= e:
            return i
    return None

def largest_remainder_allocation(total: int, weights: List[int]) -> List[int]:
    n = len(weights)
    if total <= 0 or n == 0:
        return [0]*n
    s = sum(weights)
    if s <= 0:
        base = total // n
        res = [base]*n
        for i in range(total - base*n):
            res[i] += 1
        return res

    quotas = [total * (w / s) for w in weights]
    floors = [int(q) for q in quotas]
    remainder = total - sum(floors)
    order = sorted(range(n), key=lambda i: (quotas[i] - floors[i]), reverse=True)
    for i in range(remainder):
        floors[order[i]] += 1
    return floors

def uniform_times_in_slot(start_dt: datetime, end_dt: datetime, count: int, now_cut: Optional[datetime] = None):
    if count <= 0:
        return []
    s = start_dt if (now_cut is None or start_dt >= now_cut) else now_cut
    if s > end_dt:
        return []
    duration = (end_dt - s).total_seconds()
    if duration <= 0:
        return []
    ts = [s + timedelta(seconds=random.uniform(0, duration)) for _ in range(count)]
    ts.sort()
    return ts

def generate_trip_duration():
    r = random.random()
    if r < 0.35:
        return random.randint(10, 29)
    elif r < 0.74:
        return random.randint(30, 44)
    else:
        return random.randint(45, 75)

def decide_daily_total(weekday_idx: int) -> int:
    if DAILY_CUSTOMERS:
        try:
            v = int(DAILY_CUSTOMERS)
            if v > 0:
                return v
        except Exception:
            pass

    mean_total = BASE_DAILY_CUSTOMERS
    if AVG_DAY_WEIGHT > 0:
        mean_total = BASE_DAILY_CUSTOMERS * (DAY_WEIGHTS_SUM[weekday_idx] / AVG_DAY_WEIGHT)
    mean_total *= max(0.0, TRAFFIC_MULTIPLIER)

    if DISABLE_DAILY_VARIATION or DAILY_VARIATION_PCT <= 0:
        return max(1, round(mean_total))

    lo = 1.0 - DAILY_VARIATION_PCT
    hi = 1.0 + DAILY_VARIATION_PCT
    noise_factor = random.uniform(lo, hi)
    return max(1, round(mean_total * noise_factor))

def build_redis() -> redis.Redis:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    r.ping()
    log.info(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}")
    return r

def build_event(entry_time: datetime):
    duration = generate_trip_duration()
    exit_time = entry_time + timedelta(minutes=duration)
    cid = str(uuid.uuid4())
    weekday_idx = entry_time.weekday()
    slot_idx = get_slot_index_for(entry_time)
    slot_label = f"{TIME_SLOTS_STR[slot_idx][0]}–{TIME_SLOTS_STR[slot_idx][1]}" if slot_idx is not None else "n/a"
    return {
        "event_type": "foot_traffic",
        "customer_id": cid,
        "entry_time": entry_time.isoformat(),
        "exit_time": exit_time.isoformat(),
        "trip_duration_minutes": duration,
        "weekday": WEEKDAYS_ORDER[weekday_idx],
        "time_slot": slot_label
    }, cid

class DayPlan:
    def __init__(self, day: date, weekday_idx: int, total_customers: int, now: Optional[datetime] = None):
        self.day = day
        self.weekday_idx = weekday_idx
        self.total_customers = total_customers
        self.schedule = SCHEDULE_BY_IDX[weekday_idx]
        base_counts = largest_remainder_allocation(total_customers, self.schedule)
        self.slot_counts = base_counts[:]
        if now is not None and now.date() == day:
            curr_idx = get_slot_index_for(now)
            for idx in range(len(self.slot_counts)):
                s, e = slot_bounds(day, idx)
                if curr_idx is None:
                    continue
                if idx < curr_idx:
                    self.slot_counts[idx] = 0
                elif idx == curr_idx:
                    full = (e - s).total_seconds()
                    rem = max(0.0, (e - now).total_seconds())
                    frac = rem / full if full > 0 else 0.0
                    self.slot_counts[idx] = int(round(self.slot_counts[idx] * frac))
        self.events = []
        for idx, cnt in enumerate(self.slot_counts):
            s, e = slot_bounds(day, idx)
            cutoff = None
            if now is not None and now.date() == day and s <= now <= e:
                cutoff = now
            self.events.extend(uniform_times_in_slot(s, e, cnt, now_cut=cutoff))
        self.events.sort()
        self._cursor = 0

    def next_ready(self, now: datetime):
        if self._cursor >= len(self.events):
            return None
        if self.events[self._cursor] <= now:
            t = self.events[self._cursor]
            self._cursor += 1
            return t
        return None

def main():
    rconn = build_redis()

    clock = RedisClock(host=REDIS_HOST, port=REDIS_PORT)

    current_day = None
    weekday_idx = None
    plan = None
    future_exits = []

    last_now = None

    while not stop:
        try:
            now = clock.now()
        except Exception:
            time.sleep(0.5)
            continue


        if last_now is not None and now <= last_now:
            time.sleep(0.1)
            continue

        last_now = now

        if current_day != now.date():
            current_day = now.date()
            weekday_idx = now.weekday()
            plan = DayPlan(
                current_day,
                weekday_idx,
                decide_daily_total(weekday_idx),
                now=now
            )
            log.info(
                f"[{WEEKDAYS_ORDER[weekday_idx]} {current_day}] "
                f"New plan. Total: {sum(plan.slot_counts)}; per slot: {plan.slot_counts}"
            )

        while True:
            ts = plan.next_ready(now)
            if ts is None:
                break

            event, cid = build_event(ts)

            try:
                rconn.xadd(REDIS_STREAM, {"data": json.dumps(event), "key": cid}, maxlen=20000, approximate=True)
            except Exception as e:
                log.warning(f"Redis XADD failed (foot_traffic): {e}")
            log.info(f"Simulated: {event}")

            entry_event = {
                "event_type": "entry",
                "time": event["entry_time"],
                "weekday": event["weekday"],
                "time_slot": event["time_slot"]
            }

            try:
                rconn.xadd(
                    f"{REDIS_STREAM}:realistic",
                    {"data": json.dumps(entry_event), "key": cid},
                    maxlen=20000,
                    approximate=True,
                )
            except Exception as e:
                log.warning(f"Redis XADD failed (entry): {e}")

            exit_event = {
                "event_type": "exit",
                "time": event["exit_time"],
                "weekday": event["weekday"],
                "time_slot": event["time_slot"]
            }

            future_exits.append((datetime.fromisoformat(event["exit_time"]), exit_event, cid))

        for exit_time, exit_event, cid in future_exits[:]:
            if exit_time <= now:
                try:
                    rconn.xadd(
                        f"{REDIS_STREAM}:realistic",
                        {"data": json.dumps(exit_event), "key": cid},
                        maxlen=20000,
                        approximate=True,
                    )
                except Exception as e:
                    log.warning(f"Redis XADD failed (exit): {e}")
                future_exits.remove((exit_time, exit_event, cid))


if __name__ == "__main__":
    main()
