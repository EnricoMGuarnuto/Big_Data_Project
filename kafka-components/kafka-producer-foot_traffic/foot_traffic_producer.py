from datetime import datetime
import os
import time
import uuid
import random
import json
import logging
import signal
from typing import List, Optional
from datetime import datetime, timedelta, date, time as dtime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ============================================================
# Config
# ============================================================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "foot_traffic")
TOPIC_REALISTIC = os.getenv("KAFKA_TOPIC_REALISTIC", "foot_traffic_realistic")

SLEEP = float(os.getenv("SLEEP", 0.5))
TIME_SCALE = float(os.getenv("TIME_SCALE", 1.0))

DEFAULT_DAILY_CUSTOMERS = int(os.getenv("DEFAULT_DAILY_CUSTOMERS", 1000))
BASE_DAILY_CUSTOMERS = int(os.getenv("BASE_DAILY_CUSTOMERS", DEFAULT_DAILY_CUSTOMERS))
DAILY_CUSTOMERS = os.getenv("DAILY_CUSTOMERS")

DAILY_VARIATION_PCT = float(os.getenv("DAILY_VARIATION_PCT", 0.10))
DISABLE_DAILY_VARIATION = os.getenv("DISABLE_DAILY_VARIATION", "0") in ("1", "true", "True")

SEED = os.getenv("SEED")
if SEED is not None:
    try:
        random.seed(int(SEED))
    except ValueError:
        random.seed(SEED)

MAX_RETRIES = int(os.getenv("MAX_RETRIES", 6))
RETRY_BACKOFF_SECONDS = float(os.getenv("RETRY_BACKOFF_SECONDS", 5.0))

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
SCHEDULE_BY_IDX = [
    schedule["Monday"],
    schedule["Tuesday"],
    schedule["Wednesday"],
    schedule["Thursday"],
    schedule["Friday"],
    schedule["Saturday"],
    schedule["Sunday"],
]
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

    if DISABLE_DAILY_VARIATION or DAILY_VARIATION_PCT <= 0:
        return max(1, round(mean_total))

    lo = 1.0 - DAILY_VARIATION_PCT
    hi = 1.0 + DAILY_VARIATION_PCT
    noise_factor = random.uniform(lo, hi)
    return max(1, round(mean_total * noise_factor))

def build_producer():
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info(f"Tentativo {attempt}: connessione a Kafka ({KAFKA_BROKER})")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda v: v.encode('utf-8') if v else None,
                acks='all',
                retries=5,
                linger_ms=50,
                compression_type='gzip',
                max_in_flight_requests_per_connection=1,
            )
            log.info("Connesso a Kafka.")
            return producer
        except NoBrokersAvailable as e:
            log.warning(f"Kafka non disponibile ({e}), riprovo tra {RETRY_BACKOFF_SECONDS}s...")
        except Exception as e:
            log.warning(f"Errore di connessione a Kafka ({type(e).__name__}: {e}), riprovo tra {RETRY_BACKOFF_SECONDS}s...")
        time.sleep(RETRY_BACKOFF_SECONDS)
    raise RuntimeError("Impossibile connettersi a Kafka.")

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

def now_utc():
    return datetime.utcnow().replace(tzinfo=timezone.utc)

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
    producer = build_producer()
    now = now_utc()
    current_day = now.date()
    weekday_idx = now.weekday()
    plan = DayPlan(current_day, weekday_idx, decide_daily_total(weekday_idx), now=now)
    future_exits = []

    log.info(f"[{WEEKDAYS_ORDER[weekday_idx]} {current_day}] Totale pianificato (da ora in poi): {sum(plan.slot_counts)}; per slot: {plan.slot_counts}")

    try:
        while not stop:
            now = now_utc()
            if now.date() != current_day:
                current_day = now.date()
                weekday_idx = now.weekday()
                plan = DayPlan(current_day, weekday_idx, decide_daily_total(weekday_idx), now=now)
                log.info(f"[{WEEKDAYS_ORDER[weekday_idx]} {current_day}] Nuovo piano. Totale (da ora in poi): {sum(plan.slot_counts)}; per slot: {plan.slot_counts}")

            while True:
                ts = plan.next_ready(now)
                if ts is None:
                    break
                event, cid = build_event(ts)
                producer.send(TOPIC, key=cid, value=event)
                log.info(f"Simulativo: {event}")
                

                # Evento realistico: entry
                entry_event = {
                    "event_type": "entry",
                    "time": event["entry_time"],
                    "weekday": event["weekday"],
                    "time_slot": event["time_slot"]
                }
                producer.send(TOPIC_REALISTIC, key=cid, value=entry_event)
                log.info(f"Realistico ENTRY: {entry_event}")

                # Pianifica evento exit per il futuro
                exit_event = {
                    "event_type": "exit",
                    "time": event["exit_time"],
                    "weekday": event["weekday"],
                    "time_slot": event["time_slot"]
                }
                future_exits.append((datetime.fromisoformat(event["exit_time"]), exit_event, cid))

            # Verifica eventi di uscita pronti da inviare
            for exit_time, exit_event, cid in future_exits[:]:
                if exit_time <= now:
                    producer.send(TOPIC_REALISTIC, key=cid, value=exit_event)
                    log.info(f"Realistico EXIT: {exit_event}")
                    future_exits.remove((exit_time, exit_event, cid))

            time.sleep(max(0.1, SLEEP / max(1.0, TIME_SCALE)))
    finally:
        log.info("Flush & close producer...")
        try:
            producer.flush(10)
        except Exception:
            pass
        producer.close(10)

if __name__ == "__main__":
    main()
