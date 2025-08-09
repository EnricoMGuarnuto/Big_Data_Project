import os
import time
import uuid
import random
import json
import logging
import signal
from datetime import datetime, timedelta, date, time as dtime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# === Config ===
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "foot_traffic")
SLEEP = float(os.getenv("SLEEP", 0.5))  # tick più rapido per non perdere eventi ravvicinati
MAX_RETRIES = 6
DAILY_CUSTOMERS = os.getenv("DAILY_CUSTOMERS")  # se None, usa default
DEFAULT_DAILY_CUSTOMERS = 100
TIME_SCALE = float(os.getenv("TIME_SCALE", 1.0))  # 1.0 = realtime; 60 = 60x più veloce
# NB: usiamo orari "slot" in orario locale del negozio; qui assumiamo UTC per semplicità
#      Se vuoi locale: valuta zoneinfo("Europe/Rome") e convertirci.

# === Logging ===
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s')
log = logging.getLogger(__name__)

# termination flag
stop = False
def _handle_stop(sig, frame):
    global stop
    stop = True
signal.signal(signal.SIGTERM, _handle_stop)
signal.signal(signal.SIGINT, _handle_stop)

# === Pesi per slot per weekday (non devono sommare a 100; li normalizziamo) ===
schedule = {
    "Sunday":    [22, 21, 23, 19, 16, 13],
    "Monday":    [16, 17, 20, 17, 13, 10],
    "Tuesday":   [12, 15, 21, 17, 14, 9],
    "Wednesday": [15, 16, 25, 22, 17, 12],
    "Thursday":  [14, 14, 19, 21, 18, 10],
    "Friday":    [19, 17, 25, 24, 23, 15],
    "Saturday":  [22, 24, 27, 22, 20, 18]
}

time_slots = [
    ("00:00", "06:59"),
    ("07:00", "09:59"),
    ("10:00", "13:59"),
    ("14:00", "16:59"),
    ("17:00", "19:59"),
    ("20:00", "23:59"),
]

def parse_hhmm(s: str) -> dtime:
    return datetime.strptime(s, "%H:%M").time()

def slot_bounds(day: date, slot_idx: int):
    start_s, end_s = time_slots[slot_idx]
    s = parse_hhmm(start_s)
    e = parse_hhmm(end_s)
    start_dt = datetime.combine(day, s, tzinfo=timezone.utc)
    end_dt   = datetime.combine(day, e, tzinfo=timezone.utc)
    return start_dt, end_dt

def get_current_slot_index(now: datetime):
    for i, (start, end) in enumerate(time_slots):
        s = parse_hhmm(start)
        e = parse_hhmm(end)
        if s <= now.time() <= e:
            return i
    return None

def generate_trip_duration():
    r = random.random()
    if r < 0.35:   # 35%
        return random.randint(10, 29)
    elif r < 0.74: # +39% = 74%
        return random.randint(30, 44)
    else:          # 26%
        return random.randint(45, 75)

def largest_remainder_allocation(total: int, weights: list[int]) -> list[int]:
    """Normalizza weights, fa floor e redistribuisce il resto a chi ha remainder maggiore."""
    s = sum(weights)
    if s <= 0:
        # fallback uniforme
        base = total // len(weights)
        res = [base]*len(weights)
        for i in range(total - base*len(weights)):
            res[i] += 1
        return res

    quotas = [total * (w / s) for w in weights]
    floors = [int(q) for q in quotas]
    remainder = total - sum(floors)
    # indici ordinati per parte decimale desc
    order = sorted(range(len(weights)), key=lambda i: (quotas[i] - floors[i]), reverse=True)
    for i in range(remainder):
        floors[order[i]] += 1
    return floors

def uniform_times_in_slot(start_dt: datetime, end_dt: datetime, count: int, now: datetime | None = None):
    """Crea 'count' timestamp uniformi nello slot [start, end], filtrando quelli < now (se passato)."""
    if count <= 0:
        return []
    duration = (end_dt - start_dt).total_seconds()
    ts = [start_dt + timedelta(seconds=random.uniform(0, duration)) for _ in range(count)]
    ts.sort()
    if now:
        ts = [t for t in ts if t >= now]
    return ts

class DayPlan:
    def __init__(self, day: date, weekday: str, total_customers: int):
        self.day = day
        self.weekday = weekday
        self.total_customers = total_customers
        self.schedule = schedule[weekday]
        self.slot_counts = largest_remainder_allocation(total_customers, self.schedule)
        self.events = []  # list of (entry_time)
        # riempiamo eventi per tutta la giornata (entry_time); push real-time al volo
        for idx, cnt in enumerate(self.slot_counts):
            s, e = slot_bounds(day, idx)
            self.events.extend(uniform_times_in_slot(s, e, cnt))
        self.events.sort()
        self._cursor = 0

    def next_ready(self, now: datetime):
        """Ritorna il prossimo entry_time <= now e avanza il cursore; None se nulla è pronto."""
        if self._cursor >= len(self.events):
            return None
        if self.events[self._cursor] <= now:
            t = self.events[self._cursor]
            self._cursor += 1
            return t
        return None

    def remaining_today(self):
        return len(self.events) - self._cursor

def decide_daily_total() -> int:
    if DAILY_CUSTOMERS:
        try:
            v = int(DAILY_CUSTOMERS)
            if v > 0:
                return v
        except:
            pass
    return DEFAULT_DAILY_CUSTOMERS

def build_producer():
    producer = None
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
            break
        except NoBrokersAvailable:
            log.warning("Kafka non disponibile, riprovo tra 5s...")
            time.sleep(5)
    if not producer:
        raise RuntimeError("Impossibile connettersi a Kafka.")
    return producer

def build_event(entry_time: datetime):
    duration = generate_trip_duration()
    exit_time = entry_time + timedelta(minutes=duration)
    cid = str(uuid.uuid4())
    weekday = entry_time.strftime("%A")
    slot_idx = get_current_slot_index(entry_time)
    slot_label = f"{time_slots[slot_idx][0]}–{time_slots[slot_idx][1]}" if slot_idx is not None else "n/a"
    return {
        "event_type": "foot_traffic",
        "customer_id": cid,
        "entry_time": entry_time.isoformat(),
        "exit_time": exit_time.isoformat(),
        "trip_duration_minutes": duration,
        "weekday": weekday,
        "time_slot": slot_label
    }, cid

def now_utc():
    # TIME_SCALE>1 ⇒ il tempo scorre più veloce: simuliamo moltiplicando gli intervalli di sleep inversamente
    # Implementazione semplice: il "now" è sempre l'orologio reale; la cadenza di rilascio è accelerata riducendo lo sleep.
    return datetime.utcnow().replace(tzinfo=timezone.utc)

def main():
    producer = build_producer()

    current_day = now_utc().date()
    weekday = now_utc().strftime("%A")
    plan = DayPlan(current_day, weekday, decide_daily_total())

    log.info(f"[{weekday} {current_day}] Totale clienti: {plan.total_customers}, per slot: {plan.slot_counts}")

    try:
        while not stop:
            now = now_utc()

            # cambio giorno ⇒ rigenera piano
            if now.date() != current_day:
                current_day = now.date()
                weekday = now.strftime("%A")
                plan = DayPlan(current_day, weekday, decide_daily_total())
                log.info(f"[{weekday} {current_day}] Nuovo piano. Totale: {plan.total_customers}, per slot: {plan.slot_counts}")

            # rilascia tutti gli eventi “maturi” fino a now
            while True:
                ts = plan.next_ready(now)
                if ts is None:
                    break
                event, cid = build_event(ts)
                producer.send(TOPIC, key=cid, value=event)
                log.info(f"message sent: {event}")

            # sleep (scalato)
            time.sleep(max(0.05, SLEEP / TIME_SCALE))
    finally:
        log.info("Flush & close producer...")
        try:
            producer.flush(10)
        except Exception:
            pass
        producer.close(10)

if __name__ == "__main__":
    main()
