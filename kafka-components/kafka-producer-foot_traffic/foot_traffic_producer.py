import os
import time
import uuid
import random
import json
import logging
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# === Config ===
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "foot_traffic")
SLEEP = float(os.getenv("SLEEP", 1))
MAX_RETRIES = 6

# === Logging ===
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s')

# === Connessione a Kafka ===
producer = None
for attempt in range(1, MAX_RETRIES + 1):
    try:
        logging.info(f"Tentativo {attempt}: connessione a Kafka ({KAFKA_BROKER})")
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logging.info("Connesso a Kafka.")
        break
    except NoBrokersAvailable:
        logging.warning("Kafka non disponibile, riprovo...")
        time.sleep(5)

if not producer:
    raise RuntimeError("Impossibile connettersi a Kafka.")

# === Distribuzione probabilità ===
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

# === Funzioni di supporto ===
def get_current_slot_index(now):
    for i, (start, end) in enumerate(time_slots):
        s = datetime.strptime(start, "%H:%M").time()
        e = datetime.strptime(end, "%H:%M").time()
        if s <= now.time() <= e:
            return i
    return None

def generate_trip_duration():
    r = random.random()
    if r < 0.35:
        return random.randint(10, 29)
    elif r < 0.74:
        return random.randint(30, 44)
    else:
        return random.randint(45, 75)

def generate_event(now):
    weekday = now.strftime("%A")
    slot_idx = get_current_slot_index(now)
    if slot_idx is None:
        return None

    prob = schedule[weekday][slot_idx]
    if random.random() <= (prob / 100):
        entry_time = now
        duration = generate_trip_duration()
        exit_time = entry_time + timedelta(minutes=duration)
        return {
            "event_type": "foot_traffic",
            "customer_id": str(uuid.uuid4()),
            "entry_time": entry_time.isoformat(),
            "exit_time": exit_time.isoformat(),
            "trip_duration_minutes": duration,
            "weekday": weekday,
            "time_slot": f"{time_slots[slot_idx][0]}–{time_slots[slot_idx][1]}"
        }
    return None

# === Main loop ===
while True:
    now = datetime.utcnow()
    event = generate_event(now)
    if event:
        producer.send(TOPIC, value=event)
        logging.info(f"message sent: {event}")
    time.sleep(SLEEP)