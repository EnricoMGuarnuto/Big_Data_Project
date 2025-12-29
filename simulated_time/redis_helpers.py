# simulated_time/redis_helpers.py

import redis

def get_simulated_date():
    r = redis.Redis(host="redis", port=6379, decode_responses=True)
    return r.get("sim:today")

def get_simulated_timestamp():
    r = redis.Redis(host="redis", port=6379, decode_responses=True)
    return r.get("sim:now")
