import redis
from datetime import datetime

REDIS_KEY = "sim:now"

class RedisClock:
    def __init__(self, host="redis", port=6379):
        self.r = redis.Redis(host=host, port=port, decode_responses=True)

    def now(self) -> datetime:
        s = self.r.get(REDIS_KEY)
        if s is None:
            raise RuntimeError("Redis sim:now not initialized yet")
        return datetime.fromisoformat(s)
