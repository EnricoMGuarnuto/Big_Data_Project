import os
from datetime import datetime, timedelta

USE_SIMULATED_TIME = os.getenv("USE_SIMULATED_TIME", "0") in ("1", "true", "True")

TIME_MULTIPLIER = float(os.getenv("TIME_MULTIPLIER", "1.0"))
STEP_SECONDS = int(os.getenv("STEP_SECONDS", "1"))

SIM_START = datetime(2026, 1, 1, 7, 0, 0)
SIM_DAYS = int(os.getenv("SIM_DAYS", "365"))
SIM_END = SIM_START + timedelta(days=SIM_DAYS)
