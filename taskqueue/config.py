import os

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://localhost:5432/taskqueue"
)

# Worker tuning
LEASE_SECONDS = float(os.environ.get("LEASE_SECONDS", "30"))
HEARTBEAT_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL", "10"))
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "0.5"))

# Reaper tuning
REAP_INTERVAL = float(os.environ.get("REAP_INTERVAL", "5"))

# Retry backoff: BACKOFF_BASE * 2^attempts seconds, capped at BACKOFF_CAP
BACKOFF_BASE = float(os.environ.get("BACKOFF_BASE", "1"))
BACKOFF_CAP = float(os.environ.get("BACKOFF_CAP", "300"))
