"""
Shared slowapi rate limiter instance.

Imported by main.py (to attach to the app) and by route modules (for decorators).
Limits are read from env vars at import time so they can be tuned via Cloud Run
env vars without a code change:
  RATE_LIMIT_CHAT  — default "20/minute"
  RATE_LIMIT_SHOT  — default "10/minute"
"""
import os
from slowapi import Limiter
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address, default_limits=[])

RATE_LIMIT_CHAT = os.getenv("RATE_LIMIT_CHAT", "20/minute")
RATE_LIMIT_SHOT = os.getenv("RATE_LIMIT_SHOT", "10/minute")
