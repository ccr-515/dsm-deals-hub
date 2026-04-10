
from datetime import timedelta

# ---- Config ----
FREEZE_MINUTES = 30
WEEKLY_PATTERNS_CAP_PER_VENUE = 4
MAX_DEAL_DURATION_HOURS = 12
LAST_MINUTE_MAX_HOURS = 3

CACHE_SECONDS = 45

PDF_CRON = "MON 08:00 America/Chicago"  # placeholder for later

# Sorting weights (used heuristically)
SORT_WEIGHTS = {
    "expiringSoon": 1.0,
    "neighborhoodPriority": 0.7,
    "distance": 0.5,
    "featuredBoost": 0.3,
    "freshnessBump": 0.2,
}

FEATURED_ENABLED = False  # reserved for later

# Simple admin key for moderation routes in MVP (change in prod)
ADMIN_KEY = "changeme-admin-key"
