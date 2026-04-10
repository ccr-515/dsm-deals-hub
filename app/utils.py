from datetime import datetime, timedelta
from typing import Optional
import math

from . import config, models

WEEKDAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
WEEKDAY_LOOKUP = {day.lower(): day for day in WEEKDAYS}


def normalize_slug(value: str) -> str:
    return value.strip().lower()


def parse_hhmm(s: str):
    parts = s.split(":")
    if len(parts) != 2:
        raise ValueError("Time must be in HH:MM format")
    h = int(parts[0])
    m = int(parts[1])
    if not (0 <= h <= 23 and 0 <= m <= 59):
        raise ValueError("Time must be in HH:MM format")
    return h, m


def duration_hours(start: datetime, end: datetime) -> float:
    return (end - start).total_seconds() / 3600.0


def distance_miles(lat1, lon1, lat2, lon2):
    # Haversine (rough miles)
    if None in (lat1, lon1, lat2, lon2):
        return None
    r = 3958.8
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dl / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def within_freeze(start_at: datetime, now: Optional[datetime] = None) -> bool:
    if not start_at:
        return False
    now = now or datetime.utcnow()
    delta = (start_at - now).total_seconds() / 60.0
    return delta <= config.FREEZE_MINUTES


def weekday_from_int(i: int) -> str:
    return WEEKDAYS[i]


def normalize_weekday_pattern(pattern: str) -> str:
    value = pattern.strip()
    if value.lower() == "all":
        return "All"

    normalized = []
    for part in value.split(","):
        key = part.strip().lower()
        if key not in WEEKDAY_LOOKUP:
            raise ValueError("weekday_pattern must use Mon,Tue,... or All")
        day = WEEKDAY_LOOKUP[key]
        if day not in normalized:
            normalized.append(day)

    if not normalized:
        raise ValueError("weekday_pattern must use Mon,Tue,... or All")
    return ",".join(normalized)


def matches_weekday_pattern(pattern: str, dt: datetime) -> bool:
    if not pattern:
        return False
    normalized = normalize_weekday_pattern(pattern)
    if normalized == "All":
        return True
    want = weekday_from_int(dt.weekday())
    return want in [value.strip() for value in normalized.split(",")]


def build_day_window(day_ref: datetime, start_time: str, end_time: str) -> tuple[datetime, datetime]:
    sh, sm = parse_hhmm(start_time)
    eh, em = parse_hhmm(end_time)
    start_dt = day_ref.replace(hour=sh, minute=sm, second=0, microsecond=0)
    end_dt = day_ref.replace(hour=eh, minute=em, second=0, microsecond=0)
    return start_dt, end_dt


def deal_is_live_now(deal, now: Optional[datetime] = None) -> bool:
    now = now or datetime.utcnow()
    if deal.status != models.Status.live:
        return False

    if deal.type == models.DealType.last_minute:
        return bool(deal.start_at and deal.end_at and deal.start_at <= now <= deal.end_at)

    if not all([deal.weekday_pattern, deal.start_time, deal.end_time]):
        return False
    if not matches_weekday_pattern(deal.weekday_pattern, now):
        return False

    start_dt, end_dt = build_day_window(now, deal.start_time, deal.end_time)
    return start_dt <= now <= end_dt


def deal_overlaps_window(deal, window_start: datetime, window_end: datetime) -> bool:
    if deal.status != models.Status.live:
        return False

    if deal.type == models.DealType.last_minute:
        return bool(
            deal.start_at and
            deal.end_at and
            deal.end_at >= window_start and
            deal.start_at <= window_end
        )

    if not all([deal.weekday_pattern, deal.start_time, deal.end_time]):
        return False

    total_days = max((window_end.date() - window_start.date()).days, 0)
    for offset in range(total_days + 1):
        day_ref = (window_start + timedelta(days=offset)).replace(
            hour=12,
            minute=0,
            second=0,
            microsecond=0,
        )
        if not matches_weekday_pattern(deal.weekday_pattern, day_ref):
            continue
        start_dt, end_dt = build_day_window(day_ref, deal.start_time, deal.end_time)
        if end_dt >= window_start and start_dt <= window_end:
            return True
    return False


def next_occurrence_start(deal, window_start: datetime, horizon_days: int = 7) -> Optional[datetime]:
    if deal.type == models.DealType.last_minute:
        if not deal.start_at or not deal.end_at or deal.end_at < window_start:
            return None
        return max(deal.start_at, window_start)

    if not all([deal.weekday_pattern, deal.start_time, deal.end_time]):
        return None

    for offset in range(horizon_days + 1):
        day_ref = (window_start + timedelta(days=offset)).replace(
            hour=12,
            minute=0,
            second=0,
            microsecond=0,
        )
        if not matches_weekday_pattern(deal.weekday_pattern, day_ref):
            continue
        start_dt, end_dt = build_day_window(day_ref, deal.start_time, deal.end_time)
        if end_dt < window_start:
            continue
        if start_dt <= window_start <= end_dt:
            return window_start
        return start_dt

    return None
