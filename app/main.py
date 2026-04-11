from datetime import datetime, timedelta
from html import escape
import json
import os
from pathlib import Path
import re
from typing import List, Optional
from zoneinfo import ZoneInfo

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import func, or_
from sqlalchemy.orm import Session, joinedload

from . import config, models, schemas
from .database import Base, SessionLocal, engine
from .migrations import run_migrations
from .utils import (
    build_day_window,
    deal_is_live_now,
    deal_overlaps_window,
    distance_miles,
    duration_hours,
    matches_weekday_pattern,
    next_occurrence_start,
    normalize_slug,
    parse_hhmm,
)
from . import weekly_master_content as weekly_content

Base.metadata.create_all(bind=engine)
run_migrations(engine)

APP_DIR = Path(__file__).parent
STATIC_DIR = APP_DIR / "static"
HOME_TIMEZONE = ZoneInfo("America/Chicago")
HOMEPAGE_SEED_SOURCE = "seed://homepage-curated-v2"
WEEKDAY_LONG = {
    "Mon": "Monday",
    "Tue": "Tuesday",
    "Wed": "Wednesday",
    "Thu": "Thursday",
    "Fri": "Friday",
    "Sat": "Saturday",
    "Sun": "Sunday",
}
DAY_ORDER = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
NEIGHBORHOOD_CENTERS = {
    "Altoona": (41.6446, -93.4652),
    "Beaverdale": (41.6156, -93.6712),
    "Clive": (41.6030, -93.7241),
    "Court District": (41.5854, -93.6228),
    "Des Moines Metro": (41.5868, -93.6250),
    "Downtown Des Moines": (41.5868, -93.6250),
    "East Village": (41.5912, -93.6118),
    "Grimes": (41.6883, -93.7911),
    "Ingersoll": (41.5866, -93.6596),
    "Johnston": (41.6730, -93.6975),
    "Luther": (41.9914, -93.8194),
    "Merle Hay": (41.6277, -93.6970),
    "South Side": (41.5542, -93.6130),
    "Urbandale": (41.6267, -93.7122),
    "Valley Junction": (41.5750, -93.7110),
    "Waveland": (41.6009, -93.6824),
    "West Des Moines": (41.5772, -93.7113),
    "Western Gateway": (41.5851, -93.6385),
}
DEAL_ICON_KEYWORDS = [
    ("wine", "🍷", ("wine",)),
    ("seafood", "🦪", ("oyster", "oysters", "seafood", "shrimp", "fish", "walleye")),
    ("sandwich", "🥪", ("sandwich", "sandwiches", "cheesesteak", "cheesesteaks", "reuben", "deli", "wrap", "wraps", "doner", "döner", "tenderloin")),
    ("steak", "🥩", ("steak", "steaks", "ribeye", "ribeyes", "prime rib", "sirloin", "sirloins")),
    ("burger", "🍔", ("burger", "burgers", "cheeseburger", "cheeseburgers")),
    ("taco", "🌮", ("taco", "tacos")),
    ("hot-dog", "🌭", ("hot dog", "polish", "wiener")),
    ("chicken", "🍗", ("wings", "wing", "chicken", "chicken fried", "tenders")),
    ("pizza", "🍕", ("pizza", "pizzas", "flatbread", "flatbreads")),
    ("brunch", "🍳", ("breakfast", "brunch", "french toast", "pancake", "pancakes")),
    ("buffet", "🍽️", ("buffet", "buffets")),
    ("beer", "🍺", ("beer", "pint")),
]
BRAND_PLACEHOLDER_GLYPH = "DH"
PUBLIC_NEIGHBORHOOD_LABELS = {
    "des moines area": "Greater Des Moines",
}
NEIGHBORHOOD_PLACEHOLDER_ICON_MAP = {
    "Ankeny": "AN",
    "Beaverdale": "BV",
    "Clive": "CL",
    "Court Avenue": "CA",
    "Court District": "CD",
    "Cumming": "CU",
    "Des Moines": "DM",
    "Des Moines Area": "DM",
    "Des Moines Metro": "DM",
    "Downtown": "DT",
    "Drake": "DR",
    "East Side": "ES",
    "East Village": "EV",
    "Greater Des Moines": "GDM",
    "Grimes": "GR",
    "Ingersoll": "IN",
    "Johnston": "JO",
    "Merle Hay": "MH",
    "Norwalk": "NW",
    "Prairie Meadows": "PM",
    "Prairie Trail": "PT",
    "Saylorville": "SV",
    "South Side": "SS",
    "Urbandale": "UR",
    "Valley Junction": "VJ",
    "Waukee": "WK",
    "West Des Moines": "WDM",
    "Western Gateway": "WG",
}
NEIGHBORHOOD_ICON_TONES = {
    "ankeny": "meadow",
    "beaverdale": "grove",
    "clive": "meadow",
    "court-avenue": "market",
    "court-district": "market",
    "cumming": "river",
    "des-moines": "civic",
    "des-moines-area": "civic",
    "downtown": "market",
    "drake": "civic",
    "east-side": "sunrise",
    "east-village": "sunrise",
    "grimes": "meadow",
    "greater-des-moines": "civic",
    "ingersoll": "market",
    "johnston": "grove",
    "merle-hay": "grove",
    "norwalk": "river",
    "prairie-meadows": "meadow",
    "prairie-trail": "meadow",
    "saylorville": "river",
    "south-side": "civic",
    "urbandale": "grove",
    "valley-junction": "market",
    "waukee": "meadow",
    "west-des-moines": "grove",
    "western-gateway": "market",
}


def current_site_base_path() -> str:
    return os.getenv("DSM_DEALS_SITE_BASE_PATH", "").strip().strip("/")


def site_href(path: str) -> str:
    normalized = (path or "/").strip()
    if not normalized.startswith("/"):
        normalized = f"/{normalized}"

    last_segment = normalized.rsplit("/", 1)[-1]
    if normalized != "/" and "." not in last_segment and not normalized.endswith("/"):
        normalized = f"{normalized}/"

    site_base_path = current_site_base_path()
    if not site_base_path:
        return normalized

    if normalized == "/":
        return f"/{site_base_path}/"
    return f"/{site_base_path}{normalized}"


def placeholder_icon_text(label: str, *, max_chars: int = 3) -> str:
    words = re.findall(r"[A-Za-z0-9]+", label or "")
    if not words:
        return "•"

    filtered = [word for word in words if word.lower() not in {"and", "the", "of"}]
    words = filtered or words
    if len(words) == 1:
        return words[0][: min(2, max_chars)].upper()
    return "".join(word[0].upper() for word in words[:max_chars])


def neighborhood_placeholder_icon(name: str) -> str:
    if name in NEIGHBORHOOD_PLACEHOLDER_ICON_MAP:
        return NEIGHBORHOOD_PLACEHOLDER_ICON_MAP[name]
    return placeholder_icon_text(name, max_chars=3)


def neighborhood_icon_tone(name: str) -> str:
    slug = normalize_slug(public_neighborhood_name(name))
    return NEIGHBORHOOD_ICON_TONES.get(slug, "civic")


def public_neighborhood_name(name: str) -> str:
    cleaned = " ".join((name or "").split())
    if not cleaned:
        return "Des Moines"
    return PUBLIC_NEIGHBORHOOD_LABELS.get(cleaned.lower(), cleaned)


def clean_public_time_phrase(value: str) -> str:
    cleaned = " ".join((value or "").split()).strip()
    cleaned = re.sub(r"(?i)\b(a\.?m\.?|p\.?m\.?)\b", lambda match: match.group(0).replace(".", "").upper(), cleaned)
    return cleaned


def time_label_has_explicit_window(value: str) -> bool:
    lower_value = (value or "").lower()
    return bool(
        re.search(r"\b\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?)\b", lower_value)
        or "all day" in lower_value
        or "all night" in lower_value
        or "after " in lower_value
        or "until " in lower_value
    )


def derived_day_label(deal: models.Deal, day_label: str, *, weekend_prefix: Optional[str] = None) -> str:
    category = weekly_content.site_deal_category(deal).lower()
    day_part = weekly_content.site_deal_day_part(deal)
    prefix = weekend_prefix or day_label

    if category in {"brunch buffet", "breakfast buffet", "brunch drink special"}:
        return f"{prefix} brunch"
    if category == "buffet":
        return f"{prefix} buffet"
    if category == "lunch special":
        return f"{day_label} lunch"
    if category == "family dining special":
        return "Family special"
    if category == "late night drink special" or day_part == "Late Night":
        return f"{day_label} late night"
    if weekly_content.site_deal_is_happy_hour(deal) and day_part != "Late Night":
        return f"{day_label} happy hour"
    if category == "dinner special" or day_part == "Dinner":
        return f"{prefix} dinner"
    return f"{day_label} special" if weekend_prefix is None else f"{prefix} special"


def format_weekly_master_time_label(
    deal: models.Deal,
    *,
    context: str = "mixed",
) -> str:
    raw_label = clean_public_time_phrase(weekly_content.site_deal_time_label(deal))
    if not raw_label:
        return "Time to be announced"

    day_code = weekly_content.site_deal_day_code(deal)
    day_label = WEEKDAY_LONG.get(day_code, day_code)
    category = weekly_content.site_deal_category(deal).lower()
    lower_label = raw_label.lower()

    exact_label_map = {
        "family dining special": "Family special",
        "weekend dinner special": "Weekend dinner",
        "weekend brunch buffet": "Weekend brunch",
        "weekend breakfast buffet": "Weekend breakfast",
        "all day plus after 5 pm": "All day + after 5 PM",
        "lunch and dinner": "Lunch + dinner",
        "all night thursday": "Thursday night",
    }
    normalized_label = exact_label_map.get(lower_label, raw_label)
    if normalized_label != raw_label:
        raw_label = normalized_label
        lower_label = normalized_label.lower()

    if lower_label.endswith(" special, seasonal"):
        raw_label = clean_public_time_phrase(raw_label.split(",", 1)[0])
        lower_label = raw_label.lower()

    if re.fullmatch(r"(monday|tuesday|wednesday|thursday|friday|saturday|sunday) specials", lower_label):
        raw_label = clean_public_time_phrase(raw_label[:-1])
        lower_label = raw_label.lower()

    if time_label_has_explicit_window(raw_label):
        if category == "lunch special":
            lunch_prefix = "Lunch" if context == "today" else f"{day_label} lunch"
            return f"{lunch_prefix}, {raw_label}"
        return raw_label

    generic_special_labels = {
        f"{day_label.lower()} special",
        "weekend special",
    }
    if lower_label in generic_special_labels:
        weekend_prefix = "Weekend" if lower_label.startswith("weekend") else None
        return derived_day_label(deal, day_label, weekend_prefix=weekend_prefix)

    return raw_label

app = FastAPI(title="DSM Deals MVP")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def require_admin(x_admin_key: Optional[str] = Header(None)):
    if x_admin_key != config.ADMIN_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return True


def get_venue_or_404(db: Session, venue_id: int) -> models.Venue:
    venue = db.get(models.Venue, venue_id)
    if venue is None:
        raise HTTPException(status_code=404, detail="Venue not found")
    return venue


def get_venue_by_slug_or_404(db: Session, slug: str) -> models.Venue:
    normalized = normalize_slug(slug)
    venue = db.query(models.Venue).filter(func.lower(models.Venue.slug) == normalized).first()
    if venue is None:
        raise HTTPException(status_code=404, detail="Venue not found")
    return venue


def get_deal_or_404(db: Session, deal_id: int) -> models.Deal:
    deal = db.get(models.Deal, deal_id)
    if deal is None:
        raise HTTPException(status_code=404, detail="Deal not found")
    return deal


def get_deal_with_venue_or_404(db: Session, deal_id: int) -> models.Deal:
    deal = (
        db.query(models.Deal)
        .options(joinedload(models.Deal.venue))
        .filter(models.Deal.id == deal_id)
        .first()
    )
    if deal is None:
        raise HTTPException(status_code=404, detail="Deal not found")
    return deal


def ensure_owner_exists(db: Session, owner_id: Optional[int]) -> None:
    if owner_id is None:
        return
    if db.get(models.BusinessOwner, owner_id) is None:
        raise HTTPException(status_code=400, detail="Owner not found")


def ensure_unique_venue_slug(db: Session, slug: str, exclude_id: Optional[int] = None) -> str:
    normalized = normalize_slug(slug)
    query = db.query(models.Venue).filter(func.lower(models.Venue.slug) == normalized)
    if exclude_id is not None:
        query = query.filter(models.Venue.id != exclude_id)
    if query.first():
        raise HTTPException(status_code=400, detail="Venue slug already exists")
    return normalized


def enforce_weekly_cap(db: Session, venue_id: int, exclude_id: Optional[int] = None) -> None:
    counted_statuses = [models.Status.draft, models.Status.queued, models.Status.live]
    query = db.query(models.Deal).filter(
        models.Deal.venue_id == venue_id,
        models.Deal.type == models.DealType.weekly,
        models.Deal.status.in_(counted_statuses),
    )
    if exclude_id is not None:
        query = query.filter(models.Deal.id != exclude_id)
    if query.count() >= config.WEEKLY_PATTERNS_CAP_PER_VENUE:
        raise HTTPException(
            status_code=400,
            detail=f"Weekly pattern cap reached ({config.WEEKLY_PATTERNS_CAP_PER_VENUE})",
        )


def validate_weekly_range(start_time: str, end_time: str) -> None:
    if parse_hhmm(end_time) <= parse_hhmm(start_time):
        raise HTTPException(status_code=400, detail="end_time must be after start_time")


def validate_last_minute_range(start_at: Optional[datetime], end_at: Optional[datetime]) -> None:
    if start_at is None or end_at is None:
        raise HTTPException(status_code=400, detail="start_at and end_at are required")
    if end_at <= start_at:
        raise HTTPException(status_code=400, detail="end_at must be after start_at")
    if duration_hours(start_at, end_at) > config.LAST_MINUTE_MAX_HOURS:
        raise HTTPException(
            status_code=400,
            detail=f"Last-minute deals max {config.LAST_MINUTE_MAX_HOURS} hours",
        )


def validate_deal_shape(deal_type: models.DealType, values: dict) -> None:
    if deal_type == models.DealType.weekly:
        if not values.get("weekday_pattern") or not values.get("start_time") or not values.get("end_time"):
            raise HTTPException(
                status_code=400,
                detail="Weekly deals require weekday_pattern, start_time, and end_time",
            )
        validate_weekly_range(values["start_time"], values["end_time"])
        return

    validate_last_minute_range(values.get("start_at"), values.get("end_at"))


def normalize_live_status_for_time(deal: models.Deal, now: Optional[datetime] = None) -> None:
    now = now or datetime.utcnow()
    if (
        deal.type == models.DealType.last_minute and
        deal.status == models.Status.live and
        deal.end_at is not None and
        deal.end_at < now
    ):
        deal.status = models.Status.expired


def expire_stale_last_minute_deals(db: Session) -> int:
    now = datetime.utcnow()
    stale_deals = (
        db.query(models.Deal)
        .filter(
            models.Deal.status == models.Status.live,
            models.Deal.type == models.DealType.last_minute,
            models.Deal.end_at.is_not(None),
            models.Deal.end_at < now,
        )
        .all()
    )
    for deal in stale_deals:
        deal.status = models.Status.expired
        deal.updated_at = now
    if stale_deals:
        db.commit()
    return len(stale_deals)


def load_public_deals(db: Session) -> List[models.Deal]:
    expire_stale_last_minute_deals(db)
    return (
        db.query(models.Deal)
        .options(joinedload(models.Deal.venue))
        .filter(models.Deal.status == models.Status.live)
        .order_by(models.Deal.created_at.desc())
        .all()
    )


def sort_public_deals(deals: List[models.Deal], window_start: datetime) -> List[models.Deal]:
    return sorted(
        deals,
        key=lambda deal: (
            next_occurrence_start(deal, window_start, horizon_days=7) or datetime.max,
            deal.title.lower(),
        ),
    )


def compute_sort_key(
    deal: models.Deal,
    now: datetime,
    selected_neighborhood: Optional[str],
    user_lat: Optional[float],
    user_lng: Optional[float],
) -> float:
    venue = deal.venue
    weights = config.SORT_WEIGHTS
    score = 0.0

    if deal.type == models.DealType.last_minute:
        if deal.end_at and deal.end_at > now:
            minutes_left = (deal.end_at - now).total_seconds() / 60.0
            score += weights["expiringSoon"] * (1.0 / max(minutes_left, 1))
    elif deal_is_live_now(deal, now):
        _, end_dt = build_day_window(now, deal.start_time, deal.end_time)
        minutes_left = (end_dt - now).total_seconds() / 60.0
        score += weights["expiringSoon"] * (1.0 / max(minutes_left, 1))

    if selected_neighborhood and venue.neighborhood:
        if venue.neighborhood.strip().lower() == selected_neighborhood.strip().lower():
            score += weights["neighborhoodPriority"]

    if user_lat is not None and user_lng is not None and venue.lat is not None and venue.lng is not None:
        dist = distance_miles(user_lat, user_lng, venue.lat, venue.lng)
        if dist is not None and dist >= 0:
            score += weights["distance"] * (1.0 / (1.0 + dist))

    if config.FEATURED_ENABLED and deal.sponsored:
        score += weights["featuredBoost"]

    if (now - deal.created_at).total_seconds() <= 3600:
        score += weights["freshnessBump"]

    return score


def apply_deal_update(db: Session, deal: models.Deal, payload: schemas.DealUpdate) -> None:
    values = payload.model_dump(exclude_unset=True)
    if not values:
        return

    for field in ("venue_id", "title", "short_description", "status"):
        if field in values and values[field] is None:
            raise HTTPException(status_code=400, detail=f"{field} cannot be null")

    if "venue_id" in values:
        get_venue_or_404(db, values["venue_id"])

    if deal.type == models.DealType.weekly:
        if any(field in values and values[field] is not None for field in ("start_at", "end_at")):
            raise HTTPException(
                status_code=400,
                detail="Weekly deals use weekday_pattern, start_time, and end_time",
            )
    else:
        if any(field in values and values[field] is not None for field in ("weekday_pattern", "start_time", "end_time")):
            raise HTTPException(
                status_code=400,
                detail="Last-minute deals use start_at and end_at",
            )

    merged = {
        "venue_id": values.get("venue_id", deal.venue_id),
        "weekday_pattern": values.get("weekday_pattern", deal.weekday_pattern),
        "start_time": values.get("start_time", deal.start_time),
        "end_time": values.get("end_time", deal.end_time),
        "start_at": values.get("start_at", deal.start_at),
        "end_at": values.get("end_at", deal.end_at),
        "status": values.get("status", deal.status),
    }
    validate_deal_shape(deal.type, merged)

    if deal.type == models.DealType.weekly and merged["status"] in {
        models.Status.draft,
        models.Status.queued,
        models.Status.live,
    }:
        enforce_weekly_cap(db, merged["venue_id"], exclude_id=deal.id)

    for field, value in values.items():
        setattr(deal, field, value)

    normalize_live_status_for_time(deal)
    deal.updated_at = datetime.utcnow()


def load_curated_site_deals(db: Session) -> List[models.Deal]:
    del db
    return weekly_content.load_weekly_master_deals()


def weekday_pattern_parts(pattern: Optional[str]) -> List[str]:
    if not pattern:
        return []
    if pattern == "All":
        return DAY_ORDER.copy()
    return [part.strip() for part in pattern.split(",") if part.strip()]


def deal_matches_day_code(deal: models.Deal, day_code: str) -> bool:
    return weekly_content.site_deal_matches_day_code(deal, day_code)


def homepage_sections(db: Session) -> dict[str, List[models.Deal]]:
    del db
    return weekly_content.homepage_sections(datetime.now(HOME_TIMEZONE).replace(tzinfo=None))


def homepage_metadata(deal: models.Deal) -> dict:
    if not deal.notes_private:
        return {}
    try:
        payload = json.loads(deal.notes_private)
    except (TypeError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def weekday_pattern_label(pattern: Optional[str]) -> str:
    if not pattern:
        return "Weekly"
    if pattern == "All":
        return "Daily"

    parts = [WEEKDAY_LONG.get(part.strip(), part.strip()) for part in pattern.split(",") if part.strip()]
    if not parts:
        return "Weekly"
    if len(parts) == 1:
        return parts[0]
    if len(parts) == 2:
        return f"{parts[0]} and {parts[1]}"
    return ", ".join(parts[:-1]) + f", and {parts[-1]}"


def deal_is_live_homepage(deal: models.Deal, now: datetime) -> bool:
    meta = homepage_metadata(deal)
    if meta.get("site_source") == "weekly_master":
        return weekly_content.site_deal_is_live_now(deal, now)
    if deal.status != models.Status.live:
        return False
    if deal.type == models.DealType.last_minute:
        return bool(deal.start_at and deal.end_at and deal.start_at <= now <= deal.end_at)
    if not deal.weekday_pattern or not matches_weekday_pattern(deal.weekday_pattern, now):
        return False
    if deal.start_time and deal.end_time:
        start_dt, end_dt = build_day_window(now, deal.start_time, deal.end_time)
        return start_dt <= now <= end_dt
    return True


def deal_is_tonight_homepage(deal: models.Deal, window_start: datetime, window_end: datetime) -> bool:
    if deal.status != models.Status.live:
        return False
    if deal.type == models.DealType.last_minute:
        return bool(
            deal.start_at and
            deal.end_at and
            deal.end_at >= window_start and
            deal.start_at <= window_end
        )
    if not deal.weekday_pattern or not matches_weekday_pattern(deal.weekday_pattern, window_start):
        return False
    if deal.start_time and deal.end_time:
        start_dt, end_dt = build_day_window(window_start, deal.start_time, deal.end_time)
        return end_dt >= window_start and start_dt <= window_end
    return True


def deal_is_upcoming_homepage(deal: models.Deal, window_start: datetime, window_end: datetime) -> bool:
    if deal.status != models.Status.live:
        return False
    if deal.type == models.DealType.last_minute:
        return bool(
            deal.start_at and
            deal.end_at and
            deal.end_at >= window_start and
            deal.start_at <= window_end
        )

    total_days = max((window_end.date() - window_start.date()).days, 0)
    for offset in range(total_days + 1):
        day_ref = (window_start + timedelta(days=offset)).replace(
            hour=12,
            minute=0,
            second=0,
            microsecond=0,
        )
        if not deal.weekday_pattern or not matches_weekday_pattern(deal.weekday_pattern, day_ref):
            continue
        if deal.start_time and deal.end_time:
            start_dt, end_dt = build_day_window(day_ref, deal.start_time, deal.end_time)
            if end_dt >= window_start and start_dt <= window_end:
                return True
        else:
            return True
    return False


def homepage_occurrence_key(deal: models.Deal, window_start: datetime) -> datetime:
    if deal.type == models.DealType.last_minute and deal.start_at:
        return max(deal.start_at, window_start)

    for offset in range(8):
        day_ref = (window_start + timedelta(days=offset)).replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
        midday = day_ref.replace(hour=12)
        if not deal.weekday_pattern or not matches_weekday_pattern(deal.weekday_pattern, midday):
            continue
        if deal.start_time and deal.end_time:
            start_dt, _ = build_day_window(day_ref, deal.start_time, deal.end_time)
            if start_dt < window_start:
                return window_start
            return start_dt
        return max(day_ref, window_start)

    return datetime.max


def sort_homepage_deals(deals: List[models.Deal], window_start: datetime) -> List[models.Deal]:
    if deals and homepage_metadata(deals[0]).get("site_source") == "weekly_master":
        return weekly_content.sort_site_deals(deals, window_start)
    return sorted(
        deals,
        key=lambda deal: (
            homepage_occurrence_key(deal, window_start),
            homepage_metadata(deal).get("rank", 999),
            0 if deal.start_time else 1,
            deal.title.lower(),
        ),
    )


def format_clock(hour: int, minute: int) -> str:
    pattern = "%I %p" if minute == 0 else "%I:%M %p"
    return datetime(2000, 1, 1, hour, minute).strftime(pattern).lstrip("0")


def format_time_string(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    hour, minute = parse_hhmm(value)
    return format_clock(hour, minute)


def format_date_label(value: datetime, reference: datetime) -> str:
    if value.date() == reference.date():
        return "Today"
    tomorrow = reference.date() + timedelta(days=1)
    if value.date() == tomorrow:
        return "Tomorrow"
    return f"{value.strftime('%a, %b')} {value.day}"


def format_deal_time(deal: models.Deal, reference: datetime) -> str:
    meta = homepage_metadata(deal)
    if meta.get("site_source") == "weekly_master":
        return format_weekly_master_time_label(deal, context="mixed")
    if meta.get("time_label"):
        return str(meta["time_label"])

    if deal.type == models.DealType.last_minute and deal.start_at and deal.end_at:
        day_label = format_date_label(deal.start_at, reference)
        return f"{day_label} · {format_clock(deal.start_at.hour, deal.start_at.minute)} - {format_clock(deal.end_at.hour, deal.end_at.minute)}"

    if deal.start_time and deal.end_time:
        weekday_label = weekday_pattern_label(deal.weekday_pattern)
        return f"{weekday_label} · {format_time_string(deal.start_time)} - {format_time_string(deal.end_time)}"

    if deal.weekday_pattern:
        label = weekday_pattern_label(deal.weekday_pattern)
        if deal.weekday_pattern == "All":
            return "Daily special"
        if "," in deal.weekday_pattern:
            return label
        return f"{label} special"

    return "Time to be announced"


def format_today_pick_time(deal: models.Deal, reference: datetime) -> str:
    meta = homepage_metadata(deal)
    if meta.get("site_source") == "weekly_master":
        return format_weekly_master_time_label(deal, context="today")
    if deal.type == models.DealType.last_minute and deal.start_at and deal.end_at:
        return f"Today, {format_clock(deal.start_at.hour, deal.start_at.minute)} to {format_clock(deal.end_at.hour, deal.end_at.minute)}"

    text = f"{deal.title} {deal.short_description}".lower()
    start_label = format_time_string(deal.start_time)
    end_label = format_time_string(deal.end_time)

    if start_label and end_label:
        if "lunch" in text:
            return f"Lunch, {start_label} to {end_label}"
        return f"Today, {start_label} to {end_label}"
    if start_label:
        return f"After {start_label}"
    return "Today"


def format_tonight_pick_time(deal: models.Deal, reference: datetime) -> str:
    if deal.type == models.DealType.last_minute and deal.start_at and deal.end_at:
        return f"Tonight, {format_clock(deal.start_at.hour, deal.start_at.minute)} to {format_clock(deal.end_at.hour, deal.end_at.minute)}"

    start_label = format_time_string(deal.start_time)
    end_label = format_time_string(deal.end_time)

    if start_label and end_label:
        return f"{start_label} to {end_label}"
    if start_label:
        return f"After {start_label}"
    return "Tonight"


def format_day_page_time(deal: models.Deal, day_code: str) -> str:
    meta = homepage_metadata(deal)
    if meta.get("site_source") == "weekly_master":
        return format_weekly_master_time_label(deal, context="day")
    day_label = WEEKDAY_LONG.get(day_code, day_code)
    text = f"{deal.title} {deal.short_description}".lower()

    if deal.type == models.DealType.last_minute and deal.start_at and deal.end_at:
        return f"{day_label}, {format_clock(deal.start_at.hour, deal.start_at.minute)} to {format_clock(deal.end_at.hour, deal.end_at.minute)}"

    start_label = format_time_string(deal.start_time)
    end_label = format_time_string(deal.end_time)
    pattern_parts = weekday_pattern_parts(deal.weekday_pattern)

    if day_code in {"Sat", "Sun"} and set(pattern_parts) == {"Sat", "Sun"}:
        if "buffet" in text:
            return "Weekend buffet"
        if "brunch" in text:
            return "Weekend brunch"

    if start_label and end_label:
        if "lunch" in text:
            return f"{day_label} lunch, {start_label} to {end_label}"
        return f"{day_label}, {start_label} to {end_label}"
    if start_label:
        return f"After {start_label}"
    if len(pattern_parts) == 1:
        if "brunch" in text:
            return f"{day_label} brunch"
        if "buffet" in text:
            return f"{day_label} buffet"
        return f"{day_label} special"
    return f"{day_label} special"


def deal_icon(deal: models.Deal) -> str:
    return deal_icon_meta(deal)[1]


def deal_icon_meta(deal: models.Deal) -> tuple[str, str]:
    text = f"{deal.title} {deal.short_description}".lower()
    for icon_key, icon, keywords in DEAL_ICON_KEYWORDS:
        for keyword in keywords:
            if re.search(rf"\b{re.escape(keyword)}\b", text):
                return icon_key, icon
    return "featured", "✨"


def render_icon_badge(
    glyph: str,
    *,
    kind: str,
    size: str,
    icon_key: str,
    extra_class: str,
) -> str:
    class_names = f"icon-badge icon-badge-{escape(size)} {escape(extra_class)}".strip()
    return (
        f'<span class="{class_names}" data-icon-kind="{escape(kind)}" '
        f'data-icon-key="{escape(icon_key)}" aria-hidden="true">'
        f'<span class="icon-badge-glyph">{escape(glyph)}</span>'
        f"</span>"
    )


def render_brand_slot(current_page: str) -> str:
    home_href = site_href("/")
    if current_page == "home":
        return ""
    return f"""
    <a class="brand-slot brand-slot-compact" href="{home_href}" aria-label="DSM Deals Hub home">
      {render_icon_badge(BRAND_PLACEHOLDER_GLYPH, kind="brand", size="brand", icon_key="dsm-deals-hub", extra_class="brand-mark")}
      <span class="brand-wordmark">
        <span class="brand-kicker">DSM</span>
        <span class="brand-name">Deals Hub</span>
        <span class="brand-meta">Des Moines dining guide</span>
      </span>
    </a>
    """


def render_brand_heading(title: str) -> str:
    home_href = site_href("/")
    brand_title = "Deals Hub" if title == "DSM Deals Hub" else title
    return f"""
    <h1>
      <a class="brand-slot brand-slot-heading" href="{home_href}" aria-label="DSM Deals Hub home">
        {render_icon_badge(BRAND_PLACEHOLDER_GLYPH, kind="brand", size="brand-hero", icon_key="dsm-deals-hub", extra_class="brand-mark")}
        <span class="brand-wordmark brand-wordmark-heading">
          <span class="brand-kicker">DSM</span>
          <span class="brand-name">{escape(brand_title)}</span>
          <span class="brand-meta brand-meta-heading">Des Moines dining guide</span>
        </span>
      </a>
    </h1>
    """


def section_neighborhoods(deals: List[models.Deal]) -> List[str]:
    names = {
        deal.venue.neighborhood.strip()
        for deal in deals
        if deal.venue and deal.venue.neighborhood and deal.venue.neighborhood.strip()
    }
    return sorted(names)


def render_section_sort_controls(section_id: str, deals: List[models.Deal]) -> str:
    if section_id not in {"tonight", "this-week"} or not deals:
        return ""

    neighborhoods = section_neighborhoods(deals)
    if not neighborhoods:
        return ""

    buttons = [
        '<button type="button" class="neighborhood-chip is-selected" data-neighborhood-chip="All" aria-pressed="true">All</button>'
    ]
    for neighborhood in neighborhoods:
        label = escape(neighborhood)
        buttons.append(
            f'<button type="button" class="neighborhood-chip" data-neighborhood-chip="{label}" aria-pressed="false">{label}</button>'
        )

    return f"""
    <div class="section-tools" data-neighborhood-sort="true">
      <p class="section-helper">Sort by neighborhood. Your pick moves to the top, but every deal stays in view.</p>
      <div class="neighborhood-chip-row" aria-label="{escape(section_id.replace('-', ' ').title())} neighborhood sort">
        {''.join(buttons)}
      </div>
    </div>
    """


def render_deal_card(
    deal: models.Deal,
    reference: datetime,
    order_index: int,
    time_label_override: Optional[str] = None,
) -> str:
    venue_name = escape(deal.venue.name if deal.venue else "Venue")
    title = escape(deal.title)
    description = escape(deal.short_description)
    time_label = escape(time_label_override or format_deal_time(deal, reference))
    icon_key, icon = deal_icon_meta(deal)
    neighborhood_name = deal.venue.neighborhood.strip() if deal.venue and deal.venue.neighborhood else ""
    neighborhood_attr = escape(neighborhood_name) if neighborhood_name else ""
    lat_attr = ""
    lng_attr = ""
    if deal.venue and deal.venue.lat is not None and deal.venue.lng is not None:
        lat_attr = str(deal.venue.lat)
        lng_attr = str(deal.venue.lng)
    neighborhood = ""
    if neighborhood_name:
        neighborhood = f'<span class="deal-chip">{escape(neighborhood_name)}</span>'

    return f"""
    <article class="deal-card" data-neighborhood="{neighborhood_attr}" data-lat="{lat_attr}" data-lng="{lng_attr}" data-order="{order_index}">
      <div class="deal-card-top">
        <p class="deal-venue">{venue_name}</p>
        {render_icon_badge(icon, kind="category", size="sm", icon_key=icon_key, extra_class="deal-icon")}
      </div>
      <h3>{title}</h3>
      <p class="deal-description">{description}</p>
      <div class="deal-card-footer">
        <p class="deal-time">{time_label}</p>
        {neighborhood}
      </div>
    </article>
    """


def render_section(
    section_id: str,
    title: str,
    intro: str,
    deals: List[models.Deal],
    empty_message: str,
    reference: datetime,
    time_labels: Optional[List[str]] = None,
    action_label: Optional[str] = None,
    action_href: Optional[str] = None,
    section_class: str = "content-section",
    panel_class: str = "section-panel",
    kicker_label: Optional[str] = None,
) -> str:
    grid_class = "deal-grid"
    controls = render_section_sort_controls(section_id, deals)
    if deals:
        cards = "\n".join(
            render_deal_card(
                deal,
                reference,
                index,
                time_labels[index] if time_labels and index < len(time_labels) else None,
            )
            for index, deal in enumerate(deals)
        )
    elif section_id == "today-picks":
        cards = render_today_empty_state()
        grid_class = "deal-grid deal-grid-empty"
    elif section_id == "today-preview":
        cards = render_today_preview_empty_card()
        grid_class = "deal-grid deal-grid-empty"
    else:
        grid_class = "deal-grid deal-grid-empty"
        cards = f'<div class="empty-state">{escape(empty_message)}</div>'

    count_label = f"{len(deals)} deal" if len(deals) == 1 else f"{len(deals)} deals"
    action_html = ""
    if action_label and action_href:
        action_html = f'<a class="section-action" href="{action_href}">{escape(action_label)}</a>'

    return f"""
    <section id="{section_id}" class="{escape(section_class)}">
      <div class="{escape(panel_class)}">
        <div class="section-heading">
          <div>
            <p class="section-kicker">{escape(kicker_label or title)}</p>
            <div class="section-title-row">
              <h2>{escape(title)}</h2>
              <span class="section-count">{escape(count_label)}</span>
            </div>
          </div>
          <div class="section-heading-side">
            <p>{escape(intro)}</p>
            {action_html}
          </div>
        </div>
        {controls}
        <div class="{grid_class}" data-sort-grid="{escape(section_id)}">
          {cards}
        </div>
      </div>
    </section>
    """


def render_homepage_script() -> str:
    centers_json = json.dumps(NEIGHBORHOOD_CENTERS)
    return f"""
    <script>
      (() => {{
        const neighborhoodCenters = {centers_json};

        const normalize = (value) => (value || "").trim().toLowerCase();

        const distanceMiles = (pointA, pointB) => {{
          const toRadians = (value) => (value * Math.PI) / 180;
          const earthRadiusMiles = 3958.8;
          const dLat = toRadians(pointB.lat - pointA.lat);
          const dLng = toRadians(pointB.lng - pointA.lng);
          const lat1 = toRadians(pointA.lat);
          const lat2 = toRadians(pointB.lat);
          const a =
            Math.sin(dLat / 2) * Math.sin(dLat / 2) +
            Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLng / 2) * Math.sin(dLng / 2);
          const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
          return earthRadiusMiles * c;
        }};

        const centerForNeighborhood = (name) => {{
          const match = Object.entries(neighborhoodCenters).find(([key]) => normalize(key) === normalize(name));
          if (!match) {{
            return null;
          }}
          return {{ lat: match[1][0], lng: match[1][1] }};
        }};

        const pointForCard = (card) => {{
          const lat = Number.parseFloat(card.dataset.lat || "");
          const lng = Number.parseFloat(card.dataset.lng || "");
          if (!Number.isNaN(lat) && !Number.isNaN(lng)) {{
            return {{ lat, lng }};
          }}
          return centerForNeighborhood(card.dataset.neighborhood || "");
        }};

        document.querySelectorAll("[data-neighborhood-sort='true']").forEach((tools) => {{
          const section = tools.closest(".content-section");
          const grid = section?.querySelector("[data-sort-grid]");
          const cards = Array.from(grid?.querySelectorAll(".deal-card") || []);
          const chips = Array.from(tools.querySelectorAll("[data-neighborhood-chip]"));

          if (!section || !grid || !cards.length || !chips.length) {{
            return;
          }}

          const originalCards = cards
            .map((card, index) => ({{
              card,
              order: Number.parseInt(card.dataset.order || String(index), 10),
            }}))
            .sort((left, right) => left.order - right.order);

          let selectedNeighborhood = "All";

          const syncChips = () => {{
            chips.forEach((chip) => {{
              const isSelected = chip.dataset.neighborhoodChip === selectedNeighborhood;
              chip.classList.toggle("is-selected", isSelected);
              chip.setAttribute("aria-pressed", isSelected ? "true" : "false");
            }});
          }};

          const sortCards = () => {{
            if (selectedNeighborhood === "All") {{
              originalCards.forEach((entry) => grid.appendChild(entry.card));
              return;
            }}

            const selectedCenter = centerForNeighborhood(selectedNeighborhood);
            const sorted = [...originalCards].sort((left, right) => {{
              const leftNeighborhood = left.card.dataset.neighborhood || "";
              const rightNeighborhood = right.card.dataset.neighborhood || "";
              const leftExact = normalize(leftNeighborhood) === normalize(selectedNeighborhood) ? 0 : 1;
              const rightExact = normalize(rightNeighborhood) === normalize(selectedNeighborhood) ? 0 : 1;
              if (leftExact !== rightExact) {{
                return leftExact - rightExact;
              }}

              const leftPoint = pointForCard(left.card);
              const rightPoint = pointForCard(right.card);
              const leftHasDistance = selectedCenter && leftPoint ? 0 : 1;
              const rightHasDistance = selectedCenter && rightPoint ? 0 : 1;
              if (leftHasDistance !== rightHasDistance) {{
                return leftHasDistance - rightHasDistance;
              }}

              if (selectedCenter && leftPoint && rightPoint) {{
                const distanceDiff = distanceMiles(selectedCenter, leftPoint) - distanceMiles(selectedCenter, rightPoint);
                if (Math.abs(distanceDiff) > 0.01) {{
                  return distanceDiff;
                }}
              }}

              return left.order - right.order;
            }});

            sorted.forEach((entry) => grid.appendChild(entry.card));
          }};

          chips.forEach((chip) => {{
            chip.addEventListener("click", () => {{
              const nextNeighborhood = chip.dataset.neighborhoodChip || "All";
              if (nextNeighborhood === selectedNeighborhood || nextNeighborhood === "All") {{
                selectedNeighborhood = "All";
              }} else {{
                selectedNeighborhood = nextNeighborhood;
              }}
              syncChips();
              sortCards();
            }});
          }});

          syncChips();
          sortCards();
        }});
      }})();
    </script>
    """


def render_site_nav(current_page: str) -> str:
    links = [
        ("Today", site_href("/today"), current_page == "today"),
        ("Neighborhoods", site_href("/neighborhoods"), current_page == "neighborhoods"),
        ("Days", site_href("/days"), current_page == "days"),
        ("For Venues", site_href("/for-venues"), current_page == "for-venues"),
    ]
    items = []
    for label, href, active in links:
        current_attr = ' aria-current="page"' if active else ""
        items.append(f'<a href="{href}"{current_attr}>{escape(label)}</a>')
    return f"""
    <nav class="section-nav site-nav" aria-label="Site navigation">
      {''.join(items)}
    </nav>
    """


def render_site_brand(current_page: str) -> str:
    return render_brand_slot(current_page)


def render_today_empty_state() -> str:
    return f"""
    <div class="empty-state empty-state-featured">
      <div class="live-empty-copy">
        <p class="live-empty-kicker">Today</p>
        <h3>Nothing is lined up for today yet.</h3>
        <p>
          The guide leans on recurring weekday boards and weekend patterns. Browse by neighborhood, jump into the days guide, or check back when the daily rotation changes.
        </p>
      </div>
      <div class="live-empty-actions">
        <div class="empty-state-links">
          <a href="{site_href("/neighborhoods")}" class="empty-state-link">Browse Neighborhoods</a>
          <a href="{site_href("/days")}" class="empty-state-link">Open Days</a>
        </div>
      </div>
    </div>
    """


def render_today_preview_empty_card() -> str:
    return render_browse_card(
        "Today is quiet right now",
        site_href("/today"),
        "📆",
        "Open the full Today page, or browse Neighborhoods and Days while the weekly board resets.",
        pills=["No specials today"],
        cta_label="Open Today",
        variant="day",
    )


def render_page_document(
    meta_title: str,
    meta_description: str,
    hero_eyebrow: str,
    hero_title: str,
    hero_text: str,
    current_page: str,
    main_content: str,
    utility_html: str = "",
    hero_class: str = "hero",
    include_sort_script: bool = False,
) -> str:
    sort_script = render_homepage_script() if include_sort_script else ""
    title_html = render_brand_heading(hero_title) if current_page == "home" else f"<h1>{escape(hero_title)}</h1>"
    return f"""<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>{escape(meta_title)}</title>
    <meta
      name="description"
      content="{escape(meta_description)}"
    />
    <link rel="icon" type="image/svg+xml" href="{site_href("/static/favicon.svg")}" />
    <link rel="stylesheet" href="{site_href("/static/styles.css")}" />
  </head>
  <body>
    <div class="page-shell">
      <header class="{hero_class}">
        <div class="hero-panel">
          <div class="hero-copy">
            {render_site_brand(current_page)}
            <p class="eyebrow">{escape(hero_eyebrow)}</p>
            {title_html}
            <p class="hero-text">
              {escape(hero_text)}
            </p>
          </div>
          <div class="hero-utility">
            {render_site_nav(current_page)}
            {utility_html}
          </div>
        </div>
      </header>

      <main class="content">
        {main_content}
      </main>

      <footer class="site-footer">
        DSM Deals Hub is curated manually from local posts and business updates.
      </footer>
    </div>
    {sort_script}
  </body>
</html>
"""


def render_browse_card(
    title: str,
    href: str,
    icon: str,
    description: str,
    pills: Optional[List[str]] = None,
    cta_label: str = "Open",
    variant: str = "browse",
    icon_kind: Optional[str] = None,
    icon_key: Optional[str] = None,
    icon_extra_class: str = "",
) -> str:
    pills_html = "".join(
        f'<span class="browse-card-pill">{escape(pill)}</span>'
        for pill in (pills or [])
    )
    return f"""
    <a class="browse-card browse-card-{escape(variant)}" href="{href}">
      <div class="browse-card-top">
        {render_icon_badge(icon, kind=icon_kind or variant, size="lg", icon_key=icon_key or normalize_slug(title), extra_class=f"browse-card-icon {icon_extra_class}".strip())}
        <div class="browse-card-pills">{pills_html}</div>
      </div>
      <div class="browse-card-body">
        <h3>{escape(title)}</h3>
        <p class="browse-card-copy">{escape(description)}</p>
      </div>
      <div class="browse-card-footer">
        <span class="browse-card-cta">{escape(cta_label)}</span>
        <span class="browse-card-arrow" aria-hidden="true">+</span>
      </div>
    </a>
    """


def render_link_grid_section(
    section_id: str,
    title: str,
    intro: str,
    cards_html: str,
    action_label: str,
    action_href: str,
    grid_class: str = "browse-card-grid",
    section_class: str = "content-section",
    panel_class: str = "section-panel",
    kicker_label: Optional[str] = None,
) -> str:
    return f"""
    <section id="{section_id}" class="{escape(section_class)}">
      <div class="{escape(panel_class)}">
        <div class="section-heading">
          <div>
            <p class="section-kicker">{escape(kicker_label or title)}</p>
            <div class="section-title-row">
              <h2>{escape(title)}</h2>
            </div>
          </div>
          <div class="section-heading-side">
            <p>{escape(intro)}</p>
            <a class="section-action" href="{action_href}">{escape(action_label)}</a>
          </div>
        </div>
        <div class="{grid_class}">
          {cards_html}
        </div>
      </div>
    </section>
    """


def render_live_now_module(deals: List[models.Deal], reference: datetime) -> str:
    preview_cards = "\n".join(
        render_deal_card(deal, reference, index, format_today_pick_time(deal, reference))
        for index, deal in enumerate(deals[:2])
    )
    if deals:
        details_body = f"""
        <div class="live-now-details-grid">
          {preview_cards}
        </div>
        """
    else:
        details_body = f"""
        <div class="live-now-empty">
          <p>No exact-timed specials are active right now. The fuller browse usually lives in Today, Neighborhoods, and Days.</p>
          <div class="empty-state-links">
            <a href="{site_href("/today")}" class="empty-state-link">See Today</a>
            <a href="{site_href("/days")}" class="empty-state-link">Browse Days</a>
          </div>
        </div>
        """

    return f"""
    <section id="live-now" class="content-section content-section-secondary homepage-section homepage-section-live">
      <div class="section-panel section-panel-secondary section-panel-live">
        <details class="live-now-module">
          <summary class="live-now-summary">
            <div class="live-now-summary-copy">
              <span class="live-now-badge"><span class="live-now-dot" aria-hidden="true"></span>Live Now</span>
              <h2>Exact-timed deals land here when a venue gives a real window.</h2>
              <p>Think happy hour, after-5 specials, or one-night timing. For the broader weekly edit, start with Today below.</p>
            </div>
            <div class="live-now-summary-side">
              <span class="section-count">{len(deals)} deals</span>
              <span class="live-now-toggle" aria-hidden="true">+</span>
            </div>
          </summary>
          <div class="live-now-details">
            {details_body}
          </div>
        </details>
      </div>
    </section>
    """


def next_reference_for_day(day_code: str, reference: datetime) -> datetime:
    for offset in range(7):
        candidate = (reference + timedelta(days=offset)).replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
        if candidate.strftime("%a") == day_code:
            return candidate
    return reference


def days_page_sections(db: Session) -> dict[str, List[models.Deal]]:
    del db
    return weekly_content.days_page_sections(datetime.now(HOME_TIMEZONE).replace(tzinfo=None))


def is_happy_hour_deal(deal: models.Deal) -> bool:
    text = f"{deal.title} {deal.short_description}".lower()
    if any(keyword in text for keyword in ("happy hour", "wine", "beer", "pint", "pitcher", "cocktail", "oyster")):
        return True
    if deal.start_time:
        hour, _ = parse_hhmm(deal.start_time)
        return 14 <= hour <= 18
    return False


def today_page_data(db: Session) -> dict:
    del db
    return weekly_content.today_page_data(datetime.now(HOME_TIMEZONE).replace(tzinfo=None))


def neighborhood_groups(db: Session) -> List[dict]:
    groups = weekly_content.neighborhood_groups(datetime.now(HOME_TIMEZONE).replace(tzinfo=None))
    for group in groups:
        group["display_name"] = public_neighborhood_name(group["name"])
        group["icon"] = neighborhood_placeholder_icon(group["display_name"])
    return groups


def get_neighborhood_group_or_404(db: Session, slug: str) -> dict:
    normalized = normalize_slug(slug)
    for group in neighborhood_groups(db):
        if group["slug"] == normalized:
            return group
    raise HTTPException(status_code=404, detail="Neighborhood not found")


def day_code_from_slug_or_404(day_slug: str) -> str:
    normalized = normalize_slug(day_slug)
    for day_code in DAY_ORDER:
        if normalize_slug(WEEKDAY_LONG[day_code]) == normalized:
            return day_code
    raise HTTPException(status_code=404, detail="Day not found")


def render_neighborhood_cards(groups: List[dict], limit: Optional[int] = None, context: str = "default") -> str:
    selected = groups[:limit] if limit is not None else groups
    if not selected:
        return '<div class="empty-state">Neighborhoods will appear here once more areas are added to the guide.</div>'
    return "\n".join(
        render_browse_card(
            item.get("display_name", item["name"]),
            site_href(f"/neighborhoods/{item['slug']}"),
            item["icon"],
            (
                f"Open the current local edit for {item.get('display_name', item['name'])}."
                if context == "homepage"
                else f"Browse the current local guide for {item.get('display_name', item['name'])}."
            ),
            pills=[f"{item['deal_count']} specials", f"{item['venue_count']} venues"],
            cta_label="See neighborhood" if context == "homepage" else "Open neighborhood",
            variant="neighborhood",
            icon_kind="neighborhood",
            icon_key=item["slug"],
            icon_extra_class=f"icon-tone-{neighborhood_icon_tone(item.get('display_name', item['name']))}",
        )
        for item in selected
    )


def day_card_supporting_line(day_code: str, has_deals: bool, *, context: str) -> str:
    day_label = WEEKDAY_LONG[day_code]
    if not has_deals:
        return f"No current specials are listed for {day_label} yet."
    if day_code in {"Sat", "Sun"}:
        return (
            f"Brunch, afternoon stops, dinner plans, and later-night picks for {day_label}."
            if context == "homepage"
            else f"Browse the fuller weekend lineup for {day_label}."
        )
    if day_code == "Fri":
        return (
            "Happy hour, lunch runs, and dinner-led Friday specials."
            if context == "homepage"
            else "Browse Friday lunch, happy hour, and dinner picks."
        )
    return (
        f"Happy hour, lunch, and recurring house specials for {day_label}."
        if context == "homepage"
        else f"Browse the current weekday lineup for {day_label}."
    )


def render_day_cards(sections: dict[str, List[models.Deal]], context: str = "default") -> str:
    return "\n".join(
        render_browse_card(
            WEEKDAY_LONG[day_code],
            site_href(f"/days/{normalize_slug(WEEKDAY_LONG[day_code])}"),
            WEEKDAY_LONG[day_code][:3],
            day_card_supporting_line(day_code, bool(sections[day_code]), context=context),
            pills=[f"{len(sections[day_code])} specials"],
            cta_label="See day" if context == "homepage" else "Open day",
            variant="day",
            icon_kind="day",
            icon_key=day_code.lower(),
        )
        for day_code in DAY_ORDER
    )


def render_day_page_nav(current_day_code: str) -> str:
    links = []
    for day_code in DAY_ORDER:
        current_attr = ' aria-current="page"' if day_code == current_day_code else ""
        links.append(
            f'<a href="{site_href(f"/days/{normalize_slug(WEEKDAY_LONG[day_code])}")}"{current_attr}>{escape(WEEKDAY_LONG[day_code])}</a>'
        )
    return f'<nav class="section-nav day-nav" aria-label="Days of the week">{"".join(links)}</nav>'


def render_info_card(title: str, icon: str, meta: str, description: str) -> str:
    return f"""
    <div class="browse-card browse-card-static">
      <div class="browse-card-top">
        {render_icon_badge(icon, kind="info", size="lg", icon_key=normalize_slug(title), extra_class="browse-card-icon")}
        <div class="browse-card-pills">
          <span class="browse-card-pill">{escape(meta)}</span>
        </div>
      </div>
      <div class="browse-card-body">
        <h3>{escape(title)}</h3>
        <p class="browse-card-copy">{escape(description)}</p>
      </div>
    </div>
    """


def render_homepage_html(sections: dict[str, List[models.Deal]], neighborhoods: List[dict], day_sections: dict[str, List[models.Deal]]) -> str:
    now = datetime.now(HOME_TIMEZONE).replace(tzinfo=None)
    live_deals = sections["live"]
    today_preview = sections["today"][:4]
    today_time_labels = [format_today_pick_time(deal, now) for deal in today_preview]
    utility_html = f"""
    <div class="hero-stats" aria-label="Guide overview">
      <div>
        <span>Live now</span>
        <strong>{len(live_deals)}</strong>
      </div>
      <div>
        <span>Today&#x27;s edit</span>
        <strong>{len(sections["today"])}</strong>
      </div>
      <div>
        <span>Neighborhoods</span>
        <strong>{len(neighborhoods)}</strong>
      </div>
    </div>
    """
    main_content = f"""
    <div class="homepage-flow">
    {render_live_now_module(live_deals, now)}
    {render_section(
        "today-preview",
        "Today",
        "A tighter daily edit of the strongest food, drink, and time-based picks worth opening first.",
        today_preview,
        "Nothing is on the board for today yet.",
        now,
        time_labels=today_time_labels,
        action_label="Open Today guide",
        action_href=site_href("/today"),
        section_class="content-section homepage-section homepage-section-today",
        panel_class="section-panel homepage-panel homepage-panel-today",
        kicker_label="Today’s edit",
    )}
    <div class="homepage-browse-band">
    {render_link_grid_section(
        "neighborhoods-preview",
        "Neighborhoods",
        "Start with place. These are the local boards shaping the guide right now.",
        render_neighborhood_cards(neighborhoods, limit=6, context="homepage"),
        "Browse neighborhoods",
        site_href("/neighborhoods"),
        section_class="content-section homepage-section homepage-section-browse homepage-section-neighborhoods",
        panel_class="section-panel homepage-panel homepage-panel-browse",
        kicker_label="By neighborhood",
    )}
    {render_link_grid_section(
        "days-preview",
        "Days",
        "Prefer to plan by rhythm? Open the day you care about and browse the week the way people actually go out.",
        render_day_cards(day_sections, context="homepage"),
        "Browse all days",
        site_href("/days"),
        grid_class="browse-card-grid browse-card-grid-days",
        section_class="content-section homepage-section homepage-section-browse homepage-section-days",
        panel_class="section-panel homepage-panel homepage-panel-browse",
        kicker_label="By day",
    )}
    </div>
    <section id="for-venues" class="content-section homepage-section homepage-section-venues">
      <div class="section-panel section-panel-secondary homepage-panel homepage-panel-venues">
        <div class="for-venues-panel">
          <div class="for-venues-copy">
            <p class="section-kicker">For venues</p>
            <h2>Curated for readers first, built for venues next.</h2>
            <p>
              DSM Deals Hub stays hand-curated so the public side reads like a local guide, not a feed dump. Venue onboarding and update flow still live here as the business side grows.
            </p>
          </div>
          <div class="live-empty-actions">
            <div class="empty-state-links">
              <a href="{site_href("/for-venues")}" class="empty-state-link">For Venues</a>
              <a href="{site_href("/neighborhoods")}" class="empty-state-link">Browse neighborhoods</a>
            </div>
            <div class="empty-state-cta" aria-label="Venue owners can get listed">
              <span class="empty-state-cta-plus">+</span>
              <span class="empty-state-cta-copy">
                <strong>Feature your venue</strong>
                <small>Business updates will live here</small>
              </span>
            </div>
          </div>
        </div>
      </div>
    </section>
    </div>
    """
    return render_page_document(
        "DSM Deals Hub",
        "Curated food and drink deals across Des Moines, updated manually.",
        "Curated weekly dining guide",
        "DSM Deals Hub",
        "A hand-curated guide to neighborhood specials, brunch, happy hour, dinner boards, and weekly favorites across Des Moines.",
        "home",
        main_content,
        utility_html=utility_html,
    )


def render_today_html(data: dict) -> str:
    now = datetime.now(HOME_TIMEZONE).replace(tzinfo=None)
    happy_hour_labels = [format_today_pick_time(deal, now) for deal in data["happy_hour"]]
    specials_labels = [format_today_pick_time(deal, now) for deal in data["specials"]]
    utility_html = f"""
    <div class="hero-stats" aria-label="Today overview">
      <div>
        <span>Happy hour</span>
        <strong>{len(data["happy_hour"])}</strong>
      </div>
      <div>
        <span>Specials</span>
        <strong>{len(data["specials"])}</strong>
      </div>
      <div>
        <span>Total today</span>
        <strong>{len(data["all"])}</strong>
      </div>
    </div>
    """
    featured_empty = ""
    if not data["all"]:
        featured_empty = f"""
        <section class="content-section">
          <div class="section-panel section-panel-secondary">
            {render_today_empty_state()}
          </div>
        </section>
        """
    main_content = f"""
    {featured_empty}
    {render_section(
        "today-happy-hour",
        "Happy Hour",
        "Timed pours, patio windows, and after-work stops worth opening first today.",
        data["happy_hour"],
        f"No happy hour picks are listed for {data['day_label']} yet.",
        now,
        time_labels=happy_hour_labels,
    )}
    {render_section(
        "today-specials",
        "Specials",
        "The strongest food-led picks and house specials on today’s board.",
        data["specials"],
        f"No featured specials are listed for {data['day_label']} yet.",
        now,
        time_labels=specials_labels,
    )}
    """
    return render_page_document(
        "DSM Deals Hub | Today",
        "Browse today's Des Moines dining specials and happy hour picks.",
        data["day_label"],
        "Today",
        f"A clean daily read on what is most worth opening for {data['day_label']} across Des Moines.",
        "today",
        main_content,
        utility_html=utility_html,
    )


def render_neighborhoods_html(groups: List[dict]) -> str:
    total_deals = sum(item["deal_count"] for item in groups)
    total_venues = sum(item["venue_count"] for item in groups)
    utility_html = f"""
    <div class="hero-stats" aria-label="Neighborhood overview">
      <div>
        <span>Neighborhoods</span>
        <strong>{len(groups)}</strong>
      </div>
      <div>
        <span>Specials</span>
        <strong>{total_deals}</strong>
      </div>
      <div>
        <span>Venues</span>
        <strong>{total_venues}</strong>
      </div>
    </div>
    """
    main_content = render_link_grid_section(
        "neighborhoods-grid",
        "Neighborhoods",
        "Choose a part of town, then open a tighter local board of restaurants, bars, and recurring specials.",
        render_neighborhood_cards(groups),
        "Open Days",
        site_href("/days"),
    )
    return render_page_document(
        "DSM Deals Hub | Neighborhoods",
        "Browse Des Moines dining specials by neighborhood.",
        "Browse by place",
        "Neighborhoods",
        "Start with the part of town that matters most, then open a neighborhood guide with the strongest current picks first.",
        "neighborhoods",
        main_content,
        utility_html=utility_html,
    )


def neighborhood_feature_intro(
    group: dict,
    reference: datetime,
    food_count: int,
    drinks_count: int,
    both_count: int,
    today_count: int,
    live_music_count: int,
) -> str:
    display_name = group.get("display_name", group["name"])
    total_grouped = food_count + drinks_count + both_count
    if total_grouped and today_count:
        return (
            f"{group['venue_count']} spots and {group['deal_count']} specials shape the {display_name} guide. "
            f"Food, drinks, and food-and-drink picks are split below so the area reads like a tighter local guide for today and the rest of the week."
        )
    if total_grouped and live_music_count:
        return (
            f"{group['venue_count']} spots and {group['deal_count']} specials shape the {display_name} guide. "
            f"Food, drinks, and food-and-drink picks are grouped separately below, with any music-forward note kept outside the main deal stack."
        )
    if total_grouped:
        return (
            f"{group['venue_count']} spots and {group['deal_count']} specials shape the {display_name} guide. "
            f"Browse the neighborhood by food, drinks, or spots where both are part of the same deal."
        )
    return (
        f"{group['venue_count']} spots and {group['deal_count']} specials shape the {display_name} guide. "
        f"This area is lighter on food and drink listings right now, with a smaller neighborhood note still captured on the board."
    )


def deal_has_explicit_time_signal(deal: models.Deal) -> bool:
    if deal.start_time or deal.end_time:
        return True

    time_label = weekly_content.site_deal_time_label(deal).lower()
    return bool(
        re.search(r"\b\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?)\b", time_label)
        or "after " in time_label
        or "all day" in time_label
        or "tonight" in time_label
        or "lunch" in time_label
    )


NEIGHBORHOOD_FOOD_CATEGORIES = {
    "food special",
    "dinner special",
    "lunch special",
    "family dining special",
    "buffet",
    "brunch buffet",
    "breakfast buffet",
    "dessert special",
}
NEIGHBORHOOD_DRINK_CATEGORIES = {
    "drink special",
    "late night drink special",
    "brunch drink special",
}
NEIGHBORHOOD_BOTH_CATEGORIES = {"food and drink special"}
NEIGHBORHOOD_FOOD_KEYWORDS = (
    "burger",
    "taco",
    "pizza",
    "flatbread",
    "wing",
    "wings",
    "chicken",
    "sandwich",
    "grinder",
    "tenderloin",
    "steak",
    "sirloin",
    "prime rib",
    "ribeye",
    "beef",
    "hot beef",
    "shrimp",
    "fish",
    "oyster",
    "oysters",
    "nachos",
    "fries",
    "salad",
    "soup",
    "dessert",
    "meal",
    "meals",
    "dinner",
    "lunch",
    "breakfast",
    "buffet",
    "app",
    "apps",
    "appetizer",
    "appetizers",
    "shared plate",
    "shared plates",
    "sliders",
    "sushi",
)
NEIGHBORHOOD_DRINK_KEYWORDS = (
    "happy hour",
    "beer",
    "draft",
    "wine",
    "cocktail",
    "cocktails",
    "martini",
    "martinis",
    "margarita",
    "margaritas",
    "tequila",
    "mezcal",
    "whiskey",
    "bourbon",
    "mule",
    "mules",
    "seltzer",
    "well",
    "wells",
    "pint",
    "pints",
    "pitcher",
    "pitchers",
    "mimosa",
    "mimosas",
    "ipa",
    "ipas",
    "domestic",
    "domestics",
    "shot",
    "shots",
    "drink",
    "drinks",
)


def classify_neighborhood_deal_group(deal: models.Deal) -> str:
    if weekly_content.site_deal_day_part(deal) == "Live Music":
        return "live_music"

    category = weekly_content.site_deal_category(deal).lower()
    text = f"{deal.title} {deal.short_description}".lower()

    food_signal = category in NEIGHBORHOOD_FOOD_CATEGORIES or any(keyword in text for keyword in NEIGHBORHOOD_FOOD_KEYWORDS)
    drink_signal = category in NEIGHBORHOOD_DRINK_CATEGORIES or any(keyword in text for keyword in NEIGHBORHOOD_DRINK_KEYWORDS)

    if category in NEIGHBORHOOD_BOTH_CATEGORIES:
        return "both"
    if food_signal and drink_signal:
        return "both"
    if drink_signal:
        return "drinks"
    return "food"


def sort_neighborhood_section_deals(
    section_key: str,
    deals: List[models.Deal],
    reference: datetime,
    base_order: dict[int, int],
) -> List[models.Deal]:
    today_code = weekly_content.normalize_day_code_value(reference.strftime("%a"))
    day_part_rank = (
        {"Afternoon": 0, "Dinner": 1, "Brunch": 2, "Late Night": 3, "Specials": 4}
        if section_key == "drinks"
        else {"Dinner": 0, "Brunch": 1, "Afternoon": 2, "Specials": 3, "Late Night": 4}
    )

    return [
        deal
        for _, deal in sorted(
            enumerate(deals),
            key=lambda entry: (
                0 if deal_matches_day_code(entry[1], today_code) else 1,
                0 if deal_has_explicit_time_signal(entry[1]) else 1,
                0 if section_key in {"drinks", "both"} and weekly_content.site_deal_is_happy_hour(entry[1]) else 1,
                day_part_rank.get(weekly_content.site_deal_day_part(entry[1]), 9),
                base_order.get(entry[1].id, entry[0]),
            ),
        )
    ]


def neighborhood_detail_sections(group: dict, reference: datetime) -> List[dict]:
    display_name = group.get("display_name", group["name"])
    today_code = weekly_content.normalize_day_code_value(reference.strftime("%a"))
    today_count = sum(1 for deal in group["deals"] if deal_matches_day_code(deal, today_code))
    base_order = {deal.id: index for index, deal in enumerate(group["deals"])}
    grouped: dict[str, List[models.Deal]] = {
        "food": [],
        "drinks": [],
        "both": [],
        "live_music": [],
    }

    for deal in group["deals"]:
        grouped[classify_neighborhood_deal_group(deal)].append(deal)

    sections = []
    section_meta = [
        (
            "food",
            "neighborhood-food",
            "Food",
            "Food",
            (
                f"Meals, brunches, buffets, sandwiches, seafood, and stronger food-led specials currently surfacing in {display_name}."
            ),
            "Food",
            "section-panel homepage-panel neighborhood-detail-panel neighborhood-detail-panel-food",
        ),
        (
            "drinks",
            "neighborhood-drinks",
            "Drinks",
            "Drinks",
            (
                f"Happy-hour windows, wine and beer specials, cocktail deals, and drink-led stops currently surfacing in {display_name}."
            ),
            "Drinks",
            "section-panel homepage-panel neighborhood-detail-panel neighborhood-detail-panel-drinks",
        ),
        (
            "both",
            "neighborhood-both",
            "Food + Drinks",
            "Food + Drinks",
            (
                f"Places where the value is in the pairing: food plus drinks, combo pricing, or fuller happy-hour-style boards in {display_name}."
            ),
            "Both",
            "section-panel homepage-panel neighborhood-detail-panel neighborhood-detail-panel-both",
        ),
    ]

    first_content_section = next(
        (key for key in ("food", "drinks", "both") if grouped[key]),
        None,
    )

    for key, section_id, kicker_label, title, intro, sort_key, base_panel_class in section_meta:
        section_deals = grouped[key]
        if not section_deals:
            continue
        panel_class = base_panel_class
        if key == first_content_section:
            panel_class = f"{panel_class} homepage-panel-today neighborhood-detail-panel-featured"
        ordered_deals = sort_neighborhood_section_deals(sort_key.lower(), section_deals, reference, base_order)
        sections.append(
            {
                "id": section_id,
                "title": title,
                "intro": intro,
                "deals": ordered_deals,
                "time_labels": [format_neighborhood_feature_time(deal, reference) for deal in ordered_deals],
                "section_class": "content-section neighborhood-detail-section",
                "panel_class": panel_class,
                "kicker_label": kicker_label,
            }
        )

    if grouped["live_music"]:
        sections.append(
            {
                "id": "neighborhood-live-music",
                "title": f"Live Music in {display_name}",
                "intro": f"Music-forward listings that are still part of the {display_name} board.",
                "deals": grouped["live_music"],
                "time_labels": [format_deal_time(deal, reference) for deal in grouped["live_music"]],
                "section_class": "content-section neighborhood-detail-section",
                "panel_class": "section-panel homepage-panel neighborhood-detail-panel neighborhood-detail-panel-live-music",
                "kicker_label": "Live music",
            }
        )

    if not sections:
        sections.append(
            {
                "id": "neighborhood-food",
                "title": f"Food in {display_name}",
                "intro": f"Curated picks currently filed under {display_name}.",
                "deals": group["deals"],
                "time_labels": [format_deal_time(deal, reference) for deal in group["deals"]],
                "section_class": "content-section neighborhood-detail-section",
                "panel_class": "section-panel homepage-panel homepage-panel-today neighborhood-detail-panel neighborhood-detail-panel-food neighborhood-detail-panel-featured",
                "kicker_label": "Food",
            }
        )

    return sections


def format_neighborhood_feature_time(deal: models.Deal, reference: datetime) -> str:
    today_code = weekly_content.normalize_day_code_value(reference.strftime("%a"))
    if deal_matches_day_code(deal, today_code):
        return format_today_pick_time(deal, reference)
    return format_deal_time(deal, reference)


def render_neighborhood_detail_html(group: dict) -> str:
    now = datetime.now(HOME_TIMEZONE).replace(tzinfo=None)
    display_name = group.get("display_name", group["name"])
    today_code = weekly_content.normalize_day_code_value(now.strftime("%a"))
    today_count = sum(1 for deal in group["deals"] if deal_matches_day_code(deal, today_code))
    detail_sections = neighborhood_detail_sections(group, now)
    food_count = next(
        (len(section["deals"]) for section in detail_sections if section["id"] == "neighborhood-food"),
        0,
    )
    drinks_count = next(
        (len(section["deals"]) for section in detail_sections if section["id"] == "neighborhood-drinks"),
        0,
    )
    both_count = next(
        (len(section["deals"]) for section in detail_sections if section["id"] == "neighborhood-both"),
        0,
    )
    live_music_count = next(
        (len(section["deals"]) for section in detail_sections if section["id"] == "neighborhood-live-music"),
        0,
    )
    utility_html = f"""
    <div class="hero-stats" aria-label="Neighborhood overview">
      <div>
        <span>On today&#x27;s board</span>
        <strong>{today_count}</strong>
      </div>
      <div>
        <span>Specials</span>
        <strong>{group["deal_count"]}</strong>
      </div>
      <div>
        <span>Venues</span>
        <strong>{group["venue_count"]}</strong>
      </div>
    </div>
    """
    main_content = "".join(
        render_section(
            section["id"],
            section["title"],
            section["intro"],
            section["deals"],
            f"Nothing is listed for {display_name} yet.",
            now,
            time_labels=section["time_labels"],
            action_label="Back to neighborhoods" if index == 0 else None,
            action_href=site_href("/neighborhoods") if index == 0 else None,
            section_class=section.get("section_class", "content-section"),
            panel_class=section.get("panel_class", "section-panel"),
            kicker_label=section.get("kicker_label"),
        )
        for index, section in enumerate(detail_sections)
    )
    return render_page_document(
        f"DSM Deals Hub | {display_name}",
        f"Browse featured dining deals in {display_name}, Des Moines.",
        "Neighborhood guide",
        display_name,
        neighborhood_feature_intro(group, now, food_count, drinks_count, both_count, today_count, live_music_count),
        "neighborhoods",
        main_content,
        utility_html=utility_html,
    )


def render_days_html(sections: dict[str, List[models.Deal]]) -> str:
    active_days = sum(1 for day_code in DAY_ORDER if sections[day_code])
    total_deals = sum(len(sections[day_code]) for day_code in DAY_ORDER)
    utility_html = f"""
    <div class="hero-stats" aria-label="Days overview">
      <div>
        <span>Active days</span>
        <strong>{active_days}</strong>
      </div>
      <div>
        <span>Weekend</span>
        <strong>{len(sections['Sat']) + len(sections['Sun'])}</strong>
      </div>
      <div>
        <span>Total listed</span>
        <strong>{total_deals}</strong>
      </div>
    </div>
    """
    main_content = render_link_grid_section(
        "days-grid",
        "Days",
        "Open a dedicated day page to browse the week the way people actually plan it: flatter weekdays, fuller weekends.",
        render_day_cards(sections),
        "Open Today",
        site_href("/today"),
        grid_class="browse-card-grid browse-card-grid-days",
    )
    return render_page_document(
        "DSM Deals Hub | Days",
        "Browse Des Moines food and drink deals by day of the week.",
        "Day-by-day guide",
        "Days",
        "Browse the weekly rhythm of Des Moines specials in strict calendar order, then open the day that fits your plans.",
        "days",
        main_content,
        utility_html=utility_html,
        hero_class="hero hero-days",
    )


def featured_day_deals(day_code: str, deals: List[models.Deal], limit: int = 4) -> List[models.Deal]:
    weekend = day_code in {"Sat", "Sun"}
    day_part_rank = (
        {"Brunch": 0, "Afternoon": 1, "Dinner": 2, "Late Night": 3, "Live Music": 4, "Specials": 5}
        if weekend
        else {"Afternoon": 0, "Dinner": 1, "Brunch": 2, "Specials": 3, "Late Night": 4, "Live Music": 5}
    )

    ranked = sorted(
        enumerate(deals),
        key=lambda entry: (
            0 if deal_has_explicit_time_signal(entry[1]) else 1,
            0 if weekly_content.site_deal_is_happy_hour(entry[1]) and weekly_content.site_deal_day_part(entry[1]) != "Late Night" else 1,
            day_part_rank.get(weekly_content.site_deal_day_part(entry[1]), 9),
            entry[0],
        ),
    )

    picked: List[models.Deal] = []
    picked_ids: set[int] = set()
    seen_venue_ids: set[int] = set()

    for _, deal in ranked:
        if deal.venue_id in seen_venue_ids:
            continue
        picked.append(deal)
        picked_ids.add(deal.id)
        seen_venue_ids.add(deal.venue_id)
        if len(picked) >= limit:
            break

    if len(picked) < limit:
        for _, deal in ranked:
            if deal.id in picked_ids:
                continue
            picked.append(deal)
            picked_ids.add(deal.id)
            if len(picked) >= limit:
                break

    return picked


def sort_day_section_deals(day_code: str, deals: List[models.Deal], featured_ids: set[int]) -> List[models.Deal]:
    weekend = day_code in {"Sat", "Sun"}
    day_part_rank = (
        {"Brunch": 0, "Afternoon": 1, "Dinner": 2, "Late Night": 3, "Live Music": 4, "Specials": 5}
        if weekend
        else {"Afternoon": 0, "Dinner": 1, "Brunch": 2, "Specials": 3, "Late Night": 4, "Live Music": 5}
    )

    return [
        deal
        for _, deal in sorted(
            enumerate(deals),
            key=lambda entry: (
                0 if entry[1].id in featured_ids else 1,
                0 if deal_has_explicit_time_signal(entry[1]) else 1,
                0 if weekly_content.site_deal_is_happy_hour(entry[1]) and weekly_content.site_deal_day_part(entry[1]) != "Late Night" else 1,
                day_part_rank.get(weekly_content.site_deal_day_part(entry[1]), 9),
                entry[0],
            ),
        )
    ]


def weekday_detail_sections(day_code: str, deals: List[models.Deal], featured_ids: set[int]) -> List[dict]:
    day_label = WEEKDAY_LONG[day_code]
    buckets = {"Happy Hour": [], "Dinner": [], "Specials": [], "Late Night": []}

    for deal in deals:
        day_part = weekly_content.site_deal_day_part(deal)
        if day_part == "Late Night":
            buckets["Late Night"].append(deal)
        elif weekly_content.site_deal_is_happy_hour(deal):
            buckets["Happy Hour"].append(deal)
        elif day_part == "Dinner":
            buckets["Dinner"].append(deal)
        else:
            buckets["Specials"].append(deal)

    intros = {
        "Happy Hour": f"Timed pours, patio windows, and after-work value that usually open the {day_label} board.",
        "Dinner": f"Dinner-led specials, bigger plates, and stronger evening picks currently landing on {day_label}.",
        "Specials": f"The main {day_label} board, from lunch runs to all-day house specials and neighborhood staples.",
        "Late Night": f"Later-night stops and extended windows that keep going after the main {day_label} dinner stretch.",
    }

    sections = []
    for title in ("Happy Hour", "Dinner", "Specials", "Late Night"):
        section_deals = buckets[title]
        if not section_deals:
            continue
        ordered = sort_day_section_deals(day_code, section_deals, featured_ids)
        sections.append(
            {
                "title": title,
                "intro": intros[title],
                "deals": ordered,
            }
        )

    if sections:
        return sections

    return [
        {
            "title": "Specials",
            "intro": f"Everything currently filed under the {day_label} board.",
            "deals": deals,
        }
    ]


def weekend_detail_sections(day_code: str, deals: List[models.Deal], featured_ids: set[int]) -> List[dict]:
    grouped: dict[str, List[models.Deal]] = {section: [] for section in weekly_content.WEEKEND_SECTION_ORDER}
    for deal in deals:
        grouped.setdefault(weekly_content.site_deal_day_part(deal), []).append(deal)

    sections = []
    for title in weekly_content.WEEKEND_SECTION_ORDER:
        section_deals = grouped.get(title) or []
        if not section_deals:
            continue
        ordered = sort_day_section_deals(day_code, section_deals, featured_ids)
        sections.append(
            {
                "title": title,
                "intro": weekly_content.WEEKEND_SECTION_INTROS[title],
                "deals": ordered,
            }
        )

    if sections:
        return sections

    return [
        {
            "title": "Specials",
            "intro": f"Everything currently featured for {WEEKDAY_LONG[day_code]}.",
            "deals": deals,
        }
    ]


def day_detail_sections_for_page(day_code: str, deals: List[models.Deal]) -> tuple[List[dict], List[models.Deal]]:
    featured = featured_day_deals(day_code, deals)
    featured_ids = {deal.id for deal in featured}
    sections = (
        weekend_detail_sections(day_code, deals, featured_ids)
        if day_code in {"Sat", "Sun"}
        else weekday_detail_sections(day_code, deals, featured_ids)
    )
    return sections, featured


def day_detail_hero_text(day_code: str, deals: List[models.Deal], sections: List[dict], featured: List[models.Deal]) -> str:
    day_label = WEEKDAY_LONG[day_code]
    if day_code in {"Sat", "Sun"}:
        return (
            f"A fuller weekend guide for {day_label}, organized around the way people actually eat and go out. "
            f"Start with {len(featured)} stronger picks, then move through brunch, afternoon, dinner, and later plans."
        )

    first_section = sections[0]["title"] if sections else "Specials"
    if first_section == "Happy Hour":
        return (
            f"Open {day_label} with the best timed drink windows first, then move through the broader food and dinner boards across the city."
        )
    return (
        f"A cleaner weekday edit for {day_label}, with the strongest current picks surfaced first and the rest of the board organized underneath."
    )


def render_day_detail_html(day_code: str, deals: List[models.Deal]) -> str:
    now = datetime.now(HOME_TIMEZONE).replace(tzinfo=None)
    day_label = WEEKDAY_LONG[day_code]
    detail_sections, featured = day_detail_sections_for_page(day_code, deals)
    venue_count = len({deal.venue_id for deal in deals})
    section_count = len(detail_sections)
    guide_label = "Weekend guide" if day_code in {"Sat", "Sun"} else "Weekday edit"
    description = day_detail_hero_text(day_code, deals, detail_sections, featured)
    utility_html = f"""
    {render_day_page_nav(day_code)}
    <div class="hero-stats" aria-label="{escape(day_label)} overview">
      <div>
        <span>Specials</span>
        <strong>{len(deals)}</strong>
      </div>
      <div>
        <span>Venues</span>
        <strong>{venue_count}</strong>
      </div>
      <div>
        <span>Sections</span>
        <strong>{section_count}</strong>
      </div>
    </div>
    """
    main_content = "".join(
        render_section(
            f"{normalize_slug(day_label)}-{normalize_slug(section['title'])}",
            section["title"],
            section["intro"],
            section["deals"],
            f"Nothing is listed for {day_label} yet.",
            now,
            time_labels=[format_day_page_time(deal, day_code) for deal in section["deals"]],
            action_label="Back to days" if index == 0 else None,
            action_href=site_href("/days") if index == 0 else None,
            section_class="content-section day-detail-section",
            panel_class=(
                "section-panel homepage-panel homepage-panel-today day-detail-panel day-detail-panel-featured"
                if index == 0
                else f"section-panel homepage-panel day-detail-panel{' day-detail-panel-live-music' if section['title'] == 'Live Music' else ''}"
            ),
            kicker_label="Start here" if index == 0 else section["title"],
        )
        for index, section in enumerate(detail_sections)
    )
    return render_page_document(
        f"DSM Deals Hub | {day_label}",
        f"Browse the featured Des Moines dining specials for {day_label}.",
        guide_label,
        day_label,
        description,
        "days",
        main_content,
        utility_html=utility_html,
        hero_class="hero hero-days",
    )


def render_for_venues_html() -> str:
    intro_panel = f"""
    <section class="content-section">
      <div class="section-panel section-panel-secondary">
        <div class="for-venues-panel">
          <div class="for-venues-copy">
            <p class="section-kicker">For Venues</p>
            <h2>Manual curation comes first in this MVP.</h2>
            <p>
              DSM Deals Hub is being built as a tight local guide, not an open submission board. For now, the business-facing side is about explaining what fits the guide, how featured spots will work, and where venue onboarding intent lives as the product grows.
            </p>
          </div>
          <div class="live-empty-actions">
            <div class="empty-state-links">
              <a href="{site_href("/neighborhoods")}" class="empty-state-link">Browse Neighborhoods</a>
              <a href="{site_href("/days")}" class="empty-state-link">See Days</a>
            </div>
            <div class="empty-state-cta" aria-label="Venue onboarding intent">
              <span class="empty-state-cta-plus">+</span>
              <span class="empty-state-cta-copy">
                <strong>Get featured</strong>
                <small>Venue onboarding will live here</small>
              </span>
            </div>
          </div>
        </div>
      </div>
    </section>
    """
    info_cards = "\n".join(
        [
            render_info_card(
                "What fits the guide",
                "🍽️",
                "Dining-first curation",
                "We are prioritizing restaurants, bars, brunch spots, buffets, seafood, wings, burgers, tacos, wine, and strong recurring house specials.",
            ),
            render_info_card(
                "How v1 works",
                "🖐️",
                "Manual admin workflow",
                "Deals are copied in manually and curated by hand so the public site stays clean, readable, and neighborhood friendly.",
            ),
            render_info_card(
                "What comes later",
                "⚙️",
                "Venue updates and business flow",
                "Later passes can add venue update tools, fresher schedules, and a clearer business-side submission flow without changing the public guide structure.",
            ),
        ]
    )
    main_content = f"""
    {intro_panel}
    {render_link_grid_section(
        "venue-guidelines",
        "How This Will Work",
        "A quick look at the business-facing intent for featuring, updates, and manual onboarding.",
        info_cards,
        "Back Home",
        site_href("/"),
    )}
    """
    return render_page_document(
        "DSM Deals Hub | For Venues",
        "Learn how venues fit into DSM Deals Hub and how manual featuring will work in v1.",
        "Business-facing page",
        "For Venues",
        "A clear place for venue onboarding intent, what fits the guide, and how manual curation will expand later without adding a separate contact page.",
        "for-venues",
        main_content,
    )


@app.get("/", response_class=HTMLResponse)
def homepage(db: Session = Depends(get_db)):
    return HTMLResponse(
        render_homepage_html(
            homepage_sections(db),
            neighborhood_groups(db),
            days_page_sections(db),
        )
    )


@app.get("/today", response_class=HTMLResponse)
def today_page(db: Session = Depends(get_db)):
    return HTMLResponse(render_today_html(today_page_data(db)))


@app.get("/neighborhoods", response_class=HTMLResponse)
def neighborhoods_page(db: Session = Depends(get_db)):
    return HTMLResponse(render_neighborhoods_html(neighborhood_groups(db)))


@app.get("/neighborhoods/{slug}", response_class=HTMLResponse)
def neighborhood_detail_page(slug: str, db: Session = Depends(get_db)):
    return HTMLResponse(render_neighborhood_detail_html(get_neighborhood_group_or_404(db, slug)))


@app.get("/days", response_class=HTMLResponse)
def days_page(db: Session = Depends(get_db)):
    return HTMLResponse(render_days_html(days_page_sections(db)))


@app.get("/days/{day_slug}", response_class=HTMLResponse)
def day_detail_page(day_slug: str, db: Session = Depends(get_db)):
    day_code = day_code_from_slug_or_404(day_slug)
    return HTMLResponse(render_day_detail_html(day_code, days_page_sections(db)[day_code]))


@app.get("/for-venues", response_class=HTMLResponse)
def for_venues_page():
    return HTMLResponse(render_for_venues_html())


@app.post("/owners", response_model=schemas.OwnerOut)
def create_owner(owner: schemas.OwnerCreate, db: Session = Depends(get_db)):
    existing = db.query(models.BusinessOwner).filter_by(email=owner.email).first()
    if existing:
        return existing
    business_owner = models.BusinessOwner(name=owner.name, email=owner.email, phone=owner.phone)
    db.add(business_owner)
    db.commit()
    db.refresh(business_owner)
    return business_owner


@app.post("/venues", response_model=schemas.VenueOut)
def create_venue(v: schemas.VenueCreate, db: Session = Depends(get_db)):
    ensure_owner_exists(db, v.owner_id)
    venue = models.Venue(
        **v.model_dump(exclude={"slug"}),
        slug=ensure_unique_venue_slug(db, v.slug),
    )
    db.add(venue)
    db.commit()
    db.refresh(venue)
    return venue


@app.get("/venues", response_model=List[schemas.VenueOut])
def list_venues(
    owner_id: Optional[int] = None,
    neighborhood: Optional[str] = None,
    q: Optional[str] = None,
    db: Session = Depends(get_db),
):
    query = db.query(models.Venue)
    if owner_id is not None:
        query = query.filter(models.Venue.owner_id == owner_id)
    if neighborhood:
        query = query.filter(func.lower(models.Venue.neighborhood) == neighborhood.strip().lower())
    if q:
        term = f"%{q.strip()}%"
        query = query.filter(
            or_(
                models.Venue.name.ilike(term),
                models.Venue.slug.ilike(term),
                models.Venue.address.ilike(term),
            )
        )
    return query.order_by(models.Venue.name.asc()).all()


@app.patch("/admin/venues/{venue_id}", response_model=schemas.VenueOut, dependencies=[Depends(require_admin)])
def admin_update_venue(venue_id: int, payload: schemas.VenueUpdate, db: Session = Depends(get_db)):
    venue = get_venue_or_404(db, venue_id)
    values = payload.model_dump(exclude_unset=True)

    for field in ("name", "slug", "address"):
        if field in values and values[field] is None:
            raise HTTPException(status_code=400, detail=f"{field} cannot be null")

    if "owner_id" in values:
        ensure_owner_exists(db, values["owner_id"])
    if "slug" in values and values["slug"] is not None:
        values["slug"] = ensure_unique_venue_slug(db, values["slug"], exclude_id=venue.id)

    for field, value in values.items():
        setattr(venue, field, value)

    venue.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(venue)
    return venue


@app.get("/admin/venues", response_model=List[schemas.AdminVenueOut], dependencies=[Depends(require_admin)])
def admin_list_venues(
    owner_id: Optional[int] = None,
    neighborhood: Optional[str] = None,
    q: Optional[str] = None,
    has_owner: Optional[bool] = None,
    db: Session = Depends(get_db),
):
    expire_stale_last_minute_deals(db)

    query = db.query(models.Venue).options(joinedload(models.Venue.owner))
    if owner_id is not None:
        query = query.filter(models.Venue.owner_id == owner_id)
    if neighborhood:
        query = query.filter(func.lower(models.Venue.neighborhood) == neighborhood.strip().lower())
    if has_owner is True:
        query = query.filter(models.Venue.owner_id.is_not(None))
    if has_owner is False:
        query = query.filter(models.Venue.owner_id.is_(None))
    if q:
        term = f"%{q.strip()}%"
        query = query.filter(
            or_(
                models.Venue.name.ilike(term),
                models.Venue.slug.ilike(term),
                models.Venue.address.ilike(term),
                models.Venue.neighborhood.ilike(term),
            )
        )

    venues = query.order_by(models.Venue.name.asc()).all()
    venue_ids = [venue.id for venue in venues]
    counts = {venue_id: {"deal_count": 0, "live_deal_count": 0} for venue_id in venue_ids}

    if venue_ids:
        deal_rows = (
            db.query(models.Deal.venue_id, models.Deal.status)
            .filter(models.Deal.venue_id.in_(venue_ids))
            .all()
        )
        for venue_id, status in deal_rows:
            counts[venue_id]["deal_count"] += 1
            if status == models.Status.live:
                counts[venue_id]["live_deal_count"] += 1

    return [
        {
            **schemas.VenueOut.model_validate(venue).model_dump(),
            "owner_name": venue.owner.name if venue.owner else None,
            "deal_count": counts[venue.id]["deal_count"],
            "live_deal_count": counts[venue.id]["live_deal_count"],
        }
        for venue in venues
    ]


@app.post("/deals/weekly", response_model=schemas.DealOut)
def create_weekly_deal(d: schemas.WeeklyDealCreate, db: Session = Depends(get_db)):
    get_venue_or_404(db, d.venue_id)
    enforce_weekly_cap(db, d.venue_id)

    deal = models.Deal(
        venue_id=d.venue_id,
        title=d.title,
        short_description=d.short_description,
        type=models.DealType.weekly,
        weekday_pattern=d.weekday_pattern,
        start_time=d.start_time,
        end_time=d.end_time,
        age_21_plus=d.age_21_plus,
        menu_link=d.menu_link,
        image_url=d.image_url,
        sponsored=d.sponsored,
        source_type=d.source_type,
        source_url=d.source_url,
        source_text=d.source_text,
        source_posted_at=d.source_posted_at,
        notes_private=d.notes_private,
        status=models.Status.queued,
    )
    db.add(deal)
    db.commit()
    db.refresh(deal)
    return deal


@app.post("/deals/last-minute", response_model=schemas.DealOut)
def create_last_minute(d: schemas.LastMinuteDealCreate, db: Session = Depends(get_db)):
    get_venue_or_404(db, d.venue_id)

    deal = models.Deal(
        venue_id=d.venue_id,
        title=d.title,
        short_description=d.short_description,
        type=models.DealType.last_minute,
        start_at=d.start_at,
        end_at=d.end_at,
        age_21_plus=d.age_21_plus,
        menu_link=d.menu_link,
        image_url=d.image_url,
        sponsored=d.sponsored,
        source_type=d.source_type,
        source_url=d.source_url,
        source_text=d.source_text,
        source_posted_at=d.source_posted_at,
        notes_private=d.notes_private,
        status=models.Status.queued,
    )
    db.add(deal)
    db.commit()
    db.refresh(deal)
    return deal


@app.post("/moderation/approve/{deal_id}")
def approve_deal(
    deal_id: int,
    body: schemas.ApproveRequest,
    db: Session = Depends(get_db),
    _admin=Depends(require_admin),
):
    deal = get_deal_or_404(db, deal_id)
    deal.status = models.Status.live if body.approve else models.Status.rejected
    normalize_live_status_for_time(deal)
    deal.updated_at = datetime.utcnow()
    db.commit()
    return {"ok": True, "status": deal.status.value}


@app.get("/feed", response_model=List[schemas.DealOut])
def feed(
    neighborhood: Optional[str] = Query(None),
    lat: Optional[float] = Query(None),
    lng: Optional[float] = Query(None),
    db: Session = Depends(get_db),
):
    now = datetime.utcnow()
    live_deals = [deal for deal in load_public_deals(db) if deal_is_live_now(deal, now)]
    return sorted(
        live_deals,
        key=lambda deal: compute_sort_key(deal, now, neighborhood, lat, lng),
        reverse=True,
    )


@app.get("/deals/live", response_model=List[schemas.PublicDealOut])
def deals_live(db: Session = Depends(get_db)):
    now = datetime.utcnow()
    deals = [deal for deal in load_public_deals(db) if deal_is_live_now(deal, now)]
    return sort_public_deals(deals, now)


@app.get("/deals/tonight", response_model=List[schemas.PublicDealOut])
def deals_tonight(db: Session = Depends(get_db)):
    now = datetime.utcnow()
    end_of_day = now.replace(hour=23, minute=59, second=59, microsecond=999999)
    deals = [
        deal
        for deal in load_public_deals(db)
        if deal_overlaps_window(deal, now, end_of_day)
    ]
    return sort_public_deals(deals, now)


@app.get("/deals/week", response_model=List[schemas.PublicDealOut])
def deals_week(db: Session = Depends(get_db)):
    now = datetime.utcnow()
    week_end = now + timedelta(days=7)
    deals = [
        deal
        for deal in load_public_deals(db)
        if deal_overlaps_window(deal, now, week_end)
    ]
    return sort_public_deals(deals, now)


@app.get("/venues/{slug}", response_model=schemas.VenueOut)
def venue_detail(slug: str, db: Session = Depends(get_db)):
    return get_venue_by_slug_or_404(db, slug)


@app.get("/venues/{slug}/deals", response_model=List[schemas.DealOut])
def venue_deals(slug: str, db: Session = Depends(get_db)):
    venue = get_venue_by_slug_or_404(db, slug)
    now = datetime.utcnow()
    expire_stale_last_minute_deals(db)

    deals = (
        db.query(models.Deal)
        .filter(
            models.Deal.venue_id == venue.id,
            models.Deal.status == models.Status.live,
        )
        .order_by(models.Deal.created_at.desc())
        .all()
    )

    visible = []
    for deal in deals:
        if deal.type == models.DealType.last_minute:
            if deal.end_at and deal.end_at >= now:
                visible.append(deal)
        else:
            visible.append(deal)

    return sort_public_deals(visible, now)


@app.post("/metrics/{deal_id}/{kind}")
def record_metric(deal_id: int, kind: str, db: Session = Depends(get_db)):
    if kind not in {"view", "click_menu", "click_directions", "click_call", "save", "share"}:
        raise HTTPException(status_code=400, detail="invalid metric kind")
    event = models.MetricEvent(deal_id=deal_id, kind=kind, ip_hash=None)
    db.add(event)
    db.commit()
    return {"ok": True}


@app.get("/admin/deals", response_model=List[schemas.AdminDealOut], dependencies=[Depends(require_admin)])
def admin_list_deals(
    status: Optional[models.Status] = None,
    deal_type: Optional[models.DealType] = Query(None, alias="type"),
    venue_id: Optional[int] = None,
    neighborhood: Optional[str] = None,
    q: Optional[str] = None,
    db: Session = Depends(get_db),
):
    expire_stale_last_minute_deals(db)

    query = db.query(models.Deal).options(joinedload(models.Deal.venue)).join(models.Venue)
    if status is not None:
        query = query.filter(models.Deal.status == status)
    if deal_type is not None:
        query = query.filter(models.Deal.type == deal_type)
    if venue_id is not None:
        query = query.filter(models.Deal.venue_id == venue_id)
    if neighborhood:
        query = query.filter(func.lower(models.Venue.neighborhood) == neighborhood.strip().lower())
    if q:
        term = f"%{q.strip()}%"
        query = query.filter(
            or_(
                models.Deal.title.ilike(term),
                models.Deal.short_description.ilike(term),
                models.Venue.name.ilike(term),
            )
        )

    return query.order_by(models.Deal.updated_at.desc(), models.Deal.created_at.desc()).all()


@app.patch("/admin/deals/{deal_id}", response_model=schemas.AdminDealOut, dependencies=[Depends(require_admin)])
def admin_update_deal(deal_id: int, payload: schemas.DealUpdate, db: Session = Depends(get_db)):
    deal = get_deal_or_404(db, deal_id)
    apply_deal_update(db, deal, payload)
    db.commit()
    return get_deal_with_venue_or_404(db, deal_id)


@app.post("/admin/deals/{deal_id}/archive", response_model=schemas.AdminDealOut, dependencies=[Depends(require_admin)])
def admin_archive_deal(deal_id: int, db: Session = Depends(get_db)):
    deal = get_deal_or_404(db, deal_id)
    deal.status = models.Status.archived
    deal.updated_at = datetime.utcnow()
    db.commit()
    return get_deal_with_venue_or_404(db, deal_id)


@app.post("/admin/deals/{deal_id}/expire", response_model=schemas.AdminDealOut, dependencies=[Depends(require_admin)])
def admin_expire_deal(deal_id: int, db: Session = Depends(get_db)):
    deal = get_deal_or_404(db, deal_id)
    deal.status = models.Status.expired
    deal.updated_at = datetime.utcnow()
    db.commit()
    return get_deal_with_venue_or_404(db, deal_id)


@app.post("/admin/expire_past", dependencies=[Depends(require_admin)])
def expire_past(db: Session = Depends(get_db)):
    return {"expired": expire_stale_last_minute_deals(db)}


@app.get("/health")
def health():
    return {"ok": True, "ts": datetime.utcnow().isoformat()}
