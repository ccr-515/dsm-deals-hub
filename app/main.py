from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
import hashlib
import hmac
from html import escape
import json
import logging
import os
from pathlib import Path
import re
from typing import List, Optional
from urllib.parse import quote_plus
from urllib.parse import urlencode
from urllib.request import Request as UrlRequest, urlopen
from zoneinfo import ZoneInfo

from fastapi import Depends, FastAPI, Form, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import func, or_
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, joinedload

from . import config, models, schemas
from .database import Base, SessionLocal, engine
from .migrations import run_migrations, schema_report
from .neighborhood_icons import neighborhood_icon_static_path, sync_neighborhood_icon_assets
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
    sort_live_now_deals,
)
from . import weekly_master_content as weekly_content

if engine.url.get_backend_name() == "sqlite":
    Base.metadata.create_all(bind=engine)
    run_migrations(engine)
else:
    Base.metadata.create_all(bind=engine)
    run_migrations(engine)

APP_DIR = Path(__file__).parent
STATIC_DIR = APP_DIR / "static"
HOME_TIMEZONE = ZoneInfo("America/Chicago")
HOMEPAGE_SEED_SOURCE = "seed://homepage-curated-v2"
if os.getenv('VERCEL') != '1':
    sync_neighborhood_icon_assets()
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
    "Des Moines": (41.5868, -93.6250),
    "Downtown": (41.5868, -93.6250),
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
BRAND_MARK_STATIC_PATH = "/static/dsm-deals-hub-mark.png"
FAVICON_STATIC_PATH = "/static/favicon.png"
APPLE_TOUCH_ICON_STATIC_PATH = "/static/apple-touch-icon.png"
BRAND_HOME_EYEBROW = "Curated daily"
BRAND_META_LABEL = "Des Moines daily dining guide"
PUBLIC_VENUES_LABEL = "Got a deal?"
VENUE_SUBMISSION_EMBED_HTML = """
<iframe src="https://docs.google.com/forms/d/e/1FAIpQLScBo9-2_gReSzkBRcQo6wM_8jZL4c9tV4kF2GTQeaIrtWmrdw/viewform?embedded=true" width="100%" height="2400" frameborder="0" marginheight="0" marginwidth="0" class="google-form-frame">Loading…</iframe>
"""
FOR_VENUES_TRUST_LINE = "Every submission is reviewed by hand before it goes live."
FOR_VENUES_FUTURE_NOTE = (
    "Right now, submissions are manual. Later on, businesses may get a login to update deals directly."
)
FOR_VENUES_ABOUT_HEADING = "Why I built DSM Deals Hub"
FOR_VENUES_ABOUT_PARAGRAPH = (
    "I built DSM Deals Hub because too many good deals are scattered all over the place. "
    "Some are buried in Instagram stories, some are on Facebook, some are outdated, and some are "
    "impossible to find unless you already know where to look. I wanted one clean place where people "
    "in Des Moines could quickly see what is worth going out for today, what is happening right now, "
    "and what each neighborhood has to offer. At the same time, I wanted it to help local businesses "
    "get more attention on the deals they actually need people to see."
)
PUBLIC_NEIGHBORHOOD_LABELS = {
    "des moines area": "Des Moines",
    "des moines metro": "Des Moines",
    "court district": "Downtown",
    "court avenue": "Downtown",
    "western gateway": "Downtown",
    "downtown des moines": "Downtown",
    "prairie meadows": "Altoona",
}
NEIGHBORHOOD_ROUTE_ALIASES = {
    "des-moines-area": "des-moines",
    "des-moines-metro": "des-moines",
    "court-district": "downtown",
    "court-avenue": "downtown",
    "western-gateway": "downtown",
    "prairie-meadows": "altoona",
}
NEIGHBORHOOD_PLACEHOLDER_ICON_MAP = {
    "Altoona": "AL",
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
    "altoona": "sunrise",
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
logger = logging.getLogger(__name__)

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


def current_admin_key() -> str:
    return os.getenv("ADMIN_KEY", "").strip()


def admin_key_is_valid(value: Optional[str]) -> bool:
    admin_key = current_admin_key()
    supplied = str(value or "").strip()
    return bool(admin_key and supplied) and hmac.compare_digest(supplied, admin_key)


def admin_session_cookie_value(admin_key: Optional[str] = None) -> str:
    secret = (admin_key if admin_key is not None else current_admin_key()).strip().encode("utf-8")
    signature = hmac.new(secret, b"dsm-deals-admin-session", hashlib.sha256).hexdigest()
    return f"v1:{signature}"


def admin_session_is_valid(value: Optional[str]) -> bool:
    admin_key = current_admin_key()
    supplied = str(value or "").strip()
    if not admin_key or not supplied:
        return False
    return hmac.compare_digest(supplied, admin_session_cookie_value(admin_key))


def is_admin_authorized(request: Request) -> bool:
    admin_key = current_admin_key()
    header_key = request.headers.get("x-admin-key", "").strip()
    if admin_key and header_key and hmac.compare_digest(header_key, admin_key):
        return True
    return admin_session_is_valid(request.cookies.get("admin_session"))


def require_admin(request: Request):
    if not is_admin_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    return True


def require_admin_password(request: Request):
    return require_admin(request)


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


def public_venue_is_excluded(venue: Optional[models.Venue]) -> bool:
    return bool(venue and compact_text(venue.name).lower() in PUBLIC_EXCLUDED_VENUE_NAMES)


def load_public_deals(db: Session) -> List[models.Deal]:
    expire_stale_last_minute_deals(db)
    deals = (
        db.query(models.Deal)
        .options(joinedload(models.Deal.venue))
        .filter(models.Deal.status == models.Status.live)
        .order_by(models.Deal.created_at.desc())
        .all()
    )
    return [deal for deal in deals if not public_venue_is_excluded(deal.venue)]


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


def has_public_display_metadata(deal: models.Deal) -> bool:
    if not deal.notes_private:
        return False
    try:
        payload = json.loads(deal.notes_private)
    except (TypeError, ValueError):
        return False
    return isinstance(payload, dict) and all(
        key in payload
        for key in ("day_code", "day_name", "time_label", "category", "day_part", "today_bucket")
    )


def ensure_public_display_metadata(deal: models.Deal, reference: datetime) -> None:
    if has_public_display_metadata(deal):
        return

    day_code = weekly_content.normalize_day_code_value(reference.strftime("%a"))
    day_name = weekly_content.DAY_CODE_TO_NAME[day_code]
    start_time = deal.start_time
    end_time = deal.end_time
    title = deal.title or ""
    description = deal.short_description or ""

    if start_time and end_time:
        start_label = format_clock(*parse_hhmm(start_time))
        end_label = format_clock(*parse_hhmm(end_time))
        time_label = f"{start_label} to {end_label}"
    elif start_time:
        time_label = f"After {format_clock(*parse_hhmm(start_time))}"
    else:
        time_label = "Today"

    text = f"{title} {description}".lower()
    category = "Drink special" if any(
        keyword in text for keyword in ("happy hour", "wine", "beer", "pint", "cocktail", "martini")
    ) else "Food and drink special"
    day_part = weekly_content._classify_day_part(category, time_label, title, description, start_time)
    happy_hour = weekly_content._is_happy_hour(category, time_label, title, description, start_time)

    existing = {}
    if deal.notes_private:
        try:
            existing_payload = json.loads(deal.notes_private)
            if isinstance(existing_payload, dict):
                existing = existing_payload
        except (TypeError, ValueError):
            existing = {}
    display_meta = json.loads(
        weekly_content._build_notes_meta(
            day_name=day_name,
            day_code=day_code,
            time_label=time_label,
            category=category,
            day_part=day_part,
            happy_hour=happy_hour,
        )
    )
    display_meta["site_source"] = "database"
    deal.notes_private = json.dumps({**existing, **display_meta})


def homepage_sections(db: Session) -> dict[str, List[models.Deal]]:
    now = datetime.now(HOME_TIMEZONE).replace(tzinfo=None)
    live_deals = sort_live_now_deals(
        [deal for deal in load_public_deals(db) if deal_is_live_now(deal, now)],
        now,
    )
    for deal in live_deals:
        ensure_public_display_metadata(deal, now)
    today_data = today_page_data(db)
    return {
        "live": live_deals,
        "today": today_data["all"],
    }


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
    image_src: str | None = None,
) -> str:
    class_names = f"icon-badge icon-badge-{escape(size)} {escape(extra_class)}".strip()
    content_html = (
        f'<img class="icon-badge-image" src="{escape(image_src)}" alt="" loading="lazy" decoding="async" />'
        if image_src
        else f'<span class="icon-badge-glyph">{escape(glyph)}</span>'
    )
    return (
        f'<span class="{class_names}" data-icon-kind="{escape(kind)}" '
        f'data-icon-key="{escape(icon_key)}" data-icon-render="{"image" if image_src else "glyph"}" aria-hidden="true">'
        f"{content_html}"
        f"</span>"
    )


def render_brand_slot(current_page: str) -> str:
    home_href = site_href("/")
    if current_page == "home":
        return ""
    return f"""
    <a class="brand-slot brand-slot-compact" href="{home_href}" aria-label="DSM Deals Hub home">
      {render_icon_badge(BRAND_PLACEHOLDER_GLYPH, kind="brand", size="brand", icon_key="dsm-deals-hub", extra_class="brand-mark", image_src=site_href(BRAND_MARK_STATIC_PATH))}
      <span class="brand-wordmark">
        <span class="brand-kicker">DSM</span>
        <span class="brand-name">Deals Hub</span>
        <span class="brand-meta">{BRAND_META_LABEL}</span>
      </span>
    </a>
    """


def render_brand_heading(title: str) -> str:
    home_href = site_href("/")
    brand_title = "Deals Hub" if title == "DSM Deals Hub" else title
    return f"""
    <h1>
      <a class="brand-slot brand-slot-heading" href="{home_href}" aria-label="DSM Deals Hub home">
        {render_icon_badge(BRAND_PLACEHOLDER_GLYPH, kind="brand", size="brand-hero", icon_key="dsm-deals-hub", extra_class="brand-mark", image_src=site_href(BRAND_MARK_STATIC_PATH))}
        <span class="brand-wordmark brand-wordmark-heading">
          <span class="brand-kicker">DSM</span>
          <span class="brand-name">{escape(brand_title)}</span>
          <span class="brand-meta brand-meta-heading">{BRAND_META_LABEL}</span>
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


def venue_directions_href(venue: object | None) -> Optional[str]:
    if not venue:
        return None

    lat = getattr(venue, "lat", None)
    lng = getattr(venue, "lng", None)
    if lat is not None and lng is not None:
        return f"https://www.google.com/maps/search/?api=1&query={lat},{lng}"

    address = getattr(venue, "address", None)
    if not address:
        return None

    cleaned = " ".join(str(address).split())
    if not cleaned:
        return None
    venue_name = " ".join(str(getattr(venue, "name", "")).split())
    query = f"{venue_name} {cleaned}".strip()
    return f"https://www.google.com/maps/search/?api=1&query={quote_plus(query)}"


def venue_call_href(venue: object | None) -> Optional[str]:
    if not venue:
        return None

    phone = getattr(venue, "phone", None)
    if not phone:
        return None

    digits = re.sub(r"\D+", "", str(phone))
    if len(digits) == 10:
        return f"tel:+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"tel:+{digits}"
    if len(digits) > 11:
        return f"tel:+{digits}"
    return None


def render_utility_action_icon(kind: str) -> str:
    if kind == "directions":
        return """
        <svg class="deal-utility-icon" viewBox="0 0 20 20" fill="none" aria-hidden="true">
          <path d="M10 17.1c3.18-3.87 4.77-6.64 4.77-8.31A4.77 4.77 0 1 0 5.23 8.8c0 1.67 1.59 4.44 4.77 8.3Z" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/>
          <circle cx="10" cy="8.7" r="1.9" stroke="currentColor" stroke-width="1.6"/>
        </svg>
        """
    return """
    <svg class="deal-utility-icon" viewBox="0 0 20 20" fill="none" aria-hidden="true">
      <path d="M6.32 3.56h2.02c.3 0 .56.2.64.48l.64 2.29a.7.7 0 0 1-.18.7l-1.03 1.03a10.54 10.54 0 0 0 3.53 3.53l1.03-1.03a.7.7 0 0 1 .7-.18l2.29.64c.28.08.48.34.48.64v2.02a.83.83 0 0 1-.83.83h-.63A11.48 11.48 0 0 1 4.5 4.4v-.63c0-.46.37-.83.82-.83Z" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/>
    </svg>
    """


def render_venue_utility_actions(venue: object | None) -> str:
    directions_href = venue_directions_href(venue)
    call_href = venue_call_href(venue)
    actions = []
    venue_name = escape(getattr(venue, "name", "this venue"))

    if directions_href:
        actions.append(
            f'<a class="deal-utility-action deal-utility-action-icononly" data-action-kind="directions" href="{escape(directions_href)}" aria-label="Get directions to {venue_name}" title="Directions" target="_blank" rel="noopener noreferrer">{render_utility_action_icon("directions")}</a>'
        )
    if call_href:
        actions.append(
            f'<a class="deal-utility-action deal-utility-action-icononly" data-action-kind="call" href="{escape(call_href)}" aria-label="Call {venue_name}" title="Call">{render_utility_action_icon("call")}</a>'
        )

    if not actions:
        return ""

    return f'<div class="deal-card-actions" aria-label="Venue actions">{"".join(actions)}</div>'


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
    utility_actions = render_venue_utility_actions(deal.venue)

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
      {utility_actions}
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
    collapsible: bool = False,
    initially_open: bool = True,
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

    heading_html = f"""
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
            {action_html if not collapsible else ""}
          </div>
        </div>
    """

    if collapsible:
        open_attr = " open" if initially_open else ""
        body_action_html = ""
        if action_html:
            body_action_html = f'<div class="detail-section-body-actions">{action_html}</div>'
        return f"""
    <section id="{section_id}" class="{escape(section_class)}">
      <details class="{escape(panel_class)} detail-section-module" data-toggle-glow{open_attr}>
        <summary class="detail-section-summary">
          {heading_html}
          <span class="detail-section-arrow" aria-hidden="true">›</span>
        </summary>
        <div class="detail-section-details">
          <div class="detail-section-details-inner">
            {body_action_html}
            {controls}
            <div class="{grid_class}" data-sort-grid="{escape(section_id)}">
              {cards}
            </div>
          </div>
        </div>
      </details>
    </section>
        """

    return f"""
    <section id="{section_id}" class="{escape(section_class)}">
      <div class="{escape(panel_class)}">
        {heading_html}
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


def render_collapsible_sections_script() -> str:
    return """
    <script>
      (() => {
        const toggles = document.querySelectorAll('[data-toggle-glow]');
        if (!toggles.length) {
          return;
        }

        toggles.forEach((toggle) => {
          toggle.addEventListener('toggle', () => {
            toggle.classList.remove('is-toggling');
            void toggle.offsetWidth;
            toggle.classList.add('is-toggling');
            window.setTimeout(() => {
              toggle.classList.remove('is-toggling');
            }, 320);
          });
        });
      })();
    </script>
    """


def render_site_nav(current_page: str) -> str:
    links = [
        ("Today", site_href("/today"), current_page == "today"),
        ("Days", site_href("/days"), current_page == "days"),
        ("Neighborhoods", site_href("/neighborhoods"), current_page == "neighborhoods"),
        (PUBLIC_VENUES_LABEL, site_href("/for-venues"), current_page == "for-venues"),
    ]
    items = []
    for label, href, active in links:
        current_attr = ' aria-current="page"' if active else ""
        extra_class = ' class="site-nav-cta"' if label == PUBLIC_VENUES_LABEL else ""
        items.append(f'<a href="{href}"{extra_class}{current_attr}>{escape(label)}</a>')
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
          The guide leans on recurring weekday boards and weekend patterns. Browse neighborhoods, open Days, or check back when the daily board turns over.
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
        "Open Today, or browse Neighborhoods and Days while the board resets.",
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
    include_toggle_script: bool = False,
) -> str:
    sort_script = render_homepage_script() if include_sort_script else ""
    toggle_script = render_collapsible_sections_script() if include_toggle_script else ""
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
    <link rel="icon" type="image/png" href="{site_href(FAVICON_STATIC_PATH)}" />
    <link rel="apple-touch-icon" href="{site_href(APPLE_TOUCH_ICON_STATIC_PATH)}" />
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
    {toggle_script}
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
    resolved_icon_kind = icon_kind or variant
    resolved_icon_key = icon_key or normalize_slug(title)
    image_src = None
    if resolved_icon_kind == "neighborhood":
        icon_asset_path = neighborhood_icon_static_path(resolved_icon_key)
        if icon_asset_path:
            image_src = site_href(f"/static/{icon_asset_path}")
    return f"""
    <a class="browse-card browse-card-{escape(variant)}" href="{href}">
      <div class="browse-card-top">
        {render_icon_badge(icon, kind=resolved_icon_kind, size="lg", icon_key=resolved_icon_key, extra_class=f"browse-card-icon {icon_extra_class}".strip(), image_src=image_src)}
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
    details_class = "live-now-module has-live-deals" if deals else "live-now-module"
    live_cards = "\n".join(
        render_deal_card(deal, reference, index, format_today_pick_time(deal, reference))
        for index, deal in enumerate(deals)
    )
    if deals:
        details_body = f"""
        <div class="live-now-details-grid">
          {live_cards}
        </div>
        """
    else:
        details_body = f"""
        <div class="live-now-empty">
          <p>No exact-timed specials are active right now. The fuller daily browse usually lives in Today, Neighborhoods, and Days.</p>
          <div class="empty-state-links">
            <a href="{site_href("/today")}" class="empty-state-link">See Today</a>
            <a href="{site_href("/days")}" class="empty-state-link">Browse Days</a>
          </div>
        </div>
        """

    return f"""
    <section id="live-now" class="content-section content-section-secondary homepage-section homepage-section-live">
      <div class="section-panel section-panel-secondary section-panel-live">
        <details class="{details_class}">
          <summary class="live-now-summary">
            <div class="live-now-summary-copy">
              <span class="live-now-badge"><span class="live-now-dot" aria-hidden="true"></span>Live Now</span>
              <h2>Deals happening right now</h2>
              <p>Deals currently happening right now. Support local and don’t forget to tip.</p>
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


def _supabase_config() -> Optional[tuple[str, str]]:
    base_url = (
        os.getenv("SUPABASE_URL")
        or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
        or ""
    ).strip().rstrip("/")
    api_key = next(
        (
            os.getenv(name)
            for name in (
                "SUPABASE_SERVICE_ROLE_KEY",
                "SUPABASE_ANON_KEY",
                "SUPABASE_KEY",
                "NEXT_PUBLIC_SUPABASE_ANON_KEY",
            )
            if os.getenv(name)
        ),
        "",
    ).strip()
    if not base_url or not api_key:
        return None
    return base_url, api_key


def _parse_supabase_datetime(value: object) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _fallback_supabase_notes_private(row: dict, reference: datetime) -> str:
    day_code = weekly_content.normalize_day_code_value(reference.strftime("%a"))
    day_name = weekly_content.DAY_CODE_TO_NAME[day_code]
    start_time = row.get("start_time")
    end_time = row.get("end_time")
    title = row.get("title") or ""
    description = row.get("short_description") or ""

    if start_time and end_time:
        start_label = format_clock(*parse_hhmm(start_time))
        end_label = format_clock(*parse_hhmm(end_time))
        time_label = f"{start_label} to {end_label}"
    elif start_time:
        time_label = f"After {format_clock(*parse_hhmm(start_time))}"
    else:
        time_label = f"Today"

    text = f"{title} {description}".lower()
    category = "Drink special" if any(
        keyword in text for keyword in ("happy hour", "wine", "beer", "pint", "cocktail", "martini")
    ) else "Food and drink special"
    day_part = weekly_content._classify_day_part(category, time_label, title, description, start_time)
    happy_hour = weekly_content._is_happy_hour(category, time_label, title, description, start_time)
    return weekly_content._build_notes_meta(
        day_name=day_name,
        day_code=day_code,
        time_label=time_label,
        category=category,
        day_part=day_part,
        happy_hour=happy_hour,
    )


def _load_supabase_rows(table: str, select: str, filters: list[tuple[str, str]]) -> list[dict]:
    config = _supabase_config()
    if config is None:
        return []

    base_url, api_key = config
    query = urlencode([("select", select), *filters])
    request = UrlRequest(
        f"{base_url}/rest/v1/{table}?{query}",
        headers={
            "apikey": api_key,
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
        },
        method="GET",
    )

    with urlopen(request, timeout=10) as response:
        payload = response.read().decode("utf-8")

    rows = json.loads(payload)
    if not isinstance(rows, list):
        raise RuntimeError(f"Unexpected Supabase response for {table}")
    return [row for row in rows if isinstance(row, dict)]


def _build_supabase_venue(row: dict) -> models.Venue:
    return models.Venue(
        id=row["id"],
        owner_id=row.get("owner_id"),
        name=row["name"],
        slug=row["slug"],
        address=row["address"],
        neighborhood=row.get("neighborhood"),
        lat=row.get("lat"),
        lng=row.get("lng"),
        phone=row.get("phone"),
        website=row.get("website"),
        hours_json=row.get("hours_json"),
        description=row.get("description"),
        created_at=_parse_supabase_datetime(row.get("created_at")) or datetime.utcnow(),
        updated_at=_parse_supabase_datetime(row.get("updated_at")) or datetime.utcnow(),
    )


def _build_supabase_deal(row: dict, venue: models.Venue, reference: datetime) -> models.Deal:
    notes_private = row.get("notes_private")
    if notes_private:
        try:
            payload = json.loads(notes_private)
            if not isinstance(payload, dict) or not payload.get("day_code"):
                notes_private = _fallback_supabase_notes_private(row, reference)
        except (TypeError, ValueError):
            notes_private = _fallback_supabase_notes_private(row, reference)
    else:
        notes_private = _fallback_supabase_notes_private(row, reference)

    return models.Deal(
        id=row["id"],
        venue_id=row["venue_id"],
        title=row["title"],
        short_description=row["short_description"],
        type=models.DealType(row["type"]),
        weekday_pattern=row.get("weekday_pattern"),
        start_time=row.get("start_time"),
        end_time=row.get("end_time"),
        start_at=_parse_supabase_datetime(row.get("start_at")),
        end_at=_parse_supabase_datetime(row.get("end_at")),
        age_21_plus=bool(row.get("age_21_plus", False)),
        menu_link=row.get("menu_link"),
        image_url=row.get("image_url"),
        sponsored=bool(row.get("sponsored", False)),
        status=models.Status(row["status"]),
        source_type=row.get("source_type") or "admin",
        source_url=row.get("source_url"),
        source_text=row.get("source_text"),
        source_posted_at=_parse_supabase_datetime(row.get("source_posted_at")),
        notes_private=notes_private,
        freeze_minutes=row.get("freeze_minutes"),
        created_at=_parse_supabase_datetime(row.get("created_at")) or datetime.utcnow(),
        updated_at=_parse_supabase_datetime(row.get("updated_at")) or datetime.utcnow(),
        venue=venue,
    )


def _load_supabase_today_deals(reference: datetime) -> Optional[List[models.Deal]]:
    config = _supabase_config()
    if config is None:
        return None

    try:
        venues = {
            row["id"]: _build_supabase_venue(row)
            for row in _load_supabase_rows(
                "venues",
                "id,owner_id,name,slug,address,neighborhood,lat,lng,phone,website,hours_json,description,created_at,updated_at",
                [("order", "id.asc")],
            )
            if compact_text(row.get("name")).lower() not in PUBLIC_EXCLUDED_VENUE_NAMES
        }
        deals = []
        for row in _load_supabase_rows(
            "deals",
            "id,venue_id,title,short_description,type,weekday_pattern,start_time,end_time,start_at,end_at,age_21_plus,menu_link,image_url,sponsored,status,source_type,source_url,source_text,source_posted_at,notes_private,freeze_minutes,created_at,updated_at",
            [
                ("type", "eq.weekly"),
                ("status", "eq.live"),
                ("order", "id.asc"),
            ],
        ):
            venue = venues.get(row["venue_id"])
            if venue is None:
                continue
            deal = _build_supabase_deal(row, venue, reference)
            if deal.weekday_pattern and matches_weekday_pattern(deal.weekday_pattern, reference):
                deals.append(deal)
        return deals
    except Exception:
        return None


def _load_db_today_deals(db: Session, reference: datetime) -> List[models.Deal]:
    deals = [
        deal
        for deal in load_public_deals(db)
        if deal.type == models.DealType.weekly
        and deal.weekday_pattern
        and matches_weekday_pattern(deal.weekday_pattern, reference)
    ]
    for deal in deals:
        ensure_public_display_metadata(deal, reference)
    return deals


def today_page_data(db: Session) -> dict:
    reference = datetime.now(HOME_TIMEZONE).replace(tzinfo=None)
    day_code = weekly_content.normalize_day_code_value(reference.strftime("%a"))
    db_deals = _load_db_today_deals(db, reference)
    if db_deals:
        all_today = weekly_content.sort_day_deals(db_deals)
        happy_hour = [deal for deal in all_today if weekly_content.site_deal_is_happy_hour(deal)]
        specials = [deal for deal in all_today if deal not in happy_hour]
        return {
            "day_code": day_code,
            "day_label": weekly_content.DAY_CODE_TO_NAME[day_code],
            "all": all_today,
            "happy_hour": happy_hour,
            "specials": specials,
        }

    supabase_deals = _load_supabase_today_deals(reference)
    if supabase_deals is None:
        return weekly_content.today_page_data(reference)

    all_today = weekly_content.sort_day_deals(supabase_deals)
    happy_hour = [deal for deal in all_today if weekly_content.site_deal_is_happy_hour(deal)]
    specials = [deal for deal in all_today if deal not in happy_hour]
    day_code = weekly_content.normalize_day_code_value(reference.strftime("%a"))
    return {
        "day_code": day_code,
        "day_label": weekly_content.DAY_CODE_TO_NAME[day_code],
        "all": all_today,
        "happy_hour": happy_hour,
        "specials": specials,
    }


def neighborhood_groups(db: Session) -> List[dict]:
    groups = weekly_content.neighborhood_groups(datetime.now(HOME_TIMEZONE).replace(tzinfo=None))
    for group in groups:
        group["display_name"] = public_neighborhood_name(group["name"])
        group["icon"] = neighborhood_placeholder_icon(group["display_name"])
    return groups


def get_neighborhood_group_or_404(db: Session, slug: str) -> dict:
    normalized = NEIGHBORHOOD_ROUTE_ALIASES.get(normalize_slug(slug), normalize_slug(slug))
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
                f"Start with {item.get('display_name', item['name'])}, then open the strongest current local picks."
                if context == "homepage"
                else f"Open the current guide for {item.get('display_name', item['name'])}."
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
        <span>Today&#x27;s picks</span>
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
        "We suspect that’s why you’re here ;)",
        today_preview,
        "Nothing is on the board for today yet.",
        now,
        time_labels=today_time_labels,
        action_label="Open Today",
        action_href=site_href("/today"),
        section_class="content-section homepage-section homepage-section-today",
        panel_class="section-panel homepage-panel homepage-panel-today",
        kicker_label="Today’s picks",
    )}
    <div class="homepage-browse-band">
    {render_link_grid_section(
        "neighborhoods-preview",
        "Neighborhoods",
        "Don’t wanna drive too far, check out each neighborhood",
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
        "Oh yes we appreciate a planner, or a serial deal hopper",
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
            <p class="section-kicker">{PUBLIC_VENUES_LABEL}</p>
            <h2>Curated for readers. Ready for venue submissions.</h2>
            <p>
              DSM Deals Hub stays hand-curated so the public side reads like a local guide, not a feed dump. Deal submissions and future self-serve tools still live here as the business side grows.
            </p>
          </div>
          <div class="live-empty-actions">
            <div class="empty-state-links">
              <a href="{site_href("/for-venues")}" class="empty-state-link">{PUBLIC_VENUES_LABEL}</a>
              <a href="{site_href("/neighborhoods")}" class="empty-state-link">Browse neighborhoods</a>
            </div>
            <a class="empty-state-cta" href="{site_href("/for-venues")}" aria-label="Share your deal">
              <span class="empty-state-cta-plus">+</span>
              <span class="empty-state-cta-copy">
                <strong>Share your deal</strong>
                <small>Accepting deals for today!</small>
              </span>
            </a>
          </div>
        </div>
      </div>
    </section>
    </div>
    """
    return render_page_document(
        "DSM Deals Hub",
        "Curated food and drink deals across Des Moines, updated manually.",
        BRAND_HOME_EYEBROW,
        "DSM Deals Hub",
        "A hand-curated daily guide to neighborhood specials, happy hour, brunch, dinner plans, and weekly favorites across Des Moines.",
        "home",
        main_content,
        utility_html=utility_html,
    )


def render_today_html(data: dict) -> str:
    now = datetime.now(HOME_TIMEZONE).replace(tzinfo=None)
    today_sections, _featured = day_detail_sections_for_page(data["day_code"], data["all"])
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
    sections_html = "".join(
        render_section(
            f"today-{normalize_slug(section['title'])}",
            section["title"],
            section["intro"],
            section["deals"],
            f"No {section['title'].lower()} picks are listed for {data['day_label']} yet.",
            now,
            time_labels=[format_today_pick_time(deal, now) for deal in section["deals"]],
            section_class="content-section day-detail-section",
            panel_class="section-panel homepage-panel homepage-panel-today day-detail-panel",
            kicker_label=section["title"],
            collapsible=True,
            initially_open=index == 0,
        )
        for index, section in enumerate(today_sections)
    )
    main_content = f"""
    {featured_empty}
    {sections_html}
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
        include_toggle_script=True,
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
        "Choose a part of town, then open a tighter local guide to restaurants, bars, and recurring specials.",
        render_neighborhood_cards(groups),
        "Open Days",
        site_href("/days"),
    )
    return render_page_document(
        "DSM Deals Hub | Neighborhoods",
        "Browse Des Moines dining specials by neighborhood.",
        "Browse by place",
        "Neighborhoods",
        "Start with the part of town that matters most, then open a tighter local guide with the strongest current picks.",
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
            f"Food, drinks, and both are split below so the area reads like a tighter local guide for today and the rest of the week."
        )
    if total_grouped and live_music_count:
        return (
            f"{group['venue_count']} spots and {group['deal_count']} specials shape the {display_name} guide. "
            f"Food, drinks, and both are grouped separately below, with any live music note kept outside the main deal stack."
        )
    if total_grouped:
        return (
            f"{group['venue_count']} spots and {group['deal_count']} specials shape the {display_name} guide. "
            f"Browse the neighborhood by food, drinks, or spots where both are part of the same deal."
        )
    return (
        f"{group['venue_count']} spots and {group['deal_count']} specials shape the {display_name} guide. "
        f"This area is lighter right now, with a smaller neighborhood note still captured on the board."
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


def deal_is_midday_food(deal: models.Deal) -> bool:
    text = " ".join(
        part
        for part in (
            deal.title,
            deal.short_description,
            weekly_content.site_deal_time_label(deal),
            weekly_content.site_deal_category(deal),
        )
        if part
    ).lower()
    return (
        weekly_content.site_deal_category(deal).lower() == "lunch special"
        or "lunch" in text
        or weekly_content.site_deal_day_part(deal) == "Brunch"
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
    "brunch",
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
            "Both",
            "Both",
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
            collapsible=True,
            initially_open=index == 0,
        )
        for index, section in enumerate(detail_sections)
    )
    return render_page_document(
        f"DSM Deals Hub | {display_name}",
        f"Browse featured dining deals in {display_name}.",
        "Neighborhood guide",
        display_name,
        neighborhood_feature_intro(group, now, food_count, drinks_count, both_count, today_count, live_music_count),
        "neighborhoods",
        main_content,
        utility_html=utility_html,
        include_toggle_script=True,
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
        "Open the day that fits your plans and browse the week the way people actually use it: flatter weekdays, fuller weekends.",
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
        "Browse the week in calendar order, then open the day that fits your plans.",
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
    buckets = {"Happy Hour": [], "Midday": [], "Dinner": [], "Specials": [], "Late Night": []}

    for deal in deals:
        day_part = weekly_content.site_deal_day_part(deal)
        if day_part == "Late Night":
            buckets["Late Night"].append(deal)
        elif weekly_content.site_deal_is_happy_hour(deal):
            buckets["Happy Hour"].append(deal)
        elif deal_is_midday_food(deal):
            buckets["Midday"].append(deal)
        elif day_part == "Dinner":
            buckets["Dinner"].append(deal)
        else:
            buckets["Specials"].append(deal)

    midday_deals = buckets["Midday"]
    midday_title = "Lunch"
    midday_intro = f"Earlier-day food-led stops and lunch-focused specials currently landing on the {day_label} board."
    if midday_deals:
        midday_has_brunch = any(
            weekly_content.site_deal_day_part(deal) == "Brunch"
            or "brunch" in f"{deal.title} {deal.short_description} {weekly_content.site_deal_time_label(deal)}".lower()
            or "breakfast" in f"{deal.title} {deal.short_description} {weekly_content.site_deal_time_label(deal)}".lower()
            for deal in midday_deals
        )
        midday_has_lunch = any(
            "lunch" in f"{deal.title} {deal.short_description} {weekly_content.site_deal_time_label(deal)} {weekly_content.site_deal_category(deal)}".lower()
            for deal in midday_deals
        )
        if midday_has_brunch and midday_has_lunch:
            midday_title = "Brunch + Lunch"
            midday_intro = f"Earlier-day food-led stops, from brunch boards to clear lunch windows, currently shaping the {day_label} guide."
        elif midday_has_brunch:
            midday_title = "Brunch"
            midday_intro = f"Breakfast, brunch, and first-meal stops currently shaping the {day_label} board."

    intros = {
        "Happy Hour": f"Timed pours, patio windows, and after-work value leading the {day_label} board.",
        "Midday": midday_intro,
        "Dinner": f"Dinner-led specials, bigger plates, and stronger evening picks on {day_label}.",
        "Specials": f"The main {day_label} board, from lunch runs to all-day house specials and neighborhood staples.",
        "Late Night": f"Later-night stops and extended windows that keep going after the main {day_label} dinner stretch.",
    }

    sections = []
    for bucket_key in ("Happy Hour", "Midday", "Dinner", "Specials", "Late Night"):
        section_deals = buckets[bucket_key]
        if not section_deals:
            continue
        ordered = sort_day_section_deals(day_code, section_deals, featured_ids)
        title = midday_title if bucket_key == "Midday" else bucket_key
        sections.append(
            {
                "title": title,
                "intro": intros[bucket_key],
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
    ordered_keys = ["Brunch", "Afternoon", "Happy Hour", "Dinner", "Late Night", "Live Music", "Specials"]
    grouped: dict[str, List[models.Deal]] = {section: [] for section in ordered_keys}
    for deal in deals:
        day_part = weekly_content.site_deal_day_part(deal)
        if day_part == "Late Night":
            grouped["Late Night"].append(deal)
        elif day_part == "Live Music":
            grouped["Live Music"].append(deal)
        elif weekly_content.site_deal_is_happy_hour(deal):
            grouped["Happy Hour"].append(deal)
        elif day_part in {"Brunch", "Afternoon", "Dinner"}:
            grouped[day_part].append(deal)
        else:
            grouped["Specials"].append(deal)

    weekend_intros = dict(weekly_content.WEEKEND_SECTION_INTROS)
    weekend_intros["Happy Hour"] = "Shorter pour windows, aperitivo-style stops, and drink-led value worth catching before dinner."

    sections = []
    for title in ordered_keys:
        section_deals = grouped.get(title) or []
        if not section_deals:
            continue
        ordered = sort_day_section_deals(day_code, section_deals, featured_ids)
        sections.append(
            {
                "title": title,
                "intro": weekend_intros[title],
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
    if first_section in {"Lunch", "Brunch", "Brunch + Lunch"}:
        return (
            f"Start {day_label} with the stronger earlier-day picks first, then move into dinner, later windows, and the rest of the citywide board."
        )
    return (
        f"A cleaner weekday edit for {day_label}, with the strongest current picks surfaced first and the rest of the board organized underneath."
    )


def render_day_detail_html(day_code: str, deals: List[models.Deal]) -> str:
    now = datetime.now(HOME_TIMEZONE).replace(tzinfo=None)
    day_label = WEEKDAY_LONG[day_code]
    detail_sections, featured = day_detail_sections_for_page(day_code, deals)
    venue_count = len({deal.venue_id for deal in deals})
    happy_hour_count = sum(
        1
        for deal in deals
        if weekly_content.site_deal_is_happy_hour(deal) and weekly_content.site_deal_day_part(deal) != "Late Night"
    )
    guide_label = "Weekend guide" if day_code in {"Sat", "Sun"} else "Weekday edit"
    description = day_detail_hero_text(day_code, deals, detail_sections, featured)
    stats = [
        ("Specials", len(deals)),
        ("Venues", venue_count),
    ]
    if happy_hour_count:
        stats.append(("Happy Hour", happy_hour_count))
    stats_html = "".join(
        f"""
      <div>
        <span>{escape(label)}</span>
        <strong>{value}</strong>
      </div>
        """
        for label, value in stats
    )
    utility_html = f"""
    <div class="day-detail-utility">
      <div class="day-nav-shell">
        <p class="day-nav-label">Jump to another day</p>
        {render_day_page_nav(day_code)}
      </div>
      <div class="hero-stats day-detail-stats" aria-label="{escape(day_label)} overview">
        {stats_html}
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
            section_class="content-section day-detail-section",
            panel_class=(
                "section-panel homepage-panel homepage-panel-today day-detail-panel day-detail-panel-featured"
                if index == 0
                else f"section-panel homepage-panel day-detail-panel{' day-detail-panel-live-music' if section['title'] == 'Live Music' else ''}"
            ),
            kicker_label="Start here" if index == 0 else section["title"],
            collapsible=True,
            initially_open=index == 0,
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
        include_toggle_script=True,
    )


def render_for_venues_html() -> str:
    submission_embed = VENUE_SUBMISSION_EMBED_HTML.strip()
    embed_block = (
        submission_embed
        if submission_embed
        else """
        <div class="form-embed-placeholder">
          <strong>Submission form embed will appear here.</strong>
          <p>This section is ready for a Google Form or Jotform embed.</p>
        </div>
        """
    )
    submission_panel = f"""
    <section class="content-section">
      <div class="section-panel section-panel-secondary">
        <div class="for-venues-panel">
          <div class="for-venues-copy">
            <p class="section-kicker">Manual review</p>
            <p>{FOR_VENUES_TRUST_LINE}</p>
          </div>
          <div class="form-embed-shell" aria-label="Business submission form">
            {embed_block}
          </div>
        </div>
        <p class="for-venues-note">{FOR_VENUES_FUTURE_NOTE}</p>
      </div>
    </section>
    """
    about_panel = f"""
    <section class="content-section">
      <div class="section-panel">
        <div class="about-dsm-copy">
          <p class="section-kicker">About DSM Deals Hub</p>
          <h2>{FOR_VENUES_ABOUT_HEADING}</h2>
          <p>{FOR_VENUES_ABOUT_PARAGRAPH}</p>
        </div>
      </div>
    </section>
    """
    main_content = f"""
    {submission_panel}
    {about_panel}
    """
    return render_page_document(
        f"DSM Deals Hub | {PUBLIC_VENUES_LABEL}",
        "Learn how businesses can submit deals for review and how direct account access will work later.",
        "For local businesses",
        PUBLIC_VENUES_LABEL,
        "If your business has a special worth sharing, send it our way. DSM Deals Hub is built to help people find real food and drink deals across Des Moines, and to help local spots fill seats on the days they need it most.",
        "for-venues",
        main_content,
    )


INTAKE_ACTIONS = {
    "add_deal",
    "update_deal",
    "archive_deal",
    "venue_closed",
    "ignore",
    "needs_human_review",
}
LOW_CONFIDENCE_THRESHOLD = 0.65
FREEZE_SENTINEL_MINUTES = 525600
ADMIN_DEAL_STATUS_FILTERS = ("active", "live", "draft", "queued", "archived", "expired", "rejected", "all")
ACTIVE_ADMIN_DEAL_STATUSES = (models.Status.live, models.Status.queued, models.Status.draft)
PUBLIC_EXCLUDED_VENUE_NAMES = {"django"}
DAY_ALIASES = {
    "monday": "Mon",
    "mondays": "Mon",
    "mon": "Mon",
    "tuesday": "Tue",
    "tuesdays": "Tue",
    "tue": "Tue",
    "tues": "Tue",
    "wednesday": "Wed",
    "wednesdays": "Wed",
    "wed": "Wed",
    "thursday": "Thu",
    "thursdays": "Thu",
    "thu": "Thu",
    "thur": "Thu",
    "thurs": "Thu",
    "friday": "Fri",
    "fridays": "Fri",
    "fri": "Fri",
    "saturday": "Sat",
    "saturdays": "Sat",
    "sat": "Sat",
    "sunday": "Sun",
    "sundays": "Sun",
    "sun": "Sun",
    "daily": "All",
    "everyday": "All",
    "every day": "All",
    "all": "All",
}


def json_default(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "value"):
        return value.value
    return str(value)


def compact_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def clean_optional(value: object) -> Optional[str]:
    cleaned = compact_text(value)
    return cleaned or None


def google_maps_search_url(*parts: object) -> str:
    query = " ".join(compact_text(part) for part in parts if compact_text(part))
    if not query:
        query = "Des Moines restaurant"
    return f"https://www.google.com/maps/search/?api=1&query={quote_plus(query)}"


def normalize_intake_time(value: object) -> Optional[str]:
    cleaned = compact_text(value)
    if not cleaned:
        return None
    try:
        parse_hhmm(cleaned)
        return cleaned
    except Exception:
        pass
    match = re.fullmatch(r"(?i)(\d{1,2})(?::(\d{2}))?\s*([ap])\.?m\.?", cleaned)
    if not match:
        return None
    hour = int(match.group(1))
    minute = int(match.group(2) or "0")
    meridiem = match.group(3).lower()
    if meridiem == "p" and hour != 12:
        hour += 12
    if meridiem == "a" and hour == 12:
        hour = 0
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return None
    return f"{hour:02d}:{minute:02d}"


def normalize_intake_datetime(value: object) -> Optional[str]:
    cleaned = compact_text(value)
    if not cleaned:
        return None
    try:
        return datetime.fromisoformat(cleaned.replace("Z", "+00:00")).isoformat()
    except Exception:
        return None


def normalize_intake_days(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        days: list[str] = []
        for part in value:
            mapped = DAY_ALIASES.get(compact_text(part).lower().rstrip("."))
            if mapped == "All":
                return DAY_ORDER.copy()
            if mapped and mapped not in days:
                days.append(mapped)
        return days
    text = compact_text(value).lower()
    if not text:
        return []
    if any(phrase in text for phrase in ("daily", "everyday", "every day", "all week")):
        return DAY_ORDER.copy()
    days: list[str] = []
    day_patterns = [
        ("Mon", r"\bmondays?\b|\bmon\.?\b"),
        ("Tue", r"\btuesdays?\b|\btues?\.?\b"),
        ("Wed", r"\bwednesdays?\b|\bwed\.?\b"),
        ("Thu", r"\bthursdays?\b|\bthurs?\.?\b|\bthu\.?\b"),
        ("Fri", r"\bfridays?\b|\bfri\.?\b"),
        ("Sat", r"\bsaturdays?\b|\bsat\.?\b"),
        ("Sun", r"\bsundays?\b|\bsun\.?\b"),
    ]
    for day_code, pattern in day_patterns:
        if re.search(pattern, text, flags=re.IGNORECASE) and day_code not in days:
            days.append(day_code)
    if days:
        return days
    raw_parts = re.split(r"[,/&+]|\band\b|\bthrough\b|\bthru\b|-", text, flags=re.IGNORECASE)
    for part in raw_parts:
        mapped = DAY_ALIASES.get(compact_text(part).lower().rstrip("."))
        if mapped == "All":
            return DAY_ORDER.copy()
        if mapped and mapped not in days:
            days.append(mapped)
    return days


def infer_venue_name_from_text(raw_text: str) -> Optional[str]:
    cleaned = re.sub(r"(?i)^\s*test\s+deal\s*:\s*", "", compact_text(raw_text))
    if not cleaned:
        return None
    match = re.match(
        r"(?P<venue>[A-Z0-9][A-Za-z0-9'&.\- ]{1,80}?)\s+"
        r"(?:has|have|offers?|serves?|features?|is offering|is serving|announced|posted)\b",
        cleaned,
    )
    if match:
        return clean_optional(match.group("venue"))
    return None


def infer_deal_title_from_text(raw_text: str) -> Optional[str]:
    cleaned = re.sub(r"(?i)^\s*test\s+deal\s*:\s*", "", compact_text(raw_text))
    match = re.search(
        r"\b(?:has|have|offers?|serves?|features?|is offering|is serving)\s+(.+?)"
        r"(?=\s+(?:every|on)\s+(?:mon|monday|tue|tues|tuesday|wed|wednesday|thu|thur|thurs|thursday|fri|friday|sat|saturday|sun|sunday)s?\b|\s+from\s+\d|\s+between\s+\d|[.!?]|$)",
        cleaned,
        flags=re.IGNORECASE,
    )
    if match:
        return clean_optional(match.group(1))
    first_line = next((line.strip() for line in raw_text.splitlines() if line.strip()), "")
    return clean_optional(re.sub(r"(?i)^\s*test\s+deal\s*:\s*", "", first_line)[:90])


def normalize_match_text(value: object) -> str:
    return " ".join(re.findall(r"[a-z0-9]+", compact_text(value).lower()))


def match_tokens(value: object) -> set[str]:
    return set(re.findall(r"[a-z0-9]+", compact_text(value).lower()))


def find_venue_by_name_input(db: Session, venue_query: object) -> Optional[models.Venue]:
    query_text = normalize_match_text(venue_query)
    if not query_text:
        return None
    venues = db.query(models.Venue).order_by(models.Venue.name.asc()).all()
    exact = [venue for venue in venues if normalize_match_text(venue.name) == query_text]
    if len(exact) == 1:
        return exact[0]
    scored = []
    for venue in venues:
        venue_text = normalize_match_text(venue.name)
        ratio = SequenceMatcher(None, query_text, venue_text).ratio()
        overlap = len(match_tokens(query_text) & match_tokens(venue.name))
        if query_text in venue_text or venue_text in query_text:
            ratio += 0.2
        scored.append((ratio, overlap, venue))
    scored.sort(key=lambda item: (-item[0], -item[1], item[2].name.lower()))
    if scored and (scored[0][0] >= 0.78 or scored[0][1] >= 2):
        return scored[0][2]
    return None


def find_exact_venue_by_name(db: Session, venue_name: object) -> Optional[models.Venue]:
    query_text = normalize_match_text(venue_name)
    if not query_text:
        return None
    venues = db.query(models.Venue).order_by(models.Venue.name.asc()).all()
    matches = [venue for venue in venues if normalize_match_text(venue.name) == query_text]
    return matches[0] if len(matches) == 1 else None


def sanitize_intake_payload(payload: dict, raw_text: str) -> dict:
    action = compact_text(payload.get("action") or "needs_human_review")
    if action not in INTAKE_ACTIONS:
        action = "needs_human_review"
    warnings = payload.get("warnings")
    if isinstance(warnings, str):
        warnings = [warnings]
    if not isinstance(warnings, list):
        warnings = []
    try:
        confidence = float(payload.get("confidence", 0))
    except (TypeError, ValueError):
        confidence = 0.0
    confidence = min(max(confidence, 0.0), 1.0)
    start_at = normalize_intake_datetime(payload.get("start_at"))
    end_at = normalize_intake_datetime(payload.get("end_at"))
    deal_type = compact_text(payload.get("deal_type")).lower()
    if deal_type not in {"weekly", "last_minute"}:
        deal_type = "last_minute" if start_at or end_at else "weekly"
    return {
        "action": action,
        "venue_name": clean_optional(payload.get("venue_name")),
        "venue_address": clean_optional(payload.get("venue_address")),
        "deal_title": clean_optional(payload.get("deal_title")),
        "description": clean_optional(payload.get("description")),
        "days": normalize_intake_days(payload.get("days")),
        "start_time": normalize_intake_time(payload.get("start_time")),
        "end_time": normalize_intake_time(payload.get("end_time")),
        "start_at": start_at,
        "end_at": end_at,
        "deal_type": deal_type,
        "status": compact_text(payload.get("status") or "live") or "live",
        "confidence": confidence,
        "warnings": [compact_text(item) for item in warnings if compact_text(item)],
        "source_text": raw_text,
    }


def rules_deal_parse(raw_text: str) -> dict:
    lower = raw_text.lower()
    days = normalize_intake_days(raw_text)
    times = re.findall(r"(?i)\b\d{1,2}(?::\d{2})?\s*[ap]\.?m\.?", raw_text)
    venue_name = infer_venue_name_from_text(raw_text)
    deal_title = infer_deal_title_from_text(raw_text)
    action = "archive_deal" if any(word in lower for word in ("closed", "cancelled", "canceled", "no longer")) else "add_deal"
    return sanitize_intake_payload(
        {
            "action": action,
            "venue_name": venue_name,
            "deal_title": deal_title or "Deal from source post",
            "description": raw_text[:280],
            "days": days,
            "start_time": times[0] if len(times) >= 1 else None,
            "end_time": times[1] if len(times) >= 2 else None,
            "deal_type": "weekly",
            "status": "draft",
            "confidence": 0.25,
            "warnings": ["Rules parser used. Review all fields before approving."],
        },
        raw_text,
    )


def mock_deal_parse(raw_text: str) -> dict:
    parsed = rules_deal_parse(raw_text)
    parsed["confidence"] = 0.1
    parsed["warnings"] = ["Mock parser used for tests. Fill fields manually before approving."]
    return parsed


def empty_manual_proposal(raw_text: str, warning: str) -> dict:
    return sanitize_intake_payload(
        {
            "action": "needs_human_review",
            "venue_name": None,
            "venue_address": None,
            "deal_title": None,
            "description": None,
            "days": [],
            "start_time": None,
            "end_time": None,
            "deal_type": "weekly",
            "status": "draft",
            "confidence": 0,
            "warnings": [warning],
        },
        raw_text,
    )


def ollama_deal_parse(raw_text: str) -> dict:
    if os.getenv("VERCEL") == "1":
        raise RuntimeError("Ollama is local-only. Run the admin intake locally.")
    base_url = os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434").rstrip("/")
    schema_hint = {
        "action": "add_deal|update_deal|archive_deal|venue_closed|ignore|needs_human_review",
        "venue_name": "string|null",
        "venue_address": "string|null",
        "deal_title": "string|null",
        "description": "string|null",
        "days": ["Mon|Tue|Wed|Thu|Fri|Sat|Sun"],
        "start_time": "HH:MM|null",
        "end_time": "HH:MM|null",
        "start_at": "ISO datetime|null",
        "end_at": "ISO datetime|null",
        "deal_type": "weekly|last_minute",
        "status": "draft|queued|live",
        "confidence": "0.0-1.0",
        "warnings": ["string"],
        "source_text": "verbatim source text",
    }
    body = {
        "model": os.getenv("OLLAMA_MODEL", "qwen2.5:7b"),
        "format": "json",
        "stream": False,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You parse restaurant deal posts for a human-reviewed admin queue. "
                    "Return one JSON object only. Never include SQL, markdown, prose, or code. "
                    "Use null for unknown values and warnings for uncertainty. You cannot mutate a database."
                ),
            },
            {"role": "user", "content": f"Return JSON matching this schema:\n{json.dumps(schema_hint)}\n\nSource post:\n{raw_text}"},
        ],
    }
    request = UrlRequest(
        f"{base_url}/api/chat",
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=25) as response:
        response_payload = json.loads(response.read().decode("utf-8"))
    parsed = json.loads(response_payload["message"]["content"])
    if not isinstance(parsed, dict):
        raise ValueError("Ollama response was not a JSON object")
    return sanitize_intake_payload(parsed, raw_text)


def parse_deal_post_with_provider(raw_text: str) -> dict:
    provider = configured_llm_provider()
    if provider == "mock":
        return mock_deal_parse(raw_text)
    if provider == "rules":
        return rules_deal_parse(raw_text)
    if provider == "ollama":
        return ollama_deal_parse(raw_text)
    raise RuntimeError(f"Unsupported LLM_PROVIDER: {provider}")


def configured_llm_provider() -> str:
    provider = os.getenv("LLM_PROVIDER", "rules").strip().lower() or "rules"
    if os.getenv("VERCEL") == "1" and os.getenv("VERCEL_ENV", "preview") == "preview":
        return "rules"
    return provider


def parsed_submission_json(submission: models.DealIntakeSubmission) -> dict:
    if not submission.parsed_json:
        return {}
    try:
        payload = json.loads(submission.parsed_json)
    except (TypeError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def venue_match_score(parsed: dict, venue: models.Venue) -> int:
    venue_name = normalize_match_text(parsed.get("venue_name"))
    venue_address = normalize_match_text(parsed.get("venue_address"))
    source_text = normalize_match_text(parsed.get("source_text"))
    name = normalize_match_text(venue.name)
    address = normalize_match_text(venue.address)
    score = 0
    if venue_name and venue_name == name:
        score += 120
    elif venue_name and (venue_name in name or name in venue_name):
        score += 90
    elif venue_name:
        ratio = SequenceMatcher(None, venue_name, name).ratio()
        if ratio >= 0.86:
            score += 75
        score += min(len(match_tokens(venue_name) & match_tokens(name)) * 20, 60)
    if source_text and name and re.search(rf"\b{re.escape(name)}\b", source_text):
        score += 95
    if venue_address and address and (venue_address in address or address in venue_address):
        score += 45
    return score


def find_intake_venue_matches(db: Session, parsed: dict) -> list[models.Venue]:
    scored = [(venue_match_score(parsed, venue), venue) for venue in db.query(models.Venue).order_by(models.Venue.name.asc()).all()]
    return [venue for score, venue in sorted(scored, key=lambda item: (-item[0], item[1].name.lower())) if score >= 45][:8]


def find_duplicate_deal_candidates(db: Session, parsed: dict, venues: list[models.Venue]) -> list[models.Deal]:
    if not venues:
        return []
    title_tokens = set(re.findall(r"[a-z0-9]+", compact_text(parsed.get("deal_title")).lower()))
    parsed_days = set(parsed.get("days") or [])
    deals = (
        db.query(models.Deal)
        .options(joinedload(models.Deal.venue))
        .filter(
            models.Deal.venue_id.in_([venue.id for venue in venues]),
            models.Deal.status.in_([models.Status.draft, models.Status.queued, models.Status.live]),
        )
        .all()
    )
    duplicates = []
    for deal in deals:
        deal_tokens = set(re.findall(r"[a-z0-9]+", f"{deal.title} {deal.short_description}".lower()))
        day_overlap = not parsed_days or bool(parsed_days & set(weekday_pattern_parts(deal.weekday_pattern)))
        title_overlap = not title_tokens or len(title_tokens & deal_tokens) >= min(2, len(title_tokens))
        time_overlap = not parsed.get("start_time") or parsed.get("start_time") == deal.start_time
        if day_overlap and title_overlap and time_overlap:
            duplicates.append(deal)
    return duplicates[:8]


def intake_warnings(parsed: dict, venue_matches: list[models.Venue], duplicate_deals: list[models.Deal]) -> list[str]:
    warnings = list(parsed.get("warnings") or [])
    action = parsed.get("action")
    if action in {"add_deal", "update_deal", "archive_deal", "venue_closed"}:
        if not parsed.get("venue_name") and not venue_matches:
            warnings.append("Missing venue.")
        elif not venue_matches:
            warnings.append("Venue was not found. This remains a draft proposal.")
        elif len(venue_matches) > 1:
            warnings.append("Multiple possible venue matches. Pick one before approving.")
    if action in {"add_deal", "update_deal"}:
        if not parsed.get("days") and parsed.get("deal_type") == "weekly":
            warnings.append("Missing day.")
        source_lower = compact_text(parsed.get("source_text")).lower()
        if any(phrase in source_lower for phrase in ("today", "tomorrow", "tonight", "this week", "this weekend")) and not parsed.get("start_at"):
            warnings.append("Vague date.")
        if not parsed.get("start_time") and not parsed.get("start_at"):
            warnings.append("Vague time.")
    if duplicate_deals:
        warnings.append("Duplicate possible deal.")
    seen = set()
    return [warning for warning in warnings if not (warning.lower() in seen or seen.add(warning.lower()))]


def analyze_submission(db: Session, submission: models.DealIntakeSubmission) -> None:
    try:
        parsed = parse_deal_post_with_provider(submission.raw_text)
        venue_matches = find_intake_venue_matches(db, parsed)
        duplicates = find_duplicate_deal_candidates(db, parsed, venue_matches)
        parsed["warnings"] = intake_warnings(parsed, venue_matches, duplicates)
        parsed["_system"] = {
            "llm_provider": configured_llm_provider(),
            "llm_can_write_database": False,
            "venue_match_ids": [venue.id for venue in venue_matches],
            "duplicate_deal_ids": [deal.id for deal in duplicates],
        }
        submission.parsed_json = json.dumps(parsed, indent=2, default=json_default)
        submission.confidence = parsed["confidence"]
        submission.status = "needs_human_review" if parsed["confidence"] < LOW_CONFIDENCE_THRESHOLD or parsed["warnings"] else "analyzed"
        submission.error_message = None
    except Exception as exc:
        parsed = empty_manual_proposal(submission.raw_text, str(exc))
        venue_matches = find_intake_venue_matches(db, parsed)
        duplicates = find_duplicate_deal_candidates(db, parsed, venue_matches)
        parsed["warnings"] = intake_warnings(parsed, venue_matches, duplicates)
        parsed["_system"] = {
            "llm_provider": configured_llm_provider(),
            "llm_can_write_database": False,
            "manual_fallback": True,
        }
        submission.parsed_json = json.dumps(parsed, indent=2, default=json_default)
        submission.confidence = parsed["confidence"]
        submission.status = "needs_human_review"
        submission.error_message = str(exc)


def deal_snapshot(deal: models.Deal) -> dict:
    return {
        "id": deal.id,
        "venue_id": deal.venue_id,
        "title": deal.title,
        "short_description": deal.short_description,
        "type": deal.type.value,
        "weekday_pattern": deal.weekday_pattern,
        "start_time": deal.start_time,
        "end_time": deal.end_time,
        "start_at": deal.start_at.isoformat() if deal.start_at else None,
        "end_at": deal.end_at.isoformat() if deal.end_at else None,
        "status": deal.status.value,
        "source_type": deal.source_type,
        "source_url": deal.source_url,
        "source_text": deal.source_text,
        "notes_private": deal.notes_private,
        "freeze_minutes": deal.freeze_minutes,
    }


def log_deal_change(db: Session, action: str, deal: Optional[models.Deal], venue_id: Optional[int], before: Optional[dict], source_text: Optional[str]) -> None:
    db.add(
        models.DealChangeLog(
            action=action,
            deal_id=deal.id if deal else None,
            venue_id=venue_id,
            before_json=json.dumps(before, indent=2, default=json_default) if before is not None else None,
            after_json=json.dumps(deal_snapshot(deal), indent=2, default=json_default) if deal else None,
            source_text=source_text,
        )
    )


def notes_for_intake(submission: models.DealIntakeSubmission) -> str:
    return json.dumps({"intake_submission_id": submission.id, "source_platform": submission.source_platform, "admin_notes": submission.notes})


def selected_venue_for_submission(db: Session, parsed: dict, explicit_venue_id: Optional[int]) -> models.Venue:
    if explicit_venue_id:
        return get_venue_or_404(db, explicit_venue_id)
    system_meta = parsed.get("_system") if isinstance(parsed.get("_system"), dict) else {}
    selected_venue_id = system_meta.get("selected_venue_id")
    if selected_venue_id:
        return get_venue_or_404(db, int(selected_venue_id))
    matches = find_intake_venue_matches(db, parsed)
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise HTTPException(status_code=400, detail="Venue not found. Pick or create a venue before approving.")
    raise HTTPException(status_code=400, detail="Multiple venue matches. Pick one before approving.")


def status_from_intake(value: object, default: models.Status = models.Status.live) -> models.Status:
    try:
        return models.Status(compact_text(value) or default.value)
    except ValueError:
        return default


def apply_intake_submission(db: Session, submission: models.DealIntakeSubmission, explicit_venue_id: Optional[int]) -> Optional[models.Deal]:
    parsed = parsed_submission_json(submission)
    if not parsed:
        raise HTTPException(status_code=400, detail="Submission has no parsed proposal")
    action = parsed.get("action")
    now = datetime.utcnow()
    if action in {"ignore", "needs_human_review"}:
        submission.status = "reviewed"
        submission.reviewed_at = now
        db.commit()
        return None
    venue = selected_venue_for_submission(db, parsed, explicit_venue_id)
    source_type = "scrape" if submission.source_platform else "admin"
    desired_status = status_from_intake(parsed.get("status"), models.Status.live)
    if action == "add_deal":
        if not parsed.get("deal_title") or not parsed.get("description"):
            raise HTTPException(status_code=400, detail="Deal title and description are required")
        if parsed.get("deal_type") == "last_minute":
            start_at = datetime.fromisoformat(parsed["start_at"]) if parsed.get("start_at") else None
            end_at = datetime.fromisoformat(parsed["end_at"]) if parsed.get("end_at") else None
            validate_last_minute_range(start_at, end_at)
            deal = models.Deal(venue_id=venue.id, title=parsed["deal_title"], short_description=parsed["description"], type=models.DealType.last_minute, start_at=start_at, end_at=end_at, source_type=source_type, source_url=submission.source_url, source_text=submission.raw_text, notes_private=notes_for_intake(submission), status=desired_status)
        else:
            days = parsed.get("days") or []
            if not days or not parsed.get("start_time") or not parsed.get("end_time"):
                raise HTTPException(status_code=400, detail="Weekly deals require day, start time, and end time")
            validate_weekly_range(parsed["start_time"], parsed["end_time"])
            enforce_weekly_cap(db, venue.id)
            weekday_pattern = "All" if set(days) == set(DAY_ORDER) else ",".join(days)
            deal = models.Deal(venue_id=venue.id, title=parsed["deal_title"], short_description=parsed["description"], type=models.DealType.weekly, weekday_pattern=weekday_pattern, start_time=parsed["start_time"], end_time=parsed["end_time"], source_type=source_type, source_url=submission.source_url, source_text=submission.raw_text, notes_private=notes_for_intake(submission), status=desired_status)
        db.add(deal)
        db.flush()
        log_deal_change(db, "intake_add_deal", deal, venue.id, None, submission.raw_text)
        applied_deal = deal
    elif action in {"update_deal", "archive_deal"}:
        duplicates = find_duplicate_deal_candidates(db, parsed, [venue])
        if len(duplicates) != 1:
            raise HTTPException(status_code=400, detail="Pick a single existing deal before updating or archiving.")
        deal = duplicates[0]
        before = deal_snapshot(deal)
        if action == "archive_deal":
            deal.status = models.Status.archived
        else:
            if parsed.get("deal_title"):
                deal.title = parsed["deal_title"]
            if parsed.get("description"):
                deal.short_description = parsed["description"]
            if parsed.get("days"):
                days = parsed["days"]
                deal.weekday_pattern = "All" if set(days) == set(DAY_ORDER) else ",".join(days)
            if parsed.get("start_time"):
                deal.start_time = parsed["start_time"]
            if parsed.get("end_time"):
                deal.end_time = parsed["end_time"]
            if parsed.get("status"):
                deal.status = desired_status
            deal.source_url = submission.source_url or deal.source_url
            deal.source_text = submission.raw_text
            validate_deal_shape(deal.type, {"weekday_pattern": deal.weekday_pattern, "start_time": deal.start_time, "end_time": deal.end_time, "start_at": deal.start_at, "end_at": deal.end_at})
        deal.updated_at = now
        log_deal_change(db, f"intake_{action}", deal, venue.id, before, submission.raw_text)
        applied_deal = deal
    elif action == "venue_closed":
        deals = db.query(models.Deal).filter(models.Deal.venue_id == venue.id, models.Deal.status.in_([models.Status.draft, models.Status.queued, models.Status.live])).all()
        for deal in deals:
            before = deal_snapshot(deal)
            deal.status = models.Status.archived
            deal.updated_at = now
            log_deal_change(db, "intake_venue_closed_archive", deal, venue.id, before, submission.raw_text)
        applied_deal = deals[0] if deals else None
    else:
        raise HTTPException(status_code=400, detail="Unsupported intake action")
    submission.status = "applied"
    submission.reviewed_at = now
    submission.applied_at = now
    db.commit()
    return applied_deal


def admin_badge(label: str, tone: str = "neutral") -> str:
    return f'<span class="admin-badge admin-badge-{escape(tone)}">{escape(label)}</span>'


def admin_shell(title: str, main_content: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>{escape(title)} | DSM Deals Admin</title>
    <link rel="stylesheet" href="{site_href("/static/styles.css")}" />
  </head>
  <body class="admin-body">
    <main class="admin-console">
      <header class="admin-header">
        <div><p class="admin-kicker">DSM Deals Hub</p><h1>{escape(title)}</h1></div>
        <nav class="admin-nav" aria-label="Admin navigation">
          <a href="/admin/intake">Intake</a><a href="/admin/review">Review</a><a href="/admin/deals">Deals</a><a href="/admin/logout">Log out</a><a href="/">Public site</a>
        </nav>
      </header>
      {main_content}
    </main>
  </body>
</html>"""


def render_admin_login_html(error: Optional[str] = None, next_path: str = "/admin/intake") -> str:
    error_html = f'<div class="admin-alert admin-alert-danger">{escape(error)}</div>' if error else ""
    safe_next = next_path if next_path.startswith("/admin/") else "/admin/intake"
    return admin_shell(
        "Admin Login",
        f"""
        {error_html}
        <section class="admin-panel admin-login-panel">
          <form class="admin-form" method="post" action="/admin/login">
            <input type="hidden" name="next_path" value="{escape(safe_next)}" />
            <label>
              <span>Password</span>
              <input name="password" type="password" autocomplete="current-password" required autofocus />
            </label>
            <button class="admin-primary-button" type="submit">Log in</button>
          </form>
        </section>
        """,
    )


def render_admin_intake_html(error: Optional[str] = None) -> str:
    error_html = f'<div class="admin-alert admin-alert-danger">{escape(error)}</div>' if error else ""
    return admin_shell(
        "Deal Intake",
        f"""
        {error_html}
        <section class="admin-panel">
          <form class="admin-form" method="post" action="/admin/intake">
            <label><span>Paste deal post</span><textarea name="raw_text" rows="14" required placeholder="Paste the original restaurant post here"></textarea></label>
            <div class="admin-form-grid">
              <label><span>Source URL</span><input name="source_url" type="url" placeholder="https://..." /></label>
              <label><span>Source platform</span><input name="source_platform" placeholder="Facebook, Instagram, website..." /></label>
            </div>
            <label><span>Notes</span><textarea name="notes" rows="4" placeholder="Private admin context"></textarea></label>
            <button class="admin-primary-button" type="submit">Analyze deal</button>
          </form>
        </section>
        """,
    )


def render_day_checkbox(day_code: str, selected_days: list[str]) -> str:
    checked = "checked" if day_code in selected_days else ""
    return f'<label class="admin-check-row"><input type="checkbox" name="days" value="{day_code}" {checked} /><span>{escape(WEEKDAY_LONG[day_code])}</span></label>'


def venue_select_options(venues: list[models.Venue], selected_venue_id: Optional[int]) -> str:
    options = ['<option value="">Select venue</option>']
    for venue in venues:
        selected = "selected" if selected_venue_id == venue.id else ""
        options.append(f'<option value="{venue.id}" {selected}>{escape(venue.name)} - {escape(venue.address)}</option>')
    return "".join(options)


def venue_to_admin_lookup_json(venue: models.Venue, counts: Optional[dict] = None, owner_name: Optional[str] = None) -> dict:
    venue_counts = counts or {"deal_count": 0, "live_deal_count": 0}
    return {
        "id": venue.id,
        "owner_id": venue.owner_id,
        "owner_name": owner_name,
        "name": venue.name,
        "slug": venue.slug,
        "address": venue.address,
        "neighborhood": venue.neighborhood,
        "lat": venue.lat,
        "lng": venue.lng,
        "phone": venue.phone,
        "website": venue.website,
        "hours_json": venue.hours_json,
        "description": venue.description,
        "created_at": venue.created_at.isoformat() if venue.created_at else None,
        "updated_at": venue.updated_at.isoformat() if venue.updated_at else None,
        "deal_count": venue_counts.get("deal_count", 0),
        "live_deal_count": venue_counts.get("live_deal_count", 0),
    }


def create_intake_venue(
    db: Session,
    *,
    name: object,
    address: object,
    neighborhood: object,
    source_text: Optional[str],
    phone: object = None,
    website: object = None,
    description: object = None,
) -> Optional[models.Venue]:
    venue_name = clean_optional(name)
    venue_address = clean_optional(address)
    if not venue_name or not venue_address:
        return None
    existing = find_exact_venue_by_name(db, venue_name)
    if existing:
        changed = False
        for field, value in {
            "address": venue_address,
            "neighborhood": clean_optional(neighborhood),
            "phone": clean_optional(phone),
            "website": clean_optional(website),
            "description": clean_optional(description),
        }.items():
            if value and not getattr(existing, field):
                setattr(existing, field, value)
                changed = True
        if changed:
            existing.updated_at = datetime.utcnow()
            db.flush()
            log_deal_change(db, "admin_update_existing_venue_from_intake", None, existing.id, None, source_text)
        return existing
    slug = ensure_unique_venue_slug(db, venue_name)
    venue = models.Venue(
        name=venue_name,
        slug=slug,
        address=venue_address,
        neighborhood=clean_optional(neighborhood),
        phone=clean_optional(phone),
        website=clean_optional(website),
        description=clean_optional(description),
    )
    db.add(venue)
    db.flush()
    log_deal_change(db, "admin_create_venue_from_intake", None, venue.id, None, source_text)
    return venue


def render_manual_proposal_form(
    submission: models.DealIntakeSubmission,
    parsed: dict,
    all_venues: list[models.Venue],
    venue_matches: Optional[list[models.Venue]] = None,
) -> str:
    system_meta = parsed.get("_system") if isinstance(parsed.get("_system"), dict) else {}
    selected_venue_id = system_meta.get("selected_venue_id")
    try:
        selected_venue_id = int(selected_venue_id) if selected_venue_id else None
    except (TypeError, ValueError):
        selected_venue_id = None
    if selected_venue_id is None and venue_matches and len(venue_matches) == 1:
        selected_venue_id = venue_matches[0].id
    has_venue_match = bool(venue_matches)
    action_options = "".join(
        f'<option value="{action}" {"selected" if parsed.get("action") == action else ""}>{action}</option>'
        for action in sorted(INTAKE_ACTIONS)
    )
    status_options = "".join(
        f'<option value="{status.value}" {"selected" if parsed.get("status") == status.value else ""}>{status.value}</option>'
        for status in [models.Status.draft, models.Status.queued, models.Status.live, models.Status.archived]
    )
    deal_type_options = "".join(
        f'<option value="{deal_type}" {"selected" if parsed.get("deal_type") == deal_type else ""}>{deal_type}</option>'
        for deal_type in ["weekly", "last_minute"]
    )
    day_checks = "".join(render_day_checkbox(day_code, parsed.get("days") or []) for day_code in DAY_ORDER)
    venue_query_value = parsed.get("venue_name") or ""
    venue_datalist = "".join(f'<option value="{escape(venue.name)}">{escape(venue.address)}</option>' for venue in all_venues)
    create_name_value = parsed.get("venue_name") or ""
    create_address_value = parsed.get("venue_address") or ""
    selected_venue = next((venue for venue in all_venues if selected_venue_id == venue.id), None)
    selected_venue_html = (
        f'<p class="admin-selected-note">Attached venue: <strong>{escape(selected_venue.name)}</strong> <span>{escape(selected_venue.address)}</span></p>'
        if selected_venue
        else '<p class="admin-muted">No venue is attached yet. Pick an existing venue or create one below.</p>'
    )
    maps_url = google_maps_search_url(create_name_value or venue_query_value, create_address_value, "Des Moines IA")
    missing_venue_html = (
        '<div class="admin-alert admin-alert-danger"><strong>No matching venue found.</strong> Pick an existing venue or create a venue before approving.</div>'
        if not has_venue_match
        else ""
    )
    return f"""
    <section class="admin-panel">
      <h2>Edit Proposal</h2>
      {missing_venue_html}
      <form class="admin-form" method="post" action="/admin/review/{submission.id}/manual">
        <div class="admin-form-grid">
          <label><span>Action</span><select name="action">{action_options}</select></label>
          <label><span>Venue search</span><input name="venue_query" list="admin-venue-options" value="{escape(str(venue_query_value))}" placeholder="Start typing a venue name" /></label>
          <label><span>Venue dropdown</span><select name="venue_id">{venue_select_options(all_venues, selected_venue_id)}</select></label>
          <label><span>Status</span><select name="status">{status_options}</select></label>
          <label><span>Deal type</span><select name="deal_type">{deal_type_options}</select></label>
        </div>
        {selected_venue_html}
        <datalist id="admin-venue-options">{venue_datalist}</datalist>
        <label><span>Deal title</span><input name="deal_title" value="{escape(str(parsed.get("deal_title") or ""))}" /></label>
        <label><span>Description</span><textarea name="description" rows="4">{escape(str(parsed.get("description") or ""))}</textarea></label>
        <div class="admin-check-grid">{day_checks}</div>
        <div class="admin-form-grid">
          <label><span>Start time</span><input name="start_time" value="{escape(str(parsed.get("start_time") or ""))}" placeholder="HH:MM" /></label>
          <label><span>End time</span><input name="end_time" value="{escape(str(parsed.get("end_time") or ""))}" placeholder="HH:MM" /></label>
          <label><span>Start date/time</span><input name="start_at" value="{escape(str(parsed.get("start_at") or ""))}" placeholder="2026-06-15T17:00:00" /></label>
          <label><span>End date/time</span><input name="end_at" value="{escape(str(parsed.get("end_at") or ""))}" placeholder="2026-06-15T19:00:00" /></label>
        </div>
        <label><span>Source URL</span><input name="source_url" type="url" value="{escape(submission.source_url or "")}" placeholder="https://..." /></label>
        <section class="admin-subpanel admin-create-venue-panel">
          <div class="admin-section-title">
            <div>
              <h3>Create new venue</h3>
              <p class="admin-muted">Use this when the dropdown does not have the right place. Saving here creates the venue and attaches it to this proposal.</p>
            </div>
            <a class="admin-secondary-button" id="admin-google-maps-venue-lookup" href="{escape(maps_url)}" target="_blank" rel="noopener noreferrer">Look up on Google Maps</a>
          </div>
          <div class="admin-form-grid">
            <label><span>Venue name</span><input id="admin-create-venue-name" name="create_venue_name" value="{escape(str(create_name_value))}" placeholder="Lua Brewing" /></label>
            <label><span>Venue address</span><input id="admin-create-venue-address" name="create_venue_address" value="{escape(str(create_address_value))}" placeholder="Street address" /></label>
            <label><span>Neighborhood</span><input name="create_venue_neighborhood" placeholder="Downtown" /></label>
            <label><span>Phone</span><input name="create_venue_phone" placeholder="(515) 555-1234" /></label>
            <label><span>Website</span><input name="create_venue_website" type="url" placeholder="https://venue.com" /></label>
          </div>
          <label><span>Venue notes</span><textarea name="create_venue_description" rows="3" placeholder="Optional details copied from Google Maps or the venue website"></textarea></label>
        </section>
        <div class="admin-action-row">
          <button class="admin-primary-button" type="submit" name="manual_action" value="save">Save manual proposal</button>
          <button class="admin-secondary-button" type="submit" name="manual_action" value="create_venue">Create venue and attach</button>
        </div>
      </form>
      <script>
        (() => {{
          const link = document.getElementById("admin-google-maps-venue-lookup");
          const nameInput = document.getElementById("admin-create-venue-name");
          const addressInput = document.getElementById("admin-create-venue-address");
          if (!link || !nameInput || !addressInput) return;
          const updateMapsLink = () => {{
            const query = [nameInput.value, addressInput.value, "Des Moines IA"].map((part) => part.trim()).filter(Boolean).join(" ") || "Des Moines restaurant";
            link.href = "https://www.google.com/maps/search/?api=1&query=" + encodeURIComponent(query);
          }};
          nameInput.addEventListener("input", updateMapsLink);
          addressInput.addEventListener("input", updateMapsLink);
          updateMapsLink();
        }})();
      </script>
    </section>
    """


def render_admin_review_html(submission: Optional[models.DealIntakeSubmission], venue_matches: list[models.Venue], duplicate_deals: list[models.Deal], all_venues: list[models.Venue], edit_mode: bool = False) -> str:
    if submission is None:
        return admin_shell("Review Deal", '<section class="admin-panel"><p class="admin-empty">No intake submissions are waiting for review.</p></section>')
    parsed = parsed_submission_json(submission)
    warning_html = "".join(f"<li>{escape(warning)}</li>" for warning in (parsed.get("warnings") or [])) or "<li>No warnings.</li>"
    blockers = approval_blockers(parsed, venue_matches)
    blocker_html = "".join(f"<li>{escape(blocker)}</li>" for blocker in blockers)
    approval_blocker_html = f'<div class="admin-alert admin-alert-danger"><strong>Approval blocked</strong><ul>{blocker_html}</ul></div>' if blockers else ""
    approve_disabled = "disabled" if blockers else ""
    venue_required = "required" if len(venue_matches) > 1 else ""
    venue_options = "".join(f'<label class="admin-radio-row"><input type="radio" name="venue_id" value="{venue.id}" {"checked" if len(venue_matches) == 1 else ""} {venue_required} /><span>{escape(venue.name)} <small>{escape(venue.address)}</small></span></label>' for venue in venue_matches) or '<p class="admin-muted">No matching venue found. Create or edit the venue before approving.</p>'
    duplicates_html = "".join(f"<li><strong>#{deal.id} {escape(deal.title)}</strong><span>{escape(deal.venue.name if deal.venue else 'Venue')} · {escape(deal.status.value)}</span></li>" for deal in duplicate_deals) or "<li>No close duplicates found.</li>"
    parsed_pretty = json.dumps(parsed, indent=2, default=json_default)
    error_html = f'<div class="admin-alert admin-alert-danger">{escape(submission.error_message)}</div>' if submission.error_message else ""
    edit_html = ""
    if edit_mode:
        edit_html = f"""
        {render_manual_proposal_form(submission, parsed, all_venues, venue_matches)}
        <section class="admin-panel">
          <h2>Edit Parsed JSON</h2>
          <form class="admin-form" method="post" action="/admin/review/{submission.id}/edit">
            <textarea name="parsed_json" rows="20" required>{escape(parsed_pretty)}</textarea>
            <button class="admin-primary-button" type="submit">Save parsed proposal</button>
          </form>
        </section>
        """
    source_url = f'<a href="{escape(submission.source_url)}" target="_blank" rel="noopener noreferrer">Open source</a>' if submission.source_url else "No source URL"
    return admin_shell(
        "Review Deal",
        f"""
        {error_html}
        <section class="admin-panel admin-review-grid">
          <div>
            <div class="admin-section-title"><h2>Proposed Change</h2>{admin_badge(submission.status, "warning" if submission.status == "needs_human_review" else "success")}</div>
            <dl class="admin-proposal-list">
              <div><dt>Action</dt><dd>{escape(str(parsed.get("action") or ""))}</dd></div>
              <div><dt>Venue</dt><dd>{escape(str(parsed.get("venue_name") or "Missing"))}</dd></div>
              <div><dt>Title</dt><dd>{escape(str(parsed.get("deal_title") or "Missing"))}</dd></div>
              <div><dt>Description</dt><dd>{escape(str(parsed.get("description") or "Missing"))}</dd></div>
              <div><dt>Days</dt><dd>{escape(", ".join(parsed.get("days") or []) or "Missing")}</dd></div>
              <div><dt>Time</dt><dd>{escape(f"{parsed.get('start_time') or 'Missing'} to {parsed.get('end_time') or 'Missing'}")}</dd></div>
              <div><dt>Confidence</dt><dd>{escape(str(parsed.get("confidence", 0)))}</dd></div>
              <div><dt>Source</dt><dd>{source_url}</dd></div>
            </dl>
          </div>
          <aside class="admin-side-panel">
            <h2>Warnings</h2><ul class="admin-warning-list">{warning_html}</ul>
            <h2>Venue Match</h2>
            <form method="post" action="/admin/review/{submission.id}/approve" class="admin-form">
              {approval_blocker_html}
              <div class="admin-radio-stack">{venue_options}</div>
              <div class="admin-action-row"><button class="admin-primary-button" type="submit" {approve_disabled}>Approve</button><a class="admin-secondary-button" href="/admin/review?submission_id={submission.id}&edit=1">Edit</a><button class="admin-danger-button" type="submit" formaction="/admin/review/{submission.id}/reject">Reject</button></div>
            </form>
          </aside>
        </section>
        <section class="admin-panel"><h2>Possible Duplicates</h2><ul class="admin-compact-list">{duplicates_html}</ul></section>
        <section class="admin-panel"><h2>Raw Source Text</h2><pre class="admin-source-box">{escape(submission.raw_text)}</pre></section>
        {edit_html}
        """,
    )


def admin_deal_time_label(deal: models.Deal) -> str:
    if deal.type == models.DealType.weekly:
        return f"{deal.weekday_pattern or 'No days'} · {deal.start_time or '?'}-{deal.end_time or '?'}"
    return f"{deal.start_at.isoformat() if deal.start_at else '?'} to {deal.end_at.isoformat() if deal.end_at else '?'}"


def normalize_admin_deal_status_filter(status: Optional[str]) -> str:
    status_filter = compact_text(status or "active").lower()
    return status_filter if status_filter in ADMIN_DEAL_STATUS_FILTERS else "active"


def admin_deals_return_path(q: Optional[str], status_filter: str) -> str:
    params = {}
    if clean_optional(q):
        params["q"] = compact_text(q)
    if status_filter != "active":
        params["status"] = status_filter
    query = urlencode(params)
    return f"/admin/deals?{query}" if query else "/admin/deals"


def render_admin_deals_html(deals: list[models.Deal], q: Optional[str], status: Optional[str]) -> str:
    status_filter = normalize_admin_deal_status_filter(status)
    return_to = admin_deals_return_path(q, status_filter)
    rows = []
    for deal in deals:
        source = f'<a href="{escape(deal.source_url)}" target="_blank" rel="noopener noreferrer">View source</a>' if deal.source_url else ('<span class="admin-muted">Source text saved</span>' if deal.source_text else '<span class="admin-muted">No source</span>')
        frozen = bool(deal.freeze_minutes and deal.freeze_minutes >= FREEZE_SENTINEL_MINUTES)
        remove_action = (
            f'<form method="post" action="/admin/deals/{deal.id}/archive"><input type="hidden" name="return_to" value="{escape(return_to)}" /><button class="admin-danger-button" type="submit">Remove from live</button></form>'
            if deal.status != models.Status.archived
            else '<span class="admin-muted">Removed</span>'
        )
        freeze_action = (
            f'<form method="post" action="/admin/deals/{deal.id}/freeze"><input type="hidden" name="return_to" value="{escape(return_to)}" /><button class="admin-secondary-button" type="submit">Freeze</button></form>'
            if deal.status not in {models.Status.archived, models.Status.rejected}
            else ""
        )
        rows.append(f'<tr><td><strong>{escape(deal.title)}</strong><small>{escape(deal.short_description)}</small></td><td>{escape(deal.venue.name if deal.venue else "Venue")}</td><td>{escape(admin_deal_time_label(deal))}</td><td>{admin_badge(deal.status.value, "neutral")}{admin_badge("frozen", "warning") if frozen else ""}</td><td>{source}</td><td class="admin-table-actions"><a class="admin-secondary-button" href="/admin/deals/{deal.id}/edit">Edit</a>{freeze_action}{remove_action}</td></tr>')
    empty_label = "No active deals matched." if status_filter == "active" else "No deals matched."
    rows_html = "".join(rows) or f'<tr><td colspan="6" class="admin-empty">{empty_label}</td></tr>'
    status_options = "".join(
        f'<option value="{value}" {"selected" if status_filter == value else ""}>{label}</option>'
        for value, label in [
            ("active", "Active only"),
            ("live", "Live"),
            ("draft", "Draft"),
            ("queued", "Queued"),
            ("archived", "Removed / archived"),
            ("expired", "Expired"),
            ("rejected", "Rejected"),
            ("all", "All statuses"),
        ]
    )
    return admin_shell("Admin Deals", f'<section class="admin-panel"><form class="admin-toolbar admin-search-form" role="search" method="get" action="/admin/deals"><input name="q" type="search" value="{escape(q or "")}" placeholder="Search deals or venues" /><select name="status">{status_options}</select><button class="admin-primary-button" type="submit">Search</button><a class="admin-secondary-button" href="/admin/deals?status=archived">View removed</a></form><div class="admin-table-wrap"><table class="admin-table"><thead><tr><th>Deal</th><th>Venue</th><th>Schedule</th><th>Status</th><th>Source</th><th>Actions</th></tr></thead><tbody>{rows_html}</tbody></table></div></section>')


def render_admin_deal_edit_html(deal: models.Deal, error: Optional[str] = None) -> str:
    error_html = f'<div class="admin-alert admin-alert-danger">{escape(error)}</div>' if error else ""
    status_options = "".join(f'<option value="{status.value}" {"selected" if deal.status == status else ""}>{status.value}</option>' for status in models.Status)
    return admin_shell(f"Edit Deal #{deal.id}", f'{error_html}<section class="admin-panel"><form class="admin-form" method="post" action="/admin/deals/{deal.id}/edit"><div class="admin-form-grid"><label><span>Title</span><input name="title" value="{escape(deal.title)}" required /></label><label><span>Status</span><select name="status">{status_options}</select></label></div><label><span>Description</span><textarea name="short_description" rows="4" required>{escape(deal.short_description)}</textarea></label><div class="admin-form-grid"><label><span>Weekday pattern</span><input name="weekday_pattern" value="{escape(deal.weekday_pattern or "")}" /></label><label><span>Start time</span><input name="start_time" value="{escape(deal.start_time or "")}" placeholder="HH:MM" /></label><label><span>End time</span><input name="end_time" value="{escape(deal.end_time or "")}" placeholder="HH:MM" /></label></div><label><span>Source URL</span><input name="source_url" value="{escape(deal.source_url or "")}" /></label><label><span>Source text</span><textarea name="source_text" rows="6">{escape(deal.source_text or "")}</textarea></label><div class="admin-action-row"><button class="admin-primary-button" type="submit">Save deal</button><a class="admin-secondary-button" href="/admin/deals">Cancel</a></div></form></section>')


def admin_deals_query(
    db: Session,
    status: Optional[str] = None,
    deal_type: Optional[models.DealType] = None,
    venue_id: Optional[int] = None,
    neighborhood: Optional[str] = None,
    q: Optional[str] = None,
) -> list[models.Deal]:
    expire_stale_last_minute_deals(db)
    status_filter = normalize_admin_deal_status_filter(status)
    query = db.query(models.Deal).options(joinedload(models.Deal.venue)).join(models.Venue)
    if status_filter == "active":
        query = query.filter(models.Deal.status.in_(ACTIVE_ADMIN_DEAL_STATUSES))
    elif status_filter != "all":
        query = query.filter(models.Deal.status == models.Status(status_filter))
    if deal_type is not None:
        query = query.filter(models.Deal.type == deal_type)
    if venue_id is not None:
        query = query.filter(models.Deal.venue_id == venue_id)
    if neighborhood:
        query = query.filter(func.lower(models.Venue.neighborhood) == neighborhood.strip().lower())
    if q:
        term = f"%{q.strip()}%"
        query = query.filter(or_(models.Deal.title.ilike(term), models.Deal.short_description.ilike(term), models.Deal.source_text.ilike(term), models.Venue.name.ilike(term), models.Venue.address.ilike(term)))
    return query.order_by(models.Deal.updated_at.desc(), models.Deal.created_at.desc()).all()


def build_manual_intake_payload(
    db: Session,
    submission: models.DealIntakeSubmission,
    *,
    action: str,
    venue_id: Optional[int],
    venue_query: Optional[str],
    status: str,
    deal_type: str,
    deal_title: str,
    description: str,
    days: list[str],
    start_time: str,
    end_time: str,
    start_at: str,
    end_at: str,
) -> dict:
    parsed = sanitize_intake_payload(
        {
            "action": action,
            "deal_title": deal_title,
            "description": description,
            "days": days,
            "start_time": start_time,
            "end_time": end_time,
            "start_at": start_at,
            "end_at": end_at,
            "deal_type": deal_type,
            "status": status,
            "confidence": 1.0,
            "warnings": [],
        },
        submission.raw_text,
    )
    if venue_id:
        venue = get_venue_or_404(db, venue_id)
        parsed["venue_name"] = venue.name
        parsed["venue_address"] = venue.address
        parsed["_system"] = {"selected_venue_id": venue.id, "manual_edit": True, "llm_can_write_database": False}
    else:
        parsed["venue_name"] = clean_optional(venue_query)
        parsed["_system"] = {"manual_edit": True, "llm_can_write_database": False}
    return parsed


def approval_blockers(parsed: dict, venue_matches: list[models.Venue]) -> list[str]:
    action = parsed.get("action")
    blockers: list[str] = []
    if action in {"add_deal", "update_deal", "archive_deal", "venue_closed"} and not venue_matches:
        blockers.append("Pick a valid venue before approving.")
    if action in {"add_deal", "update_deal"}:
        if not clean_optional(parsed.get("deal_title")):
            blockers.append("Add a deal title before approving.")
        if not clean_optional(parsed.get("description")):
            blockers.append("Add a description before approving.")
        if parsed.get("deal_type") == "weekly" and not parsed.get("days"):
            blockers.append("Pick at least one day before approving.")
    return blockers


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


@app.get("/admin/login", response_class=HTMLResponse)
def admin_login_page(next_path: str = "/admin/intake"):
    return HTMLResponse(render_admin_login_html(next_path=next_path))


@app.post("/admin/login", response_class=HTMLResponse)
def admin_login_submit(
    password: str = Form(...),
    next_path: str = Form("/admin/intake"),
):
    safe_next = next_path if next_path.startswith("/admin/") and not next_path.startswith("/admin/login") else "/admin/intake"
    posted_password = password.strip()
    if not admin_key_is_valid(posted_password):
        return HTMLResponse(render_admin_login_html("Incorrect password.", safe_next), status_code=401)

    response = RedirectResponse(safe_next, status_code=303)
    response.set_cookie(
        "admin_session",
        admin_session_cookie_value(),
        httponly=True,
        secure=os.getenv("VERCEL") == "1",
        samesite="lax",
        max_age=60 * 60 * 12,
        path="/admin",
    )
    return response


@app.get("/admin/logout")
def admin_logout():
    response = RedirectResponse("/admin/login", status_code=303)
    response.delete_cookie("admin_session", path="/admin")
    return response


@app.get("/admin/auth-debug")
def admin_auth_debug(request: Request):
    admin_key = current_admin_key()
    header_key = request.headers.get("x-admin-key", "").strip()
    cookie_value = request.cookies.get("admin_session", "").strip()
    return {
        "admin_key_configured": bool(admin_key),
        "llm_provider": configured_llm_provider(),
        "header_seen": bool(header_key),
        "header_matches": bool(admin_key and header_key and hmac.compare_digest(header_key, admin_key)),
        "cookie_seen": bool(cookie_value),
        "cookie_valid": admin_session_is_valid(cookie_value),
    }


@app.get("/admin/schema-debug", dependencies=[Depends(require_admin)])
def admin_schema_debug():
    return schema_report(engine)


@app.get("/admin/intake", response_class=HTMLResponse, dependencies=[Depends(require_admin_password)])
def admin_intake_page():
    return HTMLResponse(render_admin_intake_html())


@app.post("/admin/intake", dependencies=[Depends(require_admin_password)])
def admin_intake_submit(
    raw_text: str = Form(...),
    source_url: Optional[str] = Form(None),
    source_platform: Optional[str] = Form(None),
    notes: Optional[str] = Form(None),
    db: Session = Depends(get_db),
):
    cleaned_text = raw_text.strip()
    if not cleaned_text:
        return HTMLResponse(render_admin_intake_html("Paste source text before analyzing."), status_code=400)
    submission = models.DealIntakeSubmission(
        raw_text=cleaned_text,
        source_url=clean_optional(source_url),
        source_platform=clean_optional(source_platform),
        notes=clean_optional(notes),
        status="submitted",
    )
    db.add(submission)
    db.flush()
    analyze_submission(db, submission)
    db.commit()
    edit_flag = "&edit=1" if submission.error_message else ""
    return RedirectResponse(f"/admin/review?submission_id={submission.id}{edit_flag}", status_code=303)


@app.get("/admin/review", response_class=HTMLResponse, dependencies=[Depends(require_admin_password)])
def admin_review_page(
    submission_id: Optional[int] = None,
    edit: Optional[int] = None,
    db: Session = Depends(get_db),
):
    query = db.query(models.DealIntakeSubmission)
    if submission_id is not None:
        submission = query.filter(models.DealIntakeSubmission.id == submission_id).first()
    else:
        submission = (
            query.filter(models.DealIntakeSubmission.status.in_(["analyzed", "needs_human_review", "submitted"]))
            .order_by(models.DealIntakeSubmission.created_at.desc())
            .first()
        )
    if submission is None:
        return HTMLResponse(render_admin_review_html(None, [], [], [], False))
    parsed = parsed_submission_json(submission)
    venue_matches = find_intake_venue_matches(db, parsed)
    duplicate_deals = find_duplicate_deal_candidates(db, parsed, venue_matches)
    all_venues = db.query(models.Venue).order_by(models.Venue.name.asc()).all()
    return HTMLResponse(render_admin_review_html(submission, venue_matches, duplicate_deals, all_venues, bool(edit or submission.error_message)))


@app.post("/admin/review/{submission_id}/edit", dependencies=[Depends(require_admin_password)])
def admin_review_save_json(
    submission_id: int,
    parsed_json: str = Form(...),
    db: Session = Depends(get_db),
):
    submission = db.get(models.DealIntakeSubmission, submission_id)
    if submission is None:
        raise HTTPException(status_code=404, detail="Submission not found")
    try:
        payload = json.loads(parsed_json)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {exc}") from exc
    parsed = sanitize_intake_payload(payload, submission.raw_text)
    if isinstance(payload.get("_system"), dict):
        parsed["_system"] = {**payload["_system"], "llm_can_write_database": False}
    venue_matches = find_intake_venue_matches(db, parsed)
    duplicates = find_duplicate_deal_candidates(db, parsed, venue_matches)
    parsed["warnings"] = intake_warnings(parsed, venue_matches, duplicates)
    submission.parsed_json = json.dumps(parsed, indent=2, default=json_default)
    submission.confidence = parsed["confidence"]
    submission.status = "needs_human_review" if parsed["confidence"] < LOW_CONFIDENCE_THRESHOLD or parsed["warnings"] else "analyzed"
    submission.error_message = None
    db.commit()
    return RedirectResponse(f"/admin/review?submission_id={submission.id}", status_code=303)


@app.post("/admin/review/{submission_id}/manual", dependencies=[Depends(require_admin_password)])
def admin_review_save_manual(
    submission_id: int,
    action: str = Form(...),
    venue_id: str = Form(""),
    venue_query: str = Form(""),
    status: str = Form("draft"),
    deal_type: str = Form("weekly"),
    deal_title: str = Form(""),
    description: str = Form(""),
    days: List[str] = Form(default=[]),
    start_time: str = Form(""),
    end_time: str = Form(""),
    start_at: str = Form(""),
    end_at: str = Form(""),
    source_url: str = Form(""),
    manual_action: str = Form("save"),
    create_venue_name: str = Form(""),
    create_venue_address: str = Form(""),
    create_venue_neighborhood: str = Form(""),
    create_venue_phone: str = Form(""),
    create_venue_website: str = Form(""),
    create_venue_description: str = Form(""),
    db: Session = Depends(get_db),
):
    submission = db.get(models.DealIntakeSubmission, submission_id)
    if submission is None:
        raise HTTPException(status_code=404, detail="Submission not found")
    selected_venue_id = int(venue_id) if venue_id else None
    if selected_venue_id is None and manual_action != "create_venue":
        venue_from_query = find_venue_by_name_input(db, venue_query)
        selected_venue_id = venue_from_query.id if venue_from_query else None
    should_create_venue = selected_venue_id is None and (manual_action == "create_venue" or clean_optional(create_venue_name) or clean_optional(create_venue_address))
    if should_create_venue:
        create_name = clean_optional(create_venue_name) or clean_optional(venue_query)
        if not create_name or not clean_optional(create_venue_address):
            submission.error_message = "Add a venue name and address before creating a new venue."
            db.commit()
            return RedirectResponse(f"/admin/review?submission_id={submission.id}&edit=1", status_code=303)
        created_venue = create_intake_venue(
            db,
            name=create_name,
            address=create_venue_address,
            neighborhood=create_venue_neighborhood,
            source_text=submission.raw_text,
            phone=create_venue_phone,
            website=create_venue_website,
            description=create_venue_description,
        )
        selected_venue_id = created_venue.id if created_venue else None
    submission.source_url = clean_optional(source_url)
    parsed = build_manual_intake_payload(
        db,
        submission,
        action=action,
        venue_id=selected_venue_id,
        venue_query=venue_query,
        status=status,
        deal_type=deal_type,
        deal_title=deal_title,
        description=description,
        days=days,
        start_time=start_time,
        end_time=end_time,
        start_at=start_at,
        end_at=end_at,
    )
    venue_matches = [get_venue_or_404(db, selected_venue_id)] if selected_venue_id else find_intake_venue_matches(db, parsed)
    duplicates = find_duplicate_deal_candidates(db, parsed, venue_matches)
    parsed["warnings"] = intake_warnings(parsed, venue_matches, duplicates)
    submission.parsed_json = json.dumps(parsed, indent=2, default=json_default)
    submission.confidence = parsed["confidence"]
    submission.status = "needs_human_review" if parsed["warnings"] else "analyzed"
    submission.error_message = None
    db.commit()
    return RedirectResponse(f"/admin/review?submission_id={submission.id}", status_code=303)


@app.post("/admin/review/{submission_id}/approve", dependencies=[Depends(require_admin_password)])
def admin_review_approve(
    submission_id: int,
    venue_id: str = Form(""),
    db: Session = Depends(get_db),
):
    submission = db.get(models.DealIntakeSubmission, submission_id)
    if submission is None:
        raise HTTPException(status_code=404, detail="Submission not found")
    try:
        parsed = parsed_submission_json(submission)
        explicit_venue_id = int(venue_id) if venue_id else None
        venue_matches = [get_venue_or_404(db, explicit_venue_id)] if explicit_venue_id else find_intake_venue_matches(db, parsed)
        blockers = approval_blockers(parsed, venue_matches)
        if blockers:
            raise HTTPException(status_code=400, detail=" ".join(blockers))
        apply_intake_submission(db, submission, explicit_venue_id)
    except HTTPException as exc:
        submission.error_message = str(exc.detail)
        db.commit()
        return RedirectResponse(f"/admin/review?submission_id={submission.id}&edit=1", status_code=303)
    return RedirectResponse("/admin/deals", status_code=303)


@app.post("/admin/review/{submission_id}/reject", dependencies=[Depends(require_admin_password)])
def admin_review_reject(submission_id: int, db: Session = Depends(get_db)):
    submission = db.get(models.DealIntakeSubmission, submission_id)
    if submission is None:
        raise HTTPException(status_code=404, detail="Submission not found")
    submission.status = "rejected"
    submission.reviewed_at = datetime.utcnow()
    log_deal_change(db, "intake_reject", None, None, None, submission.raw_text)
    db.commit()
    return RedirectResponse("/admin/review", status_code=303)


@app.get("/admin/deals", response_class=HTMLResponse, dependencies=[Depends(require_admin_password)])
def admin_deals_page(
    status: Optional[str] = None,
    q: Optional[str] = None,
    db: Session = Depends(get_db),
):
    deals = admin_deals_query(db, status=status, q=q)
    return HTMLResponse(render_admin_deals_html(deals, q, status))


@app.get("/admin/deals/{deal_id}/edit", response_class=HTMLResponse, dependencies=[Depends(require_admin_password)])
def admin_deal_edit_page(deal_id: int, db: Session = Depends(get_db)):
    return HTMLResponse(render_admin_deal_edit_html(get_deal_with_venue_or_404(db, deal_id)))


@app.post("/admin/deals/{deal_id}/edit", dependencies=[Depends(require_admin_password)])
def admin_deal_edit_submit(
    deal_id: int,
    title: str = Form(...),
    short_description: str = Form(...),
    status: models.Status = Form(...),
    weekday_pattern: Optional[str] = Form(None),
    start_time: Optional[str] = Form(None),
    end_time: Optional[str] = Form(None),
    source_url: Optional[str] = Form(None),
    source_text: Optional[str] = Form(None),
    db: Session = Depends(get_db),
):
    deal = get_deal_or_404(db, deal_id)
    before = deal_snapshot(deal)
    payload = schemas.DealUpdate(
        title=title,
        short_description=short_description,
        status=status,
        weekday_pattern=clean_optional(weekday_pattern),
        start_time=clean_optional(start_time),
        end_time=clean_optional(end_time),
        source_url=clean_optional(source_url),
        source_text=clean_optional(source_text),
    )
    try:
        apply_deal_update(db, deal, payload)
    except HTTPException as exc:
        return HTMLResponse(render_admin_deal_edit_html(deal, str(exc.detail)), status_code=400)
    log_deal_change(db, "admin_edit_deal", deal, deal.venue_id, before, deal.source_text)
    db.commit()
    return RedirectResponse("/admin/deals", status_code=303)


def safe_admin_redirect_path(path: Optional[str]) -> str:
    cleaned = compact_text(path)
    return cleaned if cleaned.startswith("/admin/deals") else "/admin/deals"


@app.post("/admin/deals/{deal_id}/archive", dependencies=[Depends(require_admin_password)])
def admin_deals_page_archive(deal_id: int, return_to: str = Form("/admin/deals"), db: Session = Depends(get_db)):
    deal = get_deal_or_404(db, deal_id)
    before = deal_snapshot(deal)
    deal.status = models.Status.archived
    deal.updated_at = datetime.utcnow()
    log_deal_change(db, "admin_archive_deal", deal, deal.venue_id, before, deal.source_text)
    db.commit()
    return RedirectResponse(safe_admin_redirect_path(return_to), status_code=303)


@app.post("/admin/deals/{deal_id}/freeze", dependencies=[Depends(require_admin_password)])
def admin_deals_page_freeze(deal_id: int, return_to: str = Form("/admin/deals"), db: Session = Depends(get_db)):
    deal = get_deal_or_404(db, deal_id)
    before = deal_snapshot(deal)
    deal.freeze_minutes = FREEZE_SENTINEL_MINUTES
    deal.updated_at = datetime.utcnow()
    log_deal_change(db, "admin_freeze_deal", deal, deal.venue_id, before, deal.source_text)
    db.commit()
    return RedirectResponse(safe_admin_redirect_path(return_to), status_code=303)


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


@app.get("/admin/venues", dependencies=[Depends(require_admin)])
def admin_list_venues(
    owner_id: Optional[int] = None,
    neighborhood: Optional[str] = None,
    q: Optional[str] = None,
    has_owner: Optional[bool] = None,
    db: Session = Depends(get_db),
):
    try:
        expire_stale_last_minute_deals(db)

        query = db.query(models.Venue)
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

        venues = query.order_by(models.Venue.name.asc()).limit(100).all()
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

        owner_names: dict[int, str] = {}
        owner_ids = [venue.owner_id for venue in venues if venue.owner_id is not None]
        if owner_ids:
            try:
                owner_rows = db.query(models.BusinessOwner.id, models.BusinessOwner.name).filter(models.BusinessOwner.id.in_(owner_ids)).all()
                owner_names = {owner_id: name for owner_id, name in owner_rows}
            except SQLAlchemyError:
                db.rollback()
                logger.warning("Admin venue lookup skipped owner names because business_owners is unavailable.", exc_info=True)

        return JSONResponse(
            [
                venue_to_admin_lookup_json(
                    venue,
                    counts=counts.get(venue.id),
                    owner_name=owner_names.get(venue.owner_id) if venue.owner_id is not None else None,
                )
                for venue in venues
            ]
        )
    except Exception:
        db.rollback()
        logger.exception("Admin venue lookup failed.")
        return JSONResponse([])


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


@app.get("/admin/deals.json", response_model=List[schemas.AdminDealOut], dependencies=[Depends(require_admin)])
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


@app.patch("/admin/deals/{deal_id}.json", response_model=schemas.AdminDealOut, dependencies=[Depends(require_admin)])
def admin_update_deal(deal_id: int, payload: schemas.DealUpdate, db: Session = Depends(get_db)):
    deal = get_deal_or_404(db, deal_id)
    apply_deal_update(db, deal, payload)
    db.commit()
    return get_deal_with_venue_or_404(db, deal_id)


@app.post("/admin/deals/{deal_id}/archive.json", response_model=schemas.AdminDealOut, dependencies=[Depends(require_admin)])
def admin_archive_deal(deal_id: int, db: Session = Depends(get_db)):
    deal = get_deal_or_404(db, deal_id)
    deal.status = models.Status.archived
    deal.updated_at = datetime.utcnow()
    db.commit()
    return get_deal_with_venue_or_404(db, deal_id)


@app.post("/admin/deals/{deal_id}/expire.json", response_model=schemas.AdminDealOut, dependencies=[Depends(require_admin)])
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
