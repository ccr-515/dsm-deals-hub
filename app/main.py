from datetime import datetime, timedelta
from html import escape
import json
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
    ("🍷", ("wine",)),
    ("🦪", ("oyster", "oysters", "seafood", "shrimp", "fish", "walleye")),
    ("🥪", ("sandwich", "sandwiches", "cheesesteak", "cheesesteaks", "reuben", "deli", "wrap", "wraps", "doner", "döner", "tenderloin")),
    ("🥩", ("steak", "steaks", "ribeye", "ribeyes", "prime rib", "sirloin", "sirloins")),
    ("🍔", ("burger", "burgers", "cheeseburger", "cheeseburgers")),
    ("🌮", ("taco", "tacos")),
    ("🌭", ("hot dog", "polish", "wiener")),
    ("🍗", ("wings", "wing", "chicken", "chicken fried", "tenders")),
    ("🍕", ("pizza", "pizzas", "flatbread", "flatbreads")),
    ("🍳", ("breakfast", "brunch", "french toast", "pancake", "pancakes")),
    ("🍽️", ("buffet", "buffets")),
    ("🍺", ("beer", "pint")),
]

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


def homepage_sections(db: Session) -> dict[str, List[models.Deal]]:
    curated_deals = (
        db.query(models.Deal)
        .options(joinedload(models.Deal.venue))
        .filter(
            models.Deal.status == models.Status.live,
            models.Deal.source_url == HOMEPAGE_SEED_SOURCE,
        )
        .order_by(models.Deal.created_at.asc(), models.Deal.id.asc())
        .all()
    )
    buckets = {"live": [], "tonight": [], "week": []}
    for deal in curated_deals:
        bucket = homepage_metadata(deal).get("homepage_bucket", "week")
        if bucket in buckets:
            buckets[bucket].append(deal)
    for bucket, deals in buckets.items():
        buckets[bucket] = sorted(
            deals,
            key=lambda deal: (
                homepage_metadata(deal).get("rank", 999),
                deal.title.lower(),
            ),
        )
    return buckets


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
    return sorted(
        deals,
        key=lambda deal: (
            homepage_occurrence_key(deal, window_start),
            0 if deal.start_time else 1,
            deal.title.lower(),
        ),
    )


def format_clock(hour: int, minute: int) -> str:
    return datetime(2000, 1, 1, hour, minute).strftime("%I:%M %p").lstrip("0")


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


def deal_icon(deal: models.Deal) -> str:
    text = f"{deal.title} {deal.short_description}".lower()
    for icon, keywords in DEAL_ICON_KEYWORDS:
        for keyword in keywords:
            if re.search(rf"\b{re.escape(keyword)}\b", text):
                return icon
    return "✨"


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


def render_deal_card(deal: models.Deal, reference: datetime, order_index: int) -> str:
    venue_name = escape(deal.venue.name if deal.venue else "Venue")
    title = escape(deal.title)
    description = escape(deal.short_description)
    time_label = escape(format_deal_time(deal, reference))
    icon = escape(deal_icon(deal))
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
        <span class="deal-icon" aria-hidden="true">{icon}</span>
      </div>
      <h3>{title}</h3>
      <p class="deal-description">{description}</p>
      <div class="deal-card-footer">
        <p class="deal-time">{time_label}</p>
        {neighborhood}
      </div>
    </article>
    """


def render_live_empty_state() -> str:
    return """
    <div class="empty-state empty-state-live">
      <div class="live-empty-signal" aria-hidden="true"></div>
      <div class="live-empty-copy">
        <p class="live-empty-kicker">No live deals right now</p>
        <h3>Real-time specials appear here when they&#x27;re active.</h3>
        <p>
          Browse tonight&#x27;s deals below or plan ahead with this week&#x27;s recurring spots while the live board resets.
        </p>
      </div>
      <div class="live-empty-actions">
        <div class="empty-state-links">
          <a href="#tonight" class="empty-state-link">Browse Tonight</a>
          <a href="#this-week" class="empty-state-link">See This Week</a>
        </div>
        <div class="empty-state-cta" aria-label="Venue owners can get listed">
          <span class="empty-state-cta-plus">+</span>
          <span class="empty-state-cta-copy">
            <strong>Get listed</strong>
            <small>For venue owners</small>
          </span>
        </div>
      </div>
    </div>
    """


def render_section(
    section_id: str,
    title: str,
    intro: str,
    deals: List[models.Deal],
    empty_message: str,
    reference: datetime,
) -> str:
    grid_class = "deal-grid"
    controls = render_section_sort_controls(section_id, deals)
    if deals:
        cards = "\n".join(render_deal_card(deal, reference, index) for index, deal in enumerate(deals))
    elif section_id == "live-now":
        cards = render_live_empty_state()
        grid_class = "deal-grid deal-grid-empty"
    else:
        grid_class = "deal-grid deal-grid-empty"
        cards = f'<div class="empty-state">{escape(empty_message)}</div>'

    count_label = f"{len(deals)} deal" if len(deals) == 1 else f"{len(deals)} deals"

    return f"""
    <section id="{section_id}" class="content-section">
      <div class="section-panel">
        <div class="section-heading">
          <div>
            <p class="section-kicker">{escape(title)}</p>
            <div class="section-title-row">
              <h2>{escape(title)}</h2>
              <span class="section-count">{escape(count_label)}</span>
            </div>
          </div>
          <p>{escape(intro)}</p>
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


def render_homepage_html(sections: dict[str, List[models.Deal]]) -> str:
    now = datetime.now(HOME_TIMEZONE).replace(tzinfo=None)
    live_deals = sections["live"]
    tonight_deals = sections["tonight"]
    week_deals = sections["week"]

    return f"""<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>DSM Deals Hub</title>
    <meta
      name="description"
      content="Curated food and drink deals across Des Moines, updated manually."
    />
    <link rel="stylesheet" href="/static/styles.css" />
  </head>
  <body>
    <div class="page-shell">
      <header class="hero">
        <div class="hero-panel">
          <div class="hero-copy">
            <p class="eyebrow">Des Moines deals, curated daily</p>
            <h1>DSM Deals Hub</h1>
            <p class="hero-text">
              A warm, practical guide to food and drink specials happening across Des Moines right now and later this week.
            </p>
          </div>
          <div class="hero-utility">
            <nav class="section-nav" aria-label="Homepage sections">
              <a href="#live-now">Live Now</a>
              <a href="#tonight">Tonight</a>
              <a href="#this-week">This Week</a>
            </nav>
            <div class="hero-stats" aria-label="Deal counts">
              <div>
                <span>Live now</span>
                <strong>{len(live_deals)}</strong>
              </div>
              <div>
                <span>Tonight</span>
                <strong>{len(tonight_deals)}</strong>
              </div>
              <div>
                <span>This week</span>
                <strong>{len(week_deals)}</strong>
              </div>
            </div>
          </div>
        </div>
      </header>

      <main class="content">
        {render_section(
            "live-now",
            "Live Now",
            "Real-time specials that are active right now.",
            live_deals,
            "Nothing is live right now. Check the tonight section for what is coming up next.",
            now,
        )}
        {render_section(
            "tonight",
            "Tonight",
            "Good options for later today.",
            tonight_deals,
            "No deals have been posted for tonight yet.",
            now,
        )}
        {render_section(
            "this-week",
            "This Week",
            "Recurring and scheduled deals worth planning around.",
            week_deals,
            "This week's deals have not been added yet.",
            now,
        )}
      </main>

      <footer class="site-footer">
        DSM Deals Hub is curated manually from local posts and business updates.
      </footer>
    </div>
    {render_homepage_script()}
  </body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def homepage(db: Session = Depends(get_db)):
    return HTMLResponse(render_homepage_html(homepage_sections(db)))


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
