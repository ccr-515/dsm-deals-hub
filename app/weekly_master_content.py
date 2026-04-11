from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
import csv
import json
from pathlib import Path
import re
import unicodedata


WEEKLY_MASTER_JSON_PATH = Path("/Users/camilorodriguez/Downloads/dsm_deals_hub_master_weekly_list.json")
WEEKLY_MASTER_CSV_PATH = Path("/Users/camilorodriguez/Downloads/dsm_deals_hub_master_weekly_list.csv")

EXPECTED_FIELDS = ("day", "venue", "neighborhood", "time", "title", "desc", "category")
DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
DAY_NAME_TO_CODE = {
    "Monday": "Mon",
    "Tuesday": "Tue",
    "Wednesday": "Wed",
    "Thursday": "Thu",
    "Friday": "Fri",
    "Saturday": "Sat",
    "Sunday": "Sun",
}
DAY_CODE_TO_NAME = {value: key for key, value in DAY_NAME_TO_CODE.items()}
DAY_CODE_ORDER = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
DAY_ALIASES = {
    "mon": "Monday",
    "monday": "Monday",
    "tue": "Tuesday",
    "tues": "Tuesday",
    "tuesday": "Tuesday",
    "wed": "Wednesday",
    "weds": "Wednesday",
    "wednesday": "Wednesday",
    "thu": "Thursday",
    "thur": "Thursday",
    "thurs": "Thursday",
    "thursday": "Thursday",
    "fri": "Friday",
    "friday": "Friday",
    "sat": "Saturday",
    "saturday": "Saturday",
    "sun": "Sunday",
    "sunday": "Sunday",
}
NEIGHBORHOOD_NORMALIZATION_MAP = {
    "des moines area": "Des Moines",
    "des moines metro": "Des Moines",
    "court district": "Downtown",
    "court avenue": "Downtown",
    "western gateway": "Downtown",
    "downtown des moines": "Downtown",
    "prairie meadows": "Altoona",
}
WEEKEND_SECTION_ORDER = ["Brunch", "Afternoon", "Dinner", "Late Night", "Live Music", "Specials"]
WEEKEND_SECTION_INTROS = {
    "Brunch": "Brunch tables, buffet stops, and first-round weekend pours.",
    "Afternoon": "Daytime happy hours, patio windows, and mid-day weekend stops.",
    "Dinner": "Dinner-led picks, seafood nights, and stronger evening specials.",
    "Late Night": "Later pours, post-dinner stops, and end-of-night windows.",
    "Live Music": "Music-forward listings that still belong on the weekend board.",
    "Specials": "Everything else worth keeping on the weekend guide.",
}
CATEGORY_PRIORITY = {
    "Brunch buffet": 0,
    "Breakfast buffet": 1,
    "Brunch drink special": 2,
    "Buffet": 3,
    "Lunch special": 4,
    "Food and drink special": 5,
    "Dinner special": 6,
    "Food special": 7,
    "Drink special": 8,
    "Family dining special": 9,
    "Dessert special": 10,
    "Late night drink special": 11,
}
PREVIEW_CATEGORY_PRIORITY = {
    "Dinner special": 0,
    "Lunch special": 1,
    "Food and drink special": 2,
    "Brunch buffet": 3,
    "Breakfast buffet": 4,
    "Brunch drink special": 5,
    "Buffet": 6,
    "Food special": 7,
    "Drink special": 8,
    "Late night drink special": 9,
    "Family dining special": 10,
    "Dessert special": 11,
}
DAY_PART_FALLBACK_MINUTES = {
    "Brunch": 10 * 60,
    "Afternoon": 15 * 60,
    "Dinner": 18 * 60,
    "Late Night": 21 * 60,
    "Live Music": 21 * 60,
    "Specials": 12 * 60,
}
TIME_POINT_RE = re.compile(r"(\d{1,2})(?::(\d{2}))?\s*([AP]M)", re.IGNORECASE)
TIME_RANGE_RE = re.compile(
    r"(?P<start>\d{1,2}(?::\d{2})?\s*[AP]M)\s*to\s*(?P<end>\d{1,2}(?::\d{2})?\s*[AP]M|close)",
    re.IGNORECASE,
)
AFTER_RE = re.compile(r"after\s+(?P<start>\d{1,2}(?::\d{2})?\s*[AP]M)", re.IGNORECASE)
UNTIL_RE = re.compile(r"until\s+(?P<end>\d{1,2}(?::\d{2})?\s*[AP]M)", re.IGNORECASE)
DAY_TOKEN_RE = re.compile(
    r"\b(mon(?:day)?|tues?(?:day)?|wed(?:nesday)?|thu(?:rs?(?:day)?)?|fri(?:day)?|sat(?:urday)?|sun(?:day)?)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class SiteVenue:
    id: int
    name: str
    slug: str
    neighborhood: str
    lat: float | None = None
    lng: float | None = None


@dataclass(frozen=True)
class SiteDeal:
    id: int
    venue_id: int
    title: str
    short_description: str
    weekday_pattern: str
    start_time: str | None
    end_time: str | None
    notes_private: str
    venue: SiteVenue


def _normalize_slug(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
    collapsed = re.sub(r"[^a-zA-Z0-9]+", "-", ascii_value.strip().lower())
    return collapsed.strip("-")


def _normalize_day_name_token(value: str) -> str:
    token = re.sub(r"[^a-zA-Z]+", "", value or "").lower()
    if token not in DAY_ALIASES:
        raise RuntimeError(f"Unsupported weekday token: {value!r}")
    return DAY_ALIASES[token]


def normalize_day_name(
    raw_day: str,
    time_label: str = "",
    title: str = "",
    description: str = "",
) -> str:
    if raw_day and raw_day.strip():
        compact = raw_day.strip().lower()
        if compact in DAY_ALIASES:
            return DAY_ALIASES[compact]
        try:
            return _normalize_day_name_token(raw_day)
        except RuntimeError:
            pass

    combined_text = " ".join(part for part in (raw_day, time_label, title, description) if part).lower()
    day_match = DAY_TOKEN_RE.search(combined_text)
    if day_match:
        return _normalize_day_name_token(day_match.group(1))

    raise RuntimeError(
        "Could not normalize weekly master day value "
        f"from raw_day={raw_day!r}, time_label={time_label!r}, title={title!r}"
    )


def normalize_day_code(
    raw_day: str,
    time_label: str = "",
    title: str = "",
    description: str = "",
) -> str:
    return DAY_NAME_TO_CODE[normalize_day_name(raw_day, time_label, title, description)]


def normalize_day_code_value(value: str) -> str:
    if value in DAY_CODE_TO_NAME:
        return value
    return normalize_day_code(value)


def _read_json_rows() -> list[dict[str, str]]:
    if not WEEKLY_MASTER_JSON_PATH.exists():
        raise RuntimeError(f"Weekly master JSON not found: {WEEKLY_MASTER_JSON_PATH}")
    rows = json.loads(WEEKLY_MASTER_JSON_PATH.read_text())
    if not isinstance(rows, list):
        raise RuntimeError("Weekly master JSON must be a top-level list")
    return [_clean_row(row) for row in rows]


def _read_csv_rows() -> list[dict[str, str]]:
    if not WEEKLY_MASTER_CSV_PATH.exists():
        raise RuntimeError(f"Weekly master CSV not found: {WEEKLY_MASTER_CSV_PATH}")
    with WEEKLY_MASTER_CSV_PATH.open(newline="") as handle:
        reader = csv.DictReader(handle)
        return [_clean_row(row) for row in reader]


def _clean_row(row: dict[str, str]) -> dict[str, str]:
    cleaned = {}
    for field in EXPECTED_FIELDS:
        value = row.get(field, "")
        cleaned[field] = " ".join(str(value).split())
    return cleaned


def _canonical_row(row: dict[str, str]) -> tuple[str, ...]:
    return tuple(row.get(field, "") for field in EXPECTED_FIELDS)


def _validate_source_files(json_rows: list[dict[str, str]], csv_rows: list[dict[str, str]]) -> None:
    if len(json_rows) != len(csv_rows):
        raise RuntimeError(
            f"Weekly master JSON/CSV row count mismatch: {len(json_rows)} json rows vs {len(csv_rows)} csv rows"
        )
    json_set = {_canonical_row(row) for row in json_rows}
    csv_set = {_canonical_row(row) for row in csv_rows}
    if json_set != csv_set:
        raise RuntimeError("Weekly master JSON and CSV do not describe the same rows")


def _normalize_neighborhood(value: str) -> str:
    if not value:
        return "Des Moines"
    cleaned = value.split(" / ")[0].strip() if " / " in value else value.strip()
    return NEIGHBORHOOD_NORMALIZATION_MAP.get(cleaned.lower(), cleaned)


def _ampm_to_hhmm(token: str) -> str:
    match = TIME_POINT_RE.search(token)
    if not match:
        raise ValueError(f"Unsupported time token: {token}")
    hour = int(match.group(1))
    minute = int(match.group(2) or 0)
    ampm = match.group(3).upper()
    if ampm == "AM":
        if hour == 12:
            hour = 0
    else:
        if hour != 12:
            hour += 12
    return f"{hour:02d}:{minute:02d}"


def _minutes_from_hhmm(value: str | None) -> int | None:
    if not value:
        return None
    hour, minute = value.split(":")
    return int(hour) * 60 + int(minute)


def _time_sort_minutes(start_time: str | None, day_part: str) -> int:
    return _minutes_from_hhmm(start_time) or DAY_PART_FALLBACK_MINUTES.get(day_part, 12 * 60)


def _parse_time_window(time_label: str) -> tuple[str | None, str | None]:
    label = (time_label or "").strip()
    if not label:
        return None, None

    range_match = TIME_RANGE_RE.search(label)
    if range_match:
        start = _ampm_to_hhmm(range_match.group("start"))
        end_token = range_match.group("end")
        if end_token.lower() == "close":
            return start, "23:59"
        end = _ampm_to_hhmm(end_token)
        end_minutes = _minutes_from_hhmm(end)
        start_minutes = _minutes_from_hhmm(start)
        if end_minutes is not None and start_minutes is not None and end_minutes <= start_minutes:
            return start, "23:59"
        return start, end

    after_match = AFTER_RE.search(label)
    if after_match:
        return _ampm_to_hhmm(after_match.group("start")), "23:59"

    until_match = UNTIL_RE.search(label)
    if until_match:
        return "10:00", _ampm_to_hhmm(until_match.group("end"))

    lower_label = label.lower()
    if "all day" in lower_label or "all night" in lower_label:
        return "11:00", "23:59"

    return None, None


def _classify_day_part(category: str, time_label: str, title: str, description: str, start_time: str | None) -> str:
    text = f"{category} {time_label} {title} {description}".lower()
    start_minutes = _minutes_from_hhmm(start_time)

    if any(keyword in text for keyword in ("live music", "music", "band", "dj", "karaoke")):
        return "Live Music"
    if "lunch" in text:
        return "Afternoon"
    if any(keyword in text for keyword in ("brunch", "breakfast", "mimosa")):
        return "Brunch"
    if "late night" in text or (start_minutes is not None and start_minutes >= 21 * 60):
        return "Late Night"
    if any(keyword in text for keyword in ("dinner", "prime rib", "steak", "sirloin", "surf and turf")):
        return "Dinner"
    if any(keyword in text for keyword in ("happy hour", "aperitivo")):
        return "Afternoon"
    if start_minutes is not None and start_minutes < 13 * 60:
        return "Brunch"
    if start_minutes is not None and start_minutes < 17 * 60:
        return "Afternoon"
    if start_minutes is not None and start_minutes >= 17 * 60:
        return "Dinner"
    return "Specials"


def _is_happy_hour(category: str, time_label: str, title: str, description: str, start_time: str | None) -> bool:
    text = f"{category} {time_label} {title} {description}".lower()
    if "drink" in category.lower() or "happy hour" in text or "wine" in text or "cocktail" in text or "martini" in text:
        return True
    start_minutes = _minutes_from_hhmm(start_time)
    return start_minutes is not None and 14 * 60 <= start_minutes <= 18 * 60


def _category_rank(category: str) -> int:
    return CATEGORY_PRIORITY.get(category, 999)


def _preview_category_rank(category: str) -> int:
    return PREVIEW_CATEGORY_PRIORITY.get(category, 999)


def _has_explicit_window(time_label: str) -> bool:
    lower_label = time_label.lower()
    return bool(TIME_POINT_RE.search(time_label) or "all day" in lower_label or "after " in lower_label)


def _signal_keyword_rank(title: str, description: str) -> int:
    text = f"{title} {description}".lower()
    priority_groups = [
        ("prime rib", "ribeye", "sirloin", "surf and turf", "steak", "oyster", "seafood", "fish", "walleye"),
        ("brunch", "buffet", "mimosa", "wine", "martini", "cocktail", "pizza", "burger", "taco", "wings"),
        ("sandwich", "combo", "happy hour", "beer", "pint"),
    ]
    for rank, keywords in enumerate(priority_groups):
        if any(keyword in text for keyword in keywords):
            return rank
    if any(keyword in text for keyword in ("kids eat free", "baklava")):
        return 8
    return 5


def _build_notes_meta(
    day_name: str,
    day_code: str,
    time_label: str,
    category: str,
    day_part: str,
    happy_hour: bool,
) -> str:
    return json.dumps(
        {
            "site_source": "weekly_master",
            "day_name": day_name,
            "day_code": day_code,
            "time_label": time_label,
            "category": category,
            "day_part": day_part,
            "today_bucket": "happy_hour" if happy_hour else "specials",
            "confidence": "high",
        }
    )


@lru_cache(maxsize=4)
def _load_weekly_master_deals_cached(json_mtime_ns: int, csv_mtime_ns: int) -> tuple[SiteDeal, ...]:
    del json_mtime_ns, csv_mtime_ns
    json_rows = _read_json_rows()
    csv_rows = _read_csv_rows()
    _validate_source_files(json_rows, csv_rows)

    venues_by_slug: dict[str, SiteVenue] = {}
    next_venue_id = 1
    deals: list[SiteDeal] = []

    for index, row in enumerate(json_rows, start=1):
        day_name = normalize_day_name(
            row["day"],
            time_label=row["time"],
            title=row["title"],
            description=row["desc"],
        )
        day_code = normalize_day_code(
            row["day"],
            time_label=row["time"],
            title=row["title"],
            description=row["desc"],
        )
        venue_slug = _normalize_slug(row["venue"])
        neighborhood = _normalize_neighborhood(row["neighborhood"])

        if venue_slug not in venues_by_slug:
            venues_by_slug[venue_slug] = SiteVenue(
                id=next_venue_id,
                name=row["venue"],
                slug=venue_slug,
                neighborhood=neighborhood,
            )
            next_venue_id += 1

        start_time, end_time = _parse_time_window(row["time"])
        day_part = _classify_day_part(row["category"], row["time"], row["title"], row["desc"], start_time)
        happy_hour = _is_happy_hour(row["category"], row["time"], row["title"], row["desc"], start_time)

        deals.append(
            SiteDeal(
                id=index,
                venue_id=venues_by_slug[venue_slug].id,
                title=row["title"],
                short_description=row["desc"],
                weekday_pattern=day_code,
                start_time=start_time,
                end_time=end_time,
                notes_private=_build_notes_meta(
                    day_name=day_name,
                    day_code=day_code,
                    time_label=row["time"],
                    category=row["category"],
                    day_part=day_part,
                    happy_hour=happy_hour,
                ),
                venue=venues_by_slug[venue_slug],
            )
        )

    return tuple(deals)


def load_weekly_master_deals() -> list[SiteDeal]:
    return list(
        _load_weekly_master_deals_cached(
            WEEKLY_MASTER_JSON_PATH.stat().st_mtime_ns,
            WEEKLY_MASTER_CSV_PATH.stat().st_mtime_ns,
        )
    )


def site_deal_day_code(deal: SiteDeal) -> str:
    return json.loads(deal.notes_private)["day_code"]


def site_deal_day_name(deal: SiteDeal) -> str:
    return json.loads(deal.notes_private)["day_name"]


def site_deal_time_label(deal: SiteDeal) -> str:
    return json.loads(deal.notes_private)["time_label"]


def site_deal_category(deal: SiteDeal) -> str:
    return json.loads(deal.notes_private)["category"]


def site_deal_day_part(deal: SiteDeal) -> str:
    return json.loads(deal.notes_private)["day_part"]


def site_deal_is_happy_hour(deal: SiteDeal) -> bool:
    return json.loads(deal.notes_private)["today_bucket"] == "happy_hour"


def site_deal_matches_day_code(deal: SiteDeal, day_code: str) -> bool:
    return site_deal_day_code(deal) == normalize_day_code_value(day_code)


def site_deal_is_live_now(deal: SiteDeal, reference: datetime) -> bool:
    if site_deal_day_code(deal) != normalize_day_code_value(reference.strftime("%a")):
        return False
    now_minutes = reference.hour * 60 + reference.minute
    start_minutes = _minutes_from_hhmm(deal.start_time)
    end_minutes = _minutes_from_hhmm(deal.end_time)
    if start_minutes is None and end_minutes is None:
        return False
    if start_minutes is None:
        return now_minutes <= end_minutes
    if end_minutes is None:
        return now_minutes >= start_minutes
    return start_minutes <= now_minutes <= end_minutes


def _day_distance(day_code: str, reference_day_code: str) -> int:
    normalized_reference_day_code = normalize_day_code_value(reference_day_code)
    normalized_day_code = normalize_day_code_value(day_code)
    reference_index = DAY_CODE_ORDER.index(normalized_reference_day_code)
    day_index = DAY_CODE_ORDER.index(normalized_day_code)
    return (day_index - reference_index) % len(DAY_CODE_ORDER)


def sort_day_deals(deals: list[SiteDeal]) -> list[SiteDeal]:
    return sorted(
        deals,
        key=lambda deal: (
            _time_sort_minutes(deal.start_time, site_deal_day_part(deal)),
            _category_rank(site_deal_category(deal)),
            deal.venue.name.lower(),
            deal.title.lower(),
        ),
    )


def sort_site_deals(deals: list[SiteDeal], reference: datetime) -> list[SiteDeal]:
    reference_day_code = normalize_day_code_value(reference.strftime("%a"))
    return sorted(
        deals,
        key=lambda deal: (
            _day_distance(site_deal_day_code(deal), reference_day_code),
            _time_sort_minutes(deal.start_time, site_deal_day_part(deal)),
            _category_rank(site_deal_category(deal)),
            deal.venue.name.lower(),
            deal.title.lower(),
        ),
    )


def _curate_today_preview(deals: list[SiteDeal], day_code: str) -> list[SiteDeal]:
    if day_code == "Fri":
        day_part_rank = {"Afternoon": 0, "Dinner": 1, "Late Night": 2, "Specials": 3, "Brunch": 4}
    elif day_code in {"Sat", "Sun"}:
        day_part_rank = {"Brunch": 0, "Afternoon": 1, "Dinner": 2, "Late Night": 3, "Live Music": 4, "Specials": 5}
    else:
        day_part_rank = {"Afternoon": 0, "Dinner": 1, "Specials": 2, "Late Night": 3, "Brunch": 4}

    ordered = sorted(
        deals,
        key=lambda deal: (
            day_part_rank.get(site_deal_day_part(deal), 9),
            _preview_category_rank(site_deal_category(deal)),
            0 if _has_explicit_window(site_deal_time_label(deal)) else 1,
            _signal_keyword_rank(deal.title, deal.short_description),
            _time_sort_minutes(deal.start_time, site_deal_day_part(deal)),
            deal.venue.name.lower(),
            deal.title.lower(),
        ),
    )

    picked: list[SiteDeal] = []
    seen_ids: set[int] = set()
    seen_parts: set[str] = set()

    for deal in ordered:
        day_part = site_deal_day_part(deal)
        if day_part in seen_parts:
            continue
        picked.append(deal)
        seen_ids.add(deal.id)
        seen_parts.add(day_part)
        if len(picked) >= 3:
            break

    for deal in ordered:
        if deal.id in seen_ids:
            continue
        picked.append(deal)

    return picked


def homepage_sections(reference: datetime) -> dict[str, list[SiteDeal]]:
    deals = load_weekly_master_deals()
    day_code = normalize_day_code_value(reference.strftime("%a"))
    today_deals = [deal for deal in deals if site_deal_matches_day_code(deal, day_code)]
    live_deals = [deal for deal in today_deals if site_deal_is_live_now(deal, reference)]
    return {
        "live": sort_day_deals(live_deals),
        "today": _curate_today_preview(today_deals, day_code),
    }


def days_page_sections(reference: datetime) -> dict[str, list[SiteDeal]]:
    deals = load_weekly_master_deals()
    sections: dict[str, list[SiteDeal]] = {}
    for day_code in DAY_CODE_ORDER:
        sections[day_code] = sort_day_deals([deal for deal in deals if site_deal_matches_day_code(deal, day_code)])
    return sections


def today_page_data(reference: datetime) -> dict:
    day_code = normalize_day_code_value(reference.strftime("%a"))
    all_today = days_page_sections(reference)[day_code]
    happy_hour = [deal for deal in all_today if site_deal_is_happy_hour(deal)]
    specials = [deal for deal in all_today if deal not in happy_hour]
    return {
        "day_code": day_code,
        "day_label": DAY_CODE_TO_NAME[day_code],
        "all": all_today,
        "happy_hour": happy_hour,
        "specials": specials,
    }


def neighborhood_groups(reference: datetime) -> list[dict]:
    groups: dict[str, dict] = {}
    for deal in load_weekly_master_deals():
        neighborhood = deal.venue.neighborhood.strip() if deal.venue.neighborhood else "Des Moines"
        group = groups.setdefault(
            neighborhood,
            {
                "name": neighborhood,
                "slug": _normalize_slug(neighborhood),
                "deals": [],
                "venue_ids": set(),
            },
        )
        group["deals"].append(deal)
        group["venue_ids"].add(deal.venue_id)

    results = []
    for group in groups.values():
        deals = sort_site_deals(group["deals"], reference)
        results.append(
            {
                "name": group["name"],
                "slug": group["slug"],
                "deals": deals,
                "deal_count": len(deals),
                "venue_count": len(group["venue_ids"]),
            }
        )
    return sorted(results, key=lambda item: (-item["deal_count"], item["name"]))


def day_detail_sections(day_code: str, reference: datetime) -> list[dict]:
    normalized_day_code = normalize_day_code_value(day_code)
    day_name = DAY_CODE_TO_NAME[normalized_day_code]
    deals = days_page_sections(reference)[normalized_day_code]
    if normalized_day_code not in {"Sat", "Sun"}:
        return [
            {
                "title": "Featured Deals",
                "intro": f"Curated restaurants, taverns, brunches, and specials currently featured for {day_name}.",
                "deals": deals,
            }
        ]

    grouped: dict[str, list[SiteDeal]] = {section: [] for section in WEEKEND_SECTION_ORDER}
    for deal in deals:
        grouped.setdefault(site_deal_day_part(deal), []).append(deal)

    sections = []
    for section_name in WEEKEND_SECTION_ORDER:
        section_deals = grouped.get(section_name) or []
        if not section_deals:
            continue
        sections.append(
            {
                "title": section_name,
                "intro": WEEKEND_SECTION_INTROS[section_name],
                "deals": section_deals,
            }
        )

    if sections:
        return sections

    return [
        {
            "title": "Featured Deals",
            "intro": f"Curated restaurants, taverns, brunches, and specials currently featured for {day_name}.",
            "deals": deals,
        }
    ]
