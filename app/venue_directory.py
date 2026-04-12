from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import json
from pathlib import Path
import re
import unicodedata


APP_DIR = Path(__file__).resolve().parent
VENUE_DIRECTORY_PATH = APP_DIR / "venue_directory.json"


@dataclass(frozen=True)
class VenueMetadata:
    name: str
    slug: str
    aliases: tuple[str, ...] = ()
    address: str | None = None
    phone: str | None = None
    website: str | None = None
    menu_url: str | None = None
    lat: float | None = None
    lng: float | None = None


def normalize_venue_key(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
    collapsed = re.sub(r"[^a-zA-Z0-9]+", "-", ascii_value.strip().lower())
    return collapsed.strip("-")


def _clean_address(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = " ".join(str(value).split())
    return cleaned or None


def _clean_phone(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = " ".join(str(value).split())
    digits = re.sub(r"\D+", "", cleaned)
    if len(digits) < 10:
        return None
    return cleaned


def _clean_optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = " ".join(str(value).split())
    if not cleaned or cleaned.lower() == "none":
        return None
    return cleaned


@lru_cache(maxsize=4)
def _load_venue_directory_cached(mtime_ns: int) -> tuple[VenueMetadata, ...]:
    del mtime_ns
    if not VENUE_DIRECTORY_PATH.exists():
        return ()

    payload = json.loads(VENUE_DIRECTORY_PATH.read_text())
    raw_entries = payload.get("venues", []) if isinstance(payload, dict) else payload
    entries: list[VenueMetadata] = []

    for row in raw_entries:
        if not isinstance(row, dict):
            continue
        aliases = tuple(
            value
            for value in (" ".join(str(alias).split()) for alias in row.get("aliases", []))
            if value
        )
        entries.append(
            VenueMetadata(
                name=" ".join(str(row.get("name", "")).split()),
                slug=" ".join(str(row.get("slug", "")).split()),
                aliases=aliases,
                address=_clean_address(row.get("address")),
                phone=_clean_phone(row.get("phone")),
                website=_clean_optional_text(row.get("website")),
                menu_url=_clean_optional_text(row.get("menu_url")),
                lat=row.get("lat"),
                lng=row.get("lng"),
            )
        )

    return tuple(entries)


def load_venue_directory() -> list[VenueMetadata]:
    if not VENUE_DIRECTORY_PATH.exists():
        return []
    return list(_load_venue_directory_cached(VENUE_DIRECTORY_PATH.stat().st_mtime_ns))


@lru_cache(maxsize=4)
def _venue_directory_index_cached(mtime_ns: int) -> dict[str, VenueMetadata]:
    index: dict[str, VenueMetadata] = {}
    for entry in _load_venue_directory_cached(mtime_ns):
        for key in (entry.slug, entry.name, *entry.aliases):
            normalized = normalize_venue_key(key)
            if normalized and normalized not in index:
                index[normalized] = entry
    return index


def venue_directory_index() -> dict[str, VenueMetadata]:
    if not VENUE_DIRECTORY_PATH.exists():
        return {}
    return _venue_directory_index_cached(VENUE_DIRECTORY_PATH.stat().st_mtime_ns)


def match_venue_metadata(name: str, slug: str | None = None) -> VenueMetadata | None:
    index = venue_directory_index()
    candidates = [slug or "", name]
    for candidate in candidates:
        normalized = normalize_venue_key(candidate)
        if normalized and normalized in index:
            return index[normalized]
    return None
