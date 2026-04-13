from __future__ import annotations

"""Build the editable venue directory from the weekly master dataset.

This script is intentionally conservative:
- It extracts unique venues from the current weekly dataset.
- It preserves existing manual enrichment in venue_directory.json.
- It only fills address/phone/website when a safer existing local source has it.
"""

import json
from pathlib import Path
import sqlite3

PROJECT_ROOT = Path(__file__).resolve().parents[1]
WEEKLY_MASTER_JSON_PATH = Path("/Users/camilorodriguez/Downloads/dsm_deals_hub_master_weekly_list.json")
DB_PATH = PROJECT_ROOT / "dsm_deals.db"
OUTPUT_PATH = PROJECT_ROOT / "app" / "venue_directory.json"

import sys

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.venue_directory import normalize_venue_key


def clean_address(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = " ".join(str(value).split())
    lowered = cleaned.lower()
    if "pending manual verification" in lowered:
        return None
    if lowered in {"des moines metro", "greater des moines", "des moines area", "metro des moines"}:
        return None
    return cleaned or None


def clean_phone(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = " ".join(str(value).split())
    digits = "".join(ch for ch in cleaned if ch.isdigit())
    if len(digits) < 10:
        return None
    return cleaned


def clean_optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = " ".join(str(value).split())
    if not cleaned or cleaned.lower() == "none":
        return None
    return cleaned


def compact_key(value: str) -> str:
    normalized = normalize_venue_key(value).replace("-and-", "-")
    return "".join(ch for ch in normalized if ch.isalnum())


def metadata_score(row: dict) -> tuple[int, int, int]:
    return (
        1 if row.get("address") else 0,
        1 if row.get("phone") else 0,
        1 if row.get("website") else 0,
    )


def load_existing_directory() -> dict[str, dict]:
    if not OUTPUT_PATH.exists():
        return {}
    payload = json.loads(OUTPUT_PATH.read_text())
    entries = payload.get("venues", []) if isinstance(payload, dict) else payload
    indexed: dict[str, dict] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        slug = normalize_venue_key(entry.get("slug", ""))
        if not slug:
            continue
        indexed[slug] = entry
    return indexed


def load_weekly_venues() -> list[dict]:
    rows = json.loads(WEEKLY_MASTER_JSON_PATH.read_text())
    seen: dict[str, dict] = {}
    for row in rows:
        name = " ".join(str(row.get("venue", "")).split())
        slug = normalize_venue_key(name)
        if slug and slug not in seen:
            seen[slug] = {"name": name, "slug": slug}
    return [seen[key] for key in sorted(seen, key=lambda item: seen[item]["name"].lower())]


def load_db_venues() -> list[dict]:
    if not DB_PATH.exists():
        return []
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            SELECT name, slug, address, phone, website, lat, lng
            FROM venues
            ORDER BY name
            """
        ).fetchall()
    finally:
        connection.close()

    db_rows: list[dict] = []
    for row in rows:
        db_rows.append(
            {
                "name": " ".join(str(row["name"] or "").split()),
                "slug": normalize_venue_key(row["slug"] or row["name"] or ""),
                "address": clean_address(row["address"]),
                "phone": clean_phone(row["phone"]),
                "website": " ".join(str(row["website"] or "").split()) or None,
                "lat": row["lat"],
                "lng": row["lng"],
            }
        )
    return db_rows


def match_db_candidates(weekly_entry: dict, db_rows: list[dict]) -> list[dict]:
    weekly_name_key = normalize_venue_key(weekly_entry["name"])
    weekly_slug = weekly_entry["slug"]
    weekly_name_compact = compact_key(weekly_entry["name"])
    weekly_slug_compact = compact_key(weekly_slug)
    candidates: list[dict] = []

    for row in db_rows:
        row_name_key = normalize_venue_key(row["name"])
        row_slug = row["slug"]
        row_name_compact = compact_key(row["name"])
        row_slug_compact = compact_key(row_slug)
        if weekly_slug in {row_slug, row_name_key} or weekly_name_key in {row_slug, row_name_key}:
            candidates.append(row)
            continue
        if weekly_slug_compact and weekly_slug_compact in {row_slug_compact, row_name_compact}:
            candidates.append(row)
            continue
        if weekly_name_compact and weekly_name_compact in {row_slug_compact, row_name_compact}:
            candidates.append(row)
            continue
        if row_slug.startswith(f"{weekly_slug}-") or weekly_slug.startswith(f"{row_slug}-"):
            candidates.append(row)
            continue
        if row_name_key.startswith(f"{weekly_name_key}-") or weekly_name_key.startswith(f"{row_name_key}-"):
            candidates.append(row)

    deduped: list[dict] = []
    seen = set()
    for row in candidates:
        key = (row["slug"], row["name"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    deduped.sort(key=metadata_score, reverse=True)
    return deduped


def build_directory_entries() -> list[dict]:
    weekly_venues = load_weekly_venues()
    db_rows = load_db_venues()
    existing_entries = load_existing_directory()
    entries: list[dict] = []

    for venue in weekly_venues:
        candidates = match_db_candidates(venue, db_rows)
        best = candidates[0] if candidates else None
        existing = existing_entries.get(venue["slug"], {})
        aliases: list[str] = []
        for value in existing.get("aliases", []):
            cleaned = " ".join(str(value).split())
            if cleaned and cleaned not in aliases and cleaned not in {venue["slug"], venue["name"]}:
                aliases.append(cleaned)
        for candidate in candidates:
            for value in (candidate["slug"], candidate["name"]):
                if value and value not in aliases and value not in {venue["slug"], venue["name"]}:
                    aliases.append(value)

        entries.append(
            {
                "name": venue["name"],
                "slug": venue["slug"],
                "aliases": aliases,
                "address": clean_optional_text(existing.get("address")) or (best["address"] if best else None),
                "phone": clean_optional_text(existing.get("phone")) or (best["phone"] if best else None),
                "website": clean_optional_text(existing.get("website")) or (best["website"] if best else None),
                "menu_url": clean_optional_text(existing.get("menu_url")),
                "lat": existing.get("lat") if existing.get("lat") is not None else (best["lat"] if best and best["lat"] is not None else None),
                "lng": existing.get("lng") if existing.get("lng") is not None else (best["lng"] if best and best["lng"] is not None else None),
            }
        )

    return entries


def main() -> int:
    entries = build_directory_entries()
    payload = {
        "venues": entries,
    }
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n")
    print(f"Wrote {len(entries)} venue directory entries to {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
