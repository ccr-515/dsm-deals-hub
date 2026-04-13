#!/usr/bin/env python3

from __future__ import annotations

import csv
from datetime import datetime
import json
from pathlib import Path
import sys
from typing import Any
from zoneinfo import ZoneInfo


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = PROJECT_ROOT / "reports"
AUDIT_CSV_PATH = REPORTS_DIR / "venue_master_audit.csv"
AUDIT_JSON_PATH = REPORTS_DIR / "venue_master_audit.json"
MISMATCH_JSON_PATH = REPORTS_DIR / "venue_directory_mismatch_report.json"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.venue_directory import load_venue_directory, normalize_venue_key  # noqa: E402
from app.weekly_master_content import WEEKLY_MASTER_JSON_PATH  # noqa: E402


AUDIT_FIELDS = [
    "name",
    "slug",
    "aliases",
    "address",
    "phone",
    "website",
    "menu_url",
    "has_address",
    "has_phone",
    "should_show_directions",
    "should_show_call",
    "notes",
]


def load_weekly_unique_venues() -> list[dict[str, str]]:
    rows = json.loads(WEEKLY_MASTER_JSON_PATH.read_text())
    seen: dict[str, dict[str, str]] = {}
    for row in rows:
        name = " ".join(str(row.get("venue", "")).split())
        if not name:
            continue
        slug = normalize_venue_key(name)
        if slug and slug not in seen:
            seen[slug] = {"name": name, "slug": slug}
    return [seen[key] for key in sorted(seen, key=lambda item: seen[item]["name"].lower())]


def build_directory_indexes() -> tuple[dict[str, Any], dict[str, Any], dict[str, list[Any]]]:
    entries = load_venue_directory()
    by_slug: dict[str, Any] = {}
    by_name: dict[str, Any] = {}
    by_alias: dict[str, list[Any]] = {}

    for entry in entries:
        by_slug[normalize_venue_key(entry.slug)] = entry
        by_name[normalize_venue_key(entry.name)] = entry
        for alias in entry.aliases:
            by_alias.setdefault(normalize_venue_key(alias), []).append(entry)

    return by_slug, by_name, by_alias


def resolve_mapping_status(venue_name: str, venue_slug: str, by_slug: dict[str, Any], by_name: dict[str, Any], by_alias: dict[str, list[Any]]) -> tuple[str, Any | None]:
    name_key = normalize_venue_key(venue_name)
    slug_key = normalize_venue_key(venue_slug)

    if slug_key in by_slug:
        return "exact_slug", by_slug[slug_key]
    if name_key in by_name:
        return "exact_name", by_name[name_key]
    if name_key in by_alias and by_alias[name_key]:
        return "alias_match", by_alias[name_key][0]
    return "no_match", None


def bool_has_text(value: str | None) -> bool:
    return bool(value and str(value).strip())


def bool_has_phone(value: str | None) -> bool:
    if not value:
        return False
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    return len(digits) >= 10


def build_notes(mapping_status: str, record: dict[str, Any], has_address: bool, has_phone: bool) -> str:
    notes: list[str] = []
    if mapping_status == "alias_match":
        notes.append("Matched via alias")
    elif mapping_status == "no_match":
        notes.append("No venue directory match")

    missing_fields = [
        label
        for label, present in (
            ("address", has_address),
            ("phone", has_phone),
            ("website", bool_has_text(record.get("website"))),
            ("menu_url", bool_has_text(record.get("menu_url"))),
        )
        if not present
    ]
    if missing_fields:
        notes.append("Missing " + ", ".join(missing_fields))
    else:
        notes.append("Complete metadata")

    return "; ".join(notes)


def build_audit_rows() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    by_slug, by_name, by_alias = build_directory_indexes()
    weekly_venues = load_weekly_unique_venues()
    rows: list[dict[str, Any]] = []
    mismatches: list[dict[str, Any]] = []
    status_counts = {
        "exact_slug": 0,
        "exact_name": 0,
        "alias_match": 0,
        "no_match": 0,
    }

    for weekly_venue in weekly_venues:
        status, match = resolve_mapping_status(
            weekly_venue["name"],
            weekly_venue["slug"],
            by_slug,
            by_name,
            by_alias,
        )
        status_counts[status] += 1

        record = {
            "name": weekly_venue["name"],
            "slug": weekly_venue["slug"],
            "aliases": list(match.aliases) if match else [],
            "address": match.address if match else None,
            "phone": match.phone if match else None,
            "website": match.website if match else None,
            "menu_url": match.menu_url if match else None,
        }
        has_address = bool_has_text(record["address"])
        has_phone = bool_has_phone(record["phone"])
        should_show_directions = has_address or (
            match is not None and match.lat is not None and match.lng is not None
        )
        should_show_call = has_phone
        notes = build_notes(status, record, has_address, has_phone)

        row = {
            **record,
            "has_address": has_address,
            "has_phone": has_phone,
            "should_show_directions": should_show_directions,
            "should_show_call": should_show_call,
            "notes": notes,
        }
        rows.append(row)

        if status != "exact_slug":
            mismatches.append(
                {
                    "weekly_name": weekly_venue["name"],
                    "weekly_slug": weekly_venue["slug"],
                    "match_status": status,
                    "matched_directory_name": match.name if match else None,
                    "matched_directory_slug": match.slug if match else None,
                    "aliases": list(match.aliases) if match else [],
                    "notes": notes,
                }
            )

    summary = {
        "total_unique_weekly_venues": len(rows),
        "venues_with_address": sum(1 for row in rows if row["has_address"]),
        "venues_with_phone": sum(1 for row in rows if row["has_phone"]),
        "venues_showing_directions": sum(1 for row in rows if row["should_show_directions"]),
        "venues_showing_call": sum(1 for row in rows if row["should_show_call"]),
        "complete_metadata_count": sum(
            1
            for row in rows
            if row["has_address"] and row["has_phone"] and row["website"] and row["menu_url"]
        ),
        "mapping_status_counts": status_counts,
        "mismatch_count": len(mismatches),
    }

    mismatch_report = {
        "generated_at": datetime.now(ZoneInfo("America/Chicago")).isoformat(),
        "source_files": {
            "weekly_dataset": str(WEEKLY_MASTER_JSON_PATH),
            "venue_directory": str(PROJECT_ROOT / "app" / "venue_directory.json"),
        },
        "summary": summary,
        "mismatches": mismatches,
    }
    return rows, mismatch_report


def write_csv(rows: list[dict[str, Any]]) -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    with AUDIT_CSV_PATH.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=AUDIT_FIELDS)
        writer.writeheader()
        for row in rows:
            serializable = dict(row)
            serializable["aliases"] = json.dumps(serializable["aliases"], ensure_ascii=False)
            writer.writerow(serializable)


def write_json(rows: list[dict[str, Any]], mismatch_report: dict[str, Any]) -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    audit_payload = {
        "generated_at": datetime.now(ZoneInfo("America/Chicago")).isoformat(),
        "source_files": mismatch_report["source_files"],
        "summary": mismatch_report["summary"],
        "venues": rows,
    }
    AUDIT_JSON_PATH.write_text(json.dumps(audit_payload, indent=2, ensure_ascii=False) + "\n")
    MISMATCH_JSON_PATH.write_text(json.dumps(mismatch_report, indent=2, ensure_ascii=False) + "\n")


def main() -> int:
    rows, mismatch_report = build_audit_rows()
    write_csv(rows)
    write_json(rows, mismatch_report)
    print(f"Wrote {len(rows)} venue audit rows to {AUDIT_CSV_PATH}")
    print(f"Wrote venue audit JSON to {AUDIT_JSON_PATH}")
    print(f"Wrote mismatch report to {MISMATCH_JSON_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
