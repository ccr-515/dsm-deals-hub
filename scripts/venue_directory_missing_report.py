from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
VENUE_DIRECTORY_PATH = PROJECT_ROOT / "app" / "venue_directory.json"


def load_entries() -> list[dict]:
    payload = json.loads(VENUE_DIRECTORY_PATH.read_text())
    return payload.get("venues", []) if isinstance(payload, dict) else payload


def by_missing(entries: list[dict], field: str) -> list[dict]:
    return [entry for entry in entries if not entry.get(field)]


def print_group(title: str, entries: Iterable[dict]) -> None:
    entries = list(entries)
    print(f"\n{title}: {len(entries)}")
    for entry in entries:
        print(f"- {entry['name']} ({entry['slug']})")


def venues_needing_core_actions(entries: list[dict]) -> list[dict]:
    return [entry for entry in entries if not entry.get("address") or not entry.get("phone")]


def main() -> int:
    entries = load_entries()
    print(f"Venue directory entries: {len(entries)}")

    print_group("Needs address or phone", venues_needing_core_actions(entries))

    print_group("Missing address", by_missing(entries, "address"))
    print_group("Missing phone", by_missing(entries, "phone"))
    print_group("Missing website", by_missing(entries, "website"))
    print_group("Missing menu_url", by_missing(entries, "menu_url"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
