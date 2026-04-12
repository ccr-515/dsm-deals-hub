#!/usr/bin/env python3

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import re
import sys
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app import weekly_master_content as weekly_content
from app.neighborhood_icons import (
    SOURCE_ICON_DIR,
    available_neighborhood_icon_sources,
    normalize_icon_key,
    sync_neighborhood_icon_assets,
)


HOME_TIMEZONE = ZoneInfo("America/Chicago")
STROKE = "#242b31"
ACCENT_GREEN = "#3f6a50"
ACCENT_ORANGE = "#cf7243"


def neighborhood_icon_label(name: str) -> str:
    words = re.findall(r"[A-Za-z0-9]+", name or "")
    if not words:
        return "DM"
    filtered = [word for word in words if word.lower() not in {"and", "the", "of"}]
    words = filtered or words
    if len(words) == 1:
        return words[0][:2].upper()
    initials = "".join(word[0].upper() for word in words[:3])
    return initials[:3]


def icon_font_size(label: str) -> int:
    if len(label) >= 3:
        return 102
    return 136


def build_svg(name: str, label: str) -> str:
    font_size = icon_font_size(label)
    letter_spacing = "18" if len(label) >= 3 else "8"
    return f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512" fill="none">
  <title>{name}</title>
  <g stroke="{STROKE}" stroke-width="16" stroke-linecap="round" stroke-linejoin="round">
    <path d="M256 68c-47 0-84 37-84 83 0 25 14 53 42 89l42 52 42-52c28-36 42-64 42-89 0-46-37-83-84-83Z"/>
    <circle cx="256" cy="151" r="22"/>
    <path d="M122 312c0-24 19-43 43-43h182c24 0 43 19 43 43v83c0 24-19 43-43 43H165c-24 0-43-19-43-43Z"/>
    <path d="M151 452h210"/>
  </g>
  <path d="M184 438h144" stroke="{ACCENT_ORANGE}" stroke-width="12" stroke-linecap="round" opacity="0.95"/>
  <circle cx="360" cy="110" r="10" fill="{ACCENT_GREEN}" opacity="0.95"/>
  <text
    x="256"
    y="386"
    fill="{STROKE}"
    font-family="Avenir Next, Segoe UI, Helvetica Neue, Arial, sans-serif"
    font-size="{font_size}"
    font-weight="800"
    letter-spacing="{letter_spacing}"
    text-anchor="middle"
  >{label}</text>
</svg>
"""


def main() -> None:
    SOURCE_ICON_DIR.mkdir(parents=True, exist_ok=True)
    now = datetime.now(HOME_TIMEZONE).replace(tzinfo=None)
    groups = weekly_content.neighborhood_groups(now)
    existing_sources = available_neighborhood_icon_sources()
    generated = 0

    for group in groups:
        name = group["name"]
        slug = group["slug"]
        if slug in existing_sources:
            continue
        target = SOURCE_ICON_DIR / f"{slug}.svg"
        target.write_text(build_svg(name, neighborhood_icon_label(name)))
        generated += 1

    synced = sync_neighborhood_icon_assets()
    print(f"Generated {generated} neighborhood SVG icons")
    print(f"Synced {len(synced)} icon assets")


if __name__ == "__main__":
    main()
