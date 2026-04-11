#!/usr/bin/env python3

from __future__ import annotations

import os
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DOCS_DIR = PROJECT_ROOT / "docs"
DAYS_DIR = DOCS_DIR / "days"
TODAY_DIR = DOCS_DIR / "today"
FOR_VENUES_DIR = DOCS_DIR / "for-venues"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

os.environ["DSM_DEALS_SITE_BASE_PATH"] = "dsm-deals-hub"

from app.database import SessionLocal  # noqa: E402
from app.main import (  # noqa: E402
    DAY_ORDER,
    WEEKDAY_LONG,
    days_page_sections,
    homepage_sections,
    neighborhood_groups,
    normalize_slug,
    render_day_detail_html,
    render_days_html,
    render_for_venues_html,
    render_homepage_html,
    render_today_html,
    today_page_data,
)
from app.neighborhood_icons import sync_neighborhood_icon_assets  # noqa: E402


def clear_existing_day_exports() -> None:
    if not DAYS_DIR.exists():
        return
    for child in DAYS_DIR.iterdir():
        if child.is_dir():
            for nested in child.rglob("*"):
                if nested.is_file():
                    nested.unlink()
            for nested_dir in sorted((p for p in child.rglob("*") if p.is_dir()), reverse=True):
                nested_dir.rmdir()
            child.rmdir()


def main() -> None:
    sync_neighborhood_icon_assets()
    db = SessionLocal()
    try:
        day_sections = days_page_sections(db)
        homepage_data = homepage_sections(db)
        neighborhoods = neighborhood_groups(db)
        today_data = today_page_data(db)
    finally:
        db.close()

    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    DAYS_DIR.mkdir(parents=True, exist_ok=True)
    TODAY_DIR.mkdir(parents=True, exist_ok=True)
    FOR_VENUES_DIR.mkdir(parents=True, exist_ok=True)
    clear_existing_day_exports()

    (DOCS_DIR / "index.html").write_text(
        render_homepage_html(homepage_data, neighborhoods, day_sections)
    )
    (TODAY_DIR / "index.html").write_text(render_today_html(today_data))
    (DAYS_DIR / "index.html").write_text(render_days_html(day_sections))
    (FOR_VENUES_DIR / "index.html").write_text(render_for_venues_html())

    for day_code in DAY_ORDER:
        day_slug = normalize_slug(WEEKDAY_LONG[day_code])
        target_dir = DAYS_DIR / day_slug
        target_dir.mkdir(parents=True, exist_ok=True)
        (target_dir / "index.html").write_text(
            render_day_detail_html(day_code, day_sections[day_code])
        )

    print(f"Exported {len(DAY_ORDER)} day detail pages to {DAYS_DIR}")


if __name__ == "__main__":
    main()
