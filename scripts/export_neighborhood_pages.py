#!/usr/bin/env python3

from __future__ import annotations

import os
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DOCS_DIR = PROJECT_ROOT / "docs"
NEIGHBORHOODS_DIR = DOCS_DIR / "neighborhoods"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Vercel/static exports in this project should use root-relative public paths.
os.environ.pop("DSM_DEALS_SITE_BASE_PATH", None)

from app.database import SessionLocal  # noqa: E402
from app.main import (  # noqa: E402
    NEIGHBORHOOD_ROUTE_ALIASES,
    neighborhood_groups,
    render_neighborhood_detail_html,
    render_neighborhoods_html,
)
from app.neighborhood_icons import sync_neighborhood_icon_assets  # noqa: E402
from app.weekly_master_content import reset_weekly_master_deals_cache  # noqa: E402


def clear_existing_neighborhood_exports() -> None:
    if not NEIGHBORHOODS_DIR.exists():
        return
    for child in NEIGHBORHOODS_DIR.iterdir():
        if child.is_dir():
            for nested in child.rglob("*"):
                if nested.is_file():
                    nested.unlink()
            for nested_dir in sorted((p for p in child.rglob("*") if p.is_dir()), reverse=True):
                nested_dir.rmdir()
            child.rmdir()


def main() -> None:
    sync_neighborhood_icon_assets()
    reset_weekly_master_deals_cache()
    db = SessionLocal()
    try:
        groups = neighborhood_groups(db)
    finally:
        db.close()

    NEIGHBORHOODS_DIR.mkdir(parents=True, exist_ok=True)
    clear_existing_neighborhood_exports()
    (NEIGHBORHOODS_DIR / "index.html").write_text(render_neighborhoods_html(groups))

    for group in groups:
        target_dir = NEIGHBORHOODS_DIR / group["slug"]
        target_dir.mkdir(parents=True, exist_ok=True)
        (target_dir / "index.html").write_text(render_neighborhood_detail_html(group))

    groups_by_slug = {group["slug"]: group for group in groups}
    for alias_slug, canonical_slug in NEIGHBORHOOD_ROUTE_ALIASES.items():
        canonical_group = groups_by_slug.get(canonical_slug)
        if canonical_group is None:
            continue
        alias_dir = NEIGHBORHOODS_DIR / alias_slug
        alias_dir.mkdir(parents=True, exist_ok=True)
        (alias_dir / "index.html").write_text(render_neighborhood_detail_html(canonical_group))

    print(f"Exported {len(groups)} neighborhood detail pages to {NEIGHBORHOODS_DIR}")


if __name__ == "__main__":
    main()
