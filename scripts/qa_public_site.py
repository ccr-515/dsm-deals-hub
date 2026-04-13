from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from html.parser import HTMLParser
import os
from pathlib import Path
import re
import sys
from zoneinfo import ZoneInfo


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

os.environ.pop("DSM_DEALS_SITE_BASE_PATH", None)

from fastapi.testclient import TestClient

from app.main import app
from app.weekly_master_content import load_weekly_master_deals, neighborhood_groups


DOCS_ROOT = PROJECT_ROOT / "docs"
EXPECTED_STYLESHEET = "/static/styles.css"
EXPECTED_FAVICON = "/static/favicon.svg"
DAY_SLUGS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
VISIBLE_UTILITY_LABEL_RE = re.compile(r">(?:\s*)(Directions|Call)(?:\s*)</a>")
PLAIN_UTILITY_CLASS_RE = re.compile(r'class="deal-utility-action(?![^"]*deal-utility-action-icononly)[^"]*"')


@dataclass
class HtmlAudit:
    hrefs: list[str]
    srcs: list[str]
    stylesheet_links: list[str]
    favicon_links: list[str]


class ExportParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []
        self.srcs: list[str] = []
        self.stylesheet_links: list[str] = []
        self.favicon_links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = dict(attrs)
        if tag == "a" and attr_map.get("href"):
            self.hrefs.append(attr_map["href"])
        if tag in {"img", "script", "source"} and attr_map.get("src"):
            self.srcs.append(attr_map["src"])
        if tag == "link" and attr_map.get("href"):
            rel = attr_map.get("rel", "")
            href = attr_map["href"]
            if rel == "stylesheet":
                self.stylesheet_links.append(href)
            if rel == "icon":
                self.favicon_links.append(href)


def parse_html(path: Path) -> HtmlAudit:
    parser = ExportParser()
    parser.feed(path.read_text())
    return HtmlAudit(
        hrefs=parser.hrefs,
        srcs=parser.srcs,
        stylesheet_links=parser.stylesheet_links,
        favicon_links=parser.favicon_links,
    )


def export_target_exists(href: str) -> bool:
    if href == "/":
        return (DOCS_ROOT / "index.html").exists()
    if not href.startswith("/"):
        return False
    relative = href.lstrip("/")
    target = DOCS_ROOT / relative
    if href.endswith("/"):
        target = target / "index.html"
    return target.exists()


def audit_exported_docs() -> list[str]:
    failures: list[str] = []
    html_files = sorted(DOCS_ROOT.rglob("*.html"))

    for page in html_files:
        raw_html = page.read_text()
        audit = parse_html(page)
        if audit.stylesheet_links != [EXPECTED_STYLESHEET]:
            failures.append(f"{page}: stylesheet links were {audit.stylesheet_links!r}")
        if audit.favicon_links != [EXPECTED_FAVICON]:
            failures.append(f"{page}: favicon links were {audit.favicon_links!r}")

        for href in audit.hrefs:
            if href.startswith(("http://", "https://", "mailto:", "tel:", "#")):
                continue
            if href.startswith("/dsm-deals-hub/"):
                failures.append(f"{page}: deprecated GitHub Pages href {href!r}")
                continue
            if not href.startswith("/") and href != "/":
                failures.append(f"{page}: non-root-relative href {href!r}")
                continue
            if not export_target_exists(href):
                failures.append(f"{page}: missing exported target for {href!r}")

        for src in audit.srcs:
            if src.startswith(("http://", "https://", "data:")):
                continue
            if src.startswith("/dsm-deals-hub/"):
                failures.append(f"{page}: deprecated GitHub Pages src {src!r}")
                continue
            if not src.startswith("/"):
                failures.append(f"{page}: non-root-relative src {src!r}")

        if VISIBLE_UTILITY_LABEL_RE.search(raw_html):
            failures.append(f"{page}: visible text utility labels still rendered")

        if PLAIN_UTILITY_CLASS_RE.search(raw_html):
            failures.append(f"{page}: utility action missing icon-only class")

        if 'class="deal-card-actions"' in raw_html:
            if 'class="deal-utility-action deal-utility-action-icononly"' not in raw_html:
                failures.append(f"{page}: utility row missing icon-only actions")
            if 'aria-label="Get directions to ' not in raw_html and 'aria-label="Call ' not in raw_html:
                failures.append(f"{page}: utility row missing accessible icon labels")

    return failures


def audit_app_routes() -> list[str]:
    failures: list[str] = []
    client = TestClient(app)
    primary_routes = ["/", "/today", "/neighborhoods", "/days", "/for-venues", "/health"]
    for route in primary_routes:
        response = client.get(route)
        if response.status_code != 200:
            failures.append(f"Primary route {route} returned {response.status_code}")

    for slug in DAY_SLUGS:
        response = client.get(f"/days/{slug}")
        if response.status_code != 200:
            failures.append(f"Day route /days/{slug} returned {response.status_code}")

    reference = datetime.now(ZoneInfo("America/Chicago"))
    for group in neighborhood_groups(reference):
        slug = group["slug"]
        response = client.get(f"/neighborhoods/{slug}")
        if response.status_code != 200:
            failures.append(f"Neighborhood route /neighborhoods/{slug} returned {response.status_code}")

    for asset in ["/static/styles.css", "/static/favicon.svg"]:
        response = client.get(asset)
        if response.status_code != 200:
            failures.append(f"Static asset {asset} returned {response.status_code}")

    return failures


def main() -> int:
    failures = []
    failures.extend(audit_app_routes())
    failures.extend(audit_exported_docs())

    print("Primary routes audited: 6")
    print(f"Day detail routes audited: {len(DAY_SLUGS)}")
    print(f"Neighborhood detail routes audited: {len(neighborhood_groups(datetime.now(ZoneInfo('America/Chicago'))))}")
    print(f"Exported HTML files audited: {len(list(DOCS_ROOT.rglob('*.html')))}")

    if failures:
        print("\nFAILURES")
        for failure in failures:
            print(f"- {failure}")
        return 1

    deals = load_weekly_master_deals()
    print(f"Weekly master deals loaded: {len(deals)}")
    print("All public-route, export-link, and asset checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
