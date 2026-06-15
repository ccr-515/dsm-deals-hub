from pathlib import Path
import re

from fastapi import HTTPException
from fastapi.responses import HTMLResponse
from starlette.routing import Route

from app.main import app

BASE_DIR = Path(__file__).resolve().parents[1]
DOCS_DIR = BASE_DIR / "docs"

STATIC_HTML_ROUTES = {
    "/",
    "/days",
    "/days/{day_slug}",
    "/neighborhoods",
    "/neighborhoods/{slug}",
}

app.router.routes = [
    route
    for route in app.router.routes
    if not (
        isinstance(route, Route)
        and getattr(route, "path", None) in STATIC_HTML_ROUTES
    )
]

def _safe_slug(value: str) -> str:
    if not re.fullmatch(r"[a-z0-9-]+", value):
        raise HTTPException(status_code=404)
    return value

def _html_from_docs(*parts: str) -> HTMLResponse:
    path = DOCS_DIR.joinpath(*parts).resolve()

    if DOCS_DIR.resolve() not in path.parents and path != DOCS_DIR.resolve():
        raise HTTPException(status_code=404)

    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404)

    return HTMLResponse(path.read_text(encoding="utf-8"))

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def static_homepage():
    return _html_from_docs("index.html")

@app.get("/days", response_class=HTMLResponse, include_in_schema=False)
def static_days():
    return _html_from_docs("days", "index.html")

@app.get("/days/{day_slug}", response_class=HTMLResponse, include_in_schema=False)
def static_day_detail(day_slug: str):
    slug = _safe_slug(day_slug)
    return _html_from_docs("days", slug, "index.html")

@app.get("/neighborhoods", response_class=HTMLResponse, include_in_schema=False)
def static_neighborhoods():
    return _html_from_docs("neighborhoods", "index.html")

@app.get("/neighborhoods/{slug}", response_class=HTMLResponse, include_in_schema=False)
def static_neighborhood_detail(slug: str):
    safe = _safe_slug(slug)
    return _html_from_docs("neighborhoods", safe, "index.html")
