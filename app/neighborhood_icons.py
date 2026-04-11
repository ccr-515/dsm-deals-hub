from __future__ import annotations

from pathlib import Path
import re
import shutil
import unicodedata


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ICON_DIR = PROJECT_ROOT / "neighborhood icons"
REFERENCE_ICON_DIR = PROJECT_ROOT / "neighborhood images" / "neighborhood icons"
APP_STATIC_ICON_DIR = PROJECT_ROOT / "app" / "static" / "neighborhood-icons"
DOCS_STATIC_ICON_DIR = PROJECT_ROOT / "docs" / "static" / "neighborhood-icons"
SUPPORTED_ICON_EXTENSIONS = (".svg", ".webp", ".png", ".jpg", ".jpeg")
EXTENSION_PRIORITY = {suffix: index for index, suffix in enumerate(SUPPORTED_ICON_EXTENSIONS)}


def normalize_icon_key(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
    collapsed = re.sub(r"[^a-zA-Z0-9]+", "-", ascii_value.strip().lower())
    return collapsed.strip("-")


def _iter_source_icons() -> list[Path]:
    if not SOURCE_ICON_DIR.exists():
        return []
    return sorted(
        path
        for path in SOURCE_ICON_DIR.rglob("*")
        if path.is_file() and path.suffix.lower() in SUPPORTED_ICON_EXTENSIONS
    )


def available_neighborhood_icon_sources() -> dict[str, Path]:
    selected: dict[str, Path] = {}
    for path in _iter_source_icons():
        key = normalize_icon_key(path.stem)
        if not key:
            continue
        existing = selected.get(key)
        if existing is None:
            selected[key] = path
            continue

        existing_rank = EXTENSION_PRIORITY.get(existing.suffix.lower(), len(SUPPORTED_ICON_EXTENSIONS))
        current_rank = EXTENSION_PRIORITY.get(path.suffix.lower(), len(SUPPORTED_ICON_EXTENSIONS))
        if current_rank < existing_rank:
            selected[key] = path
    return selected


def neighborhood_icon_filename(name_or_slug: str) -> str | None:
    key = normalize_icon_key(name_or_slug)
    if not key:
        return None
    source = available_neighborhood_icon_sources().get(key)
    if source is None:
        return None
    return f"{key}{source.suffix.lower()}"


def neighborhood_icon_static_path(name_or_slug: str) -> str | None:
    filename = neighborhood_icon_filename(name_or_slug)
    if filename is None:
        return None
    return f"neighborhood-icons/{filename}"


def sync_neighborhood_icon_assets() -> list[str]:
    filenames: list[str] = []
    sources = available_neighborhood_icon_sources()
    if not sources:
        return filenames

    APP_STATIC_ICON_DIR.mkdir(parents=True, exist_ok=True)
    DOCS_STATIC_ICON_DIR.mkdir(parents=True, exist_ok=True)

    for key, source in sources.items():
        filename = f"{key}{source.suffix.lower()}"
        filenames.append(filename)
        for target_dir in (APP_STATIC_ICON_DIR, DOCS_STATIC_ICON_DIR):
            shutil.copy2(source, target_dir / filename)

    return filenames
