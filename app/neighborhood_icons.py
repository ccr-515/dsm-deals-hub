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
ICON_SOURCE_DIRECTORIES = (
    SOURCE_ICON_DIR,
    REFERENCE_ICON_DIR,
)
ICON_KEY_ALIASES = {
    "des-moines-area": "des-moines",
    "des-moines-metro": "des-moines",
    "court-district": "downtown",
    "court-avenue": "downtown",
    "western-gateway": "downtown",
    "downtown-des-moines": "downtown",
    "prairie-meadows": "altoona",
}


def normalize_icon_key(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
    collapsed = re.sub(r"[^a-zA-Z0-9]+", "-", ascii_value.strip().lower())
    return collapsed.strip("-")


def _iter_source_icons() -> list[tuple[int, Path]]:
    icon_paths: list[tuple[int, Path]] = []
    for directory_rank, directory in enumerate(ICON_SOURCE_DIRECTORIES):
        if not directory.exists():
            continue
        icon_paths.extend(
            (directory_rank, path)
            for path in sorted(directory.rglob("*"))
            if path.is_file()
            and not path.name.startswith(".")
            and path.suffix.lower() in SUPPORTED_ICON_EXTENSIONS
        )
    return icon_paths


def _icon_key_candidates(name_or_slug: str) -> list[str]:
    raw_key = normalize_icon_key(name_or_slug)
    if not raw_key:
        return []

    canonical_key = ICON_KEY_ALIASES.get(raw_key, raw_key)
    candidates: list[str] = []
    for key in (raw_key, canonical_key):
        if key and key not in candidates:
            candidates.append(key)
    for alias, canonical in ICON_KEY_ALIASES.items():
        if canonical == canonical_key and alias not in candidates:
            candidates.append(alias)
    return candidates


def available_neighborhood_icon_sources() -> dict[str, Path]:
    selected: dict[str, tuple[int, int, Path]] = {}
    for directory_rank, path in _iter_source_icons():
        key = normalize_icon_key(path.stem)
        if not key:
            continue
        current_rank = (
            directory_rank,
            EXTENSION_PRIORITY.get(path.suffix.lower(), len(SUPPORTED_ICON_EXTENSIONS)),
        )
        existing = selected.get(key)
        if existing is None or current_rank < existing[:2]:
            selected[key] = (*current_rank, path)
    return {key: path for key, (_, _, path) in selected.items()}


def _synced_filename_for_key(key: str, sources: dict[str, Path]) -> str | None:
    source = sources.get(key)
    if source is None:
        return None
    return f"{key}{source.suffix.lower()}"


def neighborhood_icon_filename(name_or_slug: str) -> str | None:
    sources = available_neighborhood_icon_sources()
    for key in _icon_key_candidates(name_or_slug):
        filename = _synced_filename_for_key(key, sources)
        if filename:
            return filename
    return None


def neighborhood_icon_static_path(name_or_slug: str) -> str | None:
    filename = neighborhood_icon_filename(name_or_slug)
    if filename is None:
        return None
    return f"neighborhood-icons/{filename}"


def _remove_stale_synced_icons(target_dir: Path, keep_filenames: set[str]) -> None:
    if not target_dir.exists():
        return
    for path in target_dir.iterdir():
        if not path.is_file() or path.name.startswith("."):
            continue
        if path.suffix.lower() not in SUPPORTED_ICON_EXTENSIONS:
            continue
        if path.name not in keep_filenames:
            path.unlink()


def sync_neighborhood_icon_assets() -> list[str]:
    sources = available_neighborhood_icon_sources()
    filenames = sorted(
        filename
        for key in sources
        if (filename := _synced_filename_for_key(key, sources))
    )
    keep_filenames = set(filenames)

    APP_STATIC_ICON_DIR.mkdir(parents=True, exist_ok=True)
    DOCS_STATIC_ICON_DIR.mkdir(parents=True, exist_ok=True)

    for target_dir in (APP_STATIC_ICON_DIR, DOCS_STATIC_ICON_DIR):
        _remove_stale_synced_icons(target_dir, keep_filenames)

    for key, source in sources.items():
        filename = _synced_filename_for_key(key, sources)
        if filename is None:
            continue
        for target_dir in (APP_STATIC_ICON_DIR, DOCS_STATIC_ICON_DIR):
            shutil.copy2(source, target_dir / filename)

    return filenames
