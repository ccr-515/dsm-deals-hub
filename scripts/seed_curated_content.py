from datetime import datetime
from pathlib import Path
import sqlite3
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "dsm_deals.db"
MASTER_SOURCE_URL = "weekly-master://uploaded-files"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.weekly_master_content import load_weekly_master_deals


def timestamp_now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")


def fallback_address(neighborhood: str) -> str:
    place = neighborhood or "Des Moines Metro"
    return f"{place} / address pending manual verification"


def upsert_venue(
    conn: sqlite3.Connection,
    *,
    slug: str,
    name: str,
    neighborhood: str,
    now: str,
) -> int:
    row = conn.execute("SELECT id, address FROM venues WHERE slug = ?", (slug,)).fetchone()
    address = row[1] if row and row[1] and "pending manual verification" not in row[1].lower() else fallback_address(neighborhood)

    if row:
        conn.execute(
            """
            UPDATE venues
            SET owner_id = NULL,
                name = ?,
                address = ?,
                neighborhood = ?,
                lat = NULL,
                lng = NULL,
                phone = NULL,
                website = NULL,
                hours_json = NULL,
                description = NULL,
                updated_at = ?
            WHERE id = ?
            """,
            (name, address, neighborhood, now, row[0]),
        )
        return row[0]

    cursor = conn.execute(
        """
        INSERT INTO venues (
            owner_id, name, slug, address, neighborhood, lat, lng, phone, website,
            hours_json, description, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (None, name, slug, address, neighborhood, None, None, None, None, None, None, now, now),
    )
    return cursor.lastrowid


def main() -> None:
    deals = load_weekly_master_deals()
    conn = sqlite3.connect(DB_PATH)
    try:
        now = timestamp_now()
        conn.execute("PRAGMA foreign_keys = ON")

        conn.execute("DELETE FROM deals WHERE source_url = ?", (MASTER_SOURCE_URL,))
        conn.execute("DELETE FROM deals WHERE source_url LIKE 'seed://%'")
        conn.execute("DELETE FROM deals WHERE title LIKE 'Tonight Special%'")
        conn.execute("DELETE FROM deals WHERE title LIKE 'Weekly Test%'")
        conn.execute("DELETE FROM deals WHERE title LIKE 'Flash Deal%'")

        venue_ids: dict[str, int] = {}
        for deal in deals:
            venue = deal.venue
            if venue.slug not in venue_ids:
                venue_ids[venue.slug] = upsert_venue(
                    conn,
                    slug=venue.slug,
                    name=venue.name,
                    neighborhood=venue.neighborhood,
                    now=now,
                )

            conn.execute(
                """
                INSERT INTO deals (
                    venue_id, title, short_description, type, weekday_pattern, start_time, end_time,
                    start_at, end_at, age_21_plus, menu_link, image_url, sponsored, status,
                    source_type, source_url, source_text, source_posted_at, notes_private,
                    freeze_minutes, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    venue_ids[venue.slug],
                    deal.title,
                    deal.short_description,
                    "weekly",
                    deal.weekday_pattern,
                    deal.start_time,
                    deal.end_time,
                    None,
                    None,
                    0,
                    None,
                    None,
                    0,
                    "live",
                    "admin",
                    MASTER_SOURCE_URL,
                    f"{venue.name} | {deal.weekday_pattern} | {deal.title}",
                    now,
                    deal.notes_private,
                    30,
                    now,
                    now,
                ),
            )

        conn.commit()
        print(f"Seeded {len(venue_ids)} venues and {len(deals)} weekly master deals.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
