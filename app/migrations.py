from sqlalchemy import inspect


def run_migrations(engine) -> None:
    inspector = inspect(engine)
    tables = set(inspector.get_table_names())

    if "deals" in tables:
        _ensure_deal_private_columns(engine)

    if "venues" in tables and _venue_owner_is_required(engine):
        _rebuild_venues_for_optional_owner(engine)


def _table_info(engine, table_name: str) -> dict[str, dict[str, int]]:
    with engine.connect() as conn:
        rows = conn.exec_driver_sql(f"PRAGMA table_info({table_name})").fetchall()
    return {
        row[1]: {
            "notnull": row[3],
        }
        for row in rows
    }


def _ensure_deal_private_columns(engine) -> None:
    columns = _table_info(engine, "deals")
    statements = []

    if "source_text" not in columns:
        statements.append("ALTER TABLE deals ADD COLUMN source_text TEXT")
    if "source_posted_at" not in columns:
        statements.append("ALTER TABLE deals ADD COLUMN source_posted_at DATETIME")
    if "notes_private" not in columns:
        statements.append("ALTER TABLE deals ADD COLUMN notes_private TEXT")

    if not statements:
        return

    with engine.begin() as conn:
        for statement in statements:
            conn.exec_driver_sql(statement)


def _venue_owner_is_required(engine) -> bool:
    columns = _table_info(engine, "venues")
    owner_id = columns.get("owner_id")
    return bool(owner_id and owner_id["notnull"])


def _rebuild_venues_for_optional_owner(engine) -> None:
    conn = engine.raw_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("PRAGMA foreign_keys=OFF")
        cursor.execute("DROP TABLE IF EXISTS venues__new")
        cursor.execute(
            """
            CREATE TABLE venues__new (
                id INTEGER NOT NULL PRIMARY KEY,
                owner_id INTEGER,
                name VARCHAR NOT NULL,
                slug VARCHAR NOT NULL,
                address VARCHAR NOT NULL,
                neighborhood VARCHAR,
                lat FLOAT,
                lng FLOAT,
                phone VARCHAR,
                website VARCHAR,
                hours_json TEXT,
                description TEXT,
                created_at DATETIME,
                updated_at DATETIME,
                FOREIGN KEY(owner_id) REFERENCES business_owners (id)
            )
            """
        )
        cursor.execute(
            """
            INSERT INTO venues__new (
                id, owner_id, name, slug, address, neighborhood, lat, lng,
                phone, website, hours_json, description, created_at, updated_at
            )
            SELECT
                id, owner_id, name, slug, address, neighborhood, lat, lng,
                phone, website, hours_json, description, created_at, updated_at
            FROM venues
            """
        )
        cursor.execute("DROP TABLE venues")
        cursor.execute("ALTER TABLE venues__new RENAME TO venues")
        cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS ix_venues_slug ON venues (slug)")
        cursor.execute("CREATE INDEX IF NOT EXISTS ix_venues_id ON venues (id)")
        conn.commit()
    finally:
        cursor.execute("PRAGMA foreign_keys=ON")
        conn.commit()
        cursor.close()
        conn.close()
