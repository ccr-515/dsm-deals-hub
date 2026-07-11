import json
import logging

from sqlalchemy import inspect, text


logger = logging.getLogger(__name__)


def run_migrations(engine) -> None:
    dialect = engine.url.get_backend_name()
    if dialect == "sqlite":
        _run_sqlite_migrations(engine)
    elif dialect == "postgresql":
        _run_postgres_migrations(engine)


def schema_report(engine) -> dict:
    dialect = engine.url.get_backend_name()
    tables = [
        "business_owners",
        "venues",
        "deals",
        "deal_intake_submissions",
        "deal_change_log",
        "events_metrics",
    ]
    if dialect == "postgresql":
        return _postgres_schema_report(engine, tables)
    return _sqlite_schema_report(engine, tables)


def _run_sqlite_migrations(engine) -> None:
    inspector = inspect(engine)
    tables = set(inspector.get_table_names())

    if "deals" in tables:
        _ensure_sqlite_deal_private_columns(engine)

    if "venues" in tables and _sqlite_venue_owner_is_required(engine):
        _rebuild_sqlite_venues_for_optional_owner(engine)


def _sqlite_table_info(engine, table_name: str) -> dict[str, dict[str, int]]:
    with engine.connect() as conn:
        rows = conn.exec_driver_sql(f"PRAGMA table_info({table_name})").fetchall()
    return {
        row[1]: {
            "notnull": row[3],
        }
        for row in rows
    }


def _ensure_sqlite_deal_private_columns(engine) -> None:
    columns = _sqlite_table_info(engine, "deals")
    statements = []

    if "source_text" not in columns:
        statements.append("ALTER TABLE deals ADD COLUMN source_text TEXT")
    if "source_posted_at" not in columns:
        statements.append("ALTER TABLE deals ADD COLUMN source_posted_at DATETIME")
    if "notes_private" not in columns:
        statements.append("ALTER TABLE deals ADD COLUMN notes_private TEXT")
    if "verification_status" not in columns:
        statements.append("ALTER TABLE deals ADD COLUMN verification_status VARCHAR DEFAULT 'verified'")
    if "last_verified_at" not in columns:
        statements.append("ALTER TABLE deals ADD COLUMN last_verified_at DATETIME")
    if "valid_until" not in columns:
        statements.append("ALTER TABLE deals ADD COLUMN valid_until DATETIME")
    if "verification_notes" not in columns:
        statements.append("ALTER TABLE deals ADD COLUMN verification_notes TEXT")

    if not statements:
        return

    with engine.begin() as conn:
        for statement in statements:
            conn.exec_driver_sql(statement)
        conn.exec_driver_sql(
            "UPDATE deals SET verification_status = 'verified' "
            "WHERE verification_status IS NULL OR trim(verification_status) = ''"
        )
        conn.exec_driver_sql(
            "UPDATE deals SET last_verified_at = CURRENT_TIMESTAMP "
            "WHERE status = 'live' AND last_verified_at IS NULL"
        )


def _sqlite_venue_owner_is_required(engine) -> bool:
    columns = _sqlite_table_info(engine, "venues")
    owner_id = columns.get("owner_id")
    return bool(owner_id and owner_id["notnull"])


def _rebuild_sqlite_venues_for_optional_owner(engine) -> None:
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
                hours_json JSON,
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


def _run_postgres_migrations(engine) -> None:
    with engine.begin() as conn:
        _ensure_postgres_business_owners(conn)
        _ensure_postgres_venues_columns(conn)
        _ensure_postgres_deals_columns(conn)
        _ensure_postgres_intake_tables(conn)
        _ensure_postgres_events_metrics(conn)
        _ensure_postgres_indexes(conn)
        _ensure_postgres_foreign_keys(conn)


def _ensure_postgres_business_owners(conn) -> None:
    conn.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS business_owners (
                id SERIAL PRIMARY KEY,
                name VARCHAR NOT NULL,
                email VARCHAR NOT NULL,
                phone VARCHAR,
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now()
            )
            """
        )
    )
    conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS ix_business_owners_email ON business_owners (email)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS ix_business_owners_id ON business_owners (id)"))


def _ensure_postgres_venues_columns(conn) -> None:
    _postgres_add_column(conn, "venues", "owner_id", "INTEGER")
    _postgres_add_column(conn, "venues", "name", "VARCHAR")
    _postgres_add_column(conn, "venues", "slug", "VARCHAR")
    _postgres_add_column(conn, "venues", "address", "VARCHAR")
    _postgres_add_column(conn, "venues", "neighborhood", "VARCHAR")
    _postgres_add_column(conn, "venues", "lat", "DOUBLE PRECISION")
    _postgres_add_column(conn, "venues", "lng", "DOUBLE PRECISION")
    _postgres_add_column(conn, "venues", "phone", "VARCHAR")
    _postgres_add_column(conn, "venues", "website", "VARCHAR")
    _postgres_add_column(conn, "venues", "hours_json", "JSONB")
    _postgres_add_column(conn, "venues", "description", "TEXT")
    _postgres_add_column(conn, "venues", "created_at", "TIMESTAMP WITHOUT TIME ZONE DEFAULT now()")
    _postgres_add_column(conn, "venues", "updated_at", "TIMESTAMP WITHOUT TIME ZONE DEFAULT now()")
    _ensure_postgres_hours_json_jsonb(conn)


def _ensure_postgres_deals_columns(conn) -> None:
    _postgres_add_column(conn, "deals", "source_type", "VARCHAR DEFAULT 'admin'")
    _postgres_add_column(conn, "deals", "source_url", "VARCHAR")
    _postgres_add_column(conn, "deals", "source_text", "TEXT")
    _postgres_add_column(conn, "deals", "source_posted_at", "TIMESTAMP WITHOUT TIME ZONE")
    _postgres_add_column(conn, "deals", "notes_private", "TEXT")
    _postgres_add_column(conn, "deals", "verification_status", "VARCHAR DEFAULT 'verified'")
    _postgres_add_column(conn, "deals", "last_verified_at", "TIMESTAMP WITHOUT TIME ZONE")
    _postgres_add_column(conn, "deals", "valid_until", "TIMESTAMP WITHOUT TIME ZONE")
    _postgres_add_column(conn, "deals", "verification_notes", "TEXT")
    _postgres_add_column(conn, "deals", "freeze_minutes", "INTEGER DEFAULT 30")
    _postgres_add_column(conn, "deals", "created_at", "TIMESTAMP WITHOUT TIME ZONE DEFAULT now()")
    _postgres_add_column(conn, "deals", "updated_at", "TIMESTAMP WITHOUT TIME ZONE DEFAULT now()")
    conn.execute(
        text(
            "UPDATE deals SET verification_status = 'verified' "
            "WHERE verification_status IS NULL OR btrim(verification_status) = ''"
        )
    )
    conn.execute(
        text(
            "UPDATE deals SET last_verified_at = now() "
            "WHERE status = 'live' AND last_verified_at IS NULL"
        )
    )


def _ensure_postgres_intake_tables(conn) -> None:
    conn.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS deal_intake_submissions (
                id SERIAL PRIMARY KEY,
                raw_text TEXT NOT NULL,
                source_url VARCHAR,
                source_platform VARCHAR,
                notes TEXT,
                parsed_json TEXT,
                status VARCHAR NOT NULL DEFAULT 'submitted',
                confidence DOUBLE PRECISION,
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
                reviewed_at TIMESTAMP WITHOUT TIME ZONE,
                applied_at TIMESTAMP WITHOUT TIME ZONE,
                error_message TEXT
            )
            """
        )
    )
    _postgres_add_column(conn, "deal_intake_submissions", "raw_text", "TEXT")
    _postgres_add_column(conn, "deal_intake_submissions", "source_url", "VARCHAR")
    _postgres_add_column(conn, "deal_intake_submissions", "source_platform", "VARCHAR")
    _postgres_add_column(conn, "deal_intake_submissions", "notes", "TEXT")
    _postgres_add_column(conn, "deal_intake_submissions", "parsed_json", "TEXT")
    _postgres_add_column(conn, "deal_intake_submissions", "status", "VARCHAR DEFAULT 'submitted'")
    _postgres_add_column(conn, "deal_intake_submissions", "confidence", "DOUBLE PRECISION")
    _postgres_add_column(conn, "deal_intake_submissions", "created_at", "TIMESTAMP WITHOUT TIME ZONE DEFAULT now()")
    _postgres_add_column(conn, "deal_intake_submissions", "reviewed_at", "TIMESTAMP WITHOUT TIME ZONE")
    _postgres_add_column(conn, "deal_intake_submissions", "applied_at", "TIMESTAMP WITHOUT TIME ZONE")
    _postgres_add_column(conn, "deal_intake_submissions", "error_message", "TEXT")

    conn.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS deal_change_log (
                id SERIAL PRIMARY KEY,
                action VARCHAR NOT NULL,
                deal_id INTEGER,
                venue_id INTEGER,
                before_json TEXT,
                after_json TEXT,
                source_text TEXT,
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now()
            )
            """
        )
    )
    _postgres_add_column(conn, "deal_change_log", "action", "VARCHAR")
    _postgres_add_column(conn, "deal_change_log", "deal_id", "INTEGER")
    _postgres_add_column(conn, "deal_change_log", "venue_id", "INTEGER")
    _postgres_add_column(conn, "deal_change_log", "before_json", "TEXT")
    _postgres_add_column(conn, "deal_change_log", "after_json", "TEXT")
    _postgres_add_column(conn, "deal_change_log", "source_text", "TEXT")
    _postgres_add_column(conn, "deal_change_log", "created_at", "TIMESTAMP WITHOUT TIME ZONE DEFAULT now()")


def _ensure_postgres_events_metrics(conn) -> None:
    conn.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS events_metrics (
                id SERIAL PRIMARY KEY,
                deal_id INTEGER NOT NULL,
                kind VARCHAR NOT NULL,
                ts TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
                ip_hash VARCHAR
            )
            """
        )
    )
    _postgres_add_column(conn, "events_metrics", "deal_id", "INTEGER")
    _postgres_add_column(conn, "events_metrics", "kind", "VARCHAR")
    _postgres_add_column(conn, "events_metrics", "ts", "TIMESTAMP WITHOUT TIME ZONE DEFAULT now()")
    _postgres_add_column(conn, "events_metrics", "ip_hash", "VARCHAR")


def _ensure_postgres_indexes(conn) -> None:
    conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS ix_venues_slug ON venues (slug)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS ix_venues_id ON venues (id)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS ix_deals_id ON deals (id)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS ix_deal_intake_submissions_id ON deal_intake_submissions (id)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS ix_deal_change_log_id ON deal_change_log (id)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS ix_deals_verification_status ON deals (verification_status)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS ix_deals_last_verified_at ON deals (last_verified_at)"))


def _ensure_postgres_foreign_keys(conn) -> None:
    _postgres_add_foreign_key(
        conn,
        "venues",
        "fk_venues_owner_id_business_owners",
        "owner_id",
        "business_owners",
        "id",
    )
    _postgres_add_foreign_key(conn, "deals", "fk_deals_venue_id_venues", "venue_id", "venues", "id")
    _postgres_add_foreign_key(conn, "deal_change_log", "fk_deal_change_log_deal_id_deals", "deal_id", "deals", "id")
    _postgres_add_foreign_key(conn, "deal_change_log", "fk_deal_change_log_venue_id_venues", "venue_id", "venues", "id")
    _postgres_add_foreign_key(conn, "events_metrics", "fk_events_metrics_deal_id_deals", "deal_id", "deals", "id")


def _postgres_add_column(conn, table_name: str, column_name: str, column_type: str) -> None:
    if not _postgres_table_exists(conn, table_name):
        return
    if column_name in _postgres_columns(conn, table_name):
        return
    conn.execute(text(f'ALTER TABLE "{table_name}" ADD COLUMN "{column_name}" {column_type}'))


def _postgres_columns(conn, table_name: str) -> dict[str, dict[str, str]]:
    rows = conn.execute(
        text(
            """
            SELECT column_name, data_type, udt_name, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = :table_name
            """
        ),
        {"table_name": table_name},
    ).mappings()
    return {row["column_name"]: dict(row) for row in rows}


def _postgres_table_exists(conn, table_name: str) -> bool:
    return bool(conn.execute(text("SELECT to_regclass(:table_name) IS NOT NULL"), {"table_name": f"public.{table_name}"}).scalar())


def _ensure_postgres_hours_json_jsonb(conn) -> None:
    columns = _postgres_columns(conn, "venues")
    hours_column = columns.get("hours_json")
    if not hours_column or hours_column.get("udt_name") == "jsonb":
        return

    if hours_column.get("data_type") not in {"text", "character varying", "json"}:
        logger.warning("Skipping venues.hours_json type migration from unsupported type %s.", hours_column.get("data_type"))
        return

    invalid_ids: list[int] = []
    if hours_column.get("data_type") in {"text", "character varying"}:
        rows = conn.execute(text("SELECT id, hours_json FROM venues WHERE hours_json IS NOT NULL")).mappings()
        for row in rows:
            value = row["hours_json"]
            if value is None or not str(value).strip():
                continue
            try:
                json.loads(str(value))
            except ValueError:
                invalid_ids.append(row["id"])
                if len(invalid_ids) >= 5:
                    break
        if invalid_ids:
            logger.warning(
                "Skipping venues.hours_json jsonb migration because non-JSON values exist. Example venue ids: %s",
                invalid_ids,
            )
            return

    if hours_column.get("data_type") == "json":
        using_clause = "hours_json::jsonb"
    else:
        using_clause = "CASE WHEN hours_json IS NULL OR btrim(hours_json) = '' THEN NULL ELSE hours_json::jsonb END"

    conn.execute(text(f"ALTER TABLE venues ALTER COLUMN hours_json TYPE jsonb USING {using_clause}"))


def _postgres_add_foreign_key(
    conn,
    table_name: str,
    constraint_name: str,
    column_name: str,
    referenced_table: str,
    referenced_column: str,
) -> None:
    if not _postgres_table_exists(conn, table_name) or not _postgres_table_exists(conn, referenced_table):
        return
    exists = conn.execute(
        text(
            """
            SELECT 1
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_schema = kcu.constraint_schema
             AND tc.constraint_name = kcu.constraint_name
             AND tc.table_name = kcu.table_name
            JOIN information_schema.constraint_column_usage ccu
              ON tc.constraint_schema = ccu.constraint_schema
             AND tc.constraint_name = ccu.constraint_name
            WHERE tc.table_schema = 'public'
              AND tc.table_name = :table_name
              AND tc.constraint_type = 'FOREIGN KEY'
              AND (
                tc.constraint_name = :constraint_name
                OR (
                  kcu.column_name = :column_name
                  AND ccu.table_name = :referenced_table
                  AND ccu.column_name = :referenced_column
                )
              )
            """
        ),
        {
            "table_name": table_name,
            "constraint_name": constraint_name,
            "column_name": column_name,
            "referenced_table": referenced_table,
            "referenced_column": referenced_column,
        },
    ).first()
    if exists:
        return

    invalid_count = conn.execute(
        text(
            f"""
            SELECT count(*)
            FROM "{table_name}" child
            LEFT JOIN "{referenced_table}" parent ON child."{column_name}" = parent."{referenced_column}"
            WHERE child."{column_name}" IS NOT NULL
              AND parent."{referenced_column}" IS NULL
            """
        )
    ).scalar()
    if invalid_count:
        logger.warning(
            "Skipping %s because %s rows have no matching parent.",
            constraint_name,
            invalid_count,
        )
        return

    conn.execute(
        text(
            f"""
            ALTER TABLE "{table_name}"
            ADD CONSTRAINT "{constraint_name}"
            FOREIGN KEY ("{column_name}") REFERENCES "{referenced_table}" ("{referenced_column}")
            NOT VALID
            """
        )
    )
    conn.execute(text(f'ALTER TABLE "{table_name}" VALIDATE CONSTRAINT "{constraint_name}"'))


def _postgres_schema_report(engine, tables: list[str]) -> dict:
    report = {"dialect": "postgresql", "tables": {}}
    with engine.connect() as conn:
        for table_name in tables:
            exists = _postgres_table_exists(conn, table_name)
            table_report = {"exists": exists, "columns": []}
            if exists:
                rows = conn.execute(
                    text(
                        """
                        SELECT column_name, data_type, udt_name, is_nullable
                        FROM information_schema.columns
                        WHERE table_schema = 'public' AND table_name = :table_name
                        ORDER BY ordinal_position
                        """
                    ),
                    {"table_name": table_name},
                ).mappings()
                table_report["columns"] = [dict(row) for row in rows]
            report["tables"][table_name] = table_report
    return report


def _sqlite_schema_report(engine, tables: list[str]) -> dict:
    report = {"dialect": "sqlite", "tables": {}}
    with engine.connect() as conn:
        existing_tables = set(inspect(engine).get_table_names())
        for table_name in tables:
            table_report = {"exists": table_name in existing_tables, "columns": []}
            if table_name in existing_tables:
                rows = conn.exec_driver_sql(f"PRAGMA table_info({table_name})").fetchall()
                table_report["columns"] = [
                    {
                        "column_name": row[1],
                        "data_type": row[2],
                        "is_nullable": "NO" if row[3] else "YES",
                    }
                    for row in rows
                ]
            report["tables"][table_name] = table_report
    return report
