from __future__ import annotations

from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.database import Base, engine
from app.migrations import run_migrations, schema_report


def main() -> int:
    Base.metadata.create_all(bind=engine)
    run_migrations(engine)
    report = schema_report(engine)
    missing = [name for name, table in report["tables"].items() if not table["exists"]]
    if missing:
        raise RuntimeError(f"Migration completed with missing tables: {', '.join(missing)}")
    print(f"Migration complete for {report['dialect']}: {len(report['tables'])} tables verified.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
