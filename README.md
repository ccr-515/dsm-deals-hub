# DSM Deals Hub

DSM Deals Hub is a FastAPI application for discovering curated Des Moines restaurant deals and operating a human-reviewed deal intake workflow.

## Current Product

- Public Homepage, Today, Days, and Neighborhoods views
- Supabase/Postgres-backed live deals and venues
- Password/cookie or `X-Admin-Key` admin authentication
- Rules-based deal-post parsing with structured proposals
- Human review, edit, approve, verify, freeze, and archive workflows
- Raw source retention and deal change audit log
- Weekly deal recheck and stale archive lifecycle
- Embedded Google Form for venue submissions

No paid LLM API is used. Production uses `LLM_PROVIDER=rules`.

## Local Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

export ADMIN_KEY="choose-a-local-admin-key"
export LLM_PROVIDER="rules"
uvicorn app.main:app --reload
```

Without `DATABASE_URL`, local development uses `sqlite:///./dsm_deals.db`.

## Required Production Environment

- `DATABASE_URL`: Supabase/Postgres connection string
- `ADMIN_KEY`: shared admin password/header value
- `LLM_PROVIDER=rules`

Optional freshness settings:

- `DEAL_RECHECK_DAYS=30`
- `DEAL_ARCHIVE_DAYS=45`
- `CORS_ALLOWED_ORIGINS`: comma-separated allowed browser origins

## Database Migrations

Postgres migrations are explicit and additive. They do not run during normal serverless startup.

```bash
DATABASE_URL="postgresql://..." python scripts/migrate.py
```

Local SQLite initializes and migrates automatically.

## Verification

```bash
python -m py_compile app/database.py app/main.py app/models.py app/schemas.py app/migrations.py app/weekly_master_content.py scripts/migrate.py scripts/qa_public_site.py
python -m unittest discover -s tests -v
python scripts/qa_public_site.py
```

## Admin Access

Browser: `/admin/login`

Header auth:

```bash
curl -H "X-Admin-Key: $ADMIN_KEY" http://127.0.0.1:8000/admin/deals
```

Legacy owner, venue, and deal creation APIs require admin authentication. Public deal submissions use the embedded venue form and enter the human review workflow.

## Deal Freshness

Weekly live deals begin as `verified`.

- After `DEAL_RECHECK_DAYS`, they become `needs_recheck` and remain visible during the grace period.
- After `DEAL_ARCHIVE_DAYS`, they are automatically archived unless verified again.
- Admins can use the `Needs recheck` filter and `Verify` action in `/admin/deals`.

Deals are never hard-deleted by the admin workflow.
