# 2026-06-14 - Dynamic deployment prep

- Project: DSM Deals Hub
- Phase: Dynamic deployment prep
- Task: Vercel FastAPI entrypoint + DATABASE_URL bootstrap
- Status: Ready for local review of the dynamic Vercel path. Not deployed.
- What changed: Added api/index.py, vercel.json rewrites, DATABASE_URL normalization with SQLite fallback, Postgres NullPool, and a DB-first Today query path.
- Verification: python -m compileall app api scripts passed. Direct FastAPI smoke checks passed. scripts/qa_public_site.py still flags stale favicon links in exported docs.
- Next: Set DATABASE_URL in Vercel and preview the FastAPI deployment.
