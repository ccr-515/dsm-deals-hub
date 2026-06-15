# 2026-06-14 - Dynamic deployment prep

- Project: DSM Deals Hub
- Phase: Dynamic deployment prep
- Task: Vercel FastAPI entrypoint + DATABASE_URL bootstrap
- Status: Ready for local review of the dynamic Vercel path. Not deployed.
- What changed: Added api/index.py, vercel.json rewrites, DATABASE_URL normalization with SQLite fallback, Postgres NullPool, and a DB-first Today query path.
- Verification: python -m compileall app api scripts passed. Direct FastAPI smoke checks passed. scripts/qa_public_site.py still flags stale favicon links in exported docs.
- Next: Set DATABASE_URL in Vercel and preview the FastAPI deployment.

# 2026-06-15 - Preview deployment recovery

- Project: DSM Deals Hub
- Phase: Preview deployment recovery
- Task: Rotate Supabase DB password and repair Vercel DATABASE_URL
- Status: Blocked at preview validation. Env wiring and local DB/app checks pass, but live preview `/today` fails.
- Changed files: `docs/status/latest-handoff.md`, `docs/status/handoff-log.md`, `local-data/project-status.json`, `.env.preview.local`.
- External changes: Supabase DB password rotated for `ooyfemuayqwjwazxyclx`; Vercel `DATABASE_URL` entries replaced for Production+Preview, Preview branch `post-static-transition`, and Development.
- What works: Preview branch env pull is nonblank after recreating the branch row as non-sensitive; local DB `select 1` returned `1`; `app.main` import passed; Vercel preview build reached READY.
- What remains placeholder: Production promotion is intentionally held; Browser smoke check not verified.
- Broken or risky: Preview `/today` returned `FUNCTION_INVOCATION_FAILED`; Vercel CLI wraps URL values in dotenv quotes, so `.env.preview.local` was normalized for the literal check.
- Verification: `npx vercel@latest --yes` build passed; DB check passed; `npx vercel@latest curl /today --deployment https://dsm-deals-saonlwlwo-ccr-515s-projects.vercel.app` failed.
- Next: Inspect Vercel function logs for deployment `dpl_4QqGWTiUq8jngfDtVMno7ycsojTj`, fix the runtime error, redeploy preview only, and rerun `/today`.
- Suggested dashboard update: status `blocked`, blocked `true`, blocker reason `Preview deployment returns FUNCTION_INVOCATION_FAILED for /today`, confidence `63`, portfolio readiness `3`, money potential `4`, maintenance burden `3`.
