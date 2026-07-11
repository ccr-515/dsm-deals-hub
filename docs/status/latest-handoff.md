# 2026-07-11 - Operations Truth Production Release

## Human Summary

Project name: DSM Deals Hub

Phase: Production stabilization

Task name: Deploy secure publishing, unified public data, and deal freshness

Date: 2026-07-11

Branch: `codex/dsm-deals-final-preprod`

Live URL: `https://www.dsmdealshub.online`

Verified preview: `https://dsm-deals-1vtm9r3td-ccr-515s-projects.vercel.app`

Production deployment: `https://dsm-deals-oy5nzra15-ccr-515s-projects.vercel.app`

Production deployment ID: `dpl_HpxpKMpE55LF4jrqrmAmL7Xb1PVr`

Current project status: The Operations Truth release is live and verified. Legacy writes require admin auth, all public deal views use the operational database, archive removes a deal from every public route family, and weekly deals now have a verification lifecycle.

## Changed Files

- `MVP_STATUS.md`
- `README.md`
- `app/config.py`
- `app/main.py`
- `app/migrations.py`
- `app/models.py`
- `app/schemas.py`
- `docs/status/latest-handoff.md`
- `docs/status/handoff-log.md`
- `local-data/project-status.json`
- `scripts/migrate.py`
- `scripts/test_api.sh`
- `tests/test_operations_truth.py`
- Removed tracked `scripts/__pycache__/seed_curated_content.cpython-313.pyc`

## What Works

- Owner, venue, weekly-deal, and last-minute creation APIs now require the shared admin guard.
- Production API docs and OpenAPI are disabled.
- Supabase `deals` is the public source for Homepage, Today, Days, and Neighborhoods; the weekly master is empty-database fallback only.
- Admin archive removes a deal from Homepage, Today, day detail, and neighborhood detail pages.
- Admin search submits to `/admin/deals`, preserves filters, and returns matching deals.
- Browser login cookie and `X-Admin-Key` authentication both work.
- Weekly deals support `verified`, `needs_recheck`, and `expired` lifecycle states.
- Admin Deals includes a Needs recheck filter and Verify action.
- Postgres migrations are additive and explicit instead of running on normal serverless startup.
- Supabase has the four new verification columns and the existing owner/intake/audit schema.
- Rules parsing and human-only approval behavior remain intact.
- Focused local tests now cover the core operations contract.

## What Remains Placeholder

- Business ownership exists in schema but is not used by the current venue records.
- Venue coordinates and some contact metadata remain incomplete.
- Google Maps lookup remains a manual helper; there is no automated enrichment service.
- Last-minute inventory is supported but currently unused.
- Admin authentication remains a single shared key rather than individual accounts.

## What Is Broken Or Risky

- `app/main.py` and `app/static/styles.css` remain oversized and should be modularized after this behavior is stable.
- Freshness transitions currently run when public/admin deal lists are loaded, which is acceptable for this traffic level but should become a scheduled maintenance job later.
- Existing live weekly deals received a safe verification baseline during migration; the first real recheck cycle starts from that migration date.
- GitHub CI activation remains pending because the current OAuth token lacks the `workflow` scope; the pushable release still includes the full test suite and documented commands.
- Browser smoke check was not performed through a visual browser; preview HTTP, content, auth, schema, and runtime-log checks passed.

## Verification

- Python compilation passed.
- `python -m unittest discover -s tests -v` passed: 6 tests.
- `python scripts/qa_public_site.py` passed: 6 primary routes, 7 day routes, 22 neighborhood routes, 40 exported pages, and 157 weekly master records audited.
- `git diff --check` passed.
- Additive migration script passed against local SQLite.
- Supabase schema debug confirms `verification_status`, `last_verified_at`, `valid_until`, and `verification_notes`.
- Preview public routes returned 200 for `/`, `/today`, `/days`, `/days/tuesday`, `/neighborhoods`, `/neighborhoods/des-moines`, `/neighborhoods/downtown`, and `/for-venues`.
- Preview admin intake returned 401 without auth and 200 with auth.
- Preview auth debug confirmed `LLM_PROVIDER=rules` and a matching admin header without exposing the key.
- Preview legacy write endpoints returned 401 without auth.
- A DB-only Tuesday deal appeared on both its day and neighborhood pages.
- An archived master-only deal remained absent from public output.
- Preview runtime error query returned no errors.
- Production auth debug currently confirms the requested admin key matches and `LLM_PROVIDER=rules`.
- Production browser login returned 303, set an HttpOnly session cookie, and the cookie authorized `/admin/deals` with 200.
- Production public routes returned 200; `/docs` and `/openapi.json` returned 404.
- Production legacy write endpoints returned 401 without auth.
- Production content checks confirmed the DB-only deal on day and neighborhood pages and the archived master-only deal absent.
- Production deployment is READY and aliased to `https://www.dsmdealshub.online`.
- Production runtime error query returned no errors after verification.

`npm run build` not applicable: this FastAPI project has no `package.json`.

Browser smoke check not verified.

## Next Recommended Build

Monitor the first verification/recheck cycle, activate GitHub CI after granting the `workflow` OAuth scope, and then extract admin, publishing, and freshness behavior from `app/main.py` into focused modules. Do not add Ollama or another redesign before observing real operations.

## Suggested Dashboard Update

DSM Deals Hub Operations Truth release is live: secure writes, one public publishing source, reliable archive/search behavior, and deal freshness controls are production-verified.

## Machine Readable

```json
{
  "project_name": "DSM Deals Hub",
  "phase": "Production stabilization",
  "task_name": "Deploy secure publishing, unified public data, and deal freshness",
  "date": "2026-07-11",
  "branch": "codex/dsm-deals-final-preprod",
  "live_url": "https://www.dsmdealshub.online",
  "preview_url": "https://dsm-deals-1vtm9r3td-ccr-515s-projects.vercel.app",
  "production_deployment_url": "https://dsm-deals-oy5nzra15-ccr-515s-projects.vercel.app",
  "production_deployment_id": "dpl_HpxpKMpE55LF4jrqrmAmL7Xb1PVr",
  "current_project_status": "Operations Truth release is live and verified in production.",
  "confidence_score": 98,
  "portfolio_readiness": 5,
  "money_potential": 4,
  "maintenance_burden": 4,
  "blocked_status": false,
  "blocker_reason": null,
  "production_deployed": true,
  "llm_provider": "rules",
  "browser_smoke_check": "not verified",
  "build_status": "Python compile, 6 operations tests, public QA, preview checks, Vercel build, and production checks passed; npm run build is not applicable."
}
```
