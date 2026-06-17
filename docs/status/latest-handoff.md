# 2026-06-17 - Admin Remove/Search Hotfix

## Human Summary

Project name: DSM Deals Hub

Phase: Production hotfix

Task name: Fix admin deal search/remove behavior and remove Django from public output

Date: 2026-06-17

Branch: `codex/dsm-deals-final-preprod`

Live URL: `https://www.dsmdealshub.online`

Production deployment URL: `https://dsm-deals-htboqy66u-ccr-515s-projects.vercel.app`

Production deployment ID: `dpl_HVmbzYcdjpFcacPy1abYLCPi3K7N`

Current project status: Production hotfix is live. Django no longer appears on public live routes, admin deals search defaults to active deals, removed deals are visible under the removed/archived filter, and admin logout is available to clear stale sessions.

## Changed Files

- `app/main.py`
- `data/dsm_deals_hub_master_weekly_list.csv`
- `data/dsm_deals_hub_master_weekly_list.json`
- `docs/status/latest-handoff.md`
- `docs/status/handoff-log.md`
- `local-data/project-status.json`

## What Works

- Public live routes no longer render Django.
- Django was removed from the bundled weekly master CSV and JSON fallback data.
- Public DB-backed deal loading suppresses retired venue name `Django` even if a live DB row exists.
- Admin deals search input is an explicit search field inside a GET form pointed at `/admin/deals`.
- Admin deals defaults to `Active only`, hiding archived/rejected/expired rows so archive feels like removal.
- Removed deals remain available through `Removed / archived` or `All statuses`; no hard delete was added.
- Admin action label is clearer: `Remove from live`.
- Archive/freeze actions preserve the current admin search/filter return path.
- `/admin/logout` clears the admin session cookie and returns to login.
- Production `/admin/auth-debug` confirms configured key, `LLM_PROVIDER=rules`, header seen, and header matches without exposing the key.
- Production error logs showed no new errors after verification.

## What Remains Placeholder

- Admin auth is still single shared-key MVP auth.
- Hard delete is intentionally not implemented.
- Google Maps remains a manual helper; no automated venue enrichment was added.

## Broken Or Risky

- Existing static exported docs still contain old generated Django HTML, but Vercel production routes all traffic through `api/index.py`, so the live site is dynamic and verified clean.
- Admin password value itself is an environment concern; code now includes logout so stale browser sessions can be cleared.

## Verification

- Python compile passed:
  `ADMIN_KEY=<admin-key> LLM_PROVIDER=rules python -m py_compile app/main.py app/weekly_master_content.py scripts/qa_public_site.py`
- Weekly master JSON validation passed:
  `python -m json.tool data/dsm_deals_hub_master_weekly_list.json`
- Local admin search/archive smoke passed.
- Local public routes returned 200 and did not contain Django for `/`, `/today`, `/days`, `/days/tuesday`, `/neighborhoods`, `/neighborhoods/downtown`, `/for-venues`.
- Public QA passed:
  `python scripts/qa_public_site.py`
- Production deploy passed:
  `npx vercel@latest --prod --yes`
- Production public routes returned 200 and did not contain Django for `/`, `/today`, `/days`, `/days/tuesday`, `/neighborhoods`, `/neighborhoods/downtown`, `/for-venues`.
- Production `/admin/auth-debug` returned safe matching diagnostics and did not expose the key.
- Production `/admin/deals?q=django` returned 200, kept search UI intact, and defaulted away from removed rows.
- Production `/admin/deals?q=django&status=all` and `status=archived` show Django as archived/removed only.
- Production Vercel error logs checked clean:
  `npx vercel@latest logs https://www.dsmdealshub.online --since 15m --level error`

`npm run build` not applicable: this FastAPI/static export project has no `package.json`.

Browser smoke check not verified through the in-app browser.

## Next Recommended Build

Add a small dedicated admin venue/deal maintenance screen with clearer filters, logout placement, and a password rotation note for operators.

## Suggested Dashboard Update

DSM Deals Hub live admin hotfix deployed. Public Django issue is resolved; admin deals search/remove behavior is clearer and verified in production.

## Machine Readable

```json
{
  "project_name": "DSM Deals Hub",
  "phase": "Production hotfix",
  "task_name": "Fix admin deal search/remove behavior and remove Django from public output",
  "date": "2026-06-17",
  "branch": "codex/dsm-deals-final-preprod",
  "live_url": "https://www.dsmdealshub.online",
  "production_deployment_url": "https://dsm-deals-htboqy66u-ccr-515s-projects.vercel.app",
  "production_deployment_id": "dpl_HVmbzYcdjpFcacPy1abYLCPi3K7N",
  "current_project_status": "Live hotfix deployed and verified.",
  "confidence_score": 94,
  "portfolio_readiness": 5,
  "money_potential": 4,
  "maintenance_burden": 5,
  "blocked_status": false,
  "blocker_reason": null,
  "production_deployed": true,
  "llm_provider": "rules",
  "browser_smoke_check": "not verified through in-app browser; direct production HTTP checks passed",
  "build_status": "Vercel production build passed; npm run build not applicable because no package.json exists."
}
```
