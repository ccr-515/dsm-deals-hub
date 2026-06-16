# 2026-06-16 - Final Pre-Production Preview Branch

## Human Summary

Project name: DSM Deals Hub

Phase: Final pre-production preview

Task name: Combine public visual polish and admin intake into one preview branch

Date: 2026-06-16

Branch: `codex/dsm-deals-final-preprod`

Preview URL: `https://dsm-deals-dycaioe27-ccr-515s-projects.vercel.app`

Deployment ID: `dpl_GwrjUzyasb4FDZuDbfmoB7YYTKjp`

Current project status: Final preview is deployed and verified. Production has not been deployed.

## Changed Files

- `app/database.py`
- `app/main.py`
- `app/migrations.py`
- `app/models.py`
- `app/schemas.py`
- `app/static/styles.css`
- `app/weekly_master_content.py`
- `data/dsm_deals_hub_master_weekly_list.csv`
- `data/dsm_deals_hub_master_weekly_list.json`
- `docs/schema-reconciliation-plan.md`
- `docs/static/styles.css`
- `docs/status/latest-handoff.md`
- `docs/status/handoff-log.md`
- `local-data/project-status.json`
- `requirements.txt`
- `scripts/qa_public_site.py`

## What Works

- Finished public logo/drop-shadow visual polish is included.
- Public routes return 200 in preview: `/`, `/today`, `/days`, `/neighborhoods`, `/for-venues`.
- `python scripts/qa_public_site.py` passes locally.
- Admin login works locally in browser with `ADMIN_KEY`.
- Preview `/admin/login` returns 200, accepts password, and sets an HttpOnly `admin_session` cookie.
- `/admin/auth-debug` reports auth diagnostics without exposing the admin key.
- `/admin/intake`, `/admin/review`, and `/admin/deals` return 401 without auth and 200 with `x-admin-key`.
- `/admin/venues?q=Lua` returns 200 in preview and finds Lua Brewing.
- Rules parser detects Lua Brewing, Tuesday, `16:00`, and `18:00` from the test post.
- Review/edit can attach a real venue before approval.
- Approval is blocked when an invalid venue id is supplied.
- Approved test deal appears in admin deals.
- Archive uses a soft archive path and removes the test deal from public pages.
- Preview schema debug confirms `business_owners`, `deal_intake_submissions`, `deal_change_log`, and `venues.hours_json` as `jsonb`.
- LLM provider remains `rules`.
- No Ollama integration was added.
- No paid LLM API was added.
- Vercel runtime error logs showed no new errors after verification.

## What Remains Placeholder

- Ollama remains intentionally unimplemented for this pre-production pass.
- Production env vars were not changed. The final preview used one-off preview env values for `ADMIN_KEY` and `LLM_PROVIDER=rules`.
- The schema reconciliation plan is documented, but production should still use an explicit migration runbook before promotion.

## Broken Or Risky

- Production should not be promoted until `ADMIN_KEY` and `LLM_PROVIDER=rules` are confirmed in the intended Vercel production/preview environment scopes.
- The final branch combines a broad set of public, admin, data, and schema changes; it should be reviewed as a pre-production release candidate before production deploy.
- Startup schema helpers are safe/additive, but production should prefer a deliberate migration step rather than relying on app startup forever.

## Verification

- Python compile check passed:
  `ADMIN_KEY=dsm-admin-515 LLM_PROVIDER=rules python -m py_compile app/database.py app/main.py app/models.py app/schemas.py app/migrations.py app/weekly_master_content.py scripts/qa_public_site.py`
- Public QA passed:
  `python scripts/qa_public_site.py`
- Local browser smoke check passed for `/admin/login` to `/admin/intake`.
- Preview deployed with:
  `npx vercel@latest --yes -e ADMIN_KEY=<preview-admin-key> -e LLM_PROVIDER=rules`
- Preview public routes checked with `npx vercel@latest curl`.
- Preview admin auth checked with and without `x-admin-key`.
- Preview `/admin/auth-debug` checked and confirmed no key exposure.
- Preview `/admin/venues?q=Lua` checked and returned Lua Brewing.
- Preview schema debug checked and confirmed expected tables/columns.
- Preview intake/review/edit/approve/archive flow checked with:
  `Lua Brewing has $5 burgers every Tuesday from 4 PM to 6 PM.`
- Preview Vercel error logs checked:
  `npx vercel@latest logs https://dsm-deals-dycaioe27-ccr-515s-projects.vercel.app --since 15m --level error`

`npm run build` not applicable: this FastAPI/static export project has no `package.json`.

## Next Recommended Build

Commit and review the final pre-production branch, then run an explicit production migration/env preflight before any production deployment.

## Suggested Dashboard Update

DSM Deals Hub is in final pre-production preview. Public visual polish and admin intake are combined and verified in preview. Production is intentionally held until env scope and migration runbook are confirmed.

## Machine Readable

```json
{
  "project_name": "DSM Deals Hub",
  "phase": "Final pre-production preview",
  "task_name": "Combine public visual polish and admin intake into one preview branch",
  "date": "2026-06-16",
  "branch": "codex/dsm-deals-final-preprod",
  "preview_url": "https://dsm-deals-dycaioe27-ccr-515s-projects.vercel.app",
  "deployment_id": "dpl_GwrjUzyasb4FDZuDbfmoB7YYTKjp",
  "current_project_status": "Final preview deployed and verified; production not deployed.",
  "confidence_score": 94,
  "portfolio_readiness": 5,
  "money_potential": 4,
  "maintenance_burden": 5,
  "blocked_status": false,
  "blocker_reason": null,
  "production_deployed": false,
  "llm_provider": "rules",
  "browser_smoke_check": "verified locally for admin login",
  "build_status": "Vercel preview build passed; npm run build not applicable because no package.json exists."
}
```
