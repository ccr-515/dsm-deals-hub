# 2026-07-15 - Closed Venue Removal

## Human Summary

Project name: DSM Deals Hub

Phase: Content maintenance

Task name: Remove closed venues from the deal pool

Date: 2026-07-15

Branch: `codex/dsm-deals-final-preprod`

Production deployment: `https://dsm-deals-am9sk0x44-ccr-515s-projects.vercel.app`

Production deployment ID: `dpl_BiuwM3Kt4uwVgc6FYfUj7MPYefKf`

Live URL: `https://www.dsmdealshub.online`

Current project status: Clyde’s Fine Diner is removed from the live deal pool. Its three live deals were archived through the admin API, preserving their audit history, and its fallback records were removed. No venue matching Madeline Cafe was found in the current 84-venue production pool.

## Changed Files

- `MVP_STATUS.md`
- `README.md`
- `app/config.py`
- `app/main.py`
- `app/migrations.py`
- `app/models.py`
- `app/schemas.py`
- `app/venue_directory.json`
- `data/dsm_deals_hub_master_weekly_list.csv`
- `data/dsm_deals_hub_master_weekly_list.json`
- `docs/status/latest-handoff.md`
- `docs/status/handoff-log.md`
- `local-data/project-status.json`
- `scripts/migrate.py`
- `scripts/test_api.sh`
- `tests/test_operations_truth.py`
- Removed tracked `scripts/__pycache__/seed_curated_content.cpython-313.pyc`

## What Works

- Clyde’s Fine Diner deal IDs 46, 68, and 82 are archived in production.
- Clyde’s no longer appears in the weekly master fallback CSV/JSON or runtime venue directory.
- The public site continues to use archived-state filtering, preserving the original deal and change-log history without hard deletion.
- No current production venue is named Madeline Cafe, Madeleine Cafe, or a close matching variant.

## What Remains Placeholder

- The user’s Madeline Cafe reference does not map to an existing venue record, so no second venue was changed.

## What Is Broken Or Risky

- No production risk from the Clyde’s removal. The Madeline Cafe removal is pending a precise venue name, address, or current deal title because it is absent from the pool.
- Browser smoke check was not performed through a visual browser; preview HTTP, content, auth, schema, and runtime-log checks passed.

## Verification

- Production archive API returned 200 for all three Clyde’s deals.
- Fallback JSON files parsed successfully.
- `python scripts/qa_public_site.py` passed: 6 primary routes, 7 day routes, 22 neighborhood routes, 40 exported pages, and 154 weekly master records audited.
- `git diff --check` passed.
- Production deployment is READY and aliased to `https://www.dsmdealshub.online`.
- Clyde’s is absent from the live homepage, Tuesday, Wednesday, Thursday, and East Village pages.
- Production runtime error query returned no errors after the update.

`npm run build` not applicable: this FastAPI project has no `package.json`.

## Next Recommended Build

Deploy the fallback cleanup and provide the exact Madeline Cafe venue identity if it appears under another name.

## Suggested Dashboard Update

Closed-venue maintenance completed for Clyde’s Fine Diner. One unmatched Madeline Cafe reference remains pending identification.

## Machine Readable

```json
{
  "project_name": "DSM Deals Hub",
  "phase": "Content maintenance",
  "task_name": "Remove closed venues from the deal pool",
  "date": "2026-07-15",
  "branch": "codex/dsm-deals-final-preprod",
  "live_url": "https://www.dsmdealshub.online",
  "current_project_status": "Clyde’s Fine Diner removed; Madeline Cafe is not present under that name in production.",
  "confidence_score": 99,
  "portfolio_readiness": 5,
  "money_potential": 4,
  "maintenance_burden": 4,
  "blocked_status": false,
  "blocker_reason": null,
  "production_deployment_url": "https://dsm-deals-am9sk0x44-ccr-515s-projects.vercel.app",
  "production_deployment_id": "dpl_BiuwM3Kt4uwVgc6FYfUj7MPYefKf",
  "production_deployed": true,
  "llm_provider": "rules",
  "browser_smoke_check": "not applicable for data-only cleanup",
  "build_status": "Fallback JSON validation and public QA passed; npm run build is not applicable."
}
```
