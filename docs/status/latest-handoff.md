# 2026-08-04 - Closed Venue Archive Follow-up

## Human Summary

Project name: DSM Deals Hub

Phase: Live content maintenance

Task name: Archive deals for venues confirmed closed by Google Maps

Date: 2026-08-04

Branch: `codex/dsm-deals-final-preprod`

Production deployment: `https://dsm-deals-7sar7mz0g-ccr-515s-projects.vercel.app`

Production deployment ID: `dpl_7YYv1wswLdtCNE3QgzUinZX4qHCz`

Live URL: `https://www.dsmdealshub.online`

Current project status: The live deal pool includes the vetted Monday/Tuesday refresh and excludes the three venues confirmed closed by Google Maps. Direct admin API creation, approval, updating, expiration, archiving, and venue creation create audit records.

## Changed Files

- `docs/status/latest-handoff.md`
- `docs/status/handoff-log.md`
- `local-data/project-status.json`

The live database was updated through authenticated admin archive endpoints. No deals or venues were hard-deleted.

## What Works

- Archived Louie's Wine Dive deal ID 141, The Beerhouse deal ID 121, and The Tavern Grill deal ID 120 after the Google Maps closure recheck.
- Confirmed the archived deals no longer render on the homepage, Today, Days, Neighborhoods, or For Venues routes.
- Retained all three venue and deal records for audit/history; only their public deal status changed to `archived`.
- Public routes, admin data reads, and the production Vercel deployment are healthy.

## What Remains Placeholder

- Source items without a concrete local location or offer detail remain intentionally excluded from the previous Tuesday refresh.
- Future closure checks should use the same archive workflow; no active closure-review candidate remains from the prior audit.

## What Is Broken Or Risky

- Older weekly records can still need time normalization before an API update.
- The core FastAPI module remains large, which increases maintenance cost.

## Verification

- `python scripts/qa_public_site.py` passed: 6 primary routes, 7 day routes, 22 neighborhood routes, 40 exported pages, and 154 weekly-master records audited.
- Production routes `/`, `/today`, `/days`, `/neighborhoods`, and `/for-venues` returned 200 and contained none of the three archived venue names.
- Authenticated production admin data verification confirmed deal IDs 120, 121, and 141 are archived.
- Vercel deployment `dpl_7YYv1wswLdtCNE3QgzUinZX4qHCz` remains `READY`; inspected archive and public-route runtime requests completed without server exceptions.

`npm run build` not applicable: this FastAPI project has no `package.json`.

Browser smoke check not verified; route and API verification were performed with authenticated HTTP requests.

## Next Recommended Build

Schedule a recurring closure audit and reverify the newly added weekly specials before the standard freshness window elapses.

## Suggested Dashboard Update

Three Google Maps-closed venues are archived from public listings; no known closure candidate remains from the prior audit.

## Machine Readable

```json
{
  "project_name": "DSM Deals Hub",
  "phase": "Live content maintenance",
  "task_name": "Archive deals for venues confirmed closed by Google Maps",
  "date": "2026-08-04",
  "branch": "codex/dsm-deals-final-preprod",
  "live_url": "https://www.dsmdealshub.online",
  "production_deployment_url": "https://dsm-deals-7sar7mz0g-ccr-515s-projects.vercel.app",
  "production_deployment_id": "dpl_7YYv1wswLdtCNE3QgzUinZX4qHCz",
  "production_deployed": true,
  "current_project_status": "Tuesday refresh is live; three confirmed closed venues are archived from public listings.",
  "changed_files": ["docs/status/latest-handoff.md", "docs/status/handoff-log.md", "local-data/project-status.json"],
  "what_works": ["three confirmed-closed venue deals are archived", "public routes no longer render them", "history remains in the audit trail"],
  "placeholders": ["ambiguous chain-wide and out-of-market source items remain excluded"],
  "risks": ["older weekly records may need time normalization before API edits", "app/main.py remains oversized"],
  "next_recommended_build": "Run a recurring closure audit and verify new weekly offers before their freshness window expires.",
  "dashboard_update": "Three Google Maps-closed venues archived; no prior closure candidate remains.",
  "confidence_score": 97,
  "portfolio_readiness": 5,
  "money_potential": 4,
  "maintenance_burden": 4,
  "blocked_status": false,
  "blocker_reason": null,
  "verification": {
    "public_qa": "passed",
    "production_route_checks": "passed",
    "archived_deal_ids": [120, 121, 141],
    "vercel_build": "READY",
    "runtime_server_exceptions": "none observed",
    "browser_smoke_check": "not verified"
  }
}
```
