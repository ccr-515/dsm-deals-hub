# 2026-06-16 - Admin Venue Create From Intake

## Human Summary

Project name: DSM Deals Hub

Phase: Admin deal intake refinement

Task name: Add obvious create-and-attach venue path in admin review

Date: 2026-06-16

Branch: `codex/dsm-deals-final-preprod`

Preview URL: `https://dsm-deals-q7cpc7r00-ccr-515s-projects.vercel.app`

Deployment ID: `dpl_2nj6WLM5UW8aGVHc1m5i2oE5gExq`

Current project status: Preview-only admin venue creation refinement is deployed and verified. Production has not been deployed.

## Changed Files

- `app/main.py`
- `app/static/styles.css`
- `docs/status/latest-handoff.md`
- `docs/status/handoff-log.md`
- `local-data/project-status.json`

## What Works

- Admin review edit mode now has an obvious `Create new venue` panel.
- The panel includes venue name, address, neighborhood, phone, website, and notes fields.
- `Create venue and attach` creates a real venue when no venue is selected, then stores the selected `venue_id` in the proposal.
- If an exact venue name already exists, the flow reuses that venue and fills missing optional fields instead of creating a duplicate.
- Explicit create uses exact-name matching only; normal venue search still supports fuzzy matching.
- The review page shows an attached-venue note after a venue is selected or created.
- The Google Maps helper opens a Maps search for the venue name/address and updates as the admin types.
- Venue creation failure now shows a readable admin error instead of silently leaving approval blocked.
- Existing approval guard remains intact: deals still require a real venue before approval.
- Public routes were not changed and still pass QA.
- No Ollama or paid LLM API was added.
- Production was not deployed.

## What Remains Placeholder

- Google Maps is a browser helper link, not an automated importer.
- Automatic extraction from Google Maps was not added because it would require either brittle scraping or a proper Places-style API/key decision.
- Hours, lat/lng, and richer Maps metadata still need manual entry or a future approved provider integration.

## Broken Or Risky

- Browser smoke check was not fully verified: the in-app browser password-entry path failed because its virtual clipboard is unavailable, and `file://` fallback inspection was blocked by browser policy.
- The new Google Maps helper does not write data into the database automatically; the admin still copies verified fields into the form.
- The local admin flow created a local-only test venue named `Codex Venue Create Test ...` in the local database.

## Verification

- Python compile passed:
  `ADMIN_KEY=dsm-admin-515 LLM_PROVIDER=rules python -m py_compile app/main.py app/models.py app/schemas.py app/migrations.py scripts/qa_public_site.py`
- Local admin create-and-attach flow passed with FastAPI TestClient.
- Local test confirmed created venue persisted address, phone, website, and attached `venue_id` to the proposal.
- Public QA passed:
  `python scripts/qa_public_site.py`
- Local route/auth smoke passed for `/`, `/today`, `/days`, `/neighborhoods`, `/for-venues`, `/admin/intake`, `/admin/review`, `/admin/deals`.
- Vercel preview build passed:
  `npx vercel@latest --yes -e ADMIN_KEY=<preview-admin-key> -e LLM_PROVIDER=rules`
- Preview public routes returned `200`: `/`, `/today`, `/days`, `/neighborhoods`, `/for-venues`.
- Preview protected admin routes returned `401` without auth and `200` with `x-admin-key`: `/admin/intake`, `/admin/review`, `/admin/deals`.
- Preview `/admin/review?edit=1` rendered `Create new venue`, `Create venue and attach`, `Look up on Google Maps`, and `admin-google-maps-venue-lookup`.
- Vercel runtime error logs showed no new errors:
  `npx vercel@latest logs https://dsm-deals-q7cpc7r00-ccr-515s-projects.vercel.app --since 15m --level error`

`npm run build` not applicable: this FastAPI/static export project has no `package.json`.

## Next Recommended Build

Use the preview admin review page to add a missing venue manually from Google Maps. If automatic venue enrichment becomes important, decide separately whether to use a compliant provider/API and where the key should live.

## Suggested Dashboard Update

DSM Deals Hub admin intake now supports creating and attaching a missing venue directly during review. Preview is verified. Production is still held.

## Machine Readable

```json
{
  "project_name": "DSM Deals Hub",
  "phase": "Admin deal intake refinement",
  "task_name": "Add obvious create-and-attach venue path in admin review",
  "date": "2026-06-16",
  "branch": "codex/dsm-deals-final-preprod",
  "preview_url": "https://dsm-deals-q7cpc7r00-ccr-515s-projects.vercel.app",
  "deployment_id": "dpl_2nj6WLM5UW8aGVHc1m5i2oE5gExq",
  "current_project_status": "Preview-only admin venue creation refinement deployed and verified; production not deployed.",
  "confidence_score": 92,
  "portfolio_readiness": 5,
  "money_potential": 4,
  "maintenance_burden": 5,
  "blocked_status": false,
  "blocker_reason": null,
  "production_deployed": false,
  "llm_provider": "rules",
  "browser_smoke_check": "not fully verified because browser plugin password entry/file inspection were blocked",
  "build_status": "Vercel preview build passed; npm run build not applicable because no package.json exists."
}
```
