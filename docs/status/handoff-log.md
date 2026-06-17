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

# 2026-06-15 - Secret rotation and preview verification

- Project: DSM Deals Hub
- Phase: Secret rotation and preview verification
- Task: Rotate Supabase DB password and refresh Vercel DATABASE_URL
- Status: Preview verified after secret rotation. Production not deployed.
- Changed files: `docs/status/latest-handoff.md`, `docs/status/handoff-log.md`, `local-data/project-status.json`.
- External changes: Supabase DB password rotated for `ooyfemuayqwjwazxyclx`; Vercel `DATABASE_URL` rows deleted and recreated for Production+Preview, Preview branch `post-static-transition`, and Development.
- What works: No `.env` files are tracked; preview branch env pull verified the rotated `DATABASE_URL`; local DB `select 1` returned `1`; local Vercel-mode route smoke passed; preview build reached READY; `/today` returned DSM Deals Hub HTML.
- What remains placeholder: Production deployment remains intentionally held; Browser smoke check not verified.
- Broken or risky: Preview branch row is pullable because Vercel sensitive values cannot be verified with `env pull`; Development row is not sensitive because Vercel disallows Sensitive variables in Development.
- Verification: `npx vercel@latest --yes` build passed; `npx vercel@latest curl /today --deployment https://dsm-deals-p0cxiii1a-ccr-515s-projects.vercel.app` returned HTML.
- Next: Review preview, optionally convert the branch Preview row to Sensitive after env-pull verification is no longer needed, and deploy production only with explicit approval.
- Suggested dashboard update: status `active`, blocked `false`, confidence `91`, portfolio readiness `4`, money potential `4`, maintenance burden `3`.

# 2026-06-15 - Live now and preview hardening

- Project: DSM Deals Hub
- Phase: Live now and preview hardening
- Task: Fix live now logic, mobile nav scrolling, and close the Django venue
- Status: Preview is verified and ready for review. Production has not been deployed.
- Changed files: `app/main.py`, `app/static/styles.css`, `app/utils.py`, `app/venue_directory.json`, `venue_enrichment_batch_04.json`, `docs/status/latest-handoff.md`, `docs/status/handoff-log.md`, `local-data/project-status.json`.
- What works: Live now uses DB-backed filtering and sorting; cross-midnight windows work; the live dot only glows red when live deals exist; mobile nav pills now scroll on small screens; Django content was removed from public source files; the remaining Django live deal was archived in preview; local Vercel-mode smoke passed; preview route sweep returned `200` for all checked routes.
- What remains placeholder: Browser smoke check not verified; production deploy intentionally held for review.
- Broken or risky: Preview protection stays enabled, so route verification uses `vercel curl`; the preview DB was updated directly to archive the remaining Django live deal.
- Verification: `npx vercel@latest --yes` build passed; local helper checks passed; local Vercel-mode route smoke passed; preview deployment reached READY at `https://dsm-deals-71r0pm23n-ccr-515s-projects.vercel.app`; full preview route sweep returned `200`.
- Next: Review the preview route sweep, then deploy production only if the preview looks correct.
- Suggested dashboard update: status `active`, blocked `false`, confidence `93`, portfolio readiness `4`, money potential `4`, maintenance burden `3`.

# 2026-06-15 - Homepage live-now and Today preview fix

- Project: DSM Deals Hub
- Phase: Homepage live-now and Today preview fix
- Task: Make the homepage request-time dynamic and reuse the current Today source
- Status: Homepage request-time dynamic fix is verified locally. Production has not been deployed.
- Changed files: `api/index.py`, `app/main.py`, `docs/status/latest-handoff.md`, `docs/status/handoff-log.md`, `local-data/project-status.json`.
- What works: The homepage now renders through the FastAPI app instead of static docs; the homepage Today preview now uses the same request-time Today source as `/today`; Live now still renders from the current DB-backed live set; local Vercel-mode route smoke passed; the homepage no longer shows the stale Friday preview marker in local verification; production was not deployed.
- What remains placeholder: Browser smoke check not verified; production deployment remains intentionally held for review.
- Broken or risky: Preview protection remains enabled, so deployed route verification uses `vercel curl` against the deployment.
- Verification: Local Vercel-mode route smoke passed; preview smoke command `VERCEL=1 python - <<'PY' ...` passed with `LOCAL HOMEPAGE CURRENTNESS CHECK OK`.
- Next: Review the homepage in preview, then deploy production only if the preview looks correct.
- Suggested dashboard update: status `active`, blocked `false`, confidence `92`, portfolio readiness `4`, money potential `4`, maintenance burden `3`.

# 2026-06-15 - Homepage dynamic source hardening

- Project: DSM Deals Hub
- Phase: Homepage dynamic source hardening
- Task: Fix root-only preview 500 without serving static docs
- Status: Root preview 500 is fixed on a new preview deployment. Production has not been deployed.
- Changed files: `app/main.py`, `docs/status/latest-handoff.md`, `docs/status/handoff-log.md`, `local-data/project-status.json`.
- What works: `/` renders dynamically through FastAPI; `api/index.py` remains a simple app import; Live Now remains DB-backed and request-time; homepage Today preview passes a plain current-day deal list; homepage neighborhood cards and day cards use DB-backed adapters; local smoke passed; missing-weekly-master regression passed; new preview route checks passed.
- What remains placeholder: Browser smoke check not verified; production deployment remains intentionally held for review.
- Broken or risky: Some deeper dynamic detail pages may still rely on weekly-master metadata conventions for grouping labels; preview protection remains enabled, so route verification used `vercel curl`.
- Verification: `python -m py_compile api/index.py app/main.py` passed; requested local Vercel-mode smoke passed; missing weekly master regression passed; `npx vercel@latest --yes` produced preview `https://dsm-deals-kni08hhri-ccr-515s-projects.vercel.app`; preview returned `200` for `/`, `/today`, `/days`, `/neighborhoods`, and `/for-venues`; preview homepage contains `Live Now`, uses `has-live-deals`, and does not contain the stale Friday marker.
- Next: Review the new preview homepage, then explicitly approve production only after the preview looks correct.
- Suggested dashboard update: status `active`, blocked `false`, confidence `94`, portfolio readiness `4`, money potential `4`, maintenance burden `3`.

# 2026-06-15 - Admin deal intake console

- Project: DSM Deals Hub
- Phase: Admin deal intake
- Task: Build zero-paid-LLM admin Deal Intake Console
- Status: Implemented, locally verified, browser-smoked, and deployed to preview only. Production was not deployed.
- Changed files: `app/main.py`, `app/models.py`, `app/static/styles.css`, `requirements.txt`, `scripts/qa_public_site.py`, `docs/status/latest-handoff.md`, `docs/status/handoff-log.md`, `local-data/project-status.json`.
- What works: `/admin/intake`, `/admin/review`, and `/admin/deals` are protected admin pages; provider modes are `ollama`, `rules`, and `mock`; Ollama defaults to `qwen2.5:7b`; Vercel Ollama mode shows `Ollama is local-only. Run the admin intake locally.`; proposals are structured JSON only; database writes happen only after approval; raw source text and audit log are kept; archive/freeze quick actions avoid hard deletes; manual fallback form works; public requested routes returned 200 locally and on preview.
- What remains placeholder: Hosted deployed LLM is intentionally not configured; venue creation from intake is still manual; duplicate update/archive approval needs one clear existing deal candidate.
- Broken or risky: Local Ollama must be running for `LLM_PROVIDER=ollama`; admin key should be changed before serious use; admin UI is functional but simple; known-neighborhood fallback can render empty pages when preview DB lacks a group.
- Verification: `python -m py_compile api/index.py app/main.py app/models.py app/migrations.py app/schemas.py app/utils.py` passed; `python scripts/qa_public_site.py` passed; local public/admin route smoke passed; isolated Ollama-on-Vercel fallback test passed; isolated manual approval wrote a draft deal and audit log; Browser smoke checked local admin intake/review/deals; `npx vercel@latest --yes` produced final preview `https://dsm-deals-pbxdesp0y-ccr-515s-projects.vercel.app`; final preview returned `200` for `/`, `/today`, `/days`, `/days/monday`, `/neighborhoods`, `/neighborhoods/downtown`, and `/for-venues`; final preview admin routes returned `401` without auth and `200` with `X-Admin-Key`; paid API grep found no OpenAI or Anthropic references.
- Next: Run local Ollama intake with real restaurant posts, then add a venue-create shortcut if needed.
- Suggested dashboard update: status `active`, blocked `false`, blocker reason ``, confidence `88`, portfolio readiness `4`, money potential `4`, maintenance burden `4`.

# 2026-06-16 - Admin login cookie

- Project: DSM Deals Hub
- Phase: Admin authentication
- Task: Add cookie-based admin login page
- Status: Implemented, locally verified, browser-smoked, and deployed to preview only. Production was not deployed.
- Changed files: `app/main.py`, `app/models.py`, `app/static/styles.css`, `requirements.txt`, `scripts/qa_public_site.py`, `docs/status/latest-handoff.md`, `docs/status/handoff-log.md`, `local-data/project-status.json`.
- What works: `GET /admin/login` renders a password form; `POST /admin/login` validates `ADMIN_KEY`, sets signed `admin_session`, and redirects; `/admin/intake`, `/admin/review`, and `/admin/deals` accept either `X-Admin-Key` or cookie session; curl header support remains; Vercel preview resolves `LLM_PROVIDER` to `rules`; public routes still return 200.
- What remains placeholder: No logout route yet; admin auth is still single-password MVP auth.
- Broken or risky: Default `ADMIN_KEY` should be rotated before serious use; cookie sessions intentionally invalidate when `ADMIN_KEY` changes; `vercel curl` can be slow on preview checks.
- Verification: `python -m py_compile app/main.py app/models.py` passed; `python scripts/qa_public_site.py` passed; local auth smoke passed; Browser smoke verified login to `/admin/deals`; `npx vercel@latest --yes` produced preview `https://dsm-deals-dhklbybzh-ccr-515s-projects.vercel.app`; preview returned `200` for requested public routes plus `/admin/login`; preview admin routes returned `401` without auth and `200` with `X-Admin-Key`; preview `POST /admin/login` returned redirect headers with `admin_session`.
- Next: Add `/admin/logout` and rotate `ADMIN_KEY` before real admin use.
- Suggested dashboard update: status `active`, blocked `false`, blocker reason ``, confidence `93`, portfolio readiness `4`, money potential `4`, maintenance burden `4`.

# 2026-06-16 - Nightlife liquid-glass polish

- Project: DSM Deals Hub
- Phase: Visual polish
- Task: CSS-first nightlife liquid-glass polish pass
- Status: Implemented locally, public QA passed, Browser route/mobile smoke passed, and production was not deployed.
- Changed files: `app/static/styles.css`, `docs/static/styles.css`, `docs/status/latest-handoff.md`, `docs/status/handoff-log.md`, `local-data/project-status.json`.
- What works: Public CSS now has a bottom-of-file polish layer with darker layered glass, richer green surfaces, warm orange/gold accents, CTA glow, active pill glow, desktop hover lift, icon shine/pop, section fade-in, Live Now pulse styling, and smoother detail expansion. Homepage, Today, Days, Neighborhoods, Got a deal, Tuesday detail, and Ingersoll detail rendered locally as nonblank DSM Deals Hub pages with no framework/error overlay. Live Now toggled open in Browser testing. Mobile page width stayed locked to 390px; site nav and day nav kept internal horizontal scrolling; the Google Form iframe fit inside the mobile viewport.
- What remains placeholder: No new functional placeholders were added. No new visual assets were generated. Browser screenshot artifact capture was not produced because the in-app Browser screenshot endpoint timed out and local Playwright lacks its browser binary.
- Broken or risky: Screenshot-based visual proof is not available from this run, so the final dark-theme taste pass should be reviewed by a human in the browser. The darker skin may need contrast or glow tuning on real devices. Pre-existing unrelated modified files remain in the worktree.
- Verification: `python -m py_compile app/main.py app/models.py scripts/qa_public_site.py` passed; `python scripts/qa_public_site.py` passed; local Browser smoke checked `/`, `/today/`, `/days/`, `/neighborhoods/`, `/for-venues/`, `/days/tuesday/`, and `/neighborhoods/ingersoll/`; Browser console had no warning/error entries in the final route pass; mobile site nav scrolled from `0` to `112`; mobile day nav scrolled from `0` to `329`; mobile form iframe width was `313px` inside a `342px` shell at a `390px` viewport. `npm run build` is not applicable because there is no `package.json`.
- Static export status: `docs/static/styles.css` was updated; static HTML pages were not regenerated because this was CSS-only and no route/content/data changes were required.
- Next: Human visual review of the seven polished public pages, then tune contrast or glow intensity if needed before any deployment.
- Suggested dashboard update: status `active`, blocked `false`, blocker reason ``, confidence `88`, portfolio readiness `4`, money potential `4`, maintenance burden `3`.

# 2026-06-16 - Logo-aligned tone correction

- Project: DSM Deals Hub
- Phase: Visual polish
- Task: Correct the too-green polish pass to better match the logo
- Status: Implemented locally, public QA passed, focused Browser computed-style/mobile smoke passed, and production was not deployed.
- Changed files: `app/static/styles.css`, `docs/static/styles.css`, `docs/status/latest-handoff.md`, `docs/status/handoff-log.md`, `local-data/project-status.json`.
- What works: Added a bottom-of-file tone correction that moves the palette away from saturated green toward darker neutral charcoal glass, cream typography, logo-orange accents, warm glass borders, and restrained olive/sage support. The homepage computed styles now show brand text `rgb(255, 241, 220)`, eyebrow `rgb(255, 155, 62)`, hero border `rgba(255, 225, 176, 0.16)`, and neutral glass gradients. Mobile page width stayed locked to 390px and site nav scrolled internally from `0` to `112`.
- What remains placeholder: No functional placeholders or new visual assets were added. Screenshot artifact capture remains unavailable from this environment.
- Broken or risky: Final visual quality still needs human review because the issue was screenshot-level taste. Glow intensity and contrast may need one more subjective tuning pass.
- Verification: `python -m py_compile app/main.py app/models.py scripts/qa_public_site.py` passed; `python scripts/qa_public_site.py` passed; focused Browser check on `/` passed with no warning/error console entries and no mobile page drift. `npm run build` is not applicable because there is no `package.json`.
- Static export status: `docs/static/styles.css` was updated; static HTML pages were not regenerated because this was CSS-only.
- Next: Review the corrected homepage visually, then tune exact glass darkness/glow intensity if needed before any deployment.
- Suggested dashboard update: status `active`, blocked `false`, blocker reason ``, confidence `90`, portfolio readiness `4`, money potential `4`, maintenance burden `3`.

# 2026-06-16 - Admin auth debug and lowercase header fix

- Project: DSM Deals Hub
- Phase: Admin authentication
- Task: Add browser login and auth debug for admin preview
- Status: Implemented and verified on preview. Production was not deployed.
- Changed files: `app/main.py`, `app/models.py`, `app/static/styles.css`, `requirements.txt`, `scripts/qa_public_site.py`, `docs/status/latest-handoff.md`, `docs/status/handoff-log.md`, `local-data/project-status.json`.
- What works: `GET /admin/login` renders a form; `POST /admin/login` validates `ADMIN_KEY`, sets HttpOnly `admin_session`, and redirects; `/admin/intake`, `/admin/review`, and `/admin/deals` allow either `X-Admin-Key` / lowercase `x-admin-key` or cookie auth; `/admin/auth-debug` reports `admin_key_configured`, `llm_provider`, `header_seen`, and `cookie_seen` without exposing the key; Vercel preview resolves LLM provider to `rules`; public routes still return 200.
- What remains placeholder: No logout route yet; admin auth is still single-password MVP auth.
- Broken or risky: Default `ADMIN_KEY` should be rotated before serious use; cookie sessions invalidate when `ADMIN_KEY` changes; `vercel curl` can be slow on route checks.
- Verification: `python -m py_compile app/main.py app/models.py` passed; `python scripts/qa_public_site.py` passed; local auth smoke passed for lowercase header and cookie; `npx vercel@latest --yes` produced preview `https://dsm-deals-elxbaacas-ccr-515s-projects.vercel.app`; preview routes returned 200 for `/`, `/today`, `/days`, `/days/monday`, `/neighborhoods`, `/neighborhoods/downtown`, `/for-venues`, and `/admin/login`; preview `/admin/auth-debug` saw lowercase `x-admin-key`; preview admin routes returned `401` without auth and `200` with lowercase `x-admin-key`; preview `POST /admin/login` returned redirect headers with `admin_session`.
- Next: Add `/admin/logout` and rotate `ADMIN_KEY` before real admin use.
- Suggested dashboard update: status `active`, blocked `false`, blocker reason ``, confidence `95`, portfolio readiness `4`, money potential `4`, maintenance burden `4`.

# 2026-06-16 - Admin auth guard repair

- Project: DSM Deals Hub
- Phase: Admin authentication
- Task: Repair protected admin route auth guard
- Status: Implemented, locally verified, and deployed to preview only. Production was not deployed.
- Changed files: `app/main.py`, `docs/status/latest-handoff.md`, `docs/status/handoff-log.md`, `local-data/project-status.json`.
- Preview: `https://dsm-deals-f3yopwiyv-ccr-515s-projects.vercel.app` (`dpl_HYDqSWZoW2xvm9rwX1rGAfhAQVaD`).
- What works: One shared `is_admin_authorized(request)` function now backs `/admin/intake`, `/admin/review`, `/admin/deals`, admin POST routes, and JSON admin APIs; header auth reads and strips `x-admin-key`; key comparison uses `hmac.compare_digest`; `/admin/login` POST strips the `password` form field and sets a valid HttpOnly `admin_session`; `/admin/auth-debug` reports `admin_key_configured`, `llm_provider`, `header_seen`, `header_matches`, `cookie_seen`, and `cookie_valid` without exposing the key.
- What remains placeholder: No logout route yet; admin auth remains single-password MVP auth.
- What is broken or risky: Local `npx vercel@latest build --yes` is blocked by the workstation's old `uv` binary, but remote Vercel preview build completed successfully; preview protection means verification uses `vercel curl`; cookie sessions invalidate when `ADMIN_KEY` changes.
- Verification: `ADMIN_KEY=dsm-admin-515 python -m py_compile app/main.py app/models.py` passed; local auth smoke passed for no-auth, lowercase header auth, login POST, and cookie auth; `python scripts/qa_public_site.py` passed; remote preview deploy/build passed; required `npx vercel@latest curl /admin/intake --deployment "$PREVIEW_URL" -H "x-admin-key: dsm-admin-515"` returned the Deal Intake HTML with status `200`; preview admin routes returned `401` without auth and `200` with `x-admin-key`; preview login cookie flow returned `303` then `200`; requested public preview routes all returned `200`. Browser smoke check not verified. `npm run build` is not applicable because there is no `package.json`.
- Current project status: Admin preview auth guard is fixed and verified on the new preview deployment. Production was not deployed.
- Next recommended build: Add `/admin/logout` and consider rotating `ADMIN_KEY` before real admin use.
- Suggested dashboard update: status `active`, blocked `false`, blocker reason ``, confidence `96`, portfolio readiness `4`, money potential `4`, maintenance burden `4`.

# 2026-06-16 - Clean liquid-glass refinement

- Project name: DSM Deals Hub
- Phase: Visual polish
- Task name: Clean liquid-glass color refinement and logo artifact fix
- Date: 2026-06-16 16:54 CDT
- Status: CSS-only refinement is implemented locally, verified against public routes, and not deployed.
- Changed files: `app/static/styles.css`, `docs/static/styles.css`, `docs/status/latest-handoff.md`, `docs/status/handoff-log.md`, `local-data/project-status.json`.
- What works: The final CSS polish layer now reduces muddy brown/black, uses cleaner deep green and charcoal glass, keeps warm gold/orange accents, brightens neighborhood icon badge backgrounds, and disables inherited badge chrome on both homepage and compact subpage brand image badges. Existing hover lift, active pill glow, CTA depth, Live Now pulse, section fade-in, icon pop, and detail easing remain active.
- What remains placeholder: No functional placeholders were added. No data, route, export, deal, venue, logo image, or icon asset changes were made. Screenshot capture was not included in this handoff.
- What is broken or risky: Final visual taste still needs human review; multiple bottom-of-file visual override layers may need consolidation later; pre-existing unrelated modified files remain in the worktree.
- Current project status: CSS-only liquid-glass refinement is implemented locally and verified.
- Verification: Build not verified. No `package.json` or npm build command exists. `python -m py_compile app/main.py app/models.py scripts/qa_public_site.py` passed. `python scripts/qa_public_site.py` passed. Browser smoke checked `/`, `/today/`, `/days/`, `/neighborhoods/`, `/for-venues/`, `/days/tuesday/`, and `/neighborhoods/ingersoll/`; Browser console had no warning/error entries; wide desktop had no page drift; mobile 390px had no page drift; mobile site nav scrolled from `0` to `112`; mobile day nav scrolled from `0` to `329`; mobile form iframe fit; brand pseudo-elements computed as `content: none` and `display: none`.
- Static export status: `docs/static/styles.css` was updated. Static HTML pages were not regenerated because this was CSS-only.
- Next recommended build: Human visual review of the seven public pages, followed by tiny CSS token/glow tuning if needed.
- Suggested dashboard update: status `active`, blocked `false`, blocker reason ``, confidence `92`, portfolio readiness `4`, money potential `4`, maintenance burden `3`.

# 2026-06-16 - Admin rules parser and review edit flow

- Project: DSM Deals Hub
- Phase: Admin deal intake
- Task: Improve rules parser, venue matching, and review edit approval flow
- Status: Implemented, locally browser-verified, and deployed to preview only. Production was not deployed.
- Changed files: `app/main.py`, `app/static/styles.css`, `docs/status/latest-handoff.md`, `docs/status/handoff-log.md`, `local-data/project-status.json`.
- Preview: `https://dsm-deals-2tpavbgod-ccr-515s-projects.vercel.app` (`dpl_5MYctwvFn7X19SXa25nrVXHzuCri`).
- What works: Rules parsing detects weekdays inside full sentences, including `every Tuesday` and `Tuesdays`; simple `Venue has Deal...` posts infer venue name and title; local parser check produced `Lua Brewing`, `$5 burgers`, `Tue`, `16:00`, and `18:00`; local fuzzy matching confirmed `Lua Brewing` matches a `Lua Brewing` venue when present; edit mode now has venue search/dropdown, day checkboxes, start/end time, title, description, and source URL; saving edit returns to review with updated proposal; Approve is disabled and server-blocked until venue, day, title, and description are valid.
- What remains placeholder: Ollama remains intentionally unused; venue creation from intake remains manual; preview could not prove a Lua venue match because that environment blocked approval with `Pick a valid venue before approving.`
- What is broken or risky: Preview `/admin/venues?q=Lua` returned `Internal Server Error` during a venue-presence check; Browser screenshot capture timed out; unrelated modified files remain in the worktree.
- Verification: `ADMIN_KEY=dsm-admin-515 python -m py_compile app/main.py app/models.py scripts/qa_public_site.py` passed; `python scripts/qa_public_site.py` passed; local parser, fuzzy match, TestClient, and Browser checks passed; Browser console warning/error count was `0`; screenshot capture not verified due `Page.captureScreenshot` timeout; `npx vercel@latest --yes` produced preview `https://dsm-deals-2tpavbgod-ccr-515s-projects.vercel.app`; preview review for `Lua Brewing has $5 burgers every Tuesday from 4 PM to 6 PM.` contained `Lua Brewing`, `$5 burgers`, `Tue`, and `16:00 to 18:00`; preview edit page contained the required edit fields; preview public routes returned `200`; preview admin routes returned `401` without auth and `200` with `x-admin-key`; preview test submissions `3` and `4` were rejected after verification; `npm run build` is not applicable because there is no `package.json`.
- Current project status: Admin rules intake is improved and verified, with preview deployed for review. Production was not deployed.
- Next recommended build: Fix `/admin/venues?q=Lua` internal server error, then add a venue-create shortcut or clearer missing-venue path from review.
- Suggested dashboard update: status `active`, blocked `false`, blocker reason ``, confidence `91`, portfolio readiness `4`, money potential `4`, maintenance burden `4`.
# 2026-06-16 - Admin Venue Lookup And Intake Venue Attach

- Project name: DSM Deals Hub
- Phase: Admin deal intake
- Task name: Fix preview admin venue lookup and create/select venue before approval
- Date: 2026-06-16
- Current project status: Implemented, locally verified, and deployed to preview only. Production was not deployed.
- Changed files: `app/main.py`, `docs/status/latest-handoff.md`, `docs/status/handoff-log.md`, `local-data/project-status.json`
- Preview URL: `https://dsm-deals-og0tzm9td-ccr-515s-projects.vercel.app`
- Preview deployment: `dpl_5GQQjcNVVtCUQ1qieEHvppZxPMr5`

## What Works

- `/admin/venues` still requires admin auth.
- `/admin/venues?q=Lua` no longer returns `500` when preview lacks the `business_owners` table.
- `/admin/venues` returns safe JSON directly, with `[]` when no match exists and venue objects when matches exist.
- Owner names are best-effort only; missing `business_owners` is logged and does not break venue lookup.
- Review edit mode makes missing venues obvious with `No matching venue found`.
- Review edit mode supports picking an existing venue or creating a venue with name, address, and neighborhood.
- Creating an intake venue on Postgres uses a minimal SQL insert that avoids the preview `hours_json` jsonb/type mismatch.
- Approval remains blocked until a real venue is attached.
- Approved test deals can still be archived; no hard delete behavior was added.
- `LLM_PROVIDER=rules` remains in place.
- No Ollama or paid LLM API work was added.

## What Remains Placeholder

- The new venue-create path is minimal: name, address, and neighborhood only.
- There is still no dedicated venue management page for richer fields like phone, website, hours, lat/lng, or description.
- Preview now has a `Lua Brewing` venue created through the admin intake flow for verification.

## Broken Or Risky

- Preview/Supabase schema still differs from the ORM in at least two places: `business_owners` is absent and `venues.hours_json` is jsonb.
- The admin path works around those schema differences, but a proper schema reconciliation/migration is still recommended before production.
- Browser UI automation invocation timed out during login in this run, so rendered verification used TestClient and preview HTML/API checks instead of a complete Browser interaction pass.
- Existing unrelated modified files remain in the worktree.

## Verification

- `ADMIN_KEY=dsm-admin-515 LLM_PROVIDER=rules python -m py_compile app/main.py app/models.py scripts/qa_public_site.py` passed.
- `python scripts/qa_public_site.py` passed.
- Local `/admin/venues?q=Lua` returned `401` without auth and `200` with JSON when authenticated.
- Local intake flow verified missing venue blocks approval, edit mode can create/attach `Lua Brewing`, approval creates a draft deal, and archive marks it archived.
- Browser invocation was attempted but timed out on the login click; Browser smoke check not verified for this run.
- `npx vercel@latest --yes` completed preview-only deploy and remote build.
- Preview `/admin/venues?q=Lua` returned `200 []` before venue creation and `401` without auth.
- Preview intake/edit created `Lua Brewing` as venue `81` with address `1525 High St, Des Moines, IA` and neighborhood `Sherman Hill`.
- Preview review after create/select showed `Lua Brewing`, `$5 burgers`, `16:00 to 18:00`, no approval block, and enabled Approve.
- Preview approve redirected to `/admin/deals`.
- Preview created draft deal `153`; archive returned `303`; archived deal page showed `Lua Brewing`, `$5 burgers`, and archived status.
- Preview `/admin/venues?q=Lua` now returns the `Lua Brewing` venue JSON with `deal_count: 1` and `live_deal_count: 0`.
- Preview public route sweep returned `200` for `/`, `/today`, `/days`, `/days/monday`, `/neighborhoods`, `/neighborhoods/downtown`, and `/for-venues`.
- Preview admin routes returned `401` without auth and `200` with `x-admin-key`.
- Current preview error logs returned `No logs found` after verification.
- `npm run build` is not applicable because there is no `package.json`.

## Next Recommended Build

- Reconcile preview/Supabase schema with the ORM before production, especially `business_owners` and `venues.hours_json`.

## Suggested Dashboard Update

- Status: active
- Blocked status: false
- Blocker reason:
- Confidence score: 92
- Portfolio readiness: 4
- Money potential: 4
- Maintenance burden: 4

# 2026-06-16 - Restore Public Data Flow After UI Polish

- Project name: DSM Deals Hub
- Phase: Public data flow recovery
- Task name: Keep new UI while restoring Supabase/live-deals output flow
- Date: 2026-06-16
- Current project status: Fixed and verified on preview only. Production was not deployed.
- Changed files: `app/main.py`, `app/weekly_master_content.py`, `data/dsm_deals_hub_master_weekly_list.json`, `data/dsm_deals_hub_master_weekly_list.csv`, `docs/status/latest-handoff.md`, `docs/status/handoff-log.md`, `local-data/project-status.json`
- Preview URL: `https://dsm-deals-3tssp19xw-ccr-515s-projects.vercel.app`
- Preview deployment: `dpl_CyVr2CgqCdgZNqbzBW9yhThR19bC`

## What Works

- The new liquid-glass UI/CSS remains in place.
- Public Days and Neighborhoods are restored to the stable curated weekly-master guide flow.
- Homepage Live Now and Today still use dynamic DB/Supabase-backed live deal data.
- DB/admin-created live deals now get render-time display metadata so public pages do not crash when `notes_private` lacks weekly-master fields.
- Weekly-master JSON and CSV are now bundled under `data/` so Vercel does not try to read `/Users/camilorodriguez/Downloads/...`.
- Weekly-master path resolution uses repo-bundled files first, with env overrides and local Downloads fallback still available.
- Admin auth and admin venue lookup remain protected and working.
- Preview public routes returned `200` for `/`, `/today`, `/days`, `/days/monday`, `/neighborhoods`, `/neighborhoods/downtown`, and `/for-venues`.

## What Remains Placeholder

- Browser smoke check was not performed in this run.
- Static exported HTML was not regenerated; runtime preview was verified.

## Broken Or Risky

- Preview/Supabase schema drift noted earlier still exists for `business_owners` and `venues.hours_json`.
- The bundled weekly-master files should become the maintained source of truth or be replaced by a real import pipeline before production hardening.
- Existing unrelated admin-intake modified files remain in the worktree.

## Verification

- `ADMIN_KEY=dsm-admin-515 LLM_PROVIDER=rules python -m py_compile app/main.py app/models.py app/weekly_master_content.py scripts/qa_public_site.py` passed.
- `python scripts/qa_public_site.py` passed.
- Targeted local flow check passed: a temporary live DB deal appeared on Homepage and Today, did not appear in Days/Neighborhoods curated guide, and Days/Neighborhood counts matched weekly-master counts.
- Local weekly-master resolver confirmed repo paths under `data/`.
- `npx vercel@latest --yes` completed preview-only deploy and remote build.
- Preview route sweep returned `200` for the requested public routes.
- Preview admin auth sweep returned `401` without auth and `200` with `x-admin-key` for `/admin/intake`, `/admin/review`, `/admin/deals`, and `/admin/venues?q=Lua`.
- Preview homepage contained `Live Now`, `section-panel-live`, and `DSM Deals Hub`, with no internal server error.
- Preview error logs returned `No logs found` after verification.
- Browser smoke check not verified.
- `npm run build` is not applicable because there is no `package.json`.

## Next Recommended Build

- Reconcile Supabase schema drift before production, then decide whether the bundled weekly-master files or Supabase should become the single public guide source.

## Suggested Dashboard Update

- Status: active
- Blocked status: false
- Blocker reason:
- Confidence score: 93
- Portfolio readiness: 4
- Money potential: 4
- Maintenance burden: 4

# 2026-06-16 - Supabase ORM Schema Reconciliation

- Project name: DSM Deals Hub
- Phase: Pre-production schema hardening
- Task name: Reconcile Supabase schema with ORM before production
- Date: 2026-06-16
- Current project status: Fixed, migrated on preview, and verified preview-only. Production was not deployed.
- Changed files: `app/database.py`, `app/main.py`, `app/migrations.py`, `app/models.py`, `app/schemas.py`, `docs/schema-reconciliation-plan.md`, `docs/status/latest-handoff.md`, `docs/status/handoff-log.md`, `local-data/project-status.json`
- Preview URL: `https://dsm-deals-ktsrd93iv-ccr-515s-projects.vercel.app`
- Preview deployment: `dpl_HL7sUHktmJjBT15GBm5QH6iz1GYs`

## What Works

- The ORM now maps `venues.hours_json` to Postgres `jsonb` through SQLAlchemy JSON/JSONB, while keeping local SQLite compatibility.
- Venue create/update schemas accept structured JSON, parse JSON strings when possible, and still tolerate legacy string input.
- Postgres startup migrations now create/repair `business_owners`, `deal_intake_submissions`, `deal_change_log`, `events_metrics`, expected deal source/audit columns, expected venue columns, and relationship constraints additively.
- Postgres migrations do not drop data.
- `venues.hours_json` is converted to `jsonb` only when safe.
- Admin intake venue creation now uses normal ORM writes again instead of the raw SQL workaround.
- Postgres connections disable psycopg prepared statements to work cleanly with Supabase/PgBouncer in Vercel.
- Protected `/admin/schema-debug` confirms the preview schema without exposing secrets or data.
- Preview schema confirms `business_owners` exists, intake/log tables exist, and `venues.hours_json` is `jsonb`.
- Preview `/admin/venues?q=Lua` returns `200` and includes Lua Brewing.
- Preview review/edit/create venue, approve, and archive flow works.
- Archive removed the live preview test deal from public output.
- Public preview routes returned `200`.

## What Remains Placeholder

- There is still no full venue-management UI for richer owner/venue details.
- The schema migration runs at app startup for now; a dedicated one-shot migration command would be cleaner before a larger production database.
- Static exported HTML was not regenerated in this task.

## Broken Or Risky

- The first schema preview failed with `psycopg.errors.DuplicatePreparedStatement`; this was fixed by disabling psycopg prepared statements for Postgres connections, and the replacement preview has clean logs.
- Existing unrelated/uncommitted admin intake and public-flow changes remain in the worktree.
- Browser smoke check was not performed in this run; verification used local TestClient, QA script, Vercel curl, and Vercel logs.

## Verification

- `ADMIN_KEY=dsm-admin-515 LLM_PROVIDER=rules python -m py_compile app/database.py app/main.py app/models.py app/schemas.py app/migrations.py scripts/qa_public_site.py` passed.
- `python scripts/qa_public_site.py` passed.
- Local admin flow verified create/select venue, approve only after valid venue, live public visibility, archive, public removal, and `/admin/schema-debug`.
- `npx vercel@latest --yes` completed preview-only deploy and remote build.
- Preview route sweep returned `200` for `/`, `/today`, `/days`, `/days/monday`, `/neighborhoods`, `/neighborhoods/downtown`, and `/for-venues`.
- Preview admin routes returned `401` without auth and `200` with `x-admin-key` for `/admin/intake`, `/admin/review`, `/admin/deals`, `/admin/venues?q=Lua`, and `/admin/schema-debug`.
- Preview `/admin/schema-debug` reported dialect `postgresql`, `business_owners` exists, `deal_intake_submissions` exists, `deal_change_log` exists, and `venues.hours_json` has `udt_name: jsonb`.
- Preview admin flow created submission `7`, created/attached venue, approved live test deal `154`, confirmed it appeared publicly, archived it, and confirmed it disappeared from public.
- Preview error logs after verification returned `No logs found`.
- Browser smoke check not verified.
- `npm run build` is not applicable because there is no `package.json`.

## Next Recommended Build

- Convert the startup migration into a deliberate migration command/runbook before production promotion, then add a richer venue management screen when owner data becomes useful.

## Suggested Dashboard Update

- Status: active
- Blocked status: false
- Blocker reason:
- Confidence score: 94
- Portfolio readiness: 4
- Money potential: 4
- Maintenance burden: 5

# 2026-06-16 - Logo Shadow Polish

- Project name: DSM Deals Hub
- Phase: Frontend visual polish
- Task name: Remove tinted logo background and replace with soft transparent shadow
- Date: 2026-06-16
- Current project status: Fixed and verified on preview only. Production was not deployed.
- Changed files: `app/static/styles.css`, `docs/static/styles.css`, `docs/status/latest-handoff.md`, `docs/status/handoff-log.md`, `local-data/project-status.json`
- Preview URL: `https://dsm-deals-57yzolz0c-ccr-515s-projects.vercel.app`
- Preview deployment: `dpl_HqCnUG2M8TJSroXoHAtCazH6L6tG`

## What Works

- The public logo wrapper is transparent.
- Brand image pseudo-elements are disabled for the logo treatment.
- The logo wrapper has no background image, no background color, no wrapper box shadow, and no wrapper filter.
- The logo image itself now carries a two-layer neutral drop shadow: one tighter shadow close to the artwork and one broader softer shadow farther out.
- Hover/focus states keep the same transparent wrapper and use slightly stronger neutral image shadows.
- The logo artwork/colors were not changed.
- The change is scoped to public brand image rules in the header/branding area.
- Public preview routes returned `200`.
- Admin auth smoke checks still returned `401` without auth and `200` with `x-admin-key`.

## What Remains Placeholder

- Browser screenshot capture timed out in the in-app browser, so screenshot evidence is not available.
- Static HTML was not regenerated; static CSS was updated to match runtime CSS.

## Broken Or Risky

- Browser viewport override did not appear to take effect during the mobile check, so mobile visual verification is limited to shared responsive CSS and route checks.
- Existing unrelated/uncommitted admin intake and schema work remain in the worktree.

## Verification

- `ADMIN_KEY=dsm-admin-515 LLM_PROVIDER=rules python -m py_compile app/main.py app/database.py app/models.py app/schemas.py app/migrations.py scripts/qa_public_site.py` passed.
- `python scripts/qa_public_site.py` passed.
- Local Browser computed-style check on `/` confirmed the logo wrapper has transparent background, no pseudo overlays, no box shadow, `overflow: visible`, and the image has two neutral drop shadows.
- Browser console warning/error check returned no relevant logs.
- Browser screenshot capture not verified because `Page.captureScreenshot` timed out.
- `npx vercel@latest --yes` completed preview-only deploy and remote build.
- Preview route sweep returned `200` for `/`, `/today`, `/days`, `/days/monday`, `/neighborhoods`, `/neighborhoods/downtown`, and `/for-venues`.
- Preview admin smoke checks returned `401` without auth and `200` with `x-admin-key` for `/admin/intake` and `/admin/venues?q=Lua`.
- Preview deployed CSS contains the transparent logo wrapper and two-layer neutral image shadows.
- Preview error logs after verification returned `No logs found`.
- `npm run build` is not applicable because there is no `package.json`.

## Next Recommended Build

- Do a quick human visual pass on the preview homepage and, if desired, tune only the shadow blur/opacity values by taste.

## Suggested Dashboard Update

- Status: active
- Blocked status: false
- Blocker reason:
- Confidence score: 91
- Portfolio readiness: 4
- Money potential: 4
- Maintenance burden: 4
# 2026-06-16 - Final Pre-Production Preview Branch

Project name: DSM Deals Hub

Phase: Final pre-production preview

Task name: Combine public visual polish and admin intake into one preview branch

Date: 2026-06-16

Branch: `codex/dsm-deals-final-preprod`

Preview URL: `https://dsm-deals-dycaioe27-ccr-515s-projects.vercel.app`

Deployment ID: `dpl_GwrjUzyasb4FDZuDbfmoB7YYTKjp`

Changed files: `app/database.py`, `app/main.py`, `app/migrations.py`, `app/models.py`, `app/schemas.py`, `app/static/styles.css`, `app/weekly_master_content.py`, `data/dsm_deals_hub_master_weekly_list.csv`, `data/dsm_deals_hub_master_weekly_list.json`, `docs/schema-reconciliation-plan.md`, `docs/static/styles.css`, `docs/status/latest-handoff.md`, `docs/status/handoff-log.md`, `local-data/project-status.json`, `requirements.txt`, `scripts/qa_public_site.py`

What works: Public visual/logo polish is included. Public routes return 200. Admin login, cookie auth, header auth, auth debug, intake, review, edit, approve, archive, `/admin/venues`, rules parsing, and schema debug are verified in preview. The Lua Brewing test post detected the venue, Tuesday, `16:00`, and `18:00`. Approval was blocked for an invalid venue and succeeded after attaching real venue id `81`. The approved test deal appeared in admin deals and archive removed it from public pages. Vercel logs showed no new runtime errors.

What remains placeholder: Ollama remains intentionally unimplemented. Production env vars were not changed. A deliberate migration runbook should be used before production promotion.

What is broken or risky: Production promotion is still risky until production env scopes and the migration runbook are confirmed. The release candidate combines broad public/admin/schema/data changes and needs final owner review before production.

Current project status: Final preview is deployed and verified; production has not been deployed.

Next recommended build: Commit/review the final pre-production branch, confirm production env vars and migration order, then make a separate production deployment decision.

Suggested dashboard update: DSM Deals Hub is final-preview ready, with public polish and admin intake combined. Production is intentionally held.

Confidence score: 94

Portfolio readiness: 5

Money potential: 4

Maintenance burden: 5

Blocked status: false

Blocker reason if any: None for preview. Production is gated by env and migration preflight.

Build/test verification: Python compile passed, `python scripts/qa_public_site.py` passed, Vercel preview build passed, local browser admin login smoke check passed, preview admin/public/intake/archive checks passed. `npm run build` not applicable because no `package.json` exists.
# 2026-06-16 - Admin Venue Create From Intake

Project name: DSM Deals Hub

Phase: Admin deal intake refinement

Task name: Add obvious create-and-attach venue path in admin review

Date: 2026-06-16

Branch: `codex/dsm-deals-final-preprod`

Preview URL: `https://dsm-deals-q7cpc7r00-ccr-515s-projects.vercel.app`

Deployment ID: `dpl_2nj6WLM5UW8aGVHc1m5i2oE5gExq`

Changed files: `app/main.py`, `app/static/styles.css`, `docs/status/latest-handoff.md`, `docs/status/handoff-log.md`, `local-data/project-status.json`

What works: Admin review edit mode now has an obvious `Create new venue` panel with name, address, neighborhood, phone, website, and notes. `Create venue and attach` creates a real venue and stores the selected venue id in the proposal. Exact existing venue names are reused and can receive missing optional fields. The Google Maps helper opens a Maps search and updates as the admin types. Existing approval blocking remains in place. Public routes still pass.

What remains placeholder: Google Maps is a helper link, not an automated importer. Automatic Maps extraction was not added because it requires a separate compliant provider/API decision.

What is broken or risky: Browser smoke check was not fully verified because the in-app browser password-entry path failed due unavailable virtual clipboard and the file fallback was blocked by browser policy. The admin must still copy verified Maps info into the form manually.

Current project status: Preview-only refinement is deployed and verified. Production was not deployed.

Next recommended build: Use preview admin review to add a missing venue manually; decide later whether an approved venue enrichment provider/API is worth adding.

Suggested dashboard update: DSM Deals Hub admin intake now supports creating and attaching missing venues during review. Preview verified, production held.

Confidence score: 92

Portfolio readiness: 5

Money potential: 4

Maintenance burden: 5

Blocked status: false

Blocker reason if any: None for the app. Automatic Google Maps import remains a deliberate future decision.

Build/test verification: Python compile passed, local FastAPI admin create-and-attach test passed, `python scripts/qa_public_site.py` passed, Vercel preview build passed, preview public routes returned `200`, preview admin routes returned `401` without auth and `200` with auth, preview edit page rendered the new venue creation controls, and Vercel error logs showed no new errors. `npm run build` not applicable because no `package.json` exists. Browser smoke check not fully verified because browser plugin login/file inspection were blocked.
