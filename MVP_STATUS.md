# DSM Deals Hub Status

## Current State

The public site and admin intake workflow are live on Vercel with Supabase/Postgres as the operational database.

## Working

- Public Homepage, Today, Days, and Neighborhoods
- DB-backed publishing across all public route families
- Admin login, intake, review, edit, approve, verify, freeze, and archive
- Rules-based structured proposal parsing
- Venue search and create/attach during intake
- Raw source retention and audit logging
- Weekly deal recheck and stale archive lifecycle
- Embedded venue submission form
- Focused operations contract tests

## Current Stack

- FastAPI and SQLAlchemy
- Supabase/Postgres production database
- Vercel Python deployment
- Server-rendered HTML and CSS
- `LLM_PROVIDER=rules`

## Current Priority

Operate the freshness workflow, reconcile the live release branch with GitHub, and monitor the first recheck cycle. Avoid another visual redesign until real usage identifies a concrete problem.
