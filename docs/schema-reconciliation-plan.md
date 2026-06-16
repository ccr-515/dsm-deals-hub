# DSM Deals Hub Schema Reconciliation Plan

Date: 2026-06-16

## Goal

Make the Supabase/Postgres schema agree with the SQLAlchemy ORM before production, without dropping data and without relying on admin-route workarounds.

## Current ORM Expectations

- `business_owners` exists and is referenced by `venues.owner_id`.
- `venues.hours_json` is JSON-compatible and should map to Postgres `jsonb`.
- `deal_intake_submissions` exists for raw intake posts and parsed proposals.
- `deal_change_log` exists for all admin/intake audit events.
- Deals keep source/audit fields: `source_url`, `source_text`, `source_posted_at`, `notes_private`, and `freeze_minutes`.
- Admin/archive behavior never hard-deletes deals.

## Safe Migration Strategy

- Use additive DDL only for Postgres.
- Create missing tables with `CREATE TABLE IF NOT EXISTS`.
- Add missing columns with `ALTER TABLE ... ADD COLUMN` only when absent.
- Convert `venues.hours_json` to `jsonb` only when existing values are already valid JSON or the column is currently Postgres `json`.
- Skip unsafe `hours_json` conversion and log example venue ids if non-JSON text values exist.
- Add foreign keys only when existing child rows already have matching parent rows.
- Avoid duplicate foreign keys by detecting equivalent existing constraints, even if Supabase named them differently.
- Do not drop or truncate any production/preview data.

## Migration Coverage

### `business_owners`

- Create if missing.
- Columns: `id`, `name`, `email`, `phone`, `created_at`.
- Indexes: unique `email`, indexed `id`.

### `venues`

- Ensure columns expected by the ORM: `owner_id`, `name`, `slug`, `address`, `neighborhood`, `lat`, `lng`, `phone`, `website`, `hours_json`, `description`, `created_at`, `updated_at`.
- Ensure `hours_json` is `jsonb` on Postgres.
- Ensure `owner_id` can reference `business_owners.id`.
- Keep `owner_id` nullable.

### `deals`

- Ensure source/audit/admin columns: `source_type`, `source_url`, `source_text`, `source_posted_at`, `notes_private`, `freeze_minutes`, `created_at`, `updated_at`.
- Keep archive behavior as status updates, never hard deletes.

### Admin Intake Tables

- Ensure `deal_intake_submissions`.
- Ensure `deal_change_log`.
- Keep raw source text and audit records.

### Relationship Constraints

- `venues.owner_id -> business_owners.id`
- `deals.venue_id -> venues.id`
- `deal_change_log.deal_id -> deals.id`
- `deal_change_log.venue_id -> venues.id`
- `events_metrics.deal_id -> deals.id`

Constraints are only added when existing data is valid.

## Verification Plan

- Compile Python modules.
- Run `python scripts/qa_public_site.py`.
- Verify local admin intake/edit/create/approve/archive flow.
- Deploy preview only.
- Verify preview `/admin/schema-debug` shows `business_owners`, intake tables, and `venues.hours_json` as `jsonb`.
- Verify `/admin/venues?q=Lua` returns `200`.
- Verify review/edit/create venue works.
- Verify Approve requires a valid `venue_id`.
- Verify archive removes test live deal from public output.
- Verify public routes return `200`.
- Check Vercel runtime logs after tests.
