# Venue Directory

This folder contains the editable venue metadata source of truth for DSM Deals Hub.

## Files

- `venue_directory.json`
  The venue directory used by the public site.
- `venue_directory.py`
  The loader and matcher used by the app.
- `../scripts/build_venue_directory.py`
  Refreshes the unique venue list and preserves manual values.
- `../scripts/venue_directory_missing_report.py`
  Internal-only report for tracking missing enrichment fields.

## Venue Record Shape

Each venue record supports:

- `name`
- `slug`
- `aliases`
- `address`
- `phone`
- `website`
- `menu_url`
- `lat`
- `lng`

`address`, `phone`, `website`, and `menu_url` are the main manual enrichment fields.

## How To Enrich It

1. Run `python scripts/build_venue_directory.py` to refresh the unique venue list from the current weekly dataset.
2. Open `app/venue_directory.json`.
3. Fill in verified metadata only.
4. Leave unknown fields as `null`.
5. Re-run the build script later when the weekly dataset changes.

The build script preserves existing manual values when it rebuilds the file.

To see what still needs enrichment, run:

```bash
python scripts/venue_directory_missing_report.py
```

That report is internal only. It is not shown anywhere in the public UI.

## Matching Rules

Deals map to venue records by:

1. normalized slug
2. normalized venue name
3. known aliases

That matching is intentionally conservative so the site does not invent venue metadata.
