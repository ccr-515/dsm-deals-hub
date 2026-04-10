
# DSM Deals MVP (FastAPI + SQLite)

This is a minimal, runnable MVP for your Deals + Last-Call platform.

## Features in this MVP
- Business Owners, Venues, Deals (weekly & last-minute)
- Manual moderation (admin key)
- Freeze window baked into rules (for updates later)
- Public feed sorted by: expiring soon → neighborhood priority → distance → freshness
- Metrics: view/click/save/share endpoints
- Archive via status (live → expired)
- No maps; list-first
- SQLite DB for easy local dev

## Quickstart

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

uvicorn app.main:app --reload
```

Open: http://127.0.0.1:8000/docs

## Admin key
Change `ADMIN_KEY` in `app/config.py` before deploying.

## Example flow (cURL)

Create owner:
```bash
curl -X POST http://127.0.0.1:8000/owners -H "Content-Type: application/json" -d '{"name":"Cam","email":"cam@example.com"}'
```

Create venue:
```bash
curl -X POST http://127.0.0.1:8000/venues -H "Content-Type: application/json" -d '{
  "owner_id": 1, "name":"SkyBar","slug":"skybar","address":"123 Main St",
  "neighborhood":"East Village","lat":41.590,"lng":-93.605
}'
```

Post weekly deal (queued for approval):
```bash
curl -X POST http://127.0.0.1:8000/deals/weekly -H "Content-Type: application/json" -d '{
  "venue_id":1, "title":"Half-Off Apps","short_description":"50% off apps",
  "weekday_pattern":"Mon,Wed,Fri","start_time":"16:00","end_time":"19:00"
}'
```

Approve (admin):
```bash
curl -X POST "http://127.0.0.1:8000/moderation/approve/1" -H "X-Admin-Key: changeme-admin-key" -H "Content-Type: application/json" -d '{"approve":true}'
```

Create last-minute (3h window max):
```bash
START=$(python - <<'PY'
from datetime import datetime, timedelta
print((datetime.utcnow()).isoformat())
PY)
END=$(python - <<'PY'
from datetime import datetime, timedelta
print((datetime.utcnow()+timedelta(hours=2)).isoformat())
PY)

curl -X POST http://127.0.0.1:8000/deals/last-minute -H "Content-Type: application/json" -d "{
  \"venue_id\":1,\"title\":\"Tonight Special\",\"short_description\":\"$5 drafts\",
  \"start_at\":\"$START\",\"end_at\":\"$END\",\"age_21_plus\":true
}"
```

Approve last-minute:
```bash
curl -X POST "http://127.0.0.1:8000/moderation/approve/2" -H "X-Admin-Key: changeme-admin-key" -H "Content-Type: application/json" -d '{"approve":true}'
```

Public feed:
```
GET /feed?neighborhood=East%20Village&lat=41.59&lng=-93.605
```

## Notes
- Freeze logic will matter when you add update/edit endpoints; currently creation + approval paths respect durations and max windows.
- Add Stripe + PDF generator later.
- Scraper integration: add a worker that writes rows into `deals` with `source_type="scrape"` and run through moderation.
