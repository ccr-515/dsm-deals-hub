#!/usr/bin/env bash
set -euo pipefail

BASE_URL="http://127.0.0.1:8001"
ADMIN_KEY="changeme-admin-key"
STAMP=$(date +%s)

OWNER_NAME="Cam $STAMP"
OWNER_EMAIL="cam+$STAMP@example.com"
VENUE_NAME="Riverside Tap $STAMP"
VENUE_SLUG="riverside-tap-$STAMP"

echo ""
echo "1. Health check"
curl -s "$BASE_URL/health"
echo ""
echo ""

echo "2. Create owner"
OWNER_RESPONSE=$(curl -s -X POST "$BASE_URL/owners" \
  -H "Content-Type: application/json" \
  -d "{\"name\":\"$OWNER_NAME\",\"email\":\"$OWNER_EMAIL\"}")
echo "$OWNER_RESPONSE"
echo ""
echo ""

OWNER_ID=$(python3.12 -c 'import sys, json; print(json.load(sys.stdin)["id"])' <<< "$OWNER_RESPONSE")

echo "3. Create venue"
VENUE_RESPONSE=$(curl -s -X POST "$BASE_URL/venues" \
  -H "Content-Type: application/json" \
  -d "{
    \"owner_id\": $OWNER_ID,
    \"name\": \"$VENUE_NAME\",
    \"slug\": \"$VENUE_SLUG\",
    \"address\": \"123 Main St, Des Moines, IA\",
    \"neighborhood\": \"East Village\",
    \"lat\": 41.59,
    \"lng\": -93.605
  }")
echo "$VENUE_RESPONSE"
echo ""
echo ""

VENUE_ID=$(python3.12 -c 'import sys, json; print(json.load(sys.stdin)["id"])' <<< "$VENUE_RESPONSE")

echo "4. Create last minute deal"
START=$(python3.12 - <<'PY'
from datetime import datetime, UTC
print(datetime.now(UTC).replace(microsecond=0, tzinfo=None).isoformat())
PY
)

END=$(python3.12 - <<'PY'
from datetime import datetime, timedelta, UTC
print((datetime.now(UTC).replace(microsecond=0, tzinfo=None) + timedelta(hours=2)).isoformat())
PY
)

DEAL_RESPONSE=$(curl -s -X POST "$BASE_URL/deals/last-minute" \
  -H "Content-Type: application/json" \
  -d "{
    \"venue_id\": $VENUE_ID,
    \"title\": \"Tonight Special $STAMP\",
    \"short_description\": \"\$5 drafts for the next two hours\",
    \"start_at\": \"$START\",
    \"end_at\": \"$END\",
    \"age_21_plus\": true
  }")
echo "$DEAL_RESPONSE"
echo ""
echo ""

DEAL_ID=$(python3.12 -c 'import sys, json; print(json.load(sys.stdin)["id"])' <<< "$DEAL_RESPONSE")

echo "5. Approve deal"
curl -s -X POST "$BASE_URL/moderation/approve/$DEAL_ID" \
  -H "Content-Type: application/json" \
  -H "X-Admin-Key: $ADMIN_KEY" \
  -d '{"approve": true}'
echo ""
echo ""

echo "6. List venues"
curl -s "$BASE_URL/venues"
echo ""
echo ""

echo "7. Feed query"
curl -s "$BASE_URL/feed?neighborhood=East%20Village&lat=41.59&lng=-93.605"
echo ""
echo ""
