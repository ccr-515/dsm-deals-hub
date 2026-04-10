#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [ -f dsm_deals.db ]; then
  rm dsm_deals.db
  echo "Deleted dsm_deals.db"
else
  echo "No dsm_deals.db found"
fi

echo "Database reset complete"
echo "Restart the API and rerun ./scripts/test_api.sh"
