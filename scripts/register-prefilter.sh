#!/usr/bin/env bash
# Register the Civitai safety prefix prefilter.
# Usage: BITDEX_ADMIN_TOKEN=test123 bash scripts/register-prefilter.sh

BITDEX_URL="${BITDEX_URL:-http://localhost:3002}"
TOKEN="${BITDEX_ADMIN_TOKEN:-test123}"
INDEX="civitai"

echo "Registering Civitai safety prefilter on ${BITDEX_URL}..."

# Base safety prefix: clauses shared by virtually every feed query.
# nsfwLevel varies per user, so NOT included here.
curl -s -X POST "${BITDEX_URL}/api/indexes/${INDEX}/prefilters" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${TOKEN}" \
  -d '{
    "name": "civitai_safe_base",
    "clauses": [
      {"NotEq": ["availability", {"Integer": 3}]},
      {"IsNull": "blockedFor"},
      {"Eq": ["isPublished", {"Bool": true}]}
    ],
    "refresh_interval_secs": 300
  }' | node -e "try{const d=JSON.parse(require('fs').readFileSync('/dev/stdin','utf8'));console.log(JSON.stringify(d,null,2))}catch(e){console.log('Parse error:',e.message)}"

echo ""
echo "Listing prefilters..."
curl -s "${BITDEX_URL}/api/indexes/${INDEX}/prefilters" \
  -H "Authorization: Bearer ${TOKEN}" | node -e "try{const d=JSON.parse(require('fs').readFileSync('/dev/stdin','utf8'));console.log(JSON.stringify(d,null,2))}catch(e){console.log('Parse error:',e.message)}"
