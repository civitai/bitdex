#!/usr/bin/env bash
# Benchmark diverse query patterns against local BitDex server.
# Usage: bash scripts/bench-queries.sh [count]
#
# Generates a mix of:
# - Feed queries (nsfwLevel=1, sort=reactionCount/sortAt)
# - Tag queries (tagIds=N, various sorts)
# - User queries (userId=N)
# - Multi-filter queries (nsfwLevel + type + tag)

BITDEX_URL="${BITDEX_URL:-http://localhost:3002}"
COUNT="${1:-100}"
RESULTS_FILE="/tmp/bench-results.txt"

> "$RESULTS_FILE"

echo "Running $COUNT diverse queries against $BITDEX_URL..."

sorts=("reactionCount" "sortAt" "commentCount" "collectedCount")

for i in $(seq 1 "$COUNT"); do
  r=$((RANDOM % 100))

  if [ $r -lt 30 ]; then
    # 30% feed queries (nsfwLevel=1 + isPublished, various sorts)
    sort_idx=$((RANDOM % 4))
    sort_field="${sorts[$sort_idx]}"
    filters='[{"Eq":["nsfwLevel",{"Integer":1}]},{"Eq":["isPublished",{"Bool":true}]}]'
    qtype="feed"
  elif [ $r -lt 60 ]; then
    # 30% tag queries (nsfwLevel + isPublished + tagIds)
    tag=$((RANDOM % 200 + 1))
    sort_idx=$((RANDOM % 2))
    sort_field="${sorts[$sort_idx]}"
    filters="[{\"Eq\":[\"nsfwLevel\",{\"Integer\":1}]},{\"Eq\":[\"isPublished\",{\"Bool\":true}]},{\"In\":[\"tagIds\",[{\"Integer\":${tag}}]]}]"
    qtype="tag=$tag"
  elif [ $r -lt 80 ]; then
    # 20% user queries
    user=$((RANDOM % 100000 + 1))
    sort_field="sortAt"
    filters="[{\"Eq\":[\"userId\",{\"Integer\":${user}}]}]"
    qtype="user=$user"
  else
    # 20% multi-filter (nsfwLevel + type + isPublished)
    type_val=$((RANDOM % 5 + 1))
    sort_idx=$((RANDOM % 2))
    sort_field="${sorts[$sort_idx]}"
    filters="[{\"Eq\":[\"nsfwLevel\",{\"Integer\":1}]},{\"Eq\":[\"isPublished\",{\"Bool\":true}]},{\"Eq\":[\"type\",{\"Integer\":${type_val}}]}]"
    qtype="type=$type_val"
  fi

  result=$(curl -s -X POST "${BITDEX_URL}/api/indexes/civitai/query" \
    -H "Content-Type: application/json" \
    -d "{\"filters\":${filters},\"sort\":{\"field\":\"${sort_field}\",\"direction\":\"Desc\"},\"limit\":20}")

  elapsed=$(echo "$result" | grep -o '"elapsed_us":[0-9]*' | grep -o '[0-9]*')
  matched=$(echo "$result" | grep -o '"total_matched":[0-9]*' | grep -o '[0-9]*')

  echo "${elapsed}" >> "$RESULTS_FILE"

  # Progress every 10 queries
  if [ $((i % 10)) -eq 0 ]; then
    echo "  $i/$COUNT done (last: ${qtype} sort=${sort_field} → ${elapsed}us, ${matched} matched)"
  fi
done

echo ""
echo "=== Results ==="
node -e "
const fs = require('fs');
const vals = fs.readFileSync('$RESULTS_FILE','utf8').trim().split('\n').map(Number).filter(n => !isNaN(n));
vals.sort((a,b) => a-b);
const n = vals.length;
const sum = vals.reduce((a,b) => a+b, 0);
console.log('Queries: ' + n);
console.log('P50:  ' + (vals[Math.floor(n*0.50)]/1000).toFixed(2) + 'ms');
console.log('P90:  ' + (vals[Math.floor(n*0.90)]/1000).toFixed(2) + 'ms');
console.log('P95:  ' + (vals[Math.floor(n*0.95)]/1000).toFixed(2) + 'ms');
console.log('P99:  ' + (vals[Math.floor(n*0.99)]/1000).toFixed(2) + 'ms');
console.log('Mean: ' + (sum/n/1000).toFixed(2) + 'ms');
console.log('Min:  ' + (vals[0]/1000).toFixed(2) + 'ms');
console.log('Max:  ' + (vals[n-1]/1000).toFixed(2) + 'ms');
const under1ms = vals.filter(x => x < 1000).length;
const under10ms = vals.filter(x => x < 10000).length;
console.log('Under 1ms:  ' + under1ms + '/' + n + ' (' + (under1ms/n*100).toFixed(0) + '%)');
console.log('Under 10ms: ' + under10ms + '/' + n + ' (' + (under10ms/n*100).toFixed(0) + '%)');
"
