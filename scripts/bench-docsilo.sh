#!/bin/bash
# Quick perf benchmark: fire many queries with varied filters, scrape metrics.
set -eu
PORT="${PORT:-3001}"
N="${N:-500}"
HOST="http://localhost:$PORT"

echo "Firing $N queries..."
start=$(date +%s.%N)
for i in $(seq 1 "$N"); do
    nsfw=$(( (i * 7) % 16 + 1 ))
    limit=$(( 50 + (i * 13) % 200 ))
    curl -sS -X POST "$HOST/api/indexes/civitai/query?format=compact" \
        -H "Content-Type: application/json" \
        -d "{\"filter\":{\"nsfwLevel\":$nsfw},\"sort\":\"-existedAt\",\"limit\":$limit,\"include_docs\":true}" \
        > /dev/null
done
end=$(date +%s.%N)
elapsed=$(awk "BEGIN {print $end - $start}")
qps=$(awk "BEGIN {printf \"%.1f\", $N / $elapsed}")
echo "  $N queries in ${elapsed}s = ${qps} QPS"
