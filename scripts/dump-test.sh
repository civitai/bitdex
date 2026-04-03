#!/usr/bin/env bash
# Local 6-phase dump test with 32GB RSS kill threshold.
# Usage: bash scripts/dump-test.sh
#
# Starts bitdex-server on port 3001, sends PUT /dumps for each phase,
# monitors RSS every 10s, kills if >32GB.

set -euo pipefail

PORT=3001
BASE_URL="http://localhost:${PORT}"
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DATA_DIR="${REPO_DIR}/data-dump-test"
STAGE_DIR="${REPO_DIR}/data/load_stage"
INDEX_CONFIG_DIR="${REPO_DIR}/deploy/configs"
MAX_RSS_BYTES=$((32 * 1024 * 1024 * 1024))  # 32GB in bytes
SERVER_PID=""
MONITOR_PID=""

cleanup() {
    echo "[cleanup] Stopping monitor and server..."
    [ -n "$MONITOR_PID" ] && kill "$MONITOR_PID" 2>/dev/null || true
    [ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2>/dev/null || true
    wait 2>/dev/null || true
    echo "[cleanup] Done."
}
trap cleanup EXIT

# ── 0. Clean slate ──────────────────────────────────────────────────
echo "=== Cleaning data dir: $DATA_DIR ==="
rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"

# ── 1. Start server ────────────────────────────────────────────────
echo "=== Starting bitdex-server on port $PORT ==="
"${REPO_DIR}/target/release/bitdex-server.exe" \
    --port "$PORT" \
    --data-dir "$DATA_DIR" \
    --index-dir "$INDEX_CONFIG_DIR" \
    2>&1 | tee "$DATA_DIR/server.log" &
SERVER_PID=$!
echo "Server PID: $SERVER_PID"

# Wait for server to be ready
echo "Waiting for server..."
for i in $(seq 1 60); do
    if curl -s "$BASE_URL/health" > /dev/null 2>&1; then
        echo "Server ready after ${i}s"
        break
    fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "ERROR: Server died during startup. Check $DATA_DIR/server.log"
        exit 1
    fi
    sleep 1
done

# Verify index was created
echo "=== Checking index status ==="
curl -s "$BASE_URL/api/indexes" | head -200
echo ""

# ── 2. RSS monitor (background) ───────────────────────────────────
monitor_rss() {
    local peak_rss=0
    while kill -0 "$SERVER_PID" 2>/dev/null; do
        # Windows: use tasklist to get memory (Working Set in KB)
        local mem_kb
        mem_kb=$(tasklist //FI "PID eq $SERVER_PID" //FO CSV //NH 2>/dev/null \
            | tr -d '"' | awk -F',' '{gsub(/[^0-9]/,"",$NF); print $NF}' 2>/dev/null || echo "0")

        if [ "$mem_kb" = "0" ] || [ -z "$mem_kb" ]; then
            # Fallback: try powershell
            mem_kb=$(powershell -NoProfile -Command "(Get-Process -Id $SERVER_PID -ErrorAction SilentlyContinue).WorkingSet64 / 1KB" 2>/dev/null | tr -d '\r' || echo "0")
        fi

        local mem_bytes=$((mem_kb * 1024))
        local mem_gb=$(awk "BEGIN {printf \"%.2f\", $mem_bytes / 1073741824}")

        if [ "$mem_bytes" -gt "$peak_rss" ]; then
            peak_rss=$mem_bytes
        fi

        local peak_gb=$(awk "BEGIN {printf \"%.2f\", $peak_rss / 1073741824}")
        local ts=$(date +%H:%M:%S)
        echo "[$ts] RSS: ${mem_gb}GB (peak: ${peak_gb}GB)"

        if [ "$mem_bytes" -gt "$MAX_RSS_BYTES" ]; then
            echo "!!!! RSS ${mem_gb}GB EXCEEDS ${MAX_RSS_BYTES} bytes (32GB) — KILLING SERVER !!!!"
            kill "$SERVER_PID"
            echo "OOM_KILLED" > "$DATA_DIR/result.txt"
            echo "peak_rss_bytes=$peak_rss" >> "$DATA_DIR/result.txt"
            exit 1
        fi

        sleep 10
    done
    echo "peak_rss_bytes=$peak_rss" >> "$DATA_DIR/result.txt"
}
monitor_rss &
MONITOR_PID=$!

# ── 3. Convert Windows paths ──────────────────────────────────────
# The server runs on Windows, so CSV paths need Windows-style absolute paths
ABS_STAGE_DIR=$(cd "$STAGE_DIR" && pwd -W 2>/dev/null || pwd)

# ── 4. Send dump requests (sequential) ────────────────────────────
send_dump() {
    local name="$1"
    local json="$2"
    echo ""
    echo "=== Phase: $name ==="
    echo "Sending PUT /api/indexes/civitai/dumps ..."

    local response
    response=$(curl -s -w "\n%{http_code}" -X PUT \
        "$BASE_URL/api/indexes/civitai/dumps" \
        -H "Content-Type: application/json" \
        -d "$json")

    local http_code=$(echo "$response" | tail -1)
    local body=$(echo "$response" | head -n -1)
    echo "HTTP $http_code: $body"

    if [ "$http_code" != "200" ] && [ "$http_code" != "201" ] && [ "$http_code" != "202" ]; then
        echo "ERROR: Dump registration failed for $name"
        return 1
    fi

    # Poll for completion
    echo "Polling for completion..."
    local start_time=$(date +%s)
    while true; do
        local status_resp
        status_resp=$(curl -s "$BASE_URL/api/indexes/civitai/dumps" 2>/dev/null)
        local phase_status=$(echo "$status_resp" | python3 -c "
import sys, json
data = json.load(sys.stdin)
dumps = data.get('dumps', {})
for k, v in dumps.items():
    if k.startswith('$name'):
        print(v.get('status', 'unknown'))
        sys.exit(0)
print('not_found')
" 2>/dev/null || echo "unknown")

        local elapsed=$(( $(date +%s) - start_time ))
        echo "  [$name] status=$phase_status elapsed=${elapsed}s"

        if [ "$phase_status" = "Complete" ]; then
            echo "  [$name] COMPLETE in ${elapsed}s"
            break
        elif [ "$phase_status" = "Failed" ]; then
            echo "  [$name] FAILED after ${elapsed}s"
            return 1
        fi

        sleep 5
    done
}

# Phase 1: Images (14GB, sets_alive, with enrichment)
send_dump "images" '{
  "name": "images",
  "csv_path": "'"$ABS_STAGE_DIR/images.csv"'",
  "format": "csv",
  "slot_field": "id",
  "sets_alive": true,
  "fields": [
    "nsfwLevel",
    {"column": "type", "target": "type"},
    "userId",
    "postId",
    "blockedFor",
    {"column": "url", "target": "url"},
    {"column": "hash", "target": "hash"},
    "width",
    "height"
  ],
  "computed_fields": [
    {"target": "hasMeta", "expression": "(flags >> 13) & 1 == 1 && (flags >> 2) & 1 == 0"},
    {"target": "onSite", "expression": "(flags >> 14) & 1 == 1"},
    {"target": "minor", "expression": "(flags >> 3) & 1 == 1"},
    {"target": "poi", "expression": "(flags >> 4) & 1 == 1"},
    {"target": "existedAt", "expression": "max(scannedAtSecs, createdAtSecs)"},
    {"target": "id", "expression": "id"}
  ],
  "enrichment": [
    {
      "csv_path": "'"$ABS_STAGE_DIR/posts.csv"'",
      "key": "id",
      "join_on": "postId",
      "fields": [
        {"column": "publishedAtSecs", "target": "publishedAt"},
        {"column": "availability", "target": "availability"}
      ],
      "computed_fields": [
        {"target": "postedToId", "expression": "lookup_key"},
        {"target": "isPublished", "expression": "publishedAtSecs != null"}
      ]
    }
  ]
}'

# Phase 2: Tags (63GB)
send_dump "tags" '{
  "name": "tags",
  "csv_path": "'"$ABS_STAGE_DIR/tags.csv"'",
  "format": "csv",
  "slot_field": "imageId",
  "fields": [
    {"column": "tagId", "target": "tagIds"}
  ],
  "filter": "(attributes >> 10) & 1 = 0"
}'

# Phase 3: Resources (820MB, with nested enrichment)
send_dump "resources" '{
  "name": "resources",
  "csv_path": "'"$ABS_STAGE_DIR/resources.csv"'",
  "format": "csv",
  "slot_field": "imageId",
  "fields": [
    {"column": "modelVersionId", "target": "modelVersionIds"}
  ],
  "computed_fields": [
    {"target": "modelVersionIdsManual", "expression": "detected == false", "value": "modelVersionId"}
  ],
  "enrichment": [
    {
      "csv_path": "'"$ABS_STAGE_DIR/model_versions.csv"'",
      "key": "id",
      "join_on": "modelVersionId",
      "fields": [
        {"column": "baseModel", "target": "baseModel"}
      ],
      "enrichment": [
        {
          "csv_path": "'"$ABS_STAGE_DIR/models.csv"'",
          "key": "id",
          "join_on": "modelId",
          "fields": [
            {"column": "poi", "target": "poi"}
          ],
          "filter": "type = '\''Checkpoint'\''"
        }
      ]
    }
  ]
}'

# Phase 4: Tools (50MB)
send_dump "tools" '{
  "name": "tools",
  "csv_path": "'"$ABS_STAGE_DIR/tools.csv"'",
  "format": "csv",
  "slot_field": "imageId",
  "fields": [
    {"column": "toolId", "target": "toolIds"}
  ]
}'

# Phase 5: Techniques (71MB)
send_dump "techniques" '{
  "name": "techniques",
  "csv_path": "'"$ABS_STAGE_DIR/techniques.csv"'",
  "format": "csv",
  "slot_field": "imageId",
  "fields": [
    {"column": "techniqueId", "target": "techniqueIds"}
  ]
}'

# Phase 6: Metrics (1.4GB TSV)
send_dump "metrics" '{
  "name": "metrics",
  "csv_path": "'"$ABS_STAGE_DIR/metrics.tsv"'",
  "format": "tsv",
  "slot_field": "imageId",
  "fields": ["reactionCount", "commentCount", "collectedCount"]
}'

# ── 5. Final status ──────────────────────────────────────────────
echo ""
echo "=== ALL PHASES COMPLETE ==="
echo "PASS" > "$DATA_DIR/result.txt"

# Get final stats
curl -s "$BASE_URL/api/indexes/civitai/stats" | python3 -m json.tool 2>/dev/null || true
echo ""
echo "=== Dump test finished ==="
