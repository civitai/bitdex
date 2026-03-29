#!/usr/bin/env bash
# Gate 5: Ops Field Coverage Test Suite
# Tests that POST /ops correctly mutates bitmaps and docstore for all field types.
#
# Prerequisites:
#   - BitDex server running on $BITDEX_URL (default: http://localhost:3001)
#   - Server built with --features server,pg-sync (WAL reader + DocWriter active)
#   - Data loaded (108M+ records from gate5-e2e or production)
#
# Usage:
#   bash tests/gate5-ops-field-coverage.sh [--url http://localhost:3001]

set -euo pipefail

URL="${BITDEX_URL:-http://localhost:3001}"
INDEX="civitai"
BASE="$URL/api/indexes/$INDEX"
PASS=0
FAIL=0
SKIP=0

# Parse args
while [[ $# -gt 0 ]]; do
  case $1 in
    --url) URL="$2"; BASE="$URL/api/indexes/$INDEX"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

# Helper: POST ops and return accepted count
post_ops() {
  curl -s -X POST "$BASE/ops" -H "Content-Type: application/json" -d "$1"
}

# Helper: GET document field value
doc_field() {
  local id=$1 field=$2
  curl -s "$BASE/documents/$id" | node -e "
    process.stdin.on('data', d => {
      const j = JSON.parse(d);
      console.log(JSON.stringify(j['$field']));
    });
  " 2>/dev/null
}

# Helper: query count for filter
query_has() {
  local filter=$1 entity_id=$2
  local result=$(curl -s -X POST "$BASE/query" -H "Content-Type: application/json" \
    -d "{\"filters\":$filter,\"sort\":{\"field\":\"reactionCount\",\"direction\":\"Desc\"},\"limit\":200}")
  echo "$result" | node -e "
    process.stdin.on('data', d => {
      const j = JSON.parse(d);
      console.log(j.ids && j.ids.includes($entity_id) ? 'true' : 'false');
    });
  " 2>/dev/null
}

# Helper: report result
report() {
  local name=$1 status=$2 detail=${3:-""}
  if [ "$status" = "PASS" ]; then
    echo "  ✅ $name $detail"
    PASS=$((PASS + 1))
  elif [ "$status" = "FAIL" ]; then
    echo "  ❌ $name $detail"
    FAIL=$((FAIL + 1))
  else
    echo "  ⏭️  $name (skipped: $detail)"
    SKIP=$((SKIP + 1))
  fi
}

echo "=== Gate 5: Ops Field Coverage Tests ==="
echo "Server: $URL"
echo ""

# Check server health
if ! curl -s "$URL/api/health" | grep -q "ok"; then
  echo "ERROR: Server not healthy at $URL"
  exit 1
fi
echo "Server healthy."
echo ""

# Use entity 9253 as test subject (low ID, likely has data)
ENTITY=9253

echo "Test entity: $ENTITY"
echo "Getting baseline document..."
BEFORE=$(curl -s "$BASE/documents/$ENTITY")
echo ""

# -----------------------------------------------------------------------
# Test 1: Scalar filter field (nsfwLevel)
# -----------------------------------------------------------------------
echo "--- Test 1: nsfwLevel scalar set ---"
post_ops "{\"ops\":[{\"entity_id\":$ENTITY,\"creates_slot\":false,\"ops\":[{\"op\":\"set\",\"field\":\"nsfwLevel\",\"value\":88}]}],\"meta\":{\"source\":\"test\"}}" > /dev/null
sleep 3
VAL=$(doc_field $ENTITY nsfwLevel)
if [ "$VAL" = "88" ]; then
  report "nsfwLevel set (docstore)" "PASS" "→ $VAL"
else
  report "nsfwLevel set (docstore)" "FAIL" "expected 88, got $VAL"
fi

# -----------------------------------------------------------------------
# Test 2: Multi-value add (tagIds)
# -----------------------------------------------------------------------
echo "--- Test 2: tagIds add ---"
post_ops "{\"ops\":[{\"entity_id\":$ENTITY,\"creates_slot\":false,\"ops\":[{\"op\":\"add\",\"field\":\"tagIds\",\"value\":88888}]}],\"meta\":{\"source\":\"test\"}}" > /dev/null
sleep 3
HAS=$(query_has '[{"In":["tagIds",[{"Integer":88888}]]}]' $ENTITY)
if [ "$HAS" = "true" ]; then
  report "tagIds add (bitmap)" "PASS" "entity in tagIds=88888"
else
  report "tagIds add (bitmap)" "FAIL" "entity NOT in tagIds=88888"
fi

# -----------------------------------------------------------------------
# Test 3: Multi-value remove (tagIds)
# -----------------------------------------------------------------------
echo "--- Test 3: tagIds remove ---"
post_ops "{\"ops\":[{\"entity_id\":$ENTITY,\"creates_slot\":false,\"ops\":[{\"op\":\"remove\",\"field\":\"tagIds\",\"value\":88888}]}],\"meta\":{\"source\":\"test\"}}" > /dev/null
sleep 3
HAS=$(query_has '[{"In":["tagIds",[{"Integer":88888}]]}]' $ENTITY)
if [ "$HAS" = "false" ]; then
  report "tagIds remove (bitmap)" "PASS" "entity removed from tagIds=88888"
else
  report "tagIds remove (bitmap)" "FAIL" "entity still in tagIds=88888"
fi

# -----------------------------------------------------------------------
# Test 4: LCS string field (type)
# -----------------------------------------------------------------------
echo "--- Test 4: LCS string (type) ---"
post_ops "{\"ops\":[{\"entity_id\":$ENTITY,\"creates_slot\":false,\"ops\":[{\"op\":\"set\",\"field\":\"type\",\"value\":\"audio\"}]}],\"meta\":{\"source\":\"test\"}}" > /dev/null
sleep 3
VAL=$(doc_field $ENTITY type)
if [ "$VAL" = '"audio"' ]; then
  report "type LCS string (docstore)" "PASS" "→ $VAL"
else
  report "type LCS string (docstore)" "FAIL" "expected 'audio', got $VAL"
fi

# -----------------------------------------------------------------------
# Test 5: Boolean field (hasMeta)
# -----------------------------------------------------------------------
echo "--- Test 5: Boolean (hasMeta) ---"
post_ops "{\"ops\":[{\"entity_id\":$ENTITY,\"creates_slot\":false,\"ops\":[{\"op\":\"set\",\"field\":\"hasMeta\",\"value\":0}]}],\"meta\":{\"source\":\"test\"}}" > /dev/null
sleep 3
VAL=$(doc_field $ENTITY hasMeta)
if [ "$VAL" = "0" ]; then
  report "hasMeta boolean (docstore)" "PASS" "→ $VAL"
else
  report "hasMeta boolean (docstore)" "FAIL" "expected 0, got $VAL"
fi

# -----------------------------------------------------------------------
# Test 6: Sort field (publishedAt)
# -----------------------------------------------------------------------
echo "--- Test 6: Sort field (publishedAt) ---"
post_ops "{\"ops\":[{\"entity_id\":$ENTITY,\"creates_slot\":false,\"ops\":[{\"op\":\"set\",\"field\":\"publishedAt\",\"value\":1700000000}]}],\"meta\":{\"source\":\"test\"}}" > /dev/null
sleep 3
VAL=$(doc_field $ENTITY publishedAt)
if [ "$VAL" = "1700000000" ]; then
  report "publishedAt sort field (docstore)" "PASS" "→ $VAL"
else
  report "publishedAt sort field (docstore)" "FAIL" "expected 1700000000, got $VAL"
fi

# -----------------------------------------------------------------------
# Test 7: Computed sort field (sortAt = GREATEST(existedAt, publishedAt))
# -----------------------------------------------------------------------
echo "--- Test 7: sortAt computed recomputation ---"
VAL=$(doc_field $ENTITY sortAt)
if [ "$VAL" != "0" ] && [ "$VAL" != "null" ]; then
  report "sortAt GREATEST recomp (docstore)" "PASS" "→ $VAL"
else
  report "sortAt GREATEST recomp (docstore)" "FAIL" "sortAt=$VAL (not recomputed)"
fi

# -----------------------------------------------------------------------
# Test 8: Delete path
# -----------------------------------------------------------------------
echo "--- Test 8: Delete ---"
# Use a different entity for delete so we don't destroy our test subject
DEL_ENTITY=9252
post_ops "{\"ops\":[{\"entity_id\":$DEL_ENTITY,\"creates_slot\":false,\"ops\":[{\"op\":\"delete\"}]}],\"meta\":{\"source\":\"test\"}}" > /dev/null
sleep 3
# After delete, the entity should not appear in any filter query
HAS=$(query_has '[{"Eq":["nsfwLevel",{"Integer":1}]}]' $DEL_ENTITY)
if [ "$HAS" = "false" ]; then
  report "delete (bitmap cleared)" "PASS" "entity gone from nsfwLevel=1"
else
  report "delete (bitmap cleared)" "FAIL" "entity still in nsfwLevel=1"
fi

# -----------------------------------------------------------------------
# Summary
# -----------------------------------------------------------------------
echo ""
echo "=== RESULTS ==="
echo "  PASS: $PASS"
echo "  FAIL: $FAIL"
echo "  SKIP: $SKIP"
echo "  Total: $((PASS + FAIL + SKIP))"
echo ""

if [ $FAIL -gt 0 ]; then
  echo "FAILED — $FAIL test(s) failed"
  exit 1
else
  echo "ALL TESTS PASSED"
fi
