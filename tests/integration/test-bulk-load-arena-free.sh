#!/usr/bin/env bash
# Extended integration test for the arena-free bulk loader.
#
# Runs AFTER test-bulk-load.sh (or as a standalone with the same docker stack).
# Validates arena-free-specific behavior:
#   1. Document count matches expected from PG seed data
#   2. Tag filtering works (filter by tagId, verify results)
#   3. Multi-value fields are correctly reconstructed (include_docs shows tagIds)
#   4. Sort fields work (sort by id, verify ascending/descending order)
#   5. Cursor pagination works across bulk-loaded data
#   6. Doc-only scalar fields (url) are present in documents
#   7. Multi-value overflow (55 tags on image 10) is correctly handled
#
# Prerequisites: run-bulk.sh has started all containers, bulk load complete.
# Usage: bash test-bulk-load-arena-free.sh

set -euo pipefail

BITDEX_0="http://localhost:3011"
INDEX="test"
COMPOSE_FILE="docker-compose-bulk.yml"
PASS=0
FAIL=0

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m'

pass() { PASS=$((PASS + 1)); echo -e "  ${GREEN}PASS${NC}: $1"; }
fail() { FAIL=$((FAIL + 1)); echo -e "  ${RED}FAIL${NC}: $1"; }
info() { echo -e "${YELLOW}→${NC} $1"; }

bitdex_query() {
    local url="$1"
    local body="$2"
    curl -sf "$url/api/indexes/$INDEX/query" \
        -H 'Content-Type: application/json' \
        -d "$body" 2>/dev/null
}

# ===========================================================================
echo ""
echo "========================================"
echo "  Arena-Free Bulk Load Extended Tests"
echo "========================================"
echo ""

# --- Test 1: Document count matches seed data ---
info "Test 1: Document count from bulk load"

result=$(bitdex_query "$BITDEX_0" '{"filters":[],"limit":1}')
total=$(echo "$result" | jq -r '.total_matched // 0')

# Seed data has 10 images + potentially some from the earlier test-bulk-load.sh inserts
if [ "$total" -ge 10 ] 2>/dev/null; then
    pass "Document count >= 10 from bulk load (actual: $total)"
else
    fail "Document count too low: $total (expected >= 10)"
fi

# --- Test 2: Tag filtering works on bulk-loaded data ---
info "Test 2: Tag filtering (tagIds)"

# Seed data: images 1-3 have tagIds, image 1 has tags [100, 101, 102]
result=$(bitdex_query "$BITDEX_0" '{"filters":[{"In":["tagIds",[{"Integer":100}]]}],"limit":100}')
tag_count=$(echo "$result" | jq -r '.total_matched // 0')
tag_ids=$(echo "$result" | jq -r '.ids // []')

if [ "$tag_count" -ge 1 ] 2>/dev/null; then
    pass "Tag filter tagIds IN [100] returned $tag_count results"
else
    fail "Tag filter tagIds IN [100] returned $tag_count results (expected >= 1)"
fi

# Check that image 1 is in the results (it has tag 100)
has_id_1=$(echo "$result" | jq '.ids | map(select(. == 1)) | length')
if [ "$has_id_1" -ge 1 ] 2>/dev/null; then
    pass "Image 1 found in tagIds IN [100] results"
else
    fail "Image 1 NOT found in tagIds IN [100] results (ids: $tag_ids)"
fi

# --- Test 3: Multi-value fields reconstructed in documents ---
info "Test 3: Multi-value field reconstruction (include_docs)"

result=$(bitdex_query "$BITDEX_0" '{"filters":[{"Eq":["userId",{"Integer":100}]}],"limit":10,"include_docs":true}')
doc_count=$(echo "$result" | jq '.documents | length')

if [ "$doc_count" -ge 1 ] 2>/dev/null; then
    pass "include_docs returned $doc_count documents for userId=100"
else
    fail "include_docs returned $doc_count documents for userId=100 (expected >= 1)"
fi

# Check that tagIds array is present and non-empty on image 1
# Image 1 has tags [100, 101, 102]
tag_ids_on_doc=$(echo "$result" | jq '[.documents[] | select(.id == 1) | .tagIds | length] | .[0] // 0')
if [ "$tag_ids_on_doc" -ge 3 ] 2>/dev/null; then
    pass "Image 1 has $tag_ids_on_doc tags in document (expected >= 3)"
else
    fail "Image 1 has $tag_ids_on_doc tags in document (expected >= 3, reconstructed from bitmaps)"
fi

# --- Test 4: Sort fields work ---
info "Test 4: Sort field ordering (id sort)"

# Sort by id ascending
result_asc=$(bitdex_query "$BITDEX_0" '{"filters":[],"sort":{"field":"id","direction":"Asc"},"limit":5}')
first_asc=$(echo "$result_asc" | jq '.ids[0] // -1')

# Sort by id descending
result_desc=$(bitdex_query "$BITDEX_0" '{"filters":[],"sort":{"field":"id","direction":"Desc"},"limit":5}')
first_desc=$(echo "$result_desc" | jq '.ids[0] // -1')

if [ "$first_asc" -lt "$first_desc" ] 2>/dev/null; then
    pass "Sort works: Asc first=$first_asc < Desc first=$first_desc"
else
    fail "Sort ordering broken: Asc first=$first_asc, Desc first=$first_desc"
fi

# --- Test 5: Cursor pagination ---
info "Test 5: Cursor pagination across bulk-loaded data"

# Page 1: get first 3 results with cursor
page1=$(bitdex_query "$BITDEX_0" '{"filters":[],"sort":{"field":"id","direction":"Asc"},"limit":3}')
page1_ids=$(echo "$page1" | jq '.ids')
cursor=$(echo "$page1" | jq -r '.cursor // empty')

if [ -n "$cursor" ] && [ "$cursor" != "null" ]; then
    pass "Page 1 returned cursor for pagination"
else
    fail "Page 1 did not return a cursor (result: $(echo "$page1" | jq -c .))"
fi

if [ -n "$cursor" ] && [ "$cursor" != "null" ]; then
    # Page 2: use cursor
    page2=$(bitdex_query "$BITDEX_0" "{\"filters\":[],\"sort\":{\"field\":\"id\",\"direction\":\"Asc\"},\"limit\":3,\"cursor\":$cursor}")
    page2_ids=$(echo "$page2" | jq '.ids')
    page2_first=$(echo "$page2" | jq '.ids[0] // -1')
    page1_last=$(echo "$page1" | jq '.ids[-1] // -1')

    if [ "$page2_first" -gt "$page1_last" ] 2>/dev/null; then
        pass "Page 2 continues after page 1: page1_last=$page1_last, page2_first=$page2_first"
    else
        fail "Page 2 does not continue: page1_last=$page1_last, page2_first=$page2_first"
    fi

    # Verify no overlap between pages
    overlap=$(echo "$page1_ids $page2_ids" | jq -s '.[0] as $a | .[1] as $b | [$a[] as $x | $b[] | select(. == $x)] | length')
    if [ "$overlap" -eq 0 ] 2>/dev/null; then
        pass "No overlap between page 1 and page 2"
    else
        fail "Pages overlap: $overlap common IDs"
    fi
else
    fail "Skipping page 2 test — no cursor from page 1"
fi

# --- Test 6: Doc-only scalar fields present ---
info "Test 6: Doc-only scalars (url field)"

result=$(bitdex_query "$BITDEX_0" '{"filters":[{"Eq":["userId",{"Integer":100}]}],"limit":1,"include_docs":true}')
url_val=$(echo "$result" | jq -r '.documents[0].url // empty')

if [ -n "$url_val" ]; then
    pass "url field present in document: $url_val"
else
    fail "url field missing from document (doc-only scalar not stored)"
fi

# --- Test 7: Multi-value overflow handling (55 tags on image 10) ---
info "Test 7: Multi-value overflow (55 tags on image 10)"

result=$(bitdex_query "$BITDEX_0" '{"filters":[{"Eq":["userId",{"Integer":500}]}],"limit":10,"include_docs":true}')
# Image 10 has userId=500 and 55 tags (generate_series 1000..1054)
img10_tags=$(echo "$result" | jq '[.documents[] | select(.id == 10) | .tagIds | length] | .[0] // 0')

if [ "$img10_tags" -ge 55 ] 2>/dev/null; then
    pass "Image 10 has $img10_tags tags (55 expected, overflow handled correctly)"
else
    fail "Image 10 has $img10_tags tags (expected >= 55, overflow may be broken)"
fi

# Verify a specific tag from the overflow range is present
has_tag_1050=$(echo "$result" | jq '[.documents[] | select(.id == 10) | .tagIds | map(select(. == 1050)) | length] | .[0] // 0')
if [ "$has_tag_1050" -ge 1 ] 2>/dev/null; then
    pass "Image 10 has tag 1050 (mid-range of overflow tags)"
else
    fail "Image 10 missing tag 1050 from overflow range"
fi

# --- Test 8: Filter bitmap correctness (tag from overflow set) ---
info "Test 8: Filter bitmap for overflow tag value"

result=$(bitdex_query "$BITDEX_0" '{"filters":[{"In":["tagIds",[{"Integer":1025}]]}],"limit":100}')
overflow_count=$(echo "$result" | jq '.total_matched // 0')

if [ "$overflow_count" -ge 1 ] 2>/dev/null; then
    pass "Tag 1025 filter returns $overflow_count results (bitmap present)"
else
    fail "Tag 1025 filter returns $overflow_count results (bitmap missing for overflow tag)"
fi

# Verify image 10 is in the results
has_10=$(echo "$result" | jq '.ids | map(select(. == 10)) | length')
if [ "$has_10" -ge 1 ] 2>/dev/null; then
    pass "Image 10 in tag 1025 results"
else
    fail "Image 10 NOT in tag 1025 results"
fi

# --- Test 9: Combined filter + tag + sort ---
info "Test 9: Combined filter (nsfwLevel + tagIds) with sort"

# Images with nsfwLevel=1 AND tagIds IN [100]: should include image 1, image 2
result=$(bitdex_query "$BITDEX_0" '{"filters":[{"And":[{"Eq":["nsfwLevel",{"Integer":1}]},{"In":["tagIds",[{"Integer":100}]]}]}],"sort":{"field":"id","direction":"Asc"},"limit":100}')
combined_count=$(echo "$result" | jq '.total_matched // 0')

if [ "$combined_count" -ge 1 ] 2>/dev/null; then
    pass "Combined filter (nsfwLevel=1 AND tagIds IN [100]) returned $combined_count results"
else
    fail "Combined filter returned $combined_count results (expected >= 1)"
fi

# --- Test 10: Tool and technique bitmaps ---
info "Test 10: Tool and technique bitmaps"

# Seed: image 1 has toolId=10
result=$(bitdex_query "$BITDEX_0" '{"filters":[{"In":["toolIds",[{"Integer":10}]]}],"limit":100}')
tool_count=$(echo "$result" | jq '.total_matched // 0')

if [ "$tool_count" -ge 1 ] 2>/dev/null; then
    pass "toolIds IN [10] returned $tool_count results"
else
    fail "toolIds IN [10] returned $tool_count results (expected >= 1)"
fi

# ===========================================================================
echo ""
echo "========================================"
echo -e "  Results: ${GREEN}$PASS passed${NC}, ${RED}$FAIL failed${NC}"
echo "========================================"
echo ""

if [ "$FAIL" -gt 0 ]; then
    echo -e "${RED}Some arena-free tests failed!${NC}"
    echo ""
    echo "Debug commands:"
    echo "  curl '$BITDEX_0/api/indexes/$INDEX/stats' | jq"
    echo "  curl -X POST '$BITDEX_0/api/indexes/$INDEX/query' -H 'Content-Type: application/json' -d '{\"filters\":[],\"limit\":1,\"include_docs\":true}' | jq"
    exit 1
fi

echo -e "${GREEN}All arena-free tests passed!${NC}"
