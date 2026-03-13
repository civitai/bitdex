#!/usr/bin/env bash
# Integration test for BitDex bulk-load + sync flow.
#
# Validates:
#   1. Bulk loader reads pre-existing PG data and writes bitmaps to disk
#   2. Server restores bulk-loaded data on startup
#   3. Cursor seeded by loader persists through server restart
#   4. Sync sidecar resumes from seeded cursor (no re-processing of bulk data)
#   5. New PG inserts flow through outbox → sidecar → BitDex (no gap)
#   6. Delete propagation works post-bulk-load
#
# Prerequisites: run-bulk.sh has started all containers
# Usage: bash test-bulk-load.sh

set -euo pipefail

BITDEX_0="http://localhost:3011"
BITDEX_1="http://localhost:3012"
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

query_count() {
    local url="$1"
    local result
    result=$(curl -sf "$url/api/indexes/$INDEX/query" \
        -H 'Content-Type: application/json' \
        -d '{"filters":[],"limit":1}' 2>/dev/null)
    if [ $? -ne 0 ] || [ -z "$result" ]; then
        echo "0"
        return
    fi
    echo "$result" | jq -r '.total_matched // 0'
}

get_cursor() {
    local url="$1"
    local cursor_name="$2"
    curl -sf "$url/api/indexes/$INDEX/cursors/$cursor_name" 2>/dev/null | jq -r '.value // empty' 2>/dev/null || echo ""
}

outbox_count() {
    docker compose -f "$COMPOSE_FILE" exec -T postgres psql -U bitdex -d bitdex_test -tAc \
        'SELECT COUNT(*) FROM "BitdexOutbox"' 2>/dev/null | tr -d ' '
}

pg_cursor_count() {
    docker compose -f "$COMPOSE_FILE" exec -T postgres psql -U bitdex -d bitdex_test -tAc \
        'SELECT COUNT(*) FROM bitdex_cursors' 2>/dev/null | tr -d ' '
}

pg_sql() {
    docker compose -f "$COMPOSE_FILE" exec -T postgres psql -U bitdex -d bitdex_test -c "$1" > /dev/null 2>&1
}

wait_for() {
    local desc="$1"
    local check_cmd="$2"
    local expected="$3"
    local timeout="${4:-30}"
    local elapsed=0
    while [ "$elapsed" -lt "$timeout" ]; do
        local result
        result=$(eval "$check_cmd" 2>/dev/null || echo "")
        if [ "$result" = "$expected" ]; then
            return 0
        fi
        sleep 1
        ((elapsed++))
    done
    return 1
}

wait_for_gte() {
    local desc="$1"
    local check_cmd="$2"
    local min_val="$3"
    local timeout="${4:-30}"
    local elapsed=0
    while [ "$elapsed" -lt "$timeout" ]; do
        local result
        result=$(eval "$check_cmd" 2>/dev/null || echo "0")
        if [ -n "$result" ] && [ "$result" -ge "$min_val" ] 2>/dev/null; then
            return 0
        fi
        sleep 1
        ((elapsed++))
    done
    return 1
}

# ===========================================================================
echo ""
echo "========================================"
echo "  BitDex Bulk Load Integration Test"
echo "========================================"
echo ""

# --- Test 1: Both replicas are healthy ---
info "Test 1: Health check"
if curl -sf "$BITDEX_0/api/health" > /dev/null 2>&1; then
    pass "bitdex-0 is healthy"
else
    fail "bitdex-0 is not healthy"
fi
if curl -sf "$BITDEX_1/api/health" > /dev/null 2>&1; then
    pass "bitdex-1 is healthy"
else
    fail "bitdex-1 is not healthy"
fi

# --- Test 2: Index restored with bulk-loaded data ---
info "Test 2: Bulk-loaded data restored from disk"

count_0=$(query_count "$BITDEX_0")
count_1=$(query_count "$BITDEX_1")

if [ "$count_0" -ge 10 ] 2>/dev/null; then
    pass "bitdex-0 restored $count_0 records from bulk load (expected >= 10)"
else
    fail "bitdex-0 has $count_0 records (expected >= 10 from bulk load)"
fi

if [ "$count_1" -ge 10 ] 2>/dev/null; then
    pass "bitdex-1 restored $count_1 records from bulk load (expected >= 10)"
else
    fail "bitdex-1 has $count_1 records (expected >= 10 from bulk load)"
fi

# --- Test 3: Cursors seeded by loader are present ---
info "Test 3: Cursors persisted from bulk load"

cursor_0=$(get_cursor "$BITDEX_0" "pg-sync-bitdex-0")
cursor_1=$(get_cursor "$BITDEX_1" "pg-sync-bitdex-1")

if [ -n "$cursor_0" ] && [ "$cursor_0" != "null" ]; then
    pass "bitdex-0 has cursor pg-sync-bitdex-0 = $cursor_0 (seeded by loader)"
else
    fail "bitdex-0 missing cursor pg-sync-bitdex-0 (loader didn't persist it)"
fi

if [ -n "$cursor_1" ] && [ "$cursor_1" != "null" ]; then
    pass "bitdex-1 has cursor pg-sync-bitdex-1 = $cursor_1 (seeded by loader)"
else
    fail "bitdex-1 missing cursor pg-sync-bitdex-1 (loader didn't persist it)"
fi

# --- Test 4: Outbox triggers exist (created by loader's setup phase) ---
info "Test 4: Outbox infrastructure from loader setup"

outbox_exists=$(docker compose -f "$COMPOSE_FILE" exec -T postgres psql -U bitdex -d bitdex_test -tAc \
    "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='BitdexOutbox')" 2>/dev/null | tr -d ' ')
if [ "$outbox_exists" = "t" ]; then
    pass "BitdexOutbox table exists"
else
    fail "BitdexOutbox table missing — loader setup may not have run"
fi

cursor_table=$(docker compose -f "$COMPOSE_FILE" exec -T postgres psql -U bitdex -d bitdex_test -tAc \
    "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='bitdex_cursors')" 2>/dev/null | tr -d ' ')
if [ "$cursor_table" = "t" ]; then
    pass "bitdex_cursors table exists"
else
    fail "bitdex_cursors table missing"
fi

# --- Test 5: PG cursor tracking rows from loader ---
info "Test 5: PG cursor tracking from bulk load"

pg_cursors=$(pg_cursor_count)
if [ -n "$pg_cursors" ] && [ "$pg_cursors" -ge 2 ] 2>/dev/null; then
    pass "PG has $pg_cursors cursor rows (loaders registered both replicas)"
else
    fail "PG has '$pg_cursors' cursor rows (expected >= 2)"
fi

# --- Test 6: Insert new data — should flow through outbox → sync → BitDex ---
info "Test 6: New inserts via outbox pipeline (no gap after bulk load)"

pg_sql "INSERT INTO \"Post\" (id, \"publishedAt\", availability) VALUES (10, now(), 'public') ON CONFLICT DO NOTHING"
pg_sql "INSERT INTO \"Image\" (id, \"postId\", url, \"nsfwLevel\", type, \"userId\") VALUES (100, 10, 'new-100.jpg', 1, 'image', 999)"
pg_sql "INSERT INTO \"Image\" (id, \"postId\", url, \"nsfwLevel\", type, \"userId\") VALUES (101, 10, 'new-101.jpg', 1, 'image', 999)"
pg_sql "INSERT INTO \"Image\" (id, \"postId\", url, \"nsfwLevel\", type, \"userId\") VALUES (102, 10, 'new-102.jpg', 2, 'image', 999)"
pg_sql "INSERT INTO \"Image\" (id, \"postId\", url, \"nsfwLevel\", type, \"userId\") VALUES (103, 10, 'new-103.jpg', 1, 'image', 999)"
pg_sql "INSERT INTO \"Image\" (id, \"postId\", url, \"nsfwLevel\", type, \"userId\") VALUES (104, 10, 'new-104.jpg', 1, 'image', 999)"

echo "  Inserted 5 new images (ids: 100-104). Waiting for propagation..."

if wait_for_gte "bitdex-0 has >= 15 records" "query_count $BITDEX_0" 15 45; then
    actual=$(query_count "$BITDEX_0")
    pass "bitdex-0 received new images via sync (total: $actual)"
else
    actual=$(query_count "$BITDEX_0" 2>/dev/null || echo "error")
    fail "bitdex-0 has $actual records (expected >= 15: 10 bulk + 5 new)"
fi

if wait_for_gte "bitdex-1 has >= 15 records" "query_count $BITDEX_1" 15 45; then
    actual=$(query_count "$BITDEX_1")
    pass "bitdex-1 received new images via sync (total: $actual)"
else
    actual=$(query_count "$BITDEX_1" 2>/dev/null || echo "error")
    fail "bitdex-1 has $actual records (expected >= 15: 10 bulk + 5 new)"
fi

# --- Test 7: Cursors advanced past seeded value ---
info "Test 7: Cursor advancement after sync"
sleep 2

new_cursor_0=$(get_cursor "$BITDEX_0" "pg-sync-bitdex-0")
new_cursor_1=$(get_cursor "$BITDEX_1" "pg-sync-bitdex-1")

if [ -n "$new_cursor_0" ] && [ "$new_cursor_0" -gt "${cursor_0:-0}" ] 2>/dev/null; then
    pass "bitdex-0 cursor advanced: $cursor_0 → $new_cursor_0"
else
    fail "bitdex-0 cursor did not advance (was ${cursor_0:-0}, now ${new_cursor_0:-?})"
fi

if [ -n "$new_cursor_1" ] && [ "$new_cursor_1" -gt "${cursor_1:-0}" ] 2>/dev/null; then
    pass "bitdex-1 cursor advanced: $cursor_1 → $new_cursor_1"
else
    fail "bitdex-1 cursor did not advance (was ${cursor_1:-0}, now ${new_cursor_1:-?})"
fi

# --- Test 8: Outbox cleanup ---
info "Test 8: Outbox cleanup"
sleep 3

remaining=$(outbox_count)
if [ -n "$remaining" ] && [ "$remaining" -le 5 ] 2>/dev/null; then
    pass "Outbox cleaned up ($remaining rows remaining)"
else
    fail "Outbox not fully cleaned ($remaining rows remaining)"
fi

# --- Test 9: Delete propagation ---
info "Test 9: Delete propagation"

count_before_0=$(query_count "$BITDEX_0")
count_before_1=$(query_count "$BITDEX_1")

pg_sql "DELETE FROM \"Image\" WHERE id = 100"
echo "  Deleted image id=100. Waiting for propagation..."

sleep 5

count_after_0=$(query_count "$BITDEX_0")
count_after_1=$(query_count "$BITDEX_1")

if [ "$count_after_0" -lt "$count_before_0" ] 2>/dev/null; then
    pass "bitdex-0 processed delete ($count_before_0 → $count_after_0)"
else
    fail "bitdex-0 did not process delete ($count_before_0 → $count_after_0)"
fi

if [ "$count_after_1" -lt "$count_before_1" ] 2>/dev/null; then
    pass "bitdex-1 processed delete ($count_before_1 → $count_after_1)"
else
    fail "bitdex-1 did not process delete ($count_before_1 → $count_after_1)"
fi

# --- Test 10: Insert more data after delete to verify no corruption ---
info "Test 10: Post-delete insert integrity"

pg_sql "INSERT INTO \"Image\" (id, \"postId\", url, \"nsfwLevel\", type, \"userId\") VALUES (200, 10, 'post-del-200.jpg', 1, 'image', 777)"

expected=$((count_after_0 + 1))
if wait_for_gte "bitdex-0 has >= $expected records" "query_count $BITDEX_0" "$expected" 30; then
    actual=$(query_count "$BITDEX_0")
    pass "bitdex-0 received post-delete insert (total: $actual)"
else
    actual=$(query_count "$BITDEX_0" 2>/dev/null || echo "error")
    fail "bitdex-0 has $actual records after post-delete insert (expected >= $expected)"
fi

# ===========================================================================
echo ""
echo "========================================"
echo -e "  Results: ${GREEN}$PASS passed${NC}, ${RED}$FAIL failed${NC}"
echo "========================================"
echo ""

if [ "$FAIL" -gt 0 ]; then
    echo -e "${RED}Some tests failed!${NC}"
    echo ""
    echo "Debug commands:"
    echo "  docker compose -f $COMPOSE_FILE logs loader-0"
    echo "  docker compose -f $COMPOSE_FILE logs loader-1"
    echo "  docker compose -f $COMPOSE_FILE logs sidecar-0"
    echo "  docker compose -f $COMPOSE_FILE logs sidecar-1"
    echo "  curl $BITDEX_0/api/indexes/$INDEX/cursors | jq"
    echo "  curl $BITDEX_1/api/indexes/$INDEX/cursors | jq"
    echo "  docker compose -f $COMPOSE_FILE exec postgres psql -U bitdex -d bitdex_test -c 'SELECT * FROM bitdex_cursors'"
    echo "  docker compose -f $COMPOSE_FILE exec postgres psql -U bitdex -d bitdex_test -c 'SELECT * FROM \"BitdexOutbox\"'"
    exit 1
fi

echo -e "${GREEN}All tests passed!${NC}"
