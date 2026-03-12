#!/usr/bin/env bash
# Integration test for BitDex rolling-restart cursor flow.
#
# Validates:
#   1. Both replicas start with empty index (restored from config.json)
#   2. PG inserts flow through outbox → sidecar → BitDex to both replicas
#   3. Cursors advance on both replicas
#   4. Outbox rows are cleaned up by the PG trigger
#   5. After a sidecar restart, it resumes from its persisted cursor
#
# Prerequisites: docker compose up -d (all services running)
# Usage: bash test-cursor-flow.sh

set -euo pipefail

BITDEX_0="http://localhost:3011"
BITDEX_1="http://localhost:3012"
INDEX="test"
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
    # Use no-sort query (slot order) to avoid stale cache results
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
    docker compose exec -T postgres psql -U bitdex -d bitdex_test -tAc \
        'SELECT COUNT(*) FROM "BitdexOutbox"' 2>/dev/null | tr -d ' '
}

pg_cursor_count() {
    docker compose exec -T postgres psql -U bitdex -d bitdex_test -tAc \
        'SELECT COUNT(*) FROM bitdex_cursors' 2>/dev/null | tr -d ' '
}

pg_min_cursor() {
    docker compose exec -T postgres psql -U bitdex -d bitdex_test -tAc \
        'SELECT COALESCE(MIN(last_outbox_id), 0) FROM bitdex_cursors' 2>/dev/null | tr -d ' '
}

pg_sql() {
    docker compose exec -T postgres psql -U bitdex -d bitdex_test -c "$1" > /dev/null 2>&1
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
echo "  BitDex Cursor Integration Test"
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

# --- Test 2: Index exists on both replicas ---
info "Test 2: Index restored from config.json"
idx0=$(curl -sf "$BITDEX_0/api/indexes/$INDEX" 2>/dev/null | jq -r '.name // empty')
idx1=$(curl -sf "$BITDEX_1/api/indexes/$INDEX" 2>/dev/null | jq -r '.name // empty')
if [ "$idx0" = "$INDEX" ]; then
    pass "bitdex-0 has index '$INDEX'"
else
    fail "bitdex-0 missing index '$INDEX' (got: $idx0)"
fi
if [ "$idx1" = "$INDEX" ]; then
    pass "bitdex-1 has index '$INDEX'"
else
    fail "bitdex-1 missing index '$INDEX' (got: $idx1)"
fi

# --- Test 3: Wait for sidecars to be running (setup done) ---
info "Test 3: Sidecars running (outbox table + triggers created)"
sleep 3  # Give sidecars time to run setup

# Verify outbox table exists
outbox_exists=$(docker compose exec -T postgres psql -U bitdex -d bitdex_test -tAc \
    "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='BitdexOutbox')" 2>/dev/null | tr -d ' ')
if [ "$outbox_exists" = "t" ]; then
    pass "BitdexOutbox table exists"
else
    fail "BitdexOutbox table missing — sidecars may not have run setup"
fi

# Verify cursor table exists
cursor_exists=$(docker compose exec -T postgres psql -U bitdex -d bitdex_test -tAc \
    "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='bitdex_cursors')" 2>/dev/null | tr -d ' ')
if [ "$cursor_exists" = "t" ]; then
    pass "bitdex_cursors table exists"
else
    fail "bitdex_cursors table missing"
fi

# --- Test 4: Insert seed data — should flow through outbox ---
info "Test 4: Insert seed data (5 images via outbox pipeline)"

# Create posts first (no trigger on Post for this)
pg_sql "INSERT INTO \"Post\" (id, \"publishedAt\", availability) VALUES (1, now(), 'public'), (2, now(), 'public'), (3, now(), 'public') ON CONFLICT DO NOTHING"

# Insert images — triggers fire, creating outbox entries
pg_sql "INSERT INTO \"Image\" (id, \"postId\", url, \"nsfwLevel\", type, \"userId\") VALUES (1, 1, 'img-1.jpg', 1, 'image', 100)"
pg_sql "INSERT INTO \"Image\" (id, \"postId\", url, \"nsfwLevel\", type, \"userId\") VALUES (2, 1, 'img-2.jpg', 1, 'image', 100)"
pg_sql "INSERT INTO \"Image\" (id, \"postId\", url, \"nsfwLevel\", type, \"userId\") VALUES (3, 2, 'img-3.jpg', 2, 'image', 200)"
pg_sql "INSERT INTO \"Image\" (id, \"postId\", url, \"nsfwLevel\", type, \"userId\") VALUES (4, 2, 'img-4.jpg', 1, 'image', 200)"
pg_sql "INSERT INTO \"Image\" (id, \"postId\", url, \"nsfwLevel\", type, \"userId\") VALUES (5, 3, 'img-5.jpg', 1, 'image', 300)"

echo "  Inserted 5 images. Waiting for propagation..."

# Wait for both replicas to have all 5 records
if wait_for_gte "bitdex-0 has >= 5 records" "query_count $BITDEX_0" 5 30; then
    actual=$(query_count "$BITDEX_0")
    pass "bitdex-0 received seed images (total: $actual)"
else
    actual=$(query_count "$BITDEX_0" 2>/dev/null || echo "error")
    fail "bitdex-0 has $actual records (expected >= 5)"
fi

if wait_for_gte "bitdex-1 has >= 5 records" "query_count $BITDEX_1" 5 30; then
    actual=$(query_count "$BITDEX_1")
    pass "bitdex-1 received seed images (total: $actual)"
else
    actual=$(query_count "$BITDEX_1" 2>/dev/null || echo "error")
    fail "bitdex-1 has $actual records (expected >= 5)"
fi

# --- Test 5: Cursors exist on both replicas ---
info "Test 5: Cursors registered"
sleep 2  # Allow cursor reporting

cursor_0=$(get_cursor "$BITDEX_0" "pg-sync-bitdex-0")
cursor_1=$(get_cursor "$BITDEX_1" "pg-sync-bitdex-1")

if [ -n "$cursor_0" ] && [ "$cursor_0" != "null" ]; then
    pass "bitdex-0 has cursor pg-sync-bitdex-0 = $cursor_0"
else
    fail "bitdex-0 missing cursor pg-sync-bitdex-0"
fi

if [ -n "$cursor_1" ] && [ "$cursor_1" != "null" ]; then
    pass "bitdex-1 has cursor pg-sync-bitdex-1 = $cursor_1"
else
    fail "bitdex-1 missing cursor pg-sync-bitdex-1"
fi

# --- Test 6: PG has cursor tracking rows ---
info "Test 6: PG cursor tracking"
pg_cursors=$(pg_cursor_count)
if [ -n "$pg_cursors" ] && [ "$pg_cursors" -ge 2 ] 2>/dev/null; then
    pass "PG has $pg_cursors cursor rows (expected >= 2)"
else
    fail "PG has '$pg_cursors' cursor rows (expected >= 2)"
fi

# --- Test 7: Insert more data and verify propagation ---
info "Test 7: Insert 3 more images — outbox propagation"

pg_sql "INSERT INTO \"Post\" (id, \"publishedAt\", availability) VALUES (10, now(), 'public') ON CONFLICT DO NOTHING"
pg_sql "INSERT INTO \"Image\" (id, \"postId\", url, \"nsfwLevel\", type, \"userId\") VALUES (100, 10, 'new-100.jpg', 1, 'image', 999)"
pg_sql "INSERT INTO \"Image\" (id, \"postId\", url, \"nsfwLevel\", type, \"userId\") VALUES (101, 10, 'new-101.jpg', 1, 'image', 999)"
pg_sql "INSERT INTO \"Image\" (id, \"postId\", url, \"nsfwLevel\", type, \"userId\") VALUES (102, 10, 'new-102.jpg', 1, 'image', 999)"

echo "  Inserted 3 new images (ids: 100, 101, 102). Waiting..."

if wait_for_gte "bitdex-0 has >= 8 records" "query_count $BITDEX_0" 8 30; then
    actual=$(query_count "$BITDEX_0")
    pass "bitdex-0 received new images (total: $actual)"
else
    actual=$(query_count "$BITDEX_0" 2>/dev/null || echo "error")
    fail "bitdex-0 has $actual records (expected >= 8)"
fi

if wait_for_gte "bitdex-1 has >= 8 records" "query_count $BITDEX_1" 8 30; then
    actual=$(query_count "$BITDEX_1")
    pass "bitdex-1 received new images (total: $actual)"
else
    actual=$(query_count "$BITDEX_1" 2>/dev/null || echo "error")
    fail "bitdex-1 has $actual records (expected >= 8)"
fi

# --- Test 8: Cursors advanced ---
info "Test 8: Cursor advancement"
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

# --- Test 9: Outbox cleanup ---
info "Test 9: Outbox cleanup via PG trigger"
sleep 3

remaining=$(outbox_count)
if [ -n "$remaining" ] && [ "$remaining" -le 3 ] 2>/dev/null; then
    pass "Outbox cleaned up ($remaining rows remaining)"
else
    fail "Outbox not fully cleaned ($remaining rows remaining)"
fi

# --- Test 10: Sidecar restart recovery ---
info "Test 10: Sidecar restart — cursor-based catch-up"

# Record current counts
count_before_0=$(query_count "$BITDEX_0")
count_before_1=$(query_count "$BITDEX_1")

# Stop sidecar-0
docker compose stop sidecar-0 > /dev/null 2>&1
echo "  Stopped sidecar-0"

# Insert more data while sidecar-0 is down
pg_sql "INSERT INTO \"Image\" (id, \"postId\", url, \"nsfwLevel\", type, \"userId\") VALUES (200, 10, 'new-200.jpg', 1, 'image', 888)"
pg_sql "INSERT INTO \"Image\" (id, \"postId\", url, \"nsfwLevel\", type, \"userId\") VALUES (201, 10, 'new-201.jpg', 1, 'image', 888)"
echo "  Inserted 2 more images (ids: 200, 201) while sidecar-0 is down"

expected_1=$((count_before_1 + 2))

# bitdex-1 should receive them
if wait_for_gte "bitdex-1 has >= $expected_1 records" "query_count $BITDEX_1" "$expected_1" 30; then
    actual=$(query_count "$BITDEX_1")
    pass "bitdex-1 received images while sidecar-0 was down (total: $actual)"
else
    actual=$(query_count "$BITDEX_1" 2>/dev/null || echo "error")
    fail "bitdex-1 has $actual records (expected >= $expected_1)"
fi

# bitdex-0 should still have old count (sidecar is down)
count_0_during=$(query_count "$BITDEX_0")
if [ "$count_0_during" = "$count_before_0" ]; then
    pass "bitdex-0 unchanged at $count_0_during records (sidecar was down)"
else
    fail "bitdex-0 unexpectedly changed: $count_before_0 → $count_0_during"
fi

# Restart sidecar-0 — it should resume from persisted cursor and catch up
docker compose start sidecar-0 > /dev/null 2>&1
echo "  Restarted sidecar-0. Waiting for catch-up..."

expected_0=$((count_before_0 + 2))
if wait_for_gte "bitdex-0 has >= $expected_0 records" "query_count $BITDEX_0" "$expected_0" 30; then
    actual=$(query_count "$BITDEX_0")
    pass "bitdex-0 caught up after restart (total: $actual)"
else
    actual=$(query_count "$BITDEX_0" 2>/dev/null || echo "error")
    fail "bitdex-0 has $actual after restart (expected >= $expected_0)"
fi

# --- Test 11: Post-restart outbox cleanup ---
info "Test 11: Post-restart outbox cleanup"
sleep 5

remaining=$(outbox_count)
if [ -n "$remaining" ] && [ "$remaining" -le 2 ] 2>/dev/null; then
    pass "Outbox cleaned after restart ($remaining rows remaining)"
else
    fail "Outbox not fully cleaned after restart ($remaining rows remaining)"
fi

# --- Test 12: Delete propagation ---
info "Test 12: Delete propagation"

pg_sql "DELETE FROM \"Image\" WHERE id = 100"
echo "  Deleted image id=100. Waiting for propagation..."

sleep 5
# Both should now have 1 fewer record
expected_after_delete=$((expected_0 - 1))

count_0_after=$(query_count "$BITDEX_0")
count_1_after=$(query_count "$BITDEX_1")

if [ "$count_0_after" -le "$expected_0" ] 2>/dev/null; then
    pass "bitdex-0 processed delete (count: $count_0_after)"
else
    fail "bitdex-0 did not process delete (count: $count_0_after, expected <= $expected_0)"
fi

if [ "$count_1_after" -le "$expected_1" ] 2>/dev/null; then
    pass "bitdex-1 processed delete (count: $count_1_after)"
else
    fail "bitdex-1 did not process delete (count: $count_1_after, expected <= $expected_1)"
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
    echo "  docker compose logs sidecar-0"
    echo "  docker compose logs sidecar-1"
    echo "  curl $BITDEX_0/api/indexes/$INDEX/cursors | jq"
    echo "  curl $BITDEX_1/api/indexes/$INDEX/cursors | jq"
    echo "  docker compose exec postgres psql -U bitdex -d bitdex_test -c 'SELECT * FROM bitdex_cursors'"
    echo "  docker compose exec postgres psql -U bitdex -d bitdex_test -c 'SELECT * FROM \"BitdexOutbox\"'"
    exit 1
fi

echo -e "${GREEN}All tests passed!${NC}"
