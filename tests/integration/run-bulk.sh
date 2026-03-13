#!/usr/bin/env bash
# One-shot runner for the bulk-load integration test.
#
# Orchestrates the full production flow:
#   1. Start Postgres (with pre-existing seed data)
#   2. Run loaders (setup triggers + bulk load from PG → disk)
#   3. Start BitDex servers (restore from disk)
#   4. Start sync sidecars (poll outbox from seeded cursor)
#   5. Run test script
#
# Usage:
#   cd tests/integration
#   bash run-bulk.sh
#
# Pass --keep to leave containers running after tests for debugging.

set -euo pipefail
cd "$(dirname "$0")"

COMPOSE_FILE="docker-compose-bulk.yml"
KEEP=false
[ "${1:-}" = "--keep" ] && KEEP=true

cleanup() {
    if [ "$KEEP" = false ]; then
        echo ""
        echo "Tearing down..."
        docker compose -f "$COMPOSE_FILE" down -v --remove-orphans 2>/dev/null
    else
        echo ""
        echo "Containers left running (--keep). Tear down with: docker compose -f $COMPOSE_FILE down -v"
    fi
}
trap cleanup EXIT

echo "Building BitDex image..."
docker compose -f "$COMPOSE_FILE" build --quiet

echo ""
echo "=== Phase 1: Start Postgres ==="
docker compose -f "$COMPOSE_FILE" up -d postgres
echo "Waiting for Postgres to be ready..."
until docker compose -f "$COMPOSE_FILE" exec -T postgres pg_isready -U bitdex -d bitdex_test > /dev/null 2>&1; do
    sleep 1
done
echo "  Postgres ready (10 seed images loaded)"

# Verify seed data
count=$(docker compose -f "$COMPOSE_FILE" exec -T postgres psql -U bitdex -d bitdex_test -tAc \
    'SELECT COUNT(*) FROM "Image"' 2>/dev/null | tr -d ' ')
echo "  Images in PG: $count"

echo ""
echo "=== Phase 2: Run Bulk Loaders ==="
echo "Running loader-0 (setup + bulk load)..."
docker compose -f "$COMPOSE_FILE" run --rm loader-0
echo "  loader-0 complete"

echo "Running loader-1 (setup + bulk load)..."
docker compose -f "$COMPOSE_FILE" run --rm loader-1
echo "  loader-1 complete"

echo ""
echo "=== Phase 3: Start BitDex Servers ==="
docker compose -f "$COMPOSE_FILE" up -d bitdex-0 bitdex-1
echo "Waiting for BitDex servers to restore from disk..."
for i in 0 1; do
    port=$((3011 + i))
    until curl -sf "http://localhost:$port/api/health" > /dev/null 2>&1; do
        sleep 1
    done
    echo "  bitdex-$i ready (port $port)"
done

echo ""
echo "=== Phase 4: Start Sync Sidecars ==="
docker compose -f "$COMPOSE_FILE" up -d sidecar-0 sidecar-1
echo "Waiting for sidecars to initialize..."
sleep 3

echo ""
echo "=== Phase 5: Run Tests ==="
bash test-bulk-load.sh
