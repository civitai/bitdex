#!/usr/bin/env bash
# One-shot runner: build, start, test, teardown.
#
# Usage:
#   cd tests/integration
#   bash run.sh
#
# Pass --keep to leave containers running after tests for debugging.

set -euo pipefail
cd "$(dirname "$0")"

KEEP=false
[ "${1:-}" = "--keep" ] && KEEP=true

cleanup() {
    if [ "$KEEP" = false ]; then
        echo ""
        echo "Tearing down..."
        docker compose down -v --remove-orphans 2>/dev/null
    else
        echo ""
        echo "Containers left running (--keep). Tear down with: docker compose down -v"
    fi
}
trap cleanup EXIT

echo "Building BitDex image..."
docker compose build --quiet

echo "Starting Postgres..."
docker compose up -d postgres
echo "Waiting for Postgres to be ready..."
until docker compose exec -T postgres pg_isready -U bitdex -d bitdex_test > /dev/null 2>&1; do
    sleep 1
done

echo "Starting BitDex servers..."
docker compose up -d bitdex-0 bitdex-1
echo "Waiting for BitDex servers to be ready..."
for i in 0 1; do
    port=$((3001 + i))
    until curl -sf "http://localhost:$port/api/health" > /dev/null 2>&1; do
        sleep 1
    done
    echo "  bitdex-$i ready"
done

echo "Starting sidecars..."
docker compose up -d sidecar-0 sidecar-1

echo "Waiting for sidecars to finish initial load..."
sleep 5

echo ""
bash test-cursor-flow.sh
