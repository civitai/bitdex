#!/bin/sh
# Merge gate for the ops-poller skip-race fix: runs the REAL poll_and_process
# integration tests (src/pg_sync/ops_poller.rs::pg_integration_tests) against
# a throwaway PostgreSQL 16 container. The SQL scripts (skip-race-repro.sh /
# skip-race-fixed.sh) only demonstrate semantics — THIS exercises the shipped
# code. Requires: docker, cargo. pg_current_snapshot needs PG >= 13.
set -e
NAME=bitdex-skip-race-gate
PORT=55443

docker rm -f $NAME >/dev/null 2>&1 || true
docker run -d --rm --name $NAME -e POSTGRES_PASSWORD=x -p $PORT:5432 postgres:16-bookworm >/dev/null
trap "docker stop $NAME >/dev/null 2>&1 || true" EXIT
for i in $(seq 1 30); do
  docker exec $NAME pg_isready -U postgres >/dev/null 2>&1 && break
  sleep 1
done

export SKIP_RACE_PG_URL="postgresql://postgres:x@127.0.0.1:$PORT/postgres"
# Tests share one database — run single-threaded.
cargo test --features pg-sync --lib pg_sync::ops_poller::pg_integration_tests \
  -- --ignored --test-threads=1
echo "SKIP-RACE GATE PASSED"
