#!/bin/sh
# PASSING half of the gate: same out-of-order-commit scenario as skip_repro.sh,
# but the poller advances its cursor with the gap-aware frontier walk
# (compute_safe_frontier semantics from the fix): the durable cursor holds
# below an allocated-but-invisible id; rows beyond the gap still POST.
set -e
PSQL="docker exec -i bitdex-skip-repro psql -U postgres -q -t -A"

$PSQL <<'EOF'
DROP TABLE IF EXISTS "BitdexOps" CASCADE;
DROP TABLE IF EXISTS bitdex_cursors CASCADE;
CREATE TABLE "BitdexOps" (
    id BIGSERIAL PRIMARY KEY,
    entity_id BIGINT NOT NULL,
    ops JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE bitdex_cursors (
    replica_id TEXT PRIMARY KEY,
    last_outbox_id BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE OR REPLACE FUNCTION cleanup_bitdex_ops() RETURNS trigger AS $$
BEGIN
    DELETE FROM "BitdexOps" WHERE id < (SELECT MIN(last_outbox_id) FROM bitdex_cursors);
    RETURN NEW;
END; $$ LANGUAGE plpgsql;
CREATE TRIGGER trg_cleanup_bitdex_ops AFTER INSERT OR UPDATE ON bitdex_cursors
    FOR EACH ROW EXECUTE FUNCTION cleanup_bitdex_ops();
EOF

# safe frontier walk (fix semantics): highest id reachable from cursor+1
# without an unexplained hole among the visible ids.
safe_frontier() { # $1 = cursor
  $PSQL -c "
    WITH v AS (SELECT id FROM \"BitdexOps\" WHERE id > $1 ORDER BY id)
    SELECT COALESCE(max(id), $1) FROM (
      SELECT id, row_number() OVER (ORDER BY id) AS rn FROM v
    ) w WHERE id = $1 + rn;"
}

# Conn A = long publish transaction: id 1 allocated early, committed late.
docker exec bitdex-skip-repro psql -U postgres -q -c \
  "BEGIN; INSERT INTO \"BitdexOps\" (entity_id, ops) VALUES (29674681, '[{\"op\":\"queryOpSet\"}]'::jsonb); SELECT pg_sleep(6); COMMIT;" &
APID=$!
sleep 2

# Conn B = quick op: id 2, commits immediately.
$PSQL -c "INSERT INTO \"BitdexOps\" (entity_id, ops) VALUES (555, '[]'::jsonb);"

# Poller cycle 1 (txn A open): sees only id 2. Frontier walk finds the hole at
# id 1 -> durable cursor HOLDS at 0. (id 2 still POSTs, tracked by posted_hwm.)
SAFE1=$(safe_frontier 0)
echo "poll1: visible=[$($PSQL -c "SELECT string_agg(id::text, ',') FROM \"BitdexOps\";")] safe_frontier=$SAFE1 (old code advanced to 2)"
$PSQL -c "INSERT INTO bitdex_cursors (replica_id, last_outbox_id) VALUES ('pod-0', $SAFE1) ON CONFLICT (replica_id) DO UPDATE SET last_outbox_id = $SAFE1, updated_at = now();"

# Publish transaction commits.
wait $APID
sleep 1

# Poller cycle 2: gap filled in -> frontier reaches the end; publish row is
# read and processed before the cursor passes it.
ROWS2=$($PSQL -c "SELECT string_agg(id::text || ':entity=' || entity_id::text, ',') FROM \"BitdexOps\" WHERE id > $SAFE1 ORDER BY 1;")
SAFE2=$(safe_frontier $SAFE1)
echo "poll2: rows past cursor=[$ROWS2] safe_frontier=$SAFE2"
$PSQL -c "UPDATE bitdex_cursors SET last_outbox_id = $SAFE2, updated_at = now() WHERE replica_id = 'pod-0';"

GOT_PUBLISH=$(echo "$ROWS2" | grep -c "entity=29674681" || true)
if [ "$SAFE1" = "0" ] && [ "$GOT_PUBLISH" = "1" ] && [ "$SAFE2" = "2" ]; then
  echo "FIX CONFIRMED: cursor held at gap, publish op delivered after commit"
  exit 0
else
  echo "fix simulation FAILED (safe1=$SAFE1 got_publish=$GOT_PUBLISH safe2=$SAFE2)"
  exit 1
fi
