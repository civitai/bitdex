#!/bin/sh
# Deterministic repro: ops_poller cursor skips rows whose transactions commit
# out of id order. Mirrors SETUP_V2_SQL (queries.rs) + the poller's read
# (ops_poller.rs:206 `WHERE id > $1 ORDER BY id`) + cursor advance to max id
# (ops_poller.rs:180) + cleanup trigger (queries.rs:185).
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

# Conn A = long publish transaction: inserts the fan-out early (id 1),
# holds the txn open 6s (rest of the publish flow), commits late.
docker exec bitdex-skip-repro psql -U postgres -q -c \
  "BEGIN; INSERT INTO \"BitdexOps\" (entity_id, ops) VALUES (29674681, '[{\"op\":\"queryOpSet\"}]'::jsonb); SELECT pg_sleep(6); COMMIT;" &
APID=$!
sleep 2

# Conn B = quick op: later id (2), commits immediately.
$PSQL -c "INSERT INTO \"BitdexOps\" (entity_id, ops) VALUES (555, '[]'::jsonb);"

# Poller cycle 1 (txn A still open): reads id > 0, sees ONLY the quick row.
SEEN=$($PSQL -c "SELECT string_agg(id::text, ',') FROM (SELECT id FROM \"BitdexOps\" WHERE id > 0 ORDER BY id ASC LIMIT 100) s;")
MAXID=$($PSQL -c "SELECT COALESCE(max(id),0) FROM \"BitdexOps\" WHERE id > 0;")
echo "poll1: visible ids=[$SEEN] -> cursor advances to $MAXID"
$PSQL -c "INSERT INTO bitdex_cursors (replica_id, last_outbox_id) VALUES ('pod-0', $MAXID) ON CONFLICT (replica_id) DO UPDATE SET last_outbox_id = $MAXID, updated_at = now();"

# Publish transaction commits — its row (id 1) becomes visible BEHIND the cursor.
wait $APID
sleep 1

# Poller cycle 2: reads id > cursor. The publish row is invisible to it forever.
POLL2=$($PSQL -c "SELECT COALESCE(string_agg(id::text, ','), '<none>') FROM (SELECT id FROM \"BitdexOps\" WHERE id > $MAXID ORDER BY id ASC LIMIT 100) s;")
ORPHAN=$($PSQL -c "SELECT COALESCE(string_agg(id::text || ':entity=' || entity_id::text, ','), '<none>') FROM \"BitdexOps\" WHERE id <= $MAXID;")
echo "poll2 (id > $MAXID): rows=[$POLL2]"
echo "orphaned-behind-cursor: [$ORPHAN]"

# Next cursor update (any pod, any time) fires cleanup and destroys the evidence.
$PSQL -c "UPDATE bitdex_cursors SET last_outbox_id = last_outbox_id, updated_at = now() WHERE replica_id = 'pod-0';"
REMAIN=$($PSQL -c "SELECT count(*) FROM \"BitdexOps\" WHERE entity_id = 29674681;")
echo "after next cursor update: publish rows remaining in BitdexOps = $REMAIN"

if [ "$POLL2" = "<none>" ] && [ "$REMAIN" = "0" ]; then
  echo "REPRO CONFIRMED: publish op skipped forever, then deleted"
  exit 0
else
  echo "repro did not trigger"
  exit 1
fi
