-- BitdexOps retention floor — APPLY BY HAND (psql as the `bitdex` user).
--
-- Why: cleanup_bitdex_ops() deletes every op the moment all replicas have
-- acked it, which in prod is ~48 seconds. Any write-path defect therefore has
-- to be diagnosed from resulting document state rather than from the ops that
-- produced it. This keeps a bounded 2h window of consumed ops readable.
--
-- Cost: ~117 rows/s measured => ~840k rows resident.
--
-- The floor is an id CEILING, not a created_at predicate on the DELETE: the
-- trigger fires on every cursor report, so the DELETE must stay a bounded
-- index range on id. id and created_at are both monotonic in insert order.
--
-- Step 1 runs OUTSIDE a transaction (CONCURRENTLY). Step 2 is a plain
-- CREATE OR REPLACE and is safe to run while the pipeline is live.

-- 1) index backing the ceiling lookup
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_bitdex_ops_created_at
    ON "BitdexOps" (created_at);

-- 2) cleanup function with the retention floor
CREATE OR REPLACE FUNCTION cleanup_bitdex_ops() RETURNS trigger AS $$
DECLARE
    _consumed_below BIGINT;
    _retained_below BIGINT;
BEGIN
    SELECT MIN(last_outbox_id) INTO _consumed_below FROM bitdex_cursors;
    IF _consumed_below IS NULL THEN
        RETURN NEW;
    END IF;

    SELECT id INTO _retained_below
    FROM "BitdexOps"
    WHERE created_at < now() - interval '2 hours'
    ORDER BY created_at DESC
    LIMIT 1;
    IF _retained_below IS NULL THEN
        RETURN NEW;
    END IF;

    DELETE FROM "BitdexOps"
    WHERE id < LEAST(_consumed_below, _retained_below);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Verify (expect a spread of ~2h once it has been running that long, and a
-- count on the order of 840k rather than a few thousand):
--   SELECT count(*), min(created_at), max(created_at) FROM "BitdexOps";
--
-- Rollback: re-apply the pre-floor body from src/pg_sync/queries.rs history,
-- or simply
--   CREATE OR REPLACE FUNCTION cleanup_bitdex_ops() RETURNS trigger AS $$
--   BEGIN
--       DELETE FROM "BitdexOps"
--       WHERE id < (SELECT MIN(last_outbox_id) FROM bitdex_cursors);
--       RETURN NEW;
--   END;
--   $$ LANGUAGE plpgsql;
