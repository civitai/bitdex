-- BitdexOps retention floor — APPLY BY HAND (psql as the `bitdex` user).
--
-- Why: cleanup_bitdex_ops() deletes every op the moment all replicas have
-- acked it, which in prod is ~48 seconds. Any write-path defect therefore has
-- to be diagnosed from resulting document state rather than from the ops that
-- produced it. This keeps a bounded 2h window of consumed ops readable.
--
-- NO new index. The table takes ~117 inserts/s and this trigger fires on every
-- cursor report, so neither extra write amplification nor an unbounded scan is
-- acceptable. The DELETE instead walks the PRIMARY KEY over a fixed-width id
-- window starting at the oldest surviving row: ids are assigned in insert
-- order, so "oldest by id" is "oldest by created_at". Work per firing is capped
-- at ~5,000 index entries and is LESS than the previous unbounded
-- `DELETE ... WHERE id < MIN(last_outbox_id)`.
--
-- The window is bounded by id arithmetic, not by a match count: `LIMIT n` on a
-- created_at predicate would, once the floor has caught up, walk every retained
-- row looking for matches it will not find.
--
-- Cost: ~117 rows/s => ~840k rows resident instead of a few thousand. That is
-- the whole price of the change — a larger live table for autovacuum to keep
-- up with, and no new index to maintain on the insert path.
--
-- Single CREATE OR REPLACE, safe to run while the pipeline is live.

CREATE OR REPLACE FUNCTION cleanup_bitdex_ops() RETURNS trigger AS $$
DECLARE
    _consumed_below BIGINT;
    _oldest BIGINT;
    _chunk BIGINT := 5000;
BEGIN
    SELECT MIN(last_outbox_id) INTO _consumed_below FROM bitdex_cursors;
    IF _consumed_below IS NULL THEN
        RETURN NEW;
    END IF;

    SELECT id INTO _oldest FROM "BitdexOps" ORDER BY id LIMIT 1;
    IF _oldest IS NULL THEN
        RETURN NEW;
    END IF;

    DELETE FROM "BitdexOps"
    WHERE id >= _oldest
      AND id < LEAST(_consumed_below, _oldest + _chunk)
      AND created_at < now() - interval '2 hours';
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Verify, a few minutes apart (expect the spread to grow towards 2h and the
-- count towards ~840k, then hold):
--   SELECT count(*), min(created_at), max(created_at) FROM "BitdexOps";
--
-- Rollback — restore the pre-floor body:
--   CREATE OR REPLACE FUNCTION cleanup_bitdex_ops() RETURNS trigger AS $$
--   BEGIN
--       DELETE FROM "BitdexOps"
--       WHERE id < (SELECT MIN(last_outbox_id) FROM bitdex_cursors);
--       RETURN NEW;
--   END;
--   $$ LANGUAGE plpgsql;
