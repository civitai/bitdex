-- BitdexOps retention floor — APPLY BY HAND (psql as the `bitdex` user).
--
-- Why: cleanup_bitdex_ops() deletes every op the moment all replicas have
-- acked it. Measured on prod before this change, that left the table holding
-- ONE row — retention was sub-second. Any write-path defect therefore had to be
-- diagnosed from resulting document state rather than from the ops that
-- produced it. This keeps a bounded 15-minute window of consumed ops readable.
--
-- NO index on created_at: the table takes ~117 inserts/s and this trigger fires
-- on every cursor report, so an index maintained for one lookup is not worth
-- paying for. Instead this exploits the monotonicity the window already relies
-- on — `id` is a BIGSERIAL assigned in insert order, so the PRIMARY KEY already
-- orders rows by age. The function walks the PK forward from the oldest row to
-- find the FIRST row still inside the window, then deletes strictly below it.
-- In steady state that probe stops after the handful of rows that have just
-- crossed the floor.
--
-- Cost, honestly: the previous `DELETE ... WHERE id < MIN(last_outbox_id)` was
-- unbounded in FORM but in practice touched only the rows that had newly become
-- deletable. This is NOT cheaper than what it replaces — it is the same order
-- of work plus one bounded probe, in exchange for the window. `_chunk` exists
-- only so a stalled or restarted replica cannot turn the probe into a full
-- scan; it is a safety bound, not the steady-state cost.
--
-- Note the LIMIT 1 finds the first NON-match, which sits at the front of the
-- scan. A LIMIT on the *matching* rows would look equivalent and degrade badly:
-- once the floor has caught up it would walk the whole retained window looking
-- for matches that are not there.
--
-- Measured on prod at a 15-minute window: ~139k rows, ~337 MB, 2 dead tuples,
-- autovacuum keeping up comfortably.
--
-- Single CREATE OR REPLACE, safe to run while the pipeline is live.

CREATE OR REPLACE FUNCTION cleanup_bitdex_ops() RETURNS trigger AS $$
DECLARE
    _consumed_below BIGINT;
    _oldest BIGINT;
    _oldest_at TIMESTAMPTZ;
    _floor BIGINT;
    _chunk BIGINT := 5000;
BEGIN
    SELECT MIN(last_outbox_id) INTO _consumed_below FROM bitdex_cursors;
    IF _consumed_below IS NULL THEN
        RETURN NEW;
    END IF;

    SELECT id, created_at INTO _oldest, _oldest_at
    FROM "BitdexOps" ORDER BY id LIMIT 1;
    IF _oldest IS NULL THEN
        RETURN NEW;
    END IF;

    -- Two O(1) early exits before the probe. MEASURED, not assumed: an
    -- EXPLAIN (ANALYZE, BUFFERS) of the probe against prod in the caught-up
    -- state scanned the full 5,000-row window and removed every row by filter —
    -- 10.6ms, 989 buffers, to delete nothing. At several firings a second that
    -- is not noise.
    --
    --   1. the oldest row is itself still inside the window ⇒ nothing has
    --      expired yet (the caught-up steady state);
    --   2. nothing below the replicas' cursor ⇒ nothing is deletable even if it
    --      has expired (a stalled or restarted replica).
    IF _oldest_at >= now() - interval '15 minutes' THEN
        RETURN NEW;
    END IF;
    IF _consumed_below <= _oldest THEN
        RETURN NEW;
    END IF;

    SELECT id INTO _floor
    FROM "BitdexOps"
    WHERE id >= _oldest
      AND id < _oldest + _chunk
      AND created_at >= now() - interval '15 minutes'
    ORDER BY id
    LIMIT 1;

    DELETE FROM "BitdexOps"
    WHERE id >= _oldest
      AND id < LEAST(_consumed_below, COALESCE(_floor, _oldest + _chunk));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Verify, a few minutes apart (expect the spread to hold at ~15 minutes and the
-- row count to plateau):
--   SELECT count(*), now()-min(created_at) AS span FROM "BitdexOps";
--   SELECT n_live_tup, n_dead_tup, last_autovacuum
--     FROM pg_stat_user_tables WHERE relname='BitdexOps';
--
-- Rollback — restore the pre-floor body:
--   CREATE OR REPLACE FUNCTION cleanup_bitdex_ops() RETURNS trigger AS $$
--   BEGIN
--       DELETE FROM "BitdexOps"
--       WHERE id < (SELECT MIN(last_outbox_id) FROM bitdex_cursors);
--       RETURN NEW;
--   END;
--   $$ LANGUAGE plpgsql;
