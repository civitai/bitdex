-- Retry pass for triggers that lost the deadlock race in nuke-pg-state.sql.
--
-- Hot tables (Image, ImageResourceNew, Post, ModelVersion) frequently hold
-- AccessShareLock under civitai app traffic and reject DROP TRIGGER on the
-- first pass. Run this script after nuke-pg-state.sql if the
-- "remaining_triggers" count is non-zero. If 4 or fewer triggers stay behind
-- after a retry pass, that's fine — bitdex-sync's setup_v2 reconciles them
-- at boot since trigger-name hashes match the sync config.
--
-- This script uses a longer lock_timeout (10s) and up to 8 retries per
-- trigger with pg_sleep(2) between attempts.
--
-- Run via: kubectl exec -i cnpg-cluster-nvme0-3 -n cnpg-database -c postgres
--   -- psql -U postgres -d civitai < this-file.sql
SET lock_timeout = '10s';
SET statement_timeout = '60s';

DO $$
DECLARE
  r RECORD;
  attempts INT;
  done BOOLEAN;
BEGIN
  FOR r IN
    SELECT t.tgname AS trig_name, c.relname AS tbl_name
    FROM pg_trigger t
    JOIN pg_class c ON t.tgrelid = c.oid
    WHERE t.tgname LIKE 'bitdex_%'
      AND NOT t.tgisinternal
  LOOP
    attempts := 0;
    done := false;
    WHILE NOT done AND attempts < 8 LOOP
      attempts := attempts + 1;
      BEGIN
        EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I', r.trig_name, r.tbl_name);
        done := true;
        RAISE NOTICE 'Dropped % on % (attempt %)', r.trig_name, r.tbl_name, attempts;
      EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Attempt %: failed to drop % on %: %', attempts, r.trig_name, r.tbl_name, SQLERRM;
        PERFORM pg_sleep(2);
      END;
    END LOOP;
  END LOOP;
END $$;

TRUNCATE TABLE "BitdexOps" RESTART IDENTITY;
TRUNCATE TABLE bitdex_cursors RESTART IDENTITY;

SELECT count(*) AS remaining_triggers FROM pg_trigger WHERE tgname LIKE 'bitdex_%' AND NOT tgisinternal;
SELECT count(*) AS remaining_ops FROM "BitdexOps";
SELECT count(*) AS remaining_cursors FROM bitdex_cursors;
