-- Hard-nuke PG state for BitDex sync v2.
--
-- Runs as part of the full nuke flow (see docs/guide/deploy-nukes.md). Drops
-- every bitdex_* trigger + its function, then truncates the BitdexOps outbox
-- and bitdex_cursors tables. After this runs and the v201+ pod boots,
-- bitdex-sync's run_setup_v2 (src/bin/pg_sync.rs) reconciles triggers and
-- recreates BitdexOps/bitdex_cursors from sync config.
--
-- Why short lock_timeout + per-statement EXCEPTION blocks?
--   Hot tables (Image, ImageResourceNew, Post, ModelVersion) hold
--   AccessShareLock from concurrent civitai app traffic. DROP TRIGGER needs
--   AccessExclusiveLock and will lose the deadlock race on those tables.
--   Letting individual drops fail is fine — bitdex-sync's setup_v2 reconciles
--   the trigger set on next boot (trigger-name hashes match the config).
--
-- Why no outer transaction?
--   The whole point is to NOT block other workload. A wrapping BEGIN/COMMIT
--   would hold the outer DDL lock for the duration of every retry and starve
--   readers. Per-statement BEGIN ... EXCEPTION lets each drop fail in
--   isolation.
--
-- Run via: kubectl exec -i cnpg-cluster-nvme0-3 -n cnpg-database -c postgres
--   -- psql -U postgres -d civitai < this-file.sql
SET lock_timeout = '5s';
SET statement_timeout = '30s';

DO $$
DECLARE
  r RECORD;
BEGIN
  FOR r IN
    SELECT t.tgname AS trig_name, c.relname AS tbl_name
    FROM pg_trigger t
    JOIN pg_class c ON t.tgrelid = c.oid
    WHERE t.tgname LIKE 'bitdex_%'
      AND NOT t.tgisinternal
  LOOP
    BEGIN
      EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I', r.trig_name, r.tbl_name);
      RAISE NOTICE 'Dropped trigger % on %', r.trig_name, r.tbl_name;
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'Failed to drop trigger % on %: %', r.trig_name, r.tbl_name, SQLERRM;
    END;
    BEGIN
      EXECUTE format('DROP FUNCTION IF EXISTS %I() CASCADE', r.trig_name);
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'Failed to drop function %: %', r.trig_name, SQLERRM;
    END;
  END LOOP;
END $$;

TRUNCATE TABLE "BitdexOps" RESTART IDENTITY;
TRUNCATE TABLE bitdex_cursors RESTART IDENTITY;

SELECT count(*) AS remaining_triggers FROM pg_trigger WHERE tgname LIKE 'bitdex_%' AND NOT tgisinternal;
SELECT count(*) AS remaining_ops FROM "BitdexOps";
SELECT count(*) AS remaining_cursors FROM bitdex_cursors;
