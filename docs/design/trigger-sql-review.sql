-- =======================================================================
-- BitDex Sync V2 — Trigger SQL Review
-- Generated from: deploy/configs/sync-config-civitai.yaml
-- Run `cargo test generate_trigger_sql_review_file` to regenerate
-- 
-- This file is for REVIEW ONLY. Do not execute manually.
-- bitdex-sync setup will execute these statements via run_setup_v2().
-- =======================================================================

-- -----------------------------------------------------------------------
-- Part 1: V2 Tables (BitdexOps + bitdex_cursors + cleanup trigger)
-- -----------------------------------------------------------------------


-- BitdexOps table (V2 ops pipeline)
CREATE TABLE IF NOT EXISTS "BitdexOps" (
    id BIGSERIAL PRIMARY KEY,
    entity_id BIGINT NOT NULL,
    ops JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_bitdex_ops_id ON "BitdexOps" (id);

-- Cursor tracking table for multi-replica ops consumption
CREATE TABLE IF NOT EXISTS bitdex_cursors (
    replica_id TEXT PRIMARY KEY,
    last_outbox_id BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Auto-cleanup trigger: when any replica reports its cursor, delete old ops
-- that ALL replicas have already consumed AND that are older than the
-- retention floor.
--
-- The consumed-by-all condition alone gives ~48s of retention in prod (rows die
-- as soon as both pods ack them), which makes every write-path defect
-- unobservable after the fact: by the time a bad document is noticed, the ops
-- that produced it are gone and the mechanism can only be inferred from
-- document state. The floor keeps a bounded window of consumed ops readable.
-- At the measured ~117 rows/s that is ~840k rows resident for 2h.
--
-- Deliberately NO index on created_at. This trigger fires on every cursor
-- report and the table takes ~117 inserts/s, so neither the extra write
-- amplification nor an unbounded scan is acceptable. Instead the DELETE walks
-- the PRIMARY KEY forward from the oldest row and stops after a bounded chunk:
-- ids are assigned in insert order, so "oldest by id" is "oldest by created_at",
-- and in steady state the first rows the scan meets are already past the floor.
-- Work per firing is capped at _chunk rows and is LESS than the previous
-- unbounded `DELETE ... WHERE id < MIN(cursor)`.
--
-- The window is bounded by ID ARITHMETIC, not by a match count: `LIMIT n` on a
-- created_at predicate would, once the floor has caught up, walk every retained
-- row looking for matches it will not find. An id window of fixed width cannot.
--
-- Chunk sizing: at ~117 rows/s a single firing of 5,000 covers ~43s of backlog,
-- and the trigger fires several times a second (once per replica poll), so the
-- floor keeps up with a wide margin and recovers from a stall.
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

DROP TRIGGER IF EXISTS trg_cleanup_bitdex_ops ON bitdex_cursors;
CREATE TRIGGER trg_cleanup_bitdex_ops
    AFTER INSERT OR UPDATE ON bitdex_cursors
    FOR EACH ROW EXECUTE FUNCTION cleanup_bitdex_ops();


-- -----------------------------------------------------------------------
-- Part 2: Per-table triggers (generated from sync config YAML)
-- -----------------------------------------------------------------------

-- [1/8] Table: Image → Trigger: bitdex_image_dda29cf8
-- Sets alive: yes
-- On delete: emit delete op

CREATE OR REPLACE FUNCTION bitdex_image_sortat_ops(_i "Image") RETURNS jsonb AS $$
  SELECT jsonb_build_array(
    jsonb_build_object('op', 'set', 'field', 'sortAt', 'value', to_jsonb(COALESCE(extract(epoch from _i."sortAt")::bigint, GREATEST(extract(epoch from (SELECT p."publishedAt" FROM "Post" p WHERE p.id = _i."postId"))::bigint, extract(epoch from _i."scannedAt")::bigint, extract(epoch from _i."createdAt")::bigint))))
  );
$$ LANGUAGE sql STABLE;

CREATE OR REPLACE FUNCTION bitdex_image_ops_dda29cf8() RETURNS trigger AS $$
DECLARE
  _ops jsonb;
BEGIN
  IF TG_OP = 'INSERT' THEN
    _ops := jsonb_build_array(
      jsonb_build_object('op', 'alive'),
      jsonb_build_object('op', 'set', 'field', 'nsfwLevel', 'value', to_jsonb(NEW."nsfwLevel")),
      jsonb_build_object('op', 'set', 'field', 'type', 'value', to_jsonb(NEW."type"::text)),
      jsonb_build_object('op', 'set', 'field', 'userId', 'value', to_jsonb(NEW."userId")),
      jsonb_build_object('op', 'set', 'field', 'postId', 'value', to_jsonb(NEW."postId")),
      jsonb_build_object('op', 'set', 'field', 'blockedFor', 'value', to_jsonb(NEW."blockedFor")),
      jsonb_build_object('op', 'set', 'field', 'url', 'value', to_jsonb(NEW."url")),
      jsonb_build_object('op', 'set', 'field', 'hash', 'value', to_jsonb(NEW."hash")),
      jsonb_build_object('op', 'set', 'field', 'width', 'value', to_jsonb(NEW."width")),
      jsonb_build_object('op', 'set', 'field', 'height', 'value', to_jsonb(NEW."height")),
      jsonb_build_object('op', 'set', 'field', 'hasMeta', 'value', to_jsonb((NEW."flags" >> 13) & 1 = 1 AND (NEW."flags" >> 2) & 1 = 0)),
      jsonb_build_object('op', 'set', 'field', 'onSite', 'value', to_jsonb((NEW."flags" >> 14) & 1 = 1)),
      jsonb_build_object('op', 'set', 'field', 'minor', 'value', to_jsonb((NEW."flags" >> 3) & 1 = 1)),
      jsonb_build_object('op', 'set', 'field', 'poi', 'value', to_jsonb((NEW."flags" >> 4) & 1 = 1)),
      jsonb_build_object('op', 'set', 'field', 'existedAt', 'value', to_jsonb(GREATEST(extract(epoch from NEW."scannedAt")::bigint, extract(epoch from NEW."createdAt")::bigint))),
      jsonb_build_object('op', 'set', 'field', 'publishedAt', 'value', to_jsonb((SELECT extract(epoch from p."publishedAt")::bigint FROM "Post" p WHERE p.id = NEW."postId"))),
      jsonb_build_object('op', 'set', 'field', 'availability', 'value', to_jsonb((SELECT p."availability"::text FROM "Post" p WHERE p.id = NEW."postId"))),
      jsonb_build_object('op', 'set', 'field', 'postedToId', 'value', to_jsonb((SELECT p."modelVersionId" FROM "Post" p WHERE p.id = NEW."postId"))),
      jsonb_build_object('op', 'set', 'field', 'model3dId', 'value', to_jsonb((SELECT p."model3dId" FROM "Post" p WHERE p.id = NEW."postId")))
    );
    _ops := _ops || bitdex_image_sortat_ops(NEW);
    INSERT INTO "BitdexOps" (entity_id, ops) VALUES (NEW."id", _ops);
    RETURN NEW;
  ELSIF TG_OP = 'DELETE' THEN
    INSERT INTO "BitdexOps" (entity_id, ops) VALUES (OLD."id", '[{"op":"delete"}]'::jsonb);
    RETURN OLD;
  ELSE
    _ops := '[]'::jsonb;
    IF (OLD."nsfwLevel") IS DISTINCT FROM (NEW."nsfwLevel") THEN
      _ops := _ops || jsonb_build_array(
        jsonb_build_object('op', 'remove', 'field', 'nsfwLevel', 'value', to_jsonb(OLD."nsfwLevel")),
        jsonb_build_object('op', 'set', 'field', 'nsfwLevel', 'value', to_jsonb(NEW."nsfwLevel"))
      );
    END IF;
    IF (OLD."type"::text) IS DISTINCT FROM (NEW."type"::text) THEN
      _ops := _ops || jsonb_build_array(
        jsonb_build_object('op', 'remove', 'field', 'type', 'value', to_jsonb(OLD."type"::text)),
        jsonb_build_object('op', 'set', 'field', 'type', 'value', to_jsonb(NEW."type"::text))
      );
    END IF;
    IF (OLD."userId") IS DISTINCT FROM (NEW."userId") THEN
      _ops := _ops || jsonb_build_array(
        jsonb_build_object('op', 'remove', 'field', 'userId', 'value', to_jsonb(OLD."userId")),
        jsonb_build_object('op', 'set', 'field', 'userId', 'value', to_jsonb(NEW."userId"))
      );
    END IF;
    IF (OLD."postId") IS DISTINCT FROM (NEW."postId") THEN
      _ops := _ops || jsonb_build_array(
        jsonb_build_object('op', 'remove', 'field', 'postId', 'value', to_jsonb(OLD."postId")),
        jsonb_build_object('op', 'set', 'field', 'postId', 'value', to_jsonb(NEW."postId"))
      );
    END IF;
    IF (OLD."blockedFor") IS DISTINCT FROM (NEW."blockedFor") THEN
      _ops := _ops || jsonb_build_array(
        jsonb_build_object('op', 'remove', 'field', 'blockedFor', 'value', to_jsonb(OLD."blockedFor")),
        jsonb_build_object('op', 'set', 'field', 'blockedFor', 'value', to_jsonb(NEW."blockedFor"))
      );
    END IF;
    IF (OLD."url") IS DISTINCT FROM (NEW."url") THEN
      _ops := _ops || jsonb_build_array(
        jsonb_build_object('op', 'remove', 'field', 'url', 'value', to_jsonb(OLD."url")),
        jsonb_build_object('op', 'set', 'field', 'url', 'value', to_jsonb(NEW."url"))
      );
    END IF;
    IF (OLD."hash") IS DISTINCT FROM (NEW."hash") THEN
      _ops := _ops || jsonb_build_array(
        jsonb_build_object('op', 'remove', 'field', 'hash', 'value', to_jsonb(OLD."hash")),
        jsonb_build_object('op', 'set', 'field', 'hash', 'value', to_jsonb(NEW."hash"))
      );
    END IF;
    IF (OLD."width") IS DISTINCT FROM (NEW."width") THEN
      _ops := _ops || jsonb_build_array(
        jsonb_build_object('op', 'remove', 'field', 'width', 'value', to_jsonb(OLD."width")),
        jsonb_build_object('op', 'set', 'field', 'width', 'value', to_jsonb(NEW."width"))
      );
    END IF;
    IF (OLD."height") IS DISTINCT FROM (NEW."height") THEN
      _ops := _ops || jsonb_build_array(
        jsonb_build_object('op', 'remove', 'field', 'height', 'value', to_jsonb(OLD."height")),
        jsonb_build_object('op', 'set', 'field', 'height', 'value', to_jsonb(NEW."height"))
      );
    END IF;
    IF (COALESCE(extract(epoch from OLD."sortAt")::bigint, GREATEST(extract(epoch from (SELECT p."publishedAt" FROM "Post" p WHERE p.id = OLD."postId"))::bigint, extract(epoch from OLD."scannedAt")::bigint, extract(epoch from OLD."createdAt")::bigint))) IS DISTINCT FROM (COALESCE(extract(epoch from NEW."sortAt")::bigint, GREATEST(extract(epoch from (SELECT p."publishedAt" FROM "Post" p WHERE p.id = NEW."postId"))::bigint, extract(epoch from NEW."scannedAt")::bigint, extract(epoch from NEW."createdAt")::bigint))) THEN
      _ops := _ops || jsonb_build_array(
        jsonb_build_object('op', 'remove', 'field', 'sortAt', 'value', to_jsonb(COALESCE(extract(epoch from OLD."sortAt")::bigint, GREATEST(extract(epoch from (SELECT p."publishedAt" FROM "Post" p WHERE p.id = OLD."postId"))::bigint, extract(epoch from OLD."scannedAt")::bigint, extract(epoch from OLD."createdAt")::bigint)))),
        jsonb_build_object('op', 'set', 'field', 'sortAt', 'value', to_jsonb(COALESCE(extract(epoch from NEW."sortAt")::bigint, GREATEST(extract(epoch from (SELECT p."publishedAt" FROM "Post" p WHERE p.id = NEW."postId"))::bigint, extract(epoch from NEW."scannedAt")::bigint, extract(epoch from NEW."createdAt")::bigint))))
      );
    END IF;
    IF ((OLD."flags" >> 13) & 1 = 1 AND (OLD."flags" >> 2) & 1 = 0) IS DISTINCT FROM ((NEW."flags" >> 13) & 1 = 1 AND (NEW."flags" >> 2) & 1 = 0) THEN
      _ops := _ops || jsonb_build_array(
        jsonb_build_object('op', 'remove', 'field', 'hasMeta', 'value', to_jsonb((OLD."flags" >> 13) & 1 = 1 AND (OLD."flags" >> 2) & 1 = 0)),
        jsonb_build_object('op', 'set', 'field', 'hasMeta', 'value', to_jsonb((NEW."flags" >> 13) & 1 = 1 AND (NEW."flags" >> 2) & 1 = 0))
      );
    END IF;
    IF ((OLD."flags" >> 14) & 1 = 1) IS DISTINCT FROM ((NEW."flags" >> 14) & 1 = 1) THEN
      _ops := _ops || jsonb_build_array(
        jsonb_build_object('op', 'remove', 'field', 'onSite', 'value', to_jsonb((OLD."flags" >> 14) & 1 = 1)),
        jsonb_build_object('op', 'set', 'field', 'onSite', 'value', to_jsonb((NEW."flags" >> 14) & 1 = 1))
      );
    END IF;
    IF ((OLD."flags" >> 3) & 1 = 1) IS DISTINCT FROM ((NEW."flags" >> 3) & 1 = 1) THEN
      _ops := _ops || jsonb_build_array(
        jsonb_build_object('op', 'remove', 'field', 'minor', 'value', to_jsonb((OLD."flags" >> 3) & 1 = 1)),
        jsonb_build_object('op', 'set', 'field', 'minor', 'value', to_jsonb((NEW."flags" >> 3) & 1 = 1))
      );
    END IF;
    IF ((OLD."flags" >> 4) & 1 = 1) IS DISTINCT FROM ((NEW."flags" >> 4) & 1 = 1) THEN
      _ops := _ops || jsonb_build_array(
        jsonb_build_object('op', 'remove', 'field', 'poi', 'value', to_jsonb((OLD."flags" >> 4) & 1 = 1)),
        jsonb_build_object('op', 'set', 'field', 'poi', 'value', to_jsonb((NEW."flags" >> 4) & 1 = 1))
      );
    END IF;
    IF (GREATEST(extract(epoch from OLD."scannedAt")::bigint, extract(epoch from OLD."createdAt")::bigint)) IS DISTINCT FROM (GREATEST(extract(epoch from NEW."scannedAt")::bigint, extract(epoch from NEW."createdAt")::bigint)) THEN
      _ops := _ops || jsonb_build_array(
        jsonb_build_object('op', 'remove', 'field', 'existedAt', 'value', to_jsonb(GREATEST(extract(epoch from OLD."scannedAt")::bigint, extract(epoch from OLD."createdAt")::bigint))),
        jsonb_build_object('op', 'set', 'field', 'existedAt', 'value', to_jsonb(GREATEST(extract(epoch from NEW."scannedAt")::bigint, extract(epoch from NEW."createdAt")::bigint)))
      );
    END IF;
    IF ((SELECT extract(epoch from p."publishedAt")::bigint FROM "Post" p WHERE p.id = OLD."postId")) IS DISTINCT FROM ((SELECT extract(epoch from p."publishedAt")::bigint FROM "Post" p WHERE p.id = NEW."postId")) THEN
      _ops := _ops || jsonb_build_array(
        jsonb_build_object('op', 'remove', 'field', 'publishedAt', 'value', to_jsonb((SELECT extract(epoch from p."publishedAt")::bigint FROM "Post" p WHERE p.id = OLD."postId"))),
        jsonb_build_object('op', 'set', 'field', 'publishedAt', 'value', to_jsonb((SELECT extract(epoch from p."publishedAt")::bigint FROM "Post" p WHERE p.id = NEW."postId")))
      );
    END IF;
    IF ((SELECT p."availability"::text FROM "Post" p WHERE p.id = OLD."postId")) IS DISTINCT FROM ((SELECT p."availability"::text FROM "Post" p WHERE p.id = NEW."postId")) THEN
      _ops := _ops || jsonb_build_array(
        jsonb_build_object('op', 'remove', 'field', 'availability', 'value', to_jsonb((SELECT p."availability"::text FROM "Post" p WHERE p.id = OLD."postId"))),
        jsonb_build_object('op', 'set', 'field', 'availability', 'value', to_jsonb((SELECT p."availability"::text FROM "Post" p WHERE p.id = NEW."postId")))
      );
    END IF;
    IF ((SELECT p."modelVersionId" FROM "Post" p WHERE p.id = OLD."postId")) IS DISTINCT FROM ((SELECT p."modelVersionId" FROM "Post" p WHERE p.id = NEW."postId")) THEN
      _ops := _ops || jsonb_build_array(
        jsonb_build_object('op', 'remove', 'field', 'postedToId', 'value', to_jsonb((SELECT p."modelVersionId" FROM "Post" p WHERE p.id = OLD."postId"))),
        jsonb_build_object('op', 'set', 'field', 'postedToId', 'value', to_jsonb((SELECT p."modelVersionId" FROM "Post" p WHERE p.id = NEW."postId")))
      );
    END IF;
    IF ((SELECT p."model3dId" FROM "Post" p WHERE p.id = OLD."postId")) IS DISTINCT FROM ((SELECT p."model3dId" FROM "Post" p WHERE p.id = NEW."postId")) THEN
      _ops := _ops || jsonb_build_array(
        jsonb_build_object('op', 'remove', 'field', 'model3dId', 'value', to_jsonb((SELECT p."model3dId" FROM "Post" p WHERE p.id = OLD."postId"))),
        jsonb_build_object('op', 'set', 'field', 'model3dId', 'value', to_jsonb((SELECT p."model3dId" FROM "Post" p WHERE p.id = NEW."postId")))
      );
    END IF;
    IF jsonb_array_length(_ops) > 0 THEN
      INSERT INTO "BitdexOps" (entity_id, ops) VALUES (NEW."id", _ops);
    END IF;
    RETURN NEW;
  END IF;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS bitdex_image_dda29cf8 ON "Image";
CREATE TRIGGER bitdex_image_dda29cf8 AFTER INSERT OR UPDATE OR DELETE ON "Image"
  FOR EACH ROW EXECUTE FUNCTION bitdex_image_ops_dda29cf8();
ALTER TABLE "Image" ENABLE ALWAYS TRIGGER bitdex_image_dda29cf8;


-- [2/8] Table: TagsOnImageNew → Trigger: bitdex_tagsonimagenew_bcbef3c3

CREATE OR REPLACE FUNCTION bitdex_tagsonimagenew_ops_bcbef3c3() RETURNS trigger AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    IF (NEW."attributes" >> 10) & 1 = 0 THEN
    INSERT INTO "BitdexOps" (entity_id, ops)
    VALUES (NEW."imageId", jsonb_build_array(
      jsonb_build_object('op', 'add', 'field', 'tagIds', 'value', to_jsonb(NEW."tagId"))
    ));
    END IF;
    RETURN NEW;
  ELSIF TG_OP = 'DELETE' THEN
    IF (OLD."attributes" >> 10) & 1 = 0 THEN
    INSERT INTO "BitdexOps" (entity_id, ops)
    VALUES (OLD."imageId", jsonb_build_array(
      jsonb_build_object('op', 'remove', 'field', 'tagIds', 'value', to_jsonb(OLD."tagId"))
    ));
    END IF;
    RETURN OLD;
  END IF;
  RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS bitdex_tagsonimagenew_bcbef3c3 ON "TagsOnImageNew";
CREATE TRIGGER bitdex_tagsonimagenew_bcbef3c3 AFTER INSERT OR DELETE ON "TagsOnImageNew"
  FOR EACH ROW EXECUTE FUNCTION bitdex_tagsonimagenew_ops_bcbef3c3();
ALTER TABLE "TagsOnImageNew" ENABLE ALWAYS TRIGGER bitdex_tagsonimagenew_bcbef3c3;


-- [3/8] Table: ImageTool → Trigger: bitdex_imagetool_f87e1fc4

CREATE OR REPLACE FUNCTION bitdex_imagetool_ops_f87e1fc4() RETURNS trigger AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    INSERT INTO "BitdexOps" (entity_id, ops)
    VALUES (NEW."imageId", jsonb_build_array(
      jsonb_build_object('op', 'add', 'field', 'toolIds', 'value', to_jsonb(NEW."toolId"))
    ));
    RETURN NEW;
  ELSIF TG_OP = 'DELETE' THEN
    INSERT INTO "BitdexOps" (entity_id, ops)
    VALUES (OLD."imageId", jsonb_build_array(
      jsonb_build_object('op', 'remove', 'field', 'toolIds', 'value', to_jsonb(OLD."toolId"))
    ));
    RETURN OLD;
  END IF;
  RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS bitdex_imagetool_f87e1fc4 ON "ImageTool";
CREATE TRIGGER bitdex_imagetool_f87e1fc4 AFTER INSERT OR DELETE ON "ImageTool"
  FOR EACH ROW EXECUTE FUNCTION bitdex_imagetool_ops_f87e1fc4();
ALTER TABLE "ImageTool" ENABLE ALWAYS TRIGGER bitdex_imagetool_f87e1fc4;


-- [4/8] Table: ImageTechnique → Trigger: bitdex_imagetechnique_ee2b2860

CREATE OR REPLACE FUNCTION bitdex_imagetechnique_ops_ee2b2860() RETURNS trigger AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    INSERT INTO "BitdexOps" (entity_id, ops)
    VALUES (NEW."imageId", jsonb_build_array(
      jsonb_build_object('op', 'add', 'field', 'techniqueIds', 'value', to_jsonb(NEW."techniqueId"))
    ));
    RETURN NEW;
  ELSIF TG_OP = 'DELETE' THEN
    INSERT INTO "BitdexOps" (entity_id, ops)
    VALUES (OLD."imageId", jsonb_build_array(
      jsonb_build_object('op', 'remove', 'field', 'techniqueIds', 'value', to_jsonb(OLD."techniqueId"))
    ));
    RETURN OLD;
  END IF;
  RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS bitdex_imagetechnique_ee2b2860 ON "ImageTechnique";
CREATE TRIGGER bitdex_imagetechnique_ee2b2860 AFTER INSERT OR DELETE ON "ImageTechnique"
  FOR EACH ROW EXECUTE FUNCTION bitdex_imagetechnique_ops_ee2b2860();
ALTER TABLE "ImageTechnique" ENABLE ALWAYS TRIGGER bitdex_imagetechnique_ee2b2860;


-- [5/8] Table: ImageResourceNew → Trigger: bitdex_imageresourcenew_d84d15a8

CREATE OR REPLACE FUNCTION bitdex_imageresourcenew_ops_d84d15a8() RETURNS trigger AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    INSERT INTO "BitdexOps" (entity_id, ops)
    VALUES (NEW."imageId", jsonb_build_array(
      jsonb_build_object('op', 'add', 'field', 'modelVersionIds', 'value', to_jsonb(NEW."modelVersionId"))
    ));
    IF NEW."detected" = false THEN
      INSERT INTO "BitdexOps" (entity_id, ops)
      VALUES (NEW."imageId", jsonb_build_array(
        jsonb_build_object('op', 'add', 'field', 'modelVersionIdsManual', 'value', to_jsonb(NEW."modelVersionId"))
      ));
    END IF;
    RETURN NEW;
  ELSIF TG_OP = 'DELETE' THEN
    INSERT INTO "BitdexOps" (entity_id, ops)
    VALUES (OLD."imageId", jsonb_build_array(
      jsonb_build_object('op', 'remove', 'field', 'modelVersionIds', 'value', to_jsonb(OLD."modelVersionId"))
    ));
    IF OLD."detected" = false THEN
      INSERT INTO "BitdexOps" (entity_id, ops)
      VALUES (OLD."imageId", jsonb_build_array(
        jsonb_build_object('op', 'remove', 'field', 'modelVersionIdsManual', 'value', to_jsonb(OLD."modelVersionId"))
      ));
    END IF;
    RETURN OLD;
  END IF;
  RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS bitdex_imageresourcenew_d84d15a8 ON "ImageResourceNew";
CREATE TRIGGER bitdex_imageresourcenew_d84d15a8 AFTER INSERT OR DELETE ON "ImageResourceNew"
  FOR EACH ROW EXECUTE FUNCTION bitdex_imageresourcenew_ops_d84d15a8();
ALTER TABLE "ImageResourceNew" ENABLE ALWAYS TRIGGER bitdex_imageresourcenew_d84d15a8;


-- [6/8] Table: Post → Trigger: bitdex_post_c85c5a80
-- Type: fan_out_per_row

CREATE OR REPLACE FUNCTION bitdex_post_fanout_ops(_p "Post") RETURNS jsonb AS $$
  SELECT jsonb_build_array(
    CASE WHEN _p."publishedAt" IS NULL OR _p."publishedAt" > now()
      THEN jsonb_build_object('op', 'remove', 'field', 'publishedAt', 'value', to_jsonb(extract(epoch from _p."publishedAt")::bigint))
      ELSE jsonb_build_object('op', 'set', 'field', 'publishedAt', 'value', to_jsonb(extract(epoch from _p."publishedAt")::bigint))
    END,
    jsonb_build_object('op', 'set', 'field', 'availability', 'value', to_jsonb(_p."availability"::text)),
    jsonb_build_object('op', 'set', 'field', 'postedToId', 'value', to_jsonb(_p."modelVersionId"))
  );
$$ LANGUAGE sql STABLE;

CREATE OR REPLACE FUNCTION bitdex_post_ops_c85c5a80() RETURNS trigger AS $$
DECLARE
  _ops jsonb;
BEGIN
  IF TG_OP = 'INSERT' THEN
    _ops := bitdex_post_fanout_ops(NEW);
    INSERT INTO "BitdexOps" (entity_id, ops)
      SELECT c."id", _ops FROM "Image" c WHERE c."postId" = NEW."id";
    RETURN NEW;
  ELSIF TG_OP = 'UPDATE' THEN
    _ops := '[]'::jsonb;
    IF (extract(epoch from OLD."publishedAt")::bigint) IS DISTINCT FROM (extract(epoch from NEW."publishedAt")::bigint) THEN
      _ops := _ops || CASE WHEN NEW."publishedAt" IS NULL OR NEW."publishedAt" > now()
        THEN jsonb_build_array(
          jsonb_build_object('op', 'remove', 'field', 'publishedAt', 'value', to_jsonb(extract(epoch from NEW."publishedAt")::bigint))
        )
        ELSE jsonb_build_array(
          jsonb_build_object('op', 'remove', 'field', 'publishedAt', 'value', to_jsonb(extract(epoch from OLD."publishedAt")::bigint)),
          jsonb_build_object('op', 'set', 'field', 'publishedAt', 'value', to_jsonb(extract(epoch from NEW."publishedAt")::bigint))
        )
      END;
    END IF;
    IF (OLD."availability"::text) IS DISTINCT FROM (NEW."availability"::text) THEN
      _ops := _ops || jsonb_build_array(
        jsonb_build_object('op', 'remove', 'field', 'availability', 'value', to_jsonb(OLD."availability"::text)),
        jsonb_build_object('op', 'set', 'field', 'availability', 'value', to_jsonb(NEW."availability"::text))
      );
    END IF;
    IF (OLD."modelVersionId") IS DISTINCT FROM (NEW."modelVersionId") THEN
      _ops := _ops || jsonb_build_array(
        jsonb_build_object('op', 'remove', 'field', 'postedToId', 'value', to_jsonb(OLD."modelVersionId")),
        jsonb_build_object('op', 'set', 'field', 'postedToId', 'value', to_jsonb(NEW."modelVersionId"))
      );
    END IF;
    IF jsonb_array_length(_ops) > 0 THEN
      INSERT INTO "BitdexOps" (entity_id, ops)
        SELECT c."id", _ops FROM "Image" c WHERE c."postId" = NEW."id";
    END IF;
    RETURN NEW;
  END IF;
  RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS bitdex_post_c85c5a80 ON "Post";
CREATE TRIGGER bitdex_post_c85c5a80 AFTER INSERT OR UPDATE ON "Post"
  FOR EACH ROW EXECUTE FUNCTION bitdex_post_ops_c85c5a80();
ALTER TABLE "Post" ENABLE ALWAYS TRIGGER bitdex_post_c85c5a80;


-- [7/8] Table: ModelVersion → Trigger: bitdex_modelversion_22dd59b3
-- Type: fan_out

CREATE OR REPLACE FUNCTION bitdex_modelversion_ops_22dd59b3() RETURNS trigger AS $$
DECLARE
  _ops jsonb;
  _query text;
BEGIN
  IF TG_OP = 'INSERT' THEN
    _query := 'modelVersionIds eq ' || NEW."id"::text;
    _ops := jsonb_build_array(
      jsonb_build_object('op', 'set', 'field', 'baseModel', 'value', to_jsonb(NEW."baseModel"))
    );
    INSERT INTO "BitdexOps" (entity_id, ops) VALUES (NEW.id, jsonb_build_array(
        jsonb_build_object('op', 'queryOpSet', 'query', _query, 'ops', _ops)
      ));
    RETURN NEW;
  ELSIF TG_OP = 'UPDATE' THEN
    _query := 'modelVersionIds eq ' || NEW."id"::text;
    _ops := '[]'::jsonb;
    IF (OLD."baseModel") IS DISTINCT FROM (NEW."baseModel") THEN
      _ops := _ops || jsonb_build_array(
        jsonb_build_object('op', 'remove', 'field', 'baseModel', 'value', to_jsonb(OLD."baseModel")),
        jsonb_build_object('op', 'set', 'field', 'baseModel', 'value', to_jsonb(NEW."baseModel"))
      );
    END IF;
    IF jsonb_array_length(_ops) > 0 THEN
      INSERT INTO "BitdexOps" (entity_id, ops) VALUES (NEW.id, jsonb_build_array(
        jsonb_build_object('op', 'queryOpSet', 'query', _query, 'ops', _ops)
      ));
    END IF;
    RETURN NEW;
  END IF;
  RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS bitdex_modelversion_22dd59b3 ON "ModelVersion";
CREATE TRIGGER bitdex_modelversion_22dd59b3 AFTER INSERT OR UPDATE ON "ModelVersion"
  FOR EACH ROW EXECUTE FUNCTION bitdex_modelversion_ops_22dd59b3();
ALTER TABLE "ModelVersion" ENABLE ALWAYS TRIGGER bitdex_modelversion_22dd59b3;


-- [8/8] Table: Model → Trigger: bitdex_model_a13d0fe3
-- Type: fan_out

CREATE OR REPLACE FUNCTION bitdex_model_ops_a13d0fe3() RETURNS trigger AS $$
DECLARE
  _ops jsonb;
  _query text;
  _source_result jsonb;
BEGIN
  IF TG_OP = 'INSERT' THEN
    EXECUTE 'SELECT json_agg(mv.id) as ids FROM "ModelVersion" mv WHERE mv."modelId" = $1' INTO _source_result USING NEW."id";
    _query := 'modelVersionIds in [' || _source_result::text || ']';
    _ops := jsonb_build_array(
      jsonb_build_object('op', 'set', 'field', 'poi', 'value', to_jsonb(NEW."poi"))
    );
    INSERT INTO "BitdexOps" (entity_id, ops) VALUES (NEW.id, jsonb_build_array(
        jsonb_build_object('op', 'queryOpSet', 'query', _query, 'ops', _ops)
      ));
    RETURN NEW;
  ELSIF TG_OP = 'UPDATE' THEN
    EXECUTE 'SELECT json_agg(mv.id) as ids FROM "ModelVersion" mv WHERE mv."modelId" = $1' INTO _source_result USING NEW."id";
    _query := 'modelVersionIds in [' || _source_result::text || ']';
    _ops := '[]'::jsonb;
    IF (OLD."poi") IS DISTINCT FROM (NEW."poi") THEN
      _ops := _ops || jsonb_build_array(
        jsonb_build_object('op', 'remove', 'field', 'poi', 'value', to_jsonb(OLD."poi")),
        jsonb_build_object('op', 'set', 'field', 'poi', 'value', to_jsonb(NEW."poi"))
      );
    END IF;
    IF jsonb_array_length(_ops) > 0 THEN
      INSERT INTO "BitdexOps" (entity_id, ops) VALUES (NEW.id, jsonb_build_array(
        jsonb_build_object('op', 'queryOpSet', 'query', _query, 'ops', _ops)
      ));
    END IF;
    RETURN NEW;
  END IF;
  RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS bitdex_model_a13d0fe3 ON "Model";
CREATE TRIGGER bitdex_model_a13d0fe3 AFTER INSERT OR UPDATE ON "Model"
  FOR EACH ROW EXECUTE FUNCTION bitdex_model_ops_a13d0fe3();
ALTER TABLE "Model" ENABLE ALWAYS TRIGGER bitdex_model_a13d0fe3;


-- -----------------------------------------------------------------------
-- Summary
-- -----------------------------------------------------------------------
-- Tables created: BitdexOps, bitdex_cursors
-- Triggers: 8
--   bitdex_image_dda29cf8 on "Image"
--   bitdex_tagsonimagenew_bcbef3c3 on "TagsOnImageNew"
--   bitdex_imagetool_f87e1fc4 on "ImageTool"
--   bitdex_imagetechnique_ee2b2860 on "ImageTechnique"
--   bitdex_imageresourcenew_d84d15a8 on "ImageResourceNew"
--   bitdex_post_c85c5a80 on "Post"
--   bitdex_modelversion_22dd59b3 on "ModelVersion"
--   bitdex_model_a13d0fe3 on "Model"
--
-- Safety notes:
-- - All triggers use CREATE OR REPLACE (idempotent)
-- - All triggers use ENABLE ALWAYS (works with replication)
-- - Trigger functions ONLY INSERT into "BitdexOps" (no other mutations)
-- - No table locks, no schema changes to existing tables
-- - BitdexOps cleanup trigger only DELETEs consumed ops rows
