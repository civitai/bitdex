-- =======================================================================
-- BitDex Sync V2 — Trigger SQL Review
-- Generated from: docs/design/sync-config-civitai.yaml
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
-- that ALL replicas have already consumed.
CREATE OR REPLACE FUNCTION cleanup_bitdex_ops() RETURNS trigger AS $$
BEGIN
    DELETE FROM "BitdexOps"
    WHERE id < (SELECT MIN(last_outbox_id) FROM bitdex_cursors);
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

-- [1/8] Table: Image → Trigger: bitdex_image_a48dc610
-- Sets alive: yes
-- On delete: emit delete op

CREATE OR REPLACE FUNCTION bitdex_image_ops_a48dc610() RETURNS trigger AS $$
DECLARE
  _ops jsonb;
BEGIN
  IF TG_OP = 'INSERT' THEN
    _ops := jsonb_build_array(
      jsonb_build_object('op', 'set', 'field', 'nsfwLevel', 'value', to_jsonb(NEW."nsfwLevel")),
      jsonb_build_object('op', 'set', 'field', 'type', 'value', to_jsonb(NEW."type"::text)),
      jsonb_build_object('op', 'set', 'field', 'userId', 'value', to_jsonb(NEW."userId")),
      jsonb_build_object('op', 'set', 'field', 'postId', 'value', to_jsonb(NEW."postId")),
      jsonb_build_object('op', 'set', 'field', 'blockedFor', 'value', to_jsonb(NEW."blockedFor")),
      jsonb_build_object('op', 'set', 'field', 'url', 'value', to_jsonb(NEW."url")),
      jsonb_build_object('op', 'set', 'field', 'hash', 'value', to_jsonb(NEW."hash")),
      jsonb_build_object('op', 'set', 'field', 'width', 'value', to_jsonb(NEW."width")),
      jsonb_build_object('op', 'set', 'field', 'height', 'value', to_jsonb(NEW."height"))
    );
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
    IF jsonb_array_length(_ops) > 0 THEN
      INSERT INTO "BitdexOps" (entity_id, ops) VALUES (NEW."id", _ops);
    END IF;
    RETURN NEW;
  END IF;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS bitdex_image_a48dc610 ON "Image";
CREATE TRIGGER bitdex_image_a48dc610 AFTER INSERT OR UPDATE OR DELETE ON "Image"
  FOR EACH ROW EXECUTE FUNCTION bitdex_image_ops_a48dc610();
ALTER TABLE "Image" ENABLE ALWAYS TRIGGER bitdex_image_a48dc610;


-- [2/8] Table: TagsOnImageNew → Trigger: bitdex_tagsonimagenew_47d53c08

CREATE OR REPLACE FUNCTION bitdex_tagsonimagenew_ops_47d53c08() RETURNS trigger AS $$
DECLARE
  _ops jsonb;
BEGIN
  IF TG_OP = 'INSERT' THEN
    _ops := jsonb_build_array(
      jsonb_build_object('op', 'set', 'field', 'tagIds', 'value', to_jsonb(NEW."tagIds"))
    );
    INSERT INTO "BitdexOps" (entity_id, ops) VALUES (NEW."imageId", _ops);
    RETURN NEW;
  ELSE
    _ops := '[]'::jsonb;
    IF (OLD."tagIds") IS DISTINCT FROM (NEW."tagIds") THEN
      _ops := _ops || jsonb_build_array(
        jsonb_build_object('op', 'remove', 'field', 'tagIds', 'value', to_jsonb(OLD."tagIds")),
        jsonb_build_object('op', 'set', 'field', 'tagIds', 'value', to_jsonb(NEW."tagIds"))
      );
    END IF;
    IF jsonb_array_length(_ops) > 0 THEN
      INSERT INTO "BitdexOps" (entity_id, ops) VALUES (NEW."imageId", _ops);
    END IF;
    RETURN NEW;
  END IF;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS bitdex_tagsonimagenew_47d53c08 ON "TagsOnImageNew";
CREATE TRIGGER bitdex_tagsonimagenew_47d53c08 AFTER INSERT OR UPDATE ON "TagsOnImageNew"
  FOR EACH ROW EXECUTE FUNCTION bitdex_tagsonimagenew_ops_47d53c08();
ALTER TABLE "TagsOnImageNew" ENABLE ALWAYS TRIGGER bitdex_tagsonimagenew_47d53c08;


-- [3/8] Table: ImageTool → Trigger: bitdex_imagetool_378dc25c

CREATE OR REPLACE FUNCTION bitdex_imagetool_ops_378dc25c() RETURNS trigger AS $$
DECLARE
  _ops jsonb;
BEGIN
  IF TG_OP = 'INSERT' THEN
    _ops := jsonb_build_array(
      jsonb_build_object('op', 'set', 'field', 'toolIds', 'value', to_jsonb(NEW."toolIds"))
    );
    INSERT INTO "BitdexOps" (entity_id, ops) VALUES (NEW."imageId", _ops);
    RETURN NEW;
  ELSE
    _ops := '[]'::jsonb;
    IF (OLD."toolIds") IS DISTINCT FROM (NEW."toolIds") THEN
      _ops := _ops || jsonb_build_array(
        jsonb_build_object('op', 'remove', 'field', 'toolIds', 'value', to_jsonb(OLD."toolIds")),
        jsonb_build_object('op', 'set', 'field', 'toolIds', 'value', to_jsonb(NEW."toolIds"))
      );
    END IF;
    IF jsonb_array_length(_ops) > 0 THEN
      INSERT INTO "BitdexOps" (entity_id, ops) VALUES (NEW."imageId", _ops);
    END IF;
    RETURN NEW;
  END IF;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS bitdex_imagetool_378dc25c ON "ImageTool";
CREATE TRIGGER bitdex_imagetool_378dc25c AFTER INSERT OR UPDATE ON "ImageTool"
  FOR EACH ROW EXECUTE FUNCTION bitdex_imagetool_ops_378dc25c();
ALTER TABLE "ImageTool" ENABLE ALWAYS TRIGGER bitdex_imagetool_378dc25c;


-- [4/8] Table: ImageTechnique → Trigger: bitdex_imagetechnique_ef2d0321

CREATE OR REPLACE FUNCTION bitdex_imagetechnique_ops_ef2d0321() RETURNS trigger AS $$
DECLARE
  _ops jsonb;
BEGIN
  IF TG_OP = 'INSERT' THEN
    _ops := jsonb_build_array(
      jsonb_build_object('op', 'set', 'field', 'techniqueIds', 'value', to_jsonb(NEW."techniqueIds"))
    );
    INSERT INTO "BitdexOps" (entity_id, ops) VALUES (NEW."imageId", _ops);
    RETURN NEW;
  ELSE
    _ops := '[]'::jsonb;
    IF (OLD."techniqueIds") IS DISTINCT FROM (NEW."techniqueIds") THEN
      _ops := _ops || jsonb_build_array(
        jsonb_build_object('op', 'remove', 'field', 'techniqueIds', 'value', to_jsonb(OLD."techniqueIds")),
        jsonb_build_object('op', 'set', 'field', 'techniqueIds', 'value', to_jsonb(NEW."techniqueIds"))
      );
    END IF;
    IF jsonb_array_length(_ops) > 0 THEN
      INSERT INTO "BitdexOps" (entity_id, ops) VALUES (NEW."imageId", _ops);
    END IF;
    RETURN NEW;
  END IF;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS bitdex_imagetechnique_ef2d0321 ON "ImageTechnique";
CREATE TRIGGER bitdex_imagetechnique_ef2d0321 AFTER INSERT OR UPDATE ON "ImageTechnique"
  FOR EACH ROW EXECUTE FUNCTION bitdex_imagetechnique_ops_ef2d0321();
ALTER TABLE "ImageTechnique" ENABLE ALWAYS TRIGGER bitdex_imagetechnique_ef2d0321;


-- [5/8] Table: ImageResourceNew → Trigger: bitdex_imageresourcenew_c9957318

CREATE OR REPLACE FUNCTION bitdex_imageresourcenew_ops_c9957318() RETURNS trigger AS $$
DECLARE
  _ops jsonb;
BEGIN
  IF TG_OP = 'INSERT' THEN
    _ops := jsonb_build_array(
      jsonb_build_object('op', 'set', 'field', 'modelVersionIds', 'value', to_jsonb(NEW."modelVersionId"))
    );
    INSERT INTO "BitdexOps" (entity_id, ops) VALUES (NEW."imageId", _ops);
    RETURN NEW;
  ELSE
    _ops := '[]'::jsonb;
    IF (OLD."modelVersionId") IS DISTINCT FROM (NEW."modelVersionId") THEN
      _ops := _ops || jsonb_build_array(
        jsonb_build_object('op', 'remove', 'field', 'modelVersionIds', 'value', to_jsonb(OLD."modelVersionId")),
        jsonb_build_object('op', 'set', 'field', 'modelVersionIds', 'value', to_jsonb(NEW."modelVersionId"))
      );
    END IF;
    IF jsonb_array_length(_ops) > 0 THEN
      INSERT INTO "BitdexOps" (entity_id, ops) VALUES (NEW."imageId", _ops);
    END IF;
    RETURN NEW;
  END IF;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS bitdex_imageresourcenew_c9957318 ON "ImageResourceNew";
CREATE TRIGGER bitdex_imageresourcenew_c9957318 AFTER INSERT OR UPDATE ON "ImageResourceNew"
  FOR EACH ROW EXECUTE FUNCTION bitdex_imageresourcenew_ops_c9957318();
ALTER TABLE "ImageResourceNew" ENABLE ALWAYS TRIGGER bitdex_imageresourcenew_c9957318;


-- [6/8] Table: Post → Trigger: bitdex_post_562004c5
-- Type: fan_out

CREATE OR REPLACE FUNCTION bitdex_post_ops_562004c5() RETURNS trigger AS $$
DECLARE
  _ops jsonb;
  _query text;
BEGIN
  IF TG_OP = 'UPDATE' THEN
    _query := 'postId eq NEW."id"';
    _ops := '[]'::jsonb;
    IF (extract(epoch from OLD."publishedAt")::bigint) IS DISTINCT FROM (extract(epoch from NEW."publishedAt")::bigint) THEN
      _ops := _ops || jsonb_build_array(
        jsonb_build_object('op', 'remove', 'field', 'publishedAt', 'value', to_jsonb(extract(epoch from OLD."publishedAt")::bigint)),
        jsonb_build_object('op', 'set', 'field', 'publishedAt', 'value', to_jsonb(extract(epoch from NEW."publishedAt")::bigint))
      );
    END IF;
    IF (OLD."availability"::text) IS DISTINCT FROM (NEW."availability"::text) THEN
      _ops := _ops || jsonb_build_array(
        jsonb_build_object('op', 'remove', 'field', 'availability', 'value', to_jsonb(OLD."availability"::text)),
        jsonb_build_object('op', 'set', 'field', 'availability', 'value', to_jsonb(NEW."availability"::text))
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

DROP TRIGGER IF EXISTS bitdex_post_562004c5 ON "Post";
CREATE TRIGGER bitdex_post_562004c5 AFTER INSERT OR UPDATE ON "Post"
  FOR EACH ROW EXECUTE FUNCTION bitdex_post_ops_562004c5();
ALTER TABLE "Post" ENABLE ALWAYS TRIGGER bitdex_post_562004c5;


-- [7/8] Table: ModelVersion → Trigger: bitdex_modelversion_27f2c342
-- Type: fan_out

CREATE OR REPLACE FUNCTION bitdex_modelversion_ops_27f2c342() RETURNS trigger AS $$
DECLARE
  _ops jsonb;
  _query text;
BEGIN
  IF TG_OP = 'UPDATE' THEN
    _query := 'modelVersionIds eq NEW."id"';
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

DROP TRIGGER IF EXISTS bitdex_modelversion_27f2c342 ON "ModelVersion";
CREATE TRIGGER bitdex_modelversion_27f2c342 AFTER INSERT OR UPDATE ON "ModelVersion"
  FOR EACH ROW EXECUTE FUNCTION bitdex_modelversion_ops_27f2c342();
ALTER TABLE "ModelVersion" ENABLE ALWAYS TRIGGER bitdex_modelversion_27f2c342;


-- [8/8] Table: Model → Trigger: bitdex_model_d7157ed3
-- Type: fan_out

CREATE OR REPLACE FUNCTION bitdex_model_ops_d7157ed3() RETURNS trigger AS $$
DECLARE
  _ops jsonb;
  _query text;
BEGIN
  IF TG_OP = 'UPDATE' THEN
    _query := 'modelVersionIds in [NEW."ids"]';
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

DROP TRIGGER IF EXISTS bitdex_model_d7157ed3 ON "Model";
CREATE TRIGGER bitdex_model_d7157ed3 AFTER INSERT OR UPDATE ON "Model"
  FOR EACH ROW EXECUTE FUNCTION bitdex_model_ops_d7157ed3();
ALTER TABLE "Model" ENABLE ALWAYS TRIGGER bitdex_model_d7157ed3;


-- -----------------------------------------------------------------------
-- Summary
-- -----------------------------------------------------------------------
-- Tables created: BitdexOps, bitdex_cursors
-- Triggers: 8
--   bitdex_image_a48dc610 on "Image"
--   bitdex_tagsonimagenew_47d53c08 on "TagsOnImageNew"
--   bitdex_imagetool_378dc25c on "ImageTool"
--   bitdex_imagetechnique_ef2d0321 on "ImageTechnique"
--   bitdex_imageresourcenew_c9957318 on "ImageResourceNew"
--   bitdex_post_562004c5 on "Post"
--   bitdex_modelversion_27f2c342 on "ModelVersion"
--   bitdex_model_d7157ed3 on "Model"
--
-- Safety notes:
-- - All triggers use CREATE OR REPLACE (idempotent)
-- - All triggers use ENABLE ALWAYS (works with replication)
-- - Trigger functions ONLY INSERT into "BitdexOps" (no other mutations)
-- - No table locks, no schema changes to existing tables
-- - BitdexOps cleanup trigger only DELETEs consumed ops rows
