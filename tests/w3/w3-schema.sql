-- Minimal civitai schema for W3 steady-state E2E (columns referenced by #326 triggers only).
-- Types match trigger expectations: date cols = timestamptz (extract epoch), flags = int (bitwise).

CREATE TABLE IF NOT EXISTS "Post" (
  id            BIGINT PRIMARY KEY,
  "publishedAt" TIMESTAMPTZ,
  "availability" TEXT DEFAULT 'Public',
  "modelVersionId" BIGINT,
  "model3dId"   INTEGER
);

CREATE TABLE IF NOT EXISTS "Image" (
  id            BIGINT PRIMARY KEY,
  url           TEXT,
  "nsfwLevel"   INTEGER DEFAULT 1,
  hash          TEXT,
  flags         INTEGER DEFAULT 0,
  "type"        TEXT DEFAULT 'image',
  "userId"      BIGINT,
  "blockedFor"  TEXT,
  "scannedAt"   TIMESTAMPTZ,
  "createdAt"   TIMESTAMPTZ DEFAULT now(),
  "sortAt"      TIMESTAMPTZ,          -- written by the model-share BEFORE trigger below
  "postId"      BIGINT,
  width         INTEGER,
  height        INTEGER
);
CREATE INDEX IF NOT EXISTS idx_image_postid ON "Image" ("postId");
CREATE INDEX IF NOT EXISTS idx_image_sortat ON "Image" ("sortAt");

CREATE TABLE IF NOT EXISTS "ModelVersion" (
  id           BIGINT PRIMARY KEY,
  "baseModel"  TEXT,
  "modelId"    BIGINT
);
CREATE TABLE IF NOT EXISTS "Model" (
  id     BIGINT PRIMARY KEY,
  poi    BOOLEAN DEFAULT false,
  "type" TEXT
);
CREATE TABLE IF NOT EXISTS "TagsOnImageNew" (
  "imageId"    BIGINT,
  "tagId"      BIGINT,
  "attributes" INTEGER DEFAULT 0,
  PRIMARY KEY ("imageId","tagId")
);
CREATE TABLE IF NOT EXISTS "ImageTool" (
  "imageId" BIGINT, "toolId" BIGINT, PRIMARY KEY ("imageId","toolId")
);
CREATE TABLE IF NOT EXISTS "ImageTechnique" (
  "imageId" BIGINT, "techniqueId" BIGINT, PRIMARY KEY ("imageId","techniqueId")
);
CREATE TABLE IF NOT EXISTS "ImageResourceNew" (
  "imageId" BIGINT, "modelVersionId" BIGINT, detected BOOLEAN DEFAULT true,
  PRIMARY KEY ("imageId","modelVersionId")
);

-- ==========================================================================
-- model-share migration equivalent (W1-2): PG-authored sortAt.
--   BEFORE INSERT OR UPDATE on Image: sortAt = GREATEST(post.publishedAt, scannedAt, createdAt)
--   AFTER UPDATE OF publishedAt on Post: recompute child images' sortAt (IS DISTINCT FROM guard)
-- ==========================================================================
CREATE OR REPLACE FUNCTION ms_image_sortat() RETURNS trigger AS $$
DECLARE _pub timestamptz;
BEGIN
  SELECT p."publishedAt" INTO _pub FROM "Post" p WHERE p.id = NEW."postId";
  NEW."sortAt" := GREATEST(_pub, NEW."scannedAt", NEW."createdAt");
  RETURN NEW;
END; $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS ms_image_sortat_trg ON "Image";
CREATE TRIGGER ms_image_sortat_trg BEFORE INSERT OR UPDATE ON "Image"
  FOR EACH ROW EXECUTE FUNCTION ms_image_sortat();

CREATE OR REPLACE FUNCTION ms_post_sortat_fanout() RETURNS trigger AS $$
BEGIN
  UPDATE "Image" i
     SET "sortAt" = GREATEST(NEW."publishedAt", i."scannedAt", i."createdAt")
   WHERE i."postId" = NEW.id
     AND i."sortAt" IS DISTINCT FROM GREATEST(NEW."publishedAt", i."scannedAt", i."createdAt");
  RETURN NEW;
END; $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS ms_post_sortat_fanout_trg ON "Post";
CREATE TRIGGER ms_post_sortat_fanout_trg AFTER UPDATE OF "publishedAt" ON "Post"
  FOR EACH ROW EXECUTE FUNCTION ms_post_sortat_fanout();
