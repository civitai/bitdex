-- Minimal schema for BitDex integration tests.
-- Creates tables only — NO initial data inserted here.
-- Data is inserted by the test script after outbox triggers are set up,
-- so everything flows through the outbox → sidecar → BitDex pipeline.

-- Post table (required by Image FK and trigger)
CREATE TABLE IF NOT EXISTS "Post" (
    id BIGSERIAL PRIMARY KEY,
    "publishedAt" TIMESTAMPTZ DEFAULT now(),
    availability TEXT DEFAULT 'public',
    "modelVersionId" BIGINT
);

-- Image table (core entity)
CREATE TABLE IF NOT EXISTS "Image" (
    id BIGSERIAL PRIMARY KEY,
    "postId" BIGINT NOT NULL REFERENCES "Post"(id),
    url TEXT,
    "nsfwLevel" INT DEFAULT 1,
    hash TEXT,
    "hideMeta" BOOLEAN DEFAULT false,
    type TEXT DEFAULT 'image',
    "userId" BIGINT DEFAULT 1,
    minor BOOLEAN DEFAULT false,
    poi BOOLEAN DEFAULT false,
    "blockedFor" TEXT,
    "scannedAt" TIMESTAMPTZ,
    "createdAt" TIMESTAMPTZ DEFAULT now(),
    meta JSONB
);

-- Tags join table
CREATE TABLE IF NOT EXISTS "TagsOnImageNew" (
    "imageId" BIGINT NOT NULL REFERENCES "Image"(id),
    "tagId" BIGINT NOT NULL,
    PRIMARY KEY ("imageId", "tagId")
);

-- TagsOnImageDetails view (used by fetch_tags query)
CREATE OR REPLACE VIEW "TagsOnImageDetails" AS
    SELECT "imageId", "tagId", false AS disabled FROM "TagsOnImageNew";

-- Tool join table
CREATE TABLE IF NOT EXISTS "ImageTool" (
    "imageId" BIGINT NOT NULL REFERENCES "Image"(id),
    "toolId" BIGINT NOT NULL,
    PRIMARY KEY ("imageId", "toolId")
);

-- Technique join table
CREATE TABLE IF NOT EXISTS "ImageTechnique" (
    "imageId" BIGINT NOT NULL REFERENCES "Image"(id),
    "techniqueId" BIGINT NOT NULL,
    PRIMARY KEY ("imageId", "techniqueId")
);

-- Resource tables (simplified — no deep FK chain)
CREATE TABLE IF NOT EXISTS "Model" (
    id BIGSERIAL PRIMARY KEY,
    type TEXT DEFAULT 'Checkpoint',
    poi BOOLEAN DEFAULT false
);

CREATE TABLE IF NOT EXISTS "ModelVersion" (
    id BIGSERIAL PRIMARY KEY,
    "modelId" BIGINT REFERENCES "Model"(id),
    "baseModel" TEXT DEFAULT 'SD 1.5'
);

CREATE TABLE IF NOT EXISTS "ImageResourceNew" (
    "imageId" BIGINT NOT NULL REFERENCES "Image"(id),
    "modelVersionId" BIGINT NOT NULL REFERENCES "ModelVersion"(id),
    detected BOOLEAN DEFAULT true,
    PRIMARY KEY ("imageId", "modelVersionId")
);
