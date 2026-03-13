-- Schema + seed data for the bulk-load integration test.
--
-- This data is inserted BEFORE outbox triggers exist (by PG init),
-- simulating a production database with pre-existing records.
-- The bulk loader will read these rows directly from the tables.

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

-- ===== Pre-existing seed data =====
-- These rows exist BEFORE any outbox triggers, simulating production state.

INSERT INTO "Post" (id, "publishedAt", availability) VALUES
    (1, now(), 'public'),
    (2, now(), 'public'),
    (3, now(), 'public');

INSERT INTO "Image" (id, "postId", url, "nsfwLevel", type, "userId") VALUES
    (1,  1, 'seed-1.jpg',  1, 'image', 100),
    (2,  1, 'seed-2.jpg',  1, 'image', 100),
    (3,  1, 'seed-3.jpg',  2, 'image', 200),
    (4,  2, 'seed-4.jpg',  1, 'image', 200),
    (5,  2, 'seed-5.jpg',  1, 'image', 300),
    (6,  2, 'seed-6.jpg',  1, 'image', 300),
    (7,  3, 'seed-7.jpg',  1, 'image', 400),
    (8,  3, 'seed-8.jpg',  2, 'image', 400),
    (9,  3, 'seed-9.jpg',  1, 'image', 500),
    (10, 3, 'seed-10.jpg', 1, 'image', 500);

-- Tags: images 1-3 get a few tags, image 10 gets 55 tags (overflow test: >48 inline)
INSERT INTO "TagsOnImageNew" ("imageId", "tagId") VALUES
    (1, 100), (1, 101), (1, 102),
    (2, 100), (2, 200),
    (3, 300), (3, 301);

-- Image 10: 55 tags to exercise overflow handling (max inline = 48)
INSERT INTO "TagsOnImageNew" ("imageId", "tagId")
    SELECT 10, generate_series(1000, 1054);

-- Tools
INSERT INTO "ImageTool" ("imageId", "toolId") VALUES
    (1, 10), (2, 10), (3, 20);

-- Techniques
INSERT INTO "ImageTechnique" ("imageId", "techniqueId") VALUES
    (1, 30), (4, 31);

-- Resources: model + version + resource linkage
INSERT INTO "Model" (id, type, poi) VALUES (1, 'Checkpoint', false);
INSERT INTO "ModelVersion" (id, "modelId", "baseModel") VALUES (1, 1, 'SD 1.5');
INSERT INTO "ImageResourceNew" ("imageId", "modelVersionId", detected) VALUES
    (1, 1, true), (2, 1, true), (5, 1, false);
