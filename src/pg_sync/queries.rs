use chrono::{DateTime, Utc};
use sqlx::{FromRow, PgPool};

// ---------------------------------------------------------------------------
// Setup SQL — creates BitdexOutbox table + all triggers
// ---------------------------------------------------------------------------

pub const SETUP_SQL: &str = r#"
-- BitdexOutbox table
CREATE TABLE IF NOT EXISTS "BitdexOutbox" (
    id BIGSERIAL PRIMARY KEY,
    entity_type TEXT NOT NULL,
    entity_id BIGINT NOT NULL,
    event TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_bitdex_outbox_id ON "BitdexOutbox" (id);

-- Image trigger function
-- Image table has "id"; join tables (TagsOnImageNew, ImageTool, ImageTechnique,
-- ImageResourceNew) have "imageId" instead.
CREATE OR REPLACE FUNCTION bitdex_image_notify() RETURNS trigger AS $$
DECLARE
  _image_id BIGINT;
BEGIN
  IF TG_TABLE_NAME = 'Image' THEN
    IF TG_OP = 'DELETE' THEN
      INSERT INTO "BitdexOutbox" (entity_type, entity_id, event) VALUES ('Image', OLD.id, 'DELETE');
      RETURN OLD;
    ELSE
      INSERT INTO "BitdexOutbox" (entity_type, entity_id, event) VALUES ('Image', NEW.id, 'UPSERT');
      RETURN NEW;
    END IF;
  ELSE
    -- Join tables: use "imageId" column
    IF TG_OP = 'DELETE' THEN
      _image_id := OLD."imageId";
    ELSE
      _image_id := NEW."imageId";
    END IF;
    INSERT INTO "BitdexOutbox" (entity_type, entity_id, event) VALUES ('Image', _image_id, 'UPSERT');
    RETURN COALESCE(NEW, OLD);
  END IF;
END;
$$ LANGUAGE plpgsql;

-- Triggers on direct image-ID tables
-- ENABLE ALWAYS ensures triggers fire on CDC-replicated rows (Debezium sets
-- session_replication_role = replica, which skips standard triggers).
CREATE OR REPLACE TRIGGER bitdex_image_trg AFTER INSERT OR UPDATE OR DELETE ON "Image"
  FOR EACH ROW EXECUTE FUNCTION bitdex_image_notify();
ALTER TABLE "Image" ENABLE ALWAYS TRIGGER bitdex_image_trg;
CREATE OR REPLACE TRIGGER bitdex_tags_trg AFTER INSERT OR DELETE ON "TagsOnImageNew"
  FOR EACH ROW EXECUTE FUNCTION bitdex_image_notify();
ALTER TABLE "TagsOnImageNew" ENABLE ALWAYS TRIGGER bitdex_tags_trg;
CREATE OR REPLACE TRIGGER bitdex_tool_trg AFTER INSERT OR DELETE ON "ImageTool"
  FOR EACH ROW EXECUTE FUNCTION bitdex_image_notify();
ALTER TABLE "ImageTool" ENABLE ALWAYS TRIGGER bitdex_tool_trg;
CREATE OR REPLACE TRIGGER bitdex_technique_trg AFTER INSERT OR DELETE ON "ImageTechnique"
  FOR EACH ROW EXECUTE FUNCTION bitdex_image_notify();
ALTER TABLE "ImageTechnique" ENABLE ALWAYS TRIGGER bitdex_technique_trg;
CREATE OR REPLACE TRIGGER bitdex_resource_trg AFTER INSERT OR DELETE ON "ImageResourceNew"
  FOR EACH ROW EXECUTE FUNCTION bitdex_image_notify();
ALTER TABLE "ImageResourceNew" ENABLE ALWAYS TRIGGER bitdex_resource_trg;

-- Post changes
CREATE OR REPLACE FUNCTION bitdex_post_notify() RETURNS trigger AS $$
BEGIN
  INSERT INTO "BitdexOutbox" (entity_type, entity_id, event)
    SELECT 'Image', id, 'UPSERT' FROM "Image" WHERE "postId" = NEW.id;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE TRIGGER bitdex_post_trg AFTER UPDATE ON "Post"
  FOR EACH ROW EXECUTE FUNCTION bitdex_post_notify();
ALTER TABLE "Post" ENABLE ALWAYS TRIGGER bitdex_post_trg;

-- ModelVersion changes
CREATE OR REPLACE FUNCTION bitdex_mv_notify() RETURNS trigger AS $$
BEGIN
  INSERT INTO "BitdexOutbox" (entity_type, entity_id, event)
    SELECT 'Image', "imageId", 'UPSERT' FROM "ImageResourceNew" WHERE "modelVersionId" = NEW.id;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE TRIGGER bitdex_mv_trg AFTER UPDATE ON "ModelVersion"
  FOR EACH ROW EXECUTE FUNCTION bitdex_mv_notify();
ALTER TABLE "ModelVersion" ENABLE ALWAYS TRIGGER bitdex_mv_trg;

-- Model changes
CREATE OR REPLACE FUNCTION bitdex_model_notify() RETURNS trigger AS $$
BEGIN
  INSERT INTO "BitdexOutbox" (entity_type, entity_id, event)
    SELECT DISTINCT 'Image', ir."imageId", 'UPSERT'
    FROM "ImageResourceNew" ir
    JOIN "ModelVersion" mv ON ir."modelVersionId" = mv.id
    WHERE mv."modelId" = NEW.id;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE TRIGGER bitdex_model_trg AFTER UPDATE ON "Model"
  FOR EACH ROW EXECUTE FUNCTION bitdex_model_notify();
ALTER TABLE "Model" ENABLE ALWAYS TRIGGER bitdex_model_trg;

-- Cursor tracking table for multi-replica outbox consumption
CREATE TABLE IF NOT EXISTS bitdex_cursors (
    replica_id TEXT PRIMARY KEY,
    last_outbox_id BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Auto-cleanup trigger: when any replica reports its cursor, delete outbox rows
-- that ALL replicas have already consumed.
CREATE OR REPLACE FUNCTION cleanup_bitdex_outbox() RETURNS trigger AS $$
BEGIN
    DELETE FROM "BitdexOutbox"
    WHERE id < (SELECT MIN(last_outbox_id) FROM bitdex_cursors);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_cleanup_bitdex_outbox ON bitdex_cursors;
CREATE TRIGGER trg_cleanup_bitdex_outbox
    AFTER INSERT OR UPDATE ON bitdex_cursors
    FOR EACH ROW EXECUTE FUNCTION cleanup_bitdex_outbox();
"#;

// ---------------------------------------------------------------------------
// Row types
// ---------------------------------------------------------------------------

#[derive(Debug, FromRow)]
pub struct ImageRow {
    pub id: i64,
    #[sqlx(rename = "postId")]
    pub post_id: i64,
    pub url: Option<String>,
    #[sqlx(rename = "nsfwLevel")]
    pub nsfw_level: Option<i32>,
    pub hash: Option<String>,
    #[sqlx(rename = "hideMeta")]
    pub hide_meta: Option<bool>,
    #[sqlx(rename = "type")]
    pub image_type: Option<String>,
    #[sqlx(rename = "userId")]
    pub user_id: Option<i64>,
    pub minor: Option<bool>,
    pub poi: Option<bool>,
    #[sqlx(rename = "blockedFor")]
    pub blocked_for: Option<String>,
    #[sqlx(rename = "scannedAt")]
    pub scanned_at: Option<DateTime<Utc>>,
    #[sqlx(rename = "createdAt")]
    pub created_at: Option<DateTime<Utc>>,
    pub meta: Option<serde_json::Value>,
    #[sqlx(rename = "publishedAt")]
    pub published_at: Option<DateTime<Utc>>,
    pub availability: Option<String>,
    #[sqlx(rename = "postedToId")]
    pub posted_to_id: Option<i64>,
    #[sqlx(rename = "sortAt")]
    pub sort_at: Option<DateTime<Utc>>,
}

#[derive(Debug, FromRow)]
pub struct TagRow {
    #[sqlx(rename = "imageId")]
    pub image_id: i64,
    #[sqlx(rename = "tagId")]
    pub tag_id: i64,
}

#[derive(Debug, FromRow)]
pub struct ToolRow {
    #[sqlx(rename = "imageId")]
    pub image_id: i64,
    #[sqlx(rename = "toolId")]
    pub tool_id: i64,
}

#[derive(Debug, FromRow)]
pub struct TechniqueRow {
    #[sqlx(rename = "imageId")]
    pub image_id: i64,
    #[sqlx(rename = "techniqueId")]
    pub technique_id: i64,
}

#[derive(Debug, FromRow)]
pub struct ResourceRow {
    #[sqlx(rename = "imageId")]
    pub image_id: i64,
    #[sqlx(rename = "baseModel")]
    pub base_model: Option<String>,
    #[sqlx(rename = "modelVersionIds")]
    pub model_version_ids: Vec<i64>,
    #[sqlx(rename = "modelVersionIdsManual")]
    pub model_version_ids_manual: Vec<i64>,
    #[sqlx(rename = "resourcePoi")]
    pub resource_poi: Option<bool>,
}

#[derive(Debug, FromRow)]
pub struct OutboxRow {
    pub id: i64,
    pub entity_id: i64,
    pub event: String,
}

#[derive(Debug, FromRow)]
pub struct MetricRow {
    pub id: i64,
    #[sqlx(rename = "reactionCount")]
    pub reaction_count: i64,
    #[sqlx(rename = "commentCount")]
    pub comment_count: i64,
    #[sqlx(rename = "collectedCount")]
    pub collected_count: i64,
}

// ---------------------------------------------------------------------------
// Query functions
// ---------------------------------------------------------------------------

/// Run the setup SQL to create BitdexOutbox table and triggers.
pub async fn run_setup(pool: &PgPool) -> Result<(), sqlx::Error> {
    // Check if triggers are already set up and ENABLE ALWAYS.
    // If so, skip the full setup to avoid exclusive table locks on large tables
    // (ALTER TABLE ... ENABLE ALWAYS TRIGGER requires AccessExclusive lock).
    let already_setup = check_triggers_exist(pool).await.unwrap_or(false);
    if already_setup {
        eprintln!("BitdexOutbox triggers already exist and are ENABLE ALWAYS — skipping setup.");
        return Ok(());
    }
    sqlx::raw_sql(SETUP_SQL).execute(pool).await?;
    Ok(())
}

/// Check if all bitdex triggers exist and are ENABLE ALWAYS ('A').
async fn check_triggers_exist(pool: &PgPool) -> Result<bool, sqlx::Error> {
    let row: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM pg_trigger WHERE tgname LIKE 'bitdex_%' AND tgenabled = 'A'"
    )
    .fetch_one(pool)
    .await?;
    // We expect 8 triggers: image, tags, tool, technique, resource, post, mv, model
    Ok(row.0 >= 8)
}

/// Get the max image ID for range-based bulk loading.
pub async fn get_max_image_id(pool: &PgPool) -> Result<i64, sqlx::Error> {
    let row: (i64,) = sqlx::query_as("SELECT COALESCE(MAX(id)::int8, 0) FROM \"Image\"")
        .fetch_one(pool)
        .await?;
    Ok(row.0)
}

/// Fetch images by ID range (for bulk loading).
pub async fn fetch_images_by_range(
    pool: &PgPool,
    start: i64,
    end: i64,
) -> Result<Vec<ImageRow>, sqlx::Error> {
    sqlx::query_as::<_, ImageRow>(
        r#"SELECT i.id, i."postId", i.url, i."nsfwLevel", i.hash,
           i."hideMeta", i.type::text, i."userId",
           i.minor, i.poi, i."blockedFor", i."scannedAt", i."createdAt",
           i.meta,
           p."publishedAt", p.availability::text, p."modelVersionId" as "postedToId",
           GREATEST(p."publishedAt", i."scannedAt", i."createdAt") as "sortAt"
        FROM "Image" i
        JOIN "Post" p ON p.id = i."postId"
        WHERE i.id >= $1 AND i.id < $2"#,
    )
    .bind(start)
    .bind(end)
    .fetch_all(pool)
    .await
}

/// Fetch images by ID list (for sync/streaming).
pub async fn fetch_images_by_ids(
    pool: &PgPool,
    ids: &[i64],
) -> Result<Vec<ImageRow>, sqlx::Error> {
    sqlx::query_as::<_, ImageRow>(
        r#"SELECT i.id, i."postId", i.url, i."nsfwLevel", i.hash,
           i."hideMeta", i.type::text, i."userId",
           i.minor, i.poi, i."blockedFor", i."scannedAt", i."createdAt",
           i.meta,
           p."publishedAt", p.availability::text, p."modelVersionId" as "postedToId",
           GREATEST(p."publishedAt", i."scannedAt", i."createdAt") as "sortAt"
        FROM "Image" i
        JOIN "Post" p ON p.id = i."postId"
        WHERE i.id = ANY($1)"#,
    )
    .bind(ids)
    .fetch_all(pool)
    .await
}

/// Fetch tags for a batch of image IDs.
pub async fn fetch_tags(pool: &PgPool, image_ids: &[i64]) -> Result<Vec<TagRow>, sqlx::Error> {
    sqlx::query_as::<_, TagRow>(
        r#"SELECT "imageId", "tagId" FROM "TagsOnImageDetails"
        WHERE "imageId" = ANY($1) AND disabled = false"#,
    )
    .bind(image_ids)
    .fetch_all(pool)
    .await
}

/// Fetch tools for a batch of image IDs.
pub async fn fetch_tools(pool: &PgPool, image_ids: &[i64]) -> Result<Vec<ToolRow>, sqlx::Error> {
    sqlx::query_as::<_, ToolRow>(
        r#"SELECT "imageId", "toolId" FROM "ImageTool" WHERE "imageId" = ANY($1)"#,
    )
    .bind(image_ids)
    .fetch_all(pool)
    .await
}

/// Fetch techniques for a batch of image IDs.
pub async fn fetch_techniques(
    pool: &PgPool,
    image_ids: &[i64],
) -> Result<Vec<TechniqueRow>, sqlx::Error> {
    sqlx::query_as::<_, TechniqueRow>(
        r#"SELECT "imageId", "techniqueId" FROM "ImageTechnique" WHERE "imageId" = ANY($1)"#,
    )
    .bind(image_ids)
    .fetch_all(pool)
    .await
}

/// Fetch resources + model versions for a batch of image IDs.
pub async fn fetch_resources(
    pool: &PgPool,
    image_ids: &[i64],
) -> Result<Vec<ResourceRow>, sqlx::Error> {
    sqlx::query_as::<_, ResourceRow>(
        r#"SELECT ir."imageId",
           string_agg(CASE WHEN m.type = 'Checkpoint' THEN mv."baseModel" ELSE NULL END, '') as "baseModel",
           coalesce(array_agg(mv.id) FILTER (WHERE ir.detected), '{}') as "modelVersionIds",
           coalesce(array_agg(mv.id) FILTER (WHERE NOT ir.detected), '{}') as "modelVersionIdsManual",
           bool_or(m.poi) as "resourcePoi"
        FROM "ImageResourceNew" ir
        JOIN "ModelVersion" mv ON ir."modelVersionId" = mv.id
        JOIN "Model" m ON mv."modelId" = m.id
        WHERE ir."imageId" = ANY($1)
        GROUP BY ir."imageId""#,
    )
    .bind(image_ids)
    .fetch_all(pool)
    .await
}

/// Poll the BitdexOutbox for pending changes.
pub async fn poll_outbox(pool: &PgPool, limit: i64) -> Result<Vec<OutboxRow>, sqlx::Error> {
    sqlx::query_as::<_, OutboxRow>(
        r#"SELECT id, entity_id, event FROM "BitdexOutbox"
        ORDER BY id DESC
        LIMIT $1"#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await
}

/// Delete processed outbox rows up to the given max ID.
pub async fn delete_outbox(pool: &PgPool, max_id: i64) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(r#"DELETE FROM "BitdexOutbox" WHERE id <= $1"#)
        .bind(max_id)
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}

/// Poll outbox rows after a cursor position (FIFO — oldest first).
pub async fn poll_outbox_from_cursor(
    pool: &PgPool,
    cursor: i64,
    limit: i64,
) -> Result<Vec<OutboxRow>, sqlx::Error> {
    sqlx::query_as::<_, OutboxRow>(
        r#"SELECT id, entity_id, event FROM "BitdexOutbox"
        WHERE id > $1
        ORDER BY id ASC
        LIMIT $2"#,
    )
    .bind(cursor)
    .bind(limit)
    .fetch_all(pool)
    .await
}

/// Report a replica's cursor to PG for outbox cleanup tracking.
pub async fn upsert_cursor(
    pool: &PgPool,
    replica_id: &str,
    last_outbox_id: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"INSERT INTO bitdex_cursors (replica_id, last_outbox_id, updated_at)
        VALUES ($1, $2, now())
        ON CONFLICT (replica_id)
        DO UPDATE SET last_outbox_id = $2, updated_at = now()"#,
    )
    .bind(replica_id)
    .bind(last_outbox_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Get the current max outbox ID (for cursor seeding during bulk load).
pub async fn get_max_outbox_id(pool: &PgPool) -> Result<i64, sqlx::Error> {
    let row: (Option<i64>,) = sqlx::query_as(
        r#"SELECT MAX(id) FROM "BitdexOutbox""#,
    )
    .fetch_one(pool)
    .await?;
    Ok(row.0.unwrap_or(0))
}

// ---------------------------------------------------------------------------
// Streaming bulk queries — table-at-a-time loading
// ---------------------------------------------------------------------------

/// Row type for streaming tags ordered by tagId (for bitmap-efficient insertion).
#[derive(Debug, FromRow)]
pub struct StreamTagRow {
    #[sqlx(rename = "tagId")]
    pub tag_id: i64,
    #[sqlx(rename = "imageId")]
    pub image_id: i64,
}

/// Row type for streaming resources (one row per imageId, pre-aggregated).
#[derive(Debug, FromRow)]
pub struct StreamResourceRow {
    #[sqlx(rename = "imageId")]
    pub image_id: i64,
    #[sqlx(rename = "baseModel")]
    pub base_model: Option<String>,
    #[sqlx(rename = "modelVersionIds")]
    pub model_version_ids: Vec<i64>,
    #[sqlx(rename = "modelVersionIdsManual")]
    pub model_version_ids_manual: Vec<i64>,
    #[sqlx(rename = "resourcePoi")]
    pub resource_poi: Option<bool>,
}

/// Get max tag ID for range iteration.
pub async fn get_max_tag_id(pool: &PgPool) -> Result<i64, sqlx::Error> {
    let row: (i64,) = sqlx::query_as(
        r#"SELECT COALESCE(MAX("tagId")::int8, 0) FROM "TagsOnImageDetails""#,
    )
    .fetch_one(pool)
    .await?;
    Ok(row.0)
}

/// Fetch tags by tagId range, ordered by tagId then imageId.
/// This produces bitmap-optimal ordering: all images for one tagId together.
pub async fn fetch_tags_by_tag_range(
    pool: &PgPool,
    start: i64,
    end: i64,
) -> Result<Vec<StreamTagRow>, sqlx::Error> {
    sqlx::query_as::<_, StreamTagRow>(
        r#"SELECT "tagId", "imageId" FROM "TagsOnImageDetails"
        WHERE "tagId" >= $1 AND "tagId" < $2
          AND disabled = false
        ORDER BY "tagId", "imageId""#,
    )
    .bind(start)
    .bind(end)
    .fetch_all(pool)
    .await
}

/// Get max tool ID for range iteration.
pub async fn get_max_tool_id(pool: &PgPool) -> Result<i64, sqlx::Error> {
    let row: (i64,) = sqlx::query_as(
        r#"SELECT COALESCE(MAX("toolId")::int8, 0) FROM "ImageTool""#,
    )
    .fetch_one(pool)
    .await?;
    Ok(row.0)
}

/// Fetch tools by toolId range, ordered by toolId then imageId.
pub async fn fetch_tools_by_tool_range(
    pool: &PgPool,
    start: i64,
    end: i64,
) -> Result<Vec<ToolRow>, sqlx::Error> {
    sqlx::query_as::<_, ToolRow>(
        r#"SELECT "imageId", "toolId" FROM "ImageTool"
        WHERE "toolId" >= $1 AND "toolId" < $2
        ORDER BY "toolId", "imageId""#,
    )
    .bind(start)
    .bind(end)
    .fetch_all(pool)
    .await
}

/// Get max technique ID for range iteration.
pub async fn get_max_technique_id(pool: &PgPool) -> Result<i64, sqlx::Error> {
    let row: (i64,) = sqlx::query_as(
        r#"SELECT COALESCE(MAX("techniqueId")::int8, 0) FROM "ImageTechnique""#,
    )
    .fetch_one(pool)
    .await?;
    Ok(row.0)
}

/// Fetch techniques by techniqueId range, ordered by techniqueId then imageId.
pub async fn fetch_techniques_by_technique_range(
    pool: &PgPool,
    start: i64,
    end: i64,
) -> Result<Vec<TechniqueRow>, sqlx::Error> {
    sqlx::query_as::<_, TechniqueRow>(
        r#"SELECT "imageId", "techniqueId" FROM "ImageTechnique"
        WHERE "techniqueId" >= $1 AND "techniqueId" < $2
        ORDER BY "techniqueId", "imageId""#,
    )
    .bind(start)
    .bind(end)
    .fetch_all(pool)
    .await
}

/// Fetch resources by imageId range (pre-aggregated per imageId).
pub async fn fetch_resources_by_range(
    pool: &PgPool,
    start: i64,
    end: i64,
) -> Result<Vec<StreamResourceRow>, sqlx::Error> {
    sqlx::query_as::<_, StreamResourceRow>(
        r#"SELECT ir."imageId",
           string_agg(CASE WHEN m.type = 'Checkpoint' THEN mv."baseModel" ELSE NULL END, '') as "baseModel",
           coalesce(array_agg(mv.id) FILTER (WHERE ir.detected), '{}') as "modelVersionIds",
           coalesce(array_agg(mv.id) FILTER (WHERE NOT ir.detected), '{}') as "modelVersionIdsManual",
           bool_or(m.poi) as "resourcePoi"
        FROM "ImageResourceNew" ir
        JOIN "ModelVersion" mv ON ir."modelVersionId" = mv.id
        JOIN "Model" m ON mv."modelId" = m.id
        WHERE ir."imageId" >= $1 AND ir."imageId" < $2
        GROUP BY ir."imageId""#,
    )
    .bind(start)
    .bind(end)
    .fetch_all(pool)
    .await
}
