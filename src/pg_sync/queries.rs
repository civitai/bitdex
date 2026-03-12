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
CREATE OR REPLACE FUNCTION bitdex_image_notify() RETURNS trigger AS $$
BEGIN
  IF TG_OP = 'DELETE' AND TG_TABLE_NAME = 'Image' THEN
    INSERT INTO "BitdexOutbox" (entity_type, entity_id, event) VALUES ('Image', OLD.id, 'DELETE');
  ELSE
    INSERT INTO "BitdexOutbox" (entity_type, entity_id, event) VALUES ('Image', NEW.id, 'UPSERT');
  END IF;
  RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

-- Triggers on direct image-ID tables
CREATE OR REPLACE TRIGGER bitdex_image_trg AFTER INSERT OR UPDATE OR DELETE ON "Image"
  FOR EACH ROW EXECUTE FUNCTION bitdex_image_notify();
CREATE OR REPLACE TRIGGER bitdex_tags_trg AFTER INSERT OR DELETE ON "TagsOnImageNew"
  FOR EACH ROW EXECUTE FUNCTION bitdex_image_notify();
CREATE OR REPLACE TRIGGER bitdex_tool_trg AFTER INSERT OR DELETE ON "ImageTool"
  FOR EACH ROW EXECUTE FUNCTION bitdex_image_notify();
CREATE OR REPLACE TRIGGER bitdex_technique_trg AFTER INSERT OR DELETE ON "ImageTechnique"
  FOR EACH ROW EXECUTE FUNCTION bitdex_image_notify();
CREATE OR REPLACE TRIGGER bitdex_resource_trg AFTER INSERT OR DELETE ON "ImageResourceNew"
  FOR EACH ROW EXECUTE FUNCTION bitdex_image_notify();

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
    sqlx::raw_sql(SETUP_SQL).execute(pool).await?;
    Ok(())
}

/// Get the max image ID for range-based bulk loading.
pub async fn get_max_image_id(pool: &PgPool) -> Result<i64, sqlx::Error> {
    let row: (i64,) = sqlx::query_as("SELECT COALESCE(MAX(id), 0) FROM \"Image\"")
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
