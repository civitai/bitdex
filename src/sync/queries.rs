use sqlx::PgPool;

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

-- CollectionItem changes (nullable imageId — only fire for image collections)
-- Fires on INSERT, DELETE, and UPDATE (status changes like REVIEW→ACCEPTED).
-- imageId and collectionId are immutable on CollectionItem rows — only status changes.
CREATE OR REPLACE FUNCTION bitdex_collection_notify() RETURNS trigger AS $$
DECLARE
  _image_id BIGINT;
BEGIN
  IF TG_OP = 'DELETE' THEN
    _image_id := OLD."imageId";
  ELSIF TG_OP = 'UPDATE' THEN
    -- Only fire when accepted-ness changes (REVIEW→ACCEPTED or ACCEPTED→REJECTED)
    IF (OLD.status = 'ACCEPTED') = (NEW.status = 'ACCEPTED') THEN
      RETURN NEW;
    END IF;
    _image_id := NEW."imageId";
  ELSE
    _image_id := NEW."imageId";
  END IF;
  -- Only fire for image collections (imageId is nullable)
  IF _image_id IS NOT NULL THEN
    INSERT INTO "BitdexOutbox" (entity_type, entity_id, event) VALUES ('Image', _image_id, 'UPSERT');
  END IF;
  RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE TRIGGER bitdex_collection_trg AFTER INSERT OR UPDATE OR DELETE ON "CollectionItem"
  FOR EACH ROW EXECUTE FUNCTION bitdex_collection_notify();
ALTER TABLE "CollectionItem" ENABLE ALWAYS TRIGGER bitdex_collection_trg;

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
// V2 Setup — BitdexOps table + config-driven triggers
// ---------------------------------------------------------------------------

/// SQL to create the V2 BitdexOps table + cursor tracking table.
/// Does NOT create triggers — those are generated from sync config.
pub const SETUP_V2_SQL: &str = r#"
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
"#;

/// Run V2 setup: create BitdexOps + cursors tables, then reconcile triggers
/// from the sync config. Generates trigger SQL from `trigger_gen`, drops
/// stale triggers (hash mismatch), and creates new ones.
pub async fn run_setup_v2(
    pool: &PgPool,
    triggers: &[super::trigger_gen::SyncSource],
) -> Result<(), String> {
    // 1. Create tables
    sqlx::raw_sql(SETUP_V2_SQL)
        .execute(pool)
        .await
        .map_err(|e| format!("Failed to create V2 tables: {e}"))?;
    eprintln!("Created BitdexOps + bitdex_cursors tables");

    // 2. Generate expected trigger names from config
    let expected_triggers: Vec<(String, String)> = triggers
        .iter()
        .map(|source| {
            let name = super::trigger_gen::trigger_name(source);
            let sql = super::trigger_gen::generate_trigger_sql(source);
            (name, sql)
        })
        .collect();

    let expected_names: std::collections::HashSet<&str> = expected_triggers
        .iter()
        .map(|(name, _)| name.as_str())
        .collect();

    // 3. Find existing bitdex triggers
    let existing: Vec<(String,)> = sqlx::query_as(
        "SELECT tgname::text FROM pg_trigger WHERE tgname LIKE 'bitdex_%'"
    )
    .fetch_all(pool)
    .await
    .map_err(|e| format!("Failed to list existing triggers: {e}"))?;

    // 4. Drop stale triggers (exist in PG but not in config)
    for (existing_name,) in &existing {
        if !expected_names.contains(existing_name.as_str()) {
            // Extract table name from trigger name: bitdex_{table}_{hash}
            // We need to find the table to DROP TRIGGER ... ON "table"
            if let Some(table) = find_trigger_table(pool, existing_name).await {
                let drop_sql = format!(
                    "DROP TRIGGER IF EXISTS {} ON \"{}\"",
                    existing_name, table
                );
                match sqlx::raw_sql(&drop_sql).execute(pool).await {
                    Ok(_) => eprintln!("Dropped stale trigger: {existing_name} on {table}"),
                    Err(e) => eprintln!("WARNING: Failed to drop stale trigger {existing_name}: {e}"),
                }
                // Also drop the function
                let func_name = existing_name.replace(
                    &existing_name[existing_name.rfind('_').unwrap_or(existing_name.len())..],
                    &format!("_ops{}", &existing_name[existing_name.rfind('_').unwrap_or(existing_name.len())..]),
                );
                // Try to drop function with the original name pattern
                let drop_func = format!("DROP FUNCTION IF EXISTS {}()", existing_name);
                let _ = sqlx::raw_sql(&drop_func).execute(pool).await;
            }
        }
    }

    // 5. Create/update triggers from config.
    // The bitdex user should have direct TRIGGER privilege on the tables
    // (via GRANT TRIGGER ON TABLE ... TO bitdex). No SET ROLE needed.
    for (name, sql) in &expected_triggers {
        match sqlx::raw_sql(sql).execute(pool).await {
            Ok(_) => eprintln!("Created trigger: {name}"),
            Err(e) => {
                return Err(format!("Failed to create trigger {name}: {e}"));
            }
        }
    }

    let existing_count = existing.len();
    let stale_count = existing.iter()
        .filter(|(n,)| !expected_names.contains(n.as_str()))
        .count();
    eprintln!(
        "Trigger reconciliation: {} configured, {} existing, {} stale dropped",
        triggers.len(),
        existing_count,
        stale_count,
    );

    Ok(())
}

/// Find the table a trigger is attached to via pg_catalog.
async fn find_trigger_table(pool: &PgPool, trigger_name: &str) -> Option<String> {
    let result: Result<(String,), _> = sqlx::query_as(
        "SELECT c.relname::text FROM pg_trigger t \
         JOIN pg_class c ON t.tgrelid = c.oid \
         WHERE t.tgname = $1"
    )
    .bind(trigger_name)
    .fetch_one(pool)
    .await;

    result.ok().map(|(name,)| name)
}

/// Get the max BitdexOps ID (for cursor seeding during dump).
pub async fn get_max_ops_id(pool: &PgPool) -> Result<i64, String> {
    let row: (Option<i64>,) = sqlx::query_as(
        r#"SELECT MAX(id) FROM "BitdexOps""#,
    )
    .fetch_one(pool)
    .await
    .map_err(|e| format!("Failed to get max BitdexOps ID: {e}"))?;
    Ok(row.0.unwrap_or(0))
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
    // We expect 9 triggers: image, tags, tool, technique, resource, collection, post, mv, model
    Ok(row.0 >= 9)
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
