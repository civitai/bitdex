//! Generic HTTP server for BitDex — no dataset-specific code.
//!
//! Feature-gated behind `server`. Provides `BitdexServer` which starts blank
//! and creates indexes via API.

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use axum::body::Bytes;
use axum::extract::{Path as AxumPath, Query as AxumQuery, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Json};
use axum::routing::{get, post, delete};
use axum::Router;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tower_http::cors::CorsLayer;

use crate::concurrent_engine::ConcurrentEngine;
use crate::config::{Config, DataSchema, FieldValueType, FilterFieldConfig, SortFieldConfig};
use crate::docstore::StoredDoc;
use crate::executor::{CaseSensitiveFields, StringMaps};
use crate::loader;
use crate::metrics::Metrics;
use crate::mutation::FieldValue;
use crate::query::{BitdexQuery, Value};

// ---------------------------------------------------------------------------
// Server state
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Task Registry — replaces the old LoadStatus enum
// ---------------------------------------------------------------------------

type TaskId = u64;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskType {
    Load,
    Rebuild,
    AddFields,
    RemoveFields,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Running,
    Saving,
    Complete,
    Error,
}

#[derive(Debug, Clone, Serialize)]
pub struct TaskProgress {
    pub records_processed: u64,
    pub total_estimate: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TaskInfo {
    pub task_id: TaskId,
    pub task_type: TaskType,
    pub status: TaskStatus,
    pub progress: TaskProgress,
    pub elapsed_secs: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TaskSnapshot {
    pub active: Option<TaskInfo>,
    pub history: Vec<TaskInfo>,
}

struct ActiveTask {
    id: TaskId,
    task_type: TaskType,
    status: TaskStatus,
    started_at: Instant,
}

struct RegistryState {
    active: Option<ActiveTask>,
    history: VecDeque<TaskInfo>,
}

pub struct TaskRegistry {
    next_id: AtomicU64,
    active_progress: Arc<AtomicU64>,
    state: Mutex<RegistryState>,
}

fn build_task_info(active: &ActiveTask, progress: u64) -> TaskInfo {
    TaskInfo {
        task_id: active.id,
        task_type: active.task_type.clone(),
        status: active.status.clone(),
        progress: TaskProgress {
            records_processed: progress,
            total_estimate: None,
        },
        elapsed_secs: active.started_at.elapsed().as_secs_f64(),
        result: None,
        error: if active.status == TaskStatus::Error {
            Some("Task in error state".to_string())
        } else {
            None
        },
    }
}

impl TaskRegistry {
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            active_progress: Arc::new(AtomicU64::new(0)),
            state: Mutex::new(RegistryState {
                active: None,
                history: VecDeque::new(),
            }),
        }
    }

    /// Try to start a new task. Returns (task_id, progress_counter) on success,
    /// or the active TaskInfo on conflict.
    pub fn try_start(&self, task_type: TaskType) -> Result<(TaskId, Arc<AtomicU64>), TaskInfo> {
        let mut state = self.state.lock();
        if let Some(ref active) = state.active {
            let progress = self.active_progress.load(Ordering::Acquire);
            return Err(build_task_info(active, progress));
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.active_progress.store(0, Ordering::Release);
        state.active = Some(ActiveTask {
            id,
            task_type,
            status: TaskStatus::Running,
            started_at: Instant::now(),
        });
        Ok((id, Arc::clone(&self.active_progress)))
    }

    pub fn set_saving(&self, task_id: TaskId) {
        let mut state = self.state.lock();
        if let Some(ref mut active) = state.active {
            if active.id == task_id {
                active.status = TaskStatus::Saving;
            }
        }
    }

    pub fn set_complete(&self, task_id: TaskId, result: Option<serde_json::Value>) {
        let mut state = self.state.lock();
        if let Some(active) = state.active.take() {
            if active.id == task_id {
                let progress = self.active_progress.load(Ordering::Acquire);
                let mut info = build_task_info(&active, progress);
                info.status = TaskStatus::Complete;
                info.result = result;
                state.history.push_front(info);
                if state.history.len() > 20 {
                    state.history.pop_back();
                }
            } else {
                // Put it back — wrong task_id
                state.active = Some(active);
            }
        }
    }

    pub fn set_error(&self, task_id: TaskId, message: String) {
        let mut state = self.state.lock();
        if let Some(active) = state.active.take() {
            if active.id == task_id {
                let progress = self.active_progress.load(Ordering::Acquire);
                let mut info = build_task_info(&active, progress);
                info.status = TaskStatus::Error;
                info.error = Some(message);
                state.history.push_front(info);
                if state.history.len() > 20 {
                    state.history.pop_back();
                }
            } else {
                state.active = Some(active);
            }
        }
    }

    pub fn get(&self, task_id: TaskId) -> Option<TaskInfo> {
        let state = self.state.lock();
        // Check active first
        if let Some(ref active) = state.active {
            if active.id == task_id {
                let progress = self.active_progress.load(Ordering::Acquire);
                return Some(build_task_info(active, progress));
            }
        }
        // Check history
        state.history.iter().find(|t| t.task_id == task_id).cloned()
    }

    pub fn snapshot(&self) -> TaskSnapshot {
        let state = self.state.lock();
        let active = state.active.as_ref().map(|a| {
            let progress = self.active_progress.load(Ordering::Acquire);
            build_task_info(a, progress)
        });
        TaskSnapshot {
            active,
            history: state.history.iter().cloned().collect(),
        }
    }
}

struct TaskGuard {
    tasks: Arc<TaskRegistry>,
    task_id: Option<TaskId>,
}

impl TaskGuard {
    fn defuse(&mut self) {
        self.task_id.take();
    }
}

impl Drop for TaskGuard {
    fn drop(&mut self) {
        if let Some(id) = self.task_id {
            self.tasks.set_error(id, "Task panicked".to_string());
        }
    }
}

/// Persisted index definition (saved as config.json in the index directory).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDefinition {
    pub name: String,
    pub config: Config,
    pub data_schema: DataSchema,
}

/// Reverse string maps for MappedString fields: field_name → (int → original_string).
/// Built from the original string_map before case normalization so original casing is preserved.
type ReverseStringMaps = HashMap<String, HashMap<i64, String>>;

/// Historical schema defaults: version → (field_target_name → default_json_value).
/// Used by format_document to reconstruct elided fields from docs encoded with older schemas.
type SchemaRegistry = HashMap<u8, HashMap<String, serde_json::Value>>;

/// Live state for a single index.
struct IndexState {
    engine: Arc<ConcurrentEngine>,
    definition: IndexDefinition,
    reverse_maps: Arc<ReverseStringMaps>,
    schema_registry: Arc<SchemaRegistry>,
    tasks: Arc<TaskRegistry>,
}

/// Shared application state.
struct AppState {
    data_dir: PathBuf,
    index: Mutex<Option<IndexState>>,
    metrics: Metrics,
    parser_registry: crate::parser::registry::ParserRegistry,
}

type SharedState = Arc<AppState>;

// ---------------------------------------------------------------------------
// API request/response types
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct CreateIndexRequest {
    name: String,
    config: Config,
    data_schema: DataSchema,
}

#[derive(Deserialize)]
struct LoadRequest {
    path: String,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default = "default_threads")]
    threads: usize,
    #[serde(default = "default_chunk_size")]
    chunk_size: usize,
    #[serde(default = "default_docstore_batch_size")]
    docstore_batch_size: usize,
    #[serde(default = "default_max_writer_threads")]
    max_writer_threads: usize,
    #[serde(default)]
    save_snapshot: bool,
}

fn default_threads() -> usize {
    // Unused by fused parse+bitmap loader (rayon manages parallelism),
    // kept for API compat.
    let logical = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    (logical / 2).clamp(4, 8)
}

fn default_chunk_size() -> usize {
    500_000
}

fn default_docstore_batch_size() -> usize {
    100_000
}

fn default_max_writer_threads() -> usize {
    4
}

#[derive(Deserialize)]
struct DocumentRequest {
    slot_id: u32,
    #[serde(default = "IncludeDocs::all")]
    fields: IncludeDocs,
}

#[derive(Deserialize)]
struct DocumentBatchRequest {
    slot_ids: Vec<u32>,
    #[serde(default = "IncludeDocs::all")]
    fields: IncludeDocs,
}

// ---------------------------------------------------------------------------
// Field selection for document retrieval
// ---------------------------------------------------------------------------

/// Controls which document fields to return.
///
/// - `false` → no documents
/// - `true` / `["*"]` → all fields
/// - `["field1", "field2"]` → only those fields
///
/// Default is `None` (IDs only) for query endpoints.
/// Document endpoints default to `All` since returning docs is their purpose.
#[derive(Debug, Clone)]
enum IncludeDocs {
    None,
    All,
    Fields(Vec<String>),
}

impl Default for IncludeDocs {
    fn default() -> Self {
        IncludeDocs::None
    }
}

impl<'de> Deserialize<'de> for IncludeDocs {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de;

        struct IncludeDocsVisitor;

        impl<'de> de::Visitor<'de> for IncludeDocsVisitor {
            type Value = IncludeDocs;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("bool or array of field names")
            }

            fn visit_bool<E: de::Error>(self, v: bool) -> std::result::Result<IncludeDocs, E> {
                Ok(if v { IncludeDocs::All } else { IncludeDocs::None })
            }

            fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> std::result::Result<IncludeDocs, A::Error> {
                let mut fields = Vec::new();
                while let Some(s) = seq.next_element::<String>()? {
                    if s == "*" {
                        return Ok(IncludeDocs::All);
                    }
                    fields.push(s);
                }
                if fields.is_empty() {
                    Ok(IncludeDocs::None)
                } else {
                    Ok(IncludeDocs::Fields(fields))
                }
            }
        }

        deserializer.deserialize_any(IncludeDocsVisitor)
    }
}

impl IncludeDocs {
    fn is_none(&self) -> bool {
        matches!(self, IncludeDocs::None)
    }

    fn all() -> Self {
        IncludeDocs::All
    }
}

/// Fuse schema defaults into a stored document and apply field selection.
///
/// Schema fields that were elided at write time (absent from StoredDoc) are
/// filled with their type's default value so callers always see the full shape.
/// MappedString fields are reverse-mapped from integer back to the original string.
///
/// When a document was encoded with a different schema version, uses historical
/// defaults from the schema registry instead of the current schema's defaults.
fn format_document(
    doc: &StoredDoc,
    schema: &DataSchema,
    reverse_maps: &ReverseStringMaps,
    selection: &IncludeDocs,
    schema_registry: &SchemaRegistry,
) -> serde_json::Value {
    let mut fields = serde_json::Map::new();

    // Always include "id" if present
    if let Some(id_val) = doc.fields.get("id") {
        fields.insert("id".to_string(), field_value_to_json(id_val));
    }

    // Determine which defaults to use based on the doc's schema version.
    // Version 0 = legacy (pre-versioning), use current defaults.
    let historical_defaults = if doc.schema_version != 0
        && doc.schema_version != schema.schema_version
    {
        schema_registry.get(&doc.schema_version)
    } else {
        None
    };

    for mapping in &schema.fields {
        // Apply field selection filter
        match selection {
            IncludeDocs::Fields(ref selected) => {
                if !selected.iter().any(|s| s == &mapping.target) {
                    continue;
                }
            }
            IncludeDocs::All => {}
            IncludeDocs::None => continue,
        }

        let value = if let Some(fv) = doc.fields.get(&mapping.target) {
            // Reverse-map MappedString / LowCardinalityString fields from integer back to string
            if mapping.value_type == FieldValueType::MappedString
                || mapping.value_type == FieldValueType::LowCardinalityString
            {
                if let Some(rev) = reverse_maps.get(&mapping.target) {
                    reverse_map_value(fv, rev)
                } else {
                    field_value_to_json(fv)
                }
            } else {
                field_value_to_json(fv)
            }
        } else if let Some(hist) = historical_defaults {
            // Doc encoded with an older schema — use that version's defaults
            hist.get(&mapping.target)
                .cloned()
                .unwrap_or_else(|| default_json_for_field(mapping))
        } else {
            // Current schema version or legacy — use current defaults
            default_json_for_field(mapping)
        };
        fields.insert(mapping.target.clone(), value);
    }

    serde_json::Value::Object(fields)
}

/// Reverse-map a MappedString field value from its stored integer back to
/// the original string using a precomputed int→string map.
fn reverse_map_value(fv: &FieldValue, rev: &HashMap<i64, String>) -> serde_json::Value {
    match fv {
        FieldValue::Single(Value::Integer(i)) => {
            if let Some(s) = rev.get(i) {
                serde_json::json!(s)
            } else {
                // Unknown mapping — return null rather than a meaningless integer
                serde_json::Value::Null
            }
        }
        // Non-integer values pass through (shouldn't happen for MappedString, but be safe)
        other => field_value_to_json(other),
    }
}

fn field_value_to_json(fv: &FieldValue) -> serde_json::Value {
    match fv {
        FieldValue::Single(v) => value_to_json(v),
        FieldValue::Multi(vs) => serde_json::Value::Array(vs.iter().map(value_to_json).collect()),
    }
}

fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Integer(i) => serde_json::json!(i),
        Value::Float(f) => serde_json::json!(f),
        Value::Bool(b) => serde_json::json!(b),
        Value::String(s) => serde_json::json!(s),
    }
}

fn default_json_for_type(vt: &FieldValueType) -> serde_json::Value {
    match vt {
        FieldValueType::Integer => serde_json::json!(0),
        FieldValueType::MappedString | FieldValueType::LowCardinalityString => serde_json::Value::Null,
        FieldValueType::Boolean | FieldValueType::ExistsBoolean => serde_json::json!(false),
        FieldValueType::String => serde_json::json!(""),
        FieldValueType::IntegerArray => serde_json::json!([]),
    }
}

/// Return the default JSON value for a field, preferring the per-field schema default
/// over the generic type-based fallback.
fn default_json_for_field(mapping: &crate::config::FieldMapping) -> serde_json::Value {
    if let Some(ref default) = mapping.default_value {
        default.clone()
    } else {
        default_json_for_type(&mapping.value_type)
    }
}

#[derive(Deserialize)]
struct UpsertRequest {
    documents: Vec<serde_json::Value>,
    #[serde(default)]
    cursor: Option<CursorInput>,
}

#[derive(Deserialize)]
struct DeleteDocsRequest {
    ids: Vec<u32>,
    #[serde(default)]
    cursor: Option<CursorInput>,
}

#[derive(Deserialize)]
struct SnapshotParams {
    /// If true, save bitmaps to disk then unload from memory (lazy reload on demand).
    #[serde(default)]
    unload: bool,
}

#[derive(Deserialize)]
struct CursorInput {
    name: String,
    value: String,
}

#[derive(Deserialize)]
struct RebuildRequest {
    #[serde(default)]
    sort_fields: Option<Vec<String>>,
    #[serde(default)]
    filter_fields: Option<Vec<String>>,
    #[serde(default = "default_save_snapshot")]
    save_snapshot: bool,
}

fn default_save_snapshot() -> bool {
    true
}

#[derive(Deserialize)]
struct AddFieldsRequest {
    #[serde(default)]
    filter_fields: Vec<FilterFieldConfig>,
    #[serde(default)]
    sort_fields: Vec<SortFieldConfig>,
    #[serde(default = "default_save_snapshot")]
    save_snapshot: bool,
    /// If true, skip validation that fields exist in docstore documents.
    #[serde(default)]
    skip_validation: bool,
}

#[derive(Deserialize)]
struct RemoveFieldsRequest {
    #[serde(default)]
    filter_fields: Vec<String>,
    #[serde(default)]
    sort_fields: Vec<String>,
    #[serde(default = "default_save_snapshot")]
    save_snapshot: bool,
}

// ---------------------------------------------------------------------------
// Public server entry point
// ---------------------------------------------------------------------------

/// The BitDex HTTP server. Starts blank and creates indexes via API.
pub struct BitdexServer {
    data_dir: PathBuf,
    rebuild: bool,
    default_query_format: Option<String>,
}

impl BitdexServer {
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir, rebuild: false, default_query_format: None }
    }

    /// Enable rebuild mode: on startup, delete existing bitmap indexes and
    /// rebuild all bitmaps from the docstore using the current config.
    /// Useful for config changes, corruption recovery, or fresh deployments.
    pub fn with_rebuild(mut self, rebuild: bool) -> Self {
        self.rebuild = rebuild;
        self
    }

    /// Set the default query format ("bitdex", "compact", "meilisearch").
    /// Per-request `?format=` overrides this. Falls back to "bitdex" if not set.
    pub fn with_default_query_format(mut self, format: impl Into<String>) -> Self {
        self.default_query_format = Some(format.into());
        self
    }

    /// Start the HTTP server. Blocks until the server shuts down.
    pub async fn serve(self, addr: SocketAddr) -> std::io::Result<()> {
        // Ensure data directory exists
        std::fs::create_dir_all(&self.data_dir).ok();

        let mut registry = crate::parser::registry::default_registry();
        if let Some(fmt) = &self.default_query_format {
            registry.set_default(fmt.clone());
        }

        let state = Arc::new(AppState {
            data_dir: self.data_dir.clone(),
            index: Mutex::new(None),
            metrics: Metrics::new(),
            parser_registry: registry,
        });

        // Try to restore an existing index from disk
        if let Err(e) = restore_index(&state) {
            eprintln!("Warning: failed to restore index from disk: {e}");
        }

        // Rebuild mode: delete existing bitmaps and rebuild from docstore
        if self.rebuild {
            if let Err(e) = rebuild_on_boot(&state) {
                eprintln!("FATAL: rebuild failed: {e}");
                std::process::exit(1);
            }
        }

        let shutdown_state = Arc::clone(&state);

        let app = Router::new()
            // Index management
            .route("/api/indexes", post(handle_create_index))
            .route("/api/indexes", get(handle_list_indexes))
            .route("/api/indexes/{name}", get(handle_get_index))
            .route("/api/indexes/{name}", delete(handle_delete_index))
            // Data loading
            .route("/api/indexes/{name}/load", post(handle_load))
            // Legacy /load/status removed — use /tasks or /tasks/{id} instead
            // Query & documents
            .route("/api/indexes/{name}/query", post(handle_query))
            .route("/api/indexes/{name}/document", post(handle_document))
            .route("/api/indexes/{name}/documents", post(handle_documents_batch).delete(handle_delete_docs))
            .route("/api/indexes/{name}/documents/upsert", post(handle_upsert))
            .route("/api/indexes/{name}/stats", get(handle_stats))
            .route("/api/indexes/{name}/cache", delete(handle_clear_cache))
            .route("/api/indexes/{name}/cache/persistent", delete(handle_purge_cache))
            .route("/api/indexes/{name}/rebuild", post(handle_rebuild))
            .route("/api/indexes/{name}/fields", post(handle_add_fields).delete(handle_remove_fields))
            .route("/api/indexes/{name}/tasks", get(handle_list_tasks))
            .route("/api/indexes/{name}/snapshot", post(handle_save_snapshot))
            .route("/api/tasks/{task_id}", get(handle_get_task))
            // Cursors
            .route("/api/indexes/{name}/cursors", get(handle_list_cursors))
            .route("/api/indexes/{name}/cursors/{cursor_name}", get(handle_get_cursor))
            // Utility
            .route("/api/health", get(handle_health))
            .route("/api/formats", get(handle_list_formats))
            .route("/metrics", get(handle_metrics))
            // Serve static UI
            .route("/", get(handle_ui))
            .layer(CorsLayer::permissive())
            .with_state(state);

        eprintln!("BitDex server listening on http://{}", addr);

        let shutdown_signal = async {
            #[cfg(unix)]
            {
                let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {},
                    _ = term.recv() => {},
                }
            }
            #[cfg(not(unix))]
            tokio::signal::ctrl_c().await.ok();

            eprintln!("Shutdown signal received, draining...");
        };

        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal)
            .await?;

        // After graceful shutdown: save snapshot and shut down the engine
        eprintln!("Server stopped, saving final snapshot...");
        let guard = shutdown_state.index.lock();
        if let Some(ref index_state) = *guard {
            if let Err(e) = index_state.engine.save_snapshot() {
                eprintln!("Warning: failed to save final snapshot: {e}");
            }
        }
        drop(guard);
        eprintln!("Shutdown complete.");

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Index restoration from disk
// ---------------------------------------------------------------------------

fn restore_index(state: &SharedState) -> Result<(), String> {
    let indexes_dir = state.data_dir.join("indexes");
    if !indexes_dir.exists() {
        return Ok(());
    }

    // Scan for index directories with config.json
    let entries = std::fs::read_dir(&indexes_dir).map_err(|e| e.to_string())?;
    for entry in entries {
        let entry = entry.map_err(|e| e.to_string())?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }

        let config_path = path.join("config.json");
        if !config_path.exists() {
            continue;
        }

        let json = std::fs::read_to_string(&config_path).map_err(|e| e.to_string())?;
        let mut def: IndexDefinition = serde_json::from_str(&json).map_err(|e| e.to_string())?;

        // Load LowCardinalityString dictionaries from disk
        let bitmap_path = path.join("bitmaps");
        let lcs_dicts = ConcurrentEngine::load_dictionaries(&def.data_schema, &bitmap_path)
            .map_err(|e| e.to_string())?;

        // Build reverse maps BEFORE normalization to preserve original casing
        let reverse_maps = build_reverse_string_maps_with_dicts(&def.data_schema, Some(&lcs_dicts));
        def.data_schema.normalize_string_maps();

        // Create engine from persisted config
        let docstore_path = path.join("docs");
        let mut config = def.config.clone();
        config.storage.bitmap_path = Some(bitmap_path);

        // Always use new_with_path so bitmaps restore from bitmap_path even if
        // docstore doesn't exist yet (it will be created fresh).
        let mut engine = ConcurrentEngine::new_with_path(config, &docstore_path)
            .map_err(|e| e.to_string())?;

        // Set docstore field defaults for write-side elision
        engine.set_docstore_defaults(&def.data_schema);

        // Set LowCardinalityString dictionaries
        if !lcs_dicts.is_empty() {
            engine.set_dictionaries(lcs_dicts);
        }

        // Build string_maps from DataSchema for MappedString + LowCardinalityString field reverse lookup
        let (string_maps, cs_fields) = build_string_maps_with_dicts(&def.data_schema, Some(engine.dictionaries()));
        if !string_maps.is_empty() {
            engine.set_string_maps(string_maps);
        }
        if !cs_fields.is_empty() {
            engine.set_case_sensitive_fields(cs_fields);
        }

        let alive = engine.alive_count();
        eprintln!(
            "Restored index '{}' from disk ({} records)",
            def.name, alive
        );

        let tasks = Arc::new(TaskRegistry::new());
        // If there are existing records, add a synthetic "complete" entry to history
        if alive > 0 {
            // Use try_start + set_complete to put a history entry
            if let Ok((tid, progress)) = tasks.try_start(TaskType::Load) {
                progress.store(alive, Ordering::Release);
                tasks.set_complete(tid, Some(serde_json::json!({
                    "records_loaded": alive,
                })));
            }
        }

        let schema_registry = engine.build_schema_registry();

        *state.index.lock() = Some(IndexState {
            engine: Arc::new(engine),
            definition: def,
            reverse_maps: Arc::new(reverse_maps),
            schema_registry: Arc::new(schema_registry),
            tasks,
        });

        // Only restore the first index (single-index for now)
        break;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Rebuild on boot
// ---------------------------------------------------------------------------

/// Delete existing bitmap indexes and rebuild all bitmaps from the docstore.
///
/// Requires an index to already be restored (config.json + docstore must exist).
/// Deletes the bitmaps directory, runs `build_all_from_docstore`, then
/// `save_and_unload` to persist and free memory.
fn rebuild_on_boot(state: &SharedState) -> Result<(), String> {
    use crate::concurrent_engine::get_rss_bytes;

    let guard = state.index.lock();
    let idx = guard.as_ref().ok_or("No index found — cannot rebuild without config.json")?;

    let engine = Arc::clone(&idx.engine);
    let index_name = idx.definition.name.clone();
    let bitmap_path = state.data_dir.join("indexes").join(&index_name).join("bitmaps");
    drop(guard);

    eprintln!("\n=== REBUILD MODE ===");
    eprintln!("Index: {}", index_name);

    // Step 1: Delete existing bitmaps
    if bitmap_path.exists() {
        eprintln!("Deleting existing bitmaps at {} ...", bitmap_path.display());
        std::fs::remove_dir_all(&bitmap_path).map_err(|e| format!("delete bitmaps: {e}"))?;
        std::fs::create_dir_all(&bitmap_path).map_err(|e| format!("recreate bitmaps dir: {e}"))?;
        eprintln!("  done");
    }

    // Step 2: Build all bitmap indexes from docstore
    let rss_start = get_rss_bytes();
    eprintln!("Building bitmap indexes from docstore...");
    eprintln!("  RSS before build: {:.2} GB", rss_start as f64 / 1e9);

    let progress = Arc::new(AtomicU64::new(0));
    let progress_clone = progress.clone();

    let memory_cb: Box<dyn Fn(u64, f64, u64) + Send + Sync> = Box::new(move |docs, elapsed, rss| {
        if elapsed > 0.0 {
            eprintln!("  [{:>6.1}s] {:>10} docs ({:>7.0} docs/s)  RSS={:.2} GB",
                elapsed, docs, docs as f64 / elapsed, rss as f64 / 1e9);
        }
    });

    let (total_docs, build_elapsed) = engine
        .build_all_from_docstore(progress_clone, Some(memory_cb))
        .map_err(|e| format!("build_all_from_docstore: {e}"))?;

    let rss_after_build = get_rss_bytes();
    eprintln!("Build complete: {} docs in {:.1}s ({:.0} docs/s), RSS={:.2} GB",
        total_docs, build_elapsed, total_docs as f64 / build_elapsed, rss_after_build as f64 / 1e9);

    // Step 3: Persist bitmaps to disk and unload from memory
    eprintln!("Persisting bitmaps to disk...");
    let persist_start = std::time::Instant::now();

    engine.save_and_unload().map_err(|e| format!("save_and_unload: {e}"))?;

    let persist_elapsed = persist_start.elapsed().as_secs_f64();
    let rss_final = get_rss_bytes();
    let total_elapsed = build_elapsed + persist_elapsed;

    eprintln!("\n=== REBUILD COMPLETE ===");
    eprintln!("  Docs:          {}", total_docs);
    eprintln!("  Build:         {:.1}s", build_elapsed);
    eprintln!("  Persist:       {:.1}s", persist_elapsed);
    eprintln!("  Total:         {:.1}s ({:.1} min)", total_elapsed, total_elapsed / 60.0);
    eprintln!("  RSS final:     {:.2} GB", rss_final as f64 / 1e9);
    eprintln!("Server will now start with lazy bitmap loading.\n");

    // Update task registry so the API reflects the rebuild
    let guard = state.index.lock();
    if let Some(idx) = guard.as_ref() {
        if let Ok((tid, progress)) = idx.tasks.try_start(TaskType::Rebuild) {
            progress.store(total_docs, Ordering::Release);
            idx.tasks.set_complete(tid, Some(serde_json::json!({
                "records_loaded": total_docs,
                "elapsed_secs": total_elapsed,
            })));
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Handlers: Index management
// ---------------------------------------------------------------------------

/// Build string maps with optional dictionaries for LowCardinalityString fields.
fn build_string_maps_with_dicts(
    schema: &DataSchema,
    dictionaries: Option<&HashMap<String, crate::dictionary::FieldDictionary>>,
) -> (StringMaps, CaseSensitiveFields) {
    let mut maps = StringMaps::new();
    let mut cs_fields = CaseSensitiveFields::new();
    for mapping in &schema.fields {
        if mapping.value_type == FieldValueType::MappedString {
            if let Some(ref string_map) = mapping.string_map {
                if mapping.case_sensitive {
                    cs_fields.insert(mapping.target.clone());
                    maps.insert(mapping.target.clone(), string_map.clone());
                } else {
                    // Normalize keys to lowercase for case-insensitive matching
                    let normalized: HashMap<String, i64> = string_map
                        .iter()
                        .map(|(k, v)| (k.to_lowercase(), *v))
                        .collect();
                    maps.insert(mapping.target.clone(), normalized);
                }
            }
        } else if mapping.value_type == FieldValueType::LowCardinalityString {
            // LowCardinalityString: build string map from dictionary
            if let Some(dicts) = dictionaries {
                if let Some(dict) = dicts.get(&mapping.target) {
                    let snap = dict.snapshot();
                    maps.insert(mapping.target.clone(), snap.to_string_map());
                    // LowCardinalityString is always case-insensitive (keys are already lowercase)
                }
            }
        }
    }
    (maps, cs_fields)
}

/// Build reverse string maps with optional dictionaries for LowCardinalityString fields.
fn build_reverse_string_maps_with_dicts(
    schema: &DataSchema,
    dictionaries: Option<&HashMap<String, crate::dictionary::FieldDictionary>>,
) -> ReverseStringMaps {
    let mut reverse = ReverseStringMaps::new();
    for mapping in &schema.fields {
        if mapping.value_type == FieldValueType::MappedString {
            if let Some(ref string_map) = mapping.string_map {
                let rev: HashMap<i64, String> = string_map
                    .iter()
                    .map(|(k, v)| (*v, k.clone()))
                    .collect();
                reverse.insert(mapping.target.clone(), rev);
            }
        } else if mapping.value_type == FieldValueType::LowCardinalityString {
            if let Some(dicts) = dictionaries {
                if let Some(dict) = dicts.get(&mapping.target) {
                    let snap = dict.snapshot();
                    reverse.insert(mapping.target.clone(), snap.to_reverse_map());
                }
            }
        }
    }
    reverse
}

async fn handle_create_index(
    State(state): State<SharedState>,
    Json(req): Json<CreateIndexRequest>,
) -> impl IntoResponse {
    // Validate name
    if req.name.is_empty() || req.name.len() > 64 || !req.name.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-') {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "Invalid index name. Use alphanumeric, underscore, or hyphen."})),
        ).into_response();
    }

    // Check if an index already exists
    {
        let guard = state.index.lock();
        if guard.is_some() {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({"error": "An index already exists. Delete it first."})),
            ).into_response();
        }
    }

    // Validate config
    if let Err(e) = req.config.validate() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": format!("Invalid config: {e}")})),
        ).into_response();
    }

    // Create index directory
    let index_dir = state.data_dir.join("indexes").join(&req.name);
    if let Err(e) = std::fs::create_dir_all(&index_dir) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": format!("Failed to create index directory: {e}")})),
        ).into_response();
    }

    // Initialize empty dictionaries for LowCardinalityString fields
    let mut lcs_dicts = HashMap::new();
    for mapping in &req.data_schema.fields {
        if mapping.value_type == FieldValueType::LowCardinalityString {
            lcs_dicts.insert(
                mapping.target.clone(),
                crate::dictionary::FieldDictionary::new(),
            );
        }
    }

    // Build reverse maps BEFORE normalization to preserve original casing
    let reverse_maps = build_reverse_string_maps_with_dicts(&req.data_schema, Some(&lcs_dicts));

    // Persist config
    let mut data_schema = req.data_schema;
    data_schema.normalize_string_maps();
    let definition = IndexDefinition {
        name: req.name.clone(),
        config: req.config.clone(),
        data_schema,
    };
    let config_json = serde_json::to_string_pretty(&definition).unwrap();
    let config_path = index_dir.join("config.json");
    if let Err(e) = std::fs::write(&config_path, &config_json) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": format!("Failed to write config: {e}")})),
        ).into_response();
    }

    // Create engine
    let docstore_path = index_dir.join("docs");
    let mut config = req.config;
    config.storage.bitmap_path = Some(index_dir.join("bitmaps"));

    let mut engine = match ConcurrentEngine::new_with_path(config, &docstore_path) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": format!("Failed to create engine: {e}")})),
            ).into_response();
        }
    };

    // Set docstore field defaults for write-side elision
    engine.set_docstore_defaults(&definition.data_schema);

    // Set LowCardinalityString dictionaries
    if !lcs_dicts.is_empty() {
        engine.set_dictionaries(lcs_dicts);
    }

    // Build string_maps from DataSchema for MappedString + LowCardinalityString field reverse lookup
    let (string_maps, cs_fields) = build_string_maps_with_dicts(&definition.data_schema, Some(engine.dictionaries()));
    if !string_maps.is_empty() {
        engine.set_string_maps(string_maps);
    }
    if !cs_fields.is_empty() {
        engine.set_case_sensitive_fields(cs_fields);
    }

    let schema_registry = engine.build_schema_registry();

    *state.index.lock() = Some(IndexState {
        engine: Arc::new(engine),
        definition,
        reverse_maps: Arc::new(reverse_maps),
        schema_registry: Arc::new(schema_registry),
        tasks: Arc::new(TaskRegistry::new()),
    });

    (
        StatusCode::CREATED,
        Json(serde_json::json!({"name": req.name, "status": "created"})),
    ).into_response()
}

async fn handle_list_indexes(State(state): State<SharedState>) -> impl IntoResponse {
    let guard = state.index.lock();
    let indexes: Vec<serde_json::Value> = match guard.as_ref() {
        Some(idx) => vec![serde_json::json!({
            "name": idx.definition.name,
            "alive_count": idx.engine.alive_count(),
        })],
        None => vec![],
    };
    Json(serde_json::json!({"indexes": indexes}))
}

async fn handle_get_index(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
) -> impl IntoResponse {
    let guard = state.index.lock();
    match guard.as_ref() {
        Some(idx) if idx.definition.name == name => {
            Json(serde_json::json!({
                "name": idx.definition.name,
                "config": idx.definition.config,
                "data_schema": idx.definition.data_schema,
                "stats": {
                    "alive_count": idx.engine.alive_count(),
                    "slot_count": idx.engine.slot_counter(),
                }
            })).into_response()
        }
        _ => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("Index '{}' not found", name)})),
        ).into_response(),
    }
}

async fn handle_delete_index(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
) -> impl IntoResponse {
    let mut guard = state.index.lock();
    let exists = guard.as_ref().map(|idx| idx.definition.name == name).unwrap_or(false);
    if !exists {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("Index '{}' not found", name)})),
        ).into_response();
    }

    // Check if a task is active
    if let Some(idx) = guard.as_ref() {
        let snap = idx.tasks.snapshot();
        if snap.active.is_some() {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({"error": "Cannot delete index while a task is running"})),
            ).into_response();
        }
    }

    // Drop the index
    *guard = None;

    // Remove index directory
    let index_dir = state.data_dir.join("indexes").join(&name);
    if index_dir.exists() {
        if let Err(e) = std::fs::remove_dir_all(&index_dir) {
            eprintln!("Warning: failed to remove index directory: {e}");
        }
    }

    Json(serde_json::json!({"status": "deleted"})).into_response()
}

// ---------------------------------------------------------------------------
// Handlers: Data loading
// ---------------------------------------------------------------------------

async fn handle_load(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
    Json(req): Json<LoadRequest>,
) -> impl IntoResponse {
    let (engine, schema, tasks) = {
        let guard = state.index.lock();
        match guard.as_ref() {
            Some(idx) if idx.definition.name == name => (
                Arc::clone(&idx.engine),
                idx.definition.data_schema.clone(),
                Arc::clone(&idx.tasks),
            ),
            _ => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error": format!("Index '{}' not found", name)})),
                ).into_response();
            }
        }
    };

    let path = PathBuf::from(&req.path);
    if !path.exists() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": format!("File not found: {}", req.path)})),
        ).into_response();
    }

    let (task_id, progress) = match tasks.try_start(TaskType::Load) {
        Ok(v) => v,
        Err(active_info) => {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({
                    "error": "A task is already running",
                    "active_task": serde_json::to_value(&active_info).unwrap(),
                })),
            ).into_response();
        }
    };

    let limit = req.limit;
    let threads = req.threads;
    let chunk_size = req.chunk_size;
    let docstore_batch_size = req.docstore_batch_size;
    let max_writer_threads = req.max_writer_threads;
    let save_snapshot = req.save_snapshot;

    // Spawn blocking loading task with TaskGuard for panic safety
    let tasks_clone = Arc::clone(&tasks);
    tokio::task::spawn_blocking(move || {
        let mut guard = TaskGuard { tasks: tasks_clone, task_id: Some(task_id) };

        // Enter loading mode
        engine.enter_loading_mode();

        match loader::load_ndjson(&engine, &schema, &path, limit, threads, chunk_size, docstore_batch_size, max_writer_threads, progress.clone()) {
            Ok(stats) => {
                let alive;

                if save_snapshot {
                    // Combined exit-loading + save + unload: saves directly from
                    // staging without an intermediate full publish, eliminating the
                    // memory spike from staging.clone() at scale.
                    guard.tasks.set_saving(task_id);

                    let snap_start = Instant::now();
                    if let Err(e) = engine.exit_loading_mode_and_save_unload() {
                        eprintln!("Warning: failed to exit_loading_mode_and_save_unload: {e}");
                    } else {
                        eprintln!("exit_loading_mode_and_save_unload complete in {:.1}s", snap_start.elapsed().as_secs_f64());
                    }
                    // Alive bitmap is always preserved during unload
                    alive = engine.alive_count();
                } else {
                    // Just exit loading mode — no save needed
                    engine.exit_loading_mode();
                    alive = engine.alive_count();
                }

                eprintln!("Load complete: {} records alive", alive);

                guard.tasks.set_complete(task_id, Some(serde_json::json!({
                    "records_loaded": stats.records_loaded,
                    "elapsed_secs": stats.elapsed.as_secs_f64(),
                })));
                guard.defuse();
            }
            Err(e) => {
                engine.exit_loading_mode();
                guard.tasks.set_error(task_id, e.to_string());
                guard.defuse();
            }
        }
    });

    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({"task_id": task_id})),
    ).into_response()
}

// ---------------------------------------------------------------------------
// Handlers: Query & documents
// ---------------------------------------------------------------------------

/// Query request with optional field selection for document retrieval.
/// Used for the default "bitdex" format (backward-compatible serde deserialization).
#[derive(Deserialize)]
struct QueryRequest {
    #[serde(flatten)]
    query: BitdexQuery,
    /// `true` → all fields, `["field1","field2"]` → selected fields, `false`/omitted → IDs only.
    #[serde(default)]
    include_docs: IncludeDocs,
}

#[derive(Deserialize, Default)]
struct QueryParams {
    /// Query format: "bitdex" (default), "compact", "meilisearch"
    format: Option<String>,
}

async fn handle_query(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
    AxumQuery(params): AxumQuery<QueryParams>,
    body: Bytes,
) -> impl IntoResponse {
    // Resolve effective format: explicit ?format= overrides, otherwise use registry default
    let effective_format = params
        .format
        .as_deref()
        .unwrap_or(state.parser_registry.default_format());

    // Parse the query body through the appropriate parser
    let (query, include_docs) = if effective_format == "bitdex" {
        // BitDex native format: use serde for backward compatibility (includes include_docs)
        match serde_json::from_slice::<QueryRequest>(&body) {
            Ok(req) => (req.query, req.include_docs),
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": format!("invalid query: {e}")})),
                ).into_response();
            }
        }
    } else {
        // Pluggable format: parse through registry, extract include_docs separately
        let query = match state.parser_registry.parse(Some(effective_format), &body) {
            Ok(q) => q,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": e.to_string()})),
                ).into_response();
            }
        };
        // Try to extract include_docs from the raw JSON (works for any format)
        let include_docs = serde_json::from_slice::<serde_json::Value>(&body)
            .ok()
            .and_then(|v| v.get("include_docs").cloned())
            .and_then(|v| serde_json::from_value::<IncludeDocs>(v).ok())
            .unwrap_or_default();
        (query, include_docs)
    };

    let (engine, schema, reverse_maps, schema_registry) = {
        let guard = state.index.lock();
        match guard.as_ref() {
            Some(idx) if idx.definition.name == name => (
                Arc::clone(&idx.engine),
                idx.definition.data_schema.clone(),
                Arc::clone(&idx.reverse_maps),
                Arc::clone(&idx.schema_registry),
            ),
            _ => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error": format!("Index '{}' not found", name)})),
                ).into_response();
            }
        }
    };

    tracing::info!("[{name}] {query}");
    let start = Instant::now();
    let m = &state.metrics;
    match engine.execute_query(&query) {
        Ok(result) => {
            let elapsed = start.elapsed();
            let elapsed_us = elapsed.as_micros() as u64;
            tracing::info!("[{name}]   → {} results, {elapsed_us}μs", result.total_matched);
            m.query_total.with_label_values(&[&name]).inc();
            m.query_duration_seconds
                .with_label_values(&[&name])
                .observe(elapsed.as_secs_f64());
            let cursor = result.cursor.map(|c| serde_json::to_value(c).unwrap());

            let documents = if !include_docs.is_none() {
                let mut docs = Vec::with_capacity(result.ids.len());
                for &id in &result.ids {
                    let doc = engine.get_document(id as u32);
                    docs.push(match doc {
                        Ok(Some(stored)) => {
                            format_document(&stored, &schema, &reverse_maps, &include_docs, &schema_registry)
                        }
                        _ => serde_json::json!({ "id": id }),
                    });
                }
                Some(docs)
            } else {
                None
            };

            let mut response = serde_json::json!({
                "ids": result.ids,
                "cursor": cursor,
                "total_matched": result.total_matched,
                "elapsed_us": elapsed_us,
            });
            if let Some(docs) = documents {
                response["documents"] = serde_json::json!(docs);
            }

            Json(response).into_response()
        }
        Err(e) => {
            let elapsed_us = start.elapsed().as_micros() as u64;
            tracing::warn!("[{name}]   → ERROR: {e}, {elapsed_us}μs");
            m.query_total.with_label_values(&[&name]).inc();
            m.query_duration_seconds
                .with_label_values(&[&name])
                .observe(start.elapsed().as_secs_f64());
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": e.to_string()})),
            ).into_response()
        }
    }
}

async fn handle_document(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
    Json(req): Json<DocumentRequest>,
) -> impl IntoResponse {
    let (engine, schema, reverse_maps, schema_registry) = {
        let guard = state.index.lock();
        match guard.as_ref() {
            Some(idx) if idx.definition.name == name => (
                Arc::clone(&idx.engine),
                idx.definition.data_schema.clone(),
                Arc::clone(&idx.reverse_maps),
                Arc::clone(&idx.schema_registry),
            ),
            _ => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error": format!("Index '{}' not found", name)})),
                ).into_response();
            }
        }
    };

    match engine.get_document(req.slot_id) {
        Ok(Some(doc)) => Json(format_document(&doc, &schema, &reverse_maps, &req.fields, &schema_registry)).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "not found"}))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))).into_response(),
    }
}

async fn handle_documents_batch(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
    Json(req): Json<DocumentBatchRequest>,
) -> impl IntoResponse {
    let (engine, schema, reverse_maps, schema_registry) = {
        let guard = state.index.lock();
        match guard.as_ref() {
            Some(idx) if idx.definition.name == name => (
                Arc::clone(&idx.engine),
                idx.definition.data_schema.clone(),
                Arc::clone(&idx.reverse_maps),
                Arc::clone(&idx.schema_registry),
            ),
            _ => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error": format!("Index '{}' not found", name)})),
                ).into_response();
            }
        }
    };

    let mut docs = Vec::with_capacity(req.slot_ids.len());
    for slot_id in &req.slot_ids {
        match engine.get_document(*slot_id) {
            Ok(Some(doc)) => docs.push(format_document(&doc, &schema, &reverse_maps, &req.fields, &schema_registry)),
            Ok(None) => docs.push(serde_json::json!({"id": slot_id})),
            Err(_) => docs.push(serde_json::json!({"id": slot_id})),
        }
    }
    Json(serde_json::json!({"documents": docs})).into_response()
}

async fn handle_upsert(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
    Json(req): Json<UpsertRequest>,
) -> impl IntoResponse {
    let engine = {
        let guard = state.index.lock();
        match guard.as_ref() {
            Some(idx) if idx.definition.name == name => {
                Arc::clone(&idx.engine)
            }
            _ => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error": format!("Index '{}' not found", name)})),
                ).into_response();
            }
        }
    };

    // Get schema and dictionaries for the upsert
    let (schema, has_lcs) = {
        let guard = state.index.lock();
        let idx = guard.as_ref().unwrap();
        let has_lcs = idx.definition.data_schema.fields.iter().any(|f| f.value_type == FieldValueType::LowCardinalityString);
        (idx.definition.data_schema.clone(), has_lcs)
    };

    let mut upserted = 0u64;
    let mut errors: Vec<String> = Vec::new();

    for (i, doc_json) in req.documents.iter().enumerate() {
        let dicts = if has_lcs { Some(engine.dictionaries()) } else { None };
        match loader::json_to_document_with_dicts(doc_json, &schema, dicts) {
            Ok((slot, doc)) => {
                if let Err(e) = engine.put(slot, &doc) {
                    errors.push(format!("doc[{}] id={}: {}", i, slot, e));
                } else {
                    upserted += 1;
                }
            }
            Err(e) => {
                errors.push(format!("doc[{}]: {}", i, e));
            }
        }
    }

    // Set cursor if provided (after mutations are submitted to coalescer)
    if let Some(cursor) = req.cursor {
        engine.set_cursor(cursor.name, cursor.value);
    }

    // Rebuild reverse maps if LCS dictionaries gained new values.
    // Ensures newly-upserted string values are reverse-mappable when serving documents.
    // Query-time resolution already falls through to live dictionaries (no rebuild needed).
    if has_lcs && upserted > 0 {
        // Persist dirty dictionaries before updating reverse maps.
        // This ensures dictionary mappings survive crashes — a doc on disk
        // always has its integer keys resolvable via the persisted dictionary.
        if let Err(e) = engine.persist_dirty_dictionaries() {
            eprintln!("warning: failed to persist LCS dictionaries: {}", e);
        }

        let mut guard = state.index.lock();
        if let Some(ref mut idx) = *guard {
            let dicts = engine.dictionaries();
            let reverse_maps = build_reverse_string_maps_with_dicts(&idx.definition.data_schema, Some(dicts));
            idx.reverse_maps = Arc::new(reverse_maps);
        }
    }

    state
        .metrics
        .upsert_total
        .with_label_values(&[&name])
        .inc_by(upserted);

    if errors.is_empty() {
        Json(serde_json::json!({"upserted": upserted})).into_response()
    } else {
        (
            StatusCode::OK,
            Json(serde_json::json!({"upserted": upserted, "errors": errors})),
        ).into_response()
    }
}

async fn handle_delete_docs(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
    Json(req): Json<DeleteDocsRequest>,
) -> impl IntoResponse {
    let engine = {
        let guard = state.index.lock();
        match guard.as_ref() {
            Some(idx) if idx.definition.name == name => Arc::clone(&idx.engine),
            _ => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error": format!("Index '{}' not found", name)})),
                ).into_response();
            }
        }
    };

    let mut deleted = 0u64;
    let mut errors: Vec<String> = Vec::new();

    for id in &req.ids {
        match engine.delete(*id) {
            Ok(()) => deleted += 1,
            Err(e) => errors.push(format!("id={}: {}", id, e)),
        }
    }

    // Set cursor if provided (after mutations are submitted to coalescer)
    if let Some(cursor) = req.cursor {
        engine.set_cursor(cursor.name, cursor.value);
    }

    state
        .metrics
        .delete_total
        .with_label_values(&[&name])
        .inc_by(deleted);

    if errors.is_empty() {
        Json(serde_json::json!({"deleted": deleted})).into_response()
    } else {
        (
            StatusCode::OK,
            Json(serde_json::json!({"deleted": deleted, "errors": errors})),
        ).into_response()
    }
}

async fn handle_stats(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
) -> impl IntoResponse {
    let engine = {
        let guard = state.index.lock();
        match guard.as_ref() {
            Some(idx) if idx.definition.name == name => Arc::clone(&idx.engine),
            _ => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error": format!("Index '{}' not found", name)})),
                ).into_response();
            }
        }
    };

    let (slot_bytes, filter_bytes, sort_bytes, _, _, _, _) = engine.bitmap_memory_report();
    let uc = engine.unified_cache_stats();
    let entries: Vec<serde_json::Value> = engine.unified_cache_entry_details().into_iter().map(|e| {
        serde_json::json!({
            "sort_field": e.sort_field,
            "direction": e.direction,
            "filter_count": e.filter_count,
            "cardinality": e.cardinality,
            "capacity": e.capacity,
            "max_capacity": e.max_capacity,
            "has_more": e.has_more,
            "min_tracked_value": e.min_tracked_value,
        })
    }).collect();
    let eviction: Vec<serde_json::Value> = engine.eviction_stats().into_iter().map(|(name, total, resident)| {
        serde_json::json!({
            "field": name,
            "evicted_total": total,
            "resident_values": resident,
        })
    }).collect();
    Json(serde_json::json!({
        "alive_count": engine.alive_count(),
        "slot_count": engine.slot_counter(),
        "flush_cycle": engine.flush_cycle(),
        "slot_bitmap_bytes": slot_bytes,
        "filter_bitmap_bytes": filter_bytes,
        "sort_bitmap_bytes": sort_bytes,
        "unified_cache_entries": uc.entries,
        "unified_cache_hits": uc.hits,
        "unified_cache_misses": uc.misses,
        "unified_cache_bytes": uc.memory_bytes,
        "unified_cache_meta_entries": uc.meta_index_entries,
        "unified_cache_meta_bytes": uc.meta_index_bytes,
        "unified_cache_persistence_enabled": uc.persistence_enabled,
        "unified_cache_tombstones": uc.tombstone_count,
        "unified_cache_pending_shards": uc.pending_shard_count,
        "unified_cache_dirty_shards": uc.dirty_shard_count,
        "unified_cache_meta_dirty": uc.meta_dirty,
        "unified_cache_disk_bytes": engine.boundstore_disk_bytes(),
        "unified_cache_shard_load_count": engine.boundstore_shard_loads(),
        "unified_cache_tombstones_created": engine.boundstore_tombstones_created(),
        "unified_cache_tombstones_cleaned": engine.boundstore_tombstones_cleaned(),
        "unified_cache_entries_restored": engine.boundstore_entries_restored(),
        "unified_cache_entries_skipped": engine.boundstore_entries_skipped(),
        "unified_cache_bytes_written": engine.boundstore_bytes_written(),
        "unified_cache_bytes_read": engine.boundstore_bytes_read(),
        "unified_cache_entry_details": entries,
        "eviction": eviction,
    })).into_response()
}

async fn handle_clear_cache(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
) -> impl IntoResponse {
    let engine = {
        let guard = state.index.lock();
        match guard.as_ref() {
            Some(idx) if idx.definition.name == name => Arc::clone(&idx.engine),
            _ => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error": format!("Index '{}' not found", name)})),
                ).into_response();
            }
        }
    };

    engine.clear_unified_cache();
    Json(serde_json::json!({"cleared": true, "scope": "ram_only"})).into_response()
}

/// DELETE /api/indexes/{name}/cache/persistent — purge disk + RAM cache.
/// Wipes all BoundStore files (meta.bin + shards) then clears the in-memory
/// cache and meta-index. Safe to call while the server is running.
async fn handle_purge_cache(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
) -> impl IntoResponse {
    let engine = {
        let guard = state.index.lock();
        match guard.as_ref() {
            Some(idx) if idx.definition.name == name => Arc::clone(&idx.engine),
            _ => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error": format!("Index '{}' not found", name)})),
                ).into_response();
            }
        }
    };

    match engine.purge_bounds() {
        Ok(()) => Json(serde_json::json!({
            "purged": true,
            "scope": "disk_and_ram",
        })).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": format!("purge failed: {e}")})),
        ).into_response(),
    }
}

// ---------------------------------------------------------------------------
// Handlers: Rebuild
// ---------------------------------------------------------------------------

async fn handle_rebuild(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
    Json(req): Json<RebuildRequest>,
) -> impl IntoResponse {
    let (engine, config, tasks) = {
        let guard = state.index.lock();
        match guard.as_ref() {
            Some(idx) if idx.definition.name == name => (
                Arc::clone(&idx.engine),
                idx.definition.config.clone(),
                Arc::clone(&idx.tasks),
            ),
            _ => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error": format!("Index '{}' not found", name)})),
                ).into_response();
            }
        }
    };

    // Validate field names
    if let Some(ref sort_names) = req.sort_fields {
        for name in sort_names {
            if !config.sort_fields.iter().any(|sc| &sc.name == name) {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": format!("Unknown sort field: {}", name)})),
                ).into_response();
            }
        }
    }
    if let Some(ref filter_names) = req.filter_fields {
        for name in filter_names {
            if !config.filter_fields.iter().any(|fc| &fc.name == name) {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": format!("Unknown filter field: {}", name)})),
                ).into_response();
            }
        }
    }

    let (task_id, progress) = match tasks.try_start(TaskType::Rebuild) {
        Ok(v) => v,
        Err(active_info) => {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({
                    "error": "A task is already running",
                    "active_task": serde_json::to_value(&active_info).unwrap(),
                })),
            ).into_response();
        }
    };

    let sort_fields = req.sort_fields;
    let filter_fields = req.filter_fields;
    let save = req.save_snapshot;

    let tasks_clone = Arc::clone(&tasks);
    tokio::task::spawn_blocking(move || {
        let mut guard = TaskGuard { tasks: tasks_clone, task_id: Some(task_id) };

        match engine.rebuild_fields_from_docstore(sort_fields, filter_fields, progress.clone()) {
            Ok((slots, fields)) => {
                if save {
                    guard.tasks.set_saving(task_id);

                    let snap_start = Instant::now();
                    if let Err(e) = engine.save_and_unload() {
                        eprintln!("rebuild: failed to save_and_unload: {e}");
                    } else {
                        eprintln!("rebuild: save_and_unload complete in {:.1}s", snap_start.elapsed().as_secs_f64());
                    }
                }

                guard.tasks.set_complete(task_id, Some(serde_json::json!({
                    "records_loaded": slots,
                    "fields": fields,
                })));
                guard.defuse();

                eprintln!("rebuild: done — {} slots, {} fields", slots, fields.len());
            }
            Err(e) => {
                guard.tasks.set_error(task_id, format!("Rebuild failed: {}", e));
                guard.defuse();
            }
        }
    });

    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({"task_id": task_id})),
    ).into_response()
}

// ---------------------------------------------------------------------------
// Handlers: Add Fields
// ---------------------------------------------------------------------------

async fn handle_add_fields(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
    Json(req): Json<AddFieldsRequest>,
) -> impl IntoResponse {
    if req.filter_fields.is_empty() && req.sort_fields.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "No fields specified"})),
        ).into_response();
    }

    let (engine, tasks) = {
        let mut guard = state.index.lock();
        match guard.as_mut() {
            Some(idx) if idx.definition.name == name => {
                // Validate no duplicate field names with existing config
                for fc in &req.filter_fields {
                    if idx.definition.config.filter_fields.iter().any(|f| f.name == fc.name) {
                        return (
                            StatusCode::CONFLICT,
                            Json(serde_json::json!({"error": format!("Filter field '{}' already exists", fc.name)})),
                        ).into_response();
                    }
                }
                for sc in &req.sort_fields {
                    if idx.definition.config.sort_fields.iter().any(|f| f.name == sc.name) {
                        return (
                            StatusCode::CONFLICT,
                            Json(serde_json::json!({"error": format!("Sort field '{}' already exists", sc.name)})),
                        ).into_response();
                    }
                }

                // Update the persisted config with the new fields
                idx.definition.config.filter_fields.extend(req.filter_fields.clone());
                idx.definition.config.sort_fields.extend(req.sort_fields.clone());

                // Save updated config.json
                let index_dir = state.data_dir.join("indexes").join(&name);
                let config_json = serde_json::to_string_pretty(&idx.definition).unwrap();
                if let Err(e) = std::fs::write(index_dir.join("config.json"), &config_json) {
                    // Rollback config changes
                    for fc in &req.filter_fields {
                        idx.definition.config.filter_fields.retain(|f| f.name != fc.name);
                    }
                    for sc in &req.sort_fields {
                        idx.definition.config.sort_fields.retain(|f| f.name != sc.name);
                    }
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({"error": format!("Failed to persist config: {e}")})),
                    ).into_response();
                }

                (
                    Arc::clone(&idx.engine),
                    Arc::clone(&idx.tasks),
                )
            }
            _ => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error": format!("Index '{}' not found", name)})),
                ).into_response();
            }
        }
    };

    // Validate fields exist in docstore (unless skipped)
    if !req.skip_validation {
        let all_names: Vec<&str> = req.filter_fields.iter().map(|f| f.name.as_str())
            .chain(req.sort_fields.iter().map(|f| f.name.as_str()))
            .collect();

        match engine.validate_fields_in_docstore(&all_names) {
            Ok(missing) if !missing.is_empty() => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "error": format!("Fields not found in docstore: {:?}", missing),
                        "hint": "Set skip_validation=true to add fields that may not exist in all documents"
                    })),
                ).into_response();
            }
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": format!("Validation failed: {e}")})),
                ).into_response();
            }
            _ => {}
        }
    }

    let (task_id, progress) = match tasks.try_start(TaskType::AddFields) {
        Ok(v) => v,
        Err(active_info) => {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({
                    "error": "A task is already running",
                    "active_task": serde_json::to_value(&active_info).unwrap(),
                })),
            ).into_response();
        }
    };

    let filter_fields = req.filter_fields;
    let sort_fields = req.sort_fields;
    let save = req.save_snapshot;

    let tasks_clone = Arc::clone(&tasks);
    tokio::task::spawn_blocking(move || {
        let mut guard = TaskGuard { tasks: tasks_clone, task_id: Some(task_id) };

        match engine.add_fields_from_docstore(filter_fields, sort_fields, progress) {
            Ok((slots, fields)) => {
                if save {
                    guard.tasks.set_saving(task_id);

                    let snap_start = Instant::now();
                    if let Err(e) = engine.save_and_unload() {
                        eprintln!("add_fields: save_and_unload failed: {e}");
                    } else {
                        eprintln!("add_fields: save_and_unload in {:.1}s", snap_start.elapsed().as_secs_f64());
                    }
                }

                guard.tasks.set_complete(task_id, Some(serde_json::json!({
                    "records_loaded": slots,
                    "fields": fields,
                })));
                guard.defuse();

                eprintln!("add_fields: done — {} slots, {} fields", slots, fields.len());
            }
            Err(e) => {
                guard.tasks.set_error(task_id, format!("Add fields failed: {}", e));
                guard.defuse();
            }
        }
    });

    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({"task_id": task_id})),
    ).into_response()
}

// ---------------------------------------------------------------------------
// Handlers: Remove Fields
// ---------------------------------------------------------------------------

async fn handle_remove_fields(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
    Json(req): Json<RemoveFieldsRequest>,
) -> impl IntoResponse {
    if req.filter_fields.is_empty() && req.sort_fields.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "No fields specified"})),
        ).into_response();
    }

    let (engine, tasks) = {
        let mut guard = state.index.lock();
        match guard.as_mut() {
            Some(idx) if idx.definition.name == name => {
                // Validate fields exist in current config
                for fname in &req.filter_fields {
                    if !idx.definition.config.filter_fields.iter().any(|f| &f.name == fname) {
                        return (
                            StatusCode::BAD_REQUEST,
                            Json(serde_json::json!({"error": format!("Filter field '{}' not found in config", fname)})),
                        ).into_response();
                    }
                }
                for sname in &req.sort_fields {
                    if !idx.definition.config.sort_fields.iter().any(|f| &f.name == sname) {
                        return (
                            StatusCode::BAD_REQUEST,
                            Json(serde_json::json!({"error": format!("Sort field '{}' not found in config", sname)})),
                        ).into_response();
                    }
                }

                // Update config: remove fields
                for fname in &req.filter_fields {
                    idx.definition.config.filter_fields.retain(|f| &f.name != fname);
                }
                for sname in &req.sort_fields {
                    idx.definition.config.sort_fields.retain(|f| &f.name != sname);
                }

                // Save updated config.json
                let index_dir = state.data_dir.join("indexes").join(&name);
                let config_json = serde_json::to_string_pretty(&idx.definition).unwrap();
                if let Err(e) = std::fs::write(index_dir.join("config.json"), &config_json) {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({"error": format!("Failed to persist config: {e}")})),
                    ).into_response();
                }

                (
                    Arc::clone(&idx.engine),
                    Arc::clone(&idx.tasks),
                )
            }
            _ => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error": format!("Index '{}' not found", name)})),
                ).into_response();
            }
        }
    };

    let (task_id, _progress) = match tasks.try_start(TaskType::RemoveFields) {
        Ok(v) => v,
        Err(active_info) => {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({
                    "error": "A task is already running",
                    "active_task": serde_json::to_value(&active_info).unwrap(),
                })),
            ).into_response();
        }
    };

    let filter_fields = req.filter_fields;
    let sort_fields = req.sort_fields;
    let save = req.save_snapshot;

    let tasks_clone = Arc::clone(&tasks);
    tokio::task::spawn_blocking(move || {
        let mut guard = TaskGuard { tasks: tasks_clone, task_id: Some(task_id) };

        match engine.remove_fields(&filter_fields, &sort_fields) {
            Ok(removed) => {
                if save {
                    guard.tasks.set_saving(task_id);

                    let snap_start = Instant::now();
                    if let Err(e) = engine.save_and_unload() {
                        eprintln!("remove_fields: save_and_unload failed: {e}");
                    } else {
                        eprintln!("remove_fields: save_and_unload in {:.1}s", snap_start.elapsed().as_secs_f64());
                    }
                }

                guard.tasks.set_complete(task_id, Some(serde_json::json!({
                    "removed": removed,
                })));
                guard.defuse();

                eprintln!("remove_fields: done — removed {:?}", removed);
            }
            Err(e) => {
                guard.tasks.set_error(task_id, format!("Remove fields failed: {}", e));
                guard.defuse();
            }
        }
    });

    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({"task_id": task_id})),
    ).into_response()
}

// ---------------------------------------------------------------------------
// Handlers: Task status
// ---------------------------------------------------------------------------

async fn handle_list_tasks(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
) -> impl IntoResponse {
    let guard = state.index.lock();
    match guard.as_ref() {
        Some(idx) if idx.definition.name == name => {
            let snap = idx.tasks.snapshot();
            Json(serde_json::to_value(&snap).unwrap()).into_response()
        }
        _ => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("Index '{}' not found", name)})),
        ).into_response(),
    }
}

async fn handle_get_task(
    State(state): State<SharedState>,
    AxumPath(task_id): AxumPath<u64>,
) -> impl IntoResponse {
    let guard = state.index.lock();
    if let Some(idx) = guard.as_ref() {
        if let Some(info) = idx.tasks.get(task_id) {
            return Json(serde_json::to_value(&info).unwrap()).into_response();
        }
    }
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({"error": format!("Task {} not found", task_id)})),
    ).into_response()
}

// ---------------------------------------------------------------------------
// Handlers: Snapshot
// ---------------------------------------------------------------------------

async fn handle_save_snapshot(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
    query: axum::extract::Query<SnapshotParams>,
) -> impl IntoResponse {
    let engine = {
        let guard = state.index.lock();
        match guard.as_ref() {
            Some(idx) if idx.definition.name == name => Arc::clone(&idx.engine),
            _ => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error": format!("Index '{}' not found", name)})),
                ).into_response();
            }
        }
    };

    let t0 = std::time::Instant::now();
    let result = if query.unload {
        engine.save_and_unload()
    } else {
        engine.save_snapshot()
    };
    match result {
        Ok(()) => {
            let elapsed = t0.elapsed().as_secs_f64();
            Json(serde_json::json!({
                "status": if query.unload { "saved_and_unloaded" } else { "saved" },
                "elapsed_secs": elapsed,
            })).into_response()
        }
        Err(e) => {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": format!("Snapshot save failed: {e}")})),
            ).into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// Handlers: Cursors
// ---------------------------------------------------------------------------

async fn handle_get_cursor(
    State(state): State<SharedState>,
    AxumPath((name, cursor_name)): AxumPath<(String, String)>,
) -> impl IntoResponse {
    let engine = {
        let guard = state.index.lock();
        match guard.as_ref() {
            Some(idx) if idx.definition.name == name => Arc::clone(&idx.engine),
            _ => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error": format!("Index '{}' not found", name)})),
                ).into_response();
            }
        }
    };

    match engine.get_cursor(&cursor_name) {
        Some(value) => Json(serde_json::json!({
            "name": cursor_name,
            "value": value,
        })).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("Cursor '{}' not found", cursor_name)})),
        ).into_response(),
    }
}

async fn handle_list_cursors(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
) -> impl IntoResponse {
    let engine = {
        let guard = state.index.lock();
        match guard.as_ref() {
            Some(idx) if idx.definition.name == name => Arc::clone(&idx.engine),
            _ => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error": format!("Index '{}' not found", name)})),
                ).into_response();
            }
        }
    };

    let cursors = engine.get_all_cursors();
    let entries: Vec<serde_json::Value> = cursors
        .into_iter()
        .map(|(name, value)| serde_json::json!({"name": name, "value": value}))
        .collect();
    Json(serde_json::json!({"cursors": entries})).into_response()
}

// ---------------------------------------------------------------------------
// Handlers: Utility
// ---------------------------------------------------------------------------

async fn handle_health() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn handle_list_formats(State(state): State<SharedState>) -> impl IntoResponse {
    let mut formats = state.parser_registry.formats();
    formats.sort();
    Json(serde_json::json!({
        "formats": formats,
        "default": state.parser_registry.default_format(),
    }))
}

async fn handle_metrics(State(state): State<SharedState>) -> impl IntoResponse {
    let m = &state.metrics;

    // Collect-on-scrape: refresh all gauges from current engine state.
    {
        let guard = state.index.lock();
        if let Some(idx) = guard.as_ref() {
            let name = &idx.definition.name;
            let engine = &idx.engine;

            // Document lifecycle gauges
            m.alive_documents
                .with_label_values(&[name])
                .set(engine.alive_count() as i64);
            m.slot_high_water
                .with_label_values(&[name])
                .set(engine.slot_counter() as i64);

            // Cache gauges
            let uc = engine.unified_cache_stats();
            m.cache_entries
                .with_label_values(&[name])
                .set(uc.entries as i64);
            m.cache_bytes
                .with_label_values(&[name])
                .set(uc.memory_bytes as i64);
            m.cache_hits_total
                .with_label_values(&[name])
                .set(uc.hits as i64);
            m.cache_misses_total
                .with_label_values(&[name])
                .set(uc.misses as i64);

            // Per-field bitmap memory gauges
            let (slot_bytes, _filter_bytes, _sort_bytes, _ce, _cb, filter_details, sort_details) =
                engine.bitmap_memory_report();
            m.slot_bitmap_bytes
                .with_label_values(&[name])
                .set(slot_bytes as i64);
            for (field, count, bytes) in &filter_details {
                m.filter_bitmap_bytes
                    .with_label_values(&[name, field])
                    .set(*bytes as i64);
                m.filter_bitmap_count
                    .with_label_values(&[name, field])
                    .set(*count as i64);
            }
            for (field, bytes) in &sort_details {
                m.sort_bitmap_bytes
                    .with_label_values(&[name, field])
                    .set(*bytes as i64);
            }

            // Flush pipeline stats
            let (pub_count, _cumulative_nanos, last_nanos) = engine.flush_stats();
            m.snapshot_publish_total
                .with_label_values(&[name])
                .set(pub_count as i64);
            m.flush_last_duration_seconds
                .with_label_values(&[name])
                .set(last_nanos as i64);

            // Pending fields (lazy loading)
            let pending = engine.pending_field_count();
            m.pending_fields
                .with_label_values(&[name])
                .set(pending as i64);

            // Eviction stats
            for (field, total, resident) in engine.eviction_stats() {
                m.eviction_total
                    .with_label_values(&[name, &field])
                    .set(total as i64);
                m.eviction_resident_values
                    .with_label_values(&[name, &field])
                    .set(resident as i64);
            }

            // BoundStore stats
            m.boundstore_meta_entries
                .with_label_values(&[name])
                .set(uc.meta_index_entries as i64);
            m.boundstore_tombstones
                .with_label_values(&[name])
                .set(uc.tombstone_count as i64);
            m.boundstore_pending_shards
                .with_label_values(&[name])
                .set(uc.pending_shard_count as i64);
            m.boundstore_disk_bytes
                .with_label_values(&[name])
                .set(engine.boundstore_disk_bytes() as i64);
            m.boundstore_shard_loads_total
                .with_label_values(&[name])
                .set(engine.boundstore_shard_loads() as i64);
            m.boundstore_tombstones_created
                .with_label_values(&[name])
                .set(engine.boundstore_tombstones_created() as i64);
            m.boundstore_tombstones_cleaned
                .with_label_values(&[name])
                .set(engine.boundstore_tombstones_cleaned() as i64);
            m.boundstore_entries_restored
                .with_label_values(&[name])
                .set(engine.boundstore_entries_restored() as i64);
            m.boundstore_bytes_written
                .with_label_values(&[name])
                .set(engine.boundstore_bytes_written() as i64);
            m.boundstore_bytes_read
                .with_label_values(&[name])
                .set(engine.boundstore_bytes_read() as i64);
        }
    }

    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        m.gather(),
    )
}

async fn handle_ui() -> impl IntoResponse {
    Html(include_str!("../static/index.html"))
}
