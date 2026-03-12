//! Generic HTTP server for BitDex — no dataset-specific code.
//!
//! Feature-gated behind `server`. Provides `BitdexServer` which starts blank
//! and creates indexes via API.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use axum::extract::{Path as AxumPath, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Json};
use axum::routing::{get, post, delete};
use axum::Router;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tower_http::cors::CorsLayer;

use crate::concurrent_engine::ConcurrentEngine;
use crate::config::{Config, DataSchema, FieldValueType};
use crate::docstore::StoredDoc;
use crate::executor::{CaseSensitiveFields, StringMaps};
use crate::loader;
use crate::metrics::Metrics;
use crate::mutation::FieldValue;
use crate::query::{BitdexQuery, Value};

// ---------------------------------------------------------------------------
// Server state
// ---------------------------------------------------------------------------

/// Load status for an index.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "status")]
pub enum LoadStatus {
    #[serde(rename = "idle")]
    Idle,
    #[serde(rename = "loading")]
    Loading {
        records_loaded: u64,
        elapsed_secs: f64,
    },
    #[serde(rename = "saving")]
    Saving {
        records_loaded: u64,
        elapsed_secs: f64,
    },
    #[serde(rename = "complete")]
    Complete {
        records_loaded: u64,
        elapsed_secs: f64,
    },
    #[serde(rename = "error")]
    Error { message: String },
}

/// Persisted index definition (saved as config.json in the index directory).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDefinition {
    pub name: String,
    pub config: Config,
    pub data_schema: DataSchema,
}

/// Live state for a single index.
struct IndexState {
    engine: Arc<ConcurrentEngine>,
    definition: IndexDefinition,
    load_progress: Arc<AtomicU64>,
    load_status: Arc<Mutex<LoadStatus>>,
    load_started_at: Arc<Mutex<Option<Instant>>>,
}

/// Shared application state.
struct AppState {
    data_dir: PathBuf,
    index: Mutex<Option<IndexState>>,
    metrics: Metrics,
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
fn format_document(
    doc: &StoredDoc,
    schema: &DataSchema,
    selection: &IncludeDocs,
) -> serde_json::Value {
    let mut fields = serde_json::Map::new();

    // Always include "id" if present
    if let Some(id_val) = doc.fields.get("id") {
        fields.insert("id".to_string(), field_value_to_json(id_val));
    }

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
            field_value_to_json(fv)
        } else {
            // Fuse schema default for elided fields
            default_json_for_type(&mapping.value_type)
        };
        fields.insert(mapping.target.clone(), value);
    }

    serde_json::Value::Object(fields)
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
        FieldValueType::Integer | FieldValueType::MappedString => serde_json::json!(0),
        FieldValueType::Boolean | FieldValueType::ExistsBoolean => serde_json::json!(false),
        FieldValueType::String => serde_json::json!(""),
        FieldValueType::IntegerArray => serde_json::json!([]),
    }
}

#[derive(Deserialize)]
struct UpsertRequest {
    documents: Vec<serde_json::Value>,
}

#[derive(Deserialize)]
struct DeleteDocsRequest {
    ids: Vec<u32>,
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

// ---------------------------------------------------------------------------
// Public server entry point
// ---------------------------------------------------------------------------

/// The BitDex HTTP server. Starts blank and creates indexes via API.
pub struct BitdexServer {
    data_dir: PathBuf,
}

impl BitdexServer {
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
    }

    /// Start the HTTP server. Blocks until the server shuts down.
    pub async fn serve(self, addr: SocketAddr) -> std::io::Result<()> {
        // Ensure data directory exists
        std::fs::create_dir_all(&self.data_dir).ok();

        let state = Arc::new(AppState {
            data_dir: self.data_dir.clone(),
            index: Mutex::new(None),
            metrics: Metrics::new(),
        });

        // Try to restore an existing index from disk
        if let Err(e) = restore_index(&state) {
            eprintln!("Warning: failed to restore index from disk: {e}");
        }

        let app = Router::new()
            // Index management
            .route("/api/indexes", post(handle_create_index))
            .route("/api/indexes", get(handle_list_indexes))
            .route("/api/indexes/{name}", get(handle_get_index))
            .route("/api/indexes/{name}", delete(handle_delete_index))
            // Data loading
            .route("/api/indexes/{name}/load", post(handle_load))
            .route("/api/indexes/{name}/load/status", get(handle_load_status))
            // Query & documents
            .route("/api/indexes/{name}/query", post(handle_query))
            .route("/api/indexes/{name}/document", post(handle_document))
            .route("/api/indexes/{name}/documents", post(handle_documents_batch).delete(handle_delete_docs))
            .route("/api/indexes/{name}/documents/upsert", post(handle_upsert))
            .route("/api/indexes/{name}/stats", get(handle_stats))
            .route("/api/indexes/{name}/cache", delete(handle_clear_cache))
            .route("/api/indexes/{name}/rebuild", post(handle_rebuild))
            // Utility
            .route("/api/health", get(handle_health))
            .route("/metrics", get(handle_metrics))
            // Serve static UI
            .route("/", get(handle_ui))
            .layer(CorsLayer::permissive())
            .with_state(state);

        eprintln!("BitDex server listening on http://{}", addr);

        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(listener, app).await?;
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
        def.data_schema.normalize_string_maps();

        // Create engine from persisted config
        let docstore_path = path.join("docs");
        let mut config = def.config.clone();
        config.storage.bitmap_path = Some(path.join("bitmaps"));

        // Always use new_with_path so bitmaps restore from bitmap_path even if
        // docstore doesn't exist yet (it will be created fresh).
        let mut engine = ConcurrentEngine::new_with_path(config, &docstore_path)
            .map_err(|e| e.to_string())?;

        // Build string_maps from DataSchema for MappedString field reverse lookup
        let (string_maps, cs_fields) = build_string_maps(&def.data_schema);
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

        let load_status = if alive > 0 {
            LoadStatus::Complete {
                records_loaded: alive,
                elapsed_secs: 0.0,
            }
        } else {
            LoadStatus::Idle
        };

        *state.index.lock() = Some(IndexState {
            engine: Arc::new(engine),
            definition: def,
            load_progress: Arc::new(AtomicU64::new(alive)),
            load_status: Arc::new(Mutex::new(load_status)),
            load_started_at: Arc::new(Mutex::new(None)),
        });

        // Only restore the first index (single-index for now)
        break;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Handlers: Index management
// ---------------------------------------------------------------------------

/// Build reverse string maps from DataSchema for MappedString field query resolution.
fn build_string_maps(schema: &DataSchema) -> (StringMaps, CaseSensitiveFields) {
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
        }
    }
    (maps, cs_fields)
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

    // Build string_maps from DataSchema for MappedString field reverse lookup
    let (string_maps, cs_fields) = build_string_maps(&definition.data_schema);
    if !string_maps.is_empty() {
        engine.set_string_maps(string_maps);
    }
    if !cs_fields.is_empty() {
        engine.set_case_sensitive_fields(cs_fields);
    }

    *state.index.lock() = Some(IndexState {
        engine: Arc::new(engine),
        definition,
        load_progress: Arc::new(AtomicU64::new(0)),
        load_status: Arc::new(Mutex::new(LoadStatus::Idle)),
        load_started_at: Arc::new(Mutex::new(None)),
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

    // Check if loading or saving
    if let Some(idx) = guard.as_ref() {
        let status = idx.load_status.lock();
        if matches!(*status, LoadStatus::Loading { .. } | LoadStatus::Saving { .. }) {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({"error": "Cannot delete index while loading or saving"})),
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
    let (engine, schema, load_progress, load_status, load_started_at) = {
        let guard = state.index.lock();
        match guard.as_ref() {
            Some(idx) if idx.definition.name == name => {
                // Check if already loading or saving
                {
                    let status = idx.load_status.lock();
                    if matches!(*status, LoadStatus::Loading { .. } | LoadStatus::Saving { .. }) {
                        return (
                            StatusCode::CONFLICT,
                            Json(serde_json::json!({"error": "Already loading or saving"})),
                        ).into_response();
                    }
                }
                (
                    Arc::clone(&idx.engine),
                    idx.definition.data_schema.clone(),
                    Arc::clone(&idx.load_progress),
                    Arc::clone(&idx.load_status),
                    Arc::clone(&idx.load_started_at),
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

    let path = PathBuf::from(&req.path);
    if !path.exists() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": format!("File not found: {}", req.path)})),
        ).into_response();
    }

    // Reset progress and record start time
    load_progress.store(0, Ordering::Release);
    *load_started_at.lock() = Some(Instant::now());
    *load_status.lock() = LoadStatus::Loading {
        records_loaded: 0,
        elapsed_secs: 0.0,
    };

    let limit = req.limit;
    let threads = req.threads;
    let chunk_size = req.chunk_size;
    let docstore_batch_size = req.docstore_batch_size;
    let progress = Arc::clone(&load_progress);
    let status = Arc::clone(&load_status);

    // Spawn blocking loading task
    tokio::task::spawn_blocking(move || {
        // Enter loading mode
        engine.enter_loading_mode();

        match loader::load_ndjson(&engine, &schema, &path, limit, threads, chunk_size, docstore_batch_size, progress.clone()) {
            Ok(stats) => {
                // Exit loading mode (publishes staging and restarts maintenance)
                engine.exit_loading_mode();

                let alive = engine.alive_count();
                eprintln!("Load complete: {} records alive", alive);

                // Transition to Saving — bitmap snapshot save can be slow
                *status.lock() = LoadStatus::Saving {
                    records_loaded: stats.records_loaded,
                    elapsed_secs: stats.elapsed.as_secs_f64(),
                };

                // Save bitmap snapshot for fast restart (may take a while on NTFS)
                let snap_start = Instant::now();
                if let Err(e) = engine.save_snapshot() {
                    eprintln!("Warning: failed to save bitmap snapshot: {e}");
                } else {
                    eprintln!("Bitmap snapshot saved in {:.1}s", snap_start.elapsed().as_secs_f64());
                }

                // Only report complete after snapshot is fully saved
                *status.lock() = LoadStatus::Complete {
                    records_loaded: stats.records_loaded,
                    elapsed_secs: stats.elapsed.as_secs_f64(),
                };
            }
            Err(e) => {
                engine.exit_loading_mode();
                *status.lock() = LoadStatus::Error {
                    message: e.to_string(),
                };
            }
        }
    });

    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({"status": "loading"})),
    ).into_response()
}

async fn handle_load_status(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
) -> impl IntoResponse {
    let guard = state.index.lock();
    match guard.as_ref() {
        Some(idx) if idx.definition.name == name => {
            let status = idx.load_status.lock().clone();
            // If loading or saving, update elapsed from live timer
            let status = match status {
                LoadStatus::Loading { .. } => {
                    let loaded = idx.load_progress.load(Ordering::Acquire);
                    let elapsed = idx.load_started_at.lock()
                        .map(|t| t.elapsed().as_secs_f64())
                        .unwrap_or(0.0);
                    LoadStatus::Loading {
                        records_loaded: loaded,
                        elapsed_secs: elapsed,
                    }
                }
                LoadStatus::Saving { records_loaded, .. } => {
                    let elapsed = idx.load_started_at.lock()
                        .map(|t| t.elapsed().as_secs_f64())
                        .unwrap_or(0.0);
                    LoadStatus::Saving {
                        records_loaded,
                        elapsed_secs: elapsed,
                    }
                }
                other => other,
            };
            Json(serde_json::to_value(&status).unwrap()).into_response()
        }
        _ => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("Index '{}' not found", name)})),
        ).into_response(),
    }
}

// ---------------------------------------------------------------------------
// Handlers: Query & documents
// ---------------------------------------------------------------------------

/// Query request with optional field selection for document retrieval.
#[derive(Deserialize)]
struct QueryRequest {
    #[serde(flatten)]
    query: BitdexQuery,
    /// `true` → all fields, `["field1","field2"]` → selected fields, `false`/omitted → IDs only.
    #[serde(default)]
    include_docs: IncludeDocs,
}

async fn handle_query(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
    Json(req): Json<QueryRequest>,
) -> impl IntoResponse {
    let (engine, schema) = {
        let guard = state.index.lock();
        match guard.as_ref() {
            Some(idx) if idx.definition.name == name => (
                Arc::clone(&idx.engine),
                idx.definition.data_schema.clone(),
            ),
            _ => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error": format!("Index '{}' not found", name)})),
                ).into_response();
            }
        }
    };

    let start = Instant::now();
    let m = &state.metrics;
    match engine.execute_query(&req.query) {
        Ok(result) => {
            let elapsed = start.elapsed();
            let elapsed_us = elapsed.as_micros() as u64;
            m.query_total.with_label_values(&[&name]).inc();
            m.query_duration_seconds
                .with_label_values(&[&name])
                .observe(elapsed.as_secs_f64());
            let cursor = result.cursor.map(|c| serde_json::to_value(c).unwrap());

            let documents = if !req.include_docs.is_none() {
                let mut docs = Vec::with_capacity(result.ids.len());
                for &id in &result.ids {
                    let doc = engine.get_document(id as u32);
                    docs.push(match doc {
                        Ok(Some(stored)) => {
                            format_document(&stored, &schema, &req.include_docs)
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
    let (engine, schema) = {
        let guard = state.index.lock();
        match guard.as_ref() {
            Some(idx) if idx.definition.name == name => (
                Arc::clone(&idx.engine),
                idx.definition.data_schema.clone(),
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
        Ok(Some(doc)) => Json(format_document(&doc, &schema, &req.fields)).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "not found"}))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))).into_response(),
    }
}

async fn handle_documents_batch(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
    Json(req): Json<DocumentBatchRequest>,
) -> impl IntoResponse {
    let (engine, schema) = {
        let guard = state.index.lock();
        match guard.as_ref() {
            Some(idx) if idx.definition.name == name => (
                Arc::clone(&idx.engine),
                idx.definition.data_schema.clone(),
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
            Ok(Some(doc)) => docs.push(format_document(&doc, &schema, &req.fields)),
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
    let (engine, schema) = {
        let guard = state.index.lock();
        match guard.as_ref() {
            Some(idx) if idx.definition.name == name => (
                Arc::clone(&idx.engine),
                idx.definition.data_schema.clone(),
            ),
            _ => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error": format!("Index '{}' not found", name)})),
                ).into_response();
            }
        }
    };

    let mut upserted = 0u64;
    let mut errors: Vec<String> = Vec::new();

    for (i, doc_json) in req.documents.iter().enumerate() {
        match loader::json_to_document(doc_json, &schema) {
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
    Json(serde_json::json!({
        "alive_count": engine.alive_count(),
        "slot_count": engine.slot_counter(),
        "unified_cache_entries": uc.entries,
        "unified_cache_hits": uc.hits,
        "unified_cache_misses": uc.misses,
        "unified_cache_bytes": uc.memory_bytes,
        "unified_cache_meta_entries": uc.meta_index_entries,
        "unified_cache_meta_bytes": uc.meta_index_bytes,
        "unified_cache_entry_details": entries,
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
    Json(serde_json::json!({"cleared": true})).into_response()
}

// ---------------------------------------------------------------------------
// Handlers: Rebuild
// ---------------------------------------------------------------------------

async fn handle_rebuild(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
    Json(req): Json<RebuildRequest>,
) -> impl IntoResponse {
    let (engine, config, load_progress, load_status, load_started_at) = {
        let guard = state.index.lock();
        match guard.as_ref() {
            Some(idx) if idx.definition.name == name => {
                // Check if already loading, saving, or rebuilding
                {
                    let status = idx.load_status.lock();
                    if matches!(*status, LoadStatus::Loading { .. } | LoadStatus::Saving { .. }) {
                        return (
                            StatusCode::CONFLICT,
                            Json(serde_json::json!({"error": "Already loading, saving, or rebuilding"})),
                        ).into_response();
                    }
                }
                (
                    Arc::clone(&idx.engine),
                    idx.definition.config.clone(),
                    Arc::clone(&idx.load_progress),
                    Arc::clone(&idx.load_status),
                    Arc::clone(&idx.load_started_at),
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

    // Reset progress and record start time
    load_progress.store(0, Ordering::Release);
    *load_started_at.lock() = Some(Instant::now());
    *load_status.lock() = LoadStatus::Loading {
        records_loaded: 0,
        elapsed_secs: 0.0,
    };

    let sort_fields = req.sort_fields;
    let filter_fields = req.filter_fields;
    let save = req.save_snapshot;
    let progress = Arc::clone(&load_progress);
    let status = Arc::clone(&load_status);

    // Spawn blocking rebuild task
    tokio::task::spawn_blocking(move || {
        match engine.rebuild_fields_from_docstore(sort_fields, filter_fields, progress.clone()) {
            Ok((slots, fields)) => {
                let elapsed = {
                    let started = load_started_at.lock();
                    started.map(|t| t.elapsed().as_secs_f64()).unwrap_or(0.0)
                };

                if save {
                    *status.lock() = LoadStatus::Saving {
                        records_loaded: slots,
                        elapsed_secs: elapsed,
                    };

                    let snap_start = Instant::now();
                    if let Err(e) = engine.save_snapshot() {
                        eprintln!("rebuild: failed to save bitmap snapshot: {e}");
                    } else {
                        eprintln!("rebuild: bitmap snapshot saved in {:.1}s", snap_start.elapsed().as_secs_f64());
                    }
                }

                let final_elapsed = {
                    let started = load_started_at.lock();
                    started.map(|t| t.elapsed().as_secs_f64()).unwrap_or(0.0)
                };
                *status.lock() = LoadStatus::Complete {
                    records_loaded: slots,
                    elapsed_secs: final_elapsed,
                };

                eprintln!("rebuild: done — {} slots, {} fields in {:.1}s",
                    slots, fields.len(), final_elapsed);
            }
            Err(e) => {
                *status.lock() = LoadStatus::Error {
                    message: format!("Rebuild failed: {}", e),
                };
            }
        }
    });

    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({"status": "rebuilding"})),
    ).into_response()
}

// ---------------------------------------------------------------------------
// Handlers: Utility
// ---------------------------------------------------------------------------

async fn handle_health() -> impl IntoResponse {
    (StatusCode::OK, "ok")
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
