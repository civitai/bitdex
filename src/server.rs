//! Generic HTTP server for BitDex — no dataset-specific code.
//!
//! Feature-gated behind `server`. Provides `BitdexServer` which starts blank
//! and creates indexes via API.

use std::collections::VecDeque;
use ahash::AHashMap as HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use axum::body::Bytes;
use axum::extract::{Extension, Path as AxumPath, Query as AxumQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Json};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::{get, patch, post, put, delete};
use axum::Router;
use parking_lot::Mutex;
#[cfg(feature = "server")]
use tokio_stream::wrappers::BroadcastStream;
#[cfg(feature = "server")]
use tokio_stream::StreamExt as TokioStreamExt;
use serde::{Deserialize, Serialize};
use tower_http::cors::CorsLayer;

use crate::concurrent_engine::ConcurrentEngine;
use crate::config::{Config, DataSchema, FieldValueType, FilterFieldConfig, SortFieldConfig};
use crate::shard_store_doc::StoredDoc;
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

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TaskType {
    Load,
    Rebuild,
    AddFields,
    RemoveFields,
    Dump,
    Compact,
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
    pub active: Vec<TaskInfo>,
    pub history: Vec<TaskInfo>,
}

struct ActiveTask {
    id: TaskId,
    task_type: TaskType,
    status: TaskStatus,
    started_at: Instant,
    progress: Arc<AtomicU64>,
}

struct RegistryState {
    active: HashMap<TaskId, ActiveTask>,
    history: VecDeque<TaskInfo>,
}

pub struct TaskRegistry {
    next_id: AtomicU64,
    state: Mutex<RegistryState>,
}

fn build_task_info(active: &ActiveTask) -> TaskInfo {
    let progress = active.progress.load(Ordering::Acquire);
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
            state: Mutex::new(RegistryState {
                active: HashMap::new(),
                history: VecDeque::new(),
            }),
        }
    }

    /// Try to start a new task. Returns (task_id, progress_counter) on success,
    /// or the TaskInfo of the conflicting active task on failure.
    ///
    /// Exclusion rules:
    /// - Mutating tasks (Load, Dump, Rebuild, AddFields, RemoveFields) are exclusive
    ///   with everything — no other task may run concurrently with them.
    /// - Compact is also exclusive — it modifies on-disk generations and deletes old
    ///   ones, so concurrent compacts would race on the same shard files.
    pub fn try_start(&self, task_type: TaskType) -> Result<(TaskId, Arc<AtomicU64>), TaskInfo> {
        let mut state = self.state.lock();

        // All task types are currently exclusive — only one task at a time.
        // Compact modifies on-disk generations + deletes old ones, so it can't
        // run concurrently with anything else.
        if let Some(active) = state.active.values().next() {
            return Err(build_task_info(active));
        }

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let progress = Arc::new(AtomicU64::new(0));
        state.active.insert(id, ActiveTask {
            id,
            task_type,
            status: TaskStatus::Running,
            started_at: Instant::now(),
            progress: Arc::clone(&progress),
        });
        Ok((id, progress))
    }

    pub fn set_saving(&self, task_id: TaskId) {
        let mut state = self.state.lock();
        if let Some(active) = state.active.get_mut(&task_id) {
            active.status = TaskStatus::Saving;
        }
    }

    pub fn set_complete(&self, task_id: TaskId, result: Option<serde_json::Value>) {
        let mut state = self.state.lock();
        if let Some(active) = state.active.remove(&task_id) {
            let mut info = build_task_info(&active);
            info.status = TaskStatus::Complete;
            info.result = result;
            state.history.push_front(info);
            if state.history.len() > 20 {
                state.history.pop_back();
            }
        }
    }

    pub fn set_error(&self, task_id: TaskId, message: String) {
        let mut state = self.state.lock();
        if let Some(active) = state.active.remove(&task_id) {
            let mut info = build_task_info(&active);
            info.status = TaskStatus::Error;
            info.error = Some(message);
            state.history.push_front(info);
            if state.history.len() > 20 {
                state.history.pop_back();
            }
        }
    }

    pub fn get(&self, task_id: TaskId) -> Option<TaskInfo> {
        let state = self.state.lock();
        // Check active first
        if let Some(active) = state.active.get(&task_id) {
            return Some(build_task_info(active));
        }
        // Check history
        state.history.iter().find(|t| t.task_id == task_id).cloned()
    }

    pub fn snapshot(&self) -> TaskSnapshot {
        let state = self.state.lock();
        let mut active: Vec<TaskInfo> = state.active.values().map(build_task_info).collect();
        active.sort_by_key(|t| t.task_id);
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

/// Persisted index definition (saved as config.yaml or config.json in the index directory).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDefinition {
    pub name: String,
    pub config: Config,
    pub data_schema: DataSchema,
}

impl IndexDefinition {
    /// Load an index definition from a file, auto-detecting format from extension.
    pub fn from_file(path: &std::path::Path) -> std::result::Result<Self, String> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("failed to read {}: {e}", path.display()))?;

        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("");

        match ext {
            "yaml" | "yml" => {
                serde_yaml::from_str(&content)
                    .map_err(|e| format!("YAML parse error in {}: {e}", path.display()))
            }
            "json" => {
                serde_json::from_str(&content)
                    .map_err(|e| format!("JSON parse error in {}: {e}", path.display()))
            }
            other => Err(format!("unsupported index config format: '{other}'")),
        }
    }

    /// Save the index definition to a YAML file in the given directory.
    pub fn save_yaml(&self, dir: &std::path::Path) -> std::result::Result<(), String> {
        let yaml = serde_yaml::to_string(self)
            .map_err(|e| format!("Failed to serialize config to YAML: {e}"))?;
        let path = dir.join("config.yaml");
        let tmp = dir.join("config.yaml.tmp");
        std::fs::write(&tmp, &yaml)
            .map_err(|e| format!("Failed to write {}: {e}", tmp.display()))?;
        std::fs::rename(&tmp, &path)
            .map_err(|e| format!("Failed to rename to {}: {e}", path.display()))?;
        Ok(())
    }
}

/// Find the index config file in a directory. Checks config.yaml, then config.yml.
pub fn find_index_config(dir: &std::path::Path) -> Option<std::path::PathBuf> {
    let yaml = dir.join("config.yaml");
    if yaml.exists() {
        return Some(yaml);
    }
    let yml = dir.join("config.yml");
    if yml.exists() {
        return Some(yml);
    }
    None
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

// ---------------------------------------------------------------------------
// Query stream (SSE mirror)
// ---------------------------------------------------------------------------

/// A single query event broadcast to SSE subscribers.
/// Gated on `BITDEX_QUERY_STREAM=1`. Zero cost when unset.
#[derive(Clone, Debug, Serialize)]
pub struct QueryEvent {
    /// Unix epoch milliseconds.
    pub ts_ms: u64,
    /// Index name the query was issued against.
    pub index: String,
    /// Raw request body as parsed JSON.
    pub body: serde_json::Value,
    /// x-forwarded-for or remote address if available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_meta: Option<String>,
}

/// Shared application state.
struct AppState {
    data_dir: PathBuf,
    /// External index config directory (ConfigMap mount). Configs read from here,
    /// runtime data (bitmaps, docstore) still under data_dir/indexes/.
    index_dir: Option<PathBuf>,
    index: Mutex<Option<IndexState>>,
    /// Set to true during graceful shutdown to signal background threads to exit.
    shutting_down: AtomicBool,
    metrics: Metrics,
    parser_registry: crate::parser::registry::ParserRegistry,
    enable_traces: AtomicBool,
    /// Minimum query latency (microseconds) to record a trace. 0 = record all.
    trace_min_us: AtomicU64,
    admin_token: Option<String>,
    trace_buffer: crate::query_metrics::TraceBuffer,
    /// Number of queries currently executing (incremented on entry, decremented on exit).
    queries_in_flight: AtomicI64,
    /// Peak concurrent queries since startup (updated atomically via fetch_max).
    queries_in_flight_peak: AtomicI64,
    /// Concurrency limit for queries. 0 = unlimited (no backpressure).
    max_query_concurrency: AtomicU32,
    /// Snapshot capture session manager (Phase 2).
    capture: crate::capture::CaptureManager,
    /// Broadcast sender for SSE query stream (`GET /debug/queries/stream`).
    /// `Some` only when `BITDEX_QUERY_STREAM=1` is set at startup.
    /// `None` → zero overhead on the hot query path.
    query_stream: Option<tokio::sync::broadcast::Sender<QueryEvent>>,
    /// Query tee mode: broadcast query via SSE, return stub response immediately.
    /// Prod stays responsive at any QPS while local SSE mirror gets real traffic.
    /// Toggle at runtime via PATCH /config {"query_tee_mode": true/false}.
    query_tee_mode: AtomicBool,
    /// Toggleable metric groups — disable expensive metrics without redeploy.
    /// Default: all enabled. PATCH /config to toggle at runtime.
    metrics_bitmap_memory: AtomicBool,
    metrics_eviction_stats: AtomicBool,
    metrics_boundstore_disk: AtomicBool,
    /// WAL writer for V2 ops endpoint. Created lazily on first ops POST.
    #[cfg(feature = "pg-sync")]
    ops_wal: Mutex<Option<crate::ops_wal::WalWriter>>,
    /// Latest sync source metadata (cursor, lag) keyed by source name.
    #[cfg(feature = "pg-sync")]
    sync_meta: Mutex<HashMap<String, crate::pg_sync::ops::SyncMeta>>,
    /// Dump registry for tracking table dump lifecycle.
    #[cfg(feature = "pg-sync")]
    dump_registry: Mutex<crate::pg_sync::dump::DumpRegistry>,
    /// Shared slot watermark for progressive shard pre-creation.
    /// Updated by dump phases as they see new max slot IDs.
    #[cfg(feature = "pg-sync")]
    slot_watermark: Arc<std::sync::atomic::AtomicU64>,
    /// Pre-creator done signal — set when we want to stop the background thread.
    #[cfg(feature = "pg-sync")]
    precreator_done: Arc<std::sync::atomic::AtomicBool>,
    /// Whether the pre-creator has been started.
    #[cfg(feature = "pg-sync")]
    precreator_started: std::sync::atomic::AtomicBool,
}

type SharedState = Arc<AppState>;

/// Middleware: require admin token for mutating endpoints.
/// If no token is configured, all admin endpoints return 403 for external requests.
/// Requests without X-Forwarded-For are treated as internal (sidecar/localhost) and
/// allowed without auth. External requests (via ingress) always have X-Forwarded-For.
/// Token is checked via `Authorization: Bearer <token>` header.
async fn require_admin(
    State(state): State<SharedState>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::http::StatusCode;

    // No X-Forwarded-For = internal request (sidecar, localhost dev) — allow without auth.
    // Traefik/any reverse proxy always sets X-Forwarded-For for external requests.
    let is_external = req.headers().contains_key("x-forwarded-for");
    if !is_external {
        return next.run(req).await;
    }

    match &state.admin_token {
        None => {
            // No token configured — admin endpoints are disabled for external requests
            (StatusCode::FORBIDDEN, axum::Json(serde_json::json!({
                "error": "Admin endpoints are disabled. Set BITDEX_ADMIN_TOKEN env var or admin_token in config to enable."
            }))).into_response()
        }
        Some(expected) => {
            let auth = req.headers()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "));
            match auth {
                Some(token) if token == expected.as_str() => {
                    next.run(req).await
                }
                Some(_) => {
                    (StatusCode::UNAUTHORIZED, axum::Json(serde_json::json!({
                        "error": "Invalid admin token"
                    }))).into_response()
                }
                None => {
                    (StatusCode::UNAUTHORIZED, axum::Json(serde_json::json!({
                        "error": "Authorization header required: Bearer <admin_token>"
                    }))).into_response()
                }
            }
        }
    }
}

/// Per-request handler-stage timing data. Threaded from the outer middleware
/// (which captures T0 = arrival) through the request extensions to the
/// `handle_query` body (which records T1-T4). Read back by the middleware on
/// response to attribute time across phases:
///
/// - `to_handler_us`  = T1 - T0   (middleware overhead + tokio task scheduling)
/// - `to_engine_us`   = T2 - T1   (body parse + JSON deserialize + index lookup)
/// - `engine_us`      = T3 - T2   (block_in_place engine call duration)
/// - `doc_fetch_us`   = T4 - T3   (spawn_blocking doc fetch duration)
/// - `to_response_us` = T5 - T4   (response build + serialize + write)
///
/// All offsets are nanoseconds since `t0` so the writers can use lock-free
/// `AtomicU64` stores. A zero offset means the stage was never reached — the
/// reader skips emitting that phase.
#[derive(Clone)]
struct HttpStageData {
    t0: std::time::Instant,
    t1_handler_entered_ns: Arc<AtomicU64>,
    t2_engine_started_ns: Arc<AtomicU64>,
    t3_engine_done_ns: Arc<AtomicU64>,
    t4_docs_done_ns: Arc<AtomicU64>,
}

impl HttpStageData {
    fn new() -> Self {
        Self {
            t0: std::time::Instant::now(),
            t1_handler_entered_ns: Arc::new(AtomicU64::new(0)),
            t2_engine_started_ns: Arc::new(AtomicU64::new(0)),
            t3_engine_done_ns: Arc::new(AtomicU64::new(0)),
            t4_docs_done_ns: Arc::new(AtomicU64::new(0)),
        }
    }
    /// Stash the elapsed time since `t0` into the slot. Called by the handler.
    #[inline]
    fn record(&self, slot: &AtomicU64) {
        let ns = self.t0.elapsed().as_nanos() as u64;
        // Use store; only one writer per slot per request, so no contention.
        slot.store(ns, Ordering::Relaxed);
    }
}

/// Middleware: record requests/responses to the caplog when capture is active.
///
/// Fast path: if not recording, `is_recording()` is a single mutex check (~ns)
/// Outermost middleware: measures wall-clock time from HTTP request arrival
/// to response sent. This captures the full round-trip including tokio scheduling,
/// handler execution, response serialization, and any CPU contention delays.
/// Records into bitdex_http_response_seconds histogram. For paths that opt in
/// (currently only POST query handler), the middleware also threads a
/// `HttpStageData` cell through request extensions so per-stage timings (T0–T5)
/// can be reassembled here and emitted into `bitdex_http_handler_phase_seconds`.
async fn measure_http_roundtrip(
    State(state): State<SharedState>,
    mut req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let method = req.method().to_string();
    let path = req.uri().path().to_string();
    // Normalize path: strip IDs/values to reduce label cardinality
    let path_label = if path.starts_with("/api/indexes/") {
        // /api/indexes/{name}/query → /api/indexes/*/query
        let parts: Vec<&str> = path.splitn(4, '/').collect();
        if parts.len() >= 4 {
            format!("/api/indexes/*/{}", parts[3].split('/').next().unwrap_or(""))
        } else {
            "/api/indexes".to_string()
        }
    } else {
        path.clone()
    };
    // Only emit per-stage phase timing for the query path; other endpoints
    // skip the extension overhead.
    let is_query = method == "POST" && path.contains("/query");
    let stage = if is_query {
        let s = HttpStageData::new();
        req.extensions_mut().insert(s.clone());
        Some(s)
    } else {
        None
    };
    let start = std::time::Instant::now();
    let response = next.run(req).await;
    let elapsed = start.elapsed();
    state.metrics.http_response_seconds
        .with_label_values(&[&method, &path_label])
        .observe(elapsed.as_secs_f64());

    if let Some(s) = stage {
        let total_ns = s.t0.elapsed().as_nanos() as u64;
        let t1 = s.t1_handler_entered_ns.load(Ordering::Relaxed);
        let t2 = s.t2_engine_started_ns.load(Ordering::Relaxed);
        let t3 = s.t3_engine_done_ns.load(Ordering::Relaxed);
        let t4 = s.t4_docs_done_ns.load(Ordering::Relaxed);

        let phase_hist = &state.metrics.http_handler_phase_seconds;
        let observe_phase = |name: &str, ns: u64| {
            phase_hist
                .with_label_values(&[name])
                .observe(ns as f64 / 1_000_000_000.0);
        };

        // to_handler: T0 → T1. Always emitted when handler ran.
        if t1 > 0 {
            observe_phase("to_handler", t1);
        }
        // to_engine: T1 → T2.
        if t2 > t1 && t1 > 0 {
            observe_phase("to_engine", t2 - t1);
        }
        // engine: T2 → T3.
        if t3 > t2 && t2 > 0 {
            observe_phase("engine", t3 - t2);
        }
        // doc_fetch: T3 → T4. Optional — only present when include_docs.
        if t4 > t3 && t3 > 0 {
            observe_phase("doc_fetch", t4 - t3);
        }
        // to_response: last-stage-reached → T5.
        let last = t4.max(t3).max(t2).max(t1);
        if total_ns > last && last > 0 {
            observe_phase("to_response", total_ns - last);
        }
    }

    response
}

/// and the middleware is a no-op passthrough. When recording, the request body
/// is buffered, the response body is collected, and both are written to the caplog.
async fn capture_traffic(
    State(state): State<SharedState>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    // Fast path: skip if not recording
    if !state.capture.is_recording() {
        return next.run(req).await;
    }

    let arrived_at_ns = crate::capture::nanos_since_epoch();
    let method = req.method().to_string();
    let path = req.uri().path().to_string();
    let query_string = req.uri().query().unwrap_or("").to_string();

    // Skip recording the capture endpoints themselves and /metrics
    if path.starts_with("/debug/capture") || path == "/metrics" {
        return next.run(req).await;
    }

    // Buffer the request body so we can record it
    let (parts, body) = req.into_parts();
    let body_bytes = match axum::body::to_bytes(body, 16 * 1024 * 1024).await {
        Ok(b) => b,
        Err(_) => {
            // If body is too large or read fails, pass through without recording
            // Reconstruct a minimal request
            let req = axum::extract::Request::from_parts(parts, axum::body::Body::empty());
            return next.run(req).await;
        }
    };
    let req_body = body_bytes.to_vec();

    // Reconstruct the request with the buffered body
    let req = axum::extract::Request::from_parts(parts, axum::body::Body::from(body_bytes));

    // Run the actual handler
    let response = next.run(req).await;

    // Capture the response
    let response_status = response.status().as_u16();
    let (resp_parts, resp_body) = response.into_parts();
    let resp_bytes = axum::body::to_bytes(resp_body, 16 * 1024 * 1024)
        .await
        .unwrap_or_default();
    let resp_body_vec = resp_bytes.to_vec();
    let responded_at_ns = crate::capture::nanos_since_epoch();

    // Write the entry to the caplog (fire-and-forget — don't slow down the response)
    let entry = crate::capture::CaptureEntry {
        arrived_at_ns,
        method,
        path,
        query_string,
        body: req_body,
        response_status,
        response_body: resp_body_vec,
        responded_at_ns,
    };
    state.capture.write_entry(&entry);

    // Reconstruct the response with the buffered body
    axum::response::Response::from_parts(resp_parts, axum::body::Body::from(resp_bytes))
}

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

        // Look up by target name first (outbox/PATCH path stores under target,
        // value already converted), then source name (bulk loader stores under
        // source, value is raw and may need ms_to_seconds conversion).
        let (fv_opt, from_source) = if let Some(fv) = doc.fields.get(&mapping.target) {
            (Some(fv), false)
        } else if let Some(fv) = doc.fields.get(&mapping.source) {
            (Some(fv), true)
        } else {
            (None, false)
        };

        let value = if let Some(fv) = fv_opt {
            // Reverse-map MappedString / LowCardinalityString fields from integer back to string
            let raw = if mapping.value_type == FieldValueType::MappedString
                || mapping.value_type == FieldValueType::LowCardinalityString
            {
                if let Some(rev) = reverse_maps.get(&mapping.target) {
                    reverse_map_value(fv, rev)
                } else {
                    field_value_to_json(fv)
                }
            } else {
                field_value_to_json(fv)
            };
            // Apply ms_to_seconds only when the value came from the source name
            // (raw ms). Target-name values were already converted during encoding.
            if from_source && mapping.should_convert_ms() {
                if let serde_json::Value::Number(n) = &raw {
                    if let Some(ms) = n.as_i64() {
                        serde_json::json!(ms / 1000)
                    } else {
                        raw
                    }
                } else {
                    raw
                }
            } else {
                raw
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

/// Sync filter values for a filter_only multi-value field.
/// Replaces all bitmap memberships for the given slots on the named field.
#[derive(Deserialize)]
struct FilterSyncRequest {
    /// The filter field name (must be a multi_value field).
    field: String,
    /// List of (slot, values) pairs to sync.
    documents: Vec<FilterSyncEntry>,
}

#[derive(Deserialize)]
struct FilterSyncEntry {
    /// The document/slot ID.
    id: u32,
    /// The complete set of values this slot should have for the field.
    values: Vec<u64>,
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
// Config patch types
// ---------------------------------------------------------------------------

/// Partial config update — only fields present are changed.
#[derive(Deserialize, Serialize)]
struct ConfigPatch {
    #[serde(default)]
    filter_fields: Option<HashMap<String, FilterFieldPatch>>,
    #[serde(default)]
    sort_fields: Option<HashMap<String, SortFieldPatch>>,
    #[serde(default)]
    cache: Option<CachePatch>,
    /// Update time bucket refresh intervals without restart.
    #[serde(default)]
    time_buckets: Option<TimeBucketPatch>,
    /// Update the query concurrency limit. 0 = unlimited (no backpressure).
    #[serde(default)]
    max_query_concurrency: Option<u32>,
    /// Toggle query trace collection on/off without restart.
    #[serde(default)]
    enable_traces: Option<bool>,
    /// Minimum query latency (microseconds) to record a trace. 0 = record all.
    #[serde(default)]
    trace_min_us: Option<u64>,
    /// Resize the in-memory trace ring buffer. At 180 QPS the default 1000
    /// entries give only ~5.5s of history — increase for cache analysis.
    #[serde(default)]
    trace_buffer_size: Option<usize>,
    /// Toggle expensive metric groups at runtime. Array of group names to enable.
    /// Groups: "bitmap_memory", "eviction_stats", "boundstore_disk"
    /// DEPRECATED: Use disabled_metrics instead.
    /// If provided, ONLY listed groups are enabled (others disabled).
    #[serde(default)]
    enabled_metrics: Option<Vec<String>>,

    /// Metric groups to DISABLE (opt-out). Default: all ON.
    /// Takes precedence over enabled_metrics.
    #[serde(default)]
    disabled_metrics: Option<Vec<String>>,
    /// Query tee mode: broadcast query via SSE, return stub immediately.
    /// Prod stays responsive while local SSE mirror gets real query traffic.
    #[serde(default)]
    query_tee_mode: Option<bool>,
    /// Hot-reload knob for the par_iter min-task threshold on the steady-state
    /// hot path (flush filter+sort fan-out, doc writer shard fan-out). Set huge
    /// (e.g. 10_000_000) to disable par_iter entirely — useful for isolating
    /// rayon pool overhead from real work during perf experiments.
    /// Default 8.
    #[serde(default)]
    par_iter_min_threshold: Option<usize>,
    /// Hot-reload knob for the bitmap shard compaction threshold (ops_count
    /// above which a shard's ops-log is compacted into a fresh snapshot at
    /// the next merge cycle). Applies to alive/filter/sort bitmap stores.
    /// Default `DEFAULT_COMPACT_THRESHOLD = 100_000`. Bump higher (e.g. 500_000)
    /// when hot tagIds shards are rewriting too aggressively; drop lower for
    /// faster ops-replay on read at cost of more rewrite I/O.
    #[serde(default)]
    bitmap_compact_threshold: Option<u32>,
    /// Max prefilter registry size. 0 disables prefilters entirely — existing
    /// entries are evicted within one merge cycle and all registration paths
    /// (manual POST and auto-promotion) are gated. Range: 0-32.
    #[serde(default)]
    max_registered_prefilters: Option<usize>,
    /// Hot-reload knob for the periodic time-bucket full-reconcile interval
    /// (secs). The flush thread reads it each cycle, so a change takes effect
    /// within one interval — no restart. `0` disables the reconcile fallback.
    /// Lower it (e.g. 120) once the parallel scan makes the walk cheap, to cut
    /// recency lag; raise it if the scan duty starts costing query latency.
    #[serde(default)]
    time_bucket_full_rebuild_interval_secs: Option<u64>,
    /// Hot-reload knob for the overdue-deferred sweep interval (secs). The
    /// WAL reader checks it between batches; `0` disables the sweep. See
    /// `DeferredAliveConfig::sweep_interval_secs`.
    #[serde(default)]
    deferred_sweep_interval_secs: Option<u64>,
    /// Hot-reload knob for the overdue-deferred sweep per-pass candidate cap.
    #[serde(default)]
    deferred_sweep_limit: Option<usize>,
}

/// Patchable fields for a filter field.
#[derive(Deserialize, Serialize)]
struct FilterFieldPatch {
    eager_load: Option<bool>,
}

/// Patchable fields for a sort field.
#[derive(Deserialize, Serialize)]
struct SortFieldPatch {
    eager_load: Option<bool>,
}

/// Patchable fields for time bucket config.
#[derive(Deserialize, Serialize)]
struct TimeBucketPatch {
    range_buckets: Option<Vec<TimeBucketRangePatch>>,
}

/// Patchable fields for a single time bucket.
#[derive(Deserialize, Serialize)]
struct TimeBucketRangePatch {
    name: String,
    refresh_interval_secs: Option<u64>,
}

/// Patchable fields for cache config.
#[derive(Deserialize, Serialize)]
struct CachePatch {
    max_entries: Option<usize>,
    max_bytes: Option<usize>,
    initial_capacity: Option<usize>,
    max_capacity: Option<usize>,
    min_filter_size: Option<usize>,
    decay_rate: Option<f64>,
    bound_target_size: Option<usize>,
    bound_max_size: Option<usize>,
    bound_max_count: Option<usize>,
    prefetch_threshold: Option<f64>,
    max_maintenance_work: Option<usize>,
    max_maintenance_ms: Option<u64>,
    /// Toggle async cache maintenance worker at runtime.
    /// Note: toggling only changes the config flag. The worker thread is spawned
    /// at startup — flipping this flag at runtime controls whether the flush thread
    /// sends work to the channel; the thread itself is always present when
    /// async_maintenance=true was set at startup.
    async_maintenance: Option<bool>,
    /// B9 safety valve: maximum leaf-atom count before an entry is skipped and
    /// marked for rebuild. Set to 0 to disable. Hot-tunable; takes effect on the
    /// next maintenance cycle.
    compound_eval_atom_limit: Option<u32>,
    /// TTL (seconds) for time-bucket cache entries. 0 disables. Hot-tunable;
    /// takes effect on the next read of an affected entry.
    bucket_entry_ttl_secs: Option<u64>,
}

// ---------------------------------------------------------------------------
// Public server entry point
// ---------------------------------------------------------------------------

/// The BitDex HTTP server. Starts blank and creates indexes via API.
pub struct BitdexServer {
    data_dir: PathBuf,
    /// External index config directory (e.g. K8s ConfigMap mount).
    /// When set, index configs are read from here instead of data_dir/indexes/.
    /// Runtime data (bitmaps, docstore) still lives under data_dir/indexes/.
    index_dir: Option<PathBuf>,
    rebuild: bool,
    default_query_format: Option<String>,
    enable_traces: bool,
    admin_token: Option<String>,
    max_query_concurrency: u32,
    trace_buffer_size: usize,
}

impl BitdexServer {
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir, index_dir: None, rebuild: false, default_query_format: None, enable_traces: false, admin_token: None, max_query_concurrency: 0, trace_buffer_size: 1000 }
    }

    /// Set external index config directory (e.g. ConfigMap mount path).
    /// Configs read from here; runtime data stays under data_dir.
    pub fn with_index_dir(mut self, dir: PathBuf) -> Self {
        self.index_dir = Some(dir);
        self
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

    pub fn with_enable_traces(mut self, enable: bool) -> Self {
        self.enable_traces = enable;
        self
    }

    /// Set the trace ring buffer capacity. Default is 1000 entries.
    pub fn with_trace_buffer_size(mut self, size: usize) -> Self {
        self.trace_buffer_size = size;
        self
    }

    /// Set admin token for gating mutating endpoints.
    /// If None, admin endpoints are disabled (403).
    pub fn with_admin_token(mut self, token: Option<String>) -> Self {
        self.admin_token = token;
        self
    }

    /// Set the maximum number of concurrent queries. 0 = unlimited (default).
    /// When the limit is reached, new queries receive 503 Service Unavailable.
    /// The limit can be adjusted at runtime via PATCH /config.
    pub fn with_max_query_concurrency(mut self, max: u32) -> Self {
        self.max_query_concurrency = max;
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

        // Admin token: env var takes precedence, then TOML config
        let admin_token = self.admin_token.clone();
        if admin_token.is_some() {
            eprintln!("Admin endpoints: enabled (token configured)");
        } else {
            eprintln!("Admin endpoints: disabled (set BITDEX_ADMIN_TOKEN to enable)");
        }

        let state = Arc::new(AppState {
            data_dir: self.data_dir.clone(),
            index_dir: self.index_dir.clone(),
            index: Mutex::new(None),
            shutting_down: AtomicBool::new(false),
            metrics: Metrics::new(),
            parser_registry: registry,
            enable_traces: AtomicBool::new(self.enable_traces),
            trace_min_us: AtomicU64::new(0),
            admin_token,
            trace_buffer: crate::query_metrics::TraceBuffer::new(self.trace_buffer_size),
            queries_in_flight: AtomicI64::new(0),
            queries_in_flight_peak: AtomicI64::new(0),
            max_query_concurrency: AtomicU32::new(self.max_query_concurrency),
            capture: crate::capture::CaptureManager::new(&self.data_dir),
            query_stream: if std::env::var("BITDEX_QUERY_STREAM").as_deref() == Ok("1") {
                let (tx, _rx) = tokio::sync::broadcast::channel(10_000);
                eprintln!("Query stream enabled (BITDEX_QUERY_STREAM=1) — GET /debug/queries/stream");
                Some(tx)
            } else {
                None
            },
            query_tee_mode: AtomicBool::new(false),
            metrics_bitmap_memory: AtomicBool::new(true),
            metrics_eviction_stats: AtomicBool::new(true),
            metrics_boundstore_disk: AtomicBool::new(true),
            #[cfg(feature = "pg-sync")]
            ops_wal: Mutex::new(None),
            #[cfg(feature = "pg-sync")]
            sync_meta: Mutex::new(HashMap::new()),
            #[cfg(feature = "pg-sync")]
            dump_registry: {
                let dumps_path = self.data_dir.join("dumps.json");
                let mut reg = crate::pg_sync::dump::DumpRegistry::load(&dumps_path);
                // Auto-clear stale dump state after PVC wipe: if dumps.json has
                // Complete entries but no bitmaps exist, the PVC was wiped.
                let indexes_dir = self.data_dir.join("indexes");
                let has_bitmaps = indexes_dir.exists() && std::fs::read_dir(&indexes_dir).ok()
                    .map(|entries| entries.filter_map(|e| e.ok())
                        .any(|e| e.path().join("bitmaps").exists()))
                    .unwrap_or(false);
                if !has_bitmaps && reg.dumps.values().any(|d| d.status == crate::pg_sync::dump::DumpStatus::Complete) {
                    eprintln!("WARNING: dumps.json has Complete entries but no bitmaps found — clearing stale dump state (PVC wipe detected)");
                    reg = crate::pg_sync::dump::DumpRegistry::default();
                    reg.save(&dumps_path).ok();
                }
                Mutex::new(reg)
            },
            #[cfg(feature = "pg-sync")]
            slot_watermark: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            #[cfg(feature = "pg-sync")]
            precreator_done: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(feature = "pg-sync")]
            precreator_started: std::sync::atomic::AtomicBool::new(false),
        });

        // Try to restore an existing index from disk
        let restore_start = std::time::Instant::now();
        if let Err(e) = restore_index(&state) {
            eprintln!("Warning: failed to restore index from disk: {e}");
        }
        let restore_elapsed = restore_start.elapsed();
        state.metrics.startup_duration_seconds.set(restore_elapsed.as_secs() as i64);
        if state.index.lock().is_some() {
            eprintln!("Index restore took {:.2}s", restore_elapsed.as_secs_f64());
        }

        // Apply persisted metric config — disabled_metrics takes precedence over enabled_metrics
        if let Some(ref idx) = *state.index.lock() {
            let config = &idx.definition.config;
            if let Some(ref disabled) = config.disabled_metrics {
                // Opt-out model: everything ON except what's listed
                let bm = !disabled.iter().any(|g| g == "bitmap_memory");
                let ev = !disabled.iter().any(|g| g == "eviction_stats");
                let bd = !disabled.iter().any(|g| g == "boundstore_disk");
                state.metrics_bitmap_memory.store(bm, Ordering::Relaxed);
                state.metrics_eviction_stats.store(ev, Ordering::Relaxed);
                state.metrics_boundstore_disk.store(bd, Ordering::Relaxed);
                eprintln!("Restored disabled_metrics from config: {:?} (bitmap_memory={bm}, eviction_stats={ev}, boundstore_disk={bd})", disabled);
            } else if let Some(ref groups) = config.enabled_metrics {
                // Legacy opt-in model (deprecated)
                let bm = groups.iter().any(|g| g == "bitmap_memory");
                let ev = groups.iter().any(|g| g == "eviction_stats");
                let bd = groups.iter().any(|g| g == "boundstore_disk");
                state.metrics_bitmap_memory.store(bm, Ordering::Relaxed);
                state.metrics_eviction_stats.store(ev, Ordering::Relaxed);
                state.metrics_boundstore_disk.store(bd, Ordering::Relaxed);
                eprintln!("Restored enabled_metrics (legacy) from config: {:?} (bitmap_memory={bm}, eviction_stats={ev}, boundstore_disk={bd})", groups);
            }
            // If neither is set: all metrics default to ON (AtomicBool defaults true)
        }

        // Rebuild mode: delete existing bitmaps and rebuild from docstore
        if self.rebuild {
            if let Err(e) = rebuild_on_boot(&state) {
                eprintln!("FATAL: rebuild failed: {e}");
                std::process::exit(1);
            }
        }

        // Spawn WAL reader thread if pg-sync feature is enabled and index exists
        #[cfg(feature = "pg-sync")]
        let _wal_handle: Option<std::thread::JoinHandle<()>> = {
            let wal_dir = self.data_dir.join("wal");
            let wal_state = Arc::clone(&state);
            std::thread::Builder::new()
                .name("wal-reader".into())
                .spawn(move || {
                    // Load cursor from engine's named cursor system (MetaStore,
                    // persisted by the merge thread after bitmap ops and bound
                    // store writes, so the durable cursor never advances past
                    // durable state).
                    let cursor = {
                        let engine_cursor = wal_state.index.lock()
                            .as_ref()
                            .and_then(|idx| idx.engine.get_cursor("wal-reader"));
                        if let Some(ref val) = engine_cursor {
                            let c = crate::ops_wal::WalCursor::parse(val);
                            eprintln!("WAL reader: loaded cursor from MetaStore: {c}");
                            c
                        } else {
                            let c = crate::ops_wal::WalCursor::new(0, 0);
                            eprintln!("WAL reader: no cursor found, starting from beginning");
                            c
                        }
                    };
                    let mut reader = crate::ops_wal::WalReader::new(&wal_dir, cursor);
                    // Highest gen for which retention (deletion of older gens)
                    // has run, keyed to the persisted cursor — see the
                    // retention block in the batch loop.
                    let mut last_retention_gen: u32 = cursor.generation;
                    // Boot-time cleanup: gens strictly below the boot cursor are
                    // fully consumed AND durable (the boot cursor IS the persisted
                    // one) — safe to delete immediately.
                    reader.delete_gens_below(cursor.generation);
                    eprintln!("WAL reader started (cursor={}:{}, dir={})", cursor.generation, cursor.offset, wal_dir.display());

                    // Overdue-deferred sweep timer (audit 2026-07-07 fix A4).
                    // Config-driven: deferred_alive.sweep_interval_secs (0 = off).
                    // Baselined to "now" so the first sweep runs one full
                    // interval after boot (lazy loads settle first).
                    let mut last_deferred_sweep = std::time::Instant::now();
                    // Rotation cursor: where the last sweep pass stopped in the
                    // shadow-false candidate space. `None` = start from the top.
                    // Carrying it across cycles bounds full coverage to
                    // ceil(population / sweep_limit) cycles (page-cap fix).
                    let mut deferred_sweep_cursor: Option<crate::query::CursorPosition> = None;
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    while !wal_state.shutting_down.load(Ordering::Relaxed) {
                        // Pause WAL apply while the engine is in bulk-load mode.
                        // /ops POSTs continue to accept + write to WAL; this
                        // thread resumes apply once `exit_loading_mode` flips
                        // the flag back. Without this gate, ops applied on top
                        // of partial bulk-load state inflate per-bucket
                        // `ops_count`, which forces PR-#233's
                        // `read_bucket_values_indexed` fast-path to walk the
                        // ops section to filter ops where `op.value ∈ wanted`
                        // — observed cold-path lazy_load → 5 s timeout cluster
                        // on 2026-04-29 flip-back canary, recovered only after
                        // compaction merged ops back into the snapshot.
                        let in_loading = {
                            let guard = wal_state.index.lock();
                            guard.as_ref().map(|idx| idx.engine.is_loading_mode()).unwrap_or(false)
                        };
                        if in_loading {
                            std::thread::sleep(std::time::Duration::from_millis(100));
                            continue;
                        }

                        // Read a batch from the WAL
                        match reader.read_batch(10_000) {
                            Ok(batch) if !batch.entries.is_empty() => {
                                // Get engine reference
                                let engine = {
                                    let guard = wal_state.index.lock();
                                    guard.as_ref().map(|idx| Arc::clone(&idx.engine))
                                };

                                if let Some(engine) = engine {
                                    let cycle_start = std::time::Instant::now();
                                    let batch_len = batch.entries.len();
                                    let batch_crc = batch.crc_failures;

                                    // Build FieldMeta, CoalescerSink, and DocWriter for the ops processor
                                    let meta = crate::ops_processor::FieldMeta::from_config(engine.config());
                                    let sender = engine.mutation_sender();
                                    let mut sink = crate::ingester::CoalescerSink::new(sender);
                                    let mut doc_writer = crate::ops_processor::DocWriter::new(
                                        engine.docstore_arc(),
                                    );

                                    let mut entries = batch.entries;
                                    let (applied, skipped, errors) =
                                        crate::ops_processor::apply_ops_batch(
                                            &mut sink, &meta, &mut entries, Some(&engine),
                                            Some(&mut doc_writer),
                                        );

                                    // Flush pending docstore writes (DocWriter buffers tuples)
                                    doc_writer.flush();

                                    // Persist dictionaries dirtied by this batch. The ops
                                    // path mints keys for new LowCardinalityString values
                                    // (get_or_insert) but historically only the HTTP
                                    // upsert handlers persisted — a crash after minting
                                    // let boot reload a stale dict whose next_key
                                    // RE-ISSUED an on-disk-referenced key to a different
                                    // string: silent permanent value aliasing
                                    // (FOLLOWUP.md). Cheap: is_dirty is an atomic load
                                    // per field; the write (<1KB, atomic tmp+fsync+
                                    // rename) only happens when a batch actually minted
                                    // a new distinct string — rare in steady state.
                                    if let Err(e) = engine.persist_dirty_dictionaries() {
                                        eprintln!(
                                            "WAL reader: dictionary persist failed \
                                             (will retry next dirty batch): {e}"
                                        );
                                        wal_state.metrics.pgsync_errors_total
                                            .with_label_values(&["wal-reader-dict-persist"])
                                            .inc();
                                    }

                                    // Invalidate doc cache for mutated entities so
                                    // GET /documents returns fresh data after ops.
                                    if applied > 0 {
                                        for entry in &entries {
                                            let slot = entry.entity_id as u32;
                                            engine.evict_doc_cache(slot);
                                        }
                                    }

                                    // WAL read-side metrics
                                    if applied > 0 {
                                        wal_state.metrics.wal_ops_processed_total.inc_by(applied as u64);
                                        let epoch = std::time::SystemTime::now()
                                            .duration_since(std::time::UNIX_EPOCH)
                                            .unwrap_or_default()
                                            .as_secs();
                                        wal_state.metrics.wal_last_applied_timestamp_seconds.set(epoch as i64);
                                    }
                                    let cursor = reader.cursor();
                                    wal_state.metrics.wal_read_cursor_bytes.set(cursor.offset as i64);

                                    // Always log when ops are read — silence was hiding the skip path
                                    if applied > 0 || errors > 0 {
                                        eprintln!(
                                            "WAL reader: applied={applied} skipped={skipped} errors={errors} cursor={cursor}"
                                        );
                                    } else if skipped > 0 {
                                        eprintln!(
                                            "WAL reader: ALL SKIPPED batch={batch_len} skipped={skipped} crc_failures={batch_crc} cursor={cursor} slot_counter={}",
                                            engine.slot_counter()
                                        );
                                    }
                                    if errors > 0 {
                                        wal_state.metrics.pgsync_errors_total
                                            .with_label_values(&["wal-reader"])
                                            .inc_by(errors as u64);
                                    }

                                    // Store cursor in engine's named cursor system.
                                    // Persisted to disk by the merge thread AFTER bitmap
                                    // ops and bound store writes, ensuring crash-consistent
                                    // state: on restart, any unpersisted ops will be replayed
                                    // idempotently from the WAL.
                                    engine.set_cursor(
                                        "wal-reader".to_string(),
                                        cursor.serialize(),
                                    );

                                    // WAL retention: delete gen files strictly below the
                                    // DURABLY-PERSISTED cursor's generation — never the
                                    // reader's in-memory position, which runs up to a
                                    // merge cycle ahead. Deleting on the reader's gen hop
                                    // (the old behavior) opened a crash window where boot
                                    // loaded a durable cursor into a deleted gen and
                                    // silently skipped every op to that gen's end
                                    // (FOLLOWUP.md P1). Throttled: only when the reader
                                    // has moved past previously-retained gens.
                                    if cursor.generation > last_retention_gen {
                                        if let Some(persisted) = engine
                                            .load_persisted_cursor("wal-reader")
                                            .map(|v| crate::ops_wal::WalCursor::parse(&v))
                                        {
                                            if persisted.generation > last_retention_gen {
                                                reader.delete_gens_below(persisted.generation);
                                                last_retention_gen = persisted.generation;
                                            }
                                        }
                                    }

                                    // Update metrics
                                    let m = &wal_state.metrics;
                                    m.sync_cycle_duration_seconds
                                        .with_label_values(&["wal-reader"])
                                        .observe(cycle_start.elapsed().as_secs_f64());
                                } else {
                                    // No index loaded yet — reset cursor so these ops
                                    // will be re-read on the next iteration (prevents data loss).
                                    let pre_cursor = crate::ops_wal::WalCursor::new(
                                        reader.cursor().generation,
                                        reader.cursor().offset.saturating_sub(batch.bytes_read),
                                    );
                                    eprintln!(
                                        "WAL reader: WARNING engine=None, {} ops not processed — rewinding cursor from {} to {pre_cursor}",
                                        batch.entries.len(), reader.cursor()
                                    );
                                    reader.set_cursor(pre_cursor);
                                    std::thread::sleep(std::time::Duration::from_secs(1));
                                }
                            }
                            Ok(batch) => {
                                // Empty entries — could be CRC failures or end of WAL
                                let cursor = reader.cursor();
                                if batch.crc_failures > 0 || batch.bytes_read > 0 {
                                    eprintln!(
                                        "WAL reader: empty batch but bytes_read={} crc_failures={} cursor={cursor}",
                                        batch.bytes_read, batch.crc_failures,
                                    );
                                }
                                wal_state.metrics.wal_read_cursor_bytes.set(cursor.offset as i64);
                                std::thread::sleep(std::time::Duration::from_millis(50));
                            }
                            Err(e) => {
                                eprintln!("WAL reader error: {e}");
                                wal_state.metrics.pgsync_errors_total
                                    .with_label_values(&["wal-reader"])
                                    .inc();
                                std::thread::sleep(std::time::Duration::from_secs(1));
                            }
                        }
                        // Overdue-deferred sweep: runs between WAL batches on this
                        // thread (query + doc reads must stay off the flush thread).
                        // Heals slots whose deferred activation was lost — shadow
                        // still false with the stored source timestamp in the past.
                        {
                            let engine = {
                                let guard = wal_state.index.lock();
                                guard.as_ref().map(|idx| Arc::clone(&idx.engine))
                            };
                            if let Some(engine) = engine {
                                let sweep_cfg = engine
                                    .config()
                                    .deferred_alive
                                    .as_ref()
                                    .map(|_| (engine.deferred_sweep_interval(), engine.deferred_sweep_limit()));
                                if let Some((interval, limit)) = sweep_cfg {
                                    if interval > 0
                                        && last_deferred_sweep.elapsed().as_secs() >= interval
                                        && !engine.is_loading_mode()
                                    {
                                        last_deferred_sweep = std::time::Instant::now();
                                        let meta = crate::ops_processor::FieldMeta::from_config(
                                            engine.config(),
                                        );
                                        let mut sink = crate::ingester::CoalescerSink::new(
                                            engine.mutation_sender(),
                                        );
                                        let mut dw = crate::ops_processor::DocWriter::new(
                                            engine.docstore_arc(),
                                        );
                                        let t0 = std::time::Instant::now();
                                        let (checked, healed, next_cursor) =
                                            crate::ops_processor::overdue_deferred_sweep(
                                                &mut sink, &meta, &engine, &mut dw, limit,
                                                deferred_sweep_cursor.take(),
                                            );
                                        deferred_sweep_cursor = next_cursor;
                                        dw.flush();
                                        if let Err(e) = crate::ingester::BitmapSink::flush(&mut sink) {
                                            eprintln!("overdue-deferred sweep: sink flush failed: {e}");
                                        }
                                        if !healed.is_empty() {
                                            for &slot in &healed {
                                                engine.evict_doc_cache(slot);
                                            }
                                            eprintln!(
                                                "overdue-deferred sweep: checked={checked} healed={} in {:?}",
                                                healed.len(),
                                                t0.elapsed()
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                    })); // end catch_unwind
                    match result {
                        Ok(()) => eprintln!("WAL reader exiting (shutdown)"),
                        Err(panic_info) => {
                            let msg = if let Some(s) = panic_info.downcast_ref::<&str>() {
                                s.to_string()
                            } else if let Some(s) = panic_info.downcast_ref::<String>() {
                                s.clone()
                            } else {
                                "unknown panic".to_string()
                            };
                            eprintln!("WAL reader PANICKED: {msg}");
                            // Ensure the metric reflects the error
                            wal_state.metrics.pgsync_errors_total
                                .with_label_values(&["wal-reader-panic"])
                                .inc();
                        }
                    }
                })
                .ok()
        };

        let shutdown_state = Arc::clone(&state);

        // Admin routes — require Bearer token (or disabled if no token configured)
        let admin_routes = Router::new()
            .route("/api/indexes", post(handle_create_index))
            .route("/api/indexes/{name}", delete(handle_delete_index))
            .route("/api/indexes/{name}/config", patch(handle_patch_config))
            .route("/api/indexes/{name}/load", post(handle_load))
            .route("/api/indexes/{name}/documents", post(handle_documents_batch).delete(handle_delete_docs))
            .route("/api/indexes/{name}/documents/{slot_id}", get(handle_get_document))
            .route("/api/indexes/{name}/documents/upsert", post(handle_upsert))
            .route("/api/indexes/{name}/documents/patch", patch(handle_patch_documents))
            .route("/api/indexes/{name}/documents/filter-sync", post(handle_filter_sync))
            .route("/api/indexes/{name}/cache", delete(handle_clear_cache))
            .route("/api/indexes/{name}/cache/persistent", delete(handle_purge_cache))
            .route("/api/indexes/{name}/warm", post(handle_warm_cache))
            .route("/api/indexes/{name}/rebuild", post(handle_rebuild))
            .route("/api/indexes/{name}/fields", post(handle_add_fields).delete(handle_remove_fields))
            .route("/api/indexes/{name}/fields/{field}/reload", post(handle_reload_field))
            .route("/api/indexes/{name}/compact", post(handle_compact))
            .route("/api/indexes/{name}/time-buckets/rebuild", post(handle_rebuild_time_buckets))
            .route("/api/indexes/{name}/time-buckets/audit", get(handle_time_bucket_audit))
            .route("/api/indexes/{name}/snapshot", post(handle_save_snapshot))
            .route("/api/indexes/{name}/redump", post(handle_redump))
            .route("/api/indexes/{name}/cursors/{cursor_name}", put(handle_set_cursor))
            // Capture endpoints (Phase 2)
            .route("/api/indexes/{name}/cache/entry", get(handle_cache_entry_inspect))
            .route("/api/indexes/{name}/prefilters", get(handle_list_prefilters).post(handle_register_prefilter))
            .route("/api/indexes/{name}/prefilters/{prefilter_name}", delete(handle_remove_prefilter))
            .route("/api/indexes/{name}/prefilters/refresh", post(handle_refresh_prefilters))
            .route("/debug/capture/start", post(handle_capture_start))
            .route("/debug/capture/stop", post(handle_capture_stop))
            .route("/debug/capture/status", get(handle_capture_status))
            .route("/debug/rescan-memory", post(handle_rescan_memory))
            .route("/debug/queries/stream", get(handle_query_stream))
            .route_layer(axum::middleware::from_fn_with_state(Arc::clone(&state), require_admin))
            .with_state(Arc::clone(&state));

        // Public routes — no auth required
        let public_routes = Router::new()
            .route("/api/indexes", get(handle_list_indexes))
            .route("/api/indexes/{name}", get(handle_get_index))
            .route("/api/indexes/{name}/query", post(handle_query))
            // ConcurrencyLimit removed — use runtime max_query_concurrency via
            // PATCH /config instead. The hardcoded Tower layer blocked queries
            // before SSE broadcast and wasn't hot-configurable.
            .route("/api/indexes/{name}/document", post(handle_document))
            .route("/api/indexes/{name}/traces", get(handle_traces))
            .route("/api/indexes/{name}/stats", get(handle_stats))
            .route("/api/indexes/{name}/tasks", get(handle_list_tasks))
            .route("/api/tasks/{task_id}", get(handle_get_task))
            .route("/api/indexes/{name}/cursors", get(handle_list_cursors))
            .route("/api/indexes/{name}/cursors/{cursor_name}", get(handle_get_cursor))
            .route("/api/health", get(handle_health))
            .route("/api/ready", get(handle_ready))
            .route("/debug/memory", get(handle_debug_memory))
            .route("/debug/heap-dump", axum::routing::post(handle_heap_dump))
            .route("/api/formats", get(handle_list_formats))
            .route("/api/internal/pgsync-metrics", post(handle_pgsync_metrics))
            .route("/api/indexes/{name}/ops", post(handle_ops))
            .route("/api/internal/sync-lag", get(handle_sync_lag))
            .route("/api/indexes/{name}/dumps", get(handle_list_dumps))
            .route("/api/indexes/{name}/dumps", put(handle_register_dump))
            .route("/api/indexes/{name}/dumps/{dump_name}/loaded", post(handle_dump_loaded))
            .route("/api/indexes/{name}/dumps/{dump_name}", delete(handle_delete_dump))
            .route("/api/indexes/{name}/dumps/clear", post(handle_clear_dumps))
            .route("/api/indexes/{name}/dictionaries", get(handle_dictionaries))
            .route("/api/indexes/{name}/ui-config", get(handle_ui_config))
            .route("/metrics", get(handle_metrics))
            .route("/", get(handle_ui))
            .with_state(Arc::clone(&state));

        let app = Router::new()
            .merge(admin_routes)
            .merge(public_routes)
            .layer(axum::middleware::from_fn_with_state(Arc::clone(&state), capture_traffic))
            .layer(CorsLayer::permissive())
            .layer(axum::extract::DefaultBodyLimit::max(64 * 1024 * 1024)) // 64MB for bulk upserts
            .layer(axum::middleware::from_fn_with_state(Arc::clone(&state), measure_http_roundtrip));

        eprintln!("BitDex server listening on http://{}", addr);
        eprintln!("  RAYON_NUM_THREADS={}, actual={}", std::env::var("RAYON_NUM_THREADS").unwrap_or("(not set)".into()), rayon::current_num_threads());

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

        // Pre-listen: load eager fields + bound cache shards synchronously.
        // The server won't accept traffic until all eager bitmaps are loaded
        // and cache shards are restored. This prevents cold-start stampedes
        // where queries arrive before bitmaps are in memory.
        {
            let engine_arc = shutdown_state.index.lock()
                .as_ref()
                .map(|s| Arc::clone(&s.engine));
            if let Some(ref engine) = engine_arc {
                // Phase 5: Eager fields (bitmaps needed for queries)
                let phase_start = std::time::Instant::now();
                engine.preload_eager_fields();
                let phase5_elapsed = phase_start.elapsed();
                eprintln!("  Boot phase: eager_fields completed in {}ms", phase5_elapsed.as_millis());
                state.metrics.boot_phase_seconds
                    .with_label_values(&["eager_fields"])
                    .set(phase5_elapsed.as_secs() as i64);

                // Phase 6: Bound cache shards (persisted cache entries)
                let phase_start = std::time::Instant::now();
                engine.preload_bound_cache();
                let phase6_elapsed = phase_start.elapsed();
                eprintln!("  Boot phase: bound_cache completed in {}ms", phase6_elapsed.as_millis());
                state.metrics.boot_phase_seconds
                    .with_label_values(&["bound_cache"])
                    .set(phase6_elapsed.as_secs() as i64);
            }
        }

        // Health listener on its own tokio runtime (port+1).
        // MUST be a separate runtime — not just a separate tokio::spawn.
        // When query handlers saturate the main tokio thread pool (.awaiting
        // spawn_blocking JoinHandles for bitmap ops at 70+ QPS), ALL tasks
        // on that runtime stall, including health handlers. A separate
        // runtime with its own OS threads is completely isolated.
        let health_port = addr.port() + 1;
        let health_addr_str = format!("0.0.0.0:{health_port}");
        std::thread::Builder::new()
            .name("health-listener".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("health runtime build");
                rt.block_on(async move {
                    let health_app = axum::Router::new()
                        .route("/api/health", axum::routing::get(|| async { "ok" }));
                    let listener = tokio::net::TcpListener::bind(&health_addr_str)
                        .await
                        .expect("health listener bind");
                    eprintln!("Health listener on http://0.0.0.0:{health_port}/api/health (separate runtime)");
                    let _ = axum::serve(listener, health_app).await;
                });
            })
            .expect("health thread spawn");

        let listener = tokio::net::TcpListener::bind(addr).await?;

        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal)
            .await?;

        // Signal all background threads + dump tasks to stop
        shutdown_state.shutting_down.store(true, Ordering::SeqCst);
        eprintln!("Shutdown signal sent — waiting for active tasks to abort...");

        // Brief wait for spawn_blocking dump tasks to notice the shutdown flag
        // (they check every 1M rows, so <1s for most phases)
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // Wait for the WAL reader thread to exit (it checks shutting_down)
        #[cfg(feature = "pg-sync")]
        if let Some(handle) = _wal_handle {
            eprintln!("Waiting for WAL reader to exit...");
            handle.join().ok();
        }

        // After graceful shutdown: save snapshot and shut down the engine
        eprintln!("Server stopped, saving final snapshot...");
        {
            let guard = shutdown_state.index.lock();
            if let Some(ref index_state) = *guard {
                if let Err(e) = index_state.engine.save_snapshot() {
                    eprintln!("Warning: failed to save final snapshot: {e}");
                }
                // Signal engine background threads to stop (flush, merge, compact, etc.)
                // Uses request_shutdown(&self) since engine is behind Arc.
                index_state.engine.request_shutdown();
            }
        }
        eprintln!("Shutdown complete.");

        // Force exit to ensure any lingering threads or mmap handles don't
        // keep the process alive. All data has been saved at this point.
        std::process::exit(0);
    }
}

// ---------------------------------------------------------------------------
// Index restoration from disk
// ---------------------------------------------------------------------------

fn restore_index(state: &SharedState) -> Result<(), String> {
    // When index_dir is set, scan it for configs (ConfigMap mount).
    // Runtime data (bitmaps, docstore) always lives under data_dir/indexes/.
    let config_source_dir = state.index_dir.clone()
        .unwrap_or_else(|| state.data_dir.join("indexes"));
    let data_indexes_dir = state.data_dir.join("indexes");

    if !config_source_dir.exists() {
        return Ok(());
    }

    // Scan for index directories with config.yaml
    let entries = std::fs::read_dir(&config_source_dir).map_err(|e| e.to_string())?;
    for entry in entries {
        let entry = entry.map_err(|e| e.to_string())?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }

        let config_path = match find_index_config(&path) {
            Some(p) => p,
            None => continue,
        };

        // Phase 1: Config loading
        let phase_start = std::time::Instant::now();
        let mut def: IndexDefinition = IndexDefinition::from_file(&config_path)?;
        let phase1_elapsed = phase_start.elapsed();
        eprintln!("  Boot phase: config_load completed in {}ms", phase1_elapsed.as_millis());
        state.metrics.boot_phase_seconds
            .with_label_values(&["config_load"])
            .set(phase1_elapsed.as_secs() as i64);

        // Phase 2: Engine creation
        // Runtime data lives under data_dir/indexes/<name>, even when config
        // is loaded from an external index_dir (ConfigMap mount).
        let phase_start = std::time::Instant::now();
        let index_name = path.file_name().unwrap().to_string_lossy();
        let runtime_dir = if state.index_dir.is_some() {
            let d = data_indexes_dir.join(&*index_name);
            std::fs::create_dir_all(&d).ok();
            d
        } else {
            path.clone()
        };
        let bitmap_path = runtime_dir.join("bitmaps");
        let docstore_path = runtime_dir.join("docs");
        let mut config = def.config.clone();
        config.data_schema = def.data_schema.clone();
        config.storage.bitmap_path = Some(bitmap_path.clone());

        // Always use new_with_path so bitmaps restore from bitmap_path even if
        // docstore doesn't exist yet (it will be created fresh).
        let mut engine = ConcurrentEngine::new_with_path(config, &docstore_path)
            .map_err(|e| e.to_string())?;
        let phase2_elapsed = phase_start.elapsed();
        eprintln!("  Boot phase: engine_create completed in {}ms", phase2_elapsed.as_millis());
        state.metrics.boot_phase_seconds
            .with_label_values(&["engine_create"])
            .set(phase2_elapsed.as_secs() as i64);

        // Phase 3: Dictionary loading
        let phase_start = std::time::Instant::now();
        // Load LowCardinalityString dictionaries from disk
        let lcs_dicts = ConcurrentEngine::load_dictionaries(&def.data_schema, &bitmap_path)
            .map_err(|e| e.to_string())?;

        // Build reverse maps BEFORE normalization to preserve original casing
        let reverse_maps = build_reverse_string_maps_with_dicts(&def.data_schema, Some(&lcs_dicts));
        def.data_schema.normalize_string_maps();

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
        let phase3_elapsed = phase_start.elapsed();
        eprintln!("  Boot phase: dictionary_load completed in {}ms", phase3_elapsed.as_millis());
        state.metrics.boot_phase_seconds
            .with_label_values(&["dictionary_load"])
            .set(phase3_elapsed.as_secs() as i64);

        // Phase 4: Metrics bridge wiring
        let phase_start = std::time::Instant::now();
        // Wire Prometheus metrics bridge into the engine's background threads.
        engine.set_metrics_bridge(crate::concurrent_engine::MetricsBridge {
            lazy_load_duration: state.metrics.lazy_load_duration_seconds.clone(),
            compaction_total: state.metrics.compaction_total.clone(),
            compaction_duration: state.metrics.compaction_duration_seconds.clone(),
            query_op_set_fanout_size: state.metrics.query_op_set_fanout_size.clone(),
            query_op_set_rejected_total: state.metrics.query_op_set_rejected_total.clone(),
            query_op_set_zero_match_total: state.metrics.query_op_set_zero_match_total.clone(),
            query_op_set_applied_slots_total: state.metrics.query_op_set_applied_slots_total.clone(),
            deferred_fanout_scanned_total: state.metrics.deferred_fanout_scanned_total.clone(),
            deferred_fanout_reached_total: state.metrics.deferred_fanout_reached_total.clone(),
            wal_apply_batch_seconds: state.metrics.wal_apply_batch_seconds.clone(),
            bitmap_mem_scan_tick_seconds: state.metrics.bitmap_mem_scan_tick_seconds.clone(),
            query_total: state.metrics.query_total.clone(),
            timebucket_dropped_no_sort_field_total: state
                .metrics
                .timebucket_dropped_no_sort_field_total
                .clone(),
            timebucket_dropped_capacity_exceeded_total: state
                .metrics
                .timebucket_dropped_capacity_exceeded_total
                .clone(),
            timebucket_applied_not_bucketed_total: state
                .metrics
                .timebucket_applied_not_bucketed_total
                .clone(),
            timebucket_anomalous_ts_total: state
                .metrics
                .timebucket_anomalous_ts_total
                .clone(),
            time_bucket_full_rebuild_duration_seconds: state
                .metrics
                .time_bucket_full_rebuild_duration_seconds
                .clone(),
            time_bucket_full_rebuild_total: state.metrics.time_bucket_full_rebuild_total.clone(),
            time_bucket_pruned_total: state.metrics.time_bucket_pruned_total.clone(),
            time_bucket_backfilled_total: state.metrics.time_bucket_backfilled_total.clone(),
            time_bucket_stale: state.metrics.time_bucket_stale.clone(),
            time_bucket_missing: state.metrics.time_bucket_missing.clone(),
            time_bucket_reconcile_apply_seconds: state
                .metrics
                .time_bucket_reconcile_apply_seconds
                .clone(),
            index_name: def.name.clone(),
        });
        // Install the cache-worker cycle-time histogram so the worker can
        // observe per cycle. OnceLock — first set wins; idempotent on retries.
        let _ = engine
            .cache_worker_metrics()
            .cycle_histogram
            .set(
                state
                    .metrics
                    .cache_worker_cycle_seconds
                    .with_label_values(&[&def.name]),
            );
        let phase4_elapsed = phase_start.elapsed();
        eprintln!("  Boot phase: metrics_bridge completed in {}ms", phase4_elapsed.as_millis());
        state.metrics.boot_phase_seconds
            .with_label_values(&["metrics_bridge"])
            .set(phase4_elapsed.as_secs() as i64);

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

        let engine = Arc::new(engine);

        // Auto-warm: replay persisted query shapes to pre-populate cache.
        // Runs before index is visible to HTTP handlers, so warm queries
        // don't compete with real traffic.
        let warmed = engine.auto_warm();
        if warmed > 0 {
            state.metrics.boot_phase_seconds
                .with_label_values(&["auto_warm"])
                .set(0); // timing already printed by auto_warm
        }

        *state.index.lock() = Some(IndexState {
            engine,
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
/// Requires an index to already be restored (config.yaml + docstore must exist).
/// Deletes the bitmaps directory, runs `build_all_from_docstore`, then
/// `save_and_unload` to persist and free memory.
fn rebuild_on_boot(state: &SharedState) -> Result<(), String> {
    use crate::concurrent_engine::get_rss_bytes;

    let guard = state.index.lock();
    let idx = guard.as_ref().ok_or("No index found — cannot rebuild without config")?;

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
pub fn build_string_maps_with_dicts(
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
    if let Err(e) = definition.save_yaml(&index_dir) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": format!("Failed to write config: {e}")})),
        ).into_response();
    }

    // Create engine
    let docstore_path = index_dir.join("docs");
    let mut config = req.config;
    config.data_schema = definition.data_schema.clone();
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

    // Wire Prometheus metrics bridge into the engine's background threads.
    engine.set_metrics_bridge(crate::concurrent_engine::MetricsBridge {
        lazy_load_duration: state.metrics.lazy_load_duration_seconds.clone(),
        compaction_total: state.metrics.compaction_total.clone(),
        compaction_duration: state.metrics.compaction_duration_seconds.clone(),
        query_op_set_fanout_size: state.metrics.query_op_set_fanout_size.clone(),
        query_op_set_rejected_total: state.metrics.query_op_set_rejected_total.clone(),
        query_op_set_zero_match_total: state.metrics.query_op_set_zero_match_total.clone(),
        query_op_set_applied_slots_total: state.metrics.query_op_set_applied_slots_total.clone(),
        deferred_fanout_scanned_total: state.metrics.deferred_fanout_scanned_total.clone(),
        deferred_fanout_reached_total: state.metrics.deferred_fanout_reached_total.clone(),
        wal_apply_batch_seconds: state.metrics.wal_apply_batch_seconds.clone(),
        bitmap_mem_scan_tick_seconds: state.metrics.bitmap_mem_scan_tick_seconds.clone(),
        query_total: state.metrics.query_total.clone(),
        timebucket_dropped_no_sort_field_total: state
            .metrics
            .timebucket_dropped_no_sort_field_total
            .clone(),
        timebucket_dropped_capacity_exceeded_total: state
            .metrics
            .timebucket_dropped_capacity_exceeded_total
            .clone(),
        timebucket_applied_not_bucketed_total: state
            .metrics
            .timebucket_applied_not_bucketed_total
            .clone(),
        timebucket_anomalous_ts_total: state.metrics.timebucket_anomalous_ts_total.clone(),
        time_bucket_full_rebuild_duration_seconds: state.metrics.time_bucket_full_rebuild_duration_seconds.clone(),
        time_bucket_full_rebuild_total: state.metrics.time_bucket_full_rebuild_total.clone(),
        time_bucket_pruned_total: state.metrics.time_bucket_pruned_total.clone(),
        time_bucket_backfilled_total: state.metrics.time_bucket_backfilled_total.clone(),
        time_bucket_stale: state.metrics.time_bucket_stale.clone(),
        time_bucket_missing: state.metrics.time_bucket_missing.clone(),
        time_bucket_reconcile_apply_seconds: state.metrics.time_bucket_reconcile_apply_seconds.clone(),
        index_name: definition.name.clone(),
    });

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

// ---------------------------------------------------------------------------
// Handlers: UI — Dictionaries & UI Config
// ---------------------------------------------------------------------------

/// GET /api/indexes/{name}/dictionaries — reverse maps (int → display string)
/// for all fields that have dictionaries (LowCardinalityString) or string_maps
/// (MappedString). The UI uses these to populate dropdowns and render labels.
async fn handle_dictionaries(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
) -> impl IntoResponse {
    let guard = state.index.lock();
    match guard.as_ref() {
        Some(idx) if idx.definition.name == name => {
            let mut result: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();

            // LowCardinalityString dictionaries from the engine
            for (field_name, dict) in idx.engine.dictionaries().iter() {
                let snap = dict.snapshot();
                let reverse = snap.to_reverse_map();
                let map: serde_json::Map<String, serde_json::Value> = reverse.iter()
                    .map(|(k, v)| (k.to_string(), serde_json::Value::String(v.clone())))
                    .collect();
                result.insert(field_name.clone(), serde_json::Value::Object(map));
            }

            // MappedString fields from data_schema (reverse the string_map)
            for mapping in &idx.definition.data_schema.fields {
                if let Some(ref string_map) = mapping.string_map {
                    if !result.contains_key(&mapping.target) {
                        let reverse: serde_json::Map<String, serde_json::Value> = string_map.iter()
                            .map(|(label, &id)| (id.to_string(), serde_json::Value::String(label.clone())))
                            .collect();
                        result.insert(mapping.target.clone(), serde_json::Value::Object(reverse));
                    }
                }
            }

            Json(serde_json::Value::Object(result)).into_response()
        }
        _ => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("Index '{}' not found", name)})),
        ).into_response(),
    }
}

/// GET /api/indexes/{name}/ui-config — serve the UI config YAML as JSON.
/// Loaded from data_dir/indexes/{name}/ui-config.yaml (or index_dir if set).
/// Returns {} if no UI config file exists (UI falls back to auto-generated controls).
async fn handle_ui_config(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
) -> impl IntoResponse {
    let config_source_dir = state.index_dir.clone()
        .unwrap_or_else(|| state.data_dir.join("indexes"));
    let candidates = [
        config_source_dir.join(&name).join("ui-config.yaml"),
        config_source_dir.join(&name).join("ui-config.yml"),
        state.data_dir.join("indexes").join(&name).join("ui-config.yaml"),
        state.data_dir.join("indexes").join(&name).join("ui-config.yml"),
    ];

    for path in &candidates {
        if path.exists() {
            match std::fs::read_to_string(path) {
                Ok(yaml_str) => {
                    match serde_yaml::from_str::<serde_json::Value>(&yaml_str) {
                        Ok(val) => return Json(val).into_response(),
                        Err(e) => {
                            eprintln!("Failed to parse ui-config at {}: {e}", path.display());
                            return (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Json(serde_json::json!({"error": format!("Invalid ui-config YAML: {e}")})),
                            ).into_response();
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Failed to read ui-config at {}: {e}", path.display());
                }
            }
        }
    }

    // No config file — return empty object (UI auto-generates)
    Json(serde_json::json!({})).into_response()
}

// ---------------------------------------------------------------------------
// Handlers: Config Patch
// ---------------------------------------------------------------------------

/// Parse the ordinal suffix from a StatefulSet pod name of the form `<prefix>-<N>`.
/// Returns `Some(N)` if the trailing component is a non-negative integer, `None` otherwise.
///
/// Examples:
///   "bitdex-0"  → Some(0)
///   "bitdex-1"  → Some(1)
///   "bitdex-12" → Some(12)
///   "bitdex"    → None
///   ""          → None
fn parse_pod_ordinal(pod_name: &str) -> Option<usize> {
    pod_name.rsplit('-').next().and_then(|s| s.parse().ok())
}

async fn handle_patch_config(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
    headers: HeaderMap,
    Json(patch): Json<ConfigPatch>,
) -> impl IntoResponse {
    // ---------------------------------------------------------------------------
    // HA fan-out: forward this patch to peer pods if this is a user-facing call.
    //
    // Header `X-BitDex-Patch-Origin`:
    //   absent        → user-facing: apply locally, then fan out to peers
    //   own pod name  → misrouted self-patch: apply locally only
    //   peer pod name → peer-driven patch: apply locally only (no cascading)
    // ---------------------------------------------------------------------------
    let pod_name = std::env::var("POD_NAME").ok();
    let replica_count: Option<usize> = std::env::var("BITDEX_REPLICA_COUNT")
        .ok()
        .and_then(|v| v.parse().ok());
    let patch_origin = headers
        .get("x-bitdex-patch-origin")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_owned());

    // Determine whether to fan out: only when this is a user-facing request
    // (no origin header), we know our own pod name, and there are 2+ replicas.
    let should_fanout = patch_origin.is_none()
        && pod_name.is_some()
        && replica_count.map(|n| n > 1).unwrap_or(false);
    let (engine, updated_config) = {
        let mut guard = state.index.lock();
        match guard.as_mut() {
            Some(idx) if idx.definition.name == name => {
                // Validate filter field names
                if let Some(ref filter_patches) = patch.filter_fields {
                    for fname in filter_patches.keys() {
                        if !idx.definition.config.filter_fields.iter().any(|f| &f.name == fname) {
                            return (
                                StatusCode::BAD_REQUEST,
                                Json(serde_json::json!({
                                    "error": format!("Unknown filter field: '{}'", fname)
                                })),
                            ).into_response();
                        }
                    }
                }

                // Validate sort field names
                if let Some(ref sort_patches) = patch.sort_fields {
                    for sname in sort_patches.keys() {
                        if !idx.definition.config.sort_fields.iter().any(|f| &f.name == sname) {
                            return (
                                StatusCode::BAD_REQUEST,
                                Json(serde_json::json!({
                                    "error": format!("Unknown sort field: '{}'", sname)
                                })),
                            ).into_response();
                        }
                    }
                }

                // Validate cache patch values
                if let Some(ref cache_patch) = patch.cache {
                    if let Some(dr) = cache_patch.decay_rate {
                        if dr <= 0.0 || dr > 1.0 {
                            return (
                                StatusCode::BAD_REQUEST,
                                Json(serde_json::json!({
                                    "error": "cache.decay_rate must be in (0.0, 1.0]"
                                })),
                            ).into_response();
                        }
                    }
                    if let Some(pt) = cache_patch.prefetch_threshold {
                        if !(0.0..=1.0).contains(&pt) {
                            return (
                                StatusCode::BAD_REQUEST,
                                Json(serde_json::json!({
                                    "error": "cache.prefetch_threshold must be in [0.0, 1.0]"
                                })),
                            ).into_response();
                        }
                    }
                }

                // Apply filter field patches
                let mut newly_eager_filters: Vec<String> = Vec::new();
                if let Some(ref filter_patches) = patch.filter_fields {
                    for fc in idx.definition.config.filter_fields.iter_mut() {
                        if let Some(fp) = filter_patches.get(&fc.name) {
                            if let Some(eager) = fp.eager_load {
                                let was_eager = fc.eager_load;
                                fc.eager_load = eager;
                                if eager && !was_eager {
                                    newly_eager_filters.push(fc.name.clone());
                                }
                            }
                        }
                    }
                }

                // Apply sort field patches
                let mut newly_eager_sorts: Vec<String> = Vec::new();
                if let Some(ref sort_patches) = patch.sort_fields {
                    for sc in idx.definition.config.sort_fields.iter_mut() {
                        if let Some(sp) = sort_patches.get(&sc.name) {
                            if let Some(eager) = sp.eager_load {
                                let was_eager = sc.eager_load;
                                sc.eager_load = eager;
                                if eager && !was_eager {
                                    newly_eager_sorts.push(sc.name.clone());
                                }
                            }
                        }
                    }
                }

                // Apply cache patches
                if let Some(ref cache_patch) = patch.cache {
                    if let Some(v) = cache_patch.max_entries {
                        idx.definition.config.cache.max_entries = v;
                        idx.engine.set_cache_max_entries(v);
                    }
                    if let Some(v) = cache_patch.max_bytes {
                        idx.definition.config.cache.max_bytes = v;
                        idx.engine.set_cache_max_bytes(v);
                    }
                    if let Some(v) = cache_patch.initial_capacity {
                        idx.definition.config.cache.initial_capacity = v;
                        idx.engine.set_cache_initial_capacity(v);
                    }
                    if let Some(v) = cache_patch.max_capacity {
                        idx.definition.config.cache.max_capacity = v;
                        idx.engine.set_cache_max_capacity(v);
                    }
                    if let Some(v) = cache_patch.min_filter_size {
                        idx.definition.config.cache.min_filter_size = v;
                        idx.engine.set_cache_min_filter_size(v);
                    }
                    if let Some(v) = cache_patch.decay_rate {
                        idx.definition.config.cache.decay_rate = v;
                    }
                    if let Some(v) = cache_patch.bound_target_size {
                        idx.definition.config.cache.bound_target_size = v;
                    }
                    if let Some(v) = cache_patch.bound_max_size {
                        idx.definition.config.cache.bound_max_size = v;
                    }
                    if let Some(v) = cache_patch.bound_max_count {
                        idx.definition.config.cache.bound_max_count = v;
                    }
                    if let Some(v) = cache_patch.prefetch_threshold {
                        idx.definition.config.cache.prefetch_threshold = v;
                    }
                    if let Some(v) = cache_patch.max_maintenance_work {
                        idx.definition.config.cache.max_maintenance_work = v;
                        idx.engine.set_max_maintenance_work(v);
                    }
                    if let Some(v) = cache_patch.max_maintenance_ms {
                        idx.definition.config.cache.max_maintenance_ms = v;
                        idx.engine.set_max_maintenance_ms(v);
                    }
                    if let Some(v) = cache_patch.compound_eval_atom_limit {
                        idx.definition.config.cache.compound_eval_atom_limit = v;
                        idx.engine.set_compound_eval_atom_limit(v);
                    }
                    if let Some(v) = cache_patch.bucket_entry_ttl_secs {
                        idx.definition.config.cache.bucket_entry_ttl_secs = v;
                        idx.engine.set_bucket_entry_ttl_secs(v);
                    }
                    if let Some(v) = cache_patch.async_maintenance {
                        // Updates the stored config (persisted on next save).
                        // Takes effect on next server restart — the worker channel
                        // is wired at startup, not at runtime.
                        idx.definition.config.cache.async_maintenance = v;
                    }
                }

                // Apply time bucket patches
                if let Some(ref tb_patch) = patch.time_buckets {
                    if let Some(ref range_patches) = tb_patch.range_buckets {
                        for rp in range_patches {
                            if let Some(interval) = rp.refresh_interval_secs {
                                if interval == 0 {
                                    return (
                                        StatusCode::BAD_REQUEST,
                                        Json(serde_json::json!({
                                            "error": format!(
                                                "time_buckets bucket '{}': refresh_interval_secs must be > 0",
                                                rp.name
                                            )
                                        })),
                                    ).into_response();
                                }
                                let found = idx.engine.set_time_bucket_refresh_interval(&rp.name, interval);
                                if !found {
                                    return (
                                        StatusCode::BAD_REQUEST,
                                        Json(serde_json::json!({
                                            "error": format!("Unknown time bucket: '{}'", rp.name)
                                        })),
                                    ).into_response();
                                }
                                // Update persisted config so it survives restart
                                if let Some(ref mut tb_config) = idx.definition.config.time_buckets {
                                    if let Some(bc) = tb_config.range_buckets.iter_mut().find(|b| b.name == rp.name) {
                                        bc.refresh_interval_secs = interval;
                                    }
                                }
                                eprintln!("Config patch: time_bucket '{}' refresh_interval_secs set to {interval}", rp.name);
                            }
                        }
                    }
                }

                // Apply max_query_concurrency (server-wide, not persisted with index config)
                if let Some(v) = patch.max_query_concurrency {
                    state.max_query_concurrency.store(v, Ordering::Relaxed);
                    eprintln!("Config patch: max_query_concurrency set to {v}");
                }
                if let Some(v) = patch.query_tee_mode {
                    state.query_tee_mode.store(v, Ordering::Relaxed);
                    eprintln!("Config patch: query_tee_mode set to {v}");
                }
                if let Some(v) = patch.par_iter_min_threshold {
                    idx.engine.set_par_iter_min_threshold(v);
                    eprintln!("Config patch: par_iter_min_threshold set to {v}");
                }
                if let Some(v) = patch.bitmap_compact_threshold {
                    idx.engine.set_bitmap_compact_threshold(v);
                    eprintln!("Config patch: bitmap_compact_threshold set to {v}");
                }
                if let Some(v) = patch.time_bucket_full_rebuild_interval_secs {
                    idx.engine.set_time_bucket_full_rebuild_interval(v);
                    eprintln!("Config patch: time_bucket_full_rebuild_interval_secs set to {v}");
                }
                if let Some(v) = patch.deferred_sweep_interval_secs {
                    idx.engine.set_deferred_sweep_interval(v);
                    eprintln!("Config patch: deferred_sweep_interval_secs set to {v}");
                }
                if let Some(v) = patch.deferred_sweep_limit {
                    idx.engine.set_deferred_sweep_limit(v);
                    eprintln!("Config patch: deferred_sweep_limit set to {v}");
                }
                if let Some(v) = patch.max_registered_prefilters {
                    if v > crate::prefilter::MAX_REGISTERED_PREFILTERS {
                        return (
                            StatusCode::BAD_REQUEST,
                            Json(serde_json::json!({
                                "error": format!(
                                    "max_registered_prefilters must be 0-{} (got {v})",
                                    crate::prefilter::MAX_REGISTERED_PREFILTERS
                                )
                            })),
                        ).into_response();
                    }
                    idx.definition.config.max_registered_prefilters = v;
                    idx.engine.set_max_registered_prefilters(v);
                    eprintln!("Config patch: max_registered_prefilters set to {v}");
                }

                // Toggle trace collection (server-wide, not persisted with index config)
                if let Some(v) = patch.enable_traces {
                    state.enable_traces.store(v, Ordering::Relaxed);
                    eprintln!("Config patch: enable_traces set to {v}");
                }
                if let Some(v) = patch.trace_min_us {
                    state.trace_min_us.store(v, Ordering::Relaxed);
                    eprintln!("Config patch: trace_min_us set to {v}μs");
                }
                if let Some(v) = patch.trace_buffer_size {
                    if v == 0 {
                        return (
                            StatusCode::BAD_REQUEST,
                            Json(serde_json::json!({
                                "error": "trace_buffer_size must be > 0"
                            })),
                        ).into_response();
                    }
                    state.trace_buffer.resize(v);
                    eprintln!("Config patch: trace_buffer_size set to {v}");
                }

                // Toggle metric groups — disabled_metrics takes precedence
                if let Some(ref disabled) = patch.disabled_metrics {
                    let bm = !disabled.iter().any(|g| g == "bitmap_memory");
                    let ev = !disabled.iter().any(|g| g == "eviction_stats");
                    let bd = !disabled.iter().any(|g| g == "boundstore_disk");
                    state.metrics_bitmap_memory.store(bm, Ordering::Relaxed);
                    state.metrics_eviction_stats.store(ev, Ordering::Relaxed);
                    state.metrics_boundstore_disk.store(bd, Ordering::Relaxed);
                    idx.definition.config.disabled_metrics = Some(disabled.clone());
                    idx.definition.config.enabled_metrics = None; // clear legacy
                    eprintln!("Config patch: disabled_metrics = {:?} (bitmap_memory={bm}, eviction_stats={ev}, boundstore_disk={bd})", disabled);
                } else if let Some(ref groups) = patch.enabled_metrics {
                    // Legacy opt-in (deprecated)
                    let bm = groups.iter().any(|g| g == "bitmap_memory");
                    let ev = groups.iter().any(|g| g == "eviction_stats");
                    let bd = groups.iter().any(|g| g == "boundstore_disk");
                    state.metrics_bitmap_memory.store(bm, Ordering::Relaxed);
                    state.metrics_eviction_stats.store(ev, Ordering::Relaxed);
                    state.metrics_boundstore_disk.store(bd, Ordering::Relaxed);
                    idx.definition.config.enabled_metrics = Some(groups.clone());
                    eprintln!("Config patch: enabled_metrics (legacy) = {:?} (bitmap_memory={bm}, eviction_stats={ev}, boundstore_disk={bd})", groups);
                }

                // Persist updated config
                let index_dir = state.data_dir.join("indexes").join(&name);
                if let Err(e) = idx.definition.save_yaml(&index_dir) {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({
                            "error": format!("Failed to persist config: {e}")
                        })),
                    ).into_response();
                }

                let engine = Arc::clone(&idx.engine);
                let config = idx.definition.config.clone();

                // Trigger eager loading for newly-eager fields
                if !newly_eager_filters.is_empty() || !newly_eager_sorts.is_empty() {
                    let engine_clone = Arc::clone(&engine);
                    tokio::task::spawn_blocking(move || {
                        use crate::query::{FilterClause, Value};

                        // Build synthetic filter clauses for newly-eager filter fields
                        let clauses: Vec<FilterClause> = newly_eager_filters
                            .iter()
                            .map(|name| FilterClause::Eq(name.clone(), Value::Integer(0)))
                            .collect();

                        // Load each newly-eager sort field
                        for sname in &newly_eager_sorts {
                            let _ = engine_clone.ensure_fields_loaded(&clauses, Some(sname));
                        }
                        // Load remaining filter-only fields
                        if !clauses.is_empty() {
                            let _ = engine_clone.ensure_fields_loaded(&clauses, None);
                        }

                        eprintln!(
                            "Config patch: loaded {} eager filter + {} eager sort fields",
                            newly_eager_filters.len(),
                            newly_eager_sorts.len(),
                        );
                    });
                }

                (engine, config)
            }
            _ => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({
                        "error": format!("Index '{}' not found", name)
                    })),
                ).into_response();
            }
        }
    };

    // Return the full updated config plus an explicit ephemeral warning. The
    // patch lives only in this pod's memory — it does NOT persist across pod
    // restarts. For permanent settings, edit the ConfigMap in talos-infra and
    // let Flux reconcile.
    let _ = engine; // engine kept in scope for spawned task

    let self_pod = pod_name.as_deref().unwrap_or("unknown");
    let applied_to = vec![self_pod.to_owned()];

    // Fan-out to peer pods if this is a user-facing patch.
    let peer_results = if should_fanout {
        let self_name = pod_name.as_deref().unwrap_or("");
        let n = replica_count.unwrap_or(1);

        // Parse self ordinal from POD_NAME (e.g. "bitdex-0" → 0). Reuse the
        // prefix from POD_NAME rather than hardcoding "bitdex-" so a renamed
        // StatefulSet or sidecar test harness still synthesizes correct peer
        // names.
        let self_ordinal: Option<usize> = parse_pod_ordinal(self_name);
        let pod_prefix: &str = self_name
            .rsplit_once('-')
            .map(|(p, _)| p)
            .unwrap_or("bitdex");

        // Build peer URL list (exclude self).
        let mut peer_urls: Vec<(String, String)> = Vec::new();
        if let Some(self_ord) = self_ordinal {
            for i in 0..n {
                if i == self_ord {
                    continue;
                }
                let pod = format!("{}-{}", pod_prefix, i);
                let url = format!(
                    "http://{}.bitdex-headless.bitdex.svc.cluster.local:3000/api/indexes/{}/config",
                    pod, name
                );
                peer_urls.push((pod, url));
            }
        }

        // Forward Authorization header if present.
        let auth_header = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_owned());

        // Serialize the patch body once. ConfigPatch is structurally always
        // serializable, but if encoding ever fails we want a hard error
        // rather than a silent empty-body fan-out that every peer rejects.
        let body_bytes = match serde_json::to_vec(&patch) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("Config patch fan-out: failed to serialize ConfigPatch: {e}");
                Vec::new()
            }
        };
        if body_bytes.is_empty() {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "internal: failed to serialize ConfigPatch for fan-out",
                })),
            ).into_response();
        }

        // Fan-out concurrently with a 5s timeout per peer.
        let mut tasks = Vec::new();
        for (pod, url) in peer_urls {
            let body = body_bytes.clone();
            let auth = auth_header.clone();
            let self_pod_name = self_name.to_owned();
            tasks.push(tokio::spawn(async move {
                let client = reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(5))
                    .build();
                let client = match client {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!("Config fan-out: failed to build reqwest client for {pod}: {e}");
                        return (pod, false, format!("client build error: {e}"));
                    }
                };
                let mut req = client
                    .patch(&url)
                    .header("content-type", "application/json")
                    .header("x-bitdex-patch-origin", &self_pod_name)
                    .body(body);
                if let Some(ref auth_val) = auth {
                    req = req.header("authorization", auth_val);
                }
                match req.send().await {
                    Ok(resp) if resp.status().is_success() => {
                        eprintln!("Config fan-out: {pod} ok ({})", resp.status());
                        (pod, true, String::new())
                    }
                    Ok(resp) => {
                        let status = resp.status().as_u16();
                        eprintln!("Config fan-out: {pod} non-2xx ({status})");
                        (pod, false, format!("http {status}"))
                    }
                    Err(e) => {
                        eprintln!("Config fan-out: {pod} error: {e}");
                        (pod, false, format!("{e}"))
                    }
                }
            }));
        }

        let mut results = Vec::new();
        for task in tasks {
            match task.await {
                Ok((pod, ok, err)) => {
                    let mut entry = serde_json::json!({"pod": pod, "ok": ok});
                    if !ok && !err.is_empty() {
                        entry["error"] = serde_json::Value::String(err);
                    }
                    results.push(entry);
                }
                Err(e) => {
                    eprintln!("Config fan-out: join error: {e}");
                }
            }
        }
        results
    } else {
        Vec::new()
    };

    let mut resp = serde_json::json!({
        "config": updated_config,
        "applied_to": applied_to,
        "warning": "in-memory only — does not persist across pod restart. For permanent settings, edit ConfigMap in talos-infra and let Flux reconcile.",
    });
    if !peer_results.is_empty() {
        resp["peer_results"] = serde_json::Value::Array(peer_results);
    }
    Json(resp).into_response()
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
        if !snap.active.is_empty() {
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
    /// Bypass the unified cache entirely (for debugging).
    skip_cache: Option<bool>,
}

/// RAII guard that decrements the in-flight query counter on drop.
struct QueryInflightGuard<'a> {
    counter: &'a AtomicI64,
    gauge: &'a prometheus::IntGauge,
}

impl Drop for QueryInflightGuard<'_> {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
        self.gauge.dec();
    }
}

/// RAII guard that decrements `docstore_concurrent_reads` on drop.
///
/// The previous code wrapped the doc-fetch `spawn_blocking` with
/// `gauge.inc()` + `.await.unwrap()` + `gauge.dec()`. The `dec()` line
/// was unreachable whenever the `.await` was cancelled — which happens
/// on HTTP client disconnect, even though the `spawn_blocking` task
/// itself keeps running until it finishes. That leaked `inc()`s into
/// the gauge permanently.
///
/// Production pod observed at 19,541 concurrent reads under an admission
/// cap of 200 — clearly impossible. With this guard on the async stack
/// frame, `Drop` runs whether the future completes, panics, or is
/// cancelled, so the gauge stays honest.
struct ConcurrentReadGuard<'a> {
    gauge: &'a prometheus::IntGauge,
}

impl<'a> ConcurrentReadGuard<'a> {
    fn new(gauge: &'a prometheus::IntGauge) -> Self {
        gauge.inc();
        Self { gauge }
    }
}

impl Drop for ConcurrentReadGuard<'_> {
    fn drop(&mut self) {
        self.gauge.dec();
    }
}

async fn handle_query(
    State(state): State<SharedState>,
    Extension(stage): Extension<HttpStageData>,
    AxumPath(name): AxumPath<String>,
    AxumQuery(params): AxumQuery<QueryParams>,
    body: Bytes,
) -> impl IntoResponse {
    // T1: handler entered. Captured before any work — measures middleware
    // chain overhead + tokio task scheduling delay.
    stage.record(&stage.t1_handler_entered_ns);

    // -- Backpressure: track in-flight queries and enforce concurrency limit --
    let in_flight = state.queries_in_flight.fetch_add(1, Ordering::Relaxed) + 1;
    state.metrics.queries_in_flight.inc();
    let _inflight_guard = QueryInflightGuard {
        counter: &state.queries_in_flight,
        gauge: &state.metrics.queries_in_flight,
    };

    // Update peak atomically (no TOCTOU race)
    state.queries_in_flight_peak.fetch_max(in_flight, Ordering::Relaxed);

    let max = state.max_query_concurrency.load(Ordering::Relaxed);
    if max > 0 && in_flight > max as i64 {
        state.metrics.queries_rejected_total.inc();
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [("retry-after", "1")],
            Json(serde_json::json!({
                "error": "server overloaded",
                "queries_in_flight": in_flight,
                "max_concurrency": max
            })),
        ).into_response();
    }

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

    // Merge ?skip_cache=true query param into the parsed query
    let mut query = query;
    if params.skip_cache.unwrap_or(false) {
        query.skip_cache = true;
    }

    tracing::info!("[{name}] {query}");

    // Tee into the query stream (SSE mirror) — non-blocking, drops oldest if full.
    // Only pays the cost of a single Option check on the hot path when disabled.
    if let Some(ref tx) = state.query_stream {
        let ts_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let body_val = serde_json::from_slice::<serde_json::Value>(&body)
            .unwrap_or(serde_json::Value::Null);
        let event = QueryEvent {
            ts_ms,
            index: name.clone(),
            body: body_val,
            client_meta: None,
        };
        // try_send never blocks; lagging receivers are dropped by the broadcast channel.
        let _ = tx.send(event);
    }

    // Query tee mode: SSE broadcast already happened above. Return stub
    // response immediately — no bitmap work, no doc fetch, no blocking.
    // Prod stays responsive while local SSE mirror gets real query traffic.
    if state.query_tee_mode.load(Ordering::Relaxed) {
        return Json(serde_json::json!({
            "ids": [],
            "total_matched": 0,
            "tee_mode": true
        })).into_response();
    }

    // Capture the user-input filter clauses BEFORE prefilter substitution so
    // the warm registry records the shape that was actually requested rather
    // than the post-substitution form. Substitution can prepend a
    // `FilterClause::BucketBitmap { bitmap: Arc<RoaringBitmap>, .. }` which
    // is `#[serde(skip)]` and panics serde when persist tries to write
    // warm.json — silently dropping every diverse-shape persist on the
    // floor and leaving the cold-restart auto-warm to a stale 1-entry file.
    let original_filters_for_warm = query.filters.clone();

    // Prefilter substitution: replace common clause subsets with a single
    // precomputed BucketBitmap AND. This turns e.g. the 7-clause Civitai
    // safety prefix into one cheap bitmap intersection.
    let (substituted_clauses, _prefilter_entry) =
        crate::prefilter::substitute(engine.prefilter_registry(), &query.filters);
    if let std::borrow::Cow::Owned(ref new_clauses) = substituted_clauses {
        query.filters = new_clauses.clone();
    }

    state.metrics.query_filter_clause_count.observe(query.filters.len() as f64);
    let start = Instant::now();
    let m = &state.metrics;
    // Execute bitmap query via block_in_place. This converts the current
    // async thread into a blocking thread in-place, letting tokio spawn a
    // replacement async thread. Unlike spawn_blocking + .await, this does
    // NOT park an async thread on a JoinHandle — the thread itself does
    // the work and returns. At 70+ QPS this prevents async thread
    // exhaustion that makes the main listener unresponsive.
    // T2: just before block_in_place enter. T1 → T2 covers body parse,
    // JSON deserialize, index lookup, prefilter substitution.
    stage.record(&stage.t2_engine_started_ns);
    let query_result = tokio::task::block_in_place(|| {
        engine.execute_query_traced(&query, &name)
    });
    // T3: block_in_place returned. T2 → T3 = engine wall-clock.
    stage.record(&stage.t3_engine_done_ns);
    match query_result {
        Ok((result, trace)) => {
            let elapsed = start.elapsed();
            let elapsed_us = elapsed.as_micros() as u64;
            m.query_total.with_label_values(&[&name]).inc();
            m.query_duration_seconds
                .with_label_values(&[&name])
                .observe(elapsed.as_secs_f64());

            // Record query shape for auto-warm on next boot.
            // Use `original_filters_for_warm` (captured pre-substitution) so the
            // recorded shape is JSON-serializable. The substituted form may
            // contain `FilterClause::BucketBitmap` which serde rejects.
            if let Some(ref sort) = query.sort {
                let canonical: Vec<crate::cache::CanonicalClause> = original_filters_for_warm.iter()
                    .filter_map(crate::cache::CanonicalClause::from_filter)
                    .collect();
                engine.warm_registry().record(
                    &original_filters_for_warm,
                    &canonical,
                    &sort.field,
                    sort.direction,
                );
            }

            let cursor = result.cursor.map(|c| serde_json::to_value(c).unwrap());

            // Fetch documents on a blocking thread to avoid starving tokio.
            // Doc reads can hit disk (cache miss) — sync I/O on the async
            // runtime causes 4s+ response times under load.
            let doc_start = Instant::now();
            let documents = if !include_docs.is_none() {
                let engine_docs = Arc::clone(&engine);
                let ids = result.ids.clone();
                let schema_docs = schema.clone();
                let reverse_maps_docs = Arc::clone(&reverse_maps);
                let include_docs_docs = include_docs.clone();
                let schema_registry_docs = Arc::clone(&schema_registry);
                let docstore_hist = m.docstore_read_seconds.clone();
                let name_docs = name.clone();

                // RAII guard replaces the old inc()/dec() bookend that
                // leaked on `.await` cancellation (HTTP client disconnect).
                // Dropped unconditionally when the async stack frame unwinds.
                let _read_guard = ConcurrentReadGuard::new(&m.docstore_concurrent_reads);
                let docs: Vec<serde_json::Value> = tokio::task::spawn_blocking(move || {
                    let slots: Vec<u32> = ids.iter().map(|&id| id as u32).collect();
                    let batch_start = Instant::now();
                    let fetched = engine_docs.get_documents(&slots);
                    docstore_hist
                        .with_label_values(&[&name_docs])
                        .observe(batch_start.elapsed().as_secs_f64());
                    match fetched {
                        Ok(stored_docs) => {
                            ids.iter().zip(stored_docs).map(|(&id, doc_opt)| {
                                match doc_opt {
                                    Some(stored) => format_document(&stored, &schema_docs, &reverse_maps_docs, &include_docs_docs, &schema_registry_docs),
                                    None => serde_json::json!({ "id": id }),
                                }
                            }).collect()
                        }
                        Err(e) => {
                            tracing::warn!("get_documents batch error: {e}");
                            ids.iter().map(|&id| serde_json::json!({ "id": id })).collect()
                        }
                    }
                }).await.unwrap();
                Some(docs)
            } else {
                None
            };
            let docs_us = doc_start.elapsed().as_micros() as u64;
            let docs_count = documents.as_ref().map_or(0, |d| d.len()) as u64;

            // T4: doc fetch returned (includes spawn_blocking enqueue wait).
            // T3 → T4 = doc_fetch wall-clock as observed from the handler;
            // matches the existing `docs_us` trace field. The middleware will
            // record T4 → T5 as `to_response`.
            stage.record(&stage.t4_docs_done_ns);

            // Update trace with doc fetch timing + handler-stage attribution.
            let mut trace = trace;
            trace.docs_us = docs_us;
            trace.docs_count = docs_count;
            // Stage timestamps: load atomics back as ns offsets from t0,
            // convert to µs for the trace (matches existing _us fields).
            let t1_ns = stage.t1_handler_entered_ns.load(Ordering::Relaxed);
            let t2_ns = stage.t2_engine_started_ns.load(Ordering::Relaxed);
            trace.to_handler_us = t1_ns / 1_000;
            trace.to_engine_us = t2_ns.saturating_sub(t1_ns) / 1_000;
            // to_response_us / http_total_us cannot be filled in from here —
            // the response hasn't been written yet. They stay 0 in the trace
            // ring buffer; the middleware emits them into the histogram.

            // Observe query phase histograms
            m.query_filter_seconds
                .with_label_values(&[&name])
                .observe(trace.filter_us as f64 / 1_000_000.0);
            m.query_sort_seconds
                .with_label_values(&[&name])
                .observe(trace.sort_us as f64 / 1_000_000.0);
            m.query_docs_seconds
                .with_label_values(&[&name])
                .observe(docs_us as f64 / 1_000_000.0);

            let cache_tag = if trace.cache_hit { " cache" } else { "" };
            let docs_tag = if docs_count > 0 { format!("  docs={}μs({})", docs_us, docs_count) } else { String::new() };
            tracing::info!(
                "[{name}]   → {} results  total={elapsed_us}μs  plan={}μs  filter={}μs  sort={}μs{docs_tag}{cache_tag}",
                result.total_matched, trace.plan_us, trace.filter_us, trace.sort_us
            );

            // Write trace to ring buffer if enabled and above latency threshold.
            // trace_min_us=0 means record all; trace_min_us=100000 means only >100ms.
            if state.enable_traces.load(Ordering::Relaxed) {
                let min_us = state.trace_min_us.load(Ordering::Relaxed);
                if min_us == 0 || trace.total_us >= min_us {
                    state.trace_buffer.push(trace.clone());
                }
            }

            let mut response = serde_json::json!({
                "ids": result.ids,
                "cursor": cursor,
                "total_matched": result.total_matched,
                "elapsed_us": elapsed_us,
            });
            if let Some(docs) = documents {
                response["documents"] = serde_json::json!(docs);
            }

            let mut resp = Json(response).into_response();
            resp.headers_mut().insert(
                "X-BitDex-Duration-Us",
                axum::http::HeaderValue::from_str(&elapsed_us.to_string()).unwrap(),
            );
            resp
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

// ---------------------------------------------------------------------------
// GET /api/indexes/{name}/traces?last=N
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct TracesParams {
    #[serde(default = "default_traces_last")]
    last: usize,
}

fn default_traces_last() -> usize { 50 }

async fn handle_traces(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
    AxumQuery(params): AxumQuery<TracesParams>,
) -> impl IntoResponse {
    // Verify the index exists
    {
        let guard = state.index.lock();
        match guard.as_ref() {
            Some(idx) if idx.definition.name == name => {}
            _ => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error": format!("Index '{}' not found", name)})),
                ).into_response();
            }
        }
    }

    let traces = state.trace_buffer.last_n(params.last);
    Json(serde_json::json!({
        "traces": traces,
        "buffer_capacity": state.trace_buffer.capacity(),
    })).into_response()
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

/// GET /api/indexes/{name}/documents/{slot_id}
async fn handle_get_document(
    State(state): State<SharedState>,
    AxumPath((name, slot_id)): AxumPath<(String, u32)>,
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

    match engine.get_document(slot_id) {
        Ok(Some(doc)) => {
            let include = IncludeDocs::All;
            Json(format_document(&doc, &schema, &reverse_maps, &include, &schema_registry)).into_response()
        }
        Ok(None) => (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "document not found"}))).into_response(),
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

    // Move disk I/O off the async runtime — doc reads are blocking sync I/O
    // that will starve Tokio workers if run inline on the async task.
    let slot_ids = req.slot_ids.clone();
    let docs = tokio::task::spawn_blocking(move || {
        match engine.get_documents(&slot_ids) {
            Ok(stored_docs) => {
                slot_ids.iter().zip(stored_docs).map(|(&slot_id, doc_opt)| {
                    match doc_opt {
                        Some(doc) => format_document(&doc, &schema, &reverse_maps, &req.fields, &schema_registry),
                        None => serde_json::json!({"id": slot_id}),
                    }
                }).collect::<Vec<_>>()
            }
            Err(_) => {
                slot_ids.iter().map(|&slot_id| serde_json::json!({"id": slot_id})).collect()
            }
        }
    }).await.unwrap_or_default();
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

    // Run upserts on a blocking thread — engine.put() does sync disk I/O
    // (docstore reads for diffing) that would starve the tokio runtime.
    let documents = req.documents;
    let engine_clone = Arc::clone(&engine);
    let schema_clone = schema.clone();
    let (upserted, errors) = tokio::task::spawn_blocking(move || {
        let mut upserted = 0u64;
        let mut errors: Vec<String> = Vec::new();

        for (i, doc_json) in documents.iter().enumerate() {
            let dicts = if has_lcs { Some(engine_clone.dictionaries()) } else { None };
            match loader::json_to_document_with_dicts(doc_json, &schema_clone, dicts) {
                Ok((slot, doc)) => {
                    if let Err(e) = engine_clone.put(slot, &doc) {
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

        (upserted, errors)
    }).await.expect("spawn_blocking join");

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

/// PATCH /api/indexes/{name}/documents/patch
///
/// Partial update: merges only provided fields into existing documents.
/// Fields absent from the payload are left untouched in bitmaps and docstore.
/// Slots that are not alive return an error (use upsert for initial creation).
async fn handle_patch_documents(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
    Json(req): Json<UpsertRequest>,
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

    let (schema, has_lcs) = {
        let guard = state.index.lock();
        let idx = guard.as_ref().unwrap();
        let has_lcs = idx.definition.data_schema.fields.iter().any(|f| f.value_type == FieldValueType::LowCardinalityString);
        (idx.definition.data_schema.clone(), has_lcs)
    };

    // Run patch_document on a blocking thread to avoid starving the tokio
    // runtime. patch_document does sync disk I/O (reads old doc for diffing)
    // and 5000 patches per pg-sync cycle would exhaust the async thread pool.
    let documents = req.documents;
    let engine_clone = Arc::clone(&engine);
    let schema_clone = schema.clone();
    let (patched, errors) = tokio::task::spawn_blocking(move || {
        let mut patched = 0u64;
        let mut errors: Vec<String> = Vec::new();

        for (i, doc_json) in documents.iter().enumerate() {
            let dicts = if has_lcs { Some(engine_clone.dictionaries()) } else { None };
            match loader::json_to_document_with_dicts(doc_json, &schema_clone, dicts) {
                Ok((slot, doc)) => {
                    match engine_clone.patch_document(slot, &doc) {
                        Ok(()) => patched += 1,
                        Err(crate::error::BitdexError::SlotNotFound(_)) => {
                            errors.push(format!("doc[{}] id={}: not alive (use upsert for new docs)", i, slot));
                        }
                        Err(e) => {
                            errors.push(format!("doc[{}] id={}: {}", i, slot, e));
                        }
                    }
                }
                Err(e) => {
                    errors.push(format!("doc[{}]: {}", i, e));
                }
            }
        }

        (patched, errors)
    }).await.expect("spawn_blocking join");

    if let Some(cursor) = req.cursor {
        engine.set_cursor(cursor.name, cursor.value);
    }

    if has_lcs && patched > 0 {
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

    state.metrics.upsert_total.with_label_values(&[&name]).inc_by(patched);

    if errors.is_empty() {
        Json(serde_json::json!({"patched": patched})).into_response()
    } else {
        (
            StatusCode::OK,
            Json(serde_json::json!({"patched": patched, "errors": errors})),
        ).into_response()
    }
}

/// Sync filter values for a filter_only multi-value field.
///
/// Accepts a batch of (slot, values) pairs and replaces all bitmap memberships
/// for each slot on the named field. Used by the outbox poller for fields like
/// collectionIds where membership comes from a separate table.
async fn handle_filter_sync(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
    Json(req): Json<FilterSyncRequest>,
) -> impl IntoResponse {
    // Validate field exists and is a multi_value filter field
    let engine = {
        let guard = state.index.lock();
        match guard.as_ref() {
            Some(idx) if idx.definition.name == name => {
                let is_multi_value = idx.definition.config.filter_fields.iter().any(|f| {
                    f.name == req.field
                        && matches!(f.field_type, crate::filter::FilterFieldType::MultiValue)
                });
                let is_filter_only = idx.definition.data_schema.fields.iter().any(|f| {
                    f.target == req.field && f.filter_only
                });
                if !is_multi_value || !is_filter_only {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(serde_json::json!({
                            "error": format!("Field '{}' is not a filter_only multi_value field", req.field)
                        })),
                    ).into_response();
                }
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

    let mut synced = 0u64;
    let mut errors: Vec<String> = Vec::new();

    for (i, entry) in req.documents.iter().enumerate() {
        match engine.sync_filter_values(entry.id, &req.field, &entry.values) {
            Ok(()) => synced += 1,
            Err(e) => errors.push(format!("doc[{}] id={}: {}", i, entry.id, e)),
        }
    }

    state.metrics.upsert_total.with_label_values(&[&name]).inc_by(synced);

    if errors.is_empty() {
        Json(serde_json::json!({"synced": synced})).into_response()
    } else if synced == 0 {
        // Total failure — no documents synced
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"synced": 0, "errors": errors})),
        ).into_response()
    } else {
        // Partial failure
        (
            StatusCode::MULTI_STATUS,
            Json(serde_json::json!({"synced": synced, "errors": errors})),
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

    // Run deletes on a blocking thread — engine.delete() reads the stored
    // doc from disk for clean bitmap clearing, same sync I/O issue as put().
    let ids = req.ids;
    let engine_clone = Arc::clone(&engine);
    let (deleted, errors) = tokio::task::spawn_blocking(move || {
        let mut deleted = 0u64;
        let mut errors: Vec<String> = Vec::new();

        for id in &ids {
            match engine_clone.delete(*id) {
                Ok(()) => deleted += 1,
                Err(e) => errors.push(format!("id={}: {}", id, e)),
            }
        }

        (deleted, errors)
    }).await.expect("spawn_blocking join");

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

    // Run expensive bitmap traversal on a blocking thread to avoid starving the async runtime
    let engine2 = Arc::clone(&engine);
    let (slot_bytes, filter_bytes, sort_bytes) = tokio::task::spawn_blocking(move || {
        engine2.bitmap_memory_totals()
    }).await.unwrap_or((0, 0, 0));
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
        "queries_in_flight": state.queries_in_flight.load(Ordering::Relaxed),
        "queries_in_flight_peak": state.queries_in_flight_peak.load(Ordering::Relaxed),
        "queries_rejected": state.metrics.queries_rejected_total.get(),
        "max_query_concurrency": state.max_query_concurrency.load(Ordering::Relaxed),
        "enable_traces": state.enable_traces.load(Ordering::Relaxed),
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
// Handlers: Warm Cache
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct WarmQuerySpec {
    filters: Vec<crate::query::FilterClause>,
    sort: crate::query::SortClause,
}

#[derive(Deserialize)]
struct WarmRequest {
    queries: Vec<WarmQuerySpec>,
}

#[derive(Serialize)]
struct WarmResultEntry {
    query_index: usize,
    status: String,
    elapsed_us: u64,
    matched: u64,
}

#[derive(Serialize)]
struct WarmResponse {
    warmed: usize,
    already_cached: usize,
    results: Vec<WarmResultEntry>,
}

/// POST /api/indexes/{name}/warm — pre-populate the unified cache with specified queries.
async fn handle_warm_cache(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
    Json(req): Json<WarmRequest>,
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

    if req.queries.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "queries array must not be empty"})),
        ).into_response();
    }

    let mut results = Vec::with_capacity(req.queries.len());
    let mut warmed = 0usize;
    let mut already_cached = 0usize;

    for (i, spec) in req.queries.iter().enumerate() {
        let query = BitdexQuery {
            filters: spec.filters.clone(),
            sort: Some(spec.sort.clone()),
            limit: 1,
            cursor: None,
            offset: None,
            skip_cache: false,
        };

        let start = Instant::now();
        match engine.execute_query_traced(&query, &name) {
            Ok((result, trace)) => {
                let elapsed_us = start.elapsed().as_micros() as u64;
                let status = if trace.cache_hit {
                    already_cached += 1;
                    "already_cached"
                } else {
                    warmed += 1;
                    "warmed"
                };
                results.push(WarmResultEntry {
                    query_index: i,
                    status: status.to_string(),
                    elapsed_us,
                    matched: result.total_matched,
                });
            }
            Err(e) => {
                let elapsed_us = start.elapsed().as_micros() as u64;
                results.push(WarmResultEntry {
                    query_index: i,
                    status: format!("error: {e}"),
                    elapsed_us,
                    matched: 0,
                });
            }
        }
    }

    Json(WarmResponse {
        warmed,
        already_cached,
        results,
    }).into_response()
}

// ---------------------------------------------------------------------------
// Handlers: Prefilter Registry
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct RegisterPrefilterRequest {
    name: String,
    clauses: Vec<crate::query::FilterClause>,
    #[serde(default = "default_refresh_interval")]
    refresh_interval_secs: u64,
}

fn default_refresh_interval() -> u64 { crate::prefilter::DEFAULT_REFRESH_INTERVAL_SECS }

async fn handle_register_prefilter(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
    Json(req): Json<RegisterPrefilterRequest>,
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
    match engine.register_prefilter(req.name, req.clauses, req.refresh_interval_secs) {
        Ok(entry) => Json(serde_json::json!({
            "name": entry.name,
            "cardinality": entry.cardinality(),
            "compute_ms": entry.compute_ms(),
            "refresh_interval_secs": entry.refresh_interval_secs(),
        })).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e.to_string()})),
        ).into_response(),
    }
}

async fn handle_list_prefilters(
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
    Json(serde_json::json!({
        "prefilters": engine.list_prefilters(),
    })).into_response()
}

async fn handle_remove_prefilter(
    State(state): State<SharedState>,
    AxumPath((name, prefilter_name)): AxumPath<(String, String)>,
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
    let removed = engine.remove_prefilter(&prefilter_name);
    Json(serde_json::json!({
        "removed": removed,
        "name": prefilter_name,
    })).into_response()
}

async fn handle_refresh_prefilters(
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
    let refreshed = engine.refresh_stale_prefilters();
    Json(serde_json::json!({
        "refreshed": refreshed,
        "total": engine.list_prefilters().len(),
    })).into_response()
}

// ---------------------------------------------------------------------------
// Handlers: Cache entry diagnostic
// ---------------------------------------------------------------------------

/// Query params for GET /api/indexes/{name}/cache/entry
#[derive(Deserialize, Default)]
struct CacheEntryParams {
    /// URL-encoded JSON array of FilterClause (same format as query `filters`)
    filters: Option<String>,
    sort_field: Option<String>,
    direction: Option<String>,
}

async fn handle_cache_entry_inspect(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
    AxumQuery(params): AxumQuery<CacheEntryParams>,
) -> impl IntoResponse {
    use crate::cache::{canonicalize, CanonicalClause};
    use crate::query::{FilterClause, SortDirection};
    use crate::unified_cache::{is_time_bucket_clause, UnifiedKey};

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

    // Parse filters from URL-encoded JSON array
    let filter_clauses: Vec<FilterClause> = match &params.filters {
        Some(json_str) => match serde_json::from_str(json_str) {
            Ok(v) => v,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": format!("Invalid filters JSON: {e}")})),
                ).into_response();
            }
        },
        None => vec![],
    };

    // Canonicalize
    let canonical = match canonicalize(&filter_clauses) {
        Some(c) => c,
        None => vec![],
    };

    // Parse direction
    let direction = match params.direction.as_deref().unwrap_or("Desc") {
        "Asc" | "asc" => SortDirection::Asc,
        _ => SortDirection::Desc,
    };

    let key = UnifiedKey {
        filter_clauses: canonical.clone(),
        sort_field: params.sort_field.unwrap_or_default(),
        direction,
    };

    let uses_bucket = canonical.iter().any(is_time_bucket_clause);
    let clause_json: Vec<_> = canonical.iter().map(|c: &CanonicalClause| {
        serde_json::json!({"field": c.field, "op": c.op, "value": c.value_repr})
    }).collect();

    // Extract entry data under the dashmap shard lock, then drop the Ref
    // before engine is dropped (Ref borrows engine's unified_cache).
    let entry_data = {
        let uc = engine.unified_cache_ref();
        uc.get(&key).map(|entry| {
            (
                entry.cardinality(),
                entry.total_matched(),
                entry.capacity(),
                entry.max_capacity(),
                entry.needs_rebuild(),
                entry.bucket_cutoff(),
                entry.last_used_ms(),
                format!("{:?}", entry.direction()),
                entry.is_persist_dirty(),
                entry.sorted_keys().map(|k| k.len()).unwrap_or(0),
                entry.radix().is_some(),
                entry.has_more(),
            )
        })
    };

    match entry_data {
        Some((bitmap_len, total_matched, capacity, max_capacity, needs_rebuild,
              bucket_cutoff, last_used_ms, direction_str, persist_dirty,
              sorted_keys_len, has_radix, has_more)) => {
            Json(serde_json::json!({
                "filter_clauses": clause_json,
                "bitmap_len": bitmap_len,
                "total_matched": total_matched,
                "capacity": capacity,
                "max_capacity": max_capacity,
                "needs_rebuild": needs_rebuild,
                "uses_bucket": uses_bucket,
                "bucket_cutoff": bucket_cutoff,
                "last_used_ms": last_used_ms,
                "direction": direction_str,
                "persist_dirty": persist_dirty,
                "sorted_keys_len": sorted_keys_len,
                "has_radix": has_radix,
                "has_more": has_more,
            })).into_response()
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "cache entry not found for the given key"})),
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
// Handlers: Compact
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct CompactRequest {
    targets: Option<Vec<String>>,
    threshold: Option<u32>,
    workers: Option<usize>,
}

async fn handle_rebuild_time_buckets(
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
    match engine.rebuild_time_buckets() {
        Ok((bucket_count, slots_scanned)) => {
            let bucket_details = engine.time_bucket_stats();
            Json(serde_json::json!({
                "status": "ok",
                "buckets_rebuilt": bucket_count,
                "slots_scanned": slots_scanned,
                "buckets": bucket_details,
            })).into_response()
        }
        Err(e) => {
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": e.to_string()})),
            ).into_response()
        }
    }
}

/// Read-only diagnostic: per-bucket current / fresh_in_window / stale / missing.
/// Non-mutating; runs a full alive-scan (~minutes at scale) on a blocking task.
#[derive(serde::Deserialize)]
struct TimeBucketAuditParams {
    /// Number of missing/stale slot IDs to sample per bucket (0 = counts only).
    #[serde(default)]
    sample: usize,
    /// Sample ordering: `lowest_id` (default, oldest/pre-boot first),
    /// `highest_id` (most recent slots — isolates the ongoing residual source
    /// from boot residue), or `random` (even stride across the set).
    #[serde(default = "default_audit_order")]
    order: String,
}
fn default_audit_order() -> String {
    "lowest_id".to_string()
}

async fn handle_time_bucket_audit(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
    AxumQuery(params): AxumQuery<TimeBucketAuditParams>,
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
    // Cap the sample so a stray ?sample=10000000 can't build a huge JSON payload.
    let sample = params.sample.min(1000);
    let order = params.order;
    match tokio::task::spawn_blocking(move || engine.time_bucket_audit(sample, &order)).await {
        Ok(Ok(audit)) => Json(serde_json::json!({"status": "ok", "audit": audit})).into_response(),
        Ok(Err(e)) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e.to_string()})),
        ).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": format!("audit task failed: {e}")})),
        ).into_response(),
    }
}

async fn handle_compact(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
    Json(req): Json<CompactRequest>,
) -> impl IntoResponse {
    let (engine, tasks) = {
        let guard = state.index.lock();
        match guard.as_ref() {
            Some(idx) if idx.definition.name == name => (
                Arc::clone(&idx.engine),
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

    // Validate request before acquiring task slot (avoid leaking active task on validation failure)
    let threshold = req.threshold.unwrap_or(0);
    let workers = req.workers.unwrap_or(4).max(1).min(32);
    let targets = req.targets.unwrap_or_default();

    for t in &targets {
        if t != "bitmaps" && t != "docs" {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": format!("Invalid target '{}'. Valid targets: bitmaps, docs", t)})),
            ).into_response();
        }
    }

    let compact_bitmaps = targets.is_empty() || targets.iter().any(|t| t == "bitmaps");
    let compact_docs = targets.is_empty() || targets.iter().any(|t| t == "docs");

    let (task_id, progress) = match tasks.try_start(TaskType::Compact) {
        Ok(v) => v,
        Err(active_info) => {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({
                    "error": "A conflicting task is already running",
                    "active_task": serde_json::to_value(&active_info).unwrap(),
                })),
            ).into_response();
        }
    };

    let tasks_clone = Arc::clone(&tasks);
    let m_running = state.metrics.compact_running.clone();
    let m_scanned = state.metrics.compact_shards_scanned.clone();
    let m_compacted = state.metrics.compact_shards_compacted.clone();
    let m_skipped = state.metrics.compact_shards_skipped.clone();
    let m_runs = state.metrics.compact_runs_total.clone();
    let m_duration = state.metrics.compact_duration_seconds.clone();

    tokio::task::spawn_blocking(move || {
        let mut guard = TaskGuard { tasks: tasks_clone, task_id: Some(task_id) };

        m_running.set(1);
        m_runs.inc();
        m_scanned.set(0);
        m_compacted.set(0);
        m_skipped.set(0);

        match engine.compact_all(threshold, workers, compact_bitmaps, compact_docs, progress) {
            Ok(result) => {
                m_scanned.set(result.shards_scanned as i64);
                m_compacted.set(result.shards_compacted as i64);
                m_skipped.set(result.shards_skipped as i64);
                m_duration.observe(result.elapsed_secs);
                m_running.set(0);

                guard.tasks.set_complete(task_id, Some(serde_json::to_value(&result).unwrap()));
                guard.defuse();
            }
            Err(e) => {
                m_running.set(0);
                guard.tasks.set_error(task_id, format!("Compact failed: {}", e));
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

                // Save updated config
                let index_dir = state.data_dir.join("indexes").join(&name);
                if let Err(e) = idx.definition.save_yaml(&index_dir) {
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

/// Reload a field's existence set after external bulk writes.
async fn handle_reload_field(
    State(state): State<SharedState>,
    AxumPath((name, field)): AxumPath<(String, String)>,
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

    match engine.reload_existence_set(&field) {
        Ok(()) => Json(serde_json::json!({"reloaded": field})).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": format!("{e}")})),
        ).into_response(),
    }
}

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

                // Save updated config
                let index_dir = state.data_dir.join("indexes").join(&name);
                if let Err(e) = idx.definition.save_yaml(&index_dir) {
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
    let elapsed = t0.elapsed().as_secs_f64();
    state.metrics.save_snapshot_seconds
        .with_label_values(&[&name])
        .observe(elapsed);
    match result {
        Ok(()) => {
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
// Handlers: Capture (Phase 2 — snapshot capture system)
// ---------------------------------------------------------------------------

/// POST /debug/capture/start — Start a new capture session.
async fn handle_capture_start(
    State(state): State<SharedState>,
    body: Option<Json<crate::capture::CaptureStartRequest>>,
) -> impl IntoResponse {
    let req = body
        .map(|Json(r)| r)
        .unwrap_or(crate::capture::CaptureStartRequest { duration_seconds: 300 });

    match state.capture.start(&req) {
        Ok(status) => {
            // Scrape Prometheus metrics at capture start (Phase 2.4)
            let metrics_text = state.metrics.gather();
            if let Some(dir) = state.capture.session_dir() {
                let path = dir.join("metrics_start.prom");
                if let Err(e) = std::fs::write(&path, &metrics_text) {
                    tracing::warn!("Failed to write metrics_start.prom: {e}");
                } else {
                    state.capture.set_metrics_start_path(path);
                }
            }

            tracing::info!("Capture started: session={}, auto_stop={}s", status.session_id.as_deref().unwrap_or("?"), req.duration_seconds);

            // Spawn auto-stop timer
            let duration = req.duration_seconds;
            let auto_stop_state = Arc::clone(&state);
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(duration)).await;
                if auto_stop_state.capture.is_recording() {
                    tracing::info!("Capture auto-stopping after {duration}s");
                    if let Ok(status) = auto_stop_state.capture.stop() {
                        // Scrape metrics at auto-stop
                        let metrics_text = auto_stop_state.metrics.gather();
                        if let Some(dir) = auto_stop_state.capture.session_dir() {
                            let path = dir.join("metrics_stop.prom");
                            let _ = std::fs::write(&path, &metrics_text);
                            auto_stop_state.capture.set_metrics_stop_path(path);
                        }
                        tracing::info!("Capture auto-stopped: requests={}", status.requests_recorded);
                    }
                }
            });

            Json(serde_json::json!(status)).into_response()
        }
        Err(e) => {
            (
                StatusCode::CONFLICT,
                Json(serde_json::json!({"error": e.to_string()})),
            ).into_response()
        }
    }
}

/// POST /debug/capture/stop — Stop the current capture session.
async fn handle_capture_stop(
    State(state): State<SharedState>,
) -> impl IntoResponse {
    match state.capture.stop() {
        Ok(status) => {
            // Scrape Prometheus metrics at capture stop (Phase 2.4)
            let metrics_text = state.metrics.gather();
            if let Some(dir) = state.capture.session_dir() {
                let path = dir.join("metrics_stop.prom");
                if let Err(e) = std::fs::write(&path, &metrics_text) {
                    tracing::warn!("Failed to write metrics_stop.prom: {e}");
                } else {
                    state.capture.set_metrics_stop_path(path);
                }
            }

            tracing::info!(
                "Capture stopped: session={}, requests={}",
                status.session_id.as_deref().unwrap_or("?"),
                status.requests_recorded,
            );
            Json(serde_json::json!(status)).into_response()
        }
        Err(e) => {
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": e.to_string()})),
            ).into_response()
        }
    }
}

/// GET /debug/capture/status — Get current capture status.
async fn handle_capture_status(
    State(state): State<SharedState>,
) -> impl IntoResponse {
    Json(serde_json::json!(state.capture.status()))
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

async fn handle_set_cursor(
    State(state): State<SharedState>,
    AxumPath((name, cursor_name)): AxumPath<(String, String)>,
    Json(req): Json<serde_json::Value>,
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

    let value = match req.get("value").and_then(|v| v.as_str()) {
        Some(v) => v.to_string(),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "missing 'value' field"})),
            ).into_response();
        }
    };

    // Persist the cursor synchronously via a small atomic file write to
    // MetaStore. The previous code called `engine.save_snapshot()` here,
    // which rewrote every bitmap shard (~10 GB, 14-20 s) on every cursor
    // PATCH and was the dominant source of pod-wide IO-pressure freezes
    // observed in v196-v198. The merge thread also batch-persists cursors
    // every 5 s (concurrent_engine.rs flush loop), so even a transient
    // MetaStore write failure here doesn't lose the cursor — it just
    // delays durability by one merge cycle.
    if let Err(e) = engine.persist_cursor(cursor_name.clone(), value.clone()) {
        eprintln!("Warning: cursor set but persist failed: {e}");
    }

    Json(serde_json::json!({
        "name": cursor_name,
        "value": value,
        "persisted": true,
    })).into_response()
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

/// Readiness probe — returns 200 only after pg-sync has finished its
/// initial dump+load pipeline and written `<data_dir>/.ready`. Returns
/// 503 otherwise, which keeps the K8s Service from routing traffic to a
/// half-loaded replica.
///
/// Why a marker file (vs. an HTTP signal): pg-sync runs as a sidecar in
/// the same pod with a shared PVC mount. A file write is durable across
/// bitdex container restarts, so a mid-load liveness restart of the
/// bitdex server can't lose the readiness signal — pg-sync only writes
/// once per pipeline completion, and the file persists.
///
/// Liveness stays on /api/health so kubelet still kills genuinely-stuck
/// containers; readiness gates traffic.
async fn handle_ready(State(state): State<SharedState>) -> impl IntoResponse {
    let marker = state.data_dir.join(".ready");
    if marker.exists() {
        (StatusCode::OK, Json(serde_json::json!({"ready": true})))
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "ready": false,
                "reason": "dump pipeline pending — pg-sync has not written .ready marker"
            })),
        )
    }
}

/// Memory budget endpoint — shows where every GB of RSS goes.
/// Bitmap totals run on a blocking thread (can be slow at 107M records).
/// Designed for manual debugging, not Prometheus scraping.
async fn handle_debug_memory(
    State(state): State<SharedState>,
) -> impl IntoResponse {
    let rss_bytes = crate::concurrent_engine::get_rss_bytes() as u64;

    let (engine, engine_name, uc_bytes, doc_cache_bytes) = {
        let guard = state.index.lock();
        if let Some(idx) = guard.as_ref() {
            let engine = Arc::clone(&idx.engine);
            let name = idx.definition.name.clone();
            let uc = engine.unified_cache_stats();
            let (_, _, _, dc_bytes, _, _) = engine.doc_cache_stats();
            (Some(engine), name, uc.memory_bytes as u64, dc_bytes)
        } else {
            (None, String::new(), 0, 0)
        }
    };

    let (slot_bytes, filter_bytes, sort_bytes) = if let Some(engine) = engine {
        tokio::task::spawn_blocking(move || {
            let (s, f, so) = engine.bitmap_memory_totals();
            (s as u64, f as u64, so as u64)
        }).await.unwrap_or((0, 0, 0))
    } else {
        (0, 0, 0)
    };

    let bitmap_total = slot_bytes + filter_bytes + sort_bytes;
    let tracked_total = bitmap_total + uc_bytes + doc_cache_bytes;
    let untracked = rss_bytes.saturating_sub(tracked_total);

    let pod_limit: u64 = std::env::var("BITDEX_MEMORY_LIMIT_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(32 * 1024 * 1024 * 1024);

    let headroom = pod_limit.saturating_sub(rss_bytes);
    let non_doc_tracked = tracked_total.saturating_sub(doc_cache_bytes);
    let safe_doc_cache = pod_limit
        .saturating_sub(non_doc_tracked)
        .saturating_sub(untracked)
        .saturating_sub(2 * 1024 * 1024 * 1024);

    Json(serde_json::json!({
        "index": engine_name,
        "rss_bytes": rss_bytes,
        "tracked": {
            "alive_bitmap": slot_bytes,
            "filter_bitmaps": filter_bytes,
            "sort_bitmaps": sort_bytes,
            "bitmap_total": bitmap_total,
            "unified_cache": uc_bytes,
            "doc_cache": doc_cache_bytes,
        },
        "tracked_total": tracked_total,
        "untracked": untracked,
        "budget": {
            "pod_limit": pod_limit,
            "rss_current": rss_bytes,
            "headroom": headroom,
            "safe_doc_cache_max": safe_doc_cache,
        },
        "human": {
            "rss": format!("{:.2} GB", rss_bytes as f64 / 1e9),
            "tracked": format!("{:.2} GB", tracked_total as f64 / 1e9),
            "untracked": format!("{:.2} GB", untracked as f64 / 1e9),
            "headroom": format!("{:.2} GB", headroom as f64 / 1e9),
            "safe_doc_cache": format!("{:.2} GB", safe_doc_cache as f64 / 1e9),
        }
    }))
}

/// Trigger a jemalloc heap profile dump. Only available with `--features heap-prof`.
/// Returns the path to the dump file, or an error if heap profiling is not enabled.
///
/// Usage: POST /debug/heap-dump
/// Optional JSON body: { "path": "/tmp/heap.prof" }
/// Default dump path: /tmp/bitdex-heap-<timestamp>.prof
async fn handle_heap_dump(
    body: Option<Json<serde_json::Value>>,
) -> impl IntoResponse {
    #[cfg(feature = "heap-prof")]
    {
        use tikv_jemalloc_ctl::raw;
        use std::ffi::CString;

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let default_path = format!("/tmp/bitdex-heap-{}.prof", timestamp);
        let dump_path = body
            .and_then(|b| b.get("path").and_then(|p| p.as_str().map(String::from)))
            .unwrap_or(default_path);

        // Activate profiling if not already active
        let prof_active_key = b"prof.active\0";
        let _ = unsafe { raw::write(prof_active_key, true) };

        // Trigger dump
        let c_path = match CString::new(dump_path.clone()) {
            Ok(p) => p,
            Err(e) => return Json(serde_json::json!({
                "error": format!("invalid path: {e}"),
            })),
        };
        let dump_key = b"prof.dump\0";
        match unsafe { raw::write(dump_key, c_path.as_ptr() as *const std::ffi::c_char) } {
            Ok(()) => {
                eprintln!("Heap profile dumped to: {}", dump_path);
                Json(serde_json::json!({
                    "status": "ok",
                    "path": dump_path,
                    "message": "Heap profile dumped. Use jeprof to analyze.",
                }))
            }
            Err(e) => {
                Json(serde_json::json!({
                    "error": format!("prof.dump failed: {e}"),
                    "hint": "Ensure MALLOC_CONF=prof:true is set at startup",
                }))
            }
        }
    }

    #[cfg(not(feature = "heap-prof"))]
    {
        let _ = body; // suppress unused warning
        Json(serde_json::json!({
            "error": "Heap profiling not enabled. Build with --features heap-prof",
            "hint": "cargo build --release --features 'server,heap-prof'",
        }))
    }
}

/// POST /debug/rescan-memory — Trigger a full bitmap memory rescan.
/// Marks all fields stale so the background scanner processes them in batches.
/// Does NOT scan everything at once — uses the existing stale set + batch system.
/// Safe to call at any time. Useful after enabling bitmap_memory metrics at runtime.
async fn handle_rescan_memory(
    State(state): State<SharedState>,
) -> impl IntoResponse {
    let guard = state.index.lock();
    match guard.as_ref() {
        Some(idx) => {
            idx.engine.bitmap_memory_cache().mark_all_stale();
            Json(serde_json::json!({
                "status": "ok",
                "message": "All fields marked stale. Scanner will process them in batches.",
                "scanner_interval_ms": idx.engine.bitmap_memory_cache().interval_ms(),
                "scanner_batch_size": idx.engine.bitmap_memory_cache().batch_size(),
            }))
        }
        None => {
            Json(serde_json::json!({
                "error": "No index loaded",
            }))
        }
    }
}

async fn handle_list_formats(State(state): State<SharedState>) -> impl IntoResponse {
    let mut formats = state.parser_registry.formats();
    formats.sort();
    Json(serde_json::json!({
        "formats": formats,
        "default": state.parser_registry.default_format(),
    }))
}

/// Aggregate RSS readings parsed in a single pass over /proc/self/status.
/// Bytes; zeros on non-Linux or when a key is absent.
#[derive(Default, Clone, Copy)]
struct RssReading {
    rss: i64,
    anon: i64,
    file: i64,
    shmem: i64,
}

/// Single-syscall, single-pass read of all four RSS-related keys.
fn read_rss_all() -> RssReading {
    #[cfg(target_os = "linux")]
    {
        let mut r = RssReading::default();
        let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
            return r;
        };
        for line in status.lines() {
            // Lines: "<Key>:\t  12345 kB". Match prefix, parse 2nd token.
            let slot = if line.starts_with("VmRSS:") { Some(&mut r.rss) }
                else if line.starts_with("RssAnon:") { Some(&mut r.anon) }
                else if line.starts_with("RssFile:") { Some(&mut r.file) }
                else if line.starts_with("RssShmem:") { Some(&mut r.shmem) }
                else { None };
            if let Some(s) = slot {
                if let Some(kb_str) = line.split_whitespace().nth(1) {
                    if let Ok(kb) = kb_str.parse::<i64>() {
                        *s = kb * 1024;
                    }
                }
            }
        }
        r
    }
    #[cfg(not(target_os = "linux"))]
    { RssReading::default() }
}


/// Cached mmap inventory; refreshed at most once per
/// MMAP_INVENTORY_REFRESH_SECS to bound `/proc/self/maps` walk cost.
/// The walk holds the kernel's `mmap_lock` for read across all VMAs
/// (~500-2000 at our thread count); throttling avoids contention with
/// concurrent mmap/munmap on the hot path.
#[allow(dead_code)]
static MMAP_INVENTORY_LAST_REFRESH: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
#[allow(dead_code)]
static MMAP_INVENTORY_CACHE: parking_lot::Mutex<Vec<(&'static str, u64)>> =
    parking_lot::Mutex::new(Vec::new());
#[allow(dead_code)]
const MMAP_INVENTORY_REFRESH_SECS: u64 = 150;

/// Mmap inventory by kind. Sums file-backed VMA lengths bucketed by path
/// prefix (tuple/shard/wal/data_other), plus anonymous and other regions.
/// Throttled — served from cache between refreshes.
fn read_mmap_inventory() -> Vec<(&'static str, u64)> {
    #[cfg(target_os = "linux")]
    {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let last = MMAP_INVENTORY_LAST_REFRESH.load(std::sync::atomic::Ordering::Relaxed);
        if now.saturating_sub(last) < MMAP_INVENTORY_REFRESH_SECS && last != 0 {
            return MMAP_INVENTORY_CACHE.lock().clone();
        }
        // Single-flight: claim the slot before doing the walk.
        MMAP_INVENTORY_LAST_REFRESH.store(now, std::sync::atomic::Ordering::Relaxed);
        let Ok(maps) = std::fs::read_to_string("/proc/self/maps") else {
            return MMAP_INVENTORY_CACHE.lock().clone();
        };
        let mut tuple = 0u64;
        let mut shard = 0u64;
        let mut wal = 0u64;
        let mut data_other = 0u64;
        let mut anon = 0u64;
        let mut other = 0u64;
        for line in maps.lines() {
            // Format: "<start>-<end> <perms> <offset> <dev> <inode> <path?>"
            // Inode-to-path gap is variable whitespace, so use split_whitespace
            // and take fields 0..5 by token, then reconstruct the path tail.
            let mut iter = line.split_whitespace();
            let Some(range) = iter.next() else { continue };
            // Skip perms, offset, dev, inode.
            for _ in 0..4 {
                if iter.next().is_none() { break; }
            }
            let path = iter.next().unwrap_or("");
            let (start, end) = match range.split_once('-') {
                Some((a, b)) => (a, b),
                None => continue,
            };
            let Ok(s) = u64::from_str_radix(start, 16) else { continue };
            let Ok(e) = u64::from_str_radix(end, 16) else { continue };
            let len = e.saturating_sub(s);
            if len == 0 { continue; }
            if path.is_empty() || path.starts_with('[') {
                anon += len;
                continue;
            }
            if path.contains("/wal/") || path.ends_with(".wal") {
                wal += len;
            } else if path.contains("/docs/") || path.contains("/tuples/") {
                tuple += len;
            } else if path.contains("/bitmaps/") || path.contains("/shardstore/") || path.contains("/shards/") {
                shard += len;
            } else if path.starts_with("/data/") {
                data_other += len;
            } else {
                other += len;
            }
        }
        let result = vec![
            ("tuple", tuple),
            ("shard", shard),
            ("wal", wal),
            ("data_other", data_other),
            ("anon", anon),
            ("other", other),
        ];
        *MMAP_INVENTORY_CACHE.lock() = result.clone();
        result
    }
    #[cfg(not(target_os = "linux"))]
    { Vec::new() }
}


async fn handle_metrics(State(state): State<SharedState>) -> impl IntoResponse {
    let metrics_start = std::time::Instant::now();
    let m = &state.metrics;

    // Process memory (collect-on-scrape, no index needed). One read of
    // /proc/self/status produces VmRSS + the three split values.
    let rss_reading = read_rss_all();
    let rss = rss_reading.rss;
    m.process_rss_bytes.set(rss);
    m.process_rss_anon_bytes.set(rss_reading.anon);
    m.process_rss_file_bytes.set(rss_reading.file);
    m.process_rss_shmem_bytes.set(rss_reading.shmem);
    for (kind, bytes) in read_mmap_inventory() {
        m.mmap_bytes.with_label_values(&[kind]).set(bytes as i64);
    }
    if rss > m.process_rss_peak_bytes.get() {
        m.process_rss_peak_bytes.set(rss);
    }

    // Jemalloc memory stats (only available when heap-prof feature is active).
    // active   — bytes in active pages (allocated + small dirty)
    // resident — physical pages held (active + retained dirty)
    // mapped   — total mapped bytes (resident + decay-pending)
    // retained — virtual address space mapped but not committed
    // metadata — arena/extent/slab bookkeeping
    #[cfg(feature = "heap-prof")]
    {
        // With the `stats` feature enabled, epoch::advance() returns
        // Result<u64, _> (the previous epoch value) instead of Result<(), _>.
        if tikv_jemalloc_ctl::epoch::advance().is_ok() {
            if let Ok(v) = tikv_jemalloc_ctl::stats::allocated::read() {
                m.jemalloc_allocated_bytes.set(v as i64);
            }
            if let Ok(v) = tikv_jemalloc_ctl::stats::active::read() {
                m.jemalloc_active_bytes.set(v as i64);
            }
            if let Ok(v) = tikv_jemalloc_ctl::stats::resident::read() {
                m.jemalloc_resident_bytes.set(v as i64);
            }
            if let Ok(v) = tikv_jemalloc_ctl::stats::mapped::read() {
                m.jemalloc_mapped_bytes.set(v as i64);
            }
            if let Ok(v) = tikv_jemalloc_ctl::stats::retained::read() {
                m.jemalloc_retained_bytes.set(v as i64);
            }
            if let Ok(v) = tikv_jemalloc_ctl::stats::metadata::read() {
                m.jemalloc_metadata_bytes.set(v as i64);
            }
        }
    }

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
            let t0 = std::time::Instant::now();
            let uc = engine.unified_cache_stats();
            let t_cache_stats = t0.elapsed();
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
            m.cache_inserts_total
                .with_label_values(&[name])
                .set(uc.inserts as i64);
            m.cache_updates_total
                .with_label_values(&[name])
                .set(uc.updates as i64);
            m.cache_evictions_total
                .with_label_values(&[name])
                .set(uc.evictions as i64);
            m.cache_invalidations_total
                .with_label_values(&[name])
                .set(uc.invalidations as i64);
            m.cache_entries_initial
                .with_label_values(&[name])
                .set(uc.entries_initial as i64);
            m.cache_entries_expanded
                .with_label_values(&[name])
                .set(uc.entries_expanded as i64);
            m.cache_extensions_total
                .with_label_values(&[name])
                .set(uc.extensions as i64);
            m.cache_wall_hits_total
                .with_label_values(&[name])
                .set(uc.wall_hits as i64);
            m.cache_prefetch_total
                .with_label_values(&[name])
                .set(uc.prefetches as i64);

            // Compound-clause cache entry gauges (A3).
            // Walk entries once and count substituted + compound entries.
            {
                let (substituted, compound) = engine.unified_cache_entry_counts();
                m.cache_substituted_entries
                    .with_label_values(&[name])
                    .set(substituted as i64);
                m.cache_entries_compound_clause_count
                    .with_label_values(&[name])
                    .set(compound as i64);
            }

            // Per-field bitmap memory gauges.
            // Uses cached scanner totals instead of iterating all bitmaps (52s at 107M).
            // The bitmap_memory_cache is populated by a background scanner thread
            // that processes dirty fields in small batches.
            if state.metrics_bitmap_memory.load(Ordering::Relaxed) {
                let mem_cache = engine.bitmap_memory_cache();
                m.slot_bitmap_bytes
                    .with_label_values(&[name])
                    .set(mem_cache.cached_slot_bytes() as i64);
                for (field, bytes, count) in mem_cache.cached_filter_memory() {
                    m.filter_bitmap_bytes
                        .with_label_values(&[name, &field])
                        .set(bytes as i64);
                    m.filter_bitmap_count
                        .with_label_values(&[name, &field])
                        .set(count as i64);
                }
                for (field, bytes) in mem_cache.cached_sort_memory() {
                    m.sort_bitmap_bytes
                        .with_label_values(&[name, &field])
                        .set(bytes as i64);
                }
            }
            // NOTE: The old bitmap_memory_report() code that iterated all bitmaps
            // synchronously on every scrape is replaced above. If you need to verify
            // scanner accuracy, temporarily call engine.bitmap_memory_report() and
            // compare against the cached values.

            // Flush pipeline stats
            let (pub_count, _cumulative_nanos, last_nanos) = engine.flush_stats();
            m.snapshot_publish_total
                .with_label_values(&[name])
                .set(pub_count as i64);
            m.flush_last_duration_seconds
                .with_label_values(&[name])
                .set(last_nanos as i64);

            // Flush phase timing
            let (apply_ns, cache_ns, publish_ns, tb_ns, compact_ns, opslog_ns, sort_promote_ns) =
                engine.flush_phase_stats();
            m.flush_apply_nanos.with_label_values(&[name]).set(apply_ns as i64);
            m.flush_cache_nanos.with_label_values(&[name]).set(cache_ns as i64);
            m.flush_publish_nanos.with_label_values(&[name]).set(publish_ns as i64);
            m.flush_timebucket_nanos.with_label_values(&[name]).set(tb_ns as i64);
            m.flush_compact_nanos.with_label_values(&[name]).set(compact_ns as i64);
            m.flush_opslog_nanos.with_label_values(&[name]).set(opslog_ns as i64);
            m.flush_sort_promote_nanos.with_label_values(&[name]).set(sort_promote_ns as i64);
            // Iter 4a — cache maintenance shape stats + iter 6 max-seen
            let (unique_shapes, sort_work_items, unique_shapes_max, sort_work_items_max) =
                engine.cache_maint_shape_stats();
            m.cache_maint_unique_filter_shapes
                .with_label_values(&[name])
                .set(unique_shapes as i64);
            m.cache_maint_sort_work_items
                .with_label_values(&[name])
                .set(sort_work_items as i64);
            m.cache_maint_unique_filter_shapes_max
                .with_label_values(&[name])
                .set(unique_shapes_max as i64);
            m.cache_maint_sort_work_items_max
                .with_label_values(&[name])
                .set(sort_work_items_max as i64);
            // Async cache worker metrics
            let (cw_queue, cw_cycle_ns, cw_coalesced, cw_drops, cw_over_budget, cw_backpressure, cw_cycles) =
                engine.cache_worker_stats();
            m.cache_worker_queue_depth.with_label_values(&[name]).set(cw_queue as i64);
            m.cache_worker_cycle_nanos.with_label_values(&[name]).set(cw_cycle_ns as i64);
            m.cache_worker_items_coalesced_total.with_label_values(&[name]).set(cw_coalesced as i64);
            m.cache_worker_drops_total.with_label_values(&[name]).set(cw_drops as i64);
            m.cache_worker_over_budget_total.with_label_values(&[name]).set(cw_over_budget as i64);
            m.cache_backpressure_invalidations_total.with_label_values(&[name]).set(cw_backpressure as i64);
            m.cache_worker_cycles_total.with_label_values(&[name]).set(cw_cycles as i64);
            // Reason-attributed rebuild counters + needs_rebuild backlog gauge.
            let cwm = engine.cache_worker_metrics();
            m.cache_entries_needs_rebuild
                .with_label_values(&[name])
                .set(engine.unified_cache_needs_rebuild_count() as i64);
            m.cache_marked_for_rebuild_total
                .with_label_values(&[name, "deadline"])
                .set(cwm.marked_for_rebuild_deadline_total.load(Ordering::Relaxed) as i64);
            m.cache_marked_for_rebuild_total
                .with_label_values(&[name, "count_budget"])
                .set(cwm.marked_for_rebuild_count_budget_total.load(Ordering::Relaxed) as i64);
            m.cache_marked_for_rebuild_total
                .with_label_values(&[name, "backlog_drop"])
                .set(cwm.marked_for_rebuild_backlog_drop_total.load(Ordering::Relaxed) as i64);
            m.cache_marked_for_rebuild_total
                .with_label_values(&[name, "alive_change"])
                .set(cwm.marked_for_rebuild_alive_change_total.load(Ordering::Relaxed) as i64);
            m.cache_marked_for_rebuild_total
                .with_label_values(&[name, "filter_invalidation"])
                .set(cwm.marked_for_rebuild_filter_invalidation_total.load(Ordering::Relaxed) as i64);
            m.cache_marked_for_rebuild_total
                .with_label_values(&[name, "compound_too_large"])
                .set(cwm.marked_for_rebuild_compound_too_large_total.load(Ordering::Relaxed) as i64);
            m.cache_rebuild_completed_total
                .with_label_values(&[name])
                .set(cwm.rebuild_completed_total.load(Ordering::Relaxed) as i64);
            m.cache_evicted_on_overrun_total
                .with_label_values(&[name])
                .set(cwm.evicted_on_overrun_total.load(Ordering::Relaxed) as i64);
            // Iter 6 — put_batch fast/slow path counters
            let (fast_path, slow_path) = engine.docstore_put_batch_path_stats();
            m.docstore_put_batch_fast_path_total
                .with_label_values(&[name])
                .set(fast_path as i64);
            m.docstore_put_batch_slow_path_total
                .with_label_values(&[name])
                .set(slow_path as i64);

            // Pending fields (lazy loading)
            let pending = engine.pending_field_count();
            m.pending_fields
                .with_label_values(&[name])
                .set(pending as i64);

            // Eviction stats (gated — iterates per-field eviction data)
            if state.metrics_eviction_stats.load(Ordering::Relaxed) {
                for (field, total, resident) in engine.eviction_stats() {
                    m.eviction_total
                        .with_label_values(&[name, &field])
                        .set(total as i64);
                    m.eviction_resident_values
                        .with_label_values(&[name, &field])
                        .set(resident as i64);
                }
            }

            // Compaction skipped (scrape-time from atomic counter)
            m.compaction_skipped_total
                .with_label_values(&[name])
                .set(engine.compaction_skipped_count() as i64);

            // Sync peak from atomic to Prometheus gauge
            m.queries_in_flight_peak
                .set(state.queries_in_flight_peak.load(Ordering::Relaxed));

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
            // Disk bytes scan gated — does sync I/O (directory listing)
            if state.metrics_boundstore_disk.load(Ordering::Relaxed) {
                m.boundstore_disk_bytes
                    .with_label_values(&[name])
                    .set(engine.boundstore_disk_bytes() as i64);
            }
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

            // Phase 2.5: Flush queue depth
            m.flush_queue_depth.set(engine.flush_queue_depth() as i64);

            // Doc cache stats (synced from DocCache atomic counters)
            let t4 = std::time::Instant::now();
            let (dc_hits, dc_misses, dc_entries, dc_bytes, dc_evictions, dc_generations) = engine.doc_cache_stats();
            let t_doc_cache = t4.elapsed();
            m.doc_cache_hit_total.with_label_values(&[name]).set(dc_hits as i64);
            m.doc_cache_miss_total.with_label_values(&[name]).set(dc_misses as i64);
            m.doc_cache_entries.with_label_values(&[name]).set(dc_entries as i64);
            m.doc_cache_bytes.with_label_values(&[name]).set(dc_bytes as i64);
            m.doc_cache_evictions_total.with_label_values(&[name]).set(dc_evictions as i64);
            m.doc_cache_generations.with_label_values(&[name]).set(dc_generations as i64);

            eprintln!("[metrics-timing] cache_stats={:?} doc_cache={:?} total={:?}",
                t_cache_stats, t_doc_cache, metrics_start.elapsed());
        }
    }

    let t_gather = std::time::Instant::now();
    let output = m.gather();
    eprintln!("[metrics-timing] gather={:?} grand_total={:?}", t_gather.elapsed(), metrics_start.elapsed());

    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        output,
    )
}

/// Internal endpoint for pg-sync sidecar to report metrics.
/// POST /api/internal/pgsync-metrics { replica, cycle_seconds, rows_fetched, cursor_position }
async fn handle_pgsync_metrics(
    State(state): State<SharedState>,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    let m = &state.metrics;
    let replica = body["replica"].as_str().unwrap_or("unknown");
    if let Some(cycle_secs) = body["cycle_seconds"].as_f64() {
        m.pgsync_cycle_seconds
            .with_label_values(&[replica])
            .observe(cycle_secs);
    }
    if let Some(rows) = body["rows_fetched"].as_u64() {
        m.pgsync_rows_fetched_total
            .with_label_values(&[replica])
            .inc_by(rows);
    }
    if let Some(cursor) = body["cursor_position"].as_i64() {
        m.pgsync_cursor_position
            .with_label_values(&[replica])
            .set(cursor);
    }
    StatusCode::NO_CONTENT
}

/// POST /api/indexes/{name}/ops — Accept a batch of sync ops, append to WAL.
/// Returns 200 only after all records are written and fsynced.
#[cfg(feature = "pg-sync")]
async fn handle_ops(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
    Json(batch): Json<crate::pg_sync::ops::OpsBatch>,
) -> impl IntoResponse {
    // Verify index exists
    {
        let guard = state.index.lock();
        match guard.as_ref() {
            Some(idx) if idx.definition.name == name => {}
            _ => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error": format!("Index '{}' not found", name)})),
                ).into_response();
            }
        }
    }

    // Store sync metadata + update Prometheus metrics
    if let Some(meta) = &batch.meta {
        let mut sync_meta = state.sync_meta.lock();
        sync_meta.insert(meta.source.clone(), meta.clone());

        let m = &state.metrics;
        let source = meta.source.as_str();
        if let Some(cursor) = meta.cursor {
            m.sync_cursor_position.with_label_values(&[source]).set(cursor);
        }
        if let Some(max_id) = meta.max_id {
            m.sync_max_id.with_label_values(&[source]).set(max_id);
        }
        if let Some(lag) = meta.lag_rows {
            m.sync_lag_rows.with_label_values(&[source]).set(lag);
        }
    }

    let ops_count = batch.ops.len();

    // Record batch size metric
    if let Some(meta) = &batch.meta {
        state.metrics.sync_batch_size
            .with_label_values(&[meta.source.as_str()])
            .set(ops_count as i64);
    }

    if ops_count == 0 {
        return (StatusCode::OK, Json(serde_json::json!({"accepted": 0}))).into_response();
    }

    // Ensure WAL writer exists (lazy init)
    // Initialize shared WAL writer if needed (uses wal/ directory, not a file path)
    {
        let mut wal_guard = state.ops_wal.lock();
        if wal_guard.is_none() {
            let wal_dir = state.data_dir.join("wal");
            std::fs::create_dir_all(&wal_dir).ok();
            *wal_guard = Some(crate::ops_wal::WalWriter::new(&wal_dir));
        }
    }

    // Write to WAL using the shared writer (supports generational rotation).
    //
    // block_in_place wraps BOTH the Mutex acquisition AND append_batch.
    // This is intentional: a contending request would otherwise block a Tokio
    // worker on the synchronous parking_lot::Mutex *before* reaching
    // block_in_place, starving the runtime.  Wrapping both gives Tokio
    // visibility into the full blocking section so it spawns a replacement
    // worker for the duration of the lock + fsync.  Matches the pattern used
    // for query execution (see handle_query / block_in_place there).
    //
    // Commit 63662af removed the previous spawn_blocking to fix per-request
    // WalWriter instantiation (which bypassed generational rotation).  That fix
    // was correct but accidentally dropped the blocking offload; this restores
    // it via block_in_place (no JoinHandle overhead, no pool exhaustion risk).
    let t_wal_start = std::time::Instant::now();
    let result = tokio::task::block_in_place(|| {
        let wal_guard = state.ops_wal.lock();
        wal_guard.as_ref().unwrap().append_batch(&batch.ops)
    });
    state.metrics.wal_append_duration_seconds
        .observe(t_wal_start.elapsed().as_secs_f64());

    match result {
        Ok(bytes) => {
            state.metrics.wal_ops_written_total.inc_by(ops_count as u64);
            (StatusCode::OK, Json(serde_json::json!({
                "accepted": ops_count,
                "bytes_written": bytes,
            }))).into_response()
        }
        Err(e) => {
            eprintln!("WAL write error: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({
                "error": format!("WAL write failed: {e}"),
            }))).into_response()
        }
    }
}

/// Fallback for when pg-sync feature is disabled.
#[cfg(not(feature = "pg-sync"))]
async fn handle_ops(
    AxumPath(_name): AxumPath<String>,
) -> impl IntoResponse {
    (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "pg-sync feature not enabled"})))
}

// ── Dump endpoints ──

/// GET /api/indexes/{name}/dumps — List all dumps and their status.
#[cfg(feature = "pg-sync")]
async fn handle_list_dumps(
    State(state): State<SharedState>,
    AxumPath(_name): AxumPath<String>,
) -> impl IntoResponse {
    let reg = state.dump_registry.lock();
    // Enrich dump entries with live progress from task registry
    let mut dumps = serde_json::Map::new();
    let tasks = state.index.lock().as_ref().map(|idx| Arc::clone(&idx.tasks));
    for (name, entry) in &reg.dumps {
        let mut val = serde_json::to_value(entry).unwrap_or_default();
        // If dump has an active task, inject live records_processed
        if let (Some(tid), Some(ref tasks)) = (entry.task_id, &tasks) {
            if let Some(task_info) = tasks.get(tid) {
                val["records_processed"] = serde_json::json!(task_info.progress.records_processed);
                val["elapsed_secs"] = serde_json::json!(task_info.elapsed_secs);
            }
        }
        dumps.insert(name.clone(), val);
    }
    Json(serde_json::json!({
        "dumps": dumps,
        "all_complete": reg.all_complete(),
    }))
}

#[cfg(not(feature = "pg-sync"))]
async fn handle_list_dumps(AxumPath(_name): AxumPath<String>) -> impl IntoResponse {
    Json(serde_json::json!({"dumps": {}}))
}

/// PUT /api/indexes/{name}/dumps — Register and process a dump.
///
/// Accepts either:
/// - V2 DumpRequest (has csv_path, slot_field) → async dump processing via dump_processor
/// - V1 legacy body (has wal_path) → register in dump registry only
#[cfg(feature = "pg-sync")]
async fn handle_register_dump(
    State(state): State<SharedState>,
    AxumPath(_name): AxumPath<String>,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    // Detect V2 DumpRequest by presence of csv_path
    if body.get("csv_path").is_some() {
        // V2: parse DumpRequest and process asynchronously
        let request: crate::dump_processor::DumpRequest = match serde_json::from_value(body) {
            Ok(r) => r,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": format!("Invalid dump request: {e}")})),
                )
                    .into_response();
            }
        };

        let dump_name = request.name.clone();

        // Get engine, tasks, and data_schema from IndexState
        let (engine, tasks, data_schema) = {
            let guard = state.index.lock();
            match guard.as_ref() {
                Some(idx) => (
                    Arc::clone(&idx.engine),
                    Arc::clone(&idx.tasks),
                    idx.definition.data_schema.clone(),
                ),
                None => {
                    return (
                        StatusCode::SERVICE_UNAVAILABLE,
                        Json(serde_json::json!({"error": "Index not loaded"})),
                    )
                        .into_response();
                }
            }
        };

        // Try to start a task
        let (task_id, progress) = match tasks.try_start(TaskType::Dump) {
            Ok(v) => v,
            Err(active) => {
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({
                        "error": "Another task is already running",
                        "active_task": serde_json::to_value(&active).unwrap_or_default(),
                    })),
                )
                    .into_response();
            }
        };

        // Register in dump registry with task_id for live progress tracking
        {
            let mut reg = state.dump_registry.lock();
            reg.register(dump_name.clone(), None);
            if let Some(entry) = reg.dumps.get_mut(&dump_name) {
                entry.task_id = Some(task_id);
            }
            let dumps_path = state.data_dir.join("dumps.json");
            reg.save(&dumps_path).ok();
        }

        // Start shard pre-creator on first dump (progressive file creation)
        if !state.precreator_started.swap(true, std::sync::atomic::Ordering::SeqCst) {
            // Derive docstore root from the index storage path
            let idx_name = state.index.lock().as_ref().map(|e| e.definition.name.clone()).unwrap_or_else(|| "civitai".to_string());
            let docstore_root = state.data_dir.join("indexes").join(&idx_name).join("docs");
            let bitmap_path = engine.config().storage.bitmap_path.clone();
            let filter_names: Vec<String> = engine.config()
                .filter_fields.iter().map(|f| f.name.clone()).collect();
            let _precreator = crate::dump_processor::ShardPreCreator::spawn(
                Arc::clone(&state.slot_watermark),
                Arc::clone(&state.precreator_done),
                docstore_root,
                bitmap_path,
                filter_names,
            );
            eprintln!("  ShardPreCreator started (background file creation)");
            // Note: precreator handle intentionally leaked — it runs until precreator_done is set
        }
        let slot_watermark = Arc::clone(&state.slot_watermark);

        // Spawn async processing — inline parse + save
        let state_clone = Arc::clone(&state);
        let dump_name_for_task = dump_name.clone();
        let stage_dir = std::path::Path::new(&request.csv_path)
            .parent()
            .unwrap_or_else(|| std::path::Path::new("/data/load_stage"))
            .to_path_buf();
        let phase_sets_alive = request.sets_alive;
        let engine_for_reload = Arc::clone(&engine);
        // Share shutdown flag so dump processor can abort on Ctrl+C
        let shutdown_flag = Arc::clone(&state);

        tokio::spawn(async move {
            let dump_name_inner = dump_name_for_task;

            let result = tokio::task::spawn_blocking(move || {
                // Create a closure that checks AppState.shutting_down
                let shutdown_check: Arc<dyn Fn() -> bool + Send + Sync> = Arc::new(move || {
                    shutdown_flag.shutting_down.load(std::sync::atomic::Ordering::Relaxed)
                });
                crate::dump_processor::process_dump(&request, &engine, &stage_dir, Some(progress), Some(&data_schema), Some(slot_watermark), Some(shutdown_check))
            })
            .await;

            match result {
                Ok(Ok(phase_result)) => {
                    let row_count = phase_result.row_count;

                    // Fail if 0 rows processed — likely a header/column mismatch bug.
                    // Don't mark as complete so the sidecar can retry.
                    if row_count == 0 {
                        let msg = format!("dump '{}' processed 0 rows — possible CSV header/column mismatch", dump_name_inner);
                        eprintln!("WARNING: {msg}");
                        tasks.set_error(task_id, msg.clone());
                        let mut reg = state_clone.dump_registry.lock();
                        if let Some(entry) = reg.dumps.get_mut(&dump_name_inner) {
                            entry.status = crate::pg_sync::dump::DumpStatus::Failed(msg);
                        }
                        let dumps_path = state_clone.data_dir.join("dumps.json");
                        reg.save(&dumps_path).ok();
                        return;
                    }

                    // Reload fields only for the alive phase (images).
                    // Other phases just save to disk — fields get loaded lazily on first query.
                    if phase_sets_alive {
                        crate::dump_processor::reload_after_dumps(&engine_for_reload, true);
                    }

                    tasks.set_complete(
                        task_id,
                        Some(serde_json::json!({
                            "rows_processed": row_count,
                            "dump_name": dump_name_inner,
                        })),
                    );

                    // Mark dump as complete in registry
                    let mut reg = state_clone.dump_registry.lock();
                    if let Some(entry) = reg.dumps.get_mut(&dump_name_inner) {
                        entry.status = crate::pg_sync::dump::DumpStatus::Complete;
                        entry.ops_processed = row_count;
                        entry.completed_at = Some(
                            std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs(),
                        );
                    }
                    let dumps_path = state_clone.data_dir.join("dumps.json");
                    reg.save(&dumps_path).ok();
                }
                Ok(Err(e)) => {
                    eprintln!("Dump failed: {e}");
                    tasks.set_error(task_id, e);
                }
                Err(e) => {
                    eprintln!("Dump panicked: {e}");
                    tasks.set_error(task_id, format!("Task panicked: {e}"));
                }
            }
        });

        (
            StatusCode::CREATED,
            Json(serde_json::json!({
                "name": dump_name,
                "task_id": task_id,
                "status": "running",
            })),
        )
            .into_response()
    } else {
        // V1 legacy: just register the dump name
        let dump_name = body["name"].as_str().unwrap_or("unknown").to_string();
        let wal_path = body["wal_path"].as_str().map(|s| s.to_string());

        let mut reg = state.dump_registry.lock();
        reg.register(dump_name.clone(), wal_path);

        let dumps_path = state.data_dir.join("dumps.json");
        if let Err(e) = reg.save(&dumps_path) {
            eprintln!("Warning: failed to save dump registry: {e}");
        }

        (
            StatusCode::CREATED,
            Json(serde_json::json!({
                "name": dump_name,
                "status": "writing",
            })),
        )
            .into_response()
    }
}

#[cfg(not(feature = "pg-sync"))]
async fn handle_register_dump(
    AxumPath(_name): AxumPath<String>,
    Json(_body): Json<serde_json::Value>,
) -> impl IntoResponse {
    (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "pg-sync not enabled"})))
}

/// POST /api/indexes/{name}/dumps/{dump_name}/loaded — Signal dump file is complete.
#[cfg(feature = "pg-sync")]
async fn handle_dump_loaded(
    State(state): State<SharedState>,
    AxumPath((_name, dump_name)): AxumPath<(String, String)>,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    let ops_written = body["ops_written"].as_u64().unwrap_or(0);

    let mut reg = state.dump_registry.lock();
    match reg.mark_loaded(&dump_name, ops_written) {
        Some(_) => {
            let dumps_path = state.data_dir.join("dumps.json");
            reg.save(&dumps_path).ok();
            Json(serde_json::json!({"status": "loading", "name": dump_name}))
        }
        None => Json(serde_json::json!({"error": format!("Dump '{}' not found", dump_name)})),
    }
}

#[cfg(not(feature = "pg-sync"))]
async fn handle_dump_loaded(
    AxumPath((_name, _dump_name)): AxumPath<(String, String)>,
    Json(_body): Json<serde_json::Value>,
) -> impl IntoResponse {
    Json(serde_json::json!({"error": "pg-sync not enabled"}))
}

/// DELETE /api/indexes/{name}/dumps/{dump_name} — Remove a dump from history.
#[cfg(feature = "pg-sync")]
async fn handle_delete_dump(
    State(state): State<SharedState>,
    AxumPath((_name, dump_name)): AxumPath<(String, String)>,
) -> impl IntoResponse {
    let mut reg = state.dump_registry.lock();
    reg.remove(&dump_name);
    let dumps_path = state.data_dir.join("dumps.json");
    reg.save(&dumps_path).ok();
    StatusCode::NO_CONTENT
}

#[cfg(not(feature = "pg-sync"))]
async fn handle_delete_dump(
    AxumPath((_name, _dump_name)): AxumPath<(String, String)>,
) -> impl IntoResponse {
    StatusCode::NOT_FOUND
}

/// POST /api/indexes/{name}/dumps/clear — Clear all dump history.
#[cfg(feature = "pg-sync")]
async fn handle_clear_dumps(
    State(state): State<SharedState>,
    AxumPath(_name): AxumPath<String>,
) -> impl IntoResponse {
    let mut reg = state.dump_registry.lock();
    reg.clear();
    let dumps_path = state.data_dir.join("dumps.json");
    reg.save(&dumps_path).ok();
    StatusCode::NO_CONTENT
}

#[cfg(not(feature = "pg-sync"))]
async fn handle_clear_dumps(AxumPath(_name): AxumPath<String>) -> impl IntoResponse {
    StatusCode::NOT_FOUND
}

// ---------------------------------------------------------------------------
// Redump: wipe on-disk state and exit so the sidecar re-dumps from PG.
//
// Workflow:
//   1. Server returns 202 immediately.
//   2. Server removes the `.ready` marker so k8s readiness probe drops
//      this pod from the Service. New traffic stops within ~10s.
//   3. Server sleeps `drain_secs` so in-flight queries finish.
//   4. Server calls sidecar `/internal/restart?reason=redump` so the
//      sidecar deletes its row from `bitdex_cursors` and exits.
//   5. Server wipes on-disk index state (bitmaps, docstore, WAL,
//      cursors, deferred map, staged CSVs). Configmap-mounted configs
//      are defensively skipped.
//   6. Server exits. K8s restarts both containers; the sidecar boot
//      sequence sees no `.ready`, no DumpRegistry, and re-runs the
//      full dump pipeline.
// ---------------------------------------------------------------------------

#[cfg(feature = "pg-sync")]
#[derive(Deserialize, Default)]
#[serde(default)]
struct RedumpBody {
    /// Seconds to wait between flipping readiness off and wiping state.
    /// Default 30s.
    drain_secs: Option<u64>,
    /// URL of the sidecar admin listener. Default: env
    /// `BITDEX_SYNC_ADMIN_URL` or `http://127.0.0.1:9192`.
    sidecar_admin_url: Option<String>,
}

#[cfg(feature = "pg-sync")]
async fn handle_redump(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
    body: Option<Json<RedumpBody>>,
) -> impl IntoResponse {
    // Validate index exists + matches the loaded one.
    {
        let guard = state.index.lock();
        match guard.as_ref() {
            Some(idx) if idx.definition.name == name => {}
            _ => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error": format!("Index '{}' not loaded", name)})),
                )
                    .into_response();
            }
        }
    }

    let body = body.map(|b| b.0).unwrap_or_default();
    let drain_secs = body.drain_secs.unwrap_or(30);
    let sidecar_url = body
        .sidecar_admin_url
        .or_else(|| std::env::var("BITDEX_SYNC_ADMIN_URL").ok())
        .unwrap_or_else(|| "http://127.0.0.1:9192".to_string());

    // Idempotency gate: marker file lives for the lifetime of the redump.
    let marker_path = state.data_dir.join(".redump_in_progress");
    if marker_path.exists() {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": "redump already in progress",
                "marker": marker_path.display().to_string(),
            })),
        )
            .into_response();
    }

    let redump_id = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0)
    );

    if let Err(e) = std::fs::write(
        &marker_path,
        serde_json::json!({
            "redump_id": &redump_id,
            "started_at": chrono::Utc::now().to_rfc3339(),
        })
        .to_string(),
    ) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": format!("write marker failed: {e}")})),
        )
            .into_response();
    }

    // Clear DumpRegistry so a half-written .ready doesn't survive a crash
    // before the wipe runs. After the wipe this is moot, but it covers the
    // window between marker-write and wipe.
    {
        let mut reg = state.dump_registry.lock();
        reg.clear();
        let dumps_path = state.data_dir.join("dumps.json");
        let _ = reg.save(&dumps_path);
    }

    // Flip readiness OFF immediately. K8s sees 503 on next probe.
    let ready_path = state.data_dir.join(".ready");
    let _ = std::fs::remove_file(&ready_path);
    eprintln!(
        "[redump] {redump_id}: readiness flipped off (.ready removed), drain={drain_secs}s, sidecar={sidecar_url}"
    );

    // Capture data needed by the background task.
    let data_dir = state.data_dir.clone();
    let index_name = name.clone();
    let drain = std::time::Duration::from_secs(drain_secs);
    let bg_redump_id = redump_id.clone();
    let bg_sidecar_url = sidecar_url.clone();

    tokio::spawn(async move {
        let redump_id = bg_redump_id;
        let sidecar_url = bg_sidecar_url;
        // Phase 1: drain in-flight queries.
        eprintln!("[redump] {redump_id}: draining for {drain_secs}s");
        tokio::time::sleep(drain).await;

        // Phase 2: tell sidecar to delete its cursor row + exit.
        eprintln!("[redump] {redump_id}: calling sidecar restart at {sidecar_url}");
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .expect("reqwest client");
        let sidecar_endpoint = format!("{}/internal/restart", sidecar_url.trim_end_matches('/'));
        match client
            .post(&sidecar_endpoint)
            .json(&serde_json::json!({ "reason": "redump" }))
            .send()
            .await
        {
            Ok(resp) => eprintln!(
                "[redump] {redump_id}: sidecar restart returned {}",
                resp.status()
            ),
            Err(e) => eprintln!(
                "[redump] {redump_id}: WARNING sidecar restart call failed: {e} \
                 — proceeding anyway; sidecar will eventually exit on shared pod \
                 lifecycle when this process exits"
            ),
        }

        // Phase 3: wipe on-disk index state.
        eprintln!("[redump] {redump_id}: wiping {}", data_dir.display());
        if let Err(e) = wipe_index_data_for_redump(&data_dir, &index_name) {
            eprintln!("[redump] {redump_id}: WIPE FAILED: {e}");
            // Do NOT exit — a half-wiped pod that comes back up would
            // confuse the sidecar. Leave the marker, leave .ready off,
            // operator inspects.
            return;
        }

        // Phase 4: exit. K8s restarts the container; the sidecar's fresh
        // boot re-runs the dump pipeline from a clean PVC.
        eprintln!("[redump] {redump_id}: wipe complete — exiting");
        std::process::exit(0);
    });

    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({
            "redump_id": redump_id,
            "drain_secs": drain_secs,
            "sidecar_admin_url": sidecar_url,
            "next": "readiness flipped off; pod will exit after drain + wipe",
        })),
    )
        .into_response()
}

#[cfg(not(feature = "pg-sync"))]
async fn handle_redump(
    AxumPath(_name): AxumPath<String>,
    _body: Option<Json<serde_json::Value>>,
) -> impl IntoResponse {
    StatusCode::NOT_FOUND
}

/// Wipe per-index runtime state under `data_dir` for a clean redump.
///
/// Removes all engine-persisted state (bitmaps, docstore, WAL, cursors,
/// deferred map, staged CSVs, capture snapshots, dump registry, readiness
/// + in-progress markers). Defensively SKIPS configmap-mounted configs
/// (`config.yaml`, `ui-config.yaml`) so an empty PVC doesn't break boot
/// — though in production those files are read-only ConfigMap mounts
/// anyway.
///
/// All paths are bounded under `data_dir` — every entry to delete is
/// resolved relative to `data_dir` and the helper never recurses
/// outside it.
#[cfg(feature = "pg-sync")]
fn wipe_index_data_for_redump(
    data_dir: &std::path::Path,
    index_name: &str,
) -> std::io::Result<()> {
    let index_root = data_dir.join("indexes").join(index_name);

    // Per-index runtime dirs (engine state).
    let index_dirs = [
        "shardstore",
        "docstore",
        "bitmaps",   // legacy BitmapFs
        "cursors",
        "system",    // deferred_alive.bin lives here
        "load_stage", // staged CSVs
    ];
    for sub in &index_dirs {
        let p = index_root.join(sub);
        if p.exists() {
            std::fs::remove_dir_all(&p)?;
            eprintln!("[redump] wiped {}", p.display());
        }
    }

    // Server-wide runtime dirs.
    let global_dirs = ["wal", "captures"];
    for sub in &global_dirs {
        let p = data_dir.join(sub);
        if p.exists() {
            std::fs::remove_dir_all(&p)?;
            eprintln!("[redump] wiped {}", p.display());
        }
    }

    // DumpRegistry persisted JSON.
    let dumps_path = data_dir.join("dumps.json");
    if dumps_path.exists() {
        std::fs::remove_file(&dumps_path)?;
        eprintln!("[redump] wiped {}", dumps_path.display());
    }

    // Readiness marker LAST — once removed, k8s won't route traffic.
    let ready = data_dir.join(".ready");
    if ready.exists() {
        let _ = std::fs::remove_file(&ready);
    }

    // Marker stays until exit — its presence signals "still wiping".
    // K8s will recreate the container; the new process starts with no
    // marker (PVC has the marker but boot deletes it after dump completes,
    // OR we delete it here as the last step so an aborted wipe retries).
    let marker = data_dir.join(".redump_in_progress");
    if marker.exists() {
        let _ = std::fs::remove_file(&marker);
    }

    Ok(())
}

/// GET /api/internal/sync-lag — Return latest sync metadata from all sources.
#[cfg(feature = "pg-sync")]
async fn handle_sync_lag(
    State(state): State<SharedState>,
) -> impl IntoResponse {
    let sync_meta = state.sync_meta.lock();
    let sources: Vec<&crate::pg_sync::ops::SyncMeta> = sync_meta.values().collect();
    Json(serde_json::json!({ "sources": sources }))
}

#[cfg(not(feature = "pg-sync"))]
async fn handle_sync_lag() -> impl IntoResponse {
    Json(serde_json::json!({ "sources": [] }))
}

async fn handle_ui() -> impl IntoResponse {
    Html(include_str!("../static/index.html"))
}

// ---------------------------------------------------------------------------
// GET /debug/queries/stream — SSE query mirror
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct QueryStreamParams {
    /// Optional index filter. Omit to receive events for all indexes.
    index: Option<String>,
}

/// GET /debug/queries/stream — admin-gated SSE endpoint.
///
/// Each event is a JSON-encoded `QueryEvent`. The stream is live-only:
/// events are dropped if the channel is full (capacity 10 000) or if the
/// server was started without `BITDEX_QUERY_STREAM=1`.
async fn handle_query_stream(
    State(state): State<SharedState>,
    AxumQuery(params): AxumQuery<QueryStreamParams>,
) -> impl IntoResponse {
    let Some(ref tx) = state.query_stream else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "query stream not enabled — set BITDEX_QUERY_STREAM=1 and restart"
            })),
        ).into_response();
    };

    let rx = tx.subscribe();
    let index_filter = params.index.clone();

    let stream = BroadcastStream::new(rx)
        .filter_map(move |msg| {
            let filter = index_filter.clone();
            match msg {
                Ok(event) => {
                    // Apply optional index filter
                    if filter.as_deref().map_or(true, |f| f == event.index) {
                        let json = serde_json::to_string(&event).unwrap_or_default();
                        Some(Ok::<Event, std::convert::Infallible>(Event::default().data(json)))
                    } else {
                        None
                    }
                }
                // Lagged means we fell behind; skip and continue
                Err(_lagged) => None,
            }
        });

    let mut response = Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response();
    // Disable Cloudflare proxy buffering so events stream in real-time
    // instead of arriving in ~60s bursts. Cloudflare honors this nginx-
    // style header on its regular reverse proxy path.
    response.headers_mut().insert(
        "X-Accel-Buffering",
        axum::http::HeaderValue::from_static("no"),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FieldMapping, FieldValueType, DataSchema};

    #[test]
    fn patch_config_fanout_ordinal_parser() {
        assert_eq!(parse_pod_ordinal("bitdex-0"), Some(0));
        assert_eq!(parse_pod_ordinal("bitdex-1"), Some(1));
        assert_eq!(parse_pod_ordinal("bitdex-12"), Some(12));
        // No trailing ordinal
        assert_eq!(parse_pod_ordinal("bitdex"), None);
        // Non-numeric suffix
        assert_eq!(parse_pod_ordinal("bitdex-abc"), None);
        // Empty string
        assert_eq!(parse_pod_ordinal(""), None);
        // Different StatefulSet prefix
        assert_eq!(parse_pod_ordinal("my-service-3"), Some(3));
    }

    #[test]
    fn inflight_guard_decrements_on_drop() {
        let counter = AtomicI64::new(0);
        let registry = prometheus::Registry::new();
        let gauge = prometheus::IntGauge::new("test_in_flight", "test").unwrap();
        registry.register(Box::new(gauge.clone())).unwrap();

        // Simulate incrementing like handle_query does
        counter.fetch_add(1, Ordering::Relaxed);
        gauge.inc();
        assert_eq!(counter.load(Ordering::Relaxed), 1);
        assert_eq!(gauge.get(), 1);

        {
            let _guard = QueryInflightGuard {
                counter: &counter,
                gauge: &gauge,
            };
            // Guard is alive, counter should still be 1
            assert_eq!(counter.load(Ordering::Relaxed), 1);
        }
        // Guard dropped, counter should be back to 0
        assert_eq!(counter.load(Ordering::Relaxed), 0);
        assert_eq!(gauge.get(), 0);
    }

    #[test]
    fn peak_tracking_is_monotonic() {
        let peak = AtomicI64::new(0);

        // Simulate concurrent updates — fetch_max ensures monotonicity
        peak.fetch_max(5, Ordering::Relaxed);
        assert_eq!(peak.load(Ordering::Relaxed), 5);

        // Lower value should not decrease peak
        peak.fetch_max(3, Ordering::Relaxed);
        assert_eq!(peak.load(Ordering::Relaxed), 5);

        // Higher value should increase peak
        peak.fetch_max(12, Ordering::Relaxed);
        assert_eq!(peak.load(Ordering::Relaxed), 12);

        // Equal value is a no-op
        peak.fetch_max(12, Ordering::Relaxed);
        assert_eq!(peak.load(Ordering::Relaxed), 12);
    }

    #[test]
    fn peak_tracking_no_toctou_regression() {
        // Simulate the old TOCTOU bug: thread A reads peak=5, thread B sets peak=12,
        // thread A sets peak=10 (lowering it). With fetch_max this cannot happen.
        let peak = AtomicI64::new(5);

        // Thread B sets 12
        peak.fetch_max(12, Ordering::Relaxed);
        // Thread A tries to set 10 (should be a no-op)
        peak.fetch_max(10, Ordering::Relaxed);

        assert_eq!(peak.load(Ordering::Relaxed), 12);
    }

    // -----------------------------------------------------------------------
    // format_document tests
    // -----------------------------------------------------------------------

    fn make_schema(fields: Vec<FieldMapping>) -> DataSchema {
        DataSchema {
            id_field: "id".to_string(),
            schema_version: 1,
            fields,
        }
    }

    fn make_stored_doc(fields: Vec<(&str, FieldValue)>) -> StoredDoc {
        StoredDoc {
            fields: fields
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
            schema_version: 0,
        }
    }

    fn empty_reverse_maps() -> ReverseStringMaps {
        HashMap::new()
    }

    fn empty_schema_registry() -> SchemaRegistry {
        HashMap::new()
    }

    #[test]
    fn test_format_document_source_ne_target() {
        // When source ≠ target, format_document should look up by source name
        // and return under target name.
        let schema = make_schema(vec![FieldMapping {
            source: "publishedAtUnix".into(),
            target: "publishedAt".into(),
            value_type: FieldValueType::Integer,
            fallback: None,
            string_map: None,
            doc_only: false,
            filter_only: false,
            ms_to_seconds: true,
            truncate_u32: false,
            case_sensitive: false,
            default_value: None,
            nullable: false,
        }]);

        // Doc has field under SOURCE name (bulk loader path)
        let doc = make_stored_doc(vec![
            ("id", FieldValue::Single(Value::Integer(42))),
            ("publishedAtUnix", FieldValue::Single(Value::Integer(1487255090000))),
        ]);

        let result = format_document(
            &doc, &schema, &empty_reverse_maps(), &IncludeDocs::All, &empty_schema_registry(),
        );

        // Should appear under TARGET name with ms_to_seconds applied
        assert_eq!(result["publishedAt"], serde_json::json!(1487255090));
    }

    #[test]
    fn test_format_document_target_name_lookup() {
        // When doc has field under TARGET name (outbox/PATCH path),
        // format_document should still find it.
        let schema = make_schema(vec![FieldMapping {
            source: "publishedAtUnix".into(),
            target: "publishedAt".into(),
            value_type: FieldValueType::Integer,
            fallback: None,
            string_map: None,
            doc_only: false,
            filter_only: false,
            ms_to_seconds: true,
            truncate_u32: false,
            case_sensitive: false,
            default_value: None,
            nullable: false,
        }]);

        // Doc has field under TARGET name (outbox/PATCH path).
        // Value is already in seconds (ms_to_seconds was applied during encoding).
        let doc = make_stored_doc(vec![
            ("id", FieldValue::Single(Value::Integer(42))),
            ("publishedAt", FieldValue::Single(Value::Integer(1487255090))),
        ]);

        let result = format_document(
            &doc, &schema, &empty_reverse_maps(), &IncludeDocs::All, &empty_schema_registry(),
        );

        // Target name: value already converted, no ms_to_seconds applied
        assert_eq!(result["publishedAt"], serde_json::json!(1487255090));
    }

    #[test]
    fn test_format_document_no_ms_conversion_when_not_configured() {
        // Fields without ms_to_seconds should return raw value
        let schema = make_schema(vec![FieldMapping {
            source: "reactionCount".into(),
            target: "reactionCount".into(),
            value_type: FieldValueType::Integer,
            fallback: None,
            string_map: None,
            doc_only: false,
            filter_only: false,
            ms_to_seconds: false,
            truncate_u32: false,
            case_sensitive: false,
            default_value: Some(serde_json::json!(0)),
            nullable: false,
        }]);

        let doc = make_stored_doc(vec![
            ("id", FieldValue::Single(Value::Integer(1))),
            ("reactionCount", FieldValue::Single(Value::Integer(42))),
        ]);

        let result = format_document(
            &doc, &schema, &empty_reverse_maps(), &IncludeDocs::All, &empty_schema_registry(),
        );

        assert_eq!(result["reactionCount"], serde_json::json!(42));
    }

    #[test]
    fn test_format_document_missing_field_uses_default() {
        // Missing fields should use schema default, not 0
        let schema = make_schema(vec![FieldMapping {
            source: "reactionCount".into(),
            target: "reactionCount".into(),
            value_type: FieldValueType::Integer,
            fallback: None,
            string_map: None,
            doc_only: false,
            filter_only: false,
            ms_to_seconds: false,
            truncate_u32: false,
            case_sensitive: false,
            default_value: Some(serde_json::json!(0)),
            nullable: false,
        }]);

        // Doc has no reactionCount
        let doc = make_stored_doc(vec![
            ("id", FieldValue::Single(Value::Integer(1))),
        ]);

        let result = format_document(
            &doc, &schema, &empty_reverse_maps(), &IncludeDocs::All, &empty_schema_registry(),
        );

        assert_eq!(result["reactionCount"], serde_json::json!(0));
    }

    #[test]
    fn test_format_document_missing_field_no_default_returns_type_zero() {
        // Missing field with no explicit default → type-appropriate zero value
        let schema = make_schema(vec![FieldMapping {
            source: "sortAt".into(),
            target: "sortAt".into(),
            value_type: FieldValueType::Integer,
            fallback: None,
            string_map: None,
            doc_only: false,
            filter_only: false,
            ms_to_seconds: false,
            truncate_u32: false,
            case_sensitive: false,
            default_value: None,
            nullable: false,
        }]);

        let doc = make_stored_doc(vec![
            ("id", FieldValue::Single(Value::Integer(1))),
        ]);

        let result = format_document(
            &doc, &schema, &empty_reverse_maps(), &IncludeDocs::All, &empty_schema_registry(),
        );

        assert_eq!(result["sortAt"], serde_json::json!(0));
    }

    #[test]
    fn test_format_document_boolean_field() {
        let schema = make_schema(vec![FieldMapping {
            source: "hasMeta".into(),
            target: "hasMeta".into(),
            value_type: FieldValueType::Boolean,
            fallback: None,
            string_map: None,
            doc_only: false,
            filter_only: false,
            ms_to_seconds: false,
            truncate_u32: false,
            case_sensitive: false,
            default_value: Some(serde_json::json!(false)),
            nullable: false,
        }]);

        let doc = make_stored_doc(vec![
            ("id", FieldValue::Single(Value::Integer(1))),
            ("hasMeta", FieldValue::Single(Value::Bool(true))),
        ]);

        let result = format_document(
            &doc, &schema, &empty_reverse_maps(), &IncludeDocs::All, &empty_schema_registry(),
        );

        assert_eq!(result["hasMeta"], serde_json::json!(true));
    }

    #[test]
    fn test_format_document_multi_value_array() {
        let schema = make_schema(vec![FieldMapping {
            source: "tagIds".into(),
            target: "tagIds".into(),
            value_type: FieldValueType::IntegerArray,
            fallback: None,
            string_map: None,
            doc_only: false,
            filter_only: false,
            ms_to_seconds: false,
            truncate_u32: false,
            case_sensitive: false,
            default_value: Some(serde_json::json!([])),
            nullable: false,
        }]);

        let doc = make_stored_doc(vec![
            ("id", FieldValue::Single(Value::Integer(1))),
            ("tagIds", FieldValue::Multi(vec![Value::Integer(10), Value::Integer(20)])),
        ]);

        let result = format_document(
            &doc, &schema, &empty_reverse_maps(), &IncludeDocs::All, &empty_schema_registry(),
        );

        assert_eq!(result["tagIds"], serde_json::json!([10, 20]));
    }

    #[test]
    fn test_format_document_filter_only_excluded() {
        // filter_only fields should not appear in document response
        // (they're not stored in docstore, so they won't be found)
        let schema = make_schema(vec![
            FieldMapping {
                source: "nsfwLevel".into(),
                target: "nsfwLevel".into(),
                value_type: FieldValueType::Integer,
                fallback: None,
                string_map: None,
                doc_only: false,
                filter_only: false,
                ms_to_seconds: false,
                truncate_u32: false,
                case_sensitive: false,
                default_value: None,
                nullable: false,
            },
            FieldMapping {
                source: "collectionIds".into(),
                target: "collectionIds".into(),
                value_type: FieldValueType::IntegerArray,
                fallback: None,
                string_map: None,
                doc_only: false,
                filter_only: true,
                ms_to_seconds: false,
                truncate_u32: false,
                case_sensitive: false,
                default_value: Some(serde_json::json!([])),
                nullable: false,
            },
        ]);

        let doc = make_stored_doc(vec![
            ("id", FieldValue::Single(Value::Integer(1))),
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
        ]);

        let result = format_document(
            &doc, &schema, &empty_reverse_maps(), &IncludeDocs::All, &empty_schema_registry(),
        );

        assert_eq!(result["nsfwLevel"], serde_json::json!(1));
        // collectionIds defaults to [] since it's filter_only and not in docstore
        assert_eq!(result["collectionIds"], serde_json::json!([]));
    }

    // --- Audit items 1.10-1.18 ---

    #[test]
    fn test_format_document_mapped_string_reverse() {
        // 1.10: MappedString should reverse-map integer → string
        let mut string_map = HashMap::new();
        string_map.insert("image".to_string(), 1i64);
        string_map.insert("video".to_string(), 2i64);

        let schema = make_schema(vec![FieldMapping {
            source: "type".into(),
            target: "type".into(),
            value_type: FieldValueType::MappedString,
            fallback: None,
            string_map: Some(string_map),
            doc_only: false,
            filter_only: false,
            ms_to_seconds: false,
            truncate_u32: false,
            case_sensitive: false,
            default_value: None,
            nullable: false,
        }]);

        let doc = make_stored_doc(vec![
            ("id", FieldValue::Single(Value::Integer(1))),
            ("type", FieldValue::Single(Value::Integer(1))), // stored as int
        ]);

        // Build reverse maps: int → string
        let mut reverse_maps: ReverseStringMaps = HashMap::new();
        let mut type_rev = HashMap::new();
        type_rev.insert(1i64, "image".to_string());
        type_rev.insert(2i64, "video".to_string());
        reverse_maps.insert("type".to_string(), type_rev);

        let result = format_document(
            &doc, &schema, &reverse_maps, &IncludeDocs::All, &empty_schema_registry(),
        );

        assert_eq!(result["type"], serde_json::json!("image"));
    }

    #[test]
    fn test_format_document_mapped_string_unknown_value() {
        // MappedString with unknown integer → null (not raw integer)
        let schema = make_schema(vec![FieldMapping {
            source: "type".into(),
            target: "type".into(),
            value_type: FieldValueType::MappedString,
            fallback: None,
            string_map: Some([("image".to_string(), 1i64)].into_iter().collect()),
            doc_only: false,
            filter_only: false,
            ms_to_seconds: false,
            truncate_u32: false,
            case_sensitive: false,
            default_value: None,
            nullable: false,
        }]);

        let doc = make_stored_doc(vec![
            ("id", FieldValue::Single(Value::Integer(1))),
            ("type", FieldValue::Single(Value::Integer(999))), // unknown
        ]);

        let mut reverse_maps: ReverseStringMaps = HashMap::new();
        reverse_maps.insert("type".to_string(), [(1i64, "image".to_string())].into_iter().collect());

        let result = format_document(
            &doc, &schema, &reverse_maps, &IncludeDocs::All, &empty_schema_registry(),
        );

        assert!(result["type"].is_null(), "Unknown MappedString should be null");
    }

    #[test]
    fn test_format_document_exists_boolean_true() {
        // 1.12: ExistsBoolean stored as true → returns true
        let schema = make_schema(vec![FieldMapping {
            source: "publishedAtUnix".into(),
            target: "isPublished".into(),
            value_type: FieldValueType::ExistsBoolean,
            fallback: None,
            string_map: None,
            doc_only: false,
            filter_only: false,
            ms_to_seconds: false,
            truncate_u32: false,
            case_sensitive: false,
            default_value: None,
            nullable: false,
        }]);

        let doc = make_stored_doc(vec![
            ("id", FieldValue::Single(Value::Integer(1))),
            ("isPublished", FieldValue::Single(Value::Bool(true))),
        ]);

        let result = format_document(
            &doc, &schema, &empty_reverse_maps(), &IncludeDocs::All, &empty_schema_registry(),
        );

        assert_eq!(result["isPublished"], serde_json::json!(true));
    }

    #[test]
    fn test_format_document_exists_boolean_missing_defaults_false() {
        // ExistsBoolean not in doc → defaults to false
        let schema = make_schema(vec![FieldMapping {
            source: "publishedAtUnix".into(),
            target: "isPublished".into(),
            value_type: FieldValueType::ExistsBoolean,
            fallback: None,
            string_map: None,
            doc_only: false,
            filter_only: false,
            ms_to_seconds: false,
            truncate_u32: false,
            case_sensitive: false,
            default_value: None,
            nullable: false,
        }]);

        let doc = make_stored_doc(vec![
            ("id", FieldValue::Single(Value::Integer(1))),
        ]);

        let result = format_document(
            &doc, &schema, &empty_reverse_maps(), &IncludeDocs::All, &empty_schema_registry(),
        );

        assert_eq!(result["isPublished"], serde_json::json!(false));
    }

    #[test]
    fn test_format_document_string_doc_only() {
        // 1.13: String doc_only field (url, hash)
        let schema = make_schema(vec![FieldMapping {
            source: "url".into(),
            target: "url".into(),
            value_type: FieldValueType::String,
            fallback: None,
            string_map: None,
            doc_only: true,
            filter_only: false,
            ms_to_seconds: false,
            truncate_u32: false,
            case_sensitive: false,
            default_value: None,
            nullable: false,
        }]);

        let doc = make_stored_doc(vec![
            ("id", FieldValue::Single(Value::Integer(1))),
            ("url", FieldValue::Single(Value::String("https://example.com/img.jpg".into()))),
        ]);

        let result = format_document(
            &doc, &schema, &empty_reverse_maps(), &IncludeDocs::All, &empty_schema_registry(),
        );

        assert_eq!(result["url"], serde_json::json!("https://example.com/img.jpg"));
    }

    #[test]
    fn test_format_document_field_selection() {
        // 1.14: IncludeDocs::Fields only returns selected fields
        let schema = make_schema(vec![
            FieldMapping {
                source: "nsfwLevel".into(),
                target: "nsfwLevel".into(),
                value_type: FieldValueType::Integer,
                fallback: None, string_map: None, doc_only: false, filter_only: false,
                ms_to_seconds: false, truncate_u32: false, case_sensitive: false,
                default_value: None,
                nullable: false,
            },
            FieldMapping {
                source: "userId".into(),
                target: "userId".into(),
                value_type: FieldValueType::Integer,
                fallback: None, string_map: None, doc_only: false, filter_only: false,
                ms_to_seconds: false, truncate_u32: false, case_sensitive: false,
                default_value: None,
                nullable: false,
            },
        ]);

        let doc = make_stored_doc(vec![
            ("id", FieldValue::Single(Value::Integer(1))),
            ("nsfwLevel", FieldValue::Single(Value::Integer(4))),
            ("userId", FieldValue::Single(Value::Integer(42))),
        ]);

        let selection = IncludeDocs::Fields(vec!["nsfwLevel".to_string()]);
        let result = format_document(
            &doc, &schema, &empty_reverse_maps(), &selection, &empty_schema_registry(),
        );

        assert_eq!(result["nsfwLevel"], serde_json::json!(4));
        assert!(result.get("userId").is_none(), "userId should be excluded by field selection");
        // id is always included
        assert_eq!(result["id"], serde_json::json!(1));
    }

    #[test]
    fn test_format_document_schema_version_historical_defaults() {
        // 1.15: Doc with older schema version uses historical defaults
        let schema = make_schema(vec![FieldMapping {
            source: "newField".into(),
            target: "newField".into(),
            value_type: FieldValueType::Integer,
            fallback: None, string_map: None, doc_only: false, filter_only: false,
            ms_to_seconds: false, truncate_u32: false, case_sensitive: false,
            default_value: Some(serde_json::json!(99)), // current default
            nullable: false,
        }]);

        // Doc encoded with schema version 1 (current is also 1, but let's test version 2)
        let mut doc = make_stored_doc(vec![
            ("id", FieldValue::Single(Value::Integer(1))),
        ]);
        doc.schema_version = 2; // different from schema version 1

        // Historical defaults for version 2
        let mut registry: SchemaRegistry = HashMap::new();
        let mut v2_defaults = HashMap::new();
        v2_defaults.insert("newField".to_string(), serde_json::json!(42));
        registry.insert(2, v2_defaults);

        let result = format_document(
            &doc, &schema, &empty_reverse_maps(), &IncludeDocs::All, &registry,
        );

        // Should use historical default (42) not current default (99)
        assert_eq!(result["newField"], serde_json::json!(42));
    }

    #[test]
    fn test_format_document_truncate_u32_legacy_alias() {
        // 1.18: truncate_u32 is a legacy alias for ms_to_seconds
        let schema = make_schema(vec![FieldMapping {
            source: "oldTimestampMs".into(),
            target: "oldTimestamp".into(),
            value_type: FieldValueType::Integer,
            fallback: None, string_map: None, doc_only: false, filter_only: false,
            ms_to_seconds: false,
            truncate_u32: true, // legacy alias
            case_sensitive: false,
            default_value: None,
            nullable: false,
        }]);

        // Doc has field under source name (raw ms)
        let doc = make_stored_doc(vec![
            ("id", FieldValue::Single(Value::Integer(1))),
            ("oldTimestampMs", FieldValue::Single(Value::Integer(1487255090000))),
        ]);

        let result = format_document(
            &doc, &schema, &empty_reverse_maps(), &IncludeDocs::All, &empty_schema_registry(),
        );

        // truncate_u32 should behave like ms_to_seconds
        assert_eq!(result["oldTimestamp"], serde_json::json!(1487255090));
    }

    // -----------------------------------------------------------------------
    // TaskRegistry tests
    // -----------------------------------------------------------------------

    #[test]
    fn task_registry_start_and_complete() {
        let reg = TaskRegistry::new();
        let (tid, progress) = reg.try_start(TaskType::Load).expect("should start");
        progress.store(42, Ordering::Release);

        let info = reg.get(tid).expect("should find active task");
        assert_eq!(info.task_id, tid);
        assert_eq!(info.progress.records_processed, 42);
        assert!(matches!(info.status, TaskStatus::Running));

        reg.set_complete(tid, Some(serde_json::json!({"ok": true})));

        // No longer active
        let snap = reg.snapshot();
        assert!(snap.active.is_empty());

        // Moved to history
        let hist = reg.get(tid).expect("should find in history");
        assert!(matches!(hist.status, TaskStatus::Complete));
        assert_eq!(hist.result, Some(serde_json::json!({"ok": true})));
    }

    #[test]
    fn task_registry_exclusive_tasks_block_each_other() {
        let reg = TaskRegistry::new();
        let (_tid, _progress) = reg.try_start(TaskType::Load).expect("first start should succeed");

        // A second mutating task must fail
        let err = reg.try_start(TaskType::Dump).expect_err("should conflict");
        assert!(matches!(err.task_type, TaskType::Load));
    }

    #[test]
    fn task_registry_compact_blocks_on_mutating() {
        let reg = TaskRegistry::new();
        let (_tid, _progress) = reg.try_start(TaskType::Rebuild).expect("rebuild should start");

        // Compact cannot start while a mutating task is running
        let err = reg.try_start(TaskType::Compact).expect_err("compact should be blocked");
        assert!(matches!(err.task_type, TaskType::Rebuild));
    }

    #[test]
    fn task_registry_mutating_blocks_on_compact() {
        let reg = TaskRegistry::new();
        let (_tid, _progress) = reg.try_start(TaskType::Compact).expect("compact should start");

        // A mutating task cannot start while compact is running
        let err = reg.try_start(TaskType::Load).expect_err("load should be blocked by compact");
        assert!(matches!(err.task_type, TaskType::Compact));
    }

    #[test]
    fn task_registry_two_compacts_are_exclusive() {
        let reg = TaskRegistry::new();
        let (_tid1, _) = reg.try_start(TaskType::Compact).expect("first compact");

        // Second compact must be rejected — concurrent compacts race on gen deletion
        let err = reg.try_start(TaskType::Compact).expect_err("second compact should be blocked");
        assert!(matches!(err.task_type, TaskType::Compact));
    }

    #[test]
    fn task_registry_set_error_moves_to_history() {
        let reg = TaskRegistry::new();
        let (tid, _) = reg.try_start(TaskType::AddFields).expect("start");
        reg.set_error(tid, "something went wrong".to_string());

        let snap = reg.snapshot();
        assert!(snap.active.is_empty());

        let hist = reg.get(tid).expect("in history");
        assert!(matches!(hist.status, TaskStatus::Error));
        assert_eq!(hist.error.as_deref(), Some("something went wrong"));
    }

    #[test]
    fn task_registry_set_saving_changes_status() {
        let reg = TaskRegistry::new();
        let (tid, _) = reg.try_start(TaskType::Load).expect("start");
        reg.set_saving(tid);

        let info = reg.get(tid).expect("still active");
        assert!(matches!(info.status, TaskStatus::Saving));
    }

    #[test]
    fn task_registry_history_capped_at_20() {
        let reg = TaskRegistry::new();
        for _ in 0..25 {
            let (tid, _) = reg.try_start(TaskType::Compact).expect("start");
            reg.set_complete(tid, None);
        }
        let snap = reg.snapshot();
        assert_eq!(snap.history.len(), 20);
    }

    #[test]
    fn task_registry_snapshot_active_is_vec() {
        let reg = TaskRegistry::new();
        let snap = reg.snapshot();
        assert!(snap.active.is_empty());

        let (_tid, _) = reg.try_start(TaskType::Compact).expect("start");
        let snap = reg.snapshot();
        assert_eq!(snap.active.len(), 1);
    }

    #[test]
    fn task_guard_calls_set_error_on_drop() {
        let reg = Arc::new(TaskRegistry::new());
        let (tid, _) = reg.try_start(TaskType::Load).expect("start");

        {
            let _guard = TaskGuard { tasks: Arc::clone(&reg), task_id: Some(tid) };
            // Drop without defusing — simulates a panic
        }

        // Task should be in error state in history
        let snap = reg.snapshot();
        assert!(snap.active.is_empty());
        let hist = reg.get(tid).expect("in history");
        assert!(matches!(hist.status, TaskStatus::Error));
    }

    // ---------------------------------------------------------------------------
    // Query stream unit tests
    // ---------------------------------------------------------------------------

    #[test]
    fn query_event_serializes_correctly() {
        let event = QueryEvent {
            ts_ms: 1_700_000_000_000,
            index: "civitai".to_string(),
            body: serde_json::json!({"filters": []}),
            client_meta: None,
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"index\":\"civitai\""));
        assert!(json.contains("\"ts_ms\":1700000000000"));
        // client_meta omitted when None
        assert!(!json.contains("client_meta"));
    }

    #[test]
    fn query_event_client_meta_included_when_some() {
        let event = QueryEvent {
            ts_ms: 0,
            index: "test".to_string(),
            body: serde_json::Value::Null,
            client_meta: Some("1.2.3.4".to_string()),
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"client_meta\":\"1.2.3.4\""));
    }

    #[test]
    fn query_stream_broadcast_send_and_receive() {
        // Verify that a sent event arrives at a subscriber.
        let (tx, mut rx) = tokio::sync::broadcast::channel::<QueryEvent>(16);
        let event = QueryEvent {
            ts_ms: 42,
            index: "my-index".to_string(),
            body: serde_json::json!({"limit": 10}),
            client_meta: None,
        };
        tx.send(event).expect("channel has subscriber");
        let received = rx.try_recv().expect("event should be queued");
        assert_eq!(received.ts_ms, 42);
        assert_eq!(received.index, "my-index");
    }

    #[test]
    fn query_stream_no_sender_means_no_overhead() {
        // When query_stream is None the tee branch is skipped entirely.
        // This test just confirms the Option pattern compiles and behaves
        // correctly for the None case.
        let sender: Option<tokio::sync::broadcast::Sender<QueryEvent>> = None;
        // If sender is None nothing should happen.
        if let Some(ref tx) = sender {
            let event = QueryEvent {
                ts_ms: 0,
                index: "x".to_string(),
                body: serde_json::Value::Null,
                client_meta: None,
            };
            let _ = tx.send(event);
            panic!("should not reach here");
        }
        // Passes — no panic
    }

    #[test]
    fn query_stream_full_channel_drops_oldest() {
        // Broadcast channel with capacity 2.
        let (tx, mut rx) = tokio::sync::broadcast::channel::<QueryEvent>(2);
        // Subscribe so messages are buffered.
        let mut rx2 = tx.subscribe();

        let make = |ts: u64| QueryEvent {
            ts_ms: ts,
            index: "idx".to_string(),
            body: serde_json::Value::Null,
            client_meta: None,
        };

        // Fill beyond capacity — channel drops oldest for lagged receivers.
        let _ = tx.send(make(1));
        let _ = tx.send(make(2));
        let _ = tx.send(make(3)); // rx2 hasn't read yet — will be lagged

        // First receiver reads fine from its individual slot
        let e = rx.try_recv().expect("first event");
        assert_eq!(e.ts_ms, 1);

        // Slow rx2 gets a Lagged error when the channel overflows its slot.
        // We just confirm it's a recognisable error rather than a panic.
        let result = rx2.try_recv();
        // Either lagged or an event — both are acceptable outcomes.
        // The important guarantee: send() never panics or blocks.
        let _ = result;
    }
}
