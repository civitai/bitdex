//! Snapshot capture session management.
//!
//! Manages the lifecycle of traffic + state captures for production debugging.
//! A capture session records all HTTP requests during a time window and pins
//! ShardStore generations at the boundaries for later replay.
//!
//! ## Lifecycle
//!
//! ```text
//! Idle → Recording (start) → Stopped (stop) → Idle (reset)
//! ```
//!
//! ## Integration points
//!
//! - **Traffic recording**: axum middleware checks `is_recording()` and appends to caplog
//! - **Gen pin**: On start/stop, calls a hook to bump the ShardStore generation counter
//!   (placeholder until Adam lands ShardStore — currently a no-op)
//! - **Prometheus scrape**: Metrics snapshot saved at start and stop boundaries

use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime};

use parking_lot::Mutex;

/// Capture session state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CaptureState {
    /// No capture in progress.
    Idle,
    /// Actively recording traffic.
    Recording,
    /// Capture stopped, session data available.
    Stopped,
}

/// Configuration for a capture session.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct CaptureStartRequest {
    /// Maximum capture duration in seconds. Auto-stops after this.
    #[serde(default = "default_duration")]
    pub duration_seconds: u64,
}

fn default_duration() -> u64 {
    300 // 5 minutes
}

/// A single capture session.
#[derive(Debug)]
pub struct CaptureSession {
    /// Unique session identifier (timestamp-based).
    pub session_id: String,
    /// Current state.
    pub state: CaptureState,
    /// When the capture started (wall clock).
    pub started_at: Option<SystemTime>,
    /// When the capture stopped (wall clock).
    pub stopped_at: Option<SystemTime>,
    /// Monotonic start instant for duration tracking.
    start_instant: Option<Instant>,
    /// Configured max duration in seconds.
    pub duration_seconds: u64,
    /// Number of requests recorded during this session.
    pub requests_recorded: u64,
    /// Directory where session data is stored.
    pub session_dir: PathBuf,
    /// Path to metrics_start.prom (written on capture start).
    pub metrics_start_path: Option<PathBuf>,
    /// Path to metrics_stop.prom (written on capture stop).
    pub metrics_stop_path: Option<PathBuf>,
    /// ShardStore generation pinned at capture start (pre-capture state).
    pub gen_start: Option<u64>,
    /// ShardStore generation pinned at capture stop (mutations during capture).
    pub gen_stop: Option<u64>,
}

// ---------------------------------------------------------------------------
// Caplog: append-only binary traffic log
// ---------------------------------------------------------------------------

/// A single recorded request/response pair.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CaptureEntry {
    /// Nanosecond timestamp when the request arrived.
    pub arrived_at_ns: u64,
    /// HTTP method (GET, POST, etc.).
    pub method: String,
    /// Request path (e.g., /api/indexes/civitai/query).
    pub path: String,
    /// Query string (e.g., "format=compact&skip_cache=true").
    pub query_string: String,
    /// Request body (for POST/PATCH/PUT). Empty for GET/DELETE.
    pub body: Vec<u8>,
    /// HTTP response status code.
    pub response_status: u16,
    /// Response body bytes.
    pub response_body: Vec<u8>,
    /// Nanosecond timestamp when the response was sent.
    pub responded_at_ns: u64,
}

/// Thread-safe append-only caplog writer. Writes msgpack-encoded CaptureEntry
/// records to a file. Each record is length-prefixed: [u32 len][msgpack bytes].
struct CaplogWriter {
    writer: BufWriter<std::fs::File>,
    entries_written: u64,
}

impl CaplogWriter {
    fn new(path: &Path) -> std::io::Result<Self> {
        let file = std::fs::File::create(path)?;
        Ok(Self {
            writer: BufWriter::with_capacity(64 * 1024, file),
            entries_written: 0,
        })
    }

    fn write_entry(&mut self, entry: &CaptureEntry) -> std::io::Result<()> {
        let data = rmp_serde::to_vec(entry)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        let len = data.len() as u32;
        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(&data)?;
        self.entries_written += 1;
        // Flush every 100 entries to balance throughput and durability
        if self.entries_written % 100 == 0 {
            self.writer.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

/// Thread-safe capture manager. Holds at most one active session.
pub struct CaptureManager {
    session: Mutex<Option<CaptureSession>>,
    /// Atomic counter incremented by the traffic recording middleware.
    requests_counter: AtomicU64,
    /// Base directory for capture data (e.g., /data/captures/).
    base_dir: PathBuf,
    /// Active caplog writer (only Some when recording).
    caplog: Mutex<Option<CaplogWriter>>,
    /// Package states keyed by session_id.
    packages: Mutex<std::collections::HashMap<String, SharedPackageState>>,
}

impl CaptureManager {
    /// Create a new capture manager.
    pub fn new(data_dir: &Path) -> Self {
        let base_dir = data_dir.join("captures");
        Self {
            session: Mutex::new(None),
            requests_counter: AtomicU64::new(0),
            base_dir,
            caplog: Mutex::new(None),
            packages: Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Start a new capture session. Returns error if one is already in progress.
    pub fn start(&self, req: &CaptureStartRequest) -> Result<CaptureStatus, CaptureError> {
        let mut guard = self.session.lock();
        if let Some(ref s) = *guard {
            if s.state == CaptureState::Recording {
                return Err(CaptureError::AlreadyRecording(s.session_id.clone()));
            }
        }

        let session_id = generate_session_id();
        let session_dir = self.base_dir.join(&session_id);
        std::fs::create_dir_all(&session_dir)
            .map_err(|e| CaptureError::Io(format!("create session dir: {e}")))?;

        self.requests_counter.store(0, Ordering::Release);

        // Gen pin is called by the server handler after start() returns.
        // CaptureManager doesn't hold a reference to the engine.

        // Open caplog writer
        let caplog_path = session_dir.join("traffic.caplog");
        let writer = CaplogWriter::new(&caplog_path)
            .map_err(|e| CaptureError::Io(format!("create caplog: {e}")))?;
        *self.caplog.lock() = Some(writer);

        let session = CaptureSession {
            session_id: session_id.clone(),
            state: CaptureState::Recording,
            started_at: Some(SystemTime::now()),
            stopped_at: None,
            start_instant: Some(Instant::now()),
            duration_seconds: req.duration_seconds,
            requests_recorded: 0,
            session_dir,
            metrics_start_path: None,
            metrics_stop_path: None,
            gen_start: None,
            gen_stop: None,
        };

        let status = session_to_status(&session, &self.requests_counter);
        *guard = Some(session);
        Ok(status)
    }

    /// Stop the current capture session. Returns error if not recording.
    pub fn stop(&self) -> Result<CaptureStatus, CaptureError> {
        let mut guard = self.session.lock();
        let session = guard.as_mut().ok_or(CaptureError::NoSession)?;
        if session.state != CaptureState::Recording {
            return Err(CaptureError::NotRecording);
        }

        // Gen pin is called by the server handler after stop() returns.

        // Flush and close caplog writer
        if let Some(ref mut writer) = *self.caplog.lock() {
            let _ = writer.flush();
        }
        *self.caplog.lock() = None;

        session.state = CaptureState::Stopped;
        session.stopped_at = Some(SystemTime::now());
        session.requests_recorded = self.requests_counter.load(Ordering::Acquire);

        Ok(session_to_status(session, &self.requests_counter))
    }

    /// Get the current capture status.
    pub fn status(&self) -> CaptureStatus {
        let guard = self.session.lock();
        match guard.as_ref() {
            Some(session) => session_to_status(session, &self.requests_counter),
            None => CaptureStatus {
                state: CaptureState::Idle,
                session_id: None,
                started_at: None,
                stopped_at: None,
                elapsed_seconds: None,
                duration_seconds: None,
                requests_recorded: 0,
                session_dir: None,
                gen_start: None,
                gen_stop: None,
            },
        }
    }

    /// Check if currently recording (called by traffic middleware).
    pub fn is_recording(&self) -> bool {
        let guard = self.session.lock();
        guard.as_ref().map_or(false, |s| s.state == CaptureState::Recording)
    }

    /// Increment the request counter (called by traffic middleware for each recorded request).
    pub fn record_request(&self) {
        self.requests_counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Write a capture entry to the caplog. Called by the traffic middleware.
    /// Returns false if not recording or write failed (non-fatal).
    pub fn write_entry(&self, entry: &CaptureEntry) -> bool {
        if let Some(ref mut writer) = *self.caplog.lock() {
            if writer.write_entry(entry).is_ok() {
                self.requests_counter.fetch_add(1, Ordering::Relaxed);
                return true;
            }
        }
        false
    }

    /// Check if auto-stop duration has been exceeded. Returns true if capture should auto-stop.
    pub fn should_auto_stop(&self) -> bool {
        let guard = self.session.lock();
        if let Some(ref s) = *guard {
            if s.state == CaptureState::Recording {
                if let Some(start) = s.start_instant {
                    return start.elapsed().as_secs() >= s.duration_seconds;
                }
            }
        }
        false
    }

    /// Set the path to the metrics snapshot taken at capture start.
    pub fn set_metrics_start_path(&self, path: PathBuf) {
        let mut guard = self.session.lock();
        if let Some(ref mut s) = *guard {
            s.metrics_start_path = Some(path);
        }
    }

    /// Set the path to the metrics snapshot taken at capture stop.
    pub fn set_metrics_stop_path(&self, path: PathBuf) {
        let mut guard = self.session.lock();
        if let Some(ref mut s) = *guard {
            s.metrics_stop_path = Some(path);
        }
    }

    /// Record the ShardStore generation pinned at capture start.
    pub fn set_gen_start(&self, gen: u64) {
        let mut guard = self.session.lock();
        if let Some(ref mut s) = *guard {
            s.gen_start = Some(gen);
        }
    }

    /// Record the ShardStore generation pinned at capture stop.
    pub fn set_gen_stop(&self, gen: u64) {
        let mut guard = self.session.lock();
        if let Some(ref mut s) = *guard {
            s.gen_stop = Some(gen);
        }
    }

    /// Get the session directory for the current session (if any).
    pub fn session_dir(&self) -> Option<PathBuf> {
        let guard = self.session.lock();
        guard.as_ref().map(|s| s.session_dir.clone())
    }

    /// Start packaging a session. Returns the shared state for status polling.
    /// The caller should spawn a blocking task that calls `create_package()`.
    pub fn start_package(&self, session_id: &str) -> Result<(PathBuf, SharedPackageState), CaptureError> {
        // Find the session dir
        let session_dir = self.base_dir.join(session_id);
        if !session_dir.is_dir() {
            return Err(CaptureError::Io(format!("session dir not found: {}", session_dir.display())));
        }

        // Check if already packaging
        let mut packages = self.packages.lock();
        if let Some(existing) = packages.get(session_id) {
            let state = existing.lock().unwrap();
            match &*state {
                PackageState::Compressing => return Err(CaptureError::Io("packaging already in progress".into())),
                PackageState::Ready { .. } => return Err(CaptureError::Io("package already exists".into())),
                _ => {}
            }
        }

        let state = std::sync::Arc::new(std::sync::Mutex::new(PackageState::Pending));
        packages.insert(session_id.to_string(), state.clone());
        Ok((session_dir, state))
    }

    /// Get the package state for a session.
    pub fn package_state(&self, session_id: &str) -> Option<PackageState> {
        let packages = self.packages.lock();
        packages.get(session_id).map(|s| s.lock().unwrap().clone())
    }

    /// Get the path to a completed package file.
    pub fn package_path(&self, session_id: &str) -> Option<PathBuf> {
        let packages = self.packages.lock();
        if let Some(state) = packages.get(session_id) {
            if let PackageState::Ready { ref path, .. } = *state.lock().unwrap() {
                return Some(PathBuf::from(path));
            }
        }
        None
    }

    /// Reset to idle state, discarding the current session reference.
    pub fn reset(&self) {
        let mut guard = self.session.lock();
        *guard = None;
    }
}

// ---------------------------------------------------------------------------
// Snapshot packaging: tar.zst creation from captured session
// ---------------------------------------------------------------------------

/// Package creation state.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PackageState {
    /// Not yet started.
    Pending,
    /// Compressing session data into tar.zst.
    Compressing,
    /// Package ready for download.
    Ready {
        path: String,
        size_bytes: u64,
    },
    /// Packaging failed.
    Failed {
        error: String,
    },
}

/// Shared package status, updated by the background task.
pub type SharedPackageState = std::sync::Arc<std::sync::Mutex<PackageState>>;

/// Create a tar.zst package from a capture session directory.
/// Package mode controls what's included in the snapshot archive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PackageMode {
    /// Traffic capture only: caplog + metrics + manifest (~100MB).
    MetricsOnly,
    /// Full snapshot: caplog + metrics + bitmaps (~7GB compressed). No docstore.
    Bitmaps,
    /// Everything: caplog + metrics + bitmaps + docstore (~20-30GB compressed).
    Full,
}

impl Default for PackageMode {
    fn default() -> Self {
        PackageMode::MetricsOnly
    }
}

/// Runs synchronously — call from a blocking task or spawn_blocking.
///
/// Creates a zstd-compressed tar archive containing the session data and
/// optionally the bitmap/docstore shard data from the data directory.
/// Output: `{session_dir}.snapshot.tar.zst`
pub fn create_package(
    session_dir: &Path,
    data_dir: &Path,
    mode: PackageMode,
    state: &SharedPackageState,
) -> Result<PathBuf, String> {
    *state.lock().unwrap() = PackageState::Compressing;

    // Write manifest.json
    let manifest = serde_json::json!({
        "session_id": session_dir.file_name().and_then(|n| n.to_str()).unwrap_or("unknown"),
        "packaged_at": format_system_time(SystemTime::now()),
        "mode": format!("{:?}", mode),
        "files": list_session_files(session_dir),
    });
    let manifest_path = session_dir.join("manifest.json");
    std::fs::write(&manifest_path, serde_json::to_string_pretty(&manifest).unwrap_or_default())
        .map_err(|e| format!("write manifest: {e}"))?;

    // Create tar.zst
    let output_path = session_dir.with_extension("snapshot.tar.zst");
    let file = std::fs::File::create(&output_path)
        .map_err(|e| format!("create tar.zst: {e}"))?;
    let zstd_encoder = zstd::Encoder::new(file, 3)
        .map_err(|e| format!("zstd encoder: {e}"))?;
    let mut tar_builder = tar::Builder::new(zstd_encoder);

    // 1. Always include session dir (caplog + metrics + manifest)
    let session_name = session_dir.file_name().and_then(|n| n.to_str()).unwrap_or("session");
    for entry in walkdir(session_dir).map_err(|e| format!("walk session dir: {e}"))? {
        let rel_path = entry.strip_prefix(session_dir)
            .map_err(|e| format!("strip prefix: {e}"))?;
        let archive_path = PathBuf::from(session_name).join(rel_path);
        tar_builder.append_path_with_name(&entry, &archive_path)
            .map_err(|e| format!("tar append {}: {e}", entry.display()))?;
    }

    // 2. Include bitmap shard data if mode is Bitmaps or Full
    if mode == PackageMode::Bitmaps || mode == PackageMode::Full {
        // Find the index data dir (e.g., /data/indexes/civitai/)
        let indexes_dir = data_dir.join("indexes");
        if indexes_dir.is_dir() {
            for index_entry in std::fs::read_dir(&indexes_dir).map_err(|e| format!("read indexes: {e}"))? {
                let index_entry = index_entry.map_err(|e| format!("index entry: {e}"))?;
                let index_path = index_entry.path();
                if !index_path.is_dir() { continue; }

                // Include shardstore/ (bitmap shard files)
                let shardstore = index_path.join("shardstore");
                if shardstore.is_dir() {
                    for file in walkdir(&shardstore).map_err(|e| format!("walk shardstore: {e}"))? {
                        let rel = file.strip_prefix(data_dir).map_err(|e| format!("strip: {e}"))?;
                        let archive_path = PathBuf::from("data").join(rel);
                        tar_builder.append_path_with_name(&file, &archive_path)
                            .map_err(|e| format!("tar bitmap {}: {e}", file.display()))?;
                    }
                }

                // Include bitmaps/ (legacy BitmapFs, if present)
                let bitmaps = index_path.join("bitmaps");
                if bitmaps.is_dir() {
                    for file in walkdir(&bitmaps).map_err(|e| format!("walk bitmaps: {e}"))? {
                        let rel = file.strip_prefix(data_dir).map_err(|e| format!("strip: {e}"))?;
                        let archive_path = PathBuf::from("data").join(rel);
                        tar_builder.append_path_with_name(&file, &archive_path)
                            .map_err(|e| format!("tar bitmap {}: {e}", file.display()))?;
                    }
                }
            }
        }
    }

    // 3. Include docstore if mode is Full
    if mode == PackageMode::Full {
        let indexes_dir = data_dir.join("indexes");
        if indexes_dir.is_dir() {
            for index_entry in std::fs::read_dir(&indexes_dir).map_err(|e| format!("read indexes: {e}"))? {
                let index_entry = index_entry.map_err(|e| format!("index entry: {e}"))?;
                let index_path = index_entry.path();
                if !index_path.is_dir() { continue; }

                // DocStore directory: try "docs" (production name) then "docstore" (legacy)
                let docstore = if index_path.join("docs").is_dir() {
                    index_path.join("docs")
                } else {
                    index_path.join("docstore")
                };
                if docstore.is_dir() {
                    for file in walkdir(&docstore).map_err(|e| format!("walk docstore: {e}"))? {
                        let rel = file.strip_prefix(data_dir).map_err(|e| format!("strip: {e}"))?;
                        let archive_path = PathBuf::from("data").join(rel);
                        tar_builder.append_path_with_name(&file, &archive_path)
                            .map_err(|e| format!("tar doc {}: {e}", file.display()))?;
                    }
                }
            }
        }
    }

    let zstd_encoder = tar_builder.into_inner()
        .map_err(|e| format!("tar finalize: {e}"))?;
    zstd_encoder.finish()
        .map_err(|e| format!("zstd finish: {e}"))?;

    let size_bytes = std::fs::metadata(&output_path)
        .map(|m| m.len())
        .unwrap_or(0);

    *state.lock().unwrap() = PackageState::Ready {
        path: output_path.display().to_string(),
        size_bytes,
    };

    Ok(output_path)
}

/// Recursively list all files in a directory (non-recursive into subdirs for now).
fn walkdir(dir: &Path) -> Result<Vec<PathBuf>, std::io::Error> {
    let mut files = Vec::new();
    if !dir.is_dir() {
        return Ok(files);
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            files.push(path);
        } else if path.is_dir() {
            files.extend(walkdir(&path)?);
        }
    }
    files.sort(); // Deterministic order
    Ok(files)
}

/// List filenames in a session dir (for manifest).
fn list_session_files(dir: &Path) -> Vec<String> {
    walkdir(dir)
        .unwrap_or_default()
        .iter()
        .filter_map(|p| p.strip_prefix(dir).ok())
        .map(|p| p.display().to_string())
        .collect()
}

/// Serializable capture status for the HTTP response.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CaptureStatus {
    pub state: CaptureState,
    pub session_id: Option<String>,
    pub started_at: Option<String>,
    pub stopped_at: Option<String>,
    pub elapsed_seconds: Option<f64>,
    pub duration_seconds: Option<u64>,
    pub requests_recorded: u64,
    pub session_dir: Option<String>,
    /// ShardStore generation pinned at capture start.
    pub gen_start: Option<u64>,
    /// ShardStore generation pinned at capture stop.
    pub gen_stop: Option<u64>,
}

/// Errors from capture operations.
#[derive(Debug)]
pub enum CaptureError {
    AlreadyRecording(String),
    NotRecording,
    NoSession,
    Io(String),
}

impl std::fmt::Display for CaptureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CaptureError::AlreadyRecording(id) => write!(f, "capture already in progress: {id}"),
            CaptureError::NotRecording => write!(f, "no capture in progress"),
            CaptureError::NoSession => write!(f, "no capture session exists"),
            CaptureError::Io(e) => write!(f, "I/O error: {e}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn session_to_status(session: &CaptureSession, counter: &AtomicU64) -> CaptureStatus {
    let elapsed = session.start_instant.map(|i| i.elapsed().as_secs_f64());
    CaptureStatus {
        state: session.state,
        session_id: Some(session.session_id.clone()),
        started_at: session.started_at.map(format_system_time),
        stopped_at: session.stopped_at.map(format_system_time),
        elapsed_seconds: elapsed,
        duration_seconds: Some(session.duration_seconds),
        requests_recorded: if session.state == CaptureState::Stopped {
            session.requests_recorded
        } else {
            counter.load(Ordering::Relaxed)
        },
        session_dir: Some(session.session_dir.display().to_string()),
        gen_start: session.gen_start,
        gen_stop: session.gen_stop,
    }
}

fn generate_session_id() -> String {
    let dur = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    format!("{}-{:03}", dur.as_secs(), dur.subsec_millis())
}

fn format_system_time(t: SystemTime) -> String {
    let dur = t.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default();
    let secs = dur.as_secs();
    let millis = dur.subsec_millis();
    // Simple ISO 8601 without chrono
    let days = secs / 86400;
    let tod = secs % 86400;
    let h = tod / 3600;
    let m = (tod % 3600) / 60;
    let s = tod % 60;
    let (y, mo, d) = days_to_ymd(days);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{m:02}:{s:02}.{millis:03}Z")
}

/// Current time as nanoseconds since epoch.
pub fn nanos_since_epoch() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

fn days_to_ymd(mut days: u64) -> (u64, u64, u64) {
    days += 719468;
    let era = days / 146097;
    let doe = days - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_capture_lifecycle() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = CaptureManager::new(dir.path());

        // Initially idle
        let status = mgr.status();
        assert_eq!(status.state, CaptureState::Idle);
        assert!(!mgr.is_recording());

        // Start capture
        let req = CaptureStartRequest { duration_seconds: 60 };
        let status = mgr.start(&req).unwrap();
        assert_eq!(status.state, CaptureState::Recording);
        assert!(mgr.is_recording());

        // Record some requests
        mgr.record_request();
        mgr.record_request();
        mgr.record_request();
        let status = mgr.status();
        assert_eq!(status.requests_recorded, 3);

        // Can't start another while recording
        assert!(mgr.start(&req).is_err());

        // Stop capture
        let status = mgr.stop().unwrap();
        assert_eq!(status.state, CaptureState::Stopped);
        assert_eq!(status.requests_recorded, 3);
        assert!(!mgr.is_recording());

        // Can't stop again
        assert!(mgr.stop().is_err());

        // Reset
        mgr.reset();
        assert_eq!(mgr.status().state, CaptureState::Idle);
    }

    #[test]
    fn test_auto_stop_check() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = CaptureManager::new(dir.path());

        let req = CaptureStartRequest { duration_seconds: 0 }; // 0 seconds = immediate
        mgr.start(&req).unwrap();
        assert!(mgr.should_auto_stop());
    }

    #[test]
    fn test_session_id_format() {
        let id = generate_session_id();
        // Should be "{epoch_secs}-{millis}" format
        assert!(id.contains('-'));
        let parts: Vec<&str> = id.split('-').collect();
        assert_eq!(parts.len(), 2);
        assert!(parts[0].parse::<u64>().is_ok());
        assert!(parts[1].parse::<u64>().is_ok());
    }
}
