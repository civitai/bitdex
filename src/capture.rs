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
}

/// Thread-safe capture manager. Holds at most one active session.
pub struct CaptureManager {
    session: Mutex<Option<CaptureSession>>,
    /// Atomic counter incremented by the traffic recording middleware.
    requests_counter: AtomicU64,
    /// Base directory for capture data (e.g., /data/captures/).
    base_dir: PathBuf,
}

impl CaptureManager {
    /// Create a new capture manager.
    pub fn new(data_dir: &Path) -> Self {
        let base_dir = data_dir.join("captures");
        Self {
            session: Mutex::new(None),
            requests_counter: AtomicU64::new(0),
            base_dir,
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

        // TODO: Gen pin hook — bump ShardStore generation counter here
        // once Adam's ShardStore lands. For now this is a no-op.

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

        // TODO: Gen pin hook — bump generation counter again to bracket the capture window.

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

    /// Get the session directory for the current session (if any).
    pub fn session_dir(&self) -> Option<PathBuf> {
        let guard = self.session.lock();
        guard.as_ref().map(|s| s.session_dir.clone())
    }

    /// Reset to idle state, discarding the current session reference.
    pub fn reset(&self) {
        let mut guard = self.session.lock();
        *guard = None;
    }
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
