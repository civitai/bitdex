//! Shared load progress state and HTTP endpoint for monitoring bulk loads.

use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use serde_json::json;

/// Shared progress state for bulk load monitoring.
/// All fields are atomic for concurrent access from multiple stream tasks.
pub struct LoadProgress {
    /// Load phase: 0=setup, 1=streaming, 2=cleanup, 3=applying, 4=finalizing, 5=saving, 6=done
    pub phase: AtomicU8,
    /// Wall clock start time
    start_time: Instant,
    /// Per-stream row counters
    pub image_rows: AtomicU64,
    pub tag_rows: AtomicU64,
    pub tool_rows: AtomicU64,
    pub technique_rows: AtomicU64,
    pub resource_rows: AtomicU64,
    /// Number of streams that have completed (out of 5)
    pub streams_done: AtomicU8,
}

impl LoadProgress {
    pub fn new() -> Self {
        Self {
            phase: AtomicU8::new(0),
            start_time: Instant::now(),
            image_rows: AtomicU64::new(0),
            tag_rows: AtomicU64::new(0),
            tool_rows: AtomicU64::new(0),
            technique_rows: AtomicU64::new(0),
            resource_rows: AtomicU64::new(0),
            streams_done: AtomicU8::new(0),
        }
    }

    pub fn elapsed_secs(&self) -> f64 {
        self.start_time.elapsed().as_secs_f64()
    }

    pub fn set_phase(&self, phase: u8) {
        self.phase.store(phase, Ordering::Release);
    }
}

async fn status_handler(State(progress): State<Arc<LoadProgress>>) -> Json<serde_json::Value> {
    let elapsed = progress.elapsed_secs();
    let phase = progress.phase.load(Ordering::Acquire);
    let images = progress.image_rows.load(Ordering::Relaxed);
    let tags = progress.tag_rows.load(Ordering::Relaxed);
    let tools = progress.tool_rows.load(Ordering::Relaxed);
    let techniques = progress.technique_rows.load(Ordering::Relaxed);
    let resources = progress.resource_rows.load(Ordering::Relaxed);
    let done = progress.streams_done.load(Ordering::Relaxed);

    let phase_name = match phase {
        0 => "setup",
        1 => "streaming",
        2 => "cleanup",
        3 => "applying",
        4 => "finalizing",
        5 => "saving",
        6 => "done",
        _ => "unknown",
    };

    Json(json!({
        "phase": phase_name,
        "elapsed_secs": (elapsed * 10.0).round() / 10.0,
        "streams_done": done,
        "streams": {
            "images": { "rows": images, "rate": if elapsed > 0.0 { (images as f64 / elapsed).round() } else { 0.0 } },
            "tags": { "rows": tags, "rate": if elapsed > 0.0 { (tags as f64 / elapsed).round() } else { 0.0 } },
            "tools": { "rows": tools, "rate": if elapsed > 0.0 { (tools as f64 / elapsed).round() } else { 0.0 } },
            "techniques": { "rows": techniques, "rate": if elapsed > 0.0 { (techniques as f64 / elapsed).round() } else { 0.0 } },
            "resources": { "rows": resources, "rate": if elapsed > 0.0 { (resources as f64 / elapsed).round() } else { 0.0 } },
        }
    }))
}

/// Spawn the progress HTTP server in a background tokio task.
/// Returns the shutdown sender — send `()` to gracefully stop the server.
pub fn spawn_progress_server(
    port: u16,
    progress: Arc<LoadProgress>,
) -> tokio::sync::oneshot::Sender<()> {
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    tokio::spawn(async move {
        let app = Router::new()
            .route("/status", get(status_handler))
            .with_state(progress);

        let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        eprintln!("Progress server listening on {addr}");

        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                rx.await.ok();
            })
            .await
            .ok();
    });

    tx
}
