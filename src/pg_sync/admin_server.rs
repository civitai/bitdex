//! Sidecar admin HTTP listener.
//!
//! Exposes `POST /internal/restart` so the BitDex server can request that
//! the sidecar drop its PG cursor row and exit. Used by the server's
//! `/api/indexes/{name}/redump` flow: the server wipes its own data dir
//! and exits, the sidecar wipes its cursor and exits, k8s restarts both
//! containers, and the sidecar's fresh boot re-runs the dump pipeline.
//!
//! Binds to 127.0.0.1 only — the listener is meant to be reachable from
//! the BitDex server container in the same pod, not from outside.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::PgPool;

#[derive(Clone)]
pub struct AdminState {
    pub pool: PgPool,
    pub replica_id: Arc<str>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct RestartBody {
    reason: Option<String>,
}

async fn restart_handler(
    State(state): State<AdminState>,
    body: Option<Json<RestartBody>>,
) -> (StatusCode, Json<Value>) {
    let reason = body
        .and_then(|b| b.0.reason)
        .unwrap_or_else(|| "unspecified".to_string());
    eprintln!(
        "[admin] /internal/restart received (reason={reason}) — deleting cursor row for replica_id={}",
        state.replica_id
    );

    // Delete the cursor row for THIS replica only. Other pods' cursors stay.
    let delete_result = sqlx::query(
        r#"DELETE FROM bitdex_cursors WHERE replica_id = $1"#,
    )
    .bind(state.replica_id.as_ref())
    .execute(&state.pool)
    .await;

    let rows_affected = match delete_result {
        Ok(r) => {
            let n = r.rows_affected();
            eprintln!("[admin] cursor delete: {n} row(s) removed");
            if n == 0 && reason == "redump" {
                eprintln!(
                    "[admin] WARNING: cursor row for replica_id={} not found — possible config drift or already deleted",
                    state.replica_id
                );
            }
            n
        }
        Err(e) => {
            eprintln!("[admin] cursor delete FAILED: {e}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "ok": false, "error": format!("cursor delete failed: {e}") })),
            );
        }
    };

    // Spawn fire-and-forget exit after a small delay so this response can
    // reach the caller before the process dies. 200ms is enough for axum
    // to flush the response buffer in the loopback case.
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        eprintln!("[admin] exiting process (restart requested via /internal/restart)");
        std::process::exit(0);
    });

    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "replica_id": state.replica_id.as_ref(),
            "cursor_rows_deleted": rows_affected,
            "reason": reason,
        })),
    )
}

/// Spawn the admin HTTP listener on `127.0.0.1:port` in a background task.
/// Returns a shutdown sender — send `()` to gracefully stop the listener.
pub fn spawn_admin_server(
    port: u16,
    state: AdminState,
) -> tokio::sync::oneshot::Sender<()> {
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    tokio::spawn(async move {
        let app = Router::new()
            .route("/internal/restart", post(restart_handler))
            .with_state(state);

        let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
        let listener = match tokio::net::TcpListener::bind(addr).await {
            Ok(l) => l,
            Err(e) => {
                eprintln!("[admin] failed to bind {addr}: {e}");
                return;
            }
        };
        eprintln!("[admin] listening on {addr} (POST /internal/restart)");

        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                rx.await.ok();
            })
            .await
            .ok();
    });

    tx
}
