//! Sidecar admin HTTP listener.
//!
//! Exposes `POST /internal/restart` so the BitDex server can request that
//! the sidecar exit cleanly. Used by the server's
//! `/api/indexes/{name}/redump` flow: the server wipes its own data dir
//! and exits, the sidecar exits, k8s restarts both containers, and the
//! sidecar's fresh boot re-runs the dump pipeline.
//!
//! Binds to 127.0.0.1 only — the listener is meant to be reachable from
//! the BitDex server container in the same pod, not from outside.
//!
//! ## Why we DON'T delete the cursor row
//!
//! Earlier versions of this handler `DELETE`d `bitdex_cursors WHERE
//! replica_id = $self` before exit, the intent being to "remove the
//! cursor tied to this instance" so the new boot would start fresh.
//! That's unsafe: the cleanup trigger on `bitdex_cursors` deletes
//! `BitdexOps` rows below `MIN(last_outbox_id)`. With this replica's
//! row absent during the 5–15 min redump window, `MIN` collapses to
//! the surviving replica's cursor — which keeps advancing — and rows
//! the redumping replica's COPY snapshot did NOT capture but which
//! the surviving replica DID consume get garbage-collected before the
//! new sidecar resumes its ops poller. Net result: post-redump pod
//! silently misses outbox ops it would have replayed.
//!
//! Leaving the row intact pins `MIN` low during the redump window, so
//! cleanup is gated by this replica's stale (pre-redump) cursor value.
//! The new sidecar's boot then calls `upsert_cursor(pg-sync-{id},
//! pre_dump_cursor)` (`ON CONFLICT DO UPDATE`) — that single write
//! overwrites the stale value with the fresh `pre_dump_cursor` AND
//! fires the cleanup trigger once, advancing the cleanup floor in one
//! step. No ops lost, no double-application required.

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
    let cursor_key = super::pg_sync_cursor_key(&state.replica_id);
    eprintln!(
        "[admin] /internal/restart received (reason={reason}, replica_id={}, cursor_key={cursor_key}) — exiting; cursor row left intact so cleanup MIN stays gated",
        state.replica_id
    );

    // Sanity: surface whether our cursor row even exists. If it doesn't,
    // the safety property we rely on (this row pins MIN low) doesn't hold —
    // log loudly so an operator can audit cursor schema. Don't fail the
    // request; redump still works via the new boot's UPSERT.
    let cursor_exists: Result<Option<(i64,)>, sqlx::Error> = sqlx::query_as(
        r#"SELECT last_outbox_id FROM bitdex_cursors WHERE replica_id = $1"#,
    )
    .bind(&cursor_key)
    .fetch_optional(&state.pool)
    .await;
    let cursor_value = match cursor_exists {
        Ok(Some((v,))) => {
            eprintln!("[admin] cursor row present (key={cursor_key}, last_outbox_id={v})");
            Some(v)
        }
        Ok(None) => {
            eprintln!(
                "[admin] WARNING: cursor row for key={cursor_key} not found — \
                 cleanup MIN is unpinned during redump; if surviving replicas \
                 advance fast, BitdexOps rows may be GC'd before redumping pod resumes"
            );
            None
        }
        Err(e) => {
            eprintln!("[admin] cursor lookup FAILED (non-fatal, redump continues): {e}");
            None
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
            "cursor_key": cursor_key,
            "cursor_value": cursor_value,
            "cursor_row_deleted": false,
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
