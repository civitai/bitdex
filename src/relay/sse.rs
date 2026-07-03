//! SSE egress handler — `GET /events/{channel}`.
//!
//! Lifts the pattern from `bitdex-server`'s `handle_query_stream`:
//!   - subscribe a fresh receiver
//!   - stream events as `id: <seq_id>\ndata: <payload>\n\n`
//!   - on `RecvError::Lagged(n)`, emit a `: lagged N` SSE comment, keep going
//!   - explicit `X-Accel-Buffering: no`, `Cache-Control: no-cache`,
//!     `Content-Type: text/event-stream` headers (reverse proxy may override
//!     axum defaults; set explicit)
//!   - keep-alive default

use std::convert::Infallible;
use std::net::SocketAddr;
use std::time::Duration;

use axum::extract::{ConnectInfo, Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::Json;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

use crate::relay::auth::{check, AuthDecision};
use crate::relay::config::AuthMode;
use crate::relay::SharedRelayState;

pub async fn handle_events(
    State(state): State<SharedRelayState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(channel): Path<String>,
    headers: HeaderMap,
) -> Response {
    // Egress is always bearer-gated. No XFF bypass, no loopback bypass for
    // /events/* — internal-pod observers must still present the token.
    let decision = check(
        AuthMode::Bearer,
        &headers,
        peer,
        state.admin_token.as_deref(),
    );
    if let AuthDecision::Deny(reason) = decision {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": reason})),
        )
            .into_response();
    }

    let Some(handle) = state.channels.get(&channel) else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("unknown channel '{}'", channel)})),
        )
            .into_response();
    };

    let rx = handle.sender.subscribe();
    let _keep_alive = Duration::from_secs(handle.keep_alive_seconds);
    let metrics = state.metrics.clone();
    let channel_name = channel.clone();

    metrics
        .sse_subscribers
        .with_label_values(&[&channel_name])
        .inc();

    // RAII gauge guard. Captured into the stream's filter_map closure via
    // `move`; lives as long as the stream itself. When the client
    // disconnects (or the response is dropped server-side), axum drops
    // the response → the Sse body drops → BroadcastStream drops → the
    // closure drops → SubscriberGuard::drop fires → gauge decrements.
    let gauge_guard = SubscriberGuard {
        metrics: metrics.clone(),
        channel: channel_name.clone(),
    };
    let metrics_for_lag = metrics.clone();
    let chan_for_lag = channel_name.clone();

    let stream = BroadcastStream::new(rx).filter_map(move |msg| {
        // Force-capture the guard into the closure environment without
        // dropping it on each call. The `&` borrows for the duration of
        // this match arm; the owning binding lives in the closure state.
        let _ = &gauge_guard;
        match msg {
            Ok(event) => {
                let sse = Event::default()
                    .id(event.seq_id.to_string())
                    .data(event.payload);
                Some(Ok::<Event, Infallible>(sse))
            }
            Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(n)) => {
                metrics_for_lag
                    .sse_lagged_events
                    .with_label_values(&[&chan_for_lag])
                    .inc_by(n as f64);
                metrics_for_lag
                    .drops_total
                    .with_label_values(&[&chan_for_lag, "lagged"])
                    .inc_by(n as f64);
                Some(Ok::<Event, Infallible>(
                    Event::default().comment(format!("lagged {n}")),
                ))
            }
        }
    });

    let mut response = Sse::new(stream).keep_alive(KeepAlive::default()).into_response();

    // Headers — explicit, do not rely on axum/reverse-proxy defaults.
    let h = response.headers_mut();
    h.insert("X-Accel-Buffering", HeaderValue::from_static("no"));
    h.insert("Cache-Control", HeaderValue::from_static("no-cache"));
    h.insert("Content-Type", HeaderValue::from_static("text/event-stream"));

    response
}

struct SubscriberGuard {
    metrics: crate::relay::metrics::RelayMetrics,
    channel: String,
}

impl Drop for SubscriberGuard {
    fn drop(&mut self) {
        self.metrics
            .sse_subscribers
            .with_label_values(&[&self.channel])
            .dec();
    }
}

#[cfg(test)]
mod tests {
    // SSE end-to-end is exercised via the integration test in
    // `tests/relay_integration.rs` (added in a follow-up commit).
    // Unit-testing the BroadcastStream wiring is covered by tokio's own
    // tests; we don't redo it here.
}
