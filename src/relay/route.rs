//! Route registry — turns the configured routes + channels into an axum
//! `Router`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::{ConnectInfo, MatchedPath, Path, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get, MethodRouter};
use axum::Router;

use crate::relay::auth::{check, AuthDecision};
use crate::relay::channel::RelayEvent;
use crate::relay::config::{RouteConfig, AuthMode};
use crate::relay::sse;
use crate::relay::template::{render, RenderOutcome, TemplateContext};
use crate::relay::SharedRelayState;

pub fn build_router(state: SharedRelayState) -> Router {
    let mut router = Router::new();

    // SSE egress is always available — admin bearer required at handler.
    router = router.route("/events/{channel}", get(sse::handle_events));

    // /metrics on the same listener; bare prometheus encoding.
    let metrics_path = state.config.metrics_path.clone();
    router = router.route(&metrics_path, get(handle_metrics));

    // Configured ingress routes.
    for route in state.config.routes.clone() {
        let path = route.path.clone();
        let method_router = build_method_router(route);
        router = router.route(&path, method_router);
    }

    router.with_state(state)
}

fn build_method_router(route: RouteConfig) -> MethodRouter<SharedRelayState> {
    // axum's MethodRouter dispatches one closure per (method, path);
    // use `any` and filter by configured methods inside the handler.
    let methods: Vec<Method> = route
        .methods
        .iter()
        .filter_map(|m| Method::from_bytes(m.to_uppercase().as_bytes()).ok())
        .collect();

    let route_arc = Arc::new(route);

    any(move |
        State(state): State<SharedRelayState>,
        ConnectInfo(peer): ConnectInfo<SocketAddr>,
        method: Method,
        Path(path_params): Path<HashMap<String, String>>,
        matched: MatchedPath,
        headers: HeaderMap,
        body: Bytes,
    | {
        let route = route_arc.clone();
        let methods = methods.clone();
        async move { dispatch(route, methods, state, peer, method, path_params, matched, headers, body).await }
    })
}

#[allow(clippy::too_many_arguments)]
async fn dispatch(
    route: Arc<RouteConfig>,
    methods: Vec<Method>,
    state: SharedRelayState,
    peer: SocketAddr,
    method: Method,
    path_params: HashMap<String, String>,
    matched: MatchedPath,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let m = state.metrics.clone();
    let route_label = matched.as_str().to_string();
    let timer = m.request_duration.with_label_values(&[&route_label]).start_timer();

    // Method check
    if !methods.is_empty() && !methods.contains(&method) {
        timer.observe_duration();
        return (StatusCode::METHOD_NOT_ALLOWED, "method not allowed").into_response();
    }

    // Auth
    let decision = check(route.auth, &headers, peer, state.admin_token.as_deref());
    if let AuthDecision::Deny(reason) = decision {
        timer.observe_duration();
        return (
            StatusCode::UNAUTHORIZED,
            axum::Json(serde_json::json!({"error": reason})),
        )
            .into_response();
    }

    // Per-route body limit (already partially enforced by axum middleware in
    // production; double-check here).
    let limit = route.max_body_bytes.unwrap_or(state.config.max_body_bytes);
    if body.len() > limit {
        timer.observe_duration();
        return (StatusCode::PAYLOAD_TOO_LARGE, "body too large").into_response();
    }

    // Emit (gated on subscribers OR capture)
    if let Some(emit) = &route.emit {
        if let Some(handle) = state.channels.get(&emit.channel) {
            let capture_enabled = state.config.capture.enabled;
            let has_subscriber = handle.receiver_count() > 0;
            if has_subscriber || capture_enabled {
                let seq_id = handle.next_seq();
                let ts_ms = unix_ms();
                let client_ip = client_ip(&headers, peer);
                let header_map = lowered_headers(&headers);
                let ctx = TemplateContext {
                    seq_id,
                    ts_ms,
                    body: body.as_ref(),
                    path_params: &path_params,
                    headers: &header_map,
                    client_ip: &client_ip,
                };
                let mut payload = String::with_capacity(emit.payload.len() + body.len());
                let outcome = render(&emit.payload, &ctx, &mut payload);
                if outcome == RenderOutcome::ParseErrorEmittedNull {
                    m.emit_parse_error.with_label_values(&[&route_label]).inc();
                }
                let event = RelayEvent {
                    seq_id,
                    ts_ms,
                    channel: emit.channel.clone(),
                    payload,
                };
                // Ignore SendError — receiver_count() may have raced to 0
                // between the gate check and now (TOCTOU). Benign.
                let _ = handle.sender.send(event);
                m.emit_total
                    .with_label_values(&[&emit.channel, &route_label])
                    .inc();
                // Capture write — V1 no-op while NullSink is in place.
            } else {
                m.emit_skipped_no_subscriber
                    .with_label_values(&[&emit.channel])
                    .inc();
            }
        }
    }

    // Stub response
    let response = build_response(route.response.as_ref());
    timer.observe_duration();
    response
}

fn build_response(resp: Option<&crate::relay::config::ResponseConfig>) -> Response {
    match resp {
        None => StatusCode::OK.into_response(),
        Some(r) => {
            let status =
                StatusCode::from_u16(r.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            let body = r.body.clone().unwrap_or_default();
            let mut response = (status, body).into_response();
            for (k, v) in &r.headers {
                if let (Ok(name), Ok(value)) =
                    (HeaderName::from_bytes(k.as_bytes()), HeaderValue::from_str(v))
                {
                    response.headers_mut().insert(name, value);
                }
            }
            response
        }
    }
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn client_ip(headers: &HeaderMap, peer: SocketAddr) -> String {
    if let Some(value) = headers.get("x-forwarded-for") {
        if let Ok(s) = value.to_str() {
            if let Some(first) = s.split(',').next() {
                let trimmed = first.trim();
                if !trimmed.is_empty() {
                    return trimmed.to_string();
                }
            }
        }
    }
    peer.ip().to_string()
}

fn lowered_headers(headers: &HeaderMap) -> HashMap<String, String> {
    headers
        .iter()
        .filter_map(|(k, v)| v.to_str().ok().map(|vs| (k.as_str().to_lowercase(), vs.to_string())))
        .collect()
}

async fn handle_metrics(State(state): State<SharedRelayState>) -> Response {
    use prometheus::Encoder;
    let encoder = prometheus::TextEncoder::new();
    let mut buf = Vec::new();
    let metric_families = state.metrics.registry.gather();
    if encoder.encode(&metric_families, &mut buf).is_err() {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "metrics encode failed",
        )
            .into_response();
    }
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, encoder.format_type())],
        buf,
    )
        .into_response()
}
