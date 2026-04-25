//! Route registry — turns the configured routes + channels into an axum
//! `Router`.

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::{ConnectInfo, MatchedPath, Path, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, MethodFilter, MethodRouter};
use axum::Router;

use crate::relay::auth::{check, AuthDecision};
use crate::relay::channel::RelayEvent;
use crate::relay::config::RouteConfig;
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

    // Group configured routes by path so multiple (method, handler) pairs
    // at the same path merge into a single MethodRouter. Without this,
    // `Router::route(path, MethodRouter)` panics with "Cannot merge two
    // MethodRouters that both have a fallback" when two configs share a
    // path (e.g. `GET /dumps` + `PUT /dumps`). axum 0.8 disallows two
    // wildcard MethodRouters at one path, so we register specific method
    // filters instead and let axum dispatch.
    let mut by_path: BTreeMap<String, Vec<RouteConfig>> = BTreeMap::new();
    for route in state.config.routes.clone() {
        by_path.entry(route.path.clone()).or_default().push(route);
    }
    for (path, configs) in by_path {
        let mr = build_method_router(configs);
        router = router.route(&path, mr);
    }

    router.with_state(state)
}

fn build_method_router(configs: Vec<RouteConfig>) -> MethodRouter<SharedRelayState> {
    let mut mr: MethodRouter<SharedRelayState> = MethodRouter::new();

    for config in configs {
        let route_arc = Arc::new(config);
        let method_strings: Vec<String> = route_arc.methods.clone();
        for method_str in method_strings {
            let Some(filter) = parse_method_filter(&method_str) else { continue };
            let route_for_handler = Arc::clone(&route_arc);
            mr = mr.on(
                filter,
                move |
                    State(state): State<SharedRelayState>,
                    ConnectInfo(peer): ConnectInfo<SocketAddr>,
                    method: Method,
                    Path(path_params): Path<HashMap<String, String>>,
                    matched: MatchedPath,
                    headers: HeaderMap,
                    body: Bytes,
                | {
                    let route = Arc::clone(&route_for_handler);
                    async move {
                        dispatch(route, state, peer, method, path_params, matched, headers, body)
                            .await
                    }
                },
            );
        }
    }

    mr
}

fn parse_method_filter(s: &str) -> Option<MethodFilter> {
    match s.to_uppercase().as_str() {
        "GET" => Some(MethodFilter::GET),
        "POST" => Some(MethodFilter::POST),
        "PUT" => Some(MethodFilter::PUT),
        "DELETE" => Some(MethodFilter::DELETE),
        "PATCH" => Some(MethodFilter::PATCH),
        "HEAD" => Some(MethodFilter::HEAD),
        "OPTIONS" => Some(MethodFilter::OPTIONS),
        "TRACE" => Some(MethodFilter::TRACE),
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
async fn dispatch(
    route: Arc<RouteConfig>,
    state: SharedRelayState,
    peer: SocketAddr,
    _method: Method,
    path_params: HashMap<String, String>,
    matched: MatchedPath,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let m = state.metrics.clone();
    let route_label = matched.as_str().to_string();
    let timer = m.request_duration.with_label_values(&[&route_label]).start_timer();

    // Auth (method check is now in axum's MethodRouter — only matching
    // methods reach this handler, so no inner method gate is needed).
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
