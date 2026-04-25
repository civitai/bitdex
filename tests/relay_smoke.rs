//! Relay smoke tests — exercises the V1 surface end-to-end through axum's
//! in-process test client.
//!
//! Skipped from `cargo test --lib` rot because integration tests don't
//! compile lib-test code. Compiles standalone against the public crate
//! surface.

#![cfg(feature = "server")]

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::ConnectInfo;
use bitdex_v2::relay::auth::{check, AuthDecision};
use bitdex_v2::relay::channel::ChannelRegistry;
use bitdex_v2::relay::config::{
    AuthMode, CaptureConfig, ChannelConfig, EmitConfig, RelayConfig, ResponseConfig, RouteConfig,
};
use bitdex_v2::relay::metrics::RelayMetrics;
use bitdex_v2::relay::route::build_router;
use bitdex_v2::relay::RelayState;

/// Build a request with `ConnectInfo<SocketAddr>` injected as an extension —
/// axum's `ConnectInfo` extractor reads it from there. Without this,
/// `tower::ServiceExt::oneshot` requests fail the extractor and the
/// handler returns 500.
fn req_with_peer(peer: SocketAddr) -> axum::http::request::Builder {
    let mut req = axum::http::Request::builder();
    req.extensions_mut()
        .expect("extensions mut")
        .insert(ConnectInfo::<SocketAddr>(peer));
    req
}

fn fixture_config() -> RelayConfig {
    let mut channels = BTreeMap::new();
    channels.insert(
        "queries".into(),
        ChannelConfig {
            capacity: 64,
            keep_alive_seconds: 5,
        },
    );
    let routes = vec![
        RouteConfig {
            path: "/api/indexes/{index}/query".into(),
            methods: vec!["POST".into()],
            auth: AuthMode::None,
            max_body_bytes: None,
            emit: Some(EmitConfig {
                channel: "queries".into(),
                payload: r#"{"seq_id":{seq_id},"index":"{path.index}","body":{body|json}}"#.into(),
            }),
            response: Some(ResponseConfig {
                status: 200,
                headers: BTreeMap::new(),
                body: Some(r#"{"ids":[],"total_matched":0,"tee_mode":true}"#.into()),
            }),
        },
        RouteConfig {
            path: "/api/health".into(),
            methods: vec!["GET".into()],
            auth: AuthMode::None,
            max_body_bytes: None,
            emit: None,
            response: Some(ResponseConfig {
                status: 200,
                headers: BTreeMap::new(),
                body: Some(r#"{"status":"ok","mode":"relay"}"#.into()),
            }),
        },
    ];
    RelayConfig {
        listen: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        metrics_path: "/metrics".into(),
        admin_token_env: "TEST_TOKEN_ENV".into(),
        max_body_bytes: 1024 * 1024,
        channels,
        routes,
        capture: CaptureConfig::default(),
    }
}

fn fixture_state(token: Option<&str>) -> Arc<RelayState> {
    let cfg = fixture_config();
    let metrics = RelayMetrics::new();
    let channels = ChannelRegistry::from_config(&cfg);
    Arc::new(RelayState {
        config: cfg,
        channels,
        admin_token: token.map(|s| s.to_string()),
        metrics,
    })
}

#[tokio::test]
async fn router_constructs_without_panic() {
    // Smoke: build the router off a known-good config. Doesn't bind a port,
    // doesn't run any traffic. If this passes, the route → handler wiring
    // accepts the configured shapes.
    let state = fixture_state(None);
    let _router = build_router(state);
}

#[test]
fn auth_modes_behave_consistently() {
    use axum::http::HeaderMap;

    let h = HeaderMap::new();
    let peer_external: SocketAddr = "8.8.8.8:1".parse().unwrap();
    let peer_loopback: SocketAddr = "127.0.0.1:1".parse().unwrap();

    // None always allows
    assert_eq!(
        check(AuthMode::None, &h, peer_external, None),
        AuthDecision::Allow
    );

    // Bearer with no token configured denies
    assert!(matches!(
        check(AuthMode::Bearer, &h, peer_external, None),
        AuthDecision::Deny(_)
    ));

    // LoopbackOrBearer with loopback peer allows
    assert_eq!(
        check(AuthMode::LoopbackOrBearer, &h, peer_loopback, Some("t")),
        AuthDecision::Allow
    );

    // LoopbackOrBearer with external + missing header denies
    assert!(matches!(
        check(AuthMode::LoopbackOrBearer, &h, peer_external, Some("t")),
        AuthDecision::Deny(_)
    ));
}

#[tokio::test]
async fn channel_registry_seq_ids_monotonic() {
    let state = fixture_state(None);
    let queries = state.channels.get("queries").expect("channel exists");
    assert_eq!(queries.next_seq(), 1);
    assert_eq!(queries.next_seq(), 2);
    assert_eq!(queries.next_seq(), 3);
    assert_eq!(queries.receiver_count(), 0);
}

// ---- Caller-compat smoke ----
//
// Runs each known caller's wire shape against the router via
// `tower::ServiceExt::oneshot`. Verifies the relay returns the stub the
// caller expects and emits an event carrying the request body.

#[tokio::test]
async fn caller_compat_pg_sync_ops_batch_returns_2xx_and_emits_event() {
    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    let mut channels = BTreeMap::new();
    channels.insert(
        "ops".into(),
        ChannelConfig { capacity: 64, keep_alive_seconds: 5 },
    );
    let routes = vec![RouteConfig {
        path: "/api/indexes/{index}/ops".into(),
        methods: vec!["POST".into()],
        auth: AuthMode::None, // pg-sync sidecar in prod uses LoopbackOrBearer; smoke uses None
        max_body_bytes: Some(32 * 1024 * 1024),
        emit: Some(EmitConfig {
            channel: "ops".into(),
            payload: r#"{"seq_id":{seq_id},"index":"{path.index}","body":{body|json}}"#.into(),
        }),
        // No `response` block → empty 200 OK by default. Donovan verified
        // pg-sync's bitdex_client only checks status().is_success(); body
        // shape doesn't matter.
        response: None,
    }];
    let cfg = RelayConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        metrics_path: "/metrics".into(),
        admin_token_env: "TEST_TOKEN_ENV".into(),
        max_body_bytes: 4 * 1024 * 1024,
        channels,
        routes,
        capture: CaptureConfig::default(),
    };
    let metrics = RelayMetrics::new();
    let channels_reg = ChannelRegistry::from_config(&cfg);

    // Subscribe BEFORE building the router so the emit gate sees a receiver.
    let mut rx = channels_reg.get("ops").unwrap().sender.subscribe();

    let state = Arc::new(RelayState {
        config: cfg,
        channels: channels_reg,
        admin_token: None,
        metrics,
    });
    let router = build_router(state).into_service::<Body>();

    // Real-shaped OpsBatch JSON. Mirrors the wire format
    // `bitdex_v2::pg_sync::ops::OpsBatch` serializes to.
    let body = serde_json::json!({
        "ops": [
            {
                "entity_id": 12345,
                "ops": [
                    { "op": "set", "field": "nsfwLevel", "value": 16 },
                    { "op": "add", "field": "tagIds", "value": 42 }
                ],
                "creates_slot": true
            }
        ],
        "meta": {
            "source": "pg-sync-smoke",
            "cursor": 999_999,
            "max_id": 1_000_000,
            "lag_rows": 1
        }
    });
    let body_bytes = serde_json::to_vec(&body).unwrap();

    let req = req_with_peer("8.8.8.8:1234".parse().unwrap())
        .method(Method::POST)
        .uri("/api/indexes/civitai/ops")
        .header("content-type", "application/json")
        .body(Body::from(body_bytes.clone()))
        .unwrap();

    let resp = router.oneshot(req).await.unwrap();

    // (1) PG-sync only checks status().is_success(); 200 is required.
    assert!(
        resp.status().is_success(),
        "expected 2xx, got {}",
        resp.status()
    );
    assert_eq!(resp.status(), StatusCode::OK);

    // (2) Body is empty by default (no `response` block configured).
    let collected = resp.into_body().collect().await.unwrap().to_bytes();
    assert!(collected.is_empty(), "expected empty body, got {:?}", collected);

    // (3) Emit fired — broadcast subscriber receives the event with the
    // request body re-encoded as compact JSON.
    let event = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        rx.recv(),
    )
    .await
    .expect("timeout waiting for emitted event")
    .expect("broadcast recv error");

    assert_eq!(event.channel, "ops");
    assert_eq!(event.seq_id, 1);
    assert!(event.payload.contains(r#""index":"civitai""#));
    assert!(event.payload.contains(r#""entity_id":12345"#));
    assert!(event.payload.contains(r#""op":"set""#));

}

#[tokio::test]
async fn caller_compat_query_returns_tee_mode_stub() {
    use axum::body::{Body};
    use axum::http::{Method, Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    let state = fixture_state(None);
    let router = build_router(state).into_service::<Body>();

    // Simulate the model-share / shadow-mode comparator hitting /query.
    let body = serde_json::json!({"filters": [], "limit": 20, "include_docs": false});
    let req = req_with_peer("8.8.8.8:1234".parse().unwrap())
        .method(Method::POST)
        .uri("/api/indexes/civitai/query")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();

    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let collected = resp.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&collected).unwrap();
    assert_eq!(v["ids"].as_array().unwrap().len(), 0);
    assert_eq!(v["total_matched"].as_u64().unwrap(), 0);
    // tee_mode flag is the signal model-share's compare.ts must skip on
    // (per Donovan) — confirms the stub carries it.
    assert_eq!(v["tee_mode"].as_bool().unwrap(), true);
}

#[tokio::test]
async fn caller_compat_health_returns_status_ok() {
    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    let state = fixture_state(None);
    let router = build_router(state).into_service::<Body>();

    let req = req_with_peer("8.8.8.8:1234".parse().unwrap())
        .method(Method::GET)
        .uri("/api/health")
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let collected = resp.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&collected).unwrap();
    assert_eq!(v["status"].as_str().unwrap(), "ok");
    assert_eq!(v["mode"].as_str().unwrap(), "relay");
}

#[tokio::test]
async fn caller_compat_loopback_or_bearer_external_denied() {
    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    // Same fixture as the ops smoke, but with auth = LoopbackOrBearer
    // and an external peer + no Authorization header. Should 401.
    let mut channels = BTreeMap::new();
    channels.insert(
        "ops".into(),
        ChannelConfig { capacity: 8, keep_alive_seconds: 5 },
    );
    let routes = vec![RouteConfig {
        path: "/api/indexes/{index}/ops".into(),
        methods: vec!["POST".into()],
        auth: AuthMode::LoopbackOrBearer,
        max_body_bytes: None,
        emit: Some(EmitConfig {
            channel: "ops".into(),
            payload: r#"{}"#.into(),
        }),
        response: None,
    }];
    let cfg = RelayConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        metrics_path: "/metrics".into(),
        admin_token_env: "TEST".into(),
        max_body_bytes: 1024,
        channels,
        routes,
        capture: CaptureConfig::default(),
    };
    let metrics = RelayMetrics::new();
    let channels_reg = ChannelRegistry::from_config(&cfg);
    let state = Arc::new(RelayState {
        config: cfg,
        channels: channels_reg,
        admin_token: Some("the-token".into()),
        metrics,
    });
    let router = build_router(state).into_service::<Body>();

    let req = req_with_peer("8.8.8.8:1234".parse().unwrap())
        .method(Method::POST)
        .uri("/api/indexes/civitai/ops")
        .body(Body::from("{}"))
        .unwrap();
    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // Loopback peer bypasses; same router + state must allow.
    let req = req_with_peer("127.0.0.1:1234".parse().unwrap())
        .method(Method::POST)
        .uri("/api/indexes/civitai/ops")
        .body(Body::from("{}"))
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Drain any stray response body to avoid runtime warnings.
    let _ = resp.into_body().collect().await;
}
