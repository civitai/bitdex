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

use bitdex_v2::relay::auth::{check, AuthDecision};
use bitdex_v2::relay::channel::ChannelRegistry;
use bitdex_v2::relay::config::{
    AuthMode, CaptureConfig, ChannelConfig, EmitConfig, RelayConfig, ResponseConfig, RouteConfig,
};
use bitdex_v2::relay::metrics::RelayMetrics;
use bitdex_v2::relay::route::build_router;
use bitdex_v2::relay::RelayState;

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
