//! Integration smoke that drives the relay via `BitdexClient` — the actual
//! HTTP client `pg-sync` uses against bitdex-server.
//!
//! Why: PR #230 added stub routes for `/dumps`, `/stats`, `/cursors` after
//! prod pg-sync hit a CrashLoopBackOff with "register_dump response parse
//! failed: error decoding response body" — caused by the relay returning
//! an empty 200 to `PUT /dumps` while pg-sync's client called `resp.json()`.
//!
//! The original `tests/relay_smoke.rs` only checks status codes through
//! `tower::ServiceExt::oneshot`, which doesn't exercise `resp.json()`
//! brittleness. This file binds the relay router on a real port, points
//! `BitdexClient` at it, and asserts every pg-sync code path runs to
//! completion without parse errors.
//!
//! Two scenarios:
//!   1. With the production default config (loaded from
//!      `relay-config.default.yaml`) — `BitdexClient` calls succeed.
//!   2. With a "broken" config missing the dumps stub — the same call
//!      fails with the literal error pg-sync hit in prod. Acts as a
//!      regression guard: future config changes that drop the stubs
//!      will be flagged here, not in prod.

#![cfg(all(feature = "server", feature = "pg-sync"))]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use bitdex_v2::pg_sync::bitdex_client::BitdexClient;
use bitdex_v2::relay::channel::ChannelRegistry;
use bitdex_v2::relay::config::RelayConfig;
use bitdex_v2::relay::metrics::RelayMetrics;
use bitdex_v2::relay::route::build_router;
use bitdex_v2::relay::RelayState;

async fn bind_relay(config: RelayConfig) -> SocketAddr {
    let metrics = RelayMetrics::new();
    let channels = ChannelRegistry::from_config(&config);
    let state = Arc::new(RelayState {
        config,
        channels,
        admin_token: Some("test-token".to_string()),
        metrics,
        _capture: None,
    });
    let app = build_router(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .ok();
    });

    // Tiny wait so the listener is accept()ing before the client probes.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    addr
}

fn load_default_config() -> RelayConfig {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("."));
    let path = manifest_dir.join("relay-config.default.yaml");
    RelayConfig::load_and_validate(&path).expect("default config must validate")
}

#[tokio::test]
async fn pg_sync_dump_pipeline_against_default_config() {
    // Drive the live relay binary's HTTP surface using the same client
    // pg-sync uses in prod. This test would have caught the empty-200
    // CrashLoopBackOff that hit v1.0.172 in prod.

    // Token check is required-on-startup; set the env var the way prod does
    // before constructing the relay config.
    std::env::set_var("BITDEX_ADMIN_TOKEN", "test-token");

    let config = load_default_config();
    let addr = bind_relay(config).await;
    let client = BitdexClient::with_index(&format!("http://{addr}"), Some("civitai"));

    // 1. /api/health → pg-sync's health gate
    assert!(client.is_healthy().await, "/api/health should return 200");

    // 2. PUT /dumps with a representative request body. The relay returns a
    //    JSON stub ({"name":"relay-stub","status":"complete"}). The
    //    success path here is the prod CrashLoopBackOff symptom going
    //    away — register_dump's `resp.json::<Value>()` no longer fails
    //    on an empty body.
    let dump_request = serde_json::json!({
        "name": "images-test",
        "csv_path": "/tmp/images.csv",
        "row_count": 0,
    });
    let resp = client
        .register_dump(&dump_request)
        .await
        .expect("register_dump must succeed against the stubbed relay route");
    // Don't assert the exact echoed body — the stub just needs to be
    // valid JSON and 2xx, which the Ok arm proves.
    let _ = resp;

    // 3. POST /dumps/{name}/loaded
    client
        .signal_dump_loaded("images-test", 0)
        .await
        .expect("signal_dump_loaded must succeed");

    // 4. GET /dumps — pg-sync looks at .all_complete
    let dumps = client
        .get_dumps()
        .await
        .expect("get_dumps must succeed");
    assert_eq!(
        dumps.get("all_complete").and_then(|v| v.as_bool()),
        Some(true),
        "stub must report all_complete=true so pg-sync skips dump phase"
    );
}

#[tokio::test]
async fn pg_sync_register_dump_fails_loudly_when_stub_missing() {
    // Regression guard: if a future config change drops the dump stub
    // routes, this test reproduces the exact prod failure mode (empty
    // 200 → "response parse failed") so a developer sees the brittleness
    // before it ships.

    use bitdex_v2::relay::config::{
        AuthMode, CaptureConfig, ChannelConfig, EmitConfig, RelayConfig, ResponseConfig,
        RouteConfig,
    };
    use std::collections::BTreeMap;

    std::env::set_var("BITDEX_ADMIN_TOKEN", "test-token");

    let mut channels = BTreeMap::new();
    channels.insert(
        "ops".into(),
        ChannelConfig {
            capacity: 8,
            keep_alive_seconds: 5,
        },
    );
    // Only configure /api/health + /api/indexes/{index}/ops — no /dumps stub.
    let routes = vec![
        RouteConfig {
            path: "/api/health".into(),
            methods: vec!["GET".into()],
            auth: AuthMode::None,
            max_body_bytes: None,
            emit: None,
            response: Some(ResponseConfig {
                status: 200,
                headers: BTreeMap::new(),
                body: Some(r#"{"status":"ok"}"#.into()),
            }),
        },
        RouteConfig {
            path: "/api/indexes/{index}/ops".into(),
            methods: vec!["POST".into()],
            auth: AuthMode::None,
            max_body_bytes: None,
            emit: Some(EmitConfig {
                channel: "ops".into(),
                payload: r#"{"seq_id":{seq_id}}"#.into(),
            }),
            response: None,
        },
    ];
    let config = RelayConfig {
        listen: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        metrics_path: "/metrics".into(),
        admin_token_env: "BITDEX_ADMIN_TOKEN".into(),
        max_body_bytes: 1024 * 1024,
        channels,
        routes,
        capture: CaptureConfig::default(),
    };

    let addr = bind_relay(config).await;
    let client = BitdexClient::with_index(&format!("http://{addr}"), Some("civitai"));

    let dump_request = serde_json::json!({"name": "x", "csv_path": "/tmp/x.csv"});
    let result = client.register_dump(&dump_request).await;

    // Without the stub route, axum responds with 404 and an empty body.
    // BitdexClient calls `resp.json::<Value>()` first, which fails before
    // status is checked → bubble out as "response parse failed".
    let err = result.expect_err("register_dump must fail without the dumps stub");
    assert!(
        err.contains("register_dump")
            && (err.contains("parse failed") || err.contains("decoding")),
        "expected pg-sync's parse-failure shape, got: {err}"
    );
}
