//! Relay mode — pure-dummy HTTP→SSE relay.
//!
//! Activated by `BITDEX_MODE=relay` at process startup. Replaces the
//! bitmap-engine bootstrap with a tiny axum service that:
//!
//!   - accepts HTTP on configured routes
//!   - emits an SSE event of the request body onto a configured channel
//!   - returns a configured stub response (default empty 200)
//!
//! Subscribers connect to `GET /events/{channel}` and consume events.
//!
//! Design: see `docs/_in/relay-system-design.md` (V3).

pub mod auth;
pub mod capture;
pub mod channel;
pub mod config;
pub mod metrics;
pub mod route;
pub mod sse;
pub mod template;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use crate::relay::channel::ChannelRegistry;
use crate::relay::config::RelayConfig;

/// Shared application state passed to axum handlers.
pub struct RelayState {
    pub config: RelayConfig,
    pub channels: ChannelRegistry,
    pub admin_token: Option<String>,
    pub metrics: metrics::RelayMetrics,
}

pub type SharedRelayState = Arc<RelayState>;

/// Run the relay. Loads config, builds the axum router, binds the listener.
///
/// Errors propagate up so the entrypoint can log + exit with a clear failure.
pub async fn run(config_path: PathBuf, listen_override: Option<SocketAddr>) -> Result<(), RelayError> {
    let config = RelayConfig::load_and_validate(&config_path)
        .map_err(RelayError::Config)?;

    let admin_token = std::env::var(&config.admin_token_env).ok();
    if admin_token.is_none() && config.requires_bearer() {
        return Err(RelayError::TokenMissing(config.admin_token_env.clone()));
    }

    let metrics = metrics::RelayMetrics::new();
    let channels = ChannelRegistry::from_config(&config);

    let state = Arc::new(RelayState {
        config: config.clone(),
        channels,
        admin_token,
        metrics,
    });

    let app = route::build_router(state.clone());
    let addr = listen_override.unwrap_or(config.listen);

    eprintln!("BitDex relay listening on {addr} (mode=relay)");
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(RelayError::Bind)?;

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .map_err(RelayError::Serve)?;

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum RelayError {
    #[error("config error: {0}")]
    Config(config::ConfigError),
    #[error("admin bearer required but env var '{0}' is unset")]
    TokenMissing(String),
    #[error("bind error: {0}")]
    Bind(std::io::Error),
    #[error("serve error: {0}")]
    Serve(std::io::Error),
}
