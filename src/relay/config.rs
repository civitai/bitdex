//! Relay YAML config.
//!
//! Schema mirrors `docs/_in/relay-system-design.md` §5.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::Path;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct RelayConfig {
    pub listen: SocketAddr,
    #[serde(default = "default_metrics_path")]
    pub metrics_path: String,
    #[serde(default = "default_admin_token_env")]
    pub admin_token_env: String,
    #[serde(default = "default_max_body_bytes")]
    pub max_body_bytes: usize,
    pub channels: std::collections::BTreeMap<String, ChannelConfig>,
    pub routes: Vec<RouteConfig>,
    #[serde(default)]
    pub capture: CaptureConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChannelConfig {
    pub capacity: usize,
    #[serde(default = "default_keep_alive")]
    pub keep_alive_seconds: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RouteConfig {
    pub path: String,
    #[serde(default = "default_methods")]
    pub methods: Vec<String>,
    #[serde(default)]
    pub auth: AuthMode,
    #[serde(default)]
    pub max_body_bytes: Option<usize>,
    #[serde(default)]
    pub emit: Option<EmitConfig>,
    #[serde(default)]
    pub response: Option<ResponseConfig>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuthMode {
    #[default]
    None,
    Bearer,
    LoopbackOrBearer,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EmitConfig {
    pub channel: String,
    pub payload: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ResponseConfig {
    #[serde(default = "default_status")]
    pub status: u16,
    #[serde(default)]
    pub headers: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    pub body: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct CaptureConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_capture_dir")]
    pub dir: String,
    #[serde(default = "default_rotate_bytes")]
    pub rotate_bytes: u64,
    #[serde(default = "default_gzip_after_rotate")]
    pub gzip_after_rotate: bool,
    #[serde(default = "default_max_total_bytes")]
    pub max_total_bytes: u64,
    #[serde(default)]
    pub fsync: FsyncPolicy,
    #[serde(default = "default_file_mode")]
    pub file_mode: u32,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FsyncPolicy {
    #[default]
    PerRotation,
    PerEvent,
    Never,
}

fn default_metrics_path() -> String { "/metrics".into() }
fn default_admin_token_env() -> String { "BITDEX_ADMIN_TOKEN".into() }
fn default_max_body_bytes() -> usize { 4 * 1024 * 1024 }
fn default_keep_alive() -> u64 { 15 }
fn default_methods() -> Vec<String> { vec!["POST".into()] }
fn default_status() -> u16 { 200 }
fn default_capture_dir() -> String { "/var/lib/bitdex-relay".into() }
fn default_rotate_bytes() -> u64 { 256 * 1024 * 1024 }
fn default_gzip_after_rotate() -> bool { true }
fn default_max_total_bytes() -> u64 { 20 * 1024 * 1024 * 1024 }
fn default_file_mode() -> u32 { 0o640 }

impl RelayConfig {
    /// Parse YAML from path and run startup validation. Fails fast on any
    /// schema or referential issue (unknown channel in route, duplicate
    /// route key, invalid status, etc.).
    pub fn load_and_validate(path: &Path) -> Result<Self, ConfigError> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| ConfigError::Read(path.display().to_string(), e))?;
        let cfg: RelayConfig = serde_yaml::from_str(&raw)
            .map_err(|e| ConfigError::Parse(path.display().to_string(), e))?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.channels.is_empty() {
            return Err(ConfigError::Validation("at least one channel must be defined".into()));
        }
        for (name, ch) in &self.channels {
            if ch.capacity == 0 {
                return Err(ConfigError::Validation(format!(
                    "channel '{name}' has capacity = 0; must be > 0"
                )));
            }
        }

        let mut seen_keys: HashSet<(String, String)> = HashSet::new();
        for route in &self.routes {
            for method in &route.methods {
                let key = (method.to_uppercase(), route.path.clone());
                if !seen_keys.insert(key.clone()) {
                    return Err(ConfigError::Validation(format!(
                        "duplicate route: {} {}",
                        key.0, key.1
                    )));
                }
            }
            if let Some(emit) = &route.emit {
                if !self.channels.contains_key(&emit.channel) {
                    return Err(ConfigError::Validation(format!(
                        "route {} emits to unknown channel '{}'",
                        route.path, emit.channel
                    )));
                }
            }
            if let Some(resp) = &route.response {
                if !(100..=599).contains(&resp.status) {
                    return Err(ConfigError::Validation(format!(
                        "route {} response.status = {} out of range",
                        route.path, resp.status
                    )));
                }
            }
        }
        Ok(())
    }

    /// True if any configured route requires bearer auth — used to fail
    /// startup early if the admin token env var is unset.
    pub fn requires_bearer(&self) -> bool {
        self.routes.iter().any(|r| {
            matches!(r.auth, AuthMode::Bearer | AuthMode::LoopbackOrBearer)
        })
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ConfigError {
    #[error("read {0}: {1}")]
    Read(String, std::io::Error),
    #[error("parse {0}: {1}")]
    Parse(String, serde_yaml::Error),
    #[error("validation: {0}")]
    Validation(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(yaml: &str) -> Result<RelayConfig, ConfigError> {
        let cfg: RelayConfig = serde_yaml::from_str(yaml)
            .map_err(|e| ConfigError::Parse("inline".into(), e))?;
        cfg.validate()?;
        Ok(cfg)
    }

    #[test]
    fn parses_minimal_config() {
        let yaml = r#"
listen: 127.0.0.1:3000
channels:
  queries:
    capacity: 100
routes:
  - path: /q
    methods: [POST]
    emit:
      channel: queries
      payload: '{}'
"#;
        let cfg = parse(yaml).unwrap();
        assert_eq!(cfg.routes.len(), 1);
        assert_eq!(cfg.channels.len(), 1);
    }

    #[test]
    fn rejects_unknown_channel() {
        let yaml = r#"
listen: 127.0.0.1:3000
channels:
  a:
    capacity: 1
routes:
  - path: /q
    methods: [POST]
    emit:
      channel: missing
      payload: '{}'
"#;
        assert!(parse(yaml).is_err());
    }

    #[test]
    fn rejects_duplicate_route() {
        let yaml = r#"
listen: 127.0.0.1:3000
channels:
  a:
    capacity: 1
routes:
  - path: /q
    methods: [POST]
  - path: /q
    methods: [POST]
"#;
        assert!(parse(yaml).is_err());
    }

    #[test]
    fn rejects_zero_capacity() {
        let yaml = r#"
listen: 127.0.0.1:3000
channels:
  a:
    capacity: 0
routes: []
"#;
        assert!(parse(yaml).is_err());
    }

    #[test]
    fn requires_bearer_detects() {
        let yaml = r#"
listen: 127.0.0.1:3000
channels:
  a: { capacity: 1 }
routes:
  - path: /x
    methods: [GET]
    auth: bearer
"#;
        let cfg = parse(yaml).unwrap();
        assert!(cfg.requires_bearer());
    }
}
