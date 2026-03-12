use std::path::{Path, PathBuf};

use serde::Deserialize;

/// Top-level pg-sync configuration loaded from sync.toml.
#[derive(Debug, Deserialize)]
pub struct PgSyncConfig {
    /// Postgres connection string for the read-write logical replica.
    pub postgres_url: String,
    /// ClickHouse HTTP URL for metrics queries.
    pub clickhouse_url: Option<String>,
    /// Bitdex HTTP server URL (for sync mode, e.g. "http://localhost:3000").
    pub bitdex_url: Option<String>,
    /// Path to the index definition directory (contains config.json).
    /// e.g. "data/indexes/civitai"
    pub index_dir: PathBuf,
    /// Data directory for engine storage (bitmaps + docstore).
    pub data_dir: PathBuf,
    /// Number of PG connections in the pool.
    #[serde(default = "default_pg_pool_size")]
    pub pg_pool_size: u32,
    /// Batch size for bulk loading (number of image IDs per batch).
    #[serde(default = "default_batch_size")]
    pub batch_size: i64,
    /// Outbox poll interval in seconds.
    #[serde(default = "default_poll_interval_secs")]
    pub poll_interval_secs: u64,
    /// Outbox batch limit per poll.
    #[serde(default = "default_outbox_batch_limit")]
    pub outbox_batch_limit: i64,
    /// ClickHouse metrics poll interval in seconds.
    #[serde(default = "default_metrics_poll_interval_secs")]
    pub metrics_poll_interval_secs: u64,
}

fn default_pg_pool_size() -> u32 {
    10
}
fn default_batch_size() -> i64 {
    100_000
}
fn default_poll_interval_secs() -> u64 {
    2
}
fn default_outbox_batch_limit() -> i64 {
    5000
}
fn default_metrics_poll_interval_secs() -> u64 {
    60
}

impl PgSyncConfig {
    /// Load a `PgSyncConfig` from a TOML file on disk.
    pub fn from_file(path: &Path) -> Result<Self, String> {
        let contents = std::fs::read_to_string(path)
            .map_err(|e| format!("failed to read {}: {}", path.display(), e))?;
        toml::from_str(&contents)
            .map_err(|e| format!("failed to parse {}: {}", path.display(), e))
    }
}

/// An index definition loaded from `config.json` inside an index directory.
/// Matches the shape of `data/indexes/civitai/config.json`.
#[derive(Debug, Deserialize)]
pub struct IndexDefinition {
    /// Human-readable index name (e.g. "civitai").
    pub name: String,
    /// Engine configuration (filter fields, sort fields, etc.).
    pub config: crate::config::Config,
    /// Data schema describing source columns and their mapping.
    pub data_schema: crate::config::DataSchema,
}

impl IndexDefinition {
    /// Load an `IndexDefinition` from the `config.json` file inside `dir`.
    pub fn from_dir(dir: &Path) -> Result<Self, String> {
        let config_path = dir.join("config.json");
        let contents = std::fs::read_to_string(&config_path)
            .map_err(|e| format!("failed to read {}: {}", config_path.display(), e))?;
        serde_json::from_str(&contents)
            .map_err(|e| format!("failed to parse {}: {}", config_path.display(), e))
    }
}
