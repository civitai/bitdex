use std::path::{Path, PathBuf};

use serde::Deserialize;

/// Top-level pg-sync configuration loaded from sync.toml.
#[derive(Debug, Deserialize)]
pub struct PgSyncConfig {
    /// Postgres connection string for the read-write logical replica.
    pub postgres_url: String,
    /// ClickHouse HTTP URL for metrics queries.
    pub clickhouse_url: Option<String>,
    /// ClickHouse username (default: "default").
    pub clickhouse_username: Option<String>,
    /// ClickHouse password.
    pub clickhouse_password: Option<String>,
    /// Bitdex HTTP server URL (for sync mode, e.g. "http://localhost:3000").
    pub bitdex_url: Option<String>,
    /// Path to the index definition directory (contains config.json).
    /// e.g. "data/indexes/civitai"
    pub index_dir: PathBuf,
    /// Data directory for engine storage (bitmaps + docstore).
    pub data_dir: PathBuf,
    /// Subdirectory under data_dir for indexes (default: "indexes").
    /// Must match the server's layout so it can find persisted data.
    #[serde(default = "default_index_subdir")]
    pub index_subdir: String,
    /// Subdirectory within each index dir for bitmap storage (default: "bitmaps").
    #[serde(default = "default_bitmap_subdir")]
    pub bitmap_subdir: String,
    /// Subdirectory within each index dir for document storage (default: "docs").
    #[serde(default = "default_docs_subdir")]
    pub docs_subdir: String,
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

fn default_index_subdir() -> String {
    "indexes".to_string()
}
fn default_bitmap_subdir() -> String {
    "bitmaps".to_string()
}
fn default_docs_subdir() -> String {
    "docs".to_string()
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
    ///
    /// After loading, environment variables override config values:
    /// - `DATABASE_URL` overrides `postgres_url`
    /// - `CLICKHOUSE_URL` overrides `clickhouse_url`
    /// - `BITDEX_URL` overrides `bitdex_url`
    pub fn from_file(path: &Path) -> Result<Self, String> {
        let contents = std::fs::read_to_string(path)
            .map_err(|e| format!("failed to read {}: {}", path.display(), e))?;
        let mut config: Self = toml::from_str(&contents)
            .map_err(|e| format!("failed to parse {}: {}", path.display(), e))?;

        // Environment variable overrides
        if let Ok(url) = std::env::var("DATABASE_URL") {
            config.postgres_url = url;
        }
        if let Ok(url) = std::env::var("CLICKHOUSE_URL") {
            config.clickhouse_url = Some(url);
        }
        if let Ok(url) = std::env::var("BITDEX_URL") {
            config.bitdex_url = Some(url);
        }
        if let Ok(user) = std::env::var("CLICKHOUSE_USERNAME") {
            config.clickhouse_username = Some(user);
        }
        if let Ok(pass) = std::env::var("CLICKHOUSE_PASSWORD") {
            config.clickhouse_password = Some(pass);
        }

        Ok(config)
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
