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
    /// Path to the index definition directory (contains config.yaml).
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
    /// Outbox poll interval in milliseconds. Preferred over `poll_interval_secs`.
    pub poll_interval_ms: Option<u64>,
    /// Legacy: outbox poll interval in seconds. Use `poll_interval_ms` instead.
    #[serde(default = "default_poll_interval_secs")]
    pub poll_interval_secs: u64,
    /// Outbox batch limit per poll.
    #[serde(default = "default_outbox_batch_limit")]
    pub outbox_batch_limit: i64,
    /// ClickHouse metrics poll (reconcile) interval in seconds.
    #[serde(default = "default_metrics_poll_interval_secs")]
    pub metrics_poll_interval_secs: u64,
    /// ClickHouse table the metrics poller scans to discover images with recent
    /// metric activity. MUST be the complete real-time CDC stream, not a partial
    /// legacy feed. Repoint here (no rebuild) when the upstream table moves.
    #[serde(default = "default_metrics_table")]
    pub metrics_table: String,
    /// ClickHouse table/view the poller reads all-time cumulative totals from
    /// (argMax daily-agg). Repoint here (no rebuild) if the totals source moves.
    #[serde(default = "default_metrics_totals_table")]
    pub metrics_totals_table: String,
    /// `entityType` value filtered in both discovery and totals queries.
    #[serde(default = "default_metrics_entity_type")]
    pub metrics_entity_type: String,
    /// Steady-state trailing discovery window width, in seconds. Each reconcile
    /// scans `createdAt >= now - this`. Must be comfortably larger than both the
    /// worst-case ClickHouse batch-ingestion lag (spikes to minutes on watcher
    /// rebalances) AND the totals settle lag (~2.5min), so a batch-flushed or
    /// not-yet-settled event is still re-read on a later overlapping cycle.
    #[serde(default = "default_metrics_reconcile_window_secs")]
    pub metrics_reconcile_window_secs: u64,
    /// Forward chunk width, in seconds, used only while catching up (cursor far
    /// behind — cold start with a backfill anchor, or long downtime). Historical
    /// event partitions are immutable, so backfill walks forward in bounded
    /// non-overlapping chunks; this caps a single query's window width.
    #[serde(default = "default_metrics_backfill_chunk_secs")]
    pub metrics_backfill_chunk_secs: u64,
    /// Replica identifier for cursor tracking.
    /// Defaults to "default". Override via BITDEX_REPLICA_ID env var (e.g. from StatefulSet pod name).
    #[serde(default = "default_replica_id")]
    pub replica_id: String,
    /// Port for the progress HTTP endpoint during bulk load.
    /// Set to None to disable. Default: 9091.
    #[serde(default = "default_progress_port")]
    pub progress_port: Option<u16>,
    /// Port for the sidecar admin HTTP listener (bound to 127.0.0.1).
    /// Hosts `POST /internal/restart` used by the server's redump flow.
    /// Set to None to disable. Default: 9192.
    #[serde(default = "default_admin_port")]
    pub admin_port: Option<u16>,
    /// Directory containing pre-fetched CSV files for bulk loading.
    /// If set, the loader reads CSVs from here instead of downloading from PG.
    /// Defaults to `{storage_dir}/load_stage` when not specified.
    pub stage_dir: Option<PathBuf>,

    /// Path to the V2 sync config YAML (triggers + dump phases).
    /// If set, used for trigger reconciliation and dump pipeline.
    pub sync_config_path: Option<PathBuf>,

    /// Clear .done markers from stage_dir at boot (default: false).
    /// Set true only for fresh wipe scenarios — production can't re-download 80GB on every restart.
    #[serde(default)]
    pub clear_done_markers: bool,
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
    30
}
fn default_metrics_table() -> String {
    "entityMetricEvents_month".to_string()
}
fn default_metrics_totals_table() -> String {
    "entityMetricDailyAgg_v2".to_string()
}
fn default_metrics_entity_type() -> String {
    "Image".to_string()
}
fn default_metrics_reconcile_window_secs() -> u64 {
    1200
}
fn default_metrics_backfill_chunk_secs() -> u64 {
    3600
}
fn default_replica_id() -> String {
    "default".to_string()
}
fn default_progress_port() -> Option<u16> {
    Some(9091)
}
fn default_admin_port() -> Option<u16> {
    Some(9192)
}

impl PgSyncConfig {
    /// Resolved poll interval: `poll_interval_ms` if set, else `poll_interval_secs * 1000`.
    pub fn poll_interval(&self) -> std::time::Duration {
        let ms = self.poll_interval_ms.unwrap_or(self.poll_interval_secs * 1000);
        std::time::Duration::from_millis(ms)
    }

    /// Build the `MetricsPollerConfig` from the resolved sync config.
    pub fn metrics_poller_config(&self) -> crate::pg_sync::metrics_poller::MetricsPollerConfig {
        crate::pg_sync::metrics_poller::MetricsPollerConfig {
            table: self.metrics_table.clone(),
            totals_table: self.metrics_totals_table.clone(),
            entity_type: self.metrics_entity_type.clone(),
            poll_interval_secs: self.metrics_poll_interval_secs,
            reconcile_window_secs: self.metrics_reconcile_window_secs,
            backfill_chunk_secs: self.metrics_backfill_chunk_secs,
        }
    }

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
        if let Ok(replica) = std::env::var("BITDEX_REPLICA_ID") {
            config.replica_id = replica;
        }
        if let Ok(table) = std::env::var("BITDEX_METRICS_TABLE") {
            config.metrics_table = table;
        }
        if let Ok(table) = std::env::var("BITDEX_METRICS_TOTALS_TABLE") {
            config.metrics_totals_table = table;
        }
        if let Ok(v) = std::env::var("BITDEX_METRICS_RECONCILE_WINDOW_SECS") {
            match v.trim().parse::<u64>() {
                Ok(n) => config.metrics_reconcile_window_secs = n,
                Err(e) => {
                    return Err(format!(
                        "BITDEX_METRICS_RECONCILE_WINDOW_SECS={v:?} is not a valid u64 ({e})"
                    ));
                }
            }
        }
        if let Ok(v) = std::env::var("BITDEX_METRICS_BACKFILL_CHUNK_SECS") {
            match v.trim().parse::<u64>() {
                Ok(n) => config.metrics_backfill_chunk_secs = n,
                Err(e) => {
                    return Err(format!(
                        "BITDEX_METRICS_BACKFILL_CHUNK_SECS={v:?} is not a valid u64 ({e})"
                    ));
                }
            }
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
    /// Load an `IndexDefinition` from a config file (YAML or JSON).
    pub fn from_file(path: &Path) -> Result<Self, String> {
        let contents = std::fs::read_to_string(path)
            .map_err(|e| format!("failed to read {}: {}", path.display(), e))?;
        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        match ext {
            "yaml" | "yml" => serde_yaml::from_str(&contents)
                .map_err(|e| format!("failed to parse YAML {}: {}", path.display(), e)),
            _ => serde_json::from_str(&contents)
                .map_err(|e| format!("failed to parse JSON {}: {}", path.display(), e)),
        }
    }

    /// Load an `IndexDefinition` from config.yaml (preferred) or config.json inside `dir`.
    pub fn from_dir(dir: &Path) -> Result<Self, String> {
        let yaml = dir.join("config.yaml");
        if yaml.exists() {
            return Self::from_file(&yaml);
        }
        let yml = dir.join("config.yml");
        if yml.exists() {
            return Self::from_file(&yml);
        }
        let json = dir.join("config.json");
        if json.exists() {
            return Self::from_file(&json);
        }
        Err(format!("no config.yaml or config.json found in {}", dir.display()))
    }
}
