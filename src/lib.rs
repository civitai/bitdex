pub mod bucket_diff_log;
pub mod dump_enrichment;
pub mod dump_expression;
#[cfg(feature = "pg-sync")]
pub mod ops_processor;
#[cfg(feature = "pg-sync")]
pub mod ops_wal;
pub mod capture;
pub mod config;
pub mod dictionary;

pub mod engine;
pub mod silos;
pub mod query;

pub mod error;
pub mod ingester;
pub mod janitor;
pub mod loader;
pub mod mutation;
pub mod parser;
#[cfg(feature = "server")]
pub mod metrics;
#[cfg(feature = "server")]
pub mod server;
pub mod time_buckets;
pub mod types;
// unified_cache removed in Phase 3 — CacheSilo is the sole cache now
#[cfg(feature = "pg-sync")]
pub mod dump_processor;
#[cfg(feature = "pg-sync")]
pub mod pg_sync;
