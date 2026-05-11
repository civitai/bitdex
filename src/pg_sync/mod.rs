//! Postgres-to-Bitdex sync system (V2).
//!
//! Config-driven dump pipeline + ops-based steady-state sync.

pub mod admin_server;
pub mod backfill;
pub mod bitdex_client;
pub mod bulk_loader;
pub mod config;

/// Build the PG `bitdex_cursors.replica_id` key for an ops-poller cursor.
///
/// Single source of truth — the ops_poller, dump boot sequence, and the
/// sidecar admin handler all key cursor rows by this exact format. Drift
/// here (e.g. `pgsync-` vs `pg-sync-`) silently breaks cursor resume.
pub fn pg_sync_cursor_key(replica_id: &str) -> String {
    format!("pg-sync-{}", replica_id)
}
pub mod copy_queries;
pub mod csv_ops;
pub mod dump;
pub mod metrics_poller;
pub mod op_dedup;
pub mod ops;
pub mod ops_poller;
pub mod trigger_gen;
pub mod progress;
pub mod queries;
pub mod slot_arena;
pub mod sync_config;
