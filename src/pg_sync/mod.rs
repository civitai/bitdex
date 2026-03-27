//! Postgres-to-Bitdex sync system (V2).
//!
//! Config-driven dump pipeline + ops-based steady-state sync.

pub mod backfill;
pub mod bitdex_client;
pub mod bulk_loader;
pub mod config;
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
