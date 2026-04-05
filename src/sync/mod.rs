//! BitDex sync system (V2).
//!
//! Config-driven dump pipeline + ops-based steady-state sync.

pub mod bitdex_client;
pub mod bulk_loader;
pub mod config;
pub mod dump;
pub mod dump_enrichment;
pub mod dump_expression;
pub mod dump_processor;
pub mod ingester;
pub mod metrics_poller;
pub mod op_dedup;
pub mod ops;
pub mod ops_poller;
pub mod trigger_gen;
pub mod queries;
pub mod sync_config;
