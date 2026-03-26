//! Postgres-to-Bitdex sync system.
//!
//! Provides three modes of operation:
//! - `load`: Create BitdexOutbox table/triggers + bulk load from PG
//! - `sync`: Outbox poller + ClickHouse metrics poller (steady-state)
//! - `setup`: Create BitdexOutbox table/triggers only

pub mod backfill;
pub mod bitdex_client;
pub mod bulk_loader;
pub mod config;
pub mod copy_queries;
pub mod copy_streams;
pub mod metrics_poller;
pub mod op_dedup;
pub mod ops;
pub mod outbox_poller;
pub mod progress;
pub mod queries;
pub mod row_assembler;
pub mod single_pass;
pub mod slot_arena;
pub mod table_streams;
