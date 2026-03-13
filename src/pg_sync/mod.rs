//! Postgres-to-Bitdex sync system.
//!
//! Provides three modes of operation:
//! - `load`: Create BitdexOutbox table/triggers + bulk load from PG
//! - `sync`: Outbox poller + ClickHouse metrics poller (steady-state)
//! - `setup`: Create BitdexOutbox table/triggers only

pub mod bitdex_client;
pub mod bulk_loader;
pub mod config;
pub mod metrics_poller;
pub mod outbox_poller;
pub mod queries;
pub mod row_assembler;
pub mod slot_arena;
pub mod table_streams;
