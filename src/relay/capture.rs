//! NDJSON capture writer (V1 stub).
//!
//! V1 lands the trait surface + a no-op default. Full implementation (gzip
//! rotation, fsync policy, max_total_bytes enforcement) is a follow-up
//! commit on `feat/relay`.

use crate::relay::channel::RelayEvent;

pub trait CaptureSink: Send + Sync {
    fn write(&self, event: &RelayEvent);
    fn write_lagged_marker(&self, channel: &str, lagged_n: u64);
}

/// No-op sink — used when capture is disabled.
pub struct NullSink;

impl CaptureSink for NullSink {
    fn write(&self, _event: &RelayEvent) {}
    fn write_lagged_marker(&self, _channel: &str, _lagged_n: u64) {}
}
