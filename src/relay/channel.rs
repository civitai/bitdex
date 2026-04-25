//! Per-channel broadcast registry.
//!
//! Each configured channel gets a `tokio::sync::broadcast::Sender<RelayEvent>`.
//! `RelayEvent` carries the rendered payload string and the monotonic seq_id
//! so SSE subscribers can detect gaps and capture writers can stamp markers.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::broadcast;

use crate::relay::config::RelayConfig;

#[derive(Debug, Clone)]
pub struct RelayEvent {
    pub seq_id: u64,
    pub ts_ms: u64,
    pub channel: String,
    pub payload: String,
}

pub struct ChannelRegistry {
    channels: BTreeMap<String, ChannelHandle>,
}

pub struct ChannelHandle {
    pub sender: broadcast::Sender<RelayEvent>,
    pub seq_counter: Arc<AtomicU64>,
    pub keep_alive_seconds: u64,
}

impl ChannelRegistry {
    pub fn from_config(config: &RelayConfig) -> Self {
        let mut channels = BTreeMap::new();
        for (name, ch) in &config.channels {
            let (tx, _rx) = broadcast::channel(ch.capacity);
            channels.insert(
                name.clone(),
                ChannelHandle {
                    sender: tx,
                    seq_counter: Arc::new(AtomicU64::new(0)),
                    keep_alive_seconds: ch.keep_alive_seconds,
                },
            );
        }
        Self { channels }
    }

    pub fn get(&self, name: &str) -> Option<&ChannelHandle> {
        self.channels.get(name)
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.channels.keys().map(|s| s.as_str())
    }
}

impl ChannelHandle {
    /// Atomically allocate the next seq_id.
    pub fn next_seq(&self) -> u64 {
        // fetch_add returns the old value; we want the new one for human-friendly 1-based ids.
        self.seq_counter.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Number of currently subscribed receivers. Used to gate emit work.
    pub fn receiver_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relay::config::ChannelConfig;
    use std::collections::BTreeMap;
    use std::net::SocketAddr;

    fn cfg() -> RelayConfig {
        RelayConfig {
            listen: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
            metrics_path: "/metrics".into(),
            admin_token_env: "T".into(),
            max_body_bytes: 1024,
            channels: {
                let mut m = BTreeMap::new();
                m.insert("a".into(), ChannelConfig { capacity: 4, keep_alive_seconds: 5 });
                m
            },
            routes: vec![],
            capture: Default::default(),
        }
    }

    #[test]
    fn allocates_monotonic_seq_ids() {
        let reg = ChannelRegistry::from_config(&cfg());
        let h = reg.get("a").unwrap();
        assert_eq!(h.next_seq(), 1);
        assert_eq!(h.next_seq(), 2);
        assert_eq!(h.next_seq(), 3);
    }

    #[test]
    fn receiver_count_starts_zero() {
        let reg = ChannelRegistry::from_config(&cfg());
        let h = reg.get("a").unwrap();
        assert_eq!(h.receiver_count(), 0);
        let _rx = h.sender.subscribe();
        assert_eq!(h.receiver_count(), 1);
    }
}
