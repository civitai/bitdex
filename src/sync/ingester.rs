//! Bitmap sink traits and implementations for document ingestion.
//!
//! Provides `CoalescerSink`: sends MutationOps to the write coalescer channel (online upserts).
//! The AccumSink (bulk loading) has been removed along with the V1 bulk loader.

use std::sync::Arc;

use crate::error::Result;
use crate::mutation::{MutationOp, MutationSender};

/// Trait for sinking bitmap mutations during document ingestion.
///
/// Implementations determine where bitmap operations go:
/// - Online path: send to coalescer channel for batched flush
/// - Bulk path: insert directly into accumulator for direct staging apply
pub trait BitmapSink {
    /// Record a filter bitmap insert: field[value] |= {slot}.
    fn filter_insert(&mut self, field: Arc<str>, value: u64, slot: u32);

    /// Record a filter bitmap remove: field[value] &= !{slot}.
    fn filter_remove(&mut self, field: Arc<str>, value: u64, slot: u32);

    /// Record a sort layer set: field.bit_layers[bit_layer] |= {slot}.
    fn sort_set(&mut self, field: Arc<str>, bit_layer: usize, slot: u32);

    /// Record a sort layer clear: field.bit_layers[bit_layer] &= !{slot}.
    fn sort_clear(&mut self, field: Arc<str>, bit_layer: usize, slot: u32);

    /// Record an alive bit insert.
    fn alive_insert(&mut self, slot: u32);

    /// Record an alive bit remove.
    fn alive_remove(&mut self, slot: u32);

    /// Schedule deferred alive activation at a future unix timestamp.
    /// The slot's filter/sort bitmaps are set immediately, but the alive bit
    /// is deferred until `activate_at` (seconds since epoch).
    fn deferred_alive(&mut self, slot: u32, activate_at: u64);

    /// Flush any buffered operations. Called after a batch of ingestions.
    fn flush(&mut self) -> Result<()>;
}

/// BitmapSink that sends MutationOps to the write coalescer channel.
/// Used by the online `put()` path for single-document upserts.
pub struct CoalescerSink {
    sender: MutationSender,
    /// Buffer ops for batch send.
    pending: Vec<MutationOp>,
}

impl CoalescerSink {
    pub fn new(sender: MutationSender) -> Self {
        Self {
            sender,
            pending: Vec::new(),
        }
    }
}

impl BitmapSink for CoalescerSink {
    fn filter_insert(&mut self, field: Arc<str>, value: u64, slot: u32) {
        self.pending.push(MutationOp::FilterInsert {
            field,
            value,
            slots: vec![slot],
        });
    }

    fn filter_remove(&mut self, field: Arc<str>, value: u64, slot: u32) {
        self.pending.push(MutationOp::FilterRemove {
            field,
            value,
            slots: vec![slot],
        });
    }

    fn sort_set(&mut self, field: Arc<str>, bit_layer: usize, slot: u32) {
        self.pending.push(MutationOp::SortSet {
            field,
            bit_layer,
            slots: vec![slot],
        });
    }

    fn sort_clear(&mut self, field: Arc<str>, bit_layer: usize, slot: u32) {
        self.pending.push(MutationOp::SortClear {
            field,
            bit_layer,
            slots: vec![slot],
        });
    }

    fn alive_insert(&mut self, slot: u32) {
        self.pending.push(MutationOp::AliveInsert {
            slots: vec![slot],
        });
    }

    fn deferred_alive(&mut self, slot: u32, activate_at: u64) {
        self.pending.push(MutationOp::DeferredAlive {
            slot,
            activate_at,
        });
    }

    fn alive_remove(&mut self, slot: u32) {
        self.pending.push(MutationOp::AliveRemove {
            slots: vec![slot],
        });
    }

    fn flush(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let ops = std::mem::take(&mut self.pending);
        self.sender.send_batch(ops).map_err(|_| {
            crate::error::BitdexError::CapacityExceeded(
                "coalescer channel disconnected".to_string(),
            )
        })
    }
}

