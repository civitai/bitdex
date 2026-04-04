//! Bitmap sink traits and implementations for document ingestion.
//!
//! Two bitmap sinks:
//! - `CoalescerSink`: sends MutationOps to the write coalescer channel (online upserts)
//! - `AccumSink`: inserts directly into a BitmapAccum (bulk loading)

use std::sync::Arc;

use roaring::RoaringBitmap;

use crate::error::Result;
use super::loader::BitmapAccum;
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

/// BitmapSink that inserts directly into a BitmapAccum.
/// Used by the bulk loading path where bitmaps are accumulated in-memory
/// and applied to staging in one shot.
pub struct AccumSink<'a> {
    accum: &'a mut BitmapAccum,
}

impl<'a> AccumSink<'a> {
    #[allow(dead_code)]
    pub(crate) fn new(accum: &'a mut BitmapAccum) -> Self {
        Self { accum }
    }
}

impl<'a> BitmapSink for AccumSink<'a> {
    fn filter_insert(&mut self, field: Arc<str>, value: u64, slot: u32) {
        let field_name: &str = &field;
        if let Some(value_map) = self.accum.filter_maps.get_mut(field_name) {
            value_map
                .entry(value)
                .or_insert_with(RoaringBitmap::new)
                .insert(slot);
        }
    }

    fn filter_remove(&mut self, _field: Arc<str>, _value: u64, _slot: u32) {
        // Bulk loading never removes — this is a fresh insert path.
    }

    fn sort_set(&mut self, field: Arc<str>, bit_layer: usize, slot: u32) {
        let field_name: &str = &field;
        if let Some(layer_map) = self.accum.sort_maps.get_mut(field_name) {
            layer_map
                .entry(bit_layer)
                .or_insert_with(RoaringBitmap::new)
                .insert(slot);
        }
    }

    fn sort_clear(&mut self, _field: Arc<str>, _bit_layer: usize, _slot: u32) {
        // Bulk loading never clears sort layers.
    }

    fn alive_insert(&mut self, slot: u32) {
        self.accum.alive.insert(slot);
    }

    fn alive_remove(&mut self, _slot: u32) {
        // Bulk loading never removes alive bits.
    }

    fn deferred_alive(&mut self, _slot: u32, _activate_at: u64) {
        // In dump mode, deferred alive is a no-op for AccumSink.
        // The slot is NOT added to the alive bitmap (skipped in the caller).
        // The deferred alive map is built separately by the dump pipeline
        // and applied to the engine after the dump completes.
    }

    fn flush(&mut self) -> Result<()> {
        Ok(()) // Accum is in-memory, nothing to flush.
    }
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_accum_sink() {
        let mut accum = BitmapAccum::new(
            &["nsfwLevel".to_string()],
            &[("reactionCount".to_string(), 32)],
        );

        {
            let mut sink = AccumSink::new(&mut accum);
            sink.filter_insert(Arc::from("nsfwLevel"), 1, 10);
            sink.filter_insert(Arc::from("nsfwLevel"), 1, 20);
            sink.filter_insert(Arc::from("nsfwLevel"), 2, 30);
            sink.sort_set(Arc::from("reactionCount"), 0, 10);
            sink.sort_set(Arc::from("reactionCount"), 1, 10);
            sink.alive_insert(10);
            sink.alive_insert(20);
            sink.alive_insert(30);
        }

        assert_eq!(accum.alive.len(), 3);
        let nsfw_map = &accum.filter_maps["nsfwLevel"];
        assert_eq!(nsfw_map[&1].len(), 2);
        assert_eq!(nsfw_map[&2].len(), 1);
        let sort_map = &accum.sort_maps["reactionCount"];
        assert_eq!(sort_map[&0].len(), 1);
        assert_eq!(sort_map[&1].len(), 1);
    }
}
