//! Ingester trait extraction for DocStore V2.
//!
//! Provides a unified interface for ingesting documents into BitDex,
//! abstracting the bitmap destination (coalescer channel vs accumulator)
//! and the document destination (docstore tuples).
//!
//! Two bitmap sinks:
//! - `CoalescerSink`: sends MutationOps to the write coalescer channel (online upserts)
//! - `AccumSink`: inserts directly into a BitmapAccum (bulk loading)
//!
//! `DocSink`: wraps `Arc<DocStore>` for V2 tuple appends.
//!
//! `Ingester<B: BitmapSink>`: holds a bitmap sink + doc sink, providing
//! a single `ingest()` method that routes to both.

use std::sync::Arc;

use roaring::RoaringBitmap;

use crate::doc_silo_adapter::DocSiloAdapter;
use crate::doc_format::StoredDoc;
use crate::error::Result;
use crate::loader::BitmapAccum;
use crate::write_coalescer::{MutationOp, MutationSender};

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

/// Document sink: wraps a DocSiloAdapter for doc writes.
///
/// Thread-safe via internal Mutex. Writes whole documents (not individual tuples).
pub struct DocSink {
    docstore: Arc<parking_lot::Mutex<DocSiloAdapter>>,
}

impl DocSink {
    pub fn new(docstore: Arc<parking_lot::Mutex<DocSiloAdapter>>) -> Self {
        Self { docstore }
    }

    /// Write a complete document to the silo.
    pub fn put(&self, slot: u32, doc: &StoredDoc) -> Result<()> {
        Ok(self.docstore.lock().put(slot, doc)?)
    }

    /// Write a batch of documents.
    pub fn put_batch(&self, docs: &[(u32, StoredDoc)]) -> Result<()> {
        Ok(self.docstore.lock().put_batch(docs)?)
    }
}

/// Unified ingester that routes bitmap mutations to a `BitmapSink` and
/// document tuples to a `DocSink`.
///
/// Generic over the bitmap sink to support both online (coalescer) and
/// bulk (accumulator) paths with the same ingestion logic.
pub struct Ingester<B: BitmapSink> {
    pub bitmap_sink: B,
    pub doc_sink: Option<DocSink>,
}

impl<B: BitmapSink> Ingester<B> {
    /// Create an ingester with both bitmap and doc sinks.
    pub fn new(bitmap_sink: B, doc_sink: DocSink) -> Self {
        Self {
            bitmap_sink,
            doc_sink: Some(doc_sink),
        }
    }

    /// Create an ingester with only a bitmap sink (no doc writes).
    pub fn bitmap_only(bitmap_sink: B) -> Self {
        Self {
            bitmap_sink,
            doc_sink: None,
        }
    }

    /// Emit a filter bitmap insert through the bitmap sink.
    pub fn filter_insert(&mut self, field: Arc<str>, value: u64, slot: u32) {
        self.bitmap_sink.filter_insert(field, value, slot);
    }

    /// Emit a sort layer set through the bitmap sink.
    pub fn sort_set(&mut self, field: Arc<str>, bit_layer: usize, slot: u32) {
        self.bitmap_sink.sort_set(field, bit_layer, slot);
    }

    /// Emit an alive insert through the bitmap sink.
    pub fn alive_insert(&mut self, slot: u32) {
        self.bitmap_sink.alive_insert(slot);
    }

    /// Write a document through the doc sink (if present).
    pub fn doc_put(&self, slot: u32, doc: &StoredDoc) -> Result<()> {
        if let Some(ref ds) = self.doc_sink {
            ds.put(slot, doc)?;
        }
        Ok(())
    }

    /// Flush buffered bitmap operations.
    pub fn flush(&mut self) -> Result<()> {
        self.bitmap_sink.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A test sink that records all operations for verification.
    struct RecordingSink {
        filter_inserts: Vec<(String, u64, u32)>,
        sort_sets: Vec<(String, usize, u32)>,
        alive_inserts: Vec<u32>,
    }

    impl RecordingSink {
        fn new() -> Self {
            Self {
                filter_inserts: Vec::new(),
                sort_sets: Vec::new(),
                alive_inserts: Vec::new(),
            }
        }
    }

    impl BitmapSink for RecordingSink {
        fn filter_insert(&mut self, field: Arc<str>, value: u64, slot: u32) {
            self.filter_inserts.push((field.to_string(), value, slot));
        }
        fn filter_remove(&mut self, _field: Arc<str>, _value: u64, _slot: u32) {}
        fn sort_set(&mut self, field: Arc<str>, bit_layer: usize, slot: u32) {
            self.sort_sets.push((field.to_string(), bit_layer, slot));
        }
        fn sort_clear(&mut self, _field: Arc<str>, _bit_layer: usize, _slot: u32) {}
        fn alive_insert(&mut self, slot: u32) {
            self.alive_inserts.push(slot);
        }
        fn alive_remove(&mut self, _slot: u32) {}
        fn deferred_alive(&mut self, _slot: u32, _activate_at: u64) {}
        fn flush(&mut self) -> Result<()> { Ok(()) }
    }

    #[test]
    fn test_recording_sink() {
        let mut sink = RecordingSink::new();
        let field: Arc<str> = Arc::from("nsfwLevel");

        sink.filter_insert(field.clone(), 1, 42);
        sink.filter_insert(field.clone(), 2, 43);
        sink.alive_insert(42);
        sink.alive_insert(43);
        sink.sort_set(Arc::from("reactionCount"), 0, 42);

        assert_eq!(sink.filter_inserts.len(), 2);
        assert_eq!(sink.alive_inserts, vec![42, 43]);
        assert_eq!(sink.sort_sets.len(), 1);
    }

    #[test]
    fn test_ingester_bitmap_only() {
        let sink = RecordingSink::new();
        let mut ingester = Ingester::bitmap_only(sink);

        ingester.filter_insert(Arc::from("tag"), 100, 5);
        ingester.alive_insert(5);
        ingester.flush().unwrap();

        assert_eq!(ingester.bitmap_sink.filter_inserts.len(), 1);
        assert_eq!(ingester.bitmap_sink.alive_inserts, vec![5]);
    }

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

        // Verify accum state
        assert_eq!(accum.alive.len(), 3);
        let nsfw_map = &accum.filter_maps["nsfwLevel"];
        assert_eq!(nsfw_map[&1].len(), 2); // slots 10, 20
        assert_eq!(nsfw_map[&2].len(), 1); // slot 30
        let sort_map = &accum.sort_maps["reactionCount"];
        assert_eq!(sort_map[&0].len(), 1); // slot 10
        assert_eq!(sort_map[&1].len(), 1); // slot 10
    }

    #[test]
    fn test_doc_sink_put() {
        use crate::doc_silo_adapter::DocSiloAdapter;

        let mut adapter = DocSiloAdapter::open_temp().unwrap();
        adapter.ensure_field_index("val").unwrap();

        let store = Arc::new(parking_lot::Mutex::new(adapter));
        let sink = DocSink::new(Arc::clone(&store));

        // Write a doc via DocSink
        let mut fields = std::collections::HashMap::new();
        fields.insert("val".to_string(), crate::mutation::FieldValue::Single(crate::query::Value::Integer(42)));
        let doc = StoredDoc { fields, schema_version: 0 };
        sink.put(5, &doc).unwrap();

        // Read via get and verify
        let loaded = store.lock().get(5).unwrap().unwrap();
        match &loaded.fields["val"] {
            crate::mutation::FieldValue::Single(crate::query::Value::Integer(42)) => {}
            other => panic!("expected val=42, got: {:?}", other),
        }
    }

    #[test]
    fn test_ingester_full_pipeline() {
        use crate::doc_silo_adapter::DocSiloAdapter;

        let mut adapter = DocSiloAdapter::open_temp().unwrap();
        adapter.ensure_field_index("color").unwrap();

        let store = Arc::new(parking_lot::Mutex::new(adapter));
        let doc_sink = DocSink::new(Arc::clone(&store));
        let bitmap_sink = RecordingSink::new();

        let mut ingester = Ingester::new(bitmap_sink, doc_sink);

        // Emit bitmap operations
        ingester.filter_insert(Arc::from("color"), 7, 100);
        ingester.sort_set(Arc::from("reactionCount"), 3, 100);
        ingester.alive_insert(100);

        // Write a doc
        let mut fields = std::collections::HashMap::new();
        fields.insert("color".to_string(), crate::mutation::FieldValue::Single(crate::query::Value::Integer(7)));
        let doc = StoredDoc { fields, schema_version: 0 };
        ingester.doc_put(100, &doc).unwrap();

        // Flush bitmaps
        ingester.flush().unwrap();

        // Verify bitmap sink recorded everything
        assert_eq!(ingester.bitmap_sink.filter_inserts.len(), 1);
        assert_eq!(ingester.bitmap_sink.filter_inserts[0], ("color".to_string(), 7, 100));
        assert_eq!(ingester.bitmap_sink.sort_sets.len(), 1);
        assert_eq!(ingester.bitmap_sink.sort_sets[0], ("reactionCount".to_string(), 3, 100));
        assert_eq!(ingester.bitmap_sink.alive_inserts, vec![100]);

        let loaded = store.lock().get(100).unwrap().unwrap();
        match &loaded.fields["color"] {
            crate::mutation::FieldValue::Single(crate::query::Value::Integer(7)) => {}
            other => panic!("expected color=7, got: {:?}", other),
        }
    }
}
