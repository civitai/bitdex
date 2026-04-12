//! DataSilo — mmap-backed typed KV store for BitDex.
//!
//! Three mmap'd files per silo:
//! - `index.bin` — open-addressed hash table, `u64` key → `(offset, length, allocated)`
//! - `data.bin`  — packed, compacted snapshot bytes (one encoded snapshot per key)
//! - `ops_a.log` / `ops_b.log` — typed, framed ops log (A/B swap for compaction)
//!
//! Design pattern matches v2 `ShardStore`: snapshot + ops log, with codecs plugged
//! in via the `SnapshotCodec` + `OpCodec` traits. The storage primitive is "one
//! big mmap'd file" instead of "thousands of small shard files" — that is the
//! only difference from ShardStore in spirit.
//!
//! All writes go through the active ops log. `compact` folds a frozen log into
//! the data file via `OpCodec::apply`, atomically swapping the active slot so
//! new writes keep flowing with no read-stall window.
//!
//! Field-level mutations (`Set`, `Append`, `Remove`) are first-class typed
//! ops — they are NOT re-encoded-whole-doc blobs. One `Set` on one field is
//! ~20 bytes in the log.

#![allow(clippy::missing_safety_doc)]

use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use parking_lot::Mutex;

pub mod hash_index;
pub mod ops_log;
pub mod traits;
#[cfg(test)]
mod tests;

pub use hash_index::HashIndex;
pub use ops_log::{OpsLog, ParallelOpsWriter};
pub use traits::{OpCodec, SnapshotCodec};

// ---------------------------------------------------------------------------
// Error types + result alias
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum SiloError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("hash table is full (load factor exceeded)")]
    TableFull,

    #[error("key 0 is reserved (empty sentinel)")]
    ReservedKey,

    #[error("file is too small to be a valid hash index")]
    InvalidFile,
}

pub type Result<T> = std::result::Result<T, SiloError>;

// ---------------------------------------------------------------------------
// IndexEntry — 16 bytes per key (wire-compatible with hash_index.rs)
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(C)]
pub struct IndexEntry {
    pub offset: u64,
    pub length: u32,
    pub allocated: u32,
}

// ---------------------------------------------------------------------------
// SiloConfig
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct SiloConfig {
    /// Slack multiplier applied to each snapshot on (re)write.
    /// e.g. `1.3` = 30% headroom; allows in-place updates when a re-encoded
    /// snapshot fits within the original `allocated` region.
    pub buffer_ratio: f32,
    /// Minimum bytes allocated per entry regardless of snapshot size.
    /// Gives very small docs headroom to grow before needing compaction.
    pub min_entry_size: u32,
    /// Entry alignment in bytes. Data offsets are rounded up to multiples
    /// of this value. Default `1` (no alignment). Raise for mmap'd types
    /// that require alignment (e.g. 32 for `FrozenRoaringBitmap`).
    pub alignment: u32,
    /// Auto-compaction trigger: fraction of `data.bin` that's dead space.
    /// `0.0` disables automatic compaction.
    pub compact_threshold: f32,
}

impl Default for SiloConfig {
    fn default() -> Self {
        Self {
            buffer_ratio: 1.3,
            min_entry_size: 256,
            alignment: 1,
            compact_threshold: 0.20,
        }
    }
}

// ---------------------------------------------------------------------------
// DumpMergeWriter — direct read-modify-write for dump phases 2+
// ---------------------------------------------------------------------------

const MERGE_STRIPE_COUNT: usize = 1024;

/// Handle for direct read-modify-write during dump phases.
///
/// Created by `DataSilo::prepare_dump_merge()` after phase 1 has written
/// all slot entries via `bulk_load`. Subsequent phases (tags, tools,
/// techniques, resources) use this to read existing doc records, merge new
/// field data, and write back in-place.
///
/// Bypasses the ops log entirely — no compaction needed.
///
/// Thread-safe via striped locks: each key is serialized by `key % 1024`,
/// but distinct keys can be written concurrently from rayon threads.
pub struct DumpMergeWriter {
    write_ptr: *mut u8,
    write_mmap: memmap2::MmapMut,
    data_len: usize,
    index_ptr: *const HashIndex,
    stripes: Box<[parking_lot::Mutex<()>]>,
    pub in_place_count: AtomicU64,
    pub overflow_count: AtomicU64,
}

// SAFETY: DumpMergeWriter is Send+Sync because:
// - write_ptr points to a stable MmapMut (not freed during writer lifetime)
// - Both reads and writes go through write_ptr (no dual-mmap aliasing)
// - index_ptr points to DataSilo's HashIndex (stable during dump)
// - Stripe locks ensure no two threads access the same key simultaneously
unsafe impl Send for DumpMergeWriter {}
unsafe impl Sync for DumpMergeWriter {}

impl DumpMergeWriter {
    /// Merge new data into an existing entry using a caller-provided merge function.
    ///
    /// The merge function receives `(existing_bytes, new_bytes)` and returns the
    /// merged result. If the key has no existing data (length=0), `new_bytes` is
    /// written directly without calling the merge function.
    ///
    /// Returns `true` if the write succeeded (in-place), `false` if the key
    /// doesn't exist or the merged data exceeds the allocated buffer.
    #[inline]
    pub fn merge_put<F>(&self, key: u64, new_bytes: &[u8], merge_fn: F) -> bool
    where
        F: FnOnce(&[u8], &[u8]) -> Vec<u8>,
    {
        let stripe = (key as usize) % MERGE_STRIPE_COUNT;
        let _guard = self.stripes[stripe].lock();

        let index = unsafe { &*self.index_ptr };
        let entry = match index.get(key) {
            Some(e) => e,
            None => {
                self.overflow_count.fetch_add(1, Ordering::Relaxed);
                return false;
            }
        };

        let start = entry.offset as usize;

        let to_write = if entry.length == 0 {
            std::borrow::Cow::Borrowed(new_bytes)
        } else {
            let end = start + entry.length as usize;
            if end > self.data_len {
                self.overflow_count.fetch_add(1, Ordering::Relaxed);
                return false;
            }
            let existing = unsafe {
                std::slice::from_raw_parts(self.write_ptr.add(start) as *const u8, entry.length as usize)
            };
            std::borrow::Cow::Owned(merge_fn(existing, new_bytes))
        };

        if to_write.len() as u32 > entry.allocated {
            self.overflow_count.fetch_add(1, Ordering::Relaxed);
            return false;
        }

        unsafe {
            std::ptr::copy_nonoverlapping(
                to_write.as_ptr(),
                self.write_ptr.add(start),
                to_write.len(),
            );
        }

        if to_write.len() as u32 != entry.length {
            unsafe {
                index.update_existing_concurrent(key, IndexEntry {
                    offset: entry.offset,
                    length: to_write.len() as u32,
                    allocated: entry.allocated,
                });
            }
        }

        self.in_place_count.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Write new data directly to an existing slot without merging.
    #[inline]
    pub fn put_direct(&self, key: u64, data: &[u8]) -> bool {
        let stripe = (key as usize) % MERGE_STRIPE_COUNT;
        let _guard = self.stripes[stripe].lock();

        let index = unsafe { &*self.index_ptr };
        let entry = match index.get(key) {
            Some(e) => e,
            None => {
                self.overflow_count.fetch_add(1, Ordering::Relaxed);
                return false;
            }
        };

        if data.len() as u32 > entry.allocated {
            self.overflow_count.fetch_add(1, Ordering::Relaxed);
            return false;
        }

        let start = entry.offset as usize;
        unsafe {
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                self.write_ptr.add(start),
                data.len(),
            );
        }

        if data.len() as u32 != entry.length {
            unsafe {
                index.update_existing_concurrent(key, IndexEntry {
                    offset: entry.offset,
                    length: data.len() as u32,
                    allocated: entry.allocated,
                });
            }
        }

        self.in_place_count.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Flush the writable mmap to disk.
    pub fn flush(&mut self) -> io::Result<()> {
        self.write_mmap.flush()
    }
}

// ---------------------------------------------------------------------------
// DataSilo — the main store
// ---------------------------------------------------------------------------

/// Typed, mmap-backed KV store.
///
/// Generic over a paired `SnapshotCodec + OpCodec` — the codec combo defines
/// the shape of the compacted data file entries and the shape of log entries,
/// and ties them together through `apply`.
pub struct DataSilo<S: SnapshotCodec, O: OpCodec<Snapshot = S::Snapshot>> {
    path: PathBuf,
    config: SiloConfig,
    index: Option<HashIndex>,
    data_mmap: Option<memmap2::Mmap>,
    data_len: u64,
    ops_a: Mutex<OpsLog>,
    ops_b: Mutex<OpsLog>,
    /// `false` = A is active, `true` = B is active.
    active_is_b: AtomicBool,
    /// Bytes of `data.bin` shadowed by relocated / deleted entries.
    dead_bytes: AtomicU64,
    _codecs: std::marker::PhantomData<(S, O)>,
}

unsafe impl<S: SnapshotCodec, O: OpCodec<Snapshot = S::Snapshot>> Send for DataSilo<S, O> {}
unsafe impl<S: SnapshotCodec, O: OpCodec<Snapshot = S::Snapshot>> Sync for DataSilo<S, O> {}

impl<S: SnapshotCodec, O: OpCodec<Snapshot = S::Snapshot>> DataSilo<S, O> {
    /// Open or create a silo at `path`.
    pub fn open(path: &Path, config: SiloConfig) -> io::Result<Self> {
        std::fs::create_dir_all(path)?;

        // Back-compat: if an old single-log `ops.log` exists, rename to `ops_a.log`.
        let legacy = path.join("ops.log");
        let ops_a_path = path.join("ops_a.log");
        if legacy.exists() && !ops_a_path.exists() {
            std::fs::rename(&legacy, &ops_a_path)?;
        }

        let ops_a = OpsLog::open(&ops_a_path)?;
        let ops_b = OpsLog::open(&path.join("ops_b.log"))?;

        let mut silo = Self {
            path: path.to_path_buf(),
            config,
            index: None,
            data_mmap: None,
            data_len: 0,
            ops_a: Mutex::new(ops_a),
            ops_b: Mutex::new(ops_b),
            active_is_b: AtomicBool::new(false),
            dead_bytes: AtomicU64::new(0),
            _codecs: std::marker::PhantomData,
        };

        silo.load_index()?;
        silo.load_data()?;
        Ok(silo)
    }

    fn load_index(&mut self) -> io::Result<()> {
        let index_path = self.path.join("index.bin");
        let layouts_path = self.path.join("layouts.bin");

        // Lazy-build fallback: if `layouts.bin` is present we were asked to
        // build the hash index at open time (the bulk writer deferred it).
        // `layouts.bin` wire format:
        //     [count:u64][ (key:u64, offset:u64, length:u32, allocated:u32) × count ]
        // All little-endian, tightly packed (24 bytes per entry).
        if !index_path.exists() && layouts_path.exists() {
            use std::io::Read;
            let mut file = std::fs::File::open(&layouts_path)?;
            let mut count_buf = [0u8; 8];
            file.read_exact(&mut count_buf)?;
            let count = u64::from_le_bytes(count_buf) as usize;
            let mut raw = vec![0u8; count * 24];
            file.read_exact(&mut raw)?;
            let mut entries: Vec<(u64, IndexEntry)> = Vec::with_capacity(count);
            for i in 0..count {
                let b = &raw[i * 24..(i + 1) * 24];
                let key = u64::from_le_bytes(b[0..8].try_into().unwrap());
                let offset = u64::from_le_bytes(b[8..16].try_into().unwrap());
                let length = u32::from_le_bytes(b[16..20].try_into().unwrap());
                let allocated = u32::from_le_bytes(b[20..24].try_into().unwrap());
                entries.push((key, IndexEntry { offset, length, allocated }));
            }
            drop(raw);
            let idx = HashIndex::build_bulk(&index_path, &entries)
                .map_err(|e| io::Error::new(
                    io::ErrorKind::Other,
                    format!("lazy build hash index from layouts.bin: {e}"),
                ))?;
            self.index = Some(idx);
            // Drop the sidecar now that index.bin is persisted.
            let _ = std::fs::remove_file(&layouts_path);
            return Ok(());
        }

        if !index_path.exists() {
            return Ok(());
        }
        let idx = HashIndex::open(&index_path)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("open hash index: {e}")))?;
        self.index = Some(idx);
        Ok(())
    }

    fn load_data(&mut self) -> io::Result<()> {
        let data_path = self.path.join("data.bin");
        if !data_path.exists() {
            self.data_mmap = None;
            self.data_len = 0;
            return Ok(());
        }
        let file = File::open(&data_path)?;
        let len = file.metadata()?.len();
        if len == 0 {
            self.data_mmap = None;
            self.data_len = 0;
            return Ok(());
        }
        let mmap = unsafe { memmap2::Mmap::map(&file)? };
        #[cfg(unix)]
        let _ = mmap.advise(memmap2::Advice::Random);
        self.data_mmap = Some(mmap);
        self.data_len = len;
        Ok(())
    }

    // ── Ops log write path ─────────────────────────────────────────────

    /// Reference to the currently active ops log.
    fn active_log(&self) -> &Mutex<OpsLog> {
        if self.active_is_b.load(Ordering::Acquire) {
            &self.ops_b
        } else {
            &self.ops_a
        }
    }

    /// Append one typed op to the active log, flushing immediately.
    pub fn append_op(&self, op: &O::Op) -> io::Result<()> {
        let mut log = self.active_log().lock();
        log.append::<O>(op)?;
        log.flush()
    }

    /// Append many typed ops to the active log in one batch, flushing once at end.
    pub fn append_ops_batch(&self, ops: &[O::Op]) -> io::Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        let mut log = self.active_log().lock();
        log.append_batch::<O>(ops)?;
        log.flush()
    }

    /// Is there any pending work in either log?
    pub fn has_ops(&self) -> bool {
        !self.ops_a.lock().is_empty() || !self.ops_b.lock().is_empty()
    }

    /// Total bytes in both ops logs.
    pub fn ops_size(&self) -> u64 {
        self.ops_a.lock().data_size() + self.ops_b.lock().data_size()
    }

    // ── Read path ──────────────────────────────────────────────────────

    /// Load the raw compacted snapshot bytes for `key` from `data.bin`, if any.
    fn data_bytes_for(&self, key: u64) -> Option<&[u8]> {
        let entry = self.index.as_ref()?.get(key)?;
        if entry.length == 0 {
            return None;
        }
        let mmap = self.data_mmap.as_ref()?;
        let start = entry.offset as usize;
        let end = start + entry.length as usize;
        if end <= mmap.len() {
            Some(&mmap[start..end])
        } else {
            None
        }
    }

    /// Fast path: get the compacted snapshot only (skip ops overlay).
    /// Use when you know the ops log is empty (e.g., just after compaction).
    pub fn get_compacted(&self, key: u64) -> io::Result<Option<S::Snapshot>> {
        match self.data_bytes_for(key) {
            Some(bytes) => S::decode(bytes).map(Some),
            None => Ok(None),
        }
    }

    /// Load the snapshot for `key`, overlaying any ops from both logs.
    ///
    /// Hot read path. Returns `None` iff neither the data file nor the ops
    /// logs contain any record for `key`.
    pub fn get(&self, key: u64) -> io::Result<Option<S::Snapshot>> {
        // Seed with compacted snapshot, or empty if none.
        let seed = match self.data_bytes_for(key) {
            Some(bytes) => Some(S::decode(bytes)?),
            None => None,
        };

        // Scan both logs in order (A then B) for ops matching this key.
        // Last-write-wins across the two logs — A is the older side during
        // compaction, B is the newer.
        let mut snapshot = seed;
        let mut touched = false;

        let apply_from = |log: &OpsLog,
                          snap: &mut Option<S::Snapshot>,
                          touched: &mut bool|
         -> io::Result<()> {
            log.for_each(|op_bytes| {
                if let Ok(op) = O::decode_op(op_bytes) {
                    if O::op_key(&op) == key {
                        let s = snap.get_or_insert_with(S::empty);
                        O::apply(s, &op);
                        *touched = true;
                    }
                }
            })?;
            Ok(())
        };

        apply_from(&self.ops_a.lock(), &mut snapshot, &mut touched)?;
        apply_from(&self.ops_b.lock(), &mut snapshot, &mut touched)?;

        if snapshot.is_none() {
            return Ok(None);
        }

        // If we only got a seed with no ops, return it; otherwise return the
        // overlaid version.
        let _ = touched;
        Ok(snapshot)
    }

    /// Batched read — one hash probe + ops-log scan per key. Shares the
    /// ops-log locks across the whole batch to avoid per-key lock thrash.
    pub fn get_many(&self, keys: &[u64]) -> io::Result<Vec<Option<S::Snapshot>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        // Phase 1: seed snapshots from data.bin (no locks).
        let mut out: Vec<Option<S::Snapshot>> = keys
            .iter()
            .map(|&key| match self.data_bytes_for(key) {
                Some(bytes) => S::decode(bytes).ok(),
                None => None,
            })
            .collect();

        // Phase 2: scan each ops log once, applying every op to any key that
        // matches. O(keys × ops_in_log) worst case — fine at steady state
        // where ops_in_log is bounded by compaction.
        let key_set: std::collections::HashMap<u64, usize> = keys
            .iter()
            .enumerate()
            .map(|(i, &k)| (k, i))
            .collect();

        let mut scan = |log: &OpsLog| -> io::Result<()> {
            log.for_each(|op_bytes| {
                if let Ok(op) = O::decode_op(op_bytes) {
                    if let Some(&idx) = key_set.get(&O::op_key(&op)) {
                        let slot = out[idx].get_or_insert_with(S::empty);
                        O::apply(slot, &op);
                    }
                }
            })?;
            Ok(())
        };

        scan(&self.ops_a.lock())?;
        scan(&self.ops_b.lock())?;

        Ok(out)
    }

    // ── Bulk load (destructive) ────────────────────────────────────────

    /// Replace the data file and index with a fresh bulk snapshot.
    ///
    /// Used by the dump pipeline's phase 1 (e.g. images) to seed every slot
    /// at maximum throughput. Subsequent phases use ops log appends or the
    /// `DumpMergeWriter` to layer in additional fields.
    ///
    /// Destructive: truncates both ops logs.
    pub fn bulk_load(&mut self, entries: &[(u64, S::Snapshot)]) -> io::Result<u64> {
        if entries.is_empty() {
            return Ok(0);
        }

        // Phase 1: encode each snapshot once. Delegate the rest to
        // `bulk_load_encoded` so the two paths share the same layout +
        // index build logic.
        let mut encoded: Vec<(u64, Vec<u8>)> = Vec::with_capacity(entries.len());
        for (k, snap) in entries {
            let mut buf = Vec::with_capacity(256);
            S::encode(snap, &mut buf);
            encoded.push((*k, buf));
        }
        self.bulk_load_encoded(encoded)
    }

    /// Replace the data file and index with a fresh bulk snapshot given
    /// **pre-encoded** entries. This skips the `SnapshotCodec::encode`
    /// step — the caller is responsible for producing bytes in whatever
    /// format `SnapshotCodec::decode` expects.
    ///
    /// Used by streaming populate paths where encoding happens inline
    /// during fetch so the intermediate `S::Snapshot` value never
    /// accumulates in memory. Avoids double-buffering at bulk-load
    /// scale — important when the full dataset exceeds RAM.
    ///
    /// Takes `Vec<(u64, Vec<u8>)>` by value so the caller's buffer is
    /// consumed and reclaimed after layout.
    ///
    /// Destructive: truncates both ops logs.
    pub fn bulk_load_encoded(&mut self, encoded: Vec<(u64, Vec<u8>)>) -> io::Result<u64> {
        if encoded.is_empty() {
            return Ok(0);
        }
        let count = encoded.len() as u64;
        let align = self.config.alignment.max(1) as u64;
        let buffer_ratio = self.config.buffer_ratio;
        let min_entry_size = self.config.min_entry_size;

        self.index = None;
        self.data_mmap = None;

        let mut encoded = encoded;
        // Sort by key for insertion locality.
        encoded.sort_unstable_by_key(|(k, _)| *k);

        struct Layout {
            key: u64,
            offset: u64,
            length: u32,
            allocated: u32,
        }
        let mut layouts: Vec<Layout> = Vec::with_capacity(encoded.len());
        let mut offset: u64 = 0;
        for (k, bytes) in &encoded {
            if align > 1 {
                offset = (offset + align - 1) & !(align - 1);
            }
            let length = bytes.len() as u32;
            let mut allocated = ((length as f32 * buffer_ratio).ceil() as u32).max(min_entry_size);
            if align > 1 {
                allocated = ((allocated as u64 + align - 1) & !(align - 1)) as u32;
            }
            layouts.push(Layout {
                key: *k,
                offset,
                length,
                allocated,
            });
            offset += allocated as u64;
        }
        let total = offset;

        // Phase 2: pre-allocate and mmap the data file, then parallel-write each entry.
        let data_path = self.path.join("data.bin");
        let data_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&data_path)?;
        data_file.set_len(total)?;
        let mut data_mmap = unsafe { memmap2::MmapMut::map_mut(&data_file)? };
        #[cfg(unix)]
        let _ = data_mmap.advise(memmap2::Advice::Sequential);

        let base = data_mmap.as_mut_ptr() as usize;
        let mmap_len = data_mmap.len();
        use rayon::prelude::*;
        layouts.par_iter().enumerate().for_each(|(i, layout)| {
            let bytes = &encoded[i].1;
            let start = layout.offset as usize;
            if start + bytes.len() <= mmap_len {
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        bytes.as_ptr(),
                        (base + start) as *mut u8,
                        bytes.len(),
                    );
                }
            }
        });
        data_mmap.flush()?;
        drop(data_mmap);

        // Phase 3: bulk-build the hash index from a heap-side buffer, one
        // sequential write pass.
        let index_path = self.path.join("index.bin");
        if index_path.exists() {
            let _ = std::fs::remove_file(&index_path);
        }
        let index_entries: Vec<(u64, IndexEntry)> = layouts
            .iter()
            .map(|l| {
                (
                    l.key,
                    IndexEntry {
                        offset: l.offset,
                        length: l.length,
                        allocated: l.allocated,
                    },
                )
            })
            .collect();
        let idx = HashIndex::build_bulk(&index_path, &index_entries)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("build_bulk: {e}")))?;

        self.index = Some(idx);
        self.load_data()?;
        self.data_len = total;
        self.dead_bytes.store(0, Ordering::Relaxed);

        // Truncate both ops logs — we just wrote the canonical state.
        self.ops_a.lock().truncate()?;
        self.ops_b.lock().truncate()?;

        Ok(count)
    }

    // ── Compaction ─────────────────────────────────────────────────────

    /// Does `dead_bytes / data_len` exceed the auto-compact threshold?
    pub fn needs_compaction(&self) -> bool {
        if self.config.compact_threshold <= 0.0 || self.data_len == 0 {
            return false;
        }
        let dead = self.dead_bytes.load(Ordering::Relaxed) as f64;
        dead / self.data_len as f64 > self.config.compact_threshold as f64
    }

    pub fn dead_bytes(&self) -> u64 {
        self.dead_bytes.load(Ordering::Relaxed)
    }

    pub fn data_bytes(&self) -> u64 {
        self.data_len
    }

    /// Fold the frozen ops log into the data file.
    ///
    /// Protocol:
    ///   1. Atomically flip `active_is_b`. New writes go to the fresh slot.
    ///   2. Collect every op in the (now-frozen) other slot, grouped by key,
    ///      in append order.
    ///   3. For each touched key: seed with the existing snapshot from
    ///      `data.bin` (or `S::empty()`), apply all frozen ops via `O::apply`,
    ///      re-encode, and write the result — in place if it fits `allocated`,
    ///      otherwise appended to the end of `data.bin` (leaving the old
    ///      region as `dead_bytes`).
    ///   4. Truncate the frozen log.
    ///
    /// Returns the number of ops folded.
    pub fn compact(&mut self) -> io::Result<u64> {
        // Nothing to do if the active slot is empty.
        if self.active_log().lock().is_empty() {
            return Ok(0);
        }

        // Step 1: swap. fetch_xor returns the OLD value of active_is_b,
        // which is exactly the side that just became frozen.
        let frozen_is_b = self.active_is_b.fetch_xor(true, Ordering::SeqCst);

        // Step 2: drain frozen log into (key → Vec<Op>) map in append order.
        let mut grouped: std::collections::HashMap<u64, Vec<O::Op>> =
            std::collections::HashMap::new();
        let mut total_ops: u64 = 0;
        {
            let log = if frozen_is_b {
                self.ops_b.lock()
            } else {
                self.ops_a.lock()
            };
            log.for_each(|op_bytes| {
                if let Ok(op) = O::decode_op(op_bytes) {
                    let key = O::op_key(&op);
                    grouped.entry(key).or_default().push(op);
                    total_ops += 1;
                }
            })?;
        }

        if grouped.is_empty() {
            // Frozen log had only corrupt frames — truncate and return.
            if frozen_is_b {
                self.ops_b.lock().truncate()?;
            } else {
                self.ops_a.lock().truncate()?;
            }
            return Ok(0);
        }

        // Step 3: apply per-key and write back to the data file.
        // We open a writable mmap on `data.bin` so in-place rewrites are
        // possible. Entries that grow beyond their allocated slack get
        // appended to a grow tail, with the old region marked as dead.
        let data_path = self.path.join("data.bin");
        let data_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&data_path)?;
        // Ensure the file is at least `data_len` so mmap covers existing data.
        let current_len = data_file.metadata()?.len().max(self.data_len);
        if current_len == 0 {
            // Cold path — no data file yet. Treat every key as new.
            data_file.set_len(0)?;
        }

        // Collect grow-tail writes separately; we'll extend the file at the end.
        struct Grow {
            key: u64,
            bytes: Vec<u8>,
            old_allocated: Option<u32>,
        }
        let mut grow: Vec<Grow> = Vec::new();
        // Track in-place rewrites so we can apply them after we have the mmap.
        struct InPlace {
            key: u64,
            offset: u64,
            bytes: Vec<u8>,
            new_length: u32,
            old_length: u32,
            allocated: u32,
        }
        let mut in_place: Vec<InPlace> = Vec::new();

        for (key, ops) in &grouped {
            // Seed from existing snapshot.
            let mut snap = match self.data_bytes_for(*key) {
                Some(bytes) => S::decode(bytes)?,
                None => S::empty(),
            };
            for op in ops {
                O::apply(&mut snap, op);
            }
            let mut buf = Vec::with_capacity(256);
            S::encode(&snap, &mut buf);

            match self.index.as_ref().and_then(|idx| idx.get(*key)) {
                Some(entry) if (buf.len() as u32) <= entry.allocated => {
                    in_place.push(InPlace {
                        key: *key,
                        offset: entry.offset,
                        new_length: buf.len() as u32,
                        old_length: entry.length,
                        allocated: entry.allocated,
                        bytes: buf,
                    });
                }
                Some(entry) => {
                    // Doesn't fit — relocate. Old region becomes dead.
                    grow.push(Grow {
                        key: *key,
                        bytes: buf,
                        old_allocated: Some(entry.allocated),
                    });
                }
                None => {
                    // New key. No old region.
                    grow.push(Grow {
                        key: *key,
                        bytes: buf,
                        old_allocated: None,
                    });
                }
            }
        }

        // Compute grow tail layout.
        let align = self.config.alignment.max(1) as u64;
        let buffer_ratio = self.config.buffer_ratio;
        let min_entry_size = self.config.min_entry_size;
        let mut grow_offset = current_len;
        struct GrowLayout {
            key: u64,
            offset: u64,
            length: u32,
            allocated: u32,
            bytes: Vec<u8>,
            old_allocated: Option<u32>,
        }
        let mut grow_layouts: Vec<GrowLayout> = Vec::with_capacity(grow.len());
        for g in grow {
            if align > 1 {
                grow_offset = (grow_offset + align - 1) & !(align - 1);
            }
            let length = g.bytes.len() as u32;
            let mut allocated =
                ((length as f32 * buffer_ratio).ceil() as u32).max(min_entry_size);
            if align > 1 {
                allocated = ((allocated as u64 + align - 1) & !(align - 1)) as u32;
            }
            grow_layouts.push(GrowLayout {
                key: g.key,
                offset: grow_offset,
                length,
                allocated,
                bytes: g.bytes,
                old_allocated: g.old_allocated,
            });
            grow_offset += allocated as u64;
        }
        let new_data_len = grow_offset;
        if new_data_len > current_len {
            data_file.set_len(new_data_len)?;
        }

        // Drop the read-only mmap so we can open a writable one.
        self.data_mmap = None;

        let mut write_mmap = unsafe { memmap2::MmapMut::map_mut(&data_file)? };

        // Apply in-place rewrites.
        for ip in &in_place {
            let start = ip.offset as usize;
            let end = start + ip.new_length as usize;
            if end <= write_mmap.len() {
                write_mmap[start..end].copy_from_slice(&ip.bytes);
            }
        }
        // Write grow-tail entries.
        for g in &grow_layouts {
            let start = g.offset as usize;
            let end = start + g.length as usize;
            if end <= write_mmap.len() {
                write_mmap[start..end].copy_from_slice(&g.bytes);
            }
        }
        // Flush the dirty mmap pages via the file handle rather than
        // `MmapMut::flush`: on Windows the latter calls FlushViewOfFile
        // which msyncs the entire mapping page-by-page (O(size),
        // catastrophic for multi-GB files). Flushing the underlying File
        // handle goes through FlushFileBuffers, which is a single kernel
        // call and much faster. Required for persistence — on Windows,
        // dropping an MmapMut without any flush leaves written pages
        // unassociated with the on-disk file after the handle closes.
        drop(write_mmap);
        data_file.sync_data()?;
        drop(data_file);

        // Update the hash index. The existing index may be None if the data
        // file was previously empty.
        if self.index.is_none() {
            // Cold path — build a fresh index over the grow_layouts only
            // (there are no in-place entries on a cold compact).
            let entries: Vec<(u64, IndexEntry)> = grow_layouts
                .iter()
                .map(|g| {
                    (
                        g.key,
                        IndexEntry {
                            offset: g.offset,
                            length: g.length,
                            allocated: g.allocated,
                        },
                    )
                })
                .collect();
            let index_path = self.path.join("index.bin");
            let idx = HashIndex::build_bulk(&index_path, &entries)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("build_bulk: {e}")))?;
            self.index = Some(idx);
        } else {
            // Count how many grow entries introduce new keys (put) vs update
            // existing keys (update_existing_concurrent). If the projected
            // total live count after the put loop would exceed the HashIndex
            // 75% load factor, rebuild the index at a larger capacity up
            // front so every put below fits.
            {
                let idx = self.index.as_ref().unwrap();
                let mut new_key_count: u64 = 0;
                for g in &grow_layouts {
                    if idx.get(g.key).is_none() {
                        new_key_count += 1;
                    }
                }
                let projected = idx.count() + new_key_count;
                let load_limit = idx.capacity() * 3 / 4;
                if projected >= load_limit {
                    // Grow: snapshot every live entry, drop the current
                    // index, re-build with headroom for future growth.
                    let mut all_entries: Vec<(u64, IndexEntry)> =
                        idx.iter().collect();
                    // Overlay in-place length updates onto the snapshot so
                    // the rebuild sees post-compact offsets/lengths.
                    use std::collections::HashMap;
                    let in_place_map: HashMap<u64, &InPlace> =
                        in_place.iter().map(|ip| (ip.key, ip)).collect();
                    for (key, entry) in all_entries.iter_mut() {
                        if let Some(ip) = in_place_map.get(key) {
                            entry.length = ip.new_length;
                            entry.allocated = ip.allocated;
                        }
                    }
                    // Append the brand-new grow entries (new keys only;
                    // updates to existing keys get overlaid in the next
                    // pass after rebuild).
                    let grow_set: std::collections::HashSet<u64> =
                        grow_layouts.iter().map(|g| g.key).collect();
                    // Partition grow into new-key and existing-key updates.
                    for g in &grow_layouts {
                        if idx.get(g.key).is_none() {
                            all_entries.push((
                                g.key,
                                IndexEntry {
                                    offset: g.offset,
                                    length: g.length,
                                    allocated: g.allocated,
                                },
                            ));
                        }
                    }
                    // For existing-key grow entries, apply after rebuild
                    // via update_existing_concurrent — they stay in the
                    // snapshot at their PRE-rebuild offsets otherwise.
                    // Reflect the new offsets in `all_entries` before rebuild
                    // so the fresh index already has them correct.
                    for (key, entry) in all_entries.iter_mut() {
                        if grow_set.contains(key) {
                            if let Some(g) =
                                grow_layouts.iter().find(|g| g.key == *key)
                            {
                                entry.offset = g.offset;
                                entry.length = g.length;
                                entry.allocated = g.allocated;
                            }
                        }
                    }
                    // Size new index for `all_entries.len() * 2`.
                    let target_capacity = (all_entries.len() as u64) * 2;
                    eprintln!(
                        "DataSilo: growing HashIndex from capacity {} to {} ({} entries)",
                        idx.capacity(),
                        target_capacity,
                        all_entries.len()
                    );
                    // Drop the old index mmap before bulk_build recreates the file.
                    self.index = None;
                    let index_path = self.path.join("index.bin");
                    let new_idx = HashIndex::build_bulk_with_capacity(
                        &index_path,
                        &all_entries,
                        target_capacity,
                    )
                    .map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::Other,
                            format!("HashIndex::build_bulk_with_capacity: {e}"),
                        )
                    })?;
                    self.index = Some(new_idx);
                    // The in_place and grow entries have already been written
                    // into the new index via all_entries; skip the post-rebuild
                    // update loops. Account dead bytes.
                    for ip in &in_place {
                        if ip.new_length < ip.old_length {
                            self.dead_bytes.fetch_add(
                                (ip.old_length - ip.new_length) as u64,
                                Ordering::Relaxed,
                            );
                        }
                    }
                    for g in &grow_layouts {
                        if let Some(old_alloc) = g.old_allocated {
                            self.dead_bytes
                                .fetch_add(old_alloc as u64, Ordering::Relaxed);
                        }
                    }
                    // Skip the put-loop path below.
                    self.data_len = new_data_len;
                    self.load_data()?;
                    if frozen_is_b {
                        self.ops_b.lock().truncate()?;
                    } else {
                        self.ops_a.lock().truncate()?;
                    }
                    return Ok(total_ops);
                }
            }
            // No grow needed — normal in-place + put path.
            let idx = self.index.as_mut().unwrap();
            for ip in &in_place {
                unsafe {
                    idx.update_existing_concurrent(
                        ip.key,
                        IndexEntry {
                            offset: ip.offset,
                            length: ip.new_length,
                            allocated: ip.allocated,
                        },
                    );
                }
                // Slack inside the same region isn't dead space; only the delta
                // between new and old length on shrinks counts.
                if ip.new_length < ip.old_length {
                    self.dead_bytes
                        .fetch_add((ip.old_length - ip.new_length) as u64, Ordering::Relaxed);
                }
            }
            for g in &grow_layouts {
                let new_entry = IndexEntry {
                    offset: g.offset,
                    length: g.length,
                    allocated: g.allocated,
                };
                // Insert or update.
                match idx.get(g.key) {
                    Some(_) => unsafe {
                        idx.update_existing_concurrent(g.key, new_entry);
                    },
                    None => {
                        idx.put(g.key, new_entry).map_err(|e| {
                            io::Error::new(io::ErrorKind::Other, format!("put: {e}"))
                        })?;
                    }
                }
                // The old region is now dead.
                if let Some(old_alloc) = g.old_allocated {
                    self.dead_bytes
                        .fetch_add(old_alloc as u64, Ordering::Relaxed);
                }
            }
        }

        self.data_len = new_data_len;
        // Re-open the read mmap so subsequent gets see the new data.
        self.load_data()?;

        // Step 4: truncate the frozen log.
        if frozen_is_b {
            self.ops_b.lock().truncate()?;
        } else {
            self.ops_a.lock().truncate()?;
        }

        Ok(total_ops)
    }

    // ── Metadata / introspection ───────────────────────────────────────

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn config(&self) -> &SiloConfig {
        &self.config
    }

    pub fn index_count(&self) -> u64 {
        self.index.as_ref().map(|i| i.count()).unwrap_or(0)
    }

    /// Create a `DumpMergeWriter` for in-place read-modify-write during dump
    /// phases 2+. The writer holds a writable mmap on data.bin and a pointer
    /// to the existing HashIndex, enabling concurrent in-place updates without
    /// creating new entries.
    ///
    /// Returns `None` if no data file or index exists (phase 1 hasn't run).
    pub fn prepare_dump_merge(&self) -> io::Result<Option<DumpMergeWriter>> {
        let index = match self.index.as_ref() {
            Some(idx) if idx.count() > 0 => idx,
            _ => return Ok(None),
        };
        if self.data_mmap.is_none() {
            return Ok(None);
        }

        let data_path = self.path.join("data.bin");
        let data_file = OpenOptions::new()
            .read(true).write(true).open(&data_path)?;
        let mut write_mmap = unsafe { memmap2::MmapMut::map_mut(&data_file)? };
        let data_len = write_mmap.len();

        Ok(Some(DumpMergeWriter {
            write_ptr: write_mmap.as_mut_ptr(),
            write_mmap,
            data_len,
            index_ptr: index as *const HashIndex,
            stripes: (0..MERGE_STRIPE_COUNT)
                .map(|_| parking_lot::Mutex::new(()))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            in_place_count: AtomicU64::new(0),
            overflow_count: AtomicU64::new(0),
        }))
    }

    /// Reload the data mmap after DumpMergeWriter writes complete.
    /// Call after dropping the writer so queries see the updated data.
    pub fn reload_data(&mut self) -> io::Result<()> {
        self.data_mmap = None;
        self.load_data()
    }

    /// Flush the data file to disk. `compact()` intentionally skips the
    /// per-call flush because Windows msync is O(file size) and ruins
    /// streaming populate perf. Callers running a batch of compacts
    /// should call `sync()` once when the batch is complete.
    pub fn sync(&self) -> io::Result<()> {
        // Reopen a writable mmap on data.bin and flush it. We go through a
        // fresh mmap because the read mmap (self.data_mmap) is read-only
        // and can't be flushed.
        let data_path = self.path.join("data.bin");
        if !data_path.exists() {
            return Ok(());
        }
        let file = OpenOptions::new().read(true).write(true).open(&data_path)?;
        let len = file.metadata()?.len();
        if len == 0 {
            return Ok(());
        }
        let mmap = unsafe { memmap2::MmapMut::map_mut(&file)? };
        mmap.flush()?;
        drop(mmap);
        // Also flush the index file so crash-recovery finds a consistent pair.
        let index_path = self.path.join("index.bin");
        if index_path.exists() {
            let idx_file = OpenOptions::new().read(true).write(true).open(&index_path)?;
            let idx_mmap = unsafe { memmap2::MmapMut::map_mut(&idx_file)? };
            idx_mmap.flush()?;
        }
        Ok(())
    }
}
