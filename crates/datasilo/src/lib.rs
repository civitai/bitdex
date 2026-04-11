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

        let count = entries.len() as u64;
        let align = self.config.alignment.max(1) as u64;
        let buffer_ratio = self.config.buffer_ratio;
        let min_entry_size = self.config.min_entry_size;

        self.index = None;
        self.data_mmap = None;

        // Phase 1: encode each snapshot once; lay out offsets sequentially.
        let mut encoded: Vec<(u64, Vec<u8>)> = Vec::with_capacity(entries.len());
        for (k, snap) in entries {
            let mut buf = Vec::with_capacity(256);
            S::encode(snap, &mut buf);
            encoded.push((*k, buf));
        }
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
        write_mmap.flush()?;
        drop(write_mmap);

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
}
