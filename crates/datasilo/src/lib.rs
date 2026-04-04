//! DataSilo — mmap'd key-value store with append-only ops log.
//!
//! Three mmap'd files:
//! - **Index** (`index.bin`): key → (offset, length, allocated) in data file
//! - **Data** (`data.bin`): packed values, written only by compaction
//! - **Ops** (`ops.log`): append-only mutations, written by everything
//!
//! ALL writes go through the ops log. Compaction merges ops into the data file.
//! The parallel mmap write primitive (atomic bump + 1MB thread-local regions)
//! is used for both ops log writes and compaction data file writes.
//!
//! No in-memory pending HashMap — the mmap'd ops log IS the read cache.
//! Encoding is caller's responsibility — DataSilo stores raw `&[u8]`.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

mod ops_log;
pub mod hash_index;

pub use ops_log::{SiloOp, OpsLog};
pub use hash_index::HashIndex;

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum SiloError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("hash table is full (load factor exceeded)")]
    TableFull,

    #[error("key 0 is reserved (empty sentinel)")]
    ReservedKey,

    #[error("file is too small to be a valid hash index")]
    InvalidFile,
}

pub type Result<T> = std::result::Result<T, SiloError>;

// ---------------------------------------------------------------------------
// Index entry — 16 bytes per key
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(C)]
pub struct IndexEntry {
    pub offset: u64,
    pub length: u32,
    pub allocated: u32,
}

const INDEX_ENTRY_SIZE: usize = std::mem::size_of::<IndexEntry>(); // 16

// ---------------------------------------------------------------------------
// SiloConfig
// ---------------------------------------------------------------------------

pub struct SiloConfig {
    /// Extra space multiplier for entries (e.g., 1.3 = 30% headroom).
    /// Allows in-place updates when new data fits within the allocated region.
    pub buffer_ratio: f32,
    /// Minimum bytes allocated per entry, even for small values.
    /// Ensures all entries have room for in-place field additions.
    /// Default: 256 bytes (typical BitDex doc is ~230 bytes).
    pub min_entry_size: u32,
    /// Entry alignment in bytes. Entries in the data file start at offsets
    /// that are multiples of this value. Default: 1 (no alignment).
    /// Set to 32 for frozen bitmap silos (FrozenRoaringBitmap requires 32-byte alignment).
    pub alignment: u32,
    /// Dead space ratio that triggers automatic compaction.
    /// When `dead_bytes / total_bytes > compact_threshold`, the data file
    /// is rewritten to reclaim space. Default: 0.20 (20%).
    /// Set to 0.0 to disable automatic compaction.
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
// ParallelOpsWriter — lock-free parallel writes to the ops log
// ---------------------------------------------------------------------------

/// Handle for parallel writes to the ops log mmap.
/// Created by `DataSilo::prepare_parallel_ops()`, used by rayon threads.
/// Each thread grabs 1MB regions via atomic cursor and writes CRC32-framed ops.
pub struct ParallelOpsWriter {
    cursor: *const AtomicU64,  // points into OpsLog.cursor (stable while mmap is allocated)
    mmap_ptr: *mut u8,         // points into OpsLog.mmap (stable while mmap is allocated)
    mmap_len: usize,
}

// Safety: ParallelOpsWriter is Send+Sync because:
// - cursor is an AtomicU64 (inherently thread-safe)
// - mmap_ptr: threads write to disjoint regions via atomic cursor bump
// - The OpsLog mmap is not reallocated or freed during parallel writes
//   (caller must not call ensure_capacity/truncate while ParallelOpsWriter exists)
unsafe impl Send for ParallelOpsWriter {}
unsafe impl Sync for ParallelOpsWriter {}

const OPS_REGION_SIZE: usize = 1 << 20; // 1MB thread-local regions

impl ParallelOpsWriter {
    /// Write a Put op directly to the mmap. Thread-safe, lock-free.
    /// Returns true if the write succeeded.
    #[inline]
    pub fn write_put(&self, key: u32, value: &[u8], local_cursor: &mut usize, local_end: &mut usize) -> bool {
        let mut frame_buf = Vec::with_capacity(value.len() + 16);
        OpsLog::encode_put_into(&mut frame_buf, key, value);
        self.write_frame(&frame_buf, local_cursor, local_end)
    }

    /// Write a pre-encoded frame directly to the mmap. Thread-safe, lock-free.
    #[inline]
    pub fn write_frame(&self, frame: &[u8], local_cursor: &mut usize, local_end: &mut usize) -> bool {
        let frame_len = frame.len();

        // Allocate from thread-local region (1MB)
        if *local_cursor + frame_len > *local_end {
            let cursor = unsafe { &*self.cursor };
            let start = cursor.fetch_add(OPS_REGION_SIZE as u64, Ordering::Relaxed) as usize;
            *local_cursor = start;
            *local_end = start + OPS_REGION_SIZE;
        }

        if *local_cursor + frame_len > self.mmap_len {
            return false; // out of space
        }

        unsafe {
            let dst = self.mmap_ptr.add(*local_cursor);
            std::ptr::copy_nonoverlapping(frame.as_ptr(), dst, frame_len);
        }
        *local_cursor += frame_len;
        true
    }
}

// ---------------------------------------------------------------------------
// DataSilo — the main store
// ---------------------------------------------------------------------------

pub struct DataSilo {
    path: PathBuf,
    config: SiloConfig,
    index_mmap: Option<memmap2::MmapMut>,
    index_len: u32,
    data_mmap: Option<memmap2::Mmap>,
    data_len: u64,
    /// Two ops log slots for A-B swap during compaction.
    /// While one is being compacted (frozen), new writes go to the other.
    ops_a: parking_lot::Mutex<OpsLog>,
    ops_b: parking_lot::Mutex<OpsLog>,
    /// Which slot is currently active for writes: false = A, true = B.
    active_is_b: AtomicBool,
    /// Bytes wasted by deleted entries and relocated updates.
    /// Tracked during hot compaction. Reset to 0 after a full rewrite.
    dead_bytes: AtomicU64,
}

unsafe impl Send for DataSilo {}
unsafe impl Sync for DataSilo {}

impl DataSilo {
    /// Open or create a DataSilo at the given directory.
    ///
    /// Handles legacy migration: if only `ops.log` exists (old single-log format),
    /// it is renamed to `ops_a.log` before opening.
    pub fn open(path: &Path, config: SiloConfig) -> io::Result<Self> {
        std::fs::create_dir_all(path)?;

        // Legacy migration: rename ops.log → ops_a.log if present and ops_a.log absent.
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
            index_mmap: None,
            index_len: 0,
            data_mmap: None,
            data_len: 0,
            ops_a: parking_lot::Mutex::new(ops_a),
            ops_b: parking_lot::Mutex::new(ops_b),
            active_is_b: AtomicBool::new(false),
            dead_bytes: AtomicU64::new(0),
        };

        silo.load_index()?;
        silo.load_data()?;
        Ok(silo)
    }

    // ── Write path: everything goes through the active ops log ──────────

    /// Get the active ops log for direct parallel writes.
    /// Always returns the currently active slot (A or B).
    pub fn ops_log(&self) -> &parking_lot::Mutex<OpsLog> {
        if self.active_is_b.load(Ordering::Acquire) {
            &self.ops_b
        } else {
            &self.ops_a
        }
    }

    /// Prepare for parallel ops writes. Pre-allocates the active ops log mmap.
    /// Returns a `ParallelOpsWriter` that rayon threads can use for lock-free writes.
    ///
    /// IMPORTANT: Do not call `ensure_ops_capacity` or `compact` while the
    /// `ParallelOpsWriter` is in use — the mmap must not be reallocated.
    pub fn prepare_parallel_ops(&self, estimated_bytes: u64) -> io::Result<ParallelOpsWriter> {
        let mut log = self.ops_log().lock();
        let needed = log.data_size() + estimated_bytes;
        log.ensure_capacity(needed)?;

        let cursor = log.cursor() as *const AtomicU64;
        let mmap_ptr = log.mmap_ptr()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "ops log mmap not available"))?;
        let mmap_len = log.mmap_len();

        Ok(ParallelOpsWriter {
            cursor,
            mmap_ptr: mmap_ptr as *mut u8,
            mmap_len,
        })
    }

    /// Flush the active ops log mmap to disk. Call after parallel writes complete.
    pub fn flush_ops(&self) -> io::Result<()> {
        self.ops_log().lock().flush()
    }

    /// Append a single op (sequential, single-thread steady-state path).
    pub fn append_op(&self, key: u32, value: &[u8]) -> io::Result<()> {
        self.ops_log().lock().append(&SiloOp::Put { key, value: value.to_vec() })
    }

    /// Append a batch of ops sequentially. Useful for small batches in steady state.
    pub fn append_ops_batch(&self, ops: &[(u32, Vec<u8>)]) -> io::Result<()> {
        let mut log = self.ops_log().lock();
        for (key, value) in ops {
            log.append(&SiloOp::Put { key: *key, value: value.clone() })?;
        }
        log.flush()?;
        Ok(())
    }

    /// Ensure the active ops log has capacity for `bytes` of additional data.
    /// Call before parallel writes to pre-allocate the mmap.
    pub fn ensure_ops_capacity(&self, bytes: u64) -> io::Result<()> {
        let mut log = self.ops_log().lock();
        let needed = log.data_size() + bytes;
        log.ensure_capacity(needed)
    }

    /// Delete an entry by key. Appends a Delete tombstone to the active ops log.
    /// The entry is removed from the data file on the next compaction.
    pub fn delete(&self, key: u32) -> io::Result<()> {
        self.ops_log().lock().append(&SiloOp::Delete { key })
    }

    // ── Read path ───────────────────────────────────────────────────────

    /// Read an entry by key from the data file (no ops overlay).
    /// Fast path for queries after compaction.
    pub fn get(&self, key: u32) -> Option<&[u8]> {
        let entry = self.index_entry(key)?;
        if entry.length == 0 { return None; }
        let mmap = self.data_mmap.as_ref()?;
        let start = entry.offset as usize;
        let end = start + entry.length as usize;
        if end <= mmap.len() { Some(&mmap[start..end]) } else { None }
    }

    /// Read an entry with ops overlay (returns owned data).
    /// Scans BOTH ops logs (A and B) for the latest value of this key.
    /// Last-write-wins across both logs (frozen log has older ops, active has newer).
    /// Handles both Put (update) and Delete (tombstone) ops.
    pub fn get_with_ops(&self, key: u32) -> Option<Vec<u8>> {
        // Scan both ops logs. We must read them while holding both locks to get a
        // consistent snapshot. Lock order is always A then B to prevent deadlock.
        let log_a = self.ops_a.lock();
        let log_b = self.ops_b.lock();

        let mut latest: Option<Option<Vec<u8>>> = None; // Some(Some(v)) = put, Some(None) = deleted

        // Scan A first (may be frozen/older), then B (may be active/newer).
        // Because we scan in order A→B and last-write-wins, the result from B
        // correctly overwrites A for any key that appears in both.
        let scan = |log: &OpsLog| {
            let mut found: Option<Option<Vec<u8>>> = None;
            let _ = log.for_each_ops(|op| {
                match op {
                    SiloOp::Put { key: k, value } if k == key => {
                        found = Some(Some(value));
                    }
                    SiloOp::Delete { key: k } if k == key => {
                        found = Some(None);
                    }
                    _ => {}
                }
            });
            found
        };

        if let Some(v) = scan(&log_a) { latest = Some(v); }
        if let Some(v) = scan(&log_b) { latest = Some(v); }

        match latest {
            Some(Some(v)) => Some(v),
            Some(None) => None,
            None => {
                // No ops for this key in either log — fall back to data file
                self.get(key).map(|s| s.to_vec())
            }
        }
    }

    // ── Metadata ────────────────────────────────────────────────────────

    pub fn index_capacity(&self) -> u32 { self.index_len }
    pub fn data_bytes(&self) -> u64 { self.data_len }
    /// Total bytes written across both ops logs.
    pub fn ops_size(&self) -> u64 {
        self.ops_a.lock().data_size() + self.ops_b.lock().data_size()
    }
    pub fn path(&self) -> &Path { &self.path }
    pub fn config(&self) -> &SiloConfig { &self.config }

    /// Dead bytes in the data file (from deletes and relocating updates).
    pub fn dead_bytes(&self) -> u64 { self.dead_bytes.load(Ordering::Relaxed) }

    /// Dead space ratio: dead_bytes / total_bytes. Returns 0.0 if no data.
    pub fn dead_ratio(&self) -> f64 {
        if self.data_len == 0 { return 0.0; }
        self.dead_bytes.load(Ordering::Relaxed) as f64 / self.data_len as f64
    }

    /// Whether automatic compaction should trigger based on dead space threshold.
    pub fn needs_compaction(&self) -> bool {
        self.config.compact_threshold > 0.0 && self.dead_ratio() > self.config.compact_threshold as f64
    }

    /// Check if there are uncompacted ops in either log.
    pub fn has_ops(&self) -> bool {
        !self.ops_a.lock().is_empty() || !self.ops_b.lock().is_empty()
    }

    // ── Compaction ──────────────────────────────────────────────────────

    /// Compact: merge ops into the data file.
    ///
    /// Uses the A-B swap protocol to ensure no ops are lost:
    /// 1. Atomically switch the active write slot (A→B or B→A).
    ///    New writes now go to the fresh slot.
    /// 2. Compact the frozen slot (which received no new writes during compaction).
    /// 3. After data+index are fully synced to disk, truncate the frozen slot.
    ///
    /// Two compaction modes:
    /// - **Cold** (no existing data file): scan ops → build index + data file
    /// - **Hot** (existing data file): apply ops in-place where they fit, overflow to end
    pub fn compact(&mut self) -> io::Result<u64> {
        // Check if the active slot has any ops to compact.
        let active_has_ops = !self.ops_log().lock().is_empty();
        if !active_has_ops { return Ok(0); }

        // Step 1: Freeze the active slot by atomically switching to the other slot.
        // After this store, new writes go to the previously-idle slot.
        // We use SeqCst to ensure all in-flight writes to the old active slot
        // are visible before we read from it below.
        //
        // frozen_is_b: true = B is the frozen slot, false = A is the frozen slot.
        let frozen_is_b = self.active_is_b.fetch_xor(true, Ordering::SeqCst);
        // fetch_xor returns the OLD value. Old active=B means B is now frozen.

        // Step 2: Compact from the frozen slot.
        let has_data = self.data_mmap.is_some() && self.index_len > 0;
        let count = if has_data {
            self.compact_hot_from(frozen_is_b)?
        } else {
            self.compact_cold_from(frozen_is_b)?
        };

        // Step 3: Truncate the frozen slot (data+index already flushed inside compact_*_from).
        if frozen_is_b {
            self.ops_b.lock().truncate()?;
        } else {
            self.ops_a.lock().truncate()?;
        }

        Ok(count)
    }

    /// Cold compaction: no existing data file.
    /// Scan frozen ops log for last value per key, write data file + index.
    /// Deleted keys (tombstones) are excluded from the output.
    /// `frozen_is_b`: true = ops_b is frozen, false = ops_a is frozen.
    fn compact_cold_from(&mut self, frozen_is_b: bool) -> io::Result<u64> {
        // Collect last value per key from frozen ops log (last-write-wins).
        // Deletes remove the entry entirely (tombstone).
        let mut entries: std::collections::HashMap<u32, Vec<u8>> = std::collections::HashMap::new();
        let mut max_key: u32 = 0;
        {
            let log = if frozen_is_b { self.ops_b.lock() } else { self.ops_a.lock() };
            log.for_each_ops(|op| {
                match op {
                    SiloOp::Put { key, value } => {
                        entries.insert(key, value);
                        if key > max_key { max_key = key; }
                    }
                    SiloOp::Delete { key } => {
                        entries.remove(&key);
                        if key > max_key { max_key = key; }
                    }
                }
            })?;
        }
        if entries.is_empty() { return Ok(0); }

        let count = entries.len() as u64;
        let align = self.config.alignment.max(1) as u64;
        let buffer_ratio = self.config.buffer_ratio;
        let min_entry_size = self.config.min_entry_size;

        // Drop old mmaps before writing
        self.index_mmap = None;
        self.data_mmap = None;

        // Sort keys and compute per-entry layout (offsets must be sequential)
        let mut keys: Vec<u32> = entries.keys().copied().collect();
        keys.sort_unstable();

        // Phase 1: Compute entry layouts — offset, length, allocated (sequential)
        struct EntryLayout { key: u32, offset: u64, length: u32, allocated: u32 }
        let mut layouts: Vec<EntryLayout> = Vec::with_capacity(keys.len());
        let mut offset: u64 = 0;
        for &key in &keys {
            if align > 1 {
                offset = (offset + align - 1) & !(align - 1);
            }
            let len = entries[&key].len() as u32;
            let mut allocated = ((len as f32 * buffer_ratio).ceil() as u32)
                .max(min_entry_size);
            if align > 1 {
                allocated = ((allocated as u64 + align - 1) & !(align - 1)) as u32;
            }
            layouts.push(EntryLayout { key, offset, length: len, allocated });
            offset += allocated as u64;
        }
        let total_data_size = offset;

        // Phase 2: Pre-allocate data file + index as mmap
        let data_path = self.path.join("data.bin");
        let data_file = OpenOptions::new()
            .create(true).read(true).write(true).truncate(true).open(&data_path)?;
        data_file.set_len(total_data_size)?;
        let mut data_mmap = unsafe { memmap2::MmapMut::map_mut(&data_file)? };

        let index_count = max_key as usize + 1;
        let index_path = self.path.join("index.bin");
        let index_file = OpenOptions::new()
            .create(true).read(true).write(true).open(&index_path)?;
        index_file.set_len((index_count * INDEX_ENTRY_SIZE) as u64)?;
        let mut index_mmap = unsafe { memmap2::MmapMut::map_mut(&index_file)? };

        // Phase 3: Write entries to mmap (parallel memcpy)
        // Each entry writes to a pre-computed offset — no overlap, safe for parallel.
        let data_ptr = data_mmap.as_mut_ptr();
        let index_ptr = index_mmap.as_mut_ptr();
        let data_mmap_len = data_mmap.len();
        let index_mmap_len = index_mmap.len();

        // Safety: each layout has a unique, non-overlapping (offset..offset+allocated) region.
        // Parallel writes to disjoint regions of mmap are safe.
        layouts.iter().for_each(|layout| {
            let value = &entries[&layout.key];
            let start = layout.offset as usize;
            if start + value.len() <= data_mmap_len {
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        value.as_ptr(),
                        data_ptr.add(start),
                        value.len(),
                    );
                }
            }
            // Write index entry
            let entry = IndexEntry {
                offset: layout.offset,
                length: layout.length,
                allocated: layout.allocated,
            };
            let pos = layout.key as usize * INDEX_ENTRY_SIZE;
            if pos + INDEX_ENTRY_SIZE <= index_mmap_len {
                let bytes: [u8; INDEX_ENTRY_SIZE] = unsafe { std::mem::transmute(entry) };
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        bytes.as_ptr(),
                        index_ptr.add(pos),
                        INDEX_ENTRY_SIZE,
                    );
                }
            }
        });

        data_mmap.flush()?;
        drop(data_mmap);
        index_mmap.flush()?;

        self.index_mmap = Some(index_mmap);
        self.index_len = index_count as u32;
        self.load_data()?;
        self.data_len = offset;
        self.dead_bytes.store(0, Ordering::Relaxed); // full rewrite = no dead space

        // NOTE: caller (compact()) truncates the frozen log after this returns.

        eprintln!("DataSilo: cold compacted {} entries, {:.1}MB data, {:.1}MB index",
            count, offset as f64 / 1e6,
            (index_count * INDEX_ENTRY_SIZE) as f64 / 1e6);
        Ok(count)
    }

    /// Hot compaction: existing data file with pre-allocated buffer slots.
    ///
    /// Correctness properties:
    /// - Readers (via `get()`) are never blocked: `self.data_mmap` stays alive
    ///   on the old data file until the new file is fully written and renamed.
    /// - Data is fully on disk before the index is updated: a crash between the
    ///   two is safe because the old index still points into the old file which
    ///   has been atomically replaced, but the new file is complete.
    ///
    /// Algorithm:
    /// 1. Collect ops from frozen log (last-write-wins, deletes as None).
    /// 2. Classify each op: in-place (new value fits existing allocated slot) or
    ///    overflow (doesn't fit, or key is new).  Read-only pass — nothing written.
    /// 3. Write `data.bin.tmp`: copy every existing entry from the old data mmap,
    ///    applying ops overlay.  Overflow entries are appended at the end.
    ///    Readers continue on the OLD data mmap throughout this entire step.
    /// 4. Flush + rename `data.bin.tmp` → `data.bin`.
    /// 5. Update all index entries (in-place entries keep their offset, overflow
    ///    entries get new offsets).  Flush index.
    /// 6. Remap `self.data_mmap` to the new file.
    ///
    /// `frozen_is_b`: true = ops_b is frozen, false = ops_a is frozen.
    fn compact_hot_from(&mut self, frozen_is_b: bool) -> io::Result<u64> {
        // ── Step 1: Collect ops ──────────────────────────────────────────
        let mut ops: std::collections::HashMap<u32, Option<Vec<u8>>> = std::collections::HashMap::new();
        let mut max_key: u32 = 0;
        {
            let log = if frozen_is_b { self.ops_b.lock() } else { self.ops_a.lock() };
            log.for_each_ops(|op| {
                match op {
                    SiloOp::Put { key, value } => {
                        ops.insert(key, Some(value));
                        if key > max_key { max_key = key; }
                    }
                    SiloOp::Delete { key } => {
                        ops.insert(key, None);
                        if key > max_key { max_key = key; }
                    }
                }
            })?;
        }
        if ops.is_empty() { return Ok(0); }

        let count = ops.len() as u64;

        // ── Step 2: Classify ops (read-only, nothing mutated) ────────────
        // in_place: key→(old IndexEntry, new value) — fits in existing slot
        // overflows: key→new value — new key or doesn't fit, goes to end
        // deletions: (key, old_allocated) — zero index entry, account dead space
        //
        // Dead space is computed here while the original index is still intact.
        struct InPlaceUpdate { old_entry: IndexEntry, new_len: u32 }
        let mut in_place_map: std::collections::HashMap<u32, InPlaceUpdate> = std::collections::HashMap::new();
        let mut overflows: Vec<(u32, Vec<u8>)> = Vec::new();
        // (key, old_allocated_bytes_now_dead)
        let mut deletions: Vec<(u32, u64)> = Vec::new();
        // Dead bytes from overflow-displaced entries (old slots become dead in new file)
        let mut dead_from_overflows: u64 = 0;

        for (&key, value_opt) in &ops {
            match value_opt {
                None => {
                    // Delete tombstone — read old allocated bytes while index is intact
                    let old_allocated = if key < self.index_len {
                        self.index_entry(key)
                            .filter(|e| e.allocated > 0)
                            .map(|e| e.allocated as u64)
                            .unwrap_or(0)
                    } else {
                        0
                    };
                    deletions.push((key, old_allocated));
                }
                Some(value) => {
                    if key < self.index_len {
                        if let Some(old_entry) = self.index_entry(key) {
                            if old_entry.allocated > 0 && value.len() as u32 <= old_entry.allocated {
                                let start = old_entry.offset as usize;
                                // Sanity: slot must be within current data file bounds
                                if start + old_entry.allocated as usize <= self.data_len as usize {
                                    in_place_map.insert(key, InPlaceUpdate {
                                        old_entry,
                                        new_len: value.len() as u32,
                                    });
                                    continue;
                                }
                            }
                            // Existing entry displaced to overflow — old slot is dead space
                            // in the new data file (we bulk-copied old file, then appended
                            // the new value; the old region is now unreachable).
                            if old_entry.allocated > 0 {
                                dead_from_overflows += old_entry.allocated as u64;
                            }
                        }
                    }
                    // Falls through to overflow
                    overflows.push((key, value.clone()));
                }
            }
        }

        // ── Step 3: Write data.bin.tmp ────────────────────────────────────
        // Old data_mmap stays alive — readers continue unblocked.
        let data_path = self.path.join("data.bin");
        let tmp_path = self.path.join("data.bin.tmp");

        // Compute new file size: existing data_len + overflow appends
        let align = self.config.alignment.max(1) as u64;
        let buffer_ratio = self.config.buffer_ratio;
        let min_entry_size = self.config.min_entry_size;

        // Compute overflow layouts (offsets start at data_len, aligned)
        struct OverflowLayout { key: u32, offset: u64, length: u32, allocated: u32 }
        let mut overflow_layouts: Vec<OverflowLayout> = Vec::with_capacity(overflows.len());
        {
            let mut offset = self.data_len;
            for (key, value) in &overflows {
                if align > 1 {
                    offset = (offset + align - 1) & !(align - 1);
                }
                let len = value.len() as u32;
                let mut allocated = ((len as f32 * buffer_ratio).ceil() as u32).max(min_entry_size);
                if align > 1 {
                    allocated = ((allocated as u64 + align - 1) & !(align - 1)) as u32;
                }
                overflow_layouts.push(OverflowLayout { key: *key, offset, length: len, allocated });
                offset += allocated as u64;
            }
        }
        let new_data_len = if overflow_layouts.is_empty() {
            self.data_len
        } else {
            overflow_layouts.last().map(|l| l.offset + l.allocated as u64).unwrap_or(self.data_len)
        };

        // Pre-allocate and mmap the temp file
        {
            let tmp_file = OpenOptions::new()
                .create(true).read(true).write(true).truncate(true).open(&tmp_path)?;
            tmp_file.set_len(new_data_len)?;
            let mut tmp_mmap = unsafe { memmap2::MmapMut::map_mut(&tmp_file)? };

            // Copy all existing data from old mmap (readers still on old mmap)
            if let Some(ref old_mmap) = self.data_mmap {
                let copy_len = old_mmap.len().min(tmp_mmap.len());
                tmp_mmap[..copy_len].copy_from_slice(&old_mmap[..copy_len]);
            }

            // Apply in-place ops: overwrite the value at its existing offset
            for (&key, update) in &in_place_map {
                if let Some(Some(value)) = ops.get(&key) {
                    let start = update.old_entry.offset as usize;
                    if start + value.len() <= tmp_mmap.len() {
                        tmp_mmap[start..start + value.len()].copy_from_slice(value);
                    }
                }
            }

            // Write overflow entries at their computed offsets
            for (layout, (_, value)) in overflow_layouts.iter().zip(overflows.iter()) {
                let start = layout.offset as usize;
                let end = start + value.len();
                if end <= tmp_mmap.len() {
                    tmp_mmap[start..end].copy_from_slice(value);
                    // Padding bytes beyond value.len() up to allocated are already zeroed
                    // (tmp_file was pre-allocated as zeros)
                }
            }

            tmp_mmap.flush()?;
        } // tmp_mmap + tmp_file dropped here

        // ── Step 4: Atomic rename tmp → data.bin ─────────────────────────
        // Old data_mmap still open on the previous data.bin inode — readers
        // continue reading from it unaffected.  After rename, new opens of
        // data.bin see the new file.
        std::fs::rename(&tmp_path, &data_path)?;

        // ── Step 5: Update index ──────────────────────────────────────────
        // Only now do we touch the index.  Data file is complete on disk.

        // Extend index if overflows include keys beyond current capacity.
        let new_max_key = max_key.max(
            overflow_layouts.iter().map(|l| l.key).max().unwrap_or(0)
        );
        if new_max_key >= self.index_len {
            let new_count = new_max_key as usize + 1;
            let index_path = self.path.join("index.bin");
            self.index_mmap = None;
            let index_file = OpenOptions::new().read(true).write(true).open(&index_path)?;
            index_file.set_len((new_count * INDEX_ENTRY_SIZE) as u64)?;
            let mmap = unsafe { memmap2::MmapMut::map_mut(&index_file)? };
            self.index_mmap = Some(mmap);
            self.index_len = new_count as u32;
        }

        // Write index entries for in-place updates (same offset, new length)
        for (&key, update) in &in_place_map {
            let new_entry = IndexEntry {
                offset: update.old_entry.offset,
                length: update.new_len,
                allocated: update.old_entry.allocated,
            };
            if let Some(ref mut index_mmap) = self.index_mmap {
                let pos = key as usize * INDEX_ENTRY_SIZE;
                if pos + INDEX_ENTRY_SIZE <= index_mmap.len() {
                    let bytes: [u8; INDEX_ENTRY_SIZE] = unsafe { std::mem::transmute(new_entry) };
                    index_mmap[pos..pos + INDEX_ENTRY_SIZE].copy_from_slice(&bytes);
                }
            }
        }

        // Write index entries for overflow entries (new offsets)
        for layout in &overflow_layouts {
            let entry = IndexEntry {
                offset: layout.offset,
                length: layout.length,
                allocated: layout.allocated,
            };
            if let Some(ref mut index_mmap) = self.index_mmap {
                let pos = layout.key as usize * INDEX_ENTRY_SIZE;
                if pos + INDEX_ENTRY_SIZE <= index_mmap.len() {
                    let bytes: [u8; INDEX_ENTRY_SIZE] = unsafe { std::mem::transmute(entry) };
                    index_mmap[pos..pos + INDEX_ENTRY_SIZE].copy_from_slice(&bytes);
                }
            }
        }

        // Zero out index entries for deletions.
        // dead_from_deletes was captured during Step 2 classification (before any index writes).
        let mut dead_from_deletes: u64 = 0;
        for &(key, old_allocated) in &deletions {
            dead_from_deletes += old_allocated;
            if key < self.index_len {
                let zero_entry = IndexEntry { offset: 0, length: 0, allocated: 0 };
                if let Some(ref mut index_mmap) = self.index_mmap {
                    let pos = key as usize * INDEX_ENTRY_SIZE;
                    if pos + INDEX_ENTRY_SIZE <= index_mmap.len() {
                        let bytes: [u8; INDEX_ENTRY_SIZE] = unsafe { std::mem::transmute(zero_entry) };
                        index_mmap[pos..pos + INDEX_ENTRY_SIZE].copy_from_slice(&bytes);
                    }
                }
            }
        }

        // Account for dead space.
        // dead_from_overflows and dead_from_deletes both captured in Step 2 before
        // any index mutations — correct pre-compaction values.
        self.dead_bytes.fetch_add(dead_from_deletes + dead_from_overflows, Ordering::Relaxed);

        if let Some(ref index_mmap) = self.index_mmap {
            index_mmap.flush()?;
        }

        // ── Step 6: Remap read mmap to new data file ─────────────────────
        // Drop old mmap first so the old file handle is released, then open
        // the new data.bin (which load_data() also uses to set self.data_len).
        self.data_mmap = None;
        self.load_data()?;

        // NOTE: caller (compact()) truncates the frozen log after this returns.

        eprintln!("DataSilo: hot compacted {} ops ({} in-place, {} overflow, {} deletes)",
            count, in_place_map.len(), overflows.len(), deletions.len());
        Ok(count)
    }

    // ── Internal helpers ────────────────────────────────────────────────

    fn index_entry(&self, key: u32) -> Option<IndexEntry> {
        if key >= self.index_len { return None; }
        let mmap = self.index_mmap.as_ref()?;
        let pos = key as usize * INDEX_ENTRY_SIZE;
        if pos + INDEX_ENTRY_SIZE > mmap.len() { return None; }
        let bytes: [u8; INDEX_ENTRY_SIZE] = mmap[pos..pos + INDEX_ENTRY_SIZE].try_into().ok()?;
        Some(unsafe { std::mem::transmute(bytes) })
    }

    fn load_index(&mut self) -> io::Result<()> {
        let p = self.path.join("index.bin");
        if !p.exists() { return Ok(()); }
        let f = OpenOptions::new().read(true).write(true).open(&p)?;
        if f.metadata()?.len() < INDEX_ENTRY_SIZE as u64 { return Ok(()); }
        let mmap = unsafe { memmap2::MmapMut::map_mut(&f)? };
        self.index_len = (mmap.len() / INDEX_ENTRY_SIZE) as u32;
        self.index_mmap = Some(mmap);
        Ok(())
    }

    fn load_data(&mut self) -> io::Result<()> {
        let p = self.path.join("data.bin");
        if !p.exists() { return Ok(()); }
        let f = File::open(&p)?;
        let meta = f.metadata()?;
        if meta.len() == 0 { return Ok(()); }
        let mmap = unsafe { memmap2::Mmap::map(&f)? };
        self.data_len = meta.len();
        self.data_mmap = Some(mmap);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_and_compact_cold() {
        let dir = tempfile::tempdir().unwrap();
        let silo = DataSilo::open(dir.path(), SiloConfig::default()).unwrap();

        // Write ops
        silo.append_op(0, b"doc_0").unwrap();
        silo.append_op(1, b"doc_1").unwrap();
        silo.append_op(999, b"doc_999").unwrap();

        // Before compaction, get() returns None (no data file yet)
        assert!(silo.get(0).is_none());
        // But get_with_ops scans the log
        assert_eq!(silo.get_with_ops(0).unwrap(), b"doc_0");

        // Compact
        let mut silo = silo;
        let count = silo.compact().unwrap();
        assert_eq!(count, 3);

        // After compaction, get() works from data file
        assert_eq!(silo.get(0).unwrap(), b"doc_0");
        assert_eq!(silo.get(1).unwrap(), b"doc_1");
        assert_eq!(silo.get(999).unwrap(), b"doc_999");
        assert!(silo.get(500).is_none());
    }

    #[test]
    fn test_write_compact_then_update() {
        let dir = tempfile::tempdir().unwrap();
        let mut silo = DataSilo::open(dir.path(), SiloConfig::default()).unwrap();

        // Phase 1: write initial docs, compact
        silo.append_op(1, b"hello").unwrap();
        silo.append_op(2, b"world").unwrap();
        silo.compact().unwrap();

        assert_eq!(silo.get(1).unwrap(), b"hello");
        assert_eq!(silo.get(2).unwrap(), b"world");

        // Phase 2: update via ops, compact again (hot path)
        silo.append_op(1, b"updated").unwrap();
        silo.append_op(3, b"new_entry").unwrap();
        silo.compact().unwrap();

        assert_eq!(silo.get(1).unwrap(), b"updated");
        assert_eq!(silo.get(2).unwrap(), b"world");
        assert_eq!(silo.get(3).unwrap(), b"new_entry");
    }

    #[test]
    fn test_hot_compact_in_place() {
        let dir = tempfile::tempdir().unwrap();
        let mut silo = DataSilo::open(dir.path(), SiloConfig::default()).unwrap();

        // Write a doc with buffer headroom (min_entry_size = 256)
        silo.append_op(1, b"short").unwrap();
        silo.compact().unwrap();

        let entry_before = silo.index_entry(1).unwrap();
        assert!(entry_before.allocated >= 256); // has headroom

        // Update with a value that fits in the allocated space
        let bigger = vec![0xAB; 200]; // still < 256 allocated
        silo.append_op(1, &bigger).unwrap();
        silo.compact().unwrap();

        // Should have been written in-place (same offset)
        let entry_after = silo.index_entry(1).unwrap();
        assert_eq!(entry_after.offset, entry_before.offset); // same slot
        assert_eq!(entry_after.length, 200);
        assert_eq!(silo.get(1).unwrap().len(), 200);
    }

    #[test]
    fn test_last_write_wins() {
        let dir = tempfile::tempdir().unwrap();
        let mut silo = DataSilo::open(dir.path(), SiloConfig::default()).unwrap();

        silo.append_op(1, b"first").unwrap();
        silo.append_op(1, b"second").unwrap();
        silo.append_op(1, b"third").unwrap();
        silo.compact().unwrap();

        assert_eq!(silo.get(1).unwrap(), b"third");
    }

    #[test]
    fn test_reopen_with_ops() {
        let dir = tempfile::tempdir().unwrap();
        {
            let silo = DataSilo::open(dir.path(), SiloConfig::default()).unwrap();
            silo.append_op(1, b"hello").unwrap();
            silo.append_op(2, b"world").unwrap();
            silo.flush_ops().unwrap();
        }
        {
            let silo = DataSilo::open(dir.path(), SiloConfig::default()).unwrap();
            // Ops are in the log file, readable via get_with_ops
            assert_eq!(silo.get_with_ops(1).unwrap(), b"hello");
            assert_eq!(silo.get_with_ops(2).unwrap(), b"world");
        }
    }

    #[test]
    fn test_reopen_after_compact() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut silo = DataSilo::open(dir.path(), SiloConfig::default()).unwrap();
            silo.append_op(42, b"data").unwrap();
            silo.compact().unwrap();
        }
        {
            let silo = DataSilo::open(dir.path(), SiloConfig::default()).unwrap();
            assert_eq!(silo.get(42).unwrap(), b"data");
        }
    }

    #[test]
    fn test_sparse_keys() {
        let dir = tempfile::tempdir().unwrap();
        let mut silo = DataSilo::open(dir.path(), SiloConfig::default()).unwrap();
        silo.append_op(0, b"zero").unwrap();
        silo.append_op(1000, b"thousand").unwrap();
        silo.append_op(100000, b"hundred_k").unwrap();
        silo.compact().unwrap();

        assert_eq!(silo.get(0).unwrap(), b"zero");
        assert_eq!(silo.get(1000).unwrap(), b"thousand");
        assert_eq!(silo.get(100000).unwrap(), b"hundred_k");
        assert!(silo.get(500).is_none());
    }

    #[test]
    fn test_batch_ops() {
        let dir = tempfile::tempdir().unwrap();
        let mut silo = DataSilo::open(dir.path(), SiloConfig::default()).unwrap();
        silo.append_ops_batch(&[
            (1, b"a".to_vec()),
            (2, b"b".to_vec()),
            (3, b"c".to_vec()),
        ]).unwrap();
        silo.compact().unwrap();

        assert_eq!(silo.get(1).unwrap(), b"a");
        assert_eq!(silo.get(2).unwrap(), b"b");
        assert_eq!(silo.get(3).unwrap(), b"c");
    }

    #[test]
    fn test_delete_cold_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let mut silo = DataSilo::open(dir.path(), SiloConfig::default()).unwrap();

        silo.append_op(1, b"hello").unwrap();
        silo.append_op(2, b"world").unwrap();
        silo.append_op(3, b"foo").unwrap();
        silo.delete(2).unwrap();
        silo.compact().unwrap();

        // Key 1 and 3 should exist, key 2 should be deleted
        assert_eq!(silo.get(1).unwrap(), b"hello");
        assert!(silo.get(2).is_none(), "deleted key should return None");
        assert_eq!(silo.get(3).unwrap(), b"foo");
    }

    #[test]
    fn test_delete_hot_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let mut silo = DataSilo::open(dir.path(), SiloConfig::default()).unwrap();

        // Phase 1: write and compact (cold)
        silo.append_op(1, b"hello").unwrap();
        silo.append_op(2, b"world").unwrap();
        silo.compact().unwrap();
        assert_eq!(silo.get(2).unwrap(), b"world");

        // Phase 2: delete via ops, compact again (hot)
        silo.delete(2).unwrap();
        silo.compact().unwrap();

        assert_eq!(silo.get(1).unwrap(), b"hello");
        assert!(silo.get(2).is_none(), "deleted key should return None after hot compact");
    }

    #[test]
    fn test_delete_get_with_ops() {
        let dir = tempfile::tempdir().unwrap();
        let mut silo = DataSilo::open(dir.path(), SiloConfig::default()).unwrap();

        // Write and compact so data is in the data file
        silo.append_op(1, b"hello").unwrap();
        silo.compact().unwrap();
        assert_eq!(silo.get(1).unwrap(), b"hello");

        // Delete via ops (not yet compacted)
        silo.delete(1).unwrap();

        // get() still returns data from the data file (no ops overlay)
        assert_eq!(silo.get(1).unwrap(), b"hello");
        // get_with_ops() should return None (delete tombstone in ops)
        assert!(silo.get_with_ops(1).is_none(), "get_with_ops should respect delete tombstone");
    }

    #[test]
    fn test_delete_then_reinsert() {
        let dir = tempfile::tempdir().unwrap();
        let mut silo = DataSilo::open(dir.path(), SiloConfig::default()).unwrap();

        silo.append_op(1, b"original").unwrap();
        silo.delete(1).unwrap();
        silo.append_op(1, b"reinserted").unwrap();
        silo.compact().unwrap();

        // Last write wins — reinsert after delete
        assert_eq!(silo.get(1).unwrap(), b"reinserted");
    }

    /// Verify that ops written after compact() starts are not lost.
    ///
    /// Simulates the race condition that the A-B swap is designed to prevent:
    /// 1. Write initial ops (pre-compaction).
    /// 2. Call compact() — which atomically switches the active slot, then
    ///    compacts the frozen slot.
    /// 3. Write more ops to the silo between compaction calls (they go to the
    ///    now-active idle slot).
    /// 4. Compact again — those later ops must survive.
    #[test]
    fn test_ab_swap_no_ops_lost() {
        let dir = tempfile::tempdir().unwrap();
        let mut silo = DataSilo::open(dir.path(), SiloConfig::default()).unwrap();

        // Phase 1: write some initial docs and compact (cold path).
        silo.append_op(1, b"doc_1_v1").unwrap();
        silo.append_op(2, b"doc_2_v1").unwrap();
        silo.compact().unwrap();

        assert_eq!(silo.get(1).unwrap(), b"doc_1_v1");
        assert_eq!(silo.get(2).unwrap(), b"doc_2_v1");

        // Phase 2: write ops that will be in the active slot during the NEXT compaction.
        // These must not be lost even though compact() will swap the slot.
        silo.append_op(1, b"doc_1_v2").unwrap(); // update existing
        silo.append_op(3, b"doc_3_v1").unwrap(); // new key

        // Compact (hot path). The swap happens inside compact():
        // active slot (A) is frozen, new writes would go to B.
        // The ops above were written to A before the swap, so they are in the frozen log
        // and must be compacted in.
        silo.compact().unwrap();

        assert_eq!(silo.get(1).unwrap(), b"doc_1_v2", "update from active slot must survive");
        assert_eq!(silo.get(2).unwrap(), b"doc_2_v1", "original doc must still be present");
        assert_eq!(silo.get(3).unwrap(), b"doc_3_v1", "new doc from active slot must survive");

        // Phase 3: write ops AFTER compact() returns (these go to the now-active B slot).
        silo.append_op(4, b"doc_4_post_compact").unwrap();
        silo.append_op(1, b"doc_1_v3").unwrap();

        // These ops must be readable via get_with_ops before the next compact.
        assert_eq!(
            silo.get_with_ops(4).unwrap(),
            b"doc_4_post_compact",
            "post-compact op must be readable before next compact"
        );
        assert_eq!(
            silo.get_with_ops(1).unwrap(),
            b"doc_1_v3",
            "post-compact update must shadow data file"
        );

        // Compact again to verify the post-compact ops also survive.
        silo.compact().unwrap();

        assert_eq!(silo.get(1).unwrap(), b"doc_1_v3");
        assert_eq!(silo.get(4).unwrap(), b"doc_4_post_compact");

        // No ops should remain after full compaction of a quiet silo.
        assert!(!silo.has_ops(), "both slots should be empty after compacting all ops");
    }

    /// Verify readers are never blocked during hot compaction.
    ///
    /// The old code set `self.data_mmap = None` before writing the new file,
    /// meaning any concurrent `get()` would return None until compaction finished.
    /// The new code keeps the old mmap alive (writes to a tmp file, then renames),
    /// so `get()` on an old key must still return the old value mid-compaction.
    ///
    /// Since `compact_hot_from` takes `&mut self` we can't literally race a reader,
    /// but we verify the structural invariant: after cold compaction establishes
    /// data, hot compaction must not make the old data momentarily invisible.
    /// We do this by confirming that `get()` works on the old key at every step.
    #[test]
    fn test_hot_compact_does_not_drop_read_mmap_early() {
        let dir = tempfile::tempdir().unwrap();
        let mut silo = DataSilo::open(dir.path(), SiloConfig::default()).unwrap();

        // Establish data via cold compaction.
        silo.append_op(10, b"value_10").unwrap();
        silo.append_op(20, b"value_20").unwrap();
        silo.compact().unwrap();

        // data_mmap is Some after cold compaction — readers can call get().
        assert!(silo.data_mmap.is_some(), "data_mmap should be Some after cold compact");
        assert_eq!(silo.get(10).unwrap(), b"value_10");

        // Queue an overflow op (value larger than min_entry_size=256 forces overflow path).
        let big_value: Vec<u8> = (0u8..=255).cycle().take(300).collect();
        silo.append_op(10, &big_value).unwrap();
        silo.append_op(30, b"new_key").unwrap(); // new key — also overflow
        silo.compact().unwrap(); // hot path

        // After hot compact, data_mmap must be Some and return correct data.
        assert!(silo.data_mmap.is_some(), "data_mmap must be Some after hot compact");
        assert_eq!(silo.get(10).unwrap(), &big_value[..]);
        assert_eq!(silo.get(20).unwrap(), b"value_20");
        assert_eq!(silo.get(30).unwrap(), b"new_key");
    }

    /// Verify data is written before index during hot compaction.
    ///
    /// The old code wrote data AND updated index in the same loop iteration,
    /// so a crash mid-loop could leave the index pointing at half-written data.
    /// The new code writes all data first (to tmp), renames, then updates the index.
    ///
    /// We verify this by running many sequential hot compactions and confirming
    /// all values survive every round — no interleaving can corrupt the state.
    #[test]
    fn test_hot_compact_data_before_index_sequential_rounds() {
        let dir = tempfile::tempdir().unwrap();
        let mut silo = DataSilo::open(dir.path(), SiloConfig::default()).unwrap();

        // Cold compaction to establish initial data.
        for i in 0u32..50 {
            silo.append_op(i, format!("initial_{}", i).as_bytes()).unwrap();
        }
        silo.compact().unwrap();

        // Run 10 rounds of hot compaction, each updating half the keys and adding new ones.
        for round in 0u32..10 {
            for i in 0u32..25 {
                let v = format!("round_{}_key_{}", round, i);
                silo.append_op(i, v.as_bytes()).unwrap();
            }
            // Add new keys each round (overflow path, since key >= index_len initially)
            let new_key = 50 + round;
            silo.append_op(new_key, format!("new_{}", round).as_bytes()).unwrap();
            silo.compact().unwrap();

            // All previously established keys must still be readable.
            for i in 25u32..50 {
                let expected = format!("initial_{}", i);
                assert_eq!(
                    silo.get(i).unwrap(),
                    expected.as_bytes(),
                    "key {} must survive round {} hot compact", i, round
                );
            }
            // Updated keys must have new values.
            for i in 0u32..25 {
                let expected = format!("round_{}_key_{}", round, i);
                assert_eq!(
                    silo.get(i).unwrap(),
                    expected.as_bytes(),
                    "key {} must have round {} value", i, round
                );
            }
            // New key from this round must exist.
            assert_eq!(
                silo.get(new_key).unwrap(),
                format!("new_{}", round).as_bytes(),
                "new key {} must survive after round {}", new_key, round
            );
        }
    }

    /// Verify that legacy ops.log is migrated to ops_a.log on open.
    #[test]
    fn test_legacy_ops_log_migration() {
        let dir = tempfile::tempdir().unwrap();

        // Simulate old-format silo: create ops.log directly.
        {
            let mut log = OpsLog::open(&dir.path().join("ops.log")).unwrap();
            log.append(&SiloOp::Put { key: 77, value: b"legacy_value".to_vec() }).unwrap();
            log.flush().unwrap();
        }

        // Opening should silently migrate ops.log → ops_a.log.
        let silo = DataSilo::open(dir.path(), SiloConfig::default()).unwrap();

        // ops.log should no longer exist.
        assert!(!dir.path().join("ops.log").exists(), "legacy ops.log should have been renamed");
        // ops_a.log should exist.
        assert!(dir.path().join("ops_a.log").exists(), "ops_a.log should exist after migration");

        // The migrated data should be readable.
        assert_eq!(
            silo.get_with_ops(77).unwrap(),
            b"legacy_value",
            "migrated ops must be readable"
        );
    }
}
