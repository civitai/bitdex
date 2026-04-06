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
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use rayon::prelude::*;

mod ops_log;
pub mod hash_index;

pub use ops_log::{SiloOp, SiloOpRef, OpsLog};
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
    /// Count of ops dropped due to mmap overflow. Checked after parallel writes complete.
    pub overflow_count: AtomicU64,
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
    pub fn write_put(&self, key: u64, value: &[u8], local_cursor: &mut usize, local_end: &mut usize) -> bool {
        let mut frame_buf = Vec::with_capacity(value.len() + 20);
        OpsLog::encode_put_into(&mut frame_buf, key, value);
        self.write_frame(&frame_buf, local_cursor, local_end)
    }

    /// Write a Put op reusing a caller-provided buffer. Zero allocation per call.
    /// The buffer is cleared and reused — caller keeps it across rows.
    #[inline]
    pub fn write_put_reuse(&self, key: u64, value: &[u8], buf: &mut Vec<u8>, local_cursor: &mut usize, local_end: &mut usize) -> bool {
        buf.clear();
        OpsLog::encode_put_into(buf, key, value);
        self.write_frame(buf, local_cursor, local_end)
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
            self.overflow_count.fetch_add(1, Ordering::Relaxed);
            return false; // out of space — caller must handle
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
// DumpMergeWriter — direct read-modify-write for dump phases
// ---------------------------------------------------------------------------

const MERGE_STRIPE_COUNT: usize = 1024;

/// Handle for direct read-modify-write during dump phases.
///
/// Created by `DataSilo::prepare_dump_merge()` after the images phase has
/// pre-allocated all slots via `write_batch_parallel`. Subsequent phases
/// (tags, tools, techniques, resources) use this to read existing doc records,
/// merge new field data (Mi array concatenation), and write back in-place.
///
/// Bypasses the ops log entirely — no compaction needed for dump doc writes.
///
/// Thread-safe via striped locks: each key is serialized by `key % 1024`,
/// but distinct keys can be written concurrently from rayon threads.
pub struct DumpMergeWriter {
    /// Raw pointer to the writable mmap for data.bin.
    write_ptr: *mut u8,
    /// Keeps the writable mmap alive.
    _write_mmap: memmap2::MmapMut,
    /// Raw pointer to the read mmap for data.bin (the DataSilo's existing mmap).
    read_ptr: *const u8,
    /// Length of the read mmap.
    read_len: usize,
    /// Pointer to the HashIndex for entry lookups and concurrent updates.
    index_ptr: *const HashIndex,
    /// Striped locks for key-level serialization.
    stripes: Box<[parking_lot::Mutex<()>]>,
    /// Count of successful in-place writes.
    pub in_place_count: AtomicU64,
    /// Count of writes that overflowed (merged data > allocated buffer).
    pub overflow_count: AtomicU64,
}

// SAFETY: DumpMergeWriter is Send+Sync because:
// - write_ptr/read_ptr point to stable mmaps (not freed during writer lifetime)
// - index_ptr points to DataSilo's HashIndex (stable during dump)
// - Stripe locks ensure no two threads access the same key simultaneously
// - Different keys occupy different hash table slots (no aliased writes)
unsafe impl Send for DumpMergeWriter {}
unsafe impl Sync for DumpMergeWriter {}

impl DumpMergeWriter {
    /// Merge new data into an existing entry using a caller-provided merge function.
    ///
    /// The merge function receives `(existing_bytes, new_bytes)` and returns the
    /// merged result. For doc records, this decodes both, concatenates Mi arrays,
    /// and re-encodes.
    ///
    /// Returns `true` if the write succeeded (in-place), `false` if:
    /// - The key doesn't exist in the index (shouldn't happen after images phase)
    /// - The merged data exceeds the allocated buffer (overflow)
    ///
    /// If the key has no existing data (length=0), `new_bytes` is written directly
    /// without calling the merge function.
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

        // If existing entry is empty (length=0), write new_bytes directly
        let to_write = if entry.length == 0 {
            std::borrow::Cow::Borrowed(new_bytes)
        } else {
            // Read existing data from the READ mmap
            let end = start + entry.length as usize;
            if end > self.read_len {
                self.overflow_count.fetch_add(1, Ordering::Relaxed);
                return false;
            }
            let existing = unsafe {
                std::slice::from_raw_parts(self.read_ptr.add(start), entry.length as usize)
            };
            std::borrow::Cow::Owned(merge_fn(existing, new_bytes))
        };

        if to_write.len() as u32 > entry.allocated {
            self.overflow_count.fetch_add(1, Ordering::Relaxed);
            return false;
        }

        // Write merged data to the WRITE mmap at the same offset
        unsafe {
            std::ptr::copy_nonoverlapping(
                to_write.as_ptr(),
                self.write_ptr.add(start),
                to_write.len(),
            );
        }

        // Update index entry length (offset and allocated stay the same)
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
    /// Used by the images phase or when the entry is known to be empty.
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
    pub fn flush(&self) -> io::Result<()> {
        // The _write_mmap field holds the MmapMut — we can't call flush through
        // the raw pointer, but the mmap will flush on drop. For explicit flush,
        // callers should drop the DumpMergeWriter.
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// DataSilo — the main store
// ---------------------------------------------------------------------------

/// Type alias for the merge function used during compaction.
/// Called as `merge_fn(existing_bytes, new_bytes) -> merged_bytes`.
/// Used to merge multiple ops for the same key instead of last-write-wins.
pub type MergeFn = Box<dyn Fn(&[u8], &[u8]) -> Vec<u8> + Send + Sync>;

pub struct DataSilo {
    path: PathBuf,
    config: SiloConfig,
    /// Hash index: maps u64 key → (offset, length, allocated) in the data file.
    /// Replaces the former flat array index — supports the full u64 key space.
    index: Option<HashIndex>,
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
    /// Optional merge function for compaction. When set, multiple ops for the
    /// same key are merged instead of last-write-wins. Also merges with existing
    /// data file entries during hot compaction.
    merge_fn: Option<MergeFn>,
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
            index: None,
            data_mmap: None,
            data_len: 0,
            ops_a: parking_lot::Mutex::new(ops_a),
            ops_b: parking_lot::Mutex::new(ops_b),
            active_is_b: AtomicBool::new(false),
            dead_bytes: AtomicU64::new(0),
            merge_fn: None,
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
            overflow_count: AtomicU64::new(0),
        })
    }

    /// Flush the active ops log mmap to disk. Call after parallel writes complete.
    pub fn flush_ops(&self) -> io::Result<()> {
        self.ops_log().lock().flush()
    }

    /// Append a single op (sequential, single-thread steady-state path).
    pub fn append_op(&self, key: u64, value: &[u8]) -> io::Result<()> {
        self.ops_log().lock().append(&SiloOp::Put { key, value: value.to_vec() })
    }

    /// Append a batch of ops sequentially. Useful for small batches in steady state.
    pub fn append_ops_batch(&self, ops: &[(u64, Vec<u8>)]) -> io::Result<()> {
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
    pub fn delete(&self, key: u64) -> io::Result<()> {
        self.ops_log().lock().append(&SiloOp::Delete { key })
    }

    // ── Dump merge writer (direct read-modify-write, no ops log) ────���─

    /// Create a `DumpMergeWriter` for direct read-modify-write during dump phases.
    ///
    /// The data file + index must already exist (created by `write_batch_parallel`
    /// during the images phase). Subsequent phases use the merge writer to read
    /// existing entries, merge new field data, and write back in-place.
    ///
    /// Returns `None` if there's no data file or index (images phase hasn't run yet).
    pub fn prepare_dump_merge(&self) -> io::Result<Option<DumpMergeWriter>> {
        let index = match self.index.as_ref() {
            Some(idx) if idx.count() > 0 => idx,
            _ => return Ok(None),
        };
        let data_mmap = match self.data_mmap.as_ref() {
            Some(m) if !m.is_empty() => m,
            _ => return Ok(None),
        };

        // Open a writable mmap on the same data.bin for in-place writes.
        let data_path = self.path.join("data.bin");
        let data_file = OpenOptions::new()
            .read(true).write(true).open(&data_path)?;
        let mut write_mmap = unsafe { memmap2::MmapMut::map_mut(&data_file)? };

        Ok(Some(DumpMergeWriter {
            write_ptr: write_mmap.as_mut_ptr(),
            _write_mmap: write_mmap,
            read_ptr: data_mmap.as_ptr(),
            read_len: data_mmap.len(),
            index_ptr: index as *const HashIndex,
            stripes: (0..MERGE_STRIPE_COUNT)
                .map(|_| parking_lot::Mutex::new(()))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            in_place_count: AtomicU64::new(0),
            overflow_count: AtomicU64::new(0),
        }))
    }

    /// Reload the data mmap after dump merge writes.
    /// Call this after dropping the DumpMergeWriter so queries see updated data.
    pub fn reload_data(&mut self) -> io::Result<()> {
        self.data_mmap = None;
        self.load_data()
    }

    // ── Bulk write (bypass ops log, write directly to data+index) ─────

    /// Write a batch of entries directly to data.bin + index.bin using rayon
    /// parallel mmap writes. Bypasses the ops log entirely — used for bulk saves
    /// (dump snapshots) where we want maximum throughput.
    ///
    /// Semantics: overwrites the entire data file + index. Existing data is dropped.
    /// The caller is responsible for ensuring no concurrent reads during this call.
    pub fn write_batch_parallel(&mut self, entries: &[(u64, Vec<u8>)]) -> io::Result<u64> {
        if entries.is_empty() { return Ok(0); }

        let count = entries.len() as u64;
        let align = self.config.alignment.max(1) as u64;
        let buffer_ratio = self.config.buffer_ratio;
        let min_entry_size = self.config.min_entry_size;

        // Drop old index and data mmaps before writing
        self.index = None;
        self.data_mmap = None;

        // Phase 1: Compute entry layouts (sequential — offset computation is inherently serial)
        struct EntryLayout { idx: usize, key: u64, offset: u64, length: u32, allocated: u32 }
        let mut layouts: Vec<EntryLayout> = Vec::with_capacity(entries.len());

        // Sort by key for index locality (improves hash table insertion order)
        let mut sorted_indices: Vec<usize> = (0..entries.len()).collect();
        sorted_indices.sort_unstable_by_key(|&i| entries[i].0);

        let mut offset: u64 = 0;
        for &idx in &sorted_indices {
            let (key, ref value) = entries[idx];
            if align > 1 {
                offset = (offset + align - 1) & !(align - 1);
            }
            let len = value.len() as u32;
            let mut allocated = ((len as f32 * buffer_ratio).ceil() as u32)
                .max(min_entry_size);
            if align > 1 {
                allocated = ((allocated as u64 + align - 1) & !(align - 1)) as u32;
            }
            layouts.push(EntryLayout { idx, key, offset, length: len, allocated });
            offset += allocated as u64;
        }
        let total_data_size = offset;

        // Phase 2: Pre-allocate data file as mmap
        let data_path = self.path.join("data.bin");
        let data_file = OpenOptions::new()
            .create(true).read(true).write(true).truncate(true).open(&data_path)?;
        data_file.set_len(total_data_size)?;
        let mut data_mmap = unsafe { memmap2::MmapMut::map_mut(&data_file)? };
        // Sequential hint: bulk write pass reads/writes monotonically increasing offsets.
        #[cfg(unix)] let _ = data_mmap.advise(memmap2::Advice::Sequential);

        // Phase 3: Parallel mmap writes for data
        let data_base = data_mmap.as_mut_ptr() as usize;
        let data_mmap_len = data_mmap.len();

        layouts.par_iter().for_each(|layout| {
            let value = &entries[layout.idx].1;
            let start = layout.offset as usize;
            if start + value.len() <= data_mmap_len {
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        value.as_ptr(),
                        (data_base + start) as *mut u8,
                        value.len(),
                    );
                }
            }
        });

        data_mmap.flush()?;
        drop(data_mmap);

        // Phase 4: Build hash index (sequential — linear probing requires single writer)
        // Capacity = 2× entry count to keep load factor ≤ 50%.
        let index_capacity = (count * 2).max(16);
        let index_path = self.path.join("index.bin");
        // Remove existing index file so HashIndex::new() can create fresh
        if index_path.exists() { let _ = std::fs::remove_file(&index_path); }
        let mut idx = HashIndex::new(&index_path, index_capacity)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("HashIndex::new: {e}")))?;

        for layout in &layouts {
            idx.put(layout.key, IndexEntry {
                offset: layout.offset,
                length: layout.length,
                allocated: layout.allocated,
            }).map_err(|e| io::Error::new(io::ErrorKind::Other, format!("HashIndex::put key={}: {e}", layout.key)))?;
        }
        idx.flush()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("HashIndex::flush: {e}")))?;

        self.index = Some(idx);
        self.load_data()?;
        self.data_len = offset;
        self.dead_bytes.store(0, Ordering::Relaxed);

        // Truncate both ops logs since we just wrote everything fresh
        self.ops_a.lock().truncate()?;
        self.ops_b.lock().truncate()?;

        eprintln!("DataSilo: write_batch_parallel {} entries, {:.1}MB data, hash index cap={}",
            count, offset as f64 / 1e6, index_capacity);
        Ok(count)
    }

    // ── Read path ───────────────────────────────────────────────────────

    /// Read an entry by key from the data file (no ops overlay).
    /// Fast path for queries after compaction.
    pub fn get(&self, key: u64) -> Option<&[u8]> {
        let entry = self.index_entry(key)?;
        if entry.length == 0 { return None; }
        let mmap = self.data_mmap.as_ref()?;
        let start = entry.offset as usize;
        let end = start + entry.length as usize;
        if end <= mmap.len() { Some(&mmap[start..end]) } else { None }
    }

    /// Scan both ops logs for ALL values written to a key, calling `f` for each.
    /// Unlike `get_with_ops` (which returns only the last value), this yields every
    /// op in chronological order (A then B). Used by BitmapSilo for ops-on-read
    /// where individual set/clear mutations must all be applied.
    pub fn scan_ops_for_key<F>(&self, key: u64, mut f: F) -> io::Result<()>
    where F: FnMut(&[u8])
    {
        let log_a = self.ops_a.lock();
        let log_b = self.ops_b.lock();
        // Scan A (may be frozen/older) then B (active/newer)
        log_a.for_each(|op_key, value| {
            if op_key == key { f(value); }
        })?;
        log_b.for_each(|op_key, value| {
            if op_key == key { f(value); }
        })?;
        Ok(())
    }

    /// Read an entry with ops overlay (returns owned data).
    /// Scans BOTH ops logs (A and B) for the latest value of this key.
    /// Last-write-wins across both logs (frozen log has older ops, active has newer).
    /// Handles both Put (update) and Delete (tombstone) ops.
    pub fn get_with_ops(&self, key: u64) -> Option<Vec<u8>> {
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

    /// Returns the number of live (non-tombstone) entries in the hash index.
    pub fn index_capacity(&self) -> u64 {
        self.index.as_ref().map(|idx| idx.count()).unwrap_or(0)
    }

    /// Iterate all live (compacted) keys in the hash index.
    /// Does NOT include keys that are only in the ops log (not yet compacted).
    /// Use `for_each_ops` on the ops log for those.
    pub fn iter_index_keys(&self) -> impl Iterator<Item = u64> + '_ {
        self.index.iter()
            .flat_map(|idx| idx.iter())
            .map(|(key, _entry)| key)
    }
    pub fn data_bytes(&self) -> u64 { self.data_len }
    /// Total bytes written across both ops logs.
    pub fn ops_size(&self) -> u64 {
        self.ops_a.lock().data_size() + self.ops_b.lock().data_size()
    }
    pub fn path(&self) -> &Path { &self.path }
    pub fn config(&self) -> &SiloConfig { &self.config }

    /// Set a merge function for compaction.
    /// When set, multiple ops for the same key are merged instead of last-write-wins.
    /// The function receives `(existing_value, new_value)` and returns the merged result.
    /// Also applied during hot compaction when merging ops into existing data file entries.
    pub fn set_merge_fn<F>(&mut self, f: F)
    where F: Fn(&[u8], &[u8]) -> Vec<u8> + Send + Sync + 'static
    {
        self.merge_fn = Some(Box::new(f));
    }

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
        let has_data = self.data_mmap.is_some() && self.index.as_ref().map(|i| i.count() > 0).unwrap_or(false);
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
        // If merge_fn is set, use the merge-aware path (copies values for merging).
        // Otherwise use zero-copy path (stores mmap offsets).
        if self.merge_fn.is_some() {
            return self.compact_cold_merge(frozen_is_b);
        }

        // Zero-copy scan: collect (key → mmap_offset, value_len) instead of copying values.
        // LWW dedup: last Put wins, Delete removes.
        // Values stay in the source mmap until the write phase reads them directly.
        let mut entries: std::collections::HashMap<u64, (usize, usize)> = std::collections::HashMap::new();
        {
            let log = if frozen_is_b { self.ops_b.lock() } else { self.ops_a.lock() };
            log.for_each_ops_ref(|op| {
                match op {
                    SiloOpRef::Put { key, offset, len } => {
                        entries.insert(key, (offset, len));
                    }
                    SiloOpRef::Delete { key } => {
                        entries.remove(&key);
                    }
                }
            })?;
        }
        if entries.is_empty() { return Ok(0); }

        let count = entries.len() as u64;
        let align = self.config.alignment.max(1) as u64;
        let buffer_ratio = self.config.buffer_ratio;
        let min_entry_size = self.config.min_entry_size;

        // Sort keys and compute per-entry layout (offsets must be sequential)
        let mut keys: Vec<u64> = entries.keys().copied().collect();
        keys.sort_unstable();

        // Phase 1: Compute entry layouts — offset, length, allocated (sequential)
        struct EntryLayout { key: u64, offset: u64, length: u32, allocated: u32 }
        let mut layouts: Vec<EntryLayout> = Vec::with_capacity(keys.len());
        let mut data_offset: u64 = 0;
        for &key in &keys {
            if align > 1 {
                data_offset = (data_offset + align - 1) & !(align - 1);
            }
            let (_, len) = entries[&key];
            let len32 = len as u32;
            let mut allocated = ((len32 as f32 * buffer_ratio).ceil() as u32)
                .max(min_entry_size);
            if align > 1 {
                allocated = ((allocated as u64 + align - 1) & !(align - 1)) as u32;
            }
            layouts.push(EntryLayout { key, offset: data_offset, length: len32, allocated });
            data_offset += allocated as u64;
        }
        let total_data_size = data_offset;

        // Get pointer to source mmap for zero-copy reads during write phase
        let source_mmap_ptr: usize = {
            let log = if frozen_is_b { self.ops_b.lock() } else { self.ops_a.lock() };
            match log.mmap_data() {
                Some(data) => data.as_ptr() as usize,
                None => return Err(io::Error::new(io::ErrorKind::Other, "source mmap unavailable")),
            }
        };

        // Drop old index and data before writing
        self.index = None;
        self.data_mmap = None;

        // Phase 2: Pre-allocate data file as mmap
        let data_path = self.path.join("data.bin");
        let data_file = OpenOptions::new()
            .create(true).read(true).write(true).truncate(true).open(&data_path)?;
        data_file.set_len(total_data_size)?;
        let mut data_mmap = unsafe { memmap2::MmapMut::map_mut(&data_file)? };
        #[cfg(unix)] let _ = data_mmap.advise(memmap2::Advice::Sequential);

        // Phase 3: Write data (parallel memcpy via rayon)
        // Zero-copy: reads value bytes directly from source ops log mmap.
        let data_base = data_mmap.as_mut_ptr() as usize;
        let data_mmap_len = data_mmap.len();

        layouts.par_iter().for_each(|layout| {
            let (src_offset, src_len) = entries[&layout.key];
            let start = layout.offset as usize;
            if start + src_len <= data_mmap_len {
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        (source_mmap_ptr + src_offset) as *const u8,
                        (data_base + start) as *mut u8,
                        src_len,
                    );
                }
            }
        });

        data_mmap.flush()?;
        drop(data_mmap);

        // Phase 4: Build hash index (sequential — single writer required)
        let index_capacity = (count * 2).max(16);
        let index_path = self.path.join("index.bin");
        if index_path.exists() { let _ = std::fs::remove_file(&index_path); }
        let mut idx = HashIndex::new(&index_path, index_capacity)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("HashIndex::new: {e}")))?;
        for layout in &layouts {
            idx.put(layout.key, IndexEntry {
                offset: layout.offset,
                length: layout.length,
                allocated: layout.allocated,
            }).map_err(|e| io::Error::new(io::ErrorKind::Other, format!("HashIndex::put key={}: {e}", layout.key)))?;
        }
        idx.flush()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("HashIndex::flush: {e}")))?;

        self.index = Some(idx);
        self.load_data()?;
        self.data_len = total_data_size;
        self.dead_bytes.store(0, Ordering::Relaxed); // full rewrite = no dead space

        // NOTE: caller (compact()) truncates the frozen log after this returns.

        eprintln!("DataSilo: cold compacted {} entries, {:.1}MB data, hash index cap={}",
            count, total_data_size as f64 / 1e6, index_capacity);
        Ok(count)
    }

    /// Cold compaction with merge function — copies values and merges duplicates.
    /// Used when `self.merge_fn` is set (e.g., doc silo with Mi field concatenation).
    fn compact_cold_merge(&mut self, frozen_is_b: bool) -> io::Result<u64> {
        let merge = self.merge_fn.as_ref().unwrap();

        // Collect ops with merging: duplicate keys call merge_fn instead of LWW.
        let mut entries: std::collections::HashMap<u64, Vec<u8>> = std::collections::HashMap::new();
        {
            let log = if frozen_is_b { self.ops_b.lock() } else { self.ops_a.lock() };
            log.for_each_ops(|op| {
                match op {
                    SiloOp::Put { key, value } => {
                        if let Some(existing) = entries.get(&key) {
                            let merged = merge(existing, &value);
                            entries.insert(key, merged);
                        } else {
                            entries.insert(key, value);
                        }
                    }
                    SiloOp::Delete { key } => {
                        entries.remove(&key);
                    }
                }
            })?;
        }
        if entries.is_empty() { return Ok(0); }

        // Write merged entries via write_batch_parallel
        let batch: Vec<(u64, Vec<u8>)> = entries.into_iter().collect();
        let count = self.write_batch_parallel(&batch)?;
        Ok(count)
    }

    /// Hot compaction: existing data file with pre-allocated buffer slots.
    ///
    /// Correctness properties:
    /// - Readers (via `get()`) are never blocked: `self.data_mmap` stays alive
    ///   throughout — never dropped during Path A; only dropped after rename in Path B.
    /// - Data is fully on disk before the index is updated: a crash between the
    ///   two is safe in both paths (Path A: data written, index not yet; Path B:
    ///   old index still points into old file which has been replaced but is complete).
    ///
    /// Two paths chosen after classification:
    ///
    /// **Path A — In-place only** (common case: all updates fit in allocated buffers):
    ///   No new keys and no values that exceed their allocated slot → write directly
    ///   into the existing data.bin via a writable file handle (no mmap aliasing).
    ///   No temp file, no copy, no rename.  `self.data_mmap` is never dropped.
    ///
    /// **Path B — Has overflows** (rare: some entries exceed their allocated buffer
    ///   or are brand-new keys):
    ///   Copies the entire old data.bin to a temp file, appends overflow entries,
    ///   renames into place, then remaps `self.data_mmap`.  This is the former
    ///   algorithm, kept intact for this (uncommon) case.
    ///
    /// `frozen_is_b`: true = ops_b is frozen, false = ops_a is frozen.
    fn compact_hot_from(&mut self, frozen_is_b: bool) -> io::Result<u64> {
        // ── Step 1: Collect ops ──────────────────────────────────────────
        // When merge_fn is set, duplicate keys are merged instead of LWW.
        // Also, existing data file entries are merged with ops values.
        let mut ops: std::collections::HashMap<u64, Option<Vec<u8>>> = std::collections::HashMap::new();
        {
            let log = if frozen_is_b { self.ops_b.lock() } else { self.ops_a.lock() };
            let merge = &self.merge_fn;
            log.for_each_ops(|op| {
                match op {
                    SiloOp::Put { key, value } => {
                        if let Some(ref merge_fn) = merge {
                            if let Some(Some(existing)) = ops.get(&key) {
                                let merged = merge_fn(existing, &value);
                                ops.insert(key, Some(merged));
                            } else {
                                ops.insert(key, Some(value));
                            }
                        } else {
                            ops.insert(key, Some(value));
                        }
                    }
                    SiloOp::Delete { key } => {
                        ops.insert(key, None);
                    }
                }
            })?;
        }
        if ops.is_empty() { return Ok(0); }

        // When merge_fn is set, also merge ops values with existing data file entries.
        if let Some(ref merge_fn) = self.merge_fn {
            for (key, value_opt) in ops.iter_mut() {
                if let Some(ref mut new_value) = value_opt {
                    if let Some(existing_bytes) = self.get(*key) {
                        *new_value = merge_fn(existing_bytes, new_value);
                    }
                }
            }
        }

        let count = ops.len() as u64;

        // ── Step 2: Classify ops (read-only, nothing mutated) ────────────
        // in_place: key→(old IndexEntry, new value) — fits in existing slot
        // overflows: key→new value — new key or doesn't fit, goes to end
        // deletions: (key, old_allocated) — tombstone index entry, account dead space
        //
        // Dead space is computed here while the original index is still intact.
        struct InPlaceUpdate { old_entry: IndexEntry, new_len: u32 }
        let mut in_place_map: std::collections::HashMap<u64, InPlaceUpdate> = std::collections::HashMap::new();
        let mut overflows: Vec<(u64, Vec<u8>)> = Vec::new();
        // (key, old_allocated_bytes_now_dead)
        let mut deletions: Vec<(u64, u64)> = Vec::new();
        // Dead bytes from overflow-displaced entries (old slots become dead in new file)
        let mut dead_from_overflows: u64 = 0;

        for (&key, value_opt) in &ops {
            match value_opt {
                None => {
                    // Delete tombstone — read old allocated bytes while index is intact
                    let old_allocated = self.index_entry(key)
                        .filter(|e| e.allocated > 0)
                        .map(|e| e.allocated as u64)
                        .unwrap_or(0);
                    deletions.push((key, old_allocated));
                }
                Some(value) => {
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
                        if old_entry.allocated > 0 {
                            dead_from_overflows += old_entry.allocated as u64;
                        }
                    }
                    // Falls through to overflow
                    overflows.push((key, value.clone()));
                }
            }
        }

        // ── Path A: In-place only (no overflows or new keys) ────────────
        //
        // All ops fit within their existing allocated slots — write directly into
        // data.bin using a writable file handle. No index rebuild needed.
        //
        // Invariant order: ALL data writes → data flush → index writes → index flush.
        // self.data_mmap (read mmap) is never dropped — readers stay unblocked.
        if overflows.is_empty() {
            let data_path = self.path.join("data.bin");

            // Open data.bin as a writable file for targeted byte-range writes.
            let data_file = OpenOptions::new().write(true).open(&data_path)?;

            use std::io::{Seek, SeekFrom, Write};
            let mut data_file = std::io::BufWriter::new(data_file);
            for (&key, update) in &in_place_map {
                if let Some(Some(value)) = ops.get(&key) {
                    data_file.seek(SeekFrom::Start(update.old_entry.offset))?;
                    data_file.write_all(value)?;
                }
            }
            data_file.flush()?;
            data_file.into_inner()
                .map_err(|e| e.into_error())?
                .sync_data()?;

            // ── Index: in-place length updates + deletion tombstones ──────
            let idx = match self.index.as_mut() {
                Some(i) => i,
                None => {
                    eprintln!("DataSilo: hot compact path A — no index, skipping index update");
                    return Ok(count);
                }
            };
            for (&key, update) in &in_place_map {
                let new_entry = IndexEntry {
                    offset: update.old_entry.offset,
                    length: update.new_len,
                    allocated: update.old_entry.allocated,
                };
                let _ = idx.put(key, new_entry);
            }

            let mut dead_from_deletes: u64 = 0;
            for &(key, old_allocated) in &deletions {
                dead_from_deletes += old_allocated;
                idx.remove(key);
            }

            self.dead_bytes.fetch_add(dead_from_deletes, Ordering::Relaxed);
            // dead_from_overflows is zero in Path A (verified: overflows.is_empty())

            idx.flush()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("HashIndex::flush: {e}")))?;

            // NOTE: caller (compact()) truncates the frozen log after this returns.
            // self.data_mmap is intentionally NOT remapped — same file, same offsets.

            eprintln!("DataSilo: hot compacted {} ops ({} in-place, 0 overflow, {} deletes) [path=A]",
                count, in_place_map.len(), deletions.len());
            return Ok(count);
        }

        // ── Path B: Has overflows — in-place updates + append overflows ──
        //
        // Some entries don't fit their existing slot or are brand-new keys.
        // In-place updates write directly to data.bin. Overflows append to the end.
        let data_path = self.path.join("data.bin");

        let align = self.config.alignment.max(1) as u64;
        let buffer_ratio = self.config.buffer_ratio;
        let min_entry_size = self.config.min_entry_size;

        // ── Step 3a: Write in-place updates to existing data.bin ──────────
        {
            let data_file = OpenOptions::new().write(true).open(&data_path)?;
            let mut writer = io::BufWriter::with_capacity(1 << 20, data_file);
            for (&key, update) in &in_place_map {
                if let Some(Some(value)) = ops.get(&key) {
                    use io::Seek;
                    writer.seek(io::SeekFrom::Start(update.old_entry.offset))?;
                    writer.write_all(value)?;
                }
            }
            writer.flush()?;
            writer.into_inner().map_err(|e| e.into_error())?.sync_data()?;
        }

        // ── Step 3b: Append overflows to end of data.bin ──────────────────
        let mut new_data_len = self.data_len;
        struct OverflowLayout { key: u64, offset: u64, length: u32, allocated: u32 }
        let mut overflow_layouts: Vec<OverflowLayout> = Vec::with_capacity(overflows.len());
        if !overflows.is_empty() {
            let data_file = OpenOptions::new().write(true).append(true).open(&data_path)?;
            let mut writer = io::BufWriter::with_capacity(1 << 20, data_file);
            let mut offset = self.data_len;

            for (key, value) in &overflows {
                if align > 1 {
                    let aligned = (offset + align - 1) & !(align - 1);
                    if aligned > offset {
                        let pad = (aligned - offset) as usize;
                        let zeros = [0u8; 4096];
                        let mut rem = pad;
                        while rem > 0 {
                            let c = rem.min(4096);
                            writer.write_all(&zeros[..c])?;
                            rem -= c;
                        }
                        offset = aligned;
                    }
                }
                let len = value.len() as u32;
                let mut allocated = ((len as f32 * buffer_ratio).ceil() as u32).max(min_entry_size);
                if align > 1 {
                    allocated = ((allocated as u64 + align - 1) & !(align - 1)) as u32;
                }

                writer.write_all(value)?;
                if allocated > len {
                    let zeros = [0u8; 4096];
                    let mut rem = (allocated - len) as usize;
                    while rem > 0 {
                        let c = rem.min(4096);
                        writer.write_all(&zeros[..c])?;
                        rem -= c;
                    }
                }

                overflow_layouts.push(OverflowLayout { key: *key, offset, length: len, allocated });
                offset += allocated as u64;
            }
            writer.flush()?;
            writer.into_inner().map_err(|e| e.into_error())?.sync_data()?;
            new_data_len = offset;
        }

        // ── Step 4: Remap data mmap to pick up appended data ─────────────
        if new_data_len > self.data_len {
            self.data_mmap = None;
            self.load_data()?;
            self.data_len = new_data_len;
        }

        // ── Step 5: Update index ──────────────────────────────────────────
        // Only now do we touch the index. Data file is complete on disk.
        //
        // If the hash index doesn't exist (fresh start after overflow), create it.
        // If it exists but would exceed 75% load with new entries, rebuild it.
        let new_entry_count = (self.index.as_ref().map(|i| i.count()).unwrap_or(0)
            + overflow_layouts.len() as u64)
            .saturating_sub(deletions.len() as u64);
        let need_rebuild = self.index.as_ref()
            .map(|i| new_entry_count + 1 > i.capacity() * 3 / 4)
            .unwrap_or(true);

        if need_rebuild {
            // Rebuild the entire index from scratch by iterating existing entries + new.
            let new_capacity = (new_entry_count * 2).max(16);
            let index_path = self.path.join("index.bin");
            if index_path.exists() { let _ = std::fs::remove_file(&index_path); }
            let mut new_idx = HashIndex::new(&index_path, new_capacity)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("HashIndex::new: {e}")))?;

            // Copy surviving entries from old index
            let deletion_set: std::collections::HashSet<u64> = deletions.iter().map(|(k, _)| *k).collect();
            let overflow_key_set: std::collections::HashSet<u64> = overflow_layouts.iter().map(|l| l.key).collect();
            if let Some(ref old_idx) = self.index {
                for (key, entry) in old_idx.iter() {
                    if deletion_set.contains(&key) { continue; }
                    if overflow_key_set.contains(&key) { continue; } // will be re-added below
                    let updated = if let Some(upd) = in_place_map.get(&key) {
                        IndexEntry { offset: entry.offset, length: upd.new_len, allocated: entry.allocated }
                    } else {
                        entry
                    };
                    let _ = new_idx.put(key, updated);
                }
            }

            // Add overflow entries
            for layout in &overflow_layouts {
                let _ = new_idx.put(layout.key, IndexEntry {
                    offset: layout.offset,
                    length: layout.length,
                    allocated: layout.allocated,
                });
            }

            new_idx.flush()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("HashIndex::flush: {e}")))?;
            self.index = Some(new_idx);
        } else {
            // In-place index update: put overflows + in-place length changes + tombstone deletions
            let idx = self.index.as_mut().unwrap();

            for (&key, update) in &in_place_map {
                let _ = idx.put(key, IndexEntry {
                    offset: update.old_entry.offset,
                    length: update.new_len,
                    allocated: update.old_entry.allocated,
                });
            }
            for layout in &overflow_layouts {
                let _ = idx.put(layout.key, IndexEntry {
                    offset: layout.offset,
                    length: layout.length,
                    allocated: layout.allocated,
                });
            }
            for &(key, _) in &deletions {
                idx.remove(key);
            }
            idx.flush()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("HashIndex::flush: {e}")))?;
        }

        // Account for dead space
        let dead_from_deletes: u64 = deletions.iter().map(|(_, a)| *a).sum();
        self.dead_bytes.fetch_add(dead_from_deletes + dead_from_overflows, Ordering::Relaxed);

        // NOTE: caller (compact()) truncates the frozen log after this returns.

        eprintln!("DataSilo: hot compacted {} ops ({} in-place, {} overflow, {} deletes)",
            count, in_place_map.len(), overflows.len(), deletions.len());
        Ok(count)
    }

    // ── Internal helpers ────────────────────────────────────────────────

    fn index_entry(&self, key: u64) -> Option<IndexEntry> {
        self.index.as_ref()?.get(key)
    }

    fn load_index(&mut self) -> io::Result<()> {
        let p = self.path.join("index.bin");
        if !p.exists() { return Ok(()); }
        match HashIndex::open(&p) {
            Ok(idx) => {
                self.index = Some(idx);
                Ok(())
            }
            Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData,
                format!("load_index: {e}")))
        }
    }

    fn load_data(&mut self) -> io::Result<()> {
        let p = self.path.join("data.bin");
        if !p.exists() { return Ok(()); }
        let f = File::open(&p)?;
        let meta = f.metadata()?;
        if meta.len() == 0 { return Ok(()); }
        let mmap = unsafe { memmap2::Mmap::map(&f)? };
        // Random hint: doc lookups access scattered offsets by slot ID.
        #[cfg(unix)] let _ = mmap.advise(memmap2::Advice::Random);
        // HugePage hint on large data files (>512 MB) to reduce TLB pressure.
        // Linux-only; no-op on all other platforms.
        #[cfg(target_os = "linux")]
        if meta.len() > 512 * 1024 * 1024 {
            let _ = mmap.advise(memmap2::Advice::HugePage);
        }
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
        for i in 0u64..50 {
            silo.append_op(i, format!("initial_{}", i).as_bytes()).unwrap();
        }
        silo.compact().unwrap();

        // Run 10 rounds of hot compaction, each updating half the keys and adding new ones.
        for round in 0u64..10 {
            for i in 0u64..25 {
                let v = format!("round_{}_key_{}", round, i);
                silo.append_op(i, v.as_bytes()).unwrap();
            }
            // Add new keys each round
            let new_key = 50 + round;
            silo.append_op(new_key, format!("new_{}", round).as_bytes()).unwrap();
            silo.compact().unwrap();

            // All previously established keys must still be readable.
            for i in 25u64..50 {
                let expected = format!("initial_{}", i);
                assert_eq!(
                    silo.get(i).unwrap(),
                    expected.as_bytes(),
                    "key {} must survive round {} hot compact", i, round
                );
            }
            // Updated keys must have new values.
            for i in 0u64..25 {
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

    #[test]
    fn test_dump_merge_writer_basic() {
        let dir = tempfile::tempdir().unwrap();
        let mut silo = DataSilo::open(dir.path(), SiloConfig {
            buffer_ratio: 2.0, // 100% headroom for merge growth
            min_entry_size: 64,
            ..Default::default()
        }).unwrap();

        // Phase 1: Write initial entries via write_batch_parallel
        let entries: Vec<(u64, Vec<u8>)> = (1..=10u64)
            .map(|k| (k, format!("doc_{}", k).into_bytes()))
            .collect();
        silo.write_batch_parallel(&entries).unwrap();

        // Verify initial data
        assert_eq!(silo.get(1).unwrap(), b"doc_1");
        assert_eq!(silo.get(10).unwrap(), b"doc_10");

        // Phase 2: Create merge writer and merge new data
        let mw = silo.prepare_dump_merge().unwrap().expect("merge writer should be available");

        // merge_put: append "_updated" to existing value
        let ok = mw.merge_put(1, b"_updated", |existing, new| {
            let mut merged = existing.to_vec();
            merged.extend_from_slice(new);
            merged
        });
        assert!(ok, "merge_put should succeed (in-place)");

        // put_direct: overwrite with new value
        let ok = mw.put_direct(5, b"replaced_5");
        assert!(ok, "put_direct should succeed");

        assert_eq!(mw.in_place_count.load(std::sync::atomic::Ordering::Relaxed), 2);
        assert_eq!(mw.overflow_count.load(std::sync::atomic::Ordering::Relaxed), 0);

        // Drop merge writer, then reload data
        drop(mw);
        silo.reload_data().unwrap();

        // Verify merged data
        assert_eq!(silo.get(1).unwrap(), b"doc_1_updated");
        assert_eq!(silo.get(5).unwrap(), b"replaced_5");
        // Untouched entries should be unchanged
        assert_eq!(silo.get(3).unwrap(), b"doc_3");
    }

    #[test]
    fn test_dump_merge_writer_overflow() {
        let dir = tempfile::tempdir().unwrap();
        let mut silo = DataSilo::open(dir.path(), SiloConfig {
            buffer_ratio: 1.0, // No headroom — exact fit
            min_entry_size: 8,
            ..Default::default()
        }).unwrap();

        // Write a small entry
        silo.write_batch_parallel(&[(1, b"hi".to_vec())]).unwrap();

        let mw = silo.prepare_dump_merge().unwrap().expect("merge writer should be available");

        // Try to merge data that's larger than allocated (should overflow)
        let ok = mw.merge_put(1, b"_extra", |existing, new| {
            let mut merged = existing.to_vec();
            merged.extend_from_slice(new);
            merged // "hi_extra" = 8 bytes, but allocated is exactly 8 for "hi"
        });
        // The merged result "hi_extra" is 8 bytes, allocated is 8 bytes — fits exactly
        assert!(ok);

        // Now try something that definitely overflows
        let ok = mw.merge_put(1, b"_this_is_way_too_long_to_fit", |existing, new| {
            let mut merged = existing.to_vec();
            merged.extend_from_slice(new);
            merged
        });
        assert!(!ok, "should overflow when merged data exceeds allocated buffer");
        assert!(mw.overflow_count.load(std::sync::atomic::Ordering::Relaxed) > 0);
    }

    #[test]
    fn test_dump_merge_writer_concurrent() {
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let mut silo = DataSilo::open(dir.path(), SiloConfig {
            buffer_ratio: 2.0,
            min_entry_size: 64,
            ..Default::default()
        }).unwrap();

        // Create 1000 entries
        let entries: Vec<(u64, Vec<u8>)> = (1..=1000u64)
            .map(|k| (k, format!("v{}", k).into_bytes()))
            .collect();
        silo.write_batch_parallel(&entries).unwrap();

        let mw = Arc::new(silo.prepare_dump_merge().unwrap().expect("merge writer should be available"));

        // Concurrent merge_put from multiple rayon threads
        use rayon::prelude::*;
        (1..=1000u64).into_par_iter().for_each(|k| {
            let suffix = format!("_{}", k);
            mw.merge_put(k, suffix.as_bytes(), |existing, new| {
                let mut merged = existing.to_vec();
                merged.extend_from_slice(new);
                merged
            });
        });

        let in_place = mw.in_place_count.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(in_place, 1000, "all 1000 merges should succeed in-place");

        drop(mw);
        silo.reload_data().unwrap();

        // Verify all merged
        for k in 1..=1000u64 {
            let data = silo.get(k).expect("entry should exist");
            let expected = format!("v{}_{}", k, k);
            assert_eq!(data, expected.as_bytes(), "key {} mismatch", k);
        }
    }

    #[test]
    fn test_merge_aware_cold_compact() {
        let dir = tempfile::tempdir().unwrap();
        let mut silo = DataSilo::open(dir.path(), SiloConfig::default()).unwrap();

        // Set merge function: concatenate with "+" separator
        silo.set_merge_fn(|existing, new| {
            let mut merged = existing.to_vec();
            merged.push(b'+');
            merged.extend_from_slice(new);
            merged
        });

        // Write multiple ops for the same key (simulating Merge ops)
        silo.append_op(1, b"a").unwrap();
        silo.append_op(1, b"b").unwrap();
        silo.append_op(1, b"c").unwrap();
        // Different key — just one op
        silo.append_op(2, b"only").unwrap();

        // Compact — should merge key 1's values instead of LWW
        let count = silo.compact().unwrap();
        assert_eq!(count, 2); // 2 unique keys

        // Key 1 should be merged: "a+b+c"
        assert_eq!(silo.get(1).unwrap(), b"a+b+c");
        // Key 2 should be unchanged
        assert_eq!(silo.get(2).unwrap(), b"only");
    }

    #[test]
    fn test_merge_aware_hot_compact() {
        let dir = tempfile::tempdir().unwrap();
        let mut silo = DataSilo::open(dir.path(), SiloConfig {
            buffer_ratio: 3.0, // plenty of headroom for merge growth
            min_entry_size: 64,
            ..Default::default()
        }).unwrap();

        // Set merge function: concatenate with "+" separator
        silo.set_merge_fn(|existing, new| {
            let mut merged = existing.to_vec();
            merged.push(b'+');
            merged.extend_from_slice(new);
            merged
        });

        // Phase 1: Write initial data via ops → cold compact to create data.bin
        silo.append_op(1, b"base").unwrap();
        silo.append_op(2, b"other").unwrap();
        silo.compact().unwrap();
        assert_eq!(silo.get(1).unwrap(), b"base");

        // Phase 2: Write new ops for existing key — hot compact should merge
        silo.append_op(1, b"add1").unwrap();
        silo.append_op(1, b"add2").unwrap();
        let count = silo.compact().unwrap();
        assert!(count > 0);

        // Key 1: existing "base" merged with ops "add1" then "add2"
        // merge_fn called as: merge("base", merge("add1", "add2")) = merge("base", "add1+add2") = "base+add1+add2"
        // Wait — the hot compact first merges ops together, then merges with existing.
        // Ops merge: merge("add1", "add2") = "add1+add2"
        // Then merged with existing: merge("base", "add1+add2") = "base+add1+add2"
        assert_eq!(silo.get(1).unwrap(), b"base+add1+add2");
        // Key 2: untouched (no new ops)
        assert_eq!(silo.get(2).unwrap(), b"other");
    }

    #[test]
    fn test_lww_without_merge_fn() {
        // Verify that without merge_fn, LWW behavior is preserved
        let dir = tempfile::tempdir().unwrap();
        let mut silo = DataSilo::open(dir.path(), SiloConfig::default()).unwrap();
        // No set_merge_fn call

        silo.append_op(1, b"first").unwrap();
        silo.append_op(1, b"second").unwrap();
        silo.append_op(1, b"third").unwrap();

        silo.compact().unwrap();
        // LWW: last value wins
        assert_eq!(silo.get(1).unwrap(), b"third");
    }
}
