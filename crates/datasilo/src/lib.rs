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
use std::sync::atomic::{AtomicU64, Ordering};

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
    ops_log: parking_lot::Mutex<OpsLog>,
    /// Bytes wasted by deleted entries and relocated updates.
    /// Tracked during hot compaction. Reset to 0 after a full rewrite.
    dead_bytes: AtomicU64,
}

unsafe impl Send for DataSilo {}
unsafe impl Sync for DataSilo {}

impl DataSilo {
    /// Open or create a DataSilo at the given directory.
    pub fn open(path: &Path, config: SiloConfig) -> io::Result<Self> {
        std::fs::create_dir_all(path)?;
        let ops_log = OpsLog::open(&path.join("ops.log"))?;

        let mut silo = Self {
            path: path.to_path_buf(),
            config,
            index_mmap: None,
            index_len: 0,
            data_mmap: None,
            data_len: 0,
            ops_log: parking_lot::Mutex::new(ops_log),
            dead_bytes: AtomicU64::new(0),
        };

        silo.load_index()?;
        silo.load_data()?;
        Ok(silo)
    }

    // ── Write path: everything goes through the ops log ─────────────────

    /// Get the ops log for direct parallel writes.
    /// Callers use `ops_log.cursor().fetch_add()` to reserve space,
    /// then write CRC32-framed ops directly to the mmap.
    pub fn ops_log(&self) -> &parking_lot::Mutex<OpsLog> {
        &self.ops_log
    }

    /// Prepare for parallel ops writes. Pre-allocates the ops log mmap.
    /// Returns a `ParallelOpsWriter` that rayon threads can use for lock-free writes.
    ///
    /// IMPORTANT: Do not call `ensure_ops_capacity` or `compact` while the
    /// `ParallelOpsWriter` is in use — the mmap must not be reallocated.
    pub fn prepare_parallel_ops(&self, estimated_bytes: u64) -> io::Result<ParallelOpsWriter> {
        let mut log = self.ops_log.lock();
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

    /// Flush the ops log mmap to disk. Call after parallel writes complete.
    pub fn flush_ops(&self) -> io::Result<()> {
        self.ops_log.lock().flush()
    }

    /// Append a single op (sequential, single-thread steady-state path).
    pub fn append_op(&self, key: u32, value: &[u8]) -> io::Result<()> {
        self.ops_log.lock().append(&SiloOp::Put { key, value: value.to_vec() })
    }

    /// Append a batch of ops sequentially. Useful for small batches in steady state.
    pub fn append_ops_batch(&self, ops: &[(u32, Vec<u8>)]) -> io::Result<()> {
        let mut log = self.ops_log.lock();
        for (key, value) in ops {
            log.append(&SiloOp::Put { key: *key, value: value.clone() })?;
        }
        log.flush()?;
        Ok(())
    }

    /// Ensure the ops log has capacity for `bytes` of additional data.
    /// Call before parallel writes to pre-allocate the mmap.
    pub fn ensure_ops_capacity(&self, bytes: u64) -> io::Result<()> {
        let mut log = self.ops_log.lock();
        let needed = log.data_size() + bytes;
        log.ensure_capacity(needed)
    }

    /// Delete an entry by key. Appends a Delete tombstone to the ops log.
    /// The entry is removed from the data file on the next compaction.
    pub fn delete(&self, key: u32) -> io::Result<()> {
        self.ops_log.lock().append(&SiloOp::Delete { key })
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
    /// Scans the ops log for the latest value of this key.
    /// Handles both Put (update) and Delete (tombstone) ops.
    pub fn get_with_ops(&self, key: u32) -> Option<Vec<u8>> {
        // Scan ops log for latest op affecting this key
        let log = self.ops_log.lock();
        let mut latest: Option<Option<Vec<u8>>> = None; // Some(Some(v)) = put, Some(None) = deleted
        let _ = log.for_each_ops(|op| {
            match op {
                SiloOp::Put { key: k, value } if k == key => {
                    latest = Some(Some(value));
                }
                SiloOp::Delete { key: k } if k == key => {
                    latest = Some(None); // tombstone
                }
                _ => {}
            }
        });
        match latest {
            Some(Some(v)) => Some(v),   // latest op was a put
            Some(None) => None,          // latest op was a delete
            None => {
                // No ops for this key — fall back to data file
                self.get(key).map(|s| s.to_vec())
            }
        }
    }

    // ── Metadata ────────────────────────────────────────────────────────

    pub fn index_capacity(&self) -> u32 { self.index_len }
    pub fn data_bytes(&self) -> u64 { self.data_len }
    pub fn ops_size(&self) -> u64 { self.ops_log.lock().data_size() }
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

    /// Check if there are uncompacted ops.
    pub fn has_ops(&self) -> bool {
        self.ops_log.lock().data_size() > 0
    }

    // ── Compaction ──────────────────────────────────────────────────────

    /// Compact: merge ops into the data file.
    ///
    /// Two modes:
    /// - **Cold** (no existing data file): scan ops → build index → rename ops.log → data.bin
    /// - **Hot** (existing data file): apply ops in-place where they fit, overflow to end
    pub fn compact(&mut self) -> io::Result<u64> {
        let ops_size = self.ops_log.lock().data_size();
        if ops_size == 0 { return Ok(0); }

        let has_data = self.data_mmap.is_some() && self.index_len > 0;
        if has_data {
            self.compact_hot()
        } else {
            self.compact_cold()
        }
    }

    /// Cold compaction: no existing data file.
    /// Scan ops log for last value per key, write data file + index.
    /// Deleted keys (tombstones) are excluded from the output.
    fn compact_cold(&mut self) -> io::Result<u64> {
        // Collect last value per key from ops log (last-write-wins).
        // Deletes remove the entry entirely (tombstone).
        let mut entries: std::collections::HashMap<u32, Vec<u8>> = std::collections::HashMap::new();
        let mut max_key: u32 = 0;
        {
            let log = self.ops_log.lock();
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

        // Clear ops log
        self.ops_log.lock().truncate()?;

        eprintln!("DataSilo: cold compacted {} entries, {:.1}MB data, {:.1}MB index",
            count, offset as f64 / 1e6,
            (index_count * INDEX_ENTRY_SIZE) as f64 / 1e6);
        Ok(count)
    }

    /// Hot compaction: existing data file with pre-allocated buffer slots.
    /// For each op, write in-place if it fits in the allocated slot, otherwise overflow.
    /// Delete tombstones zero out the index entry (length=0, allocated=0).
    fn compact_hot(&mut self) -> io::Result<u64> {
        // Collect last value per key from ops log (deletes stored as None)
        let mut ops: std::collections::HashMap<u32, Option<Vec<u8>>> = std::collections::HashMap::new();
        let mut max_key: u32 = 0;
        {
            let log = self.ops_log.lock();
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
        let mut in_place = 0u64;
        let mut deleted = 0u64;
        let mut overflows: Vec<(u32, Vec<u8>)> = Vec::new();

        // Drop read-only data mmap so we can open as writable
        self.data_mmap = None;

        // Open data file as writable mmap for in-place updates
        let data_path = self.path.join("data.bin");
        let mut data_mmap_mut = {
            let f = OpenOptions::new().read(true).write(true).open(&data_path)?;
            unsafe { memmap2::MmapMut::map_mut(&f)? }
        };

        // Phase 1: In-place updates for ops that fit, and tombstone deletes
        for (&key, value_opt) in &ops {
            // Handle deletes: zero out the index entry
            let value = match value_opt {
                Some(v) => v,
                None => {
                    // Tombstone: clear the index entry so get() returns None
                    if key < self.index_len {
                        if let Some(old_entry) = self.index_entry(key) {
                            if old_entry.allocated > 0 {
                                self.dead_bytes.fetch_add(old_entry.allocated as u64, Ordering::Relaxed);
                            }
                        }
                        let zero_entry = IndexEntry { offset: 0, length: 0, allocated: 0 };
                        if let Some(ref mut index_mmap) = self.index_mmap {
                            let pos = key as usize * INDEX_ENTRY_SIZE;
                            if pos + INDEX_ENTRY_SIZE <= index_mmap.len() {
                                let bytes: [u8; INDEX_ENTRY_SIZE] = unsafe { std::mem::transmute(zero_entry) };
                                index_mmap[pos..pos + INDEX_ENTRY_SIZE].copy_from_slice(&bytes);
                            }
                        }
                    }
                    deleted += 1;
                    continue;
                }
            };

            if key >= self.index_len {
                overflows.push((key, value.clone()));
                continue;
            }
            let entry = match self.index_entry(key) {
                Some(e) if e.allocated > 0 => e,
                _ => { overflows.push((key, value.clone())); continue; }
            };

            if value.len() as u32 <= entry.allocated {
                // Fits! Write in-place
                let start = entry.offset as usize;
                if start + value.len() <= data_mmap_mut.len() {
                    data_mmap_mut[start..start + value.len()].copy_from_slice(value);
                    let new_entry = IndexEntry {
                        offset: entry.offset,
                        length: value.len() as u32,
                        allocated: entry.allocated,
                    };
                    if let Some(ref mut index_mmap) = self.index_mmap {
                        let pos = key as usize * INDEX_ENTRY_SIZE;
                        let bytes: [u8; INDEX_ENTRY_SIZE] = unsafe { std::mem::transmute(new_entry) };
                        index_mmap[pos..pos + INDEX_ENTRY_SIZE].copy_from_slice(&bytes);
                    }
                    in_place += 1;
                } else {
                    // Old slot becomes dead space
                    self.dead_bytes.fetch_add(entry.allocated as u64, Ordering::Relaxed);
                    overflows.push((key, value.clone()));
                }
            } else {
                // Doesn't fit — old slot becomes dead space, value relocates to end
                self.dead_bytes.fetch_add(entry.allocated as u64, Ordering::Relaxed);
                overflows.push((key, value.clone()));
            }
        }

        data_mmap_mut.flush()?;
        drop(data_mmap_mut);

        // Phase 2: Handle overflows — append to end of data file + extend index if needed
        if !overflows.is_empty() {
            let data_file = OpenOptions::new().write(true).append(true).open(&data_path)?;
            let mut writer = io::BufWriter::with_capacity(1 << 20, data_file);
            let mut offset = self.data_len;

            // Extend index if we have keys beyond current capacity
            let new_max = overflows.iter().map(|(k, _)| *k).max().unwrap_or(0);
            if new_max >= self.index_len {
                let new_count = new_max as usize + 1;
                let index_path = self.path.join("index.bin");
                self.index_mmap = None;
                let index_file = OpenOptions::new().read(true).write(true).open(&index_path)?;
                index_file.set_len((new_count * INDEX_ENTRY_SIZE) as u64)?;
                let mmap = unsafe { memmap2::MmapMut::map_mut(&index_file)? };
                self.index_mmap = Some(mmap);
                self.index_len = new_count as u32;
            }

            for (key, value) in &overflows {
                let len = value.len() as u32;
                let allocated = ((len as f32 * self.config.buffer_ratio).ceil() as u32)
                    .max(self.config.min_entry_size);

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

                let entry = IndexEntry { offset, length: len, allocated };
                let pos = *key as usize * INDEX_ENTRY_SIZE;
                if let Some(ref mut index_mmap) = self.index_mmap {
                    if pos + INDEX_ENTRY_SIZE <= index_mmap.len() {
                        let bytes: [u8; INDEX_ENTRY_SIZE] = unsafe { std::mem::transmute(entry) };
                        index_mmap[pos..pos + INDEX_ENTRY_SIZE].copy_from_slice(&bytes);
                    }
                }

                offset += allocated as u64;
            }

            writer.flush()?;
            drop(writer);
            self.data_len = offset;
        }

        // Flush index
        if let Some(ref index_mmap) = self.index_mmap {
            index_mmap.flush()?;
        }

        // Reload read-only data mmap
        self.load_data()?;

        // Clear ops log
        self.ops_log.lock().truncate()?;

        eprintln!("DataSilo: hot compacted {} ops ({} in-place, {} overflow)",
            count, in_place, overflows.len());
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
            silo.ops_log.lock().flush().unwrap();
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
}
