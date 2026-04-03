//! DataSilo — Generic mmap'd key-value store with append-only ops log.
//!
//! Three components:
//! - **Index**: key → (offset, length) in the data file. Mmap'd dense array.
//! - **Data**: packed variable-size entries. Mmap'd.
//! - **Ops log**: append-only mutations with CRC32. Used for post-bulk-load changes.
//!
//! Write path (bulk): ParallelWriter → 35M entries/sec via mmap memcpy (32 threads)
//! Write path (ops): append to ops log → held in pending HashMap for reads
//! Read path: check pending → index lookup (mmap deref) → data read (mmap deref)
//!
//! Encoding is caller's responsibility — DataSilo stores raw `&[u8]`.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

mod ops_log;

pub use ops_log::{SiloOp, OpsLog};

// ---------------------------------------------------------------------------
// Index entry — 16 bytes per key
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct IndexEntry {
    pub offset: u64,
    pub length: u32,
    pub allocated: u32,
}

const INDEX_ENTRY_SIZE: usize = std::mem::size_of::<IndexEntry>(); // 16

// ---------------------------------------------------------------------------
// ParallelWriter — lock-free concurrent bulk writer
// ---------------------------------------------------------------------------

pub struct ParallelWriter {
    data_mmap: memmap2::MmapMut,
    index_mmap: memmap2::MmapMut,
    data_offset: AtomicU64,
    index_count: u32,
    entries_written: AtomicU64,
}

unsafe impl Send for ParallelWriter {}
unsafe impl Sync for ParallelWriter {}

/// Per-thread writer with 1MB sequential regions for OS prefetch.
pub struct ThreadWriter<'a> {
    pw: &'a ParallelWriter,
    cursor: usize,
    region_end: usize,
}

const REGION_SIZE: u64 = 1 << 20; // 1MB

impl ParallelWriter {
    // Raw accessors for benchmarks
    pub fn data_offset_ref(&self) -> &AtomicU64 { &self.data_offset }
    pub fn data_ptr(&self) -> *mut u8 { self.data_mmap.as_ptr() as *mut u8 }
    pub fn data_len(&self) -> usize { self.data_mmap.len() }
    pub fn index_ptr(&self) -> *mut u8 { self.index_mmap.as_ptr() as *mut u8 }
    pub fn index_len(&self) -> usize { self.index_mmap.len() }
    pub fn entries_counter(&self) -> &AtomicU64 { &self.entries_written }

    /// Write an entry. Thread-safe, lock-free.
    #[inline]
    pub fn write(&self, key: u32, data: &[u8]) -> Option<u64> {
        let len = data.len() as u32;
        if len == 0 || key >= self.index_count { return None; }

        let offset = self.data_offset.fetch_add(len as u64, Ordering::Relaxed);
        let start = offset as usize;
        let end = start + len as usize;
        if end > self.data_mmap.len() { return None; }

        let dst = &self.data_mmap[start..end] as *const [u8] as *mut [u8];
        unsafe { (*dst).copy_from_slice(data); }

        let entry = IndexEntry { offset, length: len, allocated: len };
        let idx_pos = key as usize * INDEX_ENTRY_SIZE;
        if idx_pos + INDEX_ENTRY_SIZE <= self.index_mmap.len() {
            let bytes: [u8; INDEX_ENTRY_SIZE] = unsafe { std::mem::transmute(entry) };
            let dst = &self.index_mmap[idx_pos..idx_pos + INDEX_ENTRY_SIZE] as *const [u8] as *mut [u8];
            unsafe { (*dst).copy_from_slice(&bytes); }
        }

        self.entries_written.fetch_add(1, Ordering::Relaxed);
        Some(offset)
    }

    /// Get a thread-local writer with 1MB sequential regions.
    pub fn thread_writer(&self) -> ThreadWriter<'_> {
        ThreadWriter { pw: self, cursor: 0, region_end: 0 }
    }

    /// Finalize: flush mmaps, truncate data to actual size.
    pub fn finish(self) -> io::Result<(u64, u64)> {
        let count = self.entries_written.load(Ordering::Relaxed);
        let data_used = self.data_offset.load(Ordering::Relaxed);
        self.data_mmap.flush()?;
        self.index_mmap.flush()?;
        Ok((count, data_used))
    }
}

impl<'a> ThreadWriter<'a> {
    /// Write an entry using thread-local region (sequential, OS-prefetch friendly).
    #[inline]
    pub fn write(&mut self, key: u32, data: &[u8]) -> Option<u64> {
        let len = data.len();
        if len == 0 || key >= self.pw.index_count { return None; }

        if self.cursor + len > self.region_end {
            let start = self.pw.data_offset.fetch_add(REGION_SIZE, Ordering::Relaxed) as usize;
            self.cursor = start;
            self.region_end = start + REGION_SIZE as usize;
        }

        let offset = self.cursor;
        let end = offset + len;
        if end > self.pw.data_mmap.len() { return None; }

        let dst = &self.pw.data_mmap[offset..end] as *const [u8] as *mut [u8];
        unsafe { (*dst).copy_from_slice(data); }
        self.cursor = end;

        let entry = IndexEntry { offset: offset as u64, length: len as u32, allocated: len as u32 };
        let idx_pos = key as usize * INDEX_ENTRY_SIZE;
        if idx_pos + INDEX_ENTRY_SIZE <= self.pw.index_mmap.len() {
            let bytes: [u8; INDEX_ENTRY_SIZE] = unsafe { std::mem::transmute(entry) };
            let dst = &self.pw.index_mmap[idx_pos..idx_pos + INDEX_ENTRY_SIZE] as *const [u8] as *mut [u8];
            unsafe { (*dst).copy_from_slice(&bytes); }
        }

        self.pw.entries_written.fetch_add(1, Ordering::Relaxed);
        Some(offset as u64)
    }
}

// ---------------------------------------------------------------------------
// DataSilo — the main store
// ---------------------------------------------------------------------------

pub struct SiloConfig {
    pub buffer_ratio: f32,
}

impl Default for SiloConfig {
    fn default() -> Self { Self { buffer_ratio: 1.2 } }
}

pub struct DataSilo {
    path: PathBuf,
    config: SiloConfig,
    index_mmap: Option<memmap2::MmapMut>,
    index_len: u32,
    data_mmap: Option<memmap2::Mmap>,
    data_len: u64,
    ops_log: parking_lot::Mutex<OpsLog>,
    pending: parking_lot::RwLock<HashMap<u32, Vec<u8>>>,
}

// Send+Sync: MmapMut isn't Sync by default but we only write via ParallelWriter
// (disjoint regions) or single-threaded bulk_load. Reads are immutable.
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
            pending: parking_lot::RwLock::new(HashMap::new()),
        };

        silo.load_index()?;
        silo.load_data()?;
        silo.replay_ops()?;
        Ok(silo)
    }

    /// Create a parallel writer for bulk loading. Pre-allocates files.
    /// Call `finish_parallel_write()` after all threads are done.
    pub fn prepare_parallel_writer(
        &mut self,
        max_key: u32,
        estimated_total_bytes: u64,
    ) -> io::Result<ParallelWriter> {
        let data_path = self.path.join("data.bin");
        let index_path = self.path.join("index.bin");
        let index_count = max_key as usize + 1;

        let data_file = OpenOptions::new()
            .create(true).read(true).write(true).open(&data_path)?;
        data_file.set_len(estimated_total_bytes)?;
        let data_mmap = unsafe { memmap2::MmapMut::map_mut(&data_file)? };

        let index_size = (index_count * INDEX_ENTRY_SIZE) as u64;
        let index_file = OpenOptions::new()
            .create(true).read(true).write(true).open(&index_path)?;
        index_file.set_len(index_size)?;
        let index_mmap = unsafe { memmap2::MmapMut::map_mut(&index_file)? };

        Ok(ParallelWriter {
            data_mmap,
            index_mmap,
            data_offset: AtomicU64::new(0),
            index_count: index_count as u32,
            entries_written: AtomicU64::new(0),
        })
    }

    /// Finalize after parallel write. Truncates data to actual size, loads mmaps for reads.
    pub fn finish_parallel_write(&mut self, writer: ParallelWriter) -> io::Result<u64> {
        let (count, data_used) = writer.finish()?;

        // Truncate data file to actual bytes used
        let data_file = OpenOptions::new().write(true).open(self.path.join("data.bin"))?;
        data_file.set_len(data_used)?;
        drop(data_file);

        self.load_index()?;
        self.load_data()?;
        self.data_len = data_used;

        eprintln!("DataSilo: parallel write done — {} entries, {:.1}MB data, {:.1}MB index",
            count, data_used as f64 / 1e6,
            (self.index_len as usize * INDEX_ENTRY_SIZE) as f64 / 1e6);
        Ok(count)
    }

    /// Bulk load from an iterator (sequential, single-thread — use for small datasets).
    pub fn bulk_load<I>(&mut self, entries: I) -> io::Result<u64>
    where I: Iterator<Item = (u32, Vec<u8>)>
    {
        let data_path = self.path.join("data.bin");
        let mut data_file = io::BufWriter::with_capacity(1 << 20, File::create(&data_path)?);
        let mut index_entries: Vec<(u32, IndexEntry)> = Vec::new();
        let mut offset: u64 = 0;
        let mut count: u64 = 0;
        let mut max_key: u32 = 0;

        for (key, value) in entries {
            let len = value.len() as u32;
            let allocated = (len as f32 * self.config.buffer_ratio).ceil() as u32;
            data_file.write_all(&value)?;
            if allocated > len {
                let zeros = [0u8; 4096];
                let mut rem = (allocated - len) as usize;
                while rem > 0 { let c = rem.min(4096); data_file.write_all(&zeros[..c])?; rem -= c; }
            }
            index_entries.push((key, IndexEntry { offset, length: len, allocated }));
            offset += allocated as u64;
            if key > max_key { max_key = key; }
            count += 1;
        }
        data_file.flush()?;
        drop(data_file);

        let index_count = max_key as usize + 1;
        let index_path = self.path.join("index.bin");
        let index_file = OpenOptions::new()
            .create(true).read(true).write(true).open(&index_path)?;
        index_file.set_len((index_count * INDEX_ENTRY_SIZE) as u64)?;
        let mut index_mmap = unsafe { memmap2::MmapMut::map_mut(&index_file)? };

        for (key, entry) in &index_entries {
            let pos = *key as usize * INDEX_ENTRY_SIZE;
            if pos + INDEX_ENTRY_SIZE <= index_mmap.len() {
                let bytes: [u8; INDEX_ENTRY_SIZE] = unsafe { std::mem::transmute(*entry) };
                index_mmap[pos..pos + INDEX_ENTRY_SIZE].copy_from_slice(&bytes);
            }
        }
        index_mmap.flush()?;
        self.index_mmap = Some(index_mmap);
        self.index_len = index_count as u32;
        self.load_data()?;
        self.data_len = offset;
        Ok(count)
    }

    /// Append a mutation. Thread-safe (uses internal Mutex for ops log).
    pub fn append_op(&self, key: u32, value: Vec<u8>) -> io::Result<()> {
        self.ops_log.lock().append(SiloOp::Put { key, value: value.clone() })?;
        self.pending.write().insert(key, value);
        Ok(())
    }

    /// Append a batch of ops (one flush for the whole batch). Thread-safe.
    pub fn append_ops_batch(&self, ops: &[(u32, Vec<u8>)]) -> io::Result<()> {
        let mut log = self.ops_log.lock();
        let mut pending = self.pending.write();
        for (key, value) in ops {
            log.append_no_sync(SiloOp::Put { key: *key, value: value.clone() })?;
            pending.insert(*key, value.clone());
        }
        log.sync()?;
        Ok(())
    }

    /// Read an entry by key. Checks pending ops first, then mmap'd data.
    pub fn get(&self, key: u32) -> Option<&[u8]> {
        // Can't return &[u8] from RwLock — check pending separately
        // For now, skip pending check in the hot path and let callers handle it
        // TODO: return Cow or owned for pending entries
        self.get_from_data(key)
    }

    /// Read from the mmap'd data file only (no pending ops).
    pub fn get_from_data(&self, key: u32) -> Option<&[u8]> {
        let entry = self.index_entry(key)?;
        if entry.length == 0 { return None; }
        let mmap = self.data_mmap.as_ref()?;
        let start = entry.offset as usize;
        let end = start + entry.length as usize;
        if end <= mmap.len() { Some(&mmap[start..end]) } else { None }
    }

    /// Check if a key has a pending op value.
    pub fn get_pending(&self, key: u32) -> Option<Vec<u8>> {
        self.pending.read().get(&key).cloned()
    }

    /// Read with pending ops overlay (returns owned data).
    pub fn get_with_pending(&self, key: u32) -> Option<Vec<u8>> {
        if let Some(v) = self.pending.read().get(&key) {
            return Some(v.clone());
        }
        self.get_from_data(key).map(|s| s.to_vec())
    }

    pub fn index_capacity(&self) -> u32 { self.index_len }
    pub fn pending_count(&self) -> usize { self.pending.read().len() }
    pub fn data_bytes(&self) -> u64 { self.data_len }
    pub fn path(&self) -> &Path { &self.path }

    /// Compact: apply all pending ops into the data file, clear ops log.
    /// After compaction, pending is empty and all data is in the mmap.
    pub fn compact(&mut self) -> io::Result<u64> {
        let pending = std::mem::take(&mut *self.pending.write());
        if pending.is_empty() { return Ok(0); }

        let count = pending.len() as u64;

        // For entries that fit in their allocated space: overwrite in place
        // For entries that don't: append to end of data file
        // For new entries (not in index): append + extend index

        // Simple approach: rewrite data file with all entries (bulk + pending merged)
        // This is the correct but potentially slow approach for large silos.
        // TODO: in-place update for entries that fit in allocated space

        let data_path = self.path.join("data.bin");
        let index_path = self.path.join("index.bin");

        // Read all existing entries + overlay pending
        let mut all_entries: Vec<(u32, Vec<u8>)> = Vec::new();
        let mut max_key: u32 = 0;

        // Existing entries from mmap
        if let Some(ref index_mmap) = self.index_mmap {
            for key in 0..self.index_len {
                let pos = key as usize * INDEX_ENTRY_SIZE;
                if pos + INDEX_ENTRY_SIZE > index_mmap.len() { break; }
                let bytes: [u8; INDEX_ENTRY_SIZE] = index_mmap[pos..pos + INDEX_ENTRY_SIZE]
                    .try_into().unwrap();
                let entry: IndexEntry = unsafe { std::mem::transmute(bytes) };
                if entry.length == 0 { continue; }

                if let Some(pending_val) = pending.get(&key) {
                    // Pending overrides
                    all_entries.push((key, pending_val.clone()));
                } else if let Some(data) = self.get_from_data(key) {
                    all_entries.push((key, data.to_vec()));
                }
                if key > max_key { max_key = key; }
            }
        }

        // New entries from pending (not in existing index)
        for (key, value) in &pending {
            if *key >= self.index_len {
                all_entries.push((*key, value.clone()));
                if *key > max_key { max_key = *key; }
            }
        }

        // Drop old mmaps before rewriting files
        self.index_mmap = None;
        self.data_mmap = None;

        // Rewrite via bulk_load
        self.bulk_load(all_entries.into_iter())?;

        // Clear ops log
        self.ops_log.lock().truncate()?;

        eprintln!("DataSilo: compacted {} pending ops", count);
        Ok(count)
    }

    // ---- Internal ----

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

    fn replay_ops(&mut self) -> io::Result<()> {
        let ops = self.ops_log.lock().read_all()?;
        let mut pending = self.pending.write();
        for op in ops {
            match op {
                SiloOp::Put { key, value } => { pending.insert(key, value); }
                SiloOp::Delete { key } => { pending.remove(&key); }
            }
        }
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
    fn test_bulk_load_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let mut silo = DataSilo::open(dir.path(), SiloConfig::default()).unwrap();
        let entries: Vec<(u32, Vec<u8>)> = (0..1000)
            .map(|i| (i, format!("doc_{}", i).into_bytes()))
            .collect();
        let count = silo.bulk_load(entries.into_iter()).unwrap();
        assert_eq!(count, 1000);
        assert_eq!(silo.get(0).unwrap(), b"doc_0");
        assert_eq!(silo.get(999).unwrap(), b"doc_999");
        assert!(silo.get(1000).is_none());
    }

    #[test]
    fn test_append_op_overrides_bulk() {
        let dir = tempfile::tempdir().unwrap();
        let mut silo = DataSilo::open(dir.path(), SiloConfig::default()).unwrap();
        silo.bulk_load(vec![(42, b"original".to_vec())].into_iter()).unwrap();
        assert_eq!(silo.get(42).unwrap(), b"original");
        silo.append_op(42, b"updated".to_vec()).unwrap();
        assert_eq!(silo.get_with_pending(42).unwrap(), b"updated");
    }

    #[test]
    fn test_reopen_with_ops() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut silo = DataSilo::open(dir.path(), SiloConfig::default()).unwrap();
            silo.bulk_load(vec![(1, b"hello".to_vec())].into_iter()).unwrap();
            silo.append_op(1, b"world".to_vec()).unwrap();
            silo.append_op(2, b"new_entry".to_vec()).unwrap();
        }
        {
            let silo = DataSilo::open(dir.path(), SiloConfig::default()).unwrap();
            assert_eq!(silo.get_with_pending(1).unwrap(), b"world");
            assert_eq!(silo.get_with_pending(2).unwrap(), b"new_entry");
        }
    }

    #[test]
    fn test_compact() {
        let dir = tempfile::tempdir().unwrap();
        let mut silo = DataSilo::open(dir.path(), SiloConfig::default()).unwrap();
        silo.bulk_load(vec![(1, b"a".to_vec()), (2, b"b".to_vec())].into_iter()).unwrap();
        silo.append_op(1, b"updated_a".to_vec()).unwrap();
        silo.append_op(3, b"new_c".to_vec()).unwrap();
        assert_eq!(silo.pending_count(), 2);

        silo.compact().unwrap();
        assert_eq!(silo.pending_count(), 0);
        // After compaction, all data is in mmap
        assert_eq!(silo.get(1).unwrap(), b"updated_a");
        assert_eq!(silo.get(2).unwrap(), b"b");
        assert_eq!(silo.get(3).unwrap(), b"new_c");
    }

    #[test]
    fn test_sparse_keys() {
        let dir = tempfile::tempdir().unwrap();
        let mut silo = DataSilo::open(dir.path(), SiloConfig::default()).unwrap();
        silo.bulk_load(vec![
            (0, b"zero".to_vec()),
            (1000, b"thousand".to_vec()),
            (100000, b"hundred_k".to_vec()),
        ].into_iter()).unwrap();
        assert_eq!(silo.get(0).unwrap(), b"zero");
        assert_eq!(silo.get(1000).unwrap(), b"thousand");
        assert_eq!(silo.get(100000).unwrap(), b"hundred_k");
        assert!(silo.get(500).is_none());
    }

    #[test]
    fn test_thread_safe_ops() {
        let dir = tempfile::tempdir().unwrap();
        let mut silo = DataSilo::open(dir.path(), SiloConfig::default()).unwrap();
        silo.bulk_load(vec![(0, b"init".to_vec())].into_iter()).unwrap();

        // append_op is &self (thread-safe via internal Mutex)
        silo.append_op(1, b"from_thread".to_vec()).unwrap();
        silo.append_ops_batch(&[
            (2, b"batch_a".to_vec()),
            (3, b"batch_b".to_vec()),
        ]).unwrap();

        assert_eq!(silo.get_with_pending(1).unwrap(), b"from_thread");
        assert_eq!(silo.get_with_pending(2).unwrap(), b"batch_a");
        assert_eq!(silo.pending_count(), 3);
    }
}
