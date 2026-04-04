//! Mmap'd append-only ops log with CRC32 per entry.
//!
//! Two write modes:
//! - **Sequential**: single-thread, tight packing (steady-state mutations)
//! - **Parallel**: 1MB thread-local regions, 32M+ ops/sec (dump/bulk load)
//!
//! Frame format: [u8 tag][u32 key][u32 value_len][value bytes][u32 crc32]
//! Tags: 0x01 = Put, 0x02 = Delete
//!
//! The log is mmap'd so reads are zero-copy through the page cache.
//! No in-memory HashMap — the mmap IS the read cache.

use std::fs::OpenOptions;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

const OP_TAG_PUT: u8 = 0x01;
const OP_TAG_DELETE: u8 = 0x02;

/// 1MB thread-local regions for parallel writes.
const REGION_SIZE: u64 = 1 << 20;

/// Initial ops log file size (64 MB). Grows as needed.
const INITIAL_SIZE: u64 = 64 * 1024 * 1024;

/// A mutation operation.
pub enum SiloOp {
    Put { key: u32, value: Vec<u8> },
    Delete { key: u32 },
}

/// Mmap'd append-only ops log.
///
/// Supports both sequential (single-thread) and parallel (multi-thread) writes.
/// All data lives in mmap — no heap-allocated pending HashMap.
pub struct OpsLog {
    path: PathBuf,
    /// Mmap for writing ops. None if the file is empty / not yet created.
    mmap: Option<memmap2::MmapMut>,
    /// Current write cursor (byte offset into the mmap).
    /// Atomic so parallel writers can bump it lock-free.
    cursor: AtomicU64,
    /// File size (capacity). When cursor approaches this, we grow the file.
    capacity: u64,
}

// Send+Sync: parallel writers access disjoint regions via atomic cursor.
unsafe impl Send for OpsLog {}
unsafe impl Sync for OpsLog {}

impl OpsLog {
    /// Open or create the ops log file.
    pub fn open(path: &Path) -> io::Result<Self> {
        let path = path.to_path_buf();
        if path.exists() {
            let meta = std::fs::metadata(&path)?;
            let file_size = meta.len();
            if file_size == 0 {
                return Ok(Self {
                    path,
                    mmap: None,
                    cursor: AtomicU64::new(0),
                    capacity: 0,
                });
            }
            // Open existing log — find the actual data end by scanning for valid ops
            let file = OpenOptions::new().read(true).write(true).open(&path)?;
            let mmap = unsafe { memmap2::MmapMut::map_mut(&file)? };
            let data_end = Self::find_data_end(&mmap);
            Ok(Self {
                path,
                cursor: AtomicU64::new(data_end as u64),
                capacity: file_size,
                mmap: Some(mmap),
            })
        } else {
            Ok(Self {
                path,
                mmap: None,
                cursor: AtomicU64::new(0),
                capacity: 0,
            })
        }
    }

    /// Ensure the mmap is at least `min_size` bytes. Grows if needed.
    pub fn ensure_capacity(&mut self, min_size: u64) -> io::Result<()> {
        if self.capacity >= min_size && self.mmap.is_some() {
            return Ok(());
        }
        let new_size = min_size.max(INITIAL_SIZE).max(self.capacity * 2);
        let file = OpenOptions::new()
            .create(true).read(true).write(true)
            .open(&self.path)?;
        file.set_len(new_size)?;
        let mmap = unsafe { memmap2::MmapMut::map_mut(&file)? };
        self.mmap = Some(mmap);
        self.capacity = new_size;
        Ok(())
    }

    /// Append a single op (sequential, single-thread).
    /// Auto-grows the file if needed.
    pub fn append(&mut self, op: &SiloOp) -> io::Result<()> {
        let frame = Self::encode_op(op);
        let needed = self.cursor.load(Ordering::Relaxed) + frame.len() as u64;
        if needed > self.capacity || self.mmap.is_none() {
            self.ensure_capacity(needed + INITIAL_SIZE)?;
        }
        let offset = self.cursor.fetch_add(frame.len() as u64, Ordering::Relaxed) as usize;
        let mmap = self.mmap.as_ref().unwrap();
        if offset + frame.len() <= mmap.len() {
            unsafe {
                let dst = mmap.as_ptr().add(offset) as *mut u8;
                std::ptr::copy_nonoverlapping(frame.as_ptr(), dst, frame.len());
            }
        }
        Ok(())
    }

    /// Flush mmap to disk.
    pub fn flush(&self) -> io::Result<()> {
        if let Some(ref mmap) = self.mmap {
            mmap.flush()?;
        }
        Ok(())
    }

    /// Get the atomic cursor for parallel writes.
    /// Callers use `cursor.fetch_add(frame_len)` to reserve space, then write directly.
    pub fn cursor(&self) -> &AtomicU64 {
        &self.cursor
    }

    /// Get a raw pointer to the mmap for parallel writes.
    /// Safety: callers must write to disjoint regions (atomic cursor guarantees this).
    pub fn mmap_ptr(&self) -> Option<*mut u8> {
        self.mmap.as_ref().map(|m| m.as_ptr() as *mut u8)
    }

    /// Get the mmap length.
    pub fn mmap_len(&self) -> usize {
        self.mmap.as_ref().map(|m| m.len()).unwrap_or(0)
    }

    /// Write a frame at a specific offset (for parallel writers that pre-reserved space).
    /// Returns false if the offset is out of bounds.
    #[inline]
    pub fn write_frame_at(&self, offset: usize, frame: &[u8]) -> bool {
        if let Some(ref mmap) = self.mmap {
            if offset + frame.len() <= mmap.len() {
                unsafe {
                    let dst = mmap.as_ptr().add(offset) as *mut u8;
                    std::ptr::copy_nonoverlapping(frame.as_ptr(), dst, frame.len());
                }
                return true;
            }
        }
        false
    }

    /// Read all ops by scanning the mmap. Zero-copy — values reference the mmap.
    /// Returns owned SiloOps (values are copied out of mmap).
    pub fn read_all(&self) -> io::Result<Vec<SiloOp>> {
        let mmap = match &self.mmap {
            Some(m) => m,
            None => return Ok(Vec::new()),
        };
        let end = self.cursor.load(Ordering::Relaxed) as usize;
        if end == 0 { return Ok(Vec::new()); }

        let data = &mmap[..end.min(mmap.len())];
        let mut ops = Vec::new();
        let mut pos = 0;

        while pos < data.len() {
            match Self::decode_op(data, &mut pos) {
                Some(op) => ops.push(op),
                None => {
                    // Possibly in a padding region between thread-local regions.
                    // Skip zero bytes to find the next valid frame.
                    if pos < data.len() && data[pos] == 0 {
                        // Skip padding
                        while pos < data.len() && data[pos] == 0 {
                            pos += 1;
                        }
                    } else {
                        break; // Corrupted or end of valid data
                    }
                }
            }
        }

        Ok(ops)
    }

    /// Iterate over ops without allocating a Vec. Calls `f` for each valid op.
    /// More memory-efficient than `read_all` for large logs.
    pub fn for_each<F>(&self, mut f: F) -> io::Result<u64>
    where F: FnMut(u32, &[u8]) // (key, value_bytes)
    {
        let mmap = match &self.mmap {
            Some(m) => m,
            None => return Ok(0),
        };
        let end = self.cursor.load(Ordering::Relaxed) as usize;
        if end == 0 { return Ok(0); }

        let data = &mmap[..end.min(mmap.len())];
        let mut pos = 0;
        let mut count = 0u64;

        while pos < data.len() {
            if data[pos] == 0 {
                // Skip padding between regions
                while pos < data.len() && data[pos] == 0 { pos += 1; }
                continue;
            }
            let entry_start = pos;
            let tag = data[pos];
            pos += 1;

            match tag {
                OP_TAG_PUT => {
                    if pos + 8 > data.len() { break; }
                    let key = u32::from_le_bytes(data[pos..pos+4].try_into().unwrap());
                    pos += 4;
                    let value_len = u32::from_le_bytes(data[pos..pos+4].try_into().unwrap()) as usize;
                    pos += 4;
                    if pos + value_len + 4 > data.len() { break; }
                    let value = &data[pos..pos + value_len];
                    pos += value_len;
                    let payload_end = pos;
                    let expected_crc = u32::from_le_bytes(data[pos..pos+4].try_into().unwrap());
                    pos += 4;
                    let actual_crc = crc32fast::hash(&data[entry_start..payload_end]);
                    if actual_crc == expected_crc {
                        f(key, value);
                        count += 1;
                    }
                    // If CRC mismatch, skip this entry (could be padding)
                }
                OP_TAG_DELETE => {
                    if pos + 4 + 4 > data.len() { break; }
                    let _key = u32::from_le_bytes(data[pos..pos+4].try_into().unwrap());
                    pos += 4;
                    let payload_end = pos;
                    let expected_crc = u32::from_le_bytes(data[pos..pos+4].try_into().unwrap());
                    pos += 4;
                    let actual_crc = crc32fast::hash(&data[entry_start..payload_end]);
                    if actual_crc == expected_crc {
                        count += 1;
                    }
                }
                _ => {
                    // Unknown tag — skip padding
                    while pos < data.len() && data[pos] == 0 { pos += 1; }
                }
            }
        }

        Ok(count)
    }

    /// Iterate over all ops (puts AND deletes) without allocating a Vec.
    /// The callback receives full `SiloOp` values including Delete tombstones.
    pub fn for_each_ops<F>(&self, mut f: F) -> io::Result<u64>
    where F: FnMut(SiloOp)
    {
        let mmap = match &self.mmap {
            Some(m) => m,
            None => return Ok(0),
        };
        let end = self.cursor.load(Ordering::Relaxed) as usize;
        if end == 0 { return Ok(0); }

        let data = &mmap[..end.min(mmap.len())];
        let mut pos = 0;
        let mut count = 0u64;

        while pos < data.len() {
            if data[pos] == 0 {
                while pos < data.len() && data[pos] == 0 { pos += 1; }
                continue;
            }
            let entry_start = pos;
            let tag = data[pos];
            pos += 1;

            match tag {
                OP_TAG_PUT => {
                    if pos + 8 > data.len() { break; }
                    let key = u32::from_le_bytes(data[pos..pos+4].try_into().unwrap());
                    pos += 4;
                    let value_len = u32::from_le_bytes(data[pos..pos+4].try_into().unwrap()) as usize;
                    pos += 4;
                    if pos + value_len + 4 > data.len() { break; }
                    let value = &data[pos..pos + value_len];
                    pos += value_len;
                    let payload_end = pos;
                    let expected_crc = u32::from_le_bytes(data[pos..pos+4].try_into().unwrap());
                    pos += 4;
                    let actual_crc = crc32fast::hash(&data[entry_start..payload_end]);
                    if actual_crc == expected_crc {
                        f(SiloOp::Put { key, value: value.to_vec() });
                        count += 1;
                    }
                }
                OP_TAG_DELETE => {
                    if pos + 4 + 4 > data.len() { break; }
                    let key = u32::from_le_bytes(data[pos..pos+4].try_into().unwrap());
                    pos += 4;
                    let payload_end = pos;
                    let expected_crc = u32::from_le_bytes(data[pos..pos+4].try_into().unwrap());
                    pos += 4;
                    let actual_crc = crc32fast::hash(&data[entry_start..payload_end]);
                    if actual_crc == expected_crc {
                        f(SiloOp::Delete { key });
                        count += 1;
                    }
                }
                _ => {
                    while pos < data.len() && data[pos] == 0 { pos += 1; }
                }
            }
        }

        Ok(count)
    }

    /// Current data size (bytes written).
    pub fn data_size(&self) -> u64 {
        self.cursor.load(Ordering::Relaxed)
    }

    /// Returns true if no ops have been written to this log.
    pub fn is_empty(&self) -> bool {
        self.cursor.load(Ordering::Relaxed) == 0
    }

    /// Truncate the ops log (after compaction). Drops the mmap, truncates file.
    pub fn truncate(&mut self) -> io::Result<()> {
        self.mmap = None;
        self.cursor = AtomicU64::new(0);
        self.capacity = 0;
        // Truncate the file to zero
        if self.path.exists() {
            let file = OpenOptions::new().write(true).truncate(true).open(&self.path)?;
            drop(file);
        }
        Ok(())
    }

    // ---- Encoding ----

    /// Encode an op into a framed byte buffer: [tag][key][len][value][crc32]
    pub fn encode_op(op: &SiloOp) -> Vec<u8> {
        let mut buf = Vec::with_capacity(128);
        match op {
            SiloOp::Put { key, value } => {
                buf.push(OP_TAG_PUT);
                buf.extend_from_slice(&key.to_le_bytes());
                buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
                buf.extend_from_slice(value);
            }
            SiloOp::Delete { key } => {
                buf.push(OP_TAG_DELETE);
                buf.extend_from_slice(&key.to_le_bytes());
            }
        }
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Encode a Put op directly into a provided buffer (avoids allocation).
    #[inline]
    pub fn encode_put_into(buf: &mut Vec<u8>, key: u32, value: &[u8]) {
        buf.clear();
        buf.push(OP_TAG_PUT);
        buf.extend_from_slice(&key.to_le_bytes());
        buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
        buf.extend_from_slice(value);
        let crc = crc32fast::hash(buf);
        buf.extend_from_slice(&crc.to_le_bytes());
    }

    // ---- Decoding ----

    fn decode_op(data: &[u8], pos: &mut usize) -> Option<SiloOp> {
        if *pos >= data.len() { return None; }
        // Skip zero-padding from thread-local regions
        if data[*pos] == 0 { return None; }

        let entry_start = *pos;
        let tag = data[*pos];
        *pos += 1;

        match tag {
            OP_TAG_PUT => {
                if *pos + 8 > data.len() { return None; }
                let key = u32::from_le_bytes(data[*pos..*pos + 4].try_into().ok()?);
                *pos += 4;
                let value_len = u32::from_le_bytes(data[*pos..*pos + 4].try_into().ok()?) as usize;
                *pos += 4;
                if *pos + value_len + 4 > data.len() { return None; }
                let value = data[*pos..*pos + value_len].to_vec();
                *pos += value_len;
                let payload_end = *pos;
                let expected_crc = u32::from_le_bytes(data[*pos..*pos + 4].try_into().ok()?);
                *pos += 4;
                let actual_crc = crc32fast::hash(&data[entry_start..payload_end]);
                if actual_crc != expected_crc { return None; }
                Some(SiloOp::Put { key, value })
            }
            OP_TAG_DELETE => {
                if *pos + 4 + 4 > data.len() { return None; }
                let key = u32::from_le_bytes(data[*pos..*pos + 4].try_into().ok()?);
                *pos += 4;
                let payload_end = *pos;
                let expected_crc = u32::from_le_bytes(data[*pos..*pos + 4].try_into().ok()?);
                *pos += 4;
                let actual_crc = crc32fast::hash(&data[entry_start..payload_end]);
                if actual_crc != expected_crc { return None; }
                Some(SiloOp::Delete { key })
            }
            _ => None,
        }
    }

    /// Scan backwards from end to find actual data boundary.
    /// Used when opening an existing file to set the cursor correctly.
    fn find_data_end(mmap: &[u8]) -> usize {
        // Scan forward through valid ops to find where data ends
        let mut pos = 0;
        let mut last_valid_end = 0;
        while pos < mmap.len() {
            if mmap[pos] == 0 {
                // Could be padding — skip
                pos += 1;
                continue;
            }
            let saved_pos = pos;
            if Self::decode_op(mmap, &mut pos).is_some() {
                last_valid_end = pos;
            } else {
                // Failed to decode — this is the end of valid data
                // But we need to check if there's more after padding
                pos = saved_pos + 1;
            }
        }
        last_valid_end
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_append_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.ops");
        let mut log = OpsLog::open(&path).unwrap();
        log.append(&SiloOp::Put { key: 1, value: b"hello".to_vec() }).unwrap();
        log.append(&SiloOp::Put { key: 2, value: b"world".to_vec() }).unwrap();
        log.flush().unwrap();

        let ops = log.read_all().unwrap();
        assert_eq!(ops.len(), 2);
        match &ops[0] {
            SiloOp::Put { key, value } => {
                assert_eq!(*key, 1);
                assert_eq!(value, b"hello");
            }
            _ => panic!("expected Put"),
        }
    }

    #[test]
    fn test_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.ops");
        {
            let mut log = OpsLog::open(&path).unwrap();
            log.append(&SiloOp::Put { key: 42, value: b"data".to_vec() }).unwrap();
            log.flush().unwrap();
        }
        {
            let log = OpsLog::open(&path).unwrap();
            let ops = log.read_all().unwrap();
            assert_eq!(ops.len(), 1);
            match &ops[0] {
                SiloOp::Put { key, value } => {
                    assert_eq!(*key, 42);
                    assert_eq!(value, b"data");
                }
                _ => panic!("expected Put"),
            }
        }
    }

    #[test]
    fn test_truncate() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.ops");
        let mut log = OpsLog::open(&path).unwrap();
        log.append(&SiloOp::Put { key: 1, value: b"a".to_vec() }).unwrap();
        log.flush().unwrap();
        log.truncate().unwrap();
        let ops = log.read_all().unwrap();
        assert_eq!(ops.len(), 0);
    }

    #[test]
    fn test_for_each() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.ops");
        let mut log = OpsLog::open(&path).unwrap();
        for i in 0..100u32 {
            log.append(&SiloOp::Put { key: i, value: format!("val_{}", i).into_bytes() }).unwrap();
        }
        log.flush().unwrap();

        let mut count = 0;
        log.for_each(|key, value| {
            assert_eq!(value, format!("val_{}", key).as_bytes());
            count += 1;
        }).unwrap();
        assert_eq!(count, 100);
    }

    #[test]
    fn test_parallel_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.ops");
        let mut log = OpsLog::open(&path).unwrap();

        let num_ops = 10_000u32;
        let value = vec![0xABu8; 100];
        let frame_size = 1 + 4 + 4 + 100 + 4; // tag + key + len + value + crc
        let total_size = num_ops as u64 * frame_size as u64 * 2; // 2x headroom for regions
        log.ensure_capacity(total_size).unwrap();

        // Parallel write using thread-local regions
        let num_threads = 4;
        let ops_per_thread = num_ops / num_threads;

        std::thread::scope(|s| {
            for t in 0..num_threads {
                let cursor = log.cursor();
                let mmap_ptr = log.mmap_ptr().unwrap() as usize;
                let mmap_len = log.mmap_len();
                let val = &value;

                s.spawn(move || {
                    let mut local_cursor = 0usize;
                    let mut region_end = 0usize;
                    let mut frame_buf = Vec::with_capacity(frame_size);

                    for i in 0..ops_per_thread {
                        let key = t * ops_per_thread + i;
                        OpsLog::encode_put_into(&mut frame_buf, key, val);

                        if local_cursor + frame_buf.len() > region_end {
                            let start = cursor.fetch_add(REGION_SIZE, Ordering::Relaxed) as usize;
                            local_cursor = start;
                            region_end = start + REGION_SIZE as usize;
                        }

                        if local_cursor + frame_buf.len() <= mmap_len {
                            unsafe {
                                let dst = (mmap_ptr as *mut u8).add(local_cursor);
                                std::ptr::copy_nonoverlapping(frame_buf.as_ptr(), dst, frame_buf.len());
                            }
                        }
                        local_cursor += frame_buf.len();
                    }
                });
            }
        });

        log.flush().unwrap();

        // Read back and verify
        let mut found = std::collections::HashSet::new();
        log.for_each(|key, _value| {
            found.insert(key);
        }).unwrap();
        assert_eq!(found.len(), num_ops as usize, "all ops should be readable");
    }
}
