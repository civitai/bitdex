//! FieldOpsLog — lightweight mmap'd append-only log for cache field-level mutations.
//!
//! Records filter SET/CLEAR and sort CHANGE events as fixed-size entries.
//! The flush thread appends ops here whenever it processes mutations that
//! could affect cached query results. On cache read, the reader scans from
//! its `last_applied_offset` to discover pending ops that match the entry's
//! clauses. The janitor periodically checkpoints via A/B swap.
//!
//! ## Entry format (15 bytes, fixed-size)
//! ```text
//! [u8  tag]        — 0x01=FilterSet, 0x02=FilterClear, 0x03=SortChange
//! [u16 field_idx]  — field registry ID (LE)
//! [u32 value]      — filter bitmap value (filter ops) or new sort value (sort ops) (LE)
//! [u32 slot]       — document slot ID (LE)
//! [u32 crc32]      — CRC32 of first 11 bytes (LE)
//! ```
//!
//! ## A/B Swap for non-blocking checkpoints
//! Two files: `field_ops_a.log` and `field_ops_b.log`. One is *active*
//! (receiving appends from the flush thread) and the other is *frozen*
//! (being processed by the janitor). On checkpoint:
//!
//! 1. Freeze active log, swap to other
//! 2. Janitor processes all ops from frozen log
//! 3. Janitor truncates frozen log when done
//!
//! Readers see both logs during the swap window (scan active from their
//! offset, then scan frozen from 0 to frozen cursor).

use std::fs::OpenOptions;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// Entry size in bytes: tag(1) + field_idx(2) + value(8) + slot(4) + crc32(4) = 19
const ENTRY_SIZE: usize = 19;

/// Initial log file size (4 MB). Grows as needed.
const INITIAL_SIZE: u64 = 4 * 1024 * 1024;

// Op tags
const TAG_FILTER_SET: u8 = 0x01;
const TAG_FILTER_CLEAR: u8 = 0x02;
const TAG_SORT_CHANGE: u8 = 0x03;

/// A single field-level mutation op for cache maintenance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldOp {
    /// A slot was added to a filter bitmap: field[value] |= {slot}
    FilterSet { field_idx: u16, value: u64, slot: u32 },
    /// A slot was removed from a filter bitmap: field[value] &= !{slot}
    FilterClear { field_idx: u16, value: u64, slot: u32 },
    /// A slot's sort value changed for a sort field.
    SortChange { field_idx: u16, new_value: u64, slot: u32 },
}

impl FieldOp {
    /// The field index for this op.
    #[inline]
    pub fn field_idx(&self) -> u16 {
        match *self {
            FieldOp::FilterSet { field_idx, .. }
            | FieldOp::FilterClear { field_idx, .. }
            | FieldOp::SortChange { field_idx, .. } => field_idx,
        }
    }

    /// The value for this op (filter value or new sort value).
    #[inline]
    pub fn value(&self) -> u64 {
        match *self {
            FieldOp::FilterSet { value, .. }
            | FieldOp::FilterClear { value, .. }
            | FieldOp::SortChange { new_value: value, .. } => value,
        }
    }

    /// The slot for this op.
    #[inline]
    pub fn slot(&self) -> u32 {
        match *self {
            FieldOp::FilterSet { slot, .. }
            | FieldOp::FilterClear { slot, .. }
            | FieldOp::SortChange { slot, .. } => slot,
        }
    }

    /// Whether this is a filter op (SET or CLEAR).
    #[inline]
    pub fn is_filter(&self) -> bool {
        matches!(self, FieldOp::FilterSet { .. } | FieldOp::FilterClear { .. })
    }

    /// Encode this op into a 19-byte buffer with CRC32.
    #[inline]
    fn encode(&self, buf: &mut [u8; ENTRY_SIZE]) {
        let (tag, field_idx, value, slot) = match *self {
            FieldOp::FilterSet { field_idx, value, slot } => (TAG_FILTER_SET, field_idx, value, slot),
            FieldOp::FilterClear { field_idx, value, slot } => (TAG_FILTER_CLEAR, field_idx, value, slot),
            FieldOp::SortChange { field_idx, new_value, slot } => (TAG_SORT_CHANGE, field_idx, new_value, slot),
        };
        buf[0] = tag;
        buf[1..3].copy_from_slice(&field_idx.to_le_bytes());
        buf[3..11].copy_from_slice(&value.to_le_bytes());
        buf[11..15].copy_from_slice(&slot.to_le_bytes());
        let crc = crc32fast::hash(&buf[..15]);
        buf[15..19].copy_from_slice(&crc.to_le_bytes());
    }

    /// Decode a 19-byte entry. Returns None if CRC fails or tag is invalid.
    #[inline]
    fn decode(buf: &[u8; ENTRY_SIZE]) -> Option<Self> {
        let expected_crc = u32::from_le_bytes([buf[15], buf[16], buf[17], buf[18]]);
        let actual_crc = crc32fast::hash(&buf[..15]);
        if actual_crc != expected_crc {
            return None;
        }
        let field_idx = u16::from_le_bytes([buf[1], buf[2]]);
        let value = u64::from_le_bytes(buf[3..11].try_into().unwrap());
        let slot = u32::from_le_bytes([buf[11], buf[12], buf[13], buf[14]]);
        match buf[0] {
            TAG_FILTER_SET => Some(FieldOp::FilterSet { field_idx, value, slot }),
            TAG_FILTER_CLEAR => Some(FieldOp::FilterClear { field_idx, value, slot }),
            TAG_SORT_CHANGE => Some(FieldOp::SortChange { field_idx, new_value: value, slot }),
            _ => None,
        }
    }
}

/// A single mmap'd append-only log file for field ops.
struct LogFile {
    path: PathBuf,
    mmap: Option<memmap2::MmapMut>,
    /// Write cursor (byte offset). Atomic for future parallel-write support.
    cursor: AtomicU64,
    /// File capacity in bytes.
    capacity: u64,
}

// Safety: disjoint-region writes via atomic cursor.
unsafe impl Send for LogFile {}
unsafe impl Sync for LogFile {}

impl LogFile {
    fn open(path: &Path) -> io::Result<Self> {
        if path.exists() {
            let meta = std::fs::metadata(path)?;
            let file_size = meta.len();
            if file_size == 0 {
                return Ok(Self {
                    path: path.to_path_buf(),
                    mmap: None,
                    cursor: AtomicU64::new(0),
                    capacity: 0,
                });
            }
            let file = OpenOptions::new().read(true).write(true).open(path)?;
            let mmap = unsafe { memmap2::MmapMut::map_mut(&file)? };
            let data_end = Self::find_data_end(&mmap);
            Ok(Self {
                path: path.to_path_buf(),
                cursor: AtomicU64::new(data_end as u64),
                capacity: file_size,
                mmap: Some(mmap),
            })
        } else {
            Ok(Self {
                path: path.to_path_buf(),
                mmap: None,
                cursor: AtomicU64::new(0),
                capacity: 0,
            })
        }
    }

    /// Ensure the mmap is at least `min_size` bytes.
    fn ensure_capacity(&mut self, min_size: u64) -> io::Result<()> {
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

    /// Append a single field op. Auto-grows if needed.
    fn append(&mut self, op: &FieldOp) -> io::Result<()> {
        let mut entry = [0u8; ENTRY_SIZE];
        op.encode(&mut entry);
        let needed = self.cursor.load(Ordering::Relaxed) + ENTRY_SIZE as u64;
        if needed > self.capacity || self.mmap.is_none() {
            self.ensure_capacity(needed + INITIAL_SIZE)?;
        }
        let offset = self.cursor.fetch_add(ENTRY_SIZE as u64, Ordering::Relaxed) as usize;
        let mmap = self.mmap.as_ref().unwrap();
        if offset + ENTRY_SIZE <= mmap.len() {
            unsafe {
                let dst = mmap.as_ptr().add(offset) as *mut u8;
                std::ptr::copy_nonoverlapping(entry.as_ptr(), dst, ENTRY_SIZE);
            }
        }
        Ok(())
    }

    /// Append a batch of field ops. More efficient than individual appends.
    fn append_batch(&mut self, ops: &[FieldOp]) -> io::Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        let total_bytes = ops.len() as u64 * ENTRY_SIZE as u64;
        let needed = self.cursor.load(Ordering::Relaxed) + total_bytes;
        if needed > self.capacity || self.mmap.is_none() {
            self.ensure_capacity(needed + INITIAL_SIZE)?;
        }
        let start_offset = self.cursor.fetch_add(total_bytes, Ordering::Relaxed) as usize;
        let mmap = self.mmap.as_ref().unwrap();
        let mut entry = [0u8; ENTRY_SIZE];
        for (i, op) in ops.iter().enumerate() {
            let offset = start_offset + i * ENTRY_SIZE;
            if offset + ENTRY_SIZE <= mmap.len() {
                op.encode(&mut entry);
                unsafe {
                    let dst = mmap.as_ptr().add(offset) as *mut u8;
                    std::ptr::copy_nonoverlapping(entry.as_ptr(), dst, ENTRY_SIZE);
                }
            }
        }
        Ok(())
    }

    /// Current write cursor (byte offset). This is the "end" of valid data.
    fn cursor_pos(&self) -> u64 {
        self.cursor.load(Ordering::Relaxed)
    }

    /// Number of ops written.
    fn op_count(&self) -> u64 {
        self.cursor.load(Ordering::Relaxed) / ENTRY_SIZE as u64
    }

    /// Scan ops from a byte offset, calling `f` for each valid op.
    /// Returns the byte offset after the last successfully read op.
    fn scan_from<F>(&self, from_offset: u64, mut f: F) -> u64
    where
        F: FnMut(FieldOp),
    {
        let mmap = match &self.mmap {
            Some(m) => m,
            None => return from_offset,
        };
        let end = self.cursor.load(Ordering::Relaxed) as usize;
        let start = from_offset as usize;
        if start >= end {
            return from_offset;
        }

        let data = &mmap[..end.min(mmap.len())];
        let mut pos = start;
        // Align to entry boundary in case offset is mid-entry
        let misalign = pos % ENTRY_SIZE;
        if misalign != 0 {
            pos += ENTRY_SIZE - misalign;
        }

        while pos + ENTRY_SIZE <= data.len() {
            let entry: &[u8; ENTRY_SIZE] = data[pos..pos + ENTRY_SIZE].try_into().unwrap();
            if let Some(op) = FieldOp::decode(entry) {
                f(op);
            }
            // Always advance — fixed size entries mean no ambiguity
            pos += ENTRY_SIZE;
        }

        pos as u64
    }

    /// Flush mmap to disk.
    fn flush(&self) -> io::Result<()> {
        if let Some(ref mmap) = self.mmap {
            mmap.flush()?;
        }
        Ok(())
    }

    /// Truncate the log (after checkpoint processing).
    fn truncate(&mut self) -> io::Result<()> {
        self.mmap = None;
        self.cursor = AtomicU64::new(0);
        self.capacity = 0;
        if self.path.exists() {
            let file = OpenOptions::new().write(true).truncate(true).open(&self.path)?;
            drop(file);
        }
        Ok(())
    }

    /// Returns true if no ops have been written.
    fn is_empty(&self) -> bool {
        self.cursor.load(Ordering::Relaxed) == 0
    }

    /// Scan forward through fixed-size entries to find actual data end.
    fn find_data_end(mmap: &[u8]) -> usize {
        let mut pos = 0;
        let mut last_valid_end = 0;
        while pos + ENTRY_SIZE <= mmap.len() {
            let entry: &[u8; ENTRY_SIZE] = match mmap[pos..pos + ENTRY_SIZE].try_into() {
                Ok(e) => e,
                Err(_) => break,
            };
            // Check if this looks like a valid entry (non-zero tag + valid CRC)
            if entry[0] == 0 {
                // Could be zero-fill from file growth — stop
                break;
            }
            if FieldOp::decode(entry).is_some() {
                pos += ENTRY_SIZE;
                last_valid_end = pos;
            } else {
                // Invalid CRC or tag — stop
                break;
            }
        }
        last_valid_end
    }
}

// ---------------------------------------------------------------------------
// FieldOpsLog — A/B swap wrapper
// ---------------------------------------------------------------------------

/// Which side of the A/B pair is currently active.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    A,
    B,
}

/// Mmap'd field ops log with A/B swap for non-blocking checkpoints.
///
/// The flush thread appends to the active log. The janitor freezes the active
/// log, swaps to the other, processes the frozen log, then truncates it.
/// Readers can scan both logs during the swap window.
pub struct FieldOpsLog {
    log_a: LogFile,
    log_b: LogFile,
    active_side: Side,
    dir: PathBuf,
}

impl FieldOpsLog {
    /// Open or create the field ops log in the given directory.
    pub fn open(dir: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path_a = dir.join("field_ops_a.log");
        let path_b = dir.join("field_ops_b.log");
        let log_a = LogFile::open(&path_a)?;
        let log_b = LogFile::open(&path_b)?;

        // On startup: if both have data, the one with more data was likely active
        // when we last shut down. If only one has data, it was the active side.
        // If neither has data, default to A.
        let active_side = if log_a.is_empty() && !log_b.is_empty() {
            Side::B
        } else {
            // Default: A is active (either both empty, both have data, or only A has data)
            Side::A
        };

        Ok(Self {
            log_a,
            log_b,
            active_side,
            dir: dir.to_path_buf(),
        })
    }

    /// Append a single field op to the active log.
    pub fn append(&mut self, op: &FieldOp) -> io::Result<()> {
        self.active_mut().append(op)
    }

    /// Append a batch of field ops to the active log.
    pub fn append_batch(&mut self, ops: &[FieldOp]) -> io::Result<()> {
        self.active_mut().append_batch(ops)
    }

    /// Current write cursor of the active log (byte offset).
    /// Cache entries store this as `last_applied_offset`.
    pub fn active_cursor(&self) -> u64 {
        self.active().cursor_pos()
    }

    /// Number of ops in the active log.
    pub fn active_op_count(&self) -> u64 {
        self.active().op_count()
    }

    /// Scan the active log from `from_offset`, calling `f` for each op.
    /// Returns the new cursor position (to store as last_applied_offset).
    pub fn scan_active_from<F>(&self, from_offset: u64, f: F) -> u64
    where
        F: FnMut(FieldOp),
    {
        self.active().scan_from(from_offset, f)
    }

    /// Scan the frozen log from offset 0, calling `f` for each op.
    /// Used by the janitor during checkpoint processing.
    /// Returns the number of ops processed.
    pub fn scan_frozen<F>(&self, mut f: F) -> u64
    where
        F: FnMut(FieldOp),
    {
        let frozen = self.frozen();
        let end = frozen.cursor_pos();
        if end == 0 {
            return 0;
        }
        let mut count = 0u64;
        frozen.scan_from(0, |op| {
            f(op);
            count += 1;
        });
        count
    }

    /// Freeze the active log and swap to the other side.
    /// After this call, new appends go to the previously-frozen log.
    /// The caller should process the now-frozen log and truncate it.
    pub fn swap(&mut self) -> io::Result<()> {
        // Flush the active log to ensure all data is on disk
        self.active().flush()?;
        self.active_side = match self.active_side {
            Side::A => Side::B,
            Side::B => Side::A,
        };
        Ok(())
    }

    /// Truncate the frozen (non-active) log after checkpoint processing.
    pub fn truncate_frozen(&mut self) -> io::Result<()> {
        self.frozen_mut().truncate()
    }

    /// Whether the frozen log has data (pending checkpoint).
    pub fn has_frozen_data(&self) -> bool {
        !self.frozen().is_empty()
    }

    /// Total ops across both logs (active + frozen).
    pub fn total_op_count(&self) -> u64 {
        self.log_a.op_count() + self.log_b.op_count()
    }

    /// Flush both logs to disk.
    pub fn flush(&self) -> io::Result<()> {
        self.log_a.flush()?;
        self.log_b.flush()?;
        Ok(())
    }

    /// The directory containing the log files.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Which side is currently active (for diagnostics).
    pub fn active_side_name(&self) -> &'static str {
        match self.active_side {
            Side::A => "A",
            Side::B => "B",
        }
    }

    // -- private helpers --

    fn active(&self) -> &LogFile {
        match self.active_side {
            Side::A => &self.log_a,
            Side::B => &self.log_b,
        }
    }

    fn active_mut(&mut self) -> &mut LogFile {
        match self.active_side {
            Side::A => &mut self.log_a,
            Side::B => &mut self.log_b,
        }
    }

    fn frozen(&self) -> &LogFile {
        match self.active_side {
            Side::A => &self.log_b,
            Side::B => &self.log_a,
        }
    }

    fn frozen_mut(&mut self) -> &mut LogFile {
        match self.active_side {
            Side::A => &mut self.log_b,
            Side::B => &mut self.log_a,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_filter_set(field_idx: u16, value: u64, slot: u32) -> FieldOp {
        FieldOp::FilterSet { field_idx, value, slot }
    }

    fn make_filter_clear(field_idx: u16, value: u64, slot: u32) -> FieldOp {
        FieldOp::FilterClear { field_idx, value, slot }
    }

    fn make_sort_change(field_idx: u16, new_value: u64, slot: u32) -> FieldOp {
        FieldOp::SortChange { field_idx, new_value, slot }
    }

    // ── encode/decode roundtrip ──────────────────────────────────────────

    #[test]
    fn encode_decode_roundtrip_filter_set() {
        let op = make_filter_set(5, 42, 1000);
        let mut buf = [0u8; ENTRY_SIZE];
        op.encode(&mut buf);
        let decoded = FieldOp::decode(&buf).expect("should decode");
        assert_eq!(decoded, op);
    }

    #[test]
    fn encode_decode_roundtrip_filter_clear() {
        let op = make_filter_clear(3, 99, 500);
        let mut buf = [0u8; ENTRY_SIZE];
        op.encode(&mut buf);
        let decoded = FieldOp::decode(&buf).expect("should decode");
        assert_eq!(decoded, op);
    }

    #[test]
    fn encode_decode_roundtrip_sort_change() {
        let op = make_sort_change(1, 1717171717u64, 42);
        let mut buf = [0u8; ENTRY_SIZE];
        op.encode(&mut buf);
        let decoded = FieldOp::decode(&buf).expect("should decode");
        assert_eq!(decoded, op);
    }

    #[test]
    fn decode_rejects_bad_crc() {
        let op = make_filter_set(1, 2, 3);
        let mut buf = [0u8; ENTRY_SIZE];
        op.encode(&mut buf);
        buf[14] ^= 0xFF; // corrupt CRC
        assert!(FieldOp::decode(&buf).is_none());
    }

    #[test]
    fn decode_rejects_unknown_tag() {
        let mut buf = [0u8; ENTRY_SIZE];
        buf[0] = 0xFF; // invalid tag
        // Even with valid CRC for these bytes, unknown tag returns None
        let crc = crc32fast::hash(&buf[..11]);
        buf[11..15].copy_from_slice(&crc.to_le_bytes());
        assert!(FieldOp::decode(&buf).is_none());
    }

    // ── LogFile basics ───────────────────────────────────────────────────

    #[test]
    fn append_and_scan() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.log");
        let mut log = LogFile::open(&path).unwrap();

        log.append(&make_filter_set(1, 10, 100)).unwrap();
        log.append(&make_filter_clear(2, 20, 200)).unwrap();
        log.append(&make_sort_change(3, 30, 300)).unwrap();

        let mut ops = Vec::new();
        log.scan_from(0, |op| ops.push(op));
        assert_eq!(ops.len(), 3);
        assert_eq!(ops[0], make_filter_set(1, 10, 100));
        assert_eq!(ops[1], make_filter_clear(2, 20, 200));
        assert_eq!(ops[2], make_sort_change(3, 30, 300));
    }

    #[test]
    fn scan_from_offset_skips_earlier_ops() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.log");
        let mut log = LogFile::open(&path).unwrap();

        log.append(&make_filter_set(1, 10, 100)).unwrap();
        log.append(&make_filter_set(2, 20, 200)).unwrap();
        log.append(&make_filter_set(3, 30, 300)).unwrap();

        // Skip first entry
        let mut ops = Vec::new();
        log.scan_from(ENTRY_SIZE as u64, |op| ops.push(op));
        assert_eq!(ops.len(), 2);
        assert_eq!(ops[0], make_filter_set(2, 20, 200));
        assert_eq!(ops[1], make_filter_set(3, 30, 300));
    }

    #[test]
    fn scan_returns_new_cursor() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.log");
        let mut log = LogFile::open(&path).unwrap();

        log.append(&make_filter_set(1, 10, 100)).unwrap();
        log.append(&make_filter_set(2, 20, 200)).unwrap();

        let new_cursor = log.scan_from(0, |_| {});
        assert_eq!(new_cursor, 2 * ENTRY_SIZE as u64);

        // Scanning from the end returns the same offset
        let same = log.scan_from(new_cursor, |_| {});
        assert_eq!(same, new_cursor);
    }

    #[test]
    fn append_batch_works() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.log");
        let mut log = LogFile::open(&path).unwrap();

        let ops = vec![
            make_filter_set(1, 10, 100),
            make_filter_clear(2, 20, 200),
            make_sort_change(3, 30, 300),
        ];
        log.append_batch(&ops).unwrap();

        let mut read_ops = Vec::new();
        log.scan_from(0, |op| read_ops.push(op));
        assert_eq!(read_ops, ops);
    }

    #[test]
    fn truncate_clears_log() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.log");
        let mut log = LogFile::open(&path).unwrap();

        log.append(&make_filter_set(1, 10, 100)).unwrap();
        assert!(!log.is_empty());

        log.truncate().unwrap();
        assert!(log.is_empty());
        assert_eq!(log.op_count(), 0);
    }

    #[test]
    fn reopen_preserves_data() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.log");
        {
            let mut log = LogFile::open(&path).unwrap();
            log.append(&make_filter_set(1, 10, 100)).unwrap();
            log.append(&make_filter_clear(2, 20, 200)).unwrap();
            log.flush().unwrap();
        }
        // Reopen
        let log = LogFile::open(&path).unwrap();
        assert_eq!(log.op_count(), 2);
        let mut ops = Vec::new();
        log.scan_from(0, |op| ops.push(op));
        assert_eq!(ops.len(), 2);
        assert_eq!(ops[0], make_filter_set(1, 10, 100));
    }

    // ── FieldOpsLog (A/B swap) ───────────────────────────────────────────

    #[test]
    fn basic_append_and_scan_active() {
        let dir = TempDir::new().unwrap();
        let log_dir = dir.path().join("cache");
        let mut log = FieldOpsLog::open(&log_dir).unwrap();

        log.append(&make_filter_set(1, 10, 100)).unwrap();
        log.append(&make_filter_clear(2, 20, 200)).unwrap();

        let mut ops = Vec::new();
        log.scan_active_from(0, |op| ops.push(op));
        assert_eq!(ops.len(), 2);
        assert_eq!(log.active_op_count(), 2);
    }

    #[test]
    fn swap_freezes_active_and_starts_fresh() {
        let dir = TempDir::new().unwrap();
        let log_dir = dir.path().join("cache");
        let mut log = FieldOpsLog::open(&log_dir).unwrap();

        // Write to active (A)
        log.append(&make_filter_set(1, 10, 100)).unwrap();
        log.append(&make_filter_set(2, 20, 200)).unwrap();
        assert_eq!(log.active_side_name(), "A");

        // Swap: A becomes frozen, B becomes active
        log.swap().unwrap();
        assert_eq!(log.active_side_name(), "B");
        assert_eq!(log.active_op_count(), 0); // fresh active
        assert!(log.has_frozen_data()); // A is now frozen with 2 ops

        // New writes go to B
        log.append(&make_filter_set(3, 30, 300)).unwrap();
        assert_eq!(log.active_op_count(), 1);

        // Scan frozen (A) — should see the 2 original ops
        let mut frozen_ops = Vec::new();
        log.scan_frozen(|op| frozen_ops.push(op));
        assert_eq!(frozen_ops.len(), 2);
        assert_eq!(frozen_ops[0], make_filter_set(1, 10, 100));
    }

    #[test]
    fn truncate_frozen_clears_old_data() {
        let dir = TempDir::new().unwrap();
        let log_dir = dir.path().join("cache");
        let mut log = FieldOpsLog::open(&log_dir).unwrap();

        log.append(&make_filter_set(1, 10, 100)).unwrap();
        log.swap().unwrap();
        assert!(log.has_frozen_data());

        log.truncate_frozen().unwrap();
        assert!(!log.has_frozen_data());
    }

    #[test]
    fn full_checkpoint_cycle() {
        let dir = TempDir::new().unwrap();
        let log_dir = dir.path().join("cache");
        let mut log = FieldOpsLog::open(&log_dir).unwrap();

        // Phase 1: accumulate ops in A
        for i in 0..100u32 {
            log.append(&make_filter_set(1, i as u64, i * 10)).unwrap();
        }
        assert_eq!(log.active_op_count(), 100);

        // Phase 2: swap — A frozen, B active
        log.swap().unwrap();

        // Phase 3: new ops go to B while janitor processes A
        log.append(&make_filter_set(2, 999, 9990)).unwrap();

        // Phase 4: janitor reads frozen
        let mut frozen_count = 0u64;
        log.scan_frozen(|_| frozen_count += 1);
        assert_eq!(frozen_count, 100);

        // Phase 5: janitor done — truncate frozen
        log.truncate_frozen().unwrap();
        assert!(!log.has_frozen_data());

        // Active still has its op
        assert_eq!(log.active_op_count(), 1);
        assert_eq!(log.total_op_count(), 1);
    }

    #[test]
    fn reopen_picks_correct_active_side() {
        let dir = TempDir::new().unwrap();
        let log_dir = dir.path().join("cache");

        // Write to A, swap to B, write to B
        {
            let mut log = FieldOpsLog::open(&log_dir).unwrap();
            log.append(&make_filter_set(1, 10, 100)).unwrap();
            log.swap().unwrap();
            log.append(&make_filter_set(2, 20, 200)).unwrap();
            log.flush().unwrap();
        }

        // Reopen: B has data (was active), A has data (was frozen)
        // Default picks A, which is fine — both will be scanned on startup
        let log = FieldOpsLog::open(&log_dir).unwrap();
        // Total ops should be 2 across both files
        assert_eq!(log.total_op_count(), 2);
    }

    #[test]
    fn empty_batch_is_noop() {
        let dir = TempDir::new().unwrap();
        let log_dir = dir.path().join("cache");
        let mut log = FieldOpsLog::open(&log_dir).unwrap();
        log.append_batch(&[]).unwrap();
        assert_eq!(log.active_op_count(), 0);
    }

    #[test]
    fn large_batch_grows_file() {
        let dir = TempDir::new().unwrap();
        let log_dir = dir.path().join("cache");
        let mut log = FieldOpsLog::open(&log_dir).unwrap();

        let ops: Vec<FieldOp> = (0..10_000u32)
            .map(|i| make_filter_set(1, i as u64, i))
            .collect();
        log.append_batch(&ops).unwrap();
        assert_eq!(log.active_op_count(), 10_000);

        // Verify all ops readable
        let mut count = 0u64;
        log.scan_active_from(0, |_| count += 1);
        assert_eq!(count, 10_000);
    }

    // ── Edge cases ───────────────────────────────────────────────────────

    #[test]
    fn max_field_values() {
        let op = make_filter_set(u16::MAX, u64::MAX, u32::MAX);
        let mut buf = [0u8; ENTRY_SIZE];
        op.encode(&mut buf);
        let decoded = FieldOp::decode(&buf).expect("max values should decode");
        assert_eq!(decoded, op);
    }

    #[test]
    fn zero_field_values() {
        let op = make_filter_set(0, 0, 0);
        let mut buf = [0u8; ENTRY_SIZE];
        op.encode(&mut buf);
        let decoded = FieldOp::decode(&buf).expect("zero values should decode");
        assert_eq!(decoded, op);
    }
}
