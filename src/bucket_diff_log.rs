//! Append-only diff log for incremental time bucket maintenance.
//!
//! Each entry records the slots that aged out of a time bucket during one
//! refresh cycle. Entries are appended on each cycle and loaded on boot
//! to restore pending diffs without a full 107M scan.
//!
//! ## File Format
//!
//! ```text
//! [cutoff_before: u64] [cutoff_after: u64] [bitmap_len: u32] [bitmap_data: [u8]] [crc32: u32]
//! ```
//!
//! ## Compaction
//!
//! Atomic rewrite (write tmp + rename) when entry count exceeds
//! `max_diffs * (1 + compaction_threshold_pct)`.

use std::io::{self, Read, Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use roaring::RoaringBitmap;

/// A single diff: slots that expired from a bucket in one refresh cycle.
#[derive(Clone)]
pub struct BucketDiff {
    /// The snapped cutoff before this refresh (unix seconds).
    pub cutoff_before: u64,
    /// The snapped cutoff after this refresh (unix seconds).
    pub cutoff_after: u64,
    /// Slots that aged out in `[cutoff_before, cutoff_after)`.
    pub expired: Arc<RoaringBitmap>,
}

/// In-memory pending diffs with a pre-computed merged bitmap.
pub struct PendingBucketDiffs {
    /// Individual diffs, ordered oldest to newest.
    diffs: Vec<BucketDiff>,
    /// Union of all `expired` bitmaps — apply this single bitmap to bring
    /// any entry from `oldest_cutoff` to `current_cutoff` in one AND-NOT.
    merged_expired: Arc<RoaringBitmap>,
    /// The newest snapped cutoff covered by these diffs.
    current_cutoff: u64,
    /// Maximum diffs to retain.
    max_diffs: usize,
}

impl PendingBucketDiffs {
    pub fn new(max_diffs: usize) -> Self {
        Self {
            diffs: Vec::new(),
            merged_expired: Arc::new(RoaringBitmap::new()),
            current_cutoff: 0,
            max_diffs,
        }
    }

    /// Load from a vec of diffs (used on boot after reading the log).
    pub fn from_diffs(diffs: Vec<BucketDiff>, max_diffs: usize) -> Self {
        let current_cutoff = diffs.last().map(|d| d.cutoff_after).unwrap_or(0);
        let merged = Self::compute_merged(&diffs);
        Self {
            diffs,
            merged_expired: Arc::new(merged),
            current_cutoff,
            max_diffs,
        }
    }

    /// Push a new diff. Trims oldest entries if over retention, rebuilds merged.
    pub fn push(&mut self, diff: BucketDiff) {
        self.current_cutoff = diff.cutoff_after;
        self.diffs.push(diff);

        // Trim oldest if over retention
        while self.diffs.len() > self.max_diffs {
            self.diffs.remove(0);
        }

        self.merged_expired = Arc::new(Self::compute_merged(&self.diffs));
    }

    /// The newest cutoff covered by pending diffs.
    pub fn current_cutoff(&self) -> u64 {
        self.current_cutoff
    }

    /// The oldest cutoff covered by pending diffs.
    pub fn oldest_cutoff(&self) -> u64 {
        self.diffs.first().map(|d| d.cutoff_before).unwrap_or(0)
    }

    /// Pre-computed union of all expired bitmaps.
    pub fn merged_expired(&self) -> &Arc<RoaringBitmap> {
        &self.merged_expired
    }

    /// Number of retained diffs.
    pub fn len(&self) -> usize {
        self.diffs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.diffs.is_empty()
    }

    /// All diffs (for persistence).
    pub fn diffs(&self) -> &[BucketDiff] {
        &self.diffs
    }

    fn compute_merged(diffs: &[BucketDiff]) -> RoaringBitmap {
        let mut merged = RoaringBitmap::new();
        for d in diffs {
            merged |= d.expired.as_ref();
        }
        merged
    }
}

/// Snap a timestamp to the nearest interval boundary (floor).
pub fn snap_cutoff(ts: u64, interval: u64) -> u64 {
    if interval == 0 {
        return ts;
    }
    (ts / interval) * interval
}

// ── Append-Only Log ──────────────────────────────────────────────────────

const ENTRY_HEADER_SIZE: usize = 20; // cutoff_before(8) + cutoff_after(8) + bitmap_len(4)
const CRC_SIZE: usize = 4;

/// Append-only log file for bucket diffs.
pub struct BucketDiffLog {
    path: PathBuf,
    max_diffs: usize,
    compaction_threshold_pct: f64,
}

impl BucketDiffLog {
    pub fn new(path: PathBuf, max_diffs: usize, compaction_threshold_pct: f64) -> Self {
        Self {
            path,
            max_diffs,
            compaction_threshold_pct,
        }
    }

    /// Append a single diff entry to the log.
    pub fn append(&self, diff: &BucketDiff) -> io::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;

        let bitmap_bytes = Self::serialize_bitmap(&diff.expired);
        let mut buf = Vec::with_capacity(ENTRY_HEADER_SIZE + bitmap_bytes.len() + CRC_SIZE);

        buf.extend_from_slice(&diff.cutoff_before.to_le_bytes());
        buf.extend_from_slice(&diff.cutoff_after.to_le_bytes());
        buf.extend_from_slice(&(bitmap_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&bitmap_bytes);

        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());

        file.write_all(&buf)?;
        Ok(())
    }

    /// Read all entries from the log. Returns entries in chronological order.
    /// Discards any trailing corrupted entry (partial write from crash).
    pub fn read_all(&self) -> io::Result<Vec<BucketDiff>> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }

        let data = std::fs::read(&self.path)?;
        let mut diffs = Vec::new();
        let mut pos = 0;

        while pos + ENTRY_HEADER_SIZE <= data.len() {
            // Read header
            let cutoff_before = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            let cutoff_after = u64::from_le_bytes(data[pos + 8..pos + 16].try_into().unwrap());
            let bitmap_len = u32::from_le_bytes(data[pos + 16..pos + 20].try_into().unwrap()) as usize;

            let entry_end = pos + ENTRY_HEADER_SIZE + bitmap_len + CRC_SIZE;
            if entry_end > data.len() {
                // Truncated entry — discard
                break;
            }

            // Verify CRC
            let payload = &data[pos..pos + ENTRY_HEADER_SIZE + bitmap_len];
            let stored_crc = u32::from_le_bytes(
                data[pos + ENTRY_HEADER_SIZE + bitmap_len..entry_end].try_into().unwrap(),
            );
            let computed_crc = crc32fast::hash(payload);
            if stored_crc != computed_crc {
                // Corrupted entry — discard this and everything after
                break;
            }

            // Deserialize bitmap
            let bitmap_data = &data[pos + ENTRY_HEADER_SIZE..pos + ENTRY_HEADER_SIZE + bitmap_len];
            match RoaringBitmap::deserialize_from(bitmap_data) {
                Ok(bitmap) => {
                    diffs.push(BucketDiff {
                        cutoff_before,
                        cutoff_after,
                        expired: Arc::new(bitmap),
                    });
                }
                Err(_) => break, // corrupted bitmap data
            }

            pos = entry_end;
        }

        Ok(diffs)
    }

    /// Read only the most recent `max_diffs` entries (retention window).
    pub fn read_retained(&self) -> io::Result<Vec<BucketDiff>> {
        let all = self.read_all()?;
        let start = all.len().saturating_sub(self.max_diffs);
        Ok(all[start..].to_vec())
    }

    /// Compact the log: keep only the most recent `max_diffs` entries.
    /// Uses atomic rewrite (write tmp + rename).
    pub fn compact_if_needed(&self) -> io::Result<bool> {
        let all = self.read_all()?;
        let threshold = (self.max_diffs as f64 * (1.0 + self.compaction_threshold_pct)) as usize;

        if all.len() <= threshold {
            return Ok(false);
        }

        let retained = &all[all.len().saturating_sub(self.max_diffs)..];
        let tmp_path = self.path.with_extension("log.tmp");

        // Write retained entries to tmp file
        {
            let mut file = std::fs::File::create(&tmp_path)?;
            for diff in retained {
                let bitmap_bytes = Self::serialize_bitmap(&diff.expired);
                let mut buf = Vec::with_capacity(ENTRY_HEADER_SIZE + bitmap_bytes.len() + CRC_SIZE);
                buf.extend_from_slice(&diff.cutoff_before.to_le_bytes());
                buf.extend_from_slice(&diff.cutoff_after.to_le_bytes());
                buf.extend_from_slice(&(bitmap_bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(&bitmap_bytes);
                let crc = crc32fast::hash(&buf);
                buf.extend_from_slice(&crc.to_le_bytes());
                file.write_all(&buf)?;
            }
        }

        // Atomic swap
        std::fs::rename(&tmp_path, &self.path)?;

        Ok(true)
    }

    /// Total number of entries currently in the log.
    pub fn entry_count(&self) -> io::Result<usize> {
        Ok(self.read_all()?.len())
    }

    fn serialize_bitmap(bitmap: &RoaringBitmap) -> Vec<u8> {
        let mut buf = Vec::new();
        bitmap.serialize_into(&mut buf).expect("bitmap serialization should not fail");
        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_log_path() -> PathBuf {
        let dir = tempfile::tempdir().unwrap();
        // Leak the dir so it persists for the test
        let path = dir.path().join("bucket_diffs.log");
        std::mem::forget(dir);
        path
    }

    #[test]
    fn test_snap_cutoff() {
        assert_eq!(snap_cutoff(1000, 300), 900);
        assert_eq!(snap_cutoff(900, 300), 900);
        assert_eq!(snap_cutoff(1199, 300), 900);
        assert_eq!(snap_cutoff(1200, 300), 1200);
        assert_eq!(snap_cutoff(0, 300), 0);
        assert_eq!(snap_cutoff(100, 0), 100);
    }

    #[test]
    fn test_pending_diffs_push_and_merge() {
        let mut pending = PendingBucketDiffs::new(5);

        let mut bm1 = RoaringBitmap::new();
        bm1.insert(1);
        bm1.insert(2);
        pending.push(BucketDiff {
            cutoff_before: 100,
            cutoff_after: 200,
            expired: Arc::new(bm1),
        });

        let mut bm2 = RoaringBitmap::new();
        bm2.insert(3);
        bm2.insert(4);
        pending.push(BucketDiff {
            cutoff_before: 200,
            cutoff_after: 300,
            expired: Arc::new(bm2),
        });

        assert_eq!(pending.len(), 2);
        assert_eq!(pending.current_cutoff(), 300);
        assert_eq!(pending.oldest_cutoff(), 100);
        assert_eq!(pending.merged_expired().len(), 4);
        assert!(pending.merged_expired().contains(1));
        assert!(pending.merged_expired().contains(4));
    }

    #[test]
    fn test_pending_diffs_retention() {
        let mut pending = PendingBucketDiffs::new(3);

        for i in 0..5u64 {
            let mut bm = RoaringBitmap::new();
            bm.insert(i as u32);
            pending.push(BucketDiff {
                cutoff_before: i * 100,
                cutoff_after: (i + 1) * 100,
                expired: Arc::new(bm),
            });
        }

        assert_eq!(pending.len(), 3);
        assert_eq!(pending.oldest_cutoff(), 200); // oldest retained
        assert_eq!(pending.current_cutoff(), 500);
        // Merged should only contain slots 2, 3, 4 (0 and 1 were trimmed)
        assert!(!pending.merged_expired().contains(0));
        assert!(!pending.merged_expired().contains(1));
        assert!(pending.merged_expired().contains(2));
        assert!(pending.merged_expired().contains(3));
        assert!(pending.merged_expired().contains(4));
    }

    #[test]
    fn test_log_append_and_read() {
        let path = temp_log_path();
        let log = BucketDiffLog::new(path.clone(), 100, 0.3);

        let mut bm = RoaringBitmap::new();
        bm.insert(42);
        bm.insert(100);

        log.append(&BucketDiff {
            cutoff_before: 1000,
            cutoff_after: 1300,
            expired: Arc::new(bm),
        }).unwrap();

        let entries = log.read_all().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].cutoff_before, 1000);
        assert_eq!(entries[0].cutoff_after, 1300);
        assert!(entries[0].expired.contains(42));
        assert!(entries[0].expired.contains(100));
        assert_eq!(entries[0].expired.len(), 2);
    }

    #[test]
    fn test_log_multiple_appends() {
        let path = temp_log_path();
        let log = BucketDiffLog::new(path.clone(), 100, 0.3);

        for i in 0..5u64 {
            let mut bm = RoaringBitmap::new();
            bm.insert(i as u32);
            log.append(&BucketDiff {
                cutoff_before: i * 300,
                cutoff_after: (i + 1) * 300,
                expired: Arc::new(bm),
            }).unwrap();
        }

        let entries = log.read_all().unwrap();
        assert_eq!(entries.len(), 5);
        assert_eq!(entries[0].cutoff_before, 0);
        assert_eq!(entries[4].cutoff_after, 1500);
    }

    #[test]
    fn test_log_compaction() {
        let path = temp_log_path();
        let log = BucketDiffLog::new(path.clone(), 3, 0.3); // max 3, compact at 4

        for i in 0..5u64 {
            let mut bm = RoaringBitmap::new();
            bm.insert(i as u32);
            log.append(&BucketDiff {
                cutoff_before: i * 300,
                cutoff_after: (i + 1) * 300,
                expired: Arc::new(bm),
            }).unwrap();
        }

        assert_eq!(log.entry_count().unwrap(), 5);

        let compacted = log.compact_if_needed().unwrap();
        assert!(compacted);

        let entries = log.read_all().unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].cutoff_before, 600); // oldest retained
        assert_eq!(entries[2].cutoff_after, 1500); // newest
    }

    #[test]
    fn test_log_read_retained() {
        let path = temp_log_path();
        let log = BucketDiffLog::new(path.clone(), 2, 0.3);

        for i in 0..5u64 {
            let mut bm = RoaringBitmap::new();
            bm.insert(i as u32);
            log.append(&BucketDiff {
                cutoff_before: i * 300,
                cutoff_after: (i + 1) * 300,
                expired: Arc::new(bm),
            }).unwrap();
        }

        let retained = log.read_retained().unwrap();
        assert_eq!(retained.len(), 2);
        assert_eq!(retained[0].cutoff_before, 900);
        assert_eq!(retained[1].cutoff_after, 1500);
    }

    #[test]
    fn test_log_empty_file() {
        let path = temp_log_path();
        let log = BucketDiffLog::new(path.clone(), 100, 0.3);

        let entries = log.read_all().unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_log_nonexistent_file() {
        let log = BucketDiffLog::new(PathBuf::from("/tmp/nonexistent_bucket_diffs.log"), 100, 0.3);
        let entries = log.read_all().unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_log_truncated_entry_discarded() {
        let path = temp_log_path();
        let log = BucketDiffLog::new(path.clone(), 100, 0.3);

        // Write a valid entry
        let mut bm = RoaringBitmap::new();
        bm.insert(1);
        log.append(&BucketDiff {
            cutoff_before: 100,
            cutoff_after: 200,
            expired: Arc::new(bm),
        }).unwrap();

        // Append garbage (simulates partial write)
        let mut file = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&[0xFF; 10]).unwrap();

        // Should still read the valid entry
        let entries = log.read_all().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].cutoff_before, 100);
    }
}
