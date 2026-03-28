//! Data Silo — per-thread append-only document storage with mmap reads.
//!
//! Replaces the 210K-shard DocStore V2 with N large files (one per CPU thread).
//! Bulk writes are zero-contention (each thread owns its file). Reads use mmap
//! with a flat index for O(1) slot→(file, offset, length) lookup at 7ns/read.
//!
//! Architecture:
//!   Bulk load: N threads → N silo files + N local indexes → merge → global index
//!   Read:      index[slot_id] → (file_id, offset, length) → mmap slice
//!   Upsert:    append to active silo → update index → old data = dead space
//!   Compact:   rewrite silo excluding dead entries (deferred, <1% dead space)

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use memmap2::Mmap;

// ---------------------------------------------------------------------------
// DocDataFile — append-only data file
// ---------------------------------------------------------------------------

/// A single silo data file. Wraps a File handle with BufWriter for sequential
/// append-only writes during bulk load.
pub struct DocDataFile {
    writer: BufWriter<File>,
    offset: u64,
    path: PathBuf,
}

impl DocDataFile {
    /// Create a new silo data file. Pre-creates the file on disk.
    pub fn create(path: &Path) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = File::create(path)?;
        Ok(Self {
            writer: BufWriter::with_capacity(8 * 1024 * 1024, file), // 8MB buffer (benchmark-validated)
            offset: 0,
            path: path.to_path_buf(),
        })
    }

    /// Append raw bytes to the silo. Returns (offset, length) for the index.
    #[inline]
    pub fn append(&mut self, data: &[u8]) -> io::Result<(u64, u32)> {
        let offset = self.offset;
        let len = data.len() as u32;
        self.writer.write_all(data)?;
        self.offset += len as u64;
        Ok((offset, len))
    }

    /// Flush the BufWriter to disk.
    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }

    /// Current write offset (total bytes written).
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Path to this silo file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl DocDataFile {
    /// Open an existing silo file for appending (steady-state upserts).
    pub fn open_for_append(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().append(true).open(path)?;
        let offset = file.metadata()?.len();
        Ok(Self {
            writer: BufWriter::with_capacity(64 * 1024, file),
            offset,
            path: path.to_path_buf(),
        })
    }
}

/// Memory-map a silo file for reads.
pub fn mmap_silo(path: &Path) -> io::Result<Mmap> {
    let file = File::open(path)?;
    // Safety: we only read from the mmap'd region, and the file is not
    // modified while mmap'd (bulk load flushes before mmap, upserts
    // append beyond the mmap'd region).
    unsafe { Mmap::map(&file) }
}

// ---------------------------------------------------------------------------
// DocIndex — flat slot→(file_id, offset, length) lookup
// ---------------------------------------------------------------------------

/// Entry in the document index: where to find a slot's document data.
#[derive(Clone, Copy, Debug, Default)]
pub struct IndexEntry {
    pub file_id: u8,
    pub offset: u64,
    pub length: u32,
}

impl IndexEntry {
    pub const SIZE: usize = 13; // 1 + 8 + 4

    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    fn encode(&self, buf: &mut Vec<u8>) {
        buf.push(self.file_id);
        buf.extend_from_slice(&self.offset.to_le_bytes());
        buf.extend_from_slice(&self.length.to_le_bytes());
    }

    fn decode(data: &[u8]) -> Self {
        debug_assert!(data.len() >= Self::SIZE);
        Self {
            file_id: data[0],
            offset: u64::from_le_bytes(data[1..9].try_into().unwrap()),
            length: u32::from_le_bytes(data[9..13].try_into().unwrap()),
        }
    }
}

/// Flat index mapping slot_id → IndexEntry. Indexed by slot_id directly.
/// At 107M entries × 13 bytes = ~1.4 GB.
pub struct DocIndex {
    entries: Vec<IndexEntry>,
}

impl DocIndex {
    /// Create an empty index with capacity for slots 0..=max_slot.
    pub fn new(max_slot: u32) -> Self {
        Self {
            entries: vec![IndexEntry::default(); max_slot as usize + 1],
        }
    }

    /// Look up a slot's document location. Returns None if slot has no entry.
    #[inline]
    pub fn get(&self, slot_id: u32) -> Option<&IndexEntry> {
        let entry = self.entries.get(slot_id as usize)?;
        if entry.is_empty() { None } else { Some(entry) }
    }

    /// Set a slot's document location.
    #[inline]
    pub fn set(&mut self, slot_id: u32, file_id: u8, offset: u64, length: u32) {
        let idx = slot_id as usize;
        if idx >= self.entries.len() {
            self.entries.resize(idx + 1, IndexEntry::default());
        }
        self.entries[idx] = IndexEntry { file_id, offset, length };
    }

    /// Number of non-empty entries.
    pub fn count(&self) -> usize {
        self.entries.iter().filter(|e| !e.is_empty()).count()
    }

    /// Total capacity (max slot_id + 1).
    pub fn capacity(&self) -> usize {
        self.entries.len()
    }

    /// Persist the index to a binary file.
    /// Format: [u32 num_entries][N × 13 bytes (file_id, offset, length)]
    pub fn persist(&self, path: &Path) -> io::Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let num_entries = self.entries.len() as u32;
        let mut buf = Vec::with_capacity(4 + self.entries.len() * IndexEntry::SIZE);
        buf.extend_from_slice(&num_entries.to_le_bytes());
        for entry in &self.entries {
            entry.encode(&mut buf);
        }
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, &buf)?;
        fs::rename(&tmp, path)?;
        Ok(())
    }

    /// Load a persisted index from disk.
    pub fn load(path: &Path) -> io::Result<Self> {
        let data = fs::read(path)?;
        if data.len() < 4 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "index too short"));
        }
        let num_entries = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let expected = 4 + num_entries * IndexEntry::SIZE;
        if data.len() < expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("index truncated: expected {} bytes, got {}", expected, data.len()),
            ));
        }
        let mut entries = Vec::with_capacity(num_entries);
        for i in 0..num_entries {
            let start = 4 + i * IndexEntry::SIZE;
            entries.push(IndexEntry::decode(&data[start..start + IndexEntry::SIZE]));
        }
        Ok(Self { entries })
    }
}

// ---------------------------------------------------------------------------
// BulkDocWriter — per-thread staging for dump processor
// ---------------------------------------------------------------------------

/// Per-thread document writer for bulk load. Each rayon thread owns one of
/// these, writing to its dedicated silo file with zero contention.
pub struct BulkDocWriter {
    file: DocDataFile,
    file_id: u8,
    local_index: Vec<(u32, u64, u32)>, // (slot_id, offset, length)
}

impl BulkDocWriter {
    /// Create a new bulk writer for a specific thread/file.
    pub fn new(file: DocDataFile, file_id: u8) -> Self {
        Self {
            file,
            file_id,
            local_index: Vec::with_capacity(1_000_000),
        }
    }

    /// Append a document for a slot. Returns the (offset, length) written.
    #[inline]
    pub fn append(&mut self, slot_id: u32, data: &[u8]) -> io::Result<(u64, u32)> {
        let (offset, length) = self.file.append(data)?;
        self.local_index.push((slot_id, offset, length));
        Ok((offset, length))
    }

    /// Flush the underlying file.
    pub fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }

    /// Consume this writer, returning the local index entries and file_id.
    pub fn into_local_index(mut self) -> io::Result<(u8, Vec<(u32, u64, u32)>)> {
        self.file.flush()?;
        Ok((self.file_id, self.local_index))
    }

    /// File ID for this writer's silo.
    pub fn file_id(&self) -> u8 {
        self.file_id
    }

    /// Bytes written so far.
    pub fn bytes_written(&self) -> u64 {
        self.file.offset()
    }
}

/// Pre-create N silo files and return BulkDocWriters for each.
pub fn create_bulk_writers(silo_dir: &Path, count: usize) -> io::Result<Vec<BulkDocWriter>> {
    fs::create_dir_all(silo_dir)?;
    let mut writers = Vec::with_capacity(count);
    for i in 0..count {
        let path = silo_dir.join(format!("silo_{:02}.dat", i));
        let file = DocDataFile::create(&path)?;
        writers.push(BulkDocWriter::new(file, i as u8));
    }
    Ok(writers)
}

/// Merge per-thread local indexes into a single DocIndex.
/// Note: if multiple threads wrote the same slot_id, the last one wins.
/// This is correct for single-phase bulk loads where each slot is processed
/// by exactly one thread. For cross-phase merging, use read-merge-write instead.
pub fn merge_indexes(locals: Vec<(u8, Vec<(u32, u64, u32)>)>, max_slot: u32) -> DocIndex {
    let mut index = DocIndex::new(max_slot);
    for (file_id, entries) in locals {
        for (slot_id, offset, length) in entries {
            index.set(slot_id, file_id, offset, length);
        }
    }
    index
}

// ---------------------------------------------------------------------------
// DataSiloReader — mmap-based read path
// ---------------------------------------------------------------------------

/// Reader for silo data files. Holds mmap'd silo files + loaded index.
pub struct DataSiloReader {
    index: DocIndex,
    silos: Vec<Mmap>,
}

impl DataSiloReader {
    /// Open a silo reader from a directory containing silo files + index.
    pub fn open(silo_dir: &Path) -> io::Result<Self> {
        let index_path = silo_dir.join("doc_index.bin");
        let index = DocIndex::load(&index_path)?;

        // Discover and mmap all silo files in order
        let mut silos = Vec::new();
        for i in 0u8.. {
            let path = silo_dir.join(format!("silo_{:02}.dat", i));
            if !path.exists() {
                break;
            }
            silos.push(mmap_silo(&path)?);
        }

        Ok(Self { index, silos })
    }

    /// Look up a document by slot_id. Returns the raw bytes or None.
    #[inline]
    pub fn get(&self, slot_id: u32) -> Option<&[u8]> {
        let entry = self.index.get(slot_id)?;
        let silo = self.silos.get(entry.file_id as usize)?;
        let start = entry.offset as usize;
        let end = start + entry.length as usize;
        if end <= silo.len() {
            Some(&silo[start..end])
        } else {
            None
        }
    }

    /// Number of indexed documents.
    pub fn count(&self) -> usize {
        self.index.count()
    }

    /// Number of silo files.
    pub fn silo_count(&self) -> usize {
        self.silos.len()
    }
}

// ---------------------------------------------------------------------------
// DataSiloWriter — steady-state single-doc upsert path
// ---------------------------------------------------------------------------

/// Writer for steady-state single-document upserts. Appends to the last silo
/// file in a directory, updating the shared index so the new offset wins.
/// The old data at the previous offset becomes dead space — compaction reclaims
/// it eventually, but correctness is not affected because reads always use the
/// index-tracked offset.
pub struct DataSiloWriter {
    file: DocDataFile,
    file_id: u8,
    index: DocIndex,
    silo_dir: PathBuf,
}

impl DataSiloWriter {
    /// Open a `DataSiloWriter` for the given silo directory.
    ///
    /// Loads the existing `doc_index.bin` (if present) and opens the last silo
    /// file for appending.  If the directory is empty (first write after a
    /// brand-new bulk-load hasn't run yet), it creates `silo_00.dat` and an
    /// empty index.
    pub fn open(silo_dir: &Path) -> io::Result<Self> {
        fs::create_dir_all(silo_dir)?;

        // Load existing index, or start with a fresh empty one.
        let index_path = silo_dir.join("doc_index.bin");
        let index = if index_path.exists() {
            DocIndex::load(&index_path)?
        } else {
            DocIndex::new(0)
        };

        // Find the highest-numbered silo file that exists, or create silo_00.
        let mut last_id = 0u8;
        loop {
            let next_path = silo_dir.join(format!("silo_{:02}.dat", last_id + 1));
            if next_path.exists() {
                last_id += 1;
            } else {
                break;
            }
        }

        let silo_path = silo_dir.join(format!("silo_{:02}.dat", last_id));
        let file = if silo_path.exists() {
            DocDataFile::open_for_append(&silo_path)?
        } else {
            DocDataFile::create(&silo_path)?
        };

        Ok(Self {
            file,
            file_id: last_id,
            index,
            silo_dir: silo_dir.to_path_buf(),
        })
    }

    /// Upsert a document for `slot`. Appends `data` to the active silo and
    /// updates the index entry.  The previous offset (if any) becomes dead
    /// space — it is no longer reachable via the index.
    pub fn upsert(&mut self, slot: u32, data: &[u8]) -> io::Result<()> {
        let (offset, length) = self.file.append(data)?;
        self.index.set(slot, self.file_id, offset, length);
        Ok(())
    }

    /// Flush the underlying silo file write buffer to the OS.
    pub fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }

    /// Atomically persist the in-memory index to `<silo_dir>/doc_index.bin`.
    /// Uses a temp-file + rename so a crash mid-write never corrupts the index.
    pub fn persist_index(&self) -> io::Result<()> {
        let index_path = self.silo_dir.join("doc_index.bin");
        self.index.persist(&index_path)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    fn test_dir() -> PathBuf {
        let dir = std::env::temp_dir().join("bitdex_data_silo_test");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn test_doc_data_file_append_and_read() {
        let dir = test_dir();
        let path = dir.join("test.dat");

        let mut file = DocDataFile::create(&path).unwrap();
        let (off1, len1) = file.append(b"hello").unwrap();
        let (off2, len2) = file.append(b"world").unwrap();
        file.flush().unwrap();

        assert_eq!(off1, 0);
        assert_eq!(len1, 5);
        assert_eq!(off2, 5);
        assert_eq!(len2, 5);

        // Drop the file handle before mmap (Windows requires exclusive access)
        drop(file);

        // Read back via mmap
        let mmap = mmap_silo(&path).unwrap();
        assert_eq!(&mmap[0..5], b"hello");
        assert_eq!(&mmap[5..10], b"world");
    }

    #[test]
    fn test_doc_index_persist_load() {
        let dir = test_dir();
        let path = dir.join("index.bin");

        let mut index = DocIndex::new(100);
        index.set(0, 0, 0, 5);
        index.set(50, 1, 1000, 200);
        index.set(99, 2, 5000, 50);
        assert_eq!(index.count(), 3);

        index.persist(&path).unwrap();
        let loaded = DocIndex::load(&path).unwrap();
        assert_eq!(loaded.capacity(), 101); // new(100) creates slots 0..=100
        assert_eq!(loaded.count(), 3);

        let e0 = loaded.get(0).unwrap();
        assert_eq!(e0.file_id, 0);
        assert_eq!(e0.offset, 0);
        assert_eq!(e0.length, 5);

        let e50 = loaded.get(50).unwrap();
        assert_eq!(e50.file_id, 1);
        assert_eq!(e50.offset, 1000);
        assert_eq!(e50.length, 200);

        assert!(loaded.get(10).is_none());
    }

    #[test]
    fn test_bulk_writer_end_to_end() {
        let dir = test_dir();

        // Create 2 bulk writers (simulating 2 threads)
        let mut writers = create_bulk_writers(&dir, 2).unwrap();

        // Thread 0 writes slots 0, 2, 4
        writers[0].append(0, b"doc_zero").unwrap();
        writers[0].append(2, b"doc_two").unwrap();
        writers[0].append(4, b"doc_four").unwrap();

        // Thread 1 writes slots 1, 3
        writers[1].append(1, b"doc_one").unwrap();
        writers[1].append(3, b"doc_three").unwrap();

        // Collect local indexes
        let locals: Vec<_> = writers.into_iter()
            .map(|w| w.into_local_index().unwrap())
            .collect();

        // Merge
        let index = merge_indexes(locals, 5);
        assert_eq!(index.count(), 5);

        // Persist
        index.persist(&dir.join("doc_index.bin")).unwrap();

        // Read back
        let reader = DataSiloReader::open(&dir).unwrap();
        assert_eq!(reader.count(), 5);
        assert_eq!(reader.silo_count(), 2);
        assert_eq!(reader.get(0).unwrap(), b"doc_zero");
        assert_eq!(reader.get(1).unwrap(), b"doc_one");
        assert_eq!(reader.get(2).unwrap(), b"doc_two");
        assert_eq!(reader.get(3).unwrap(), b"doc_three");
        assert_eq!(reader.get(4).unwrap(), b"doc_four");
        assert!(reader.get(5).is_none());
    }

    #[test]
    fn test_index_entry_encode_decode() {
        let entry = IndexEntry { file_id: 7, offset: 123456789, length: 42 };
        let mut buf = Vec::new();
        entry.encode(&mut buf);
        assert_eq!(buf.len(), IndexEntry::SIZE);
        let decoded = IndexEntry::decode(&buf);
        assert_eq!(decoded.file_id, 7);
        assert_eq!(decoded.offset, 123456789);
        assert_eq!(decoded.length, 42);
    }

    #[test]
    fn test_empty_index() {
        let index = DocIndex::new(0);
        assert_eq!(index.count(), 0);
        assert!(index.get(0).is_none());
    }

    #[test]
    fn test_index_auto_grow() {
        let mut index = DocIndex::new(10);
        index.set(100, 0, 0, 5); // Beyond initial capacity
        assert!(index.get(100).is_some());
        assert_eq!(index.capacity(), 101);
    }

    /// Roundtrip test: write PackedValue::Mi msgpack payloads via BulkDocWriter,
    /// persist the index, then read back via DataSiloReader and decode.
    #[test]
    fn test_msgpack_packed_value_roundtrip() {
        use crate::docstore::PackedValue;

        let dir = std::env::temp_dir().join("bitdex_data_silo_msgpack_test");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let test_cases: &[(u32, i64)] = &[
            (1, 100), (2, 200), (3, 999), (42, 31337),
        ];

        let silo_path = dir.join("silo_00.dat");
        let doc_file = DocDataFile::create(&silo_path).unwrap();
        let mut writer = BulkDocWriter::new(doc_file, 0u8);

        let mut buf = Vec::with_capacity(32);
        for &(slot, value) in test_cases {
            buf.clear();
            rmp_serde::encode::write(&mut buf, &PackedValue::Mi(vec![value])).unwrap();
            writer.append(slot, &buf).unwrap();
        }

        let (file_id, local_entries) = writer.into_local_index().unwrap();
        let max_slot = local_entries.iter().map(|(s, _, _)| *s).max().unwrap_or(0);
        let mut index = DocIndex::new(max_slot);
        for (slot_id, offset, length) in local_entries {
            index.set(slot_id, file_id, offset, length);
        }
        index.persist(&dir.join("doc_index.bin")).unwrap();

        let reader = DataSiloReader::open(&dir).unwrap();
        assert_eq!(reader.count(), test_cases.len());

        for &(slot, expected_value) in test_cases {
            let bytes = reader.get(slot).expect("slot should be present");
            let decoded: PackedValue = rmp_serde::from_slice(bytes)
                .unwrap_or_else(|e| panic!("slot {}: msgpack decode failed: {e}", slot));
            match decoded {
                PackedValue::Mi(vals) => {
                    assert_eq!(vals.len(), 1);
                    assert_eq!(vals[0], expected_value);
                }
                other => panic!("slot {}: expected PackedValue::Mi, got {:?}", slot, other),
            }
        }
    }

    // -----------------------------------------------------------------------
    // DataSiloWriter tests
    // -----------------------------------------------------------------------

    /// Upsert into an empty slot, then read back via DataSiloReader.
    #[test]
    fn test_upsert_new_doc() {
        let dir = std::env::temp_dir().join("bitdex_data_silo_upsert_new");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let data = b"new_document_payload";

        {
            let mut writer = DataSiloWriter::open(&dir).unwrap();
            writer.upsert(42, data).unwrap();
            writer.flush().unwrap();
            writer.persist_index().unwrap();
        }

        let reader = DataSiloReader::open(&dir).unwrap();
        assert_eq!(reader.count(), 1);
        let got = reader.get(42).expect("slot 42 should be present");
        assert_eq!(got, data);
        assert!(reader.get(0).is_none());
        assert!(reader.get(99).is_none());
    }

    /// Write a slot, then upsert the same slot with new data — latest wins.
    #[test]
    fn test_upsert_overwrite() {
        let dir = std::env::temp_dir().join("bitdex_data_silo_upsert_overwrite");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let first = b"original_data";
        let second = b"updated_data_longer";

        {
            let mut writer = DataSiloWriter::open(&dir).unwrap();
            writer.upsert(7, first).unwrap();
            writer.upsert(7, second).unwrap(); // same slot, new data
            writer.flush().unwrap();
            writer.persist_index().unwrap();
        }

        // Re-open to confirm the persisted index reflects the latest write.
        let reader = DataSiloReader::open(&dir).unwrap();
        // Index has exactly one entry for slot 7 (overwrite, not two separate entries).
        assert_eq!(reader.count(), 1);
        let got = reader.get(7).expect("slot 7 should be present");
        assert_eq!(got, second, "latest upsert should win");
    }

    /// After overwriting a slot, the old offset is unreachable via the index.
    #[test]
    fn test_upsert_dead_space() {
        let dir = std::env::temp_dir().join("bitdex_data_silo_upsert_dead");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let first = b"first_write_becomes_dead_space";
        let second = b"second_write_is_live";

        let first_offset;
        let second_offset;

        {
            let mut writer = DataSiloWriter::open(&dir).unwrap();
            // First write — records where old data lands.
            writer.upsert(5, first).unwrap();
            first_offset = writer.file.offset() - first.len() as u64;

            // Second write — overwrites the index entry for slot 5.
            writer.upsert(5, second).unwrap();
            second_offset = writer.file.offset() - second.len() as u64;

            writer.flush().unwrap();
            writer.persist_index().unwrap();
        }

        // The live index entry should point at the second write.
        let index_path = dir.join("doc_index.bin");
        let index = DocIndex::load(&index_path).unwrap();
        let entry = index.get(5).expect("slot 5 must be indexed");
        assert_eq!(entry.offset, second_offset, "index must point to second write");
        assert_ne!(entry.offset, first_offset, "old offset must not be in index");
        assert_eq!(entry.length as usize, second.len());

        // The first write's bytes are still physically on disk (dead space),
        // but the index no longer references them.
        let silo_path = dir.join("silo_00.dat");
        let raw = fs::read(&silo_path).unwrap();
        assert!(
            raw.windows(first.len()).any(|w| w == first),
            "old bytes should still be in silo file (dead space, not zeroed)"
        );
    }
}
