//! Ops WAL — append-only log for sync operations.
//!
//! Format per record:
//!   [4 bytes: payload_len (u32 LE)]
//!   [8 bytes: entity_id (i64 LE)]
//!   [1 byte:  flags (bit 0 = creates_slot)]
//!   [payload_len bytes: ops JSONB]
//!   [4 bytes: CRC32 of entity_id + flags + ops]
//!
//! The writer appends records and fsyncs. The reader tails the file,
//! reading batches of records and tracking a byte-offset cursor.
//! Partial records at EOF are skipped (crash recovery).

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use crate::pg_sync::ops::{EntityOps, Op};

const HEADER_SIZE: usize = 4 + 8 + 1; // payload_len + entity_id + flags
const FLAG_CREATES_SLOT: u8 = 0x01;
const CRC_SIZE: usize = 4;

/// WAL writer — appends ops records to a file with CRC32 integrity.
pub struct WalWriter {
    path: PathBuf,
}

impl WalWriter {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// Append a batch of entity ops to the WAL. Writes all records and fsyncs.
    /// Returns the number of bytes written.
    pub fn append_batch(&self, batch: &[EntityOps]) -> io::Result<u64> {
        if batch.is_empty() {
            return Ok(0);
        }

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;

        let mut total_bytes = 0u64;
        for entry in batch {
            let ops_json = serde_json::to_vec(&entry.ops)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            let payload_len = ops_json.len() as u32;
            let entity_id_bytes = entry.entity_id.to_le_bytes();
            let flags: u8 = if entry.creates_slot { FLAG_CREATES_SLOT } else { 0 };

            // CRC covers entity_id + flags + ops (not the length prefix)
            let mut crc_input = Vec::with_capacity(9 + ops_json.len());
            crc_input.extend_from_slice(&entity_id_bytes);
            crc_input.push(flags);
            crc_input.extend_from_slice(&ops_json);
            let crc = crc32fast::hash(&crc_input);

            // Write: [len][entity_id][flags][ops][crc]
            file.write_all(&payload_len.to_le_bytes())?;
            file.write_all(&entity_id_bytes)?;
            file.write_all(&[flags])?;
            file.write_all(&ops_json)?;
            file.write_all(&crc.to_le_bytes())?;

            total_bytes += (HEADER_SIZE + ops_json.len() + CRC_SIZE) as u64;
        }

        file.sync_all()?;
        Ok(total_bytes)
    }

    /// Get the file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get current file size (0 if file doesn't exist).
    pub fn file_size(&self) -> u64 {
        fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0)
    }
}

/// WAL reader — reads ops records from a file starting at a byte offset.
pub struct WalReader {
    path: PathBuf,
    /// Current read position (byte offset into the file)
    cursor: u64,
}

/// Result of reading a batch from the WAL.
pub struct WalBatch {
    /// The ops read from the WAL
    pub entries: Vec<EntityOps>,
    /// New cursor position after this batch
    pub new_cursor: u64,
    /// Number of bytes read
    pub bytes_read: u64,
    /// Number of records skipped due to CRC failure
    pub crc_failures: u64,
}

impl WalReader {
    pub fn new(path: impl Into<PathBuf>, cursor: u64) -> Self {
        Self {
            path: path.into(),
            cursor,
        }
    }

    /// Read up to `max_records` from the WAL starting at the current cursor.
    /// Advances the cursor past successfully read records.
    /// Stops at EOF or on partial/corrupted records.
    pub fn read_batch(&mut self, max_records: usize) -> io::Result<WalBatch> {
        if !self.path.exists() {
            return Ok(WalBatch {
                entries: Vec::new(),
                new_cursor: self.cursor,
                bytes_read: 0,
                crc_failures: 0,
            });
        }

        let data = fs::read(&self.path)?;
        let mut entries = Vec::new();
        let mut pos = self.cursor as usize;
        let mut crc_failures = 0u64;
        let start_pos = pos;

        while entries.len() < max_records && pos + HEADER_SIZE <= data.len() {
            // Read header: [4-byte len][8-byte entity_id][1-byte flags]
            let payload_len =
                u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            let entity_id =
                i64::from_le_bytes(data[pos + 4..pos + 12].try_into().unwrap());
            let flags = data[pos + 12];
            let creates_slot = (flags & FLAG_CREATES_SLOT) != 0;

            let record_end = pos + HEADER_SIZE + payload_len + CRC_SIZE;
            if record_end > data.len() {
                // Truncated record at EOF — stop here, don't advance cursor
                break;
            }

            // Verify CRC (covers entity_id + flags + ops)
            let crc_input = &data[pos + 4..pos + HEADER_SIZE + payload_len];
            let stored_crc = u32::from_le_bytes(
                data[pos + HEADER_SIZE + payload_len..record_end]
                    .try_into()
                    .unwrap(),
            );
            let computed_crc = crc32fast::hash(crc_input);

            if stored_crc != computed_crc {
                // CRC failure — skip this record
                crc_failures += 1;
                pos = record_end;
                continue;
            }

            // Parse ops JSON
            let ops_data = &data[pos + HEADER_SIZE..pos + HEADER_SIZE + payload_len];
            match serde_json::from_slice::<Vec<Op>>(ops_data) {
                Ok(ops) => {
                    entries.push(EntityOps { entity_id, ops, creates_slot });
                }
                Err(_) => {
                    // Invalid JSON — skip
                    crc_failures += 1;
                }
            }

            pos = record_end;
        }

        let bytes_read = (pos - start_pos) as u64;
        self.cursor = pos as u64;

        Ok(WalBatch {
            entries,
            new_cursor: self.cursor,
            bytes_read,
            crc_failures,
        })
    }

    /// Get the current cursor position.
    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// Set the cursor position (for recovery from persisted state).
    pub fn set_cursor(&mut self, cursor: u64) {
        self.cursor = cursor;
    }

    /// Check if there are more records to read (cursor < file size).
    pub fn has_more(&self) -> bool {
        let file_size = fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0);
        self.cursor < file_size
    }
}

/// Delete a WAL file.
pub fn remove_wal(path: &Path) -> io::Result<()> {
    if path.exists() {
        fs::remove_file(path)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;

    fn make_ops(entity_id: i64, ops: Vec<Op>) -> EntityOps {
        EntityOps { entity_id, ops, creates_slot: false }
    }

    #[test]
    fn test_write_read_roundtrip() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");

        let writer = WalWriter::new(&wal_path);
        let batch = vec![
            make_ops(1, vec![Op::Set { field: "nsfwLevel".into(), value: json!(16) }]),
            make_ops(2, vec![Op::Add { field: "tagIds".into(), value: json!(42) }]),
            make_ops(3, vec![Op::Delete]),
        ];
        let bytes = writer.append_batch(&batch).unwrap();
        assert!(bytes > 0);

        let mut reader = WalReader::new(&wal_path, 0);
        let result = reader.read_batch(100).unwrap();
        assert_eq!(result.entries.len(), 3);
        assert_eq!(result.entries[0].entity_id, 1);
        assert_eq!(result.entries[1].entity_id, 2);
        assert_eq!(result.entries[2].entity_id, 3);
        assert_eq!(result.crc_failures, 0);
        assert!(!reader.has_more());
    }

    #[test]
    fn test_multiple_appends() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");
        let writer = WalWriter::new(&wal_path);

        // First batch
        writer.append_batch(&[
            make_ops(1, vec![Op::Set { field: "a".into(), value: json!(1) }]),
        ]).unwrap();

        // Second batch
        writer.append_batch(&[
            make_ops(2, vec![Op::Set { field: "b".into(), value: json!(2) }]),
        ]).unwrap();

        let mut reader = WalReader::new(&wal_path, 0);
        let result = reader.read_batch(100).unwrap();
        assert_eq!(result.entries.len(), 2);
        assert_eq!(result.entries[0].entity_id, 1);
        assert_eq!(result.entries[1].entity_id, 2);
    }

    #[test]
    fn test_cursor_resume() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");
        let writer = WalWriter::new(&wal_path);

        writer.append_batch(&[
            make_ops(1, vec![Op::Set { field: "a".into(), value: json!(1) }]),
            make_ops(2, vec![Op::Set { field: "b".into(), value: json!(2) }]),
            make_ops(3, vec![Op::Set { field: "c".into(), value: json!(3) }]),
        ]).unwrap();

        // Read first 2
        let mut reader = WalReader::new(&wal_path, 0);
        let result = reader.read_batch(2).unwrap();
        assert_eq!(result.entries.len(), 2);
        let saved_cursor = reader.cursor();

        // Resume from cursor — should get the 3rd
        let mut reader2 = WalReader::new(&wal_path, saved_cursor);
        let result2 = reader2.read_batch(100).unwrap();
        assert_eq!(result2.entries.len(), 1);
        assert_eq!(result2.entries[0].entity_id, 3);
        assert!(!reader2.has_more());
    }

    #[test]
    fn test_truncated_record_at_eof() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");
        let writer = WalWriter::new(&wal_path);

        writer.append_batch(&[
            make_ops(1, vec![Op::Set { field: "a".into(), value: json!(1) }]),
        ]).unwrap();

        // Append garbage (partial record)
        let mut file = OpenOptions::new().append(true).open(&wal_path).unwrap();
        file.write_all(&[0u8; 6]).unwrap(); // Too short to be a valid header+payload

        let mut reader = WalReader::new(&wal_path, 0);
        let result = reader.read_batch(100).unwrap();
        // Should read the valid record and stop at the truncated one
        assert_eq!(result.entries.len(), 1);
        assert_eq!(result.crc_failures, 0);
    }

    #[test]
    fn test_corrupted_crc_skipped() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");
        let writer = WalWriter::new(&wal_path);

        writer.append_batch(&[
            make_ops(1, vec![Op::Set { field: "a".into(), value: json!(1) }]),
            make_ops(2, vec![Op::Set { field: "b".into(), value: json!(2) }]),
        ]).unwrap();

        // Corrupt the CRC of the first record
        let mut data = fs::read(&wal_path).unwrap();
        // First record: header(12) + ops_json + crc(4)
        // Find where the CRC is for the first record
        let payload_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let crc_offset = HEADER_SIZE + payload_len;
        data[crc_offset] ^= 0xFF; // Flip bits in CRC
        fs::write(&wal_path, &data).unwrap();

        let mut reader = WalReader::new(&wal_path, 0);
        let result = reader.read_batch(100).unwrap();
        // First record should be skipped (CRC failure), second should be read
        assert_eq!(result.entries.len(), 1);
        assert_eq!(result.entries[0].entity_id, 2);
        assert_eq!(result.crc_failures, 1);
    }

    #[test]
    fn test_empty_file() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");

        let mut reader = WalReader::new(&wal_path, 0);
        let result = reader.read_batch(100).unwrap();
        assert_eq!(result.entries.len(), 0);
        assert!(!reader.has_more());
    }

    #[test]
    fn test_query_op_set_roundtrip() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");
        let writer = WalWriter::new(&wal_path);

        writer.append_batch(&[make_ops(456, vec![
            Op::QueryOpSet {
                query: "modelVersionIds eq 456".into(),
                ops: vec![
                    Op::Remove { field: "baseModel".into(), value: json!("SD 1.5") },
                    Op::Set { field: "baseModel".into(), value: json!("SDXL") },
                ],
            },
        ])]).unwrap();

        let mut reader = WalReader::new(&wal_path, 0);
        let result = reader.read_batch(100).unwrap();
        assert_eq!(result.entries.len(), 1);
        assert_eq!(result.entries[0].entity_id, 456);
        match &result.entries[0].ops[0] {
            Op::QueryOpSet { query, ops } => {
                assert_eq!(query, "modelVersionIds eq 456");
                assert_eq!(ops.len(), 2);
            }
            _ => panic!("Expected QueryOpSet"),
        }
    }

    #[test]
    fn test_file_size_tracking() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");
        let writer = WalWriter::new(&wal_path);

        assert_eq!(writer.file_size(), 0);

        writer.append_batch(&[
            make_ops(1, vec![Op::Delete]),
        ]).unwrap();

        assert!(writer.file_size() > 0);
    }

    #[test]
    fn test_remove_wal() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");

        let writer = WalWriter::new(&wal_path);
        writer.append_batch(&[make_ops(1, vec![Op::Delete])]).unwrap();
        assert!(wal_path.exists());

        remove_wal(&wal_path).unwrap();
        assert!(!wal_path.exists());

        // Remove non-existent is ok
        remove_wal(&wal_path).unwrap();
    }
}
