//! Append-only ops log with CRC32 per entry.
//!
//! Format: [u8 tag][u32 key][u32 value_len][value bytes][u32 crc32]
//! Tags: 0x01 = Put, 0x02 = Delete

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, Write};
use std::path::PathBuf;

const OP_TAG_PUT: u8 = 0x01;
const OP_TAG_DELETE: u8 = 0x02;

/// A mutation operation.
pub enum SiloOp {
    Put { key: u32, value: Vec<u8> },
    Delete { key: u32 },
}

/// Append-only ops log file.
pub struct OpsLog {
    path: PathBuf,
    writer: BufWriter<File>,
}

impl OpsLog {
    /// Open or create the ops log file.
    pub fn open(path: &PathBuf) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;
        Ok(Self {
            path: path.clone(),
            writer: BufWriter::with_capacity(65536, file),
        })
    }

    /// Append an op and sync to disk.
    pub fn append(&mut self, op: SiloOp) -> io::Result<()> {
        self.write_op(&op)?;
        self.writer.flush()?;
        Ok(())
    }

    /// Append an op without syncing (for batch use — call sync() after).
    pub fn append_no_sync(&mut self, op: SiloOp) -> io::Result<()> {
        self.write_op(&op)
    }

    /// Flush the write buffer to disk.
    pub fn sync(&mut self) -> io::Result<()> {
        self.writer.flush()
    }

    /// Read all ops from the log file (for replay on startup).
    pub fn read_all(&self) -> io::Result<Vec<SiloOp>> {
        let mut file = File::open(&self.path)?;
        let meta = file.metadata()?;
        if meta.len() == 0 {
            return Ok(Vec::new());
        }
        file.seek(io::SeekFrom::Start(0))?;
        let mut data = Vec::with_capacity(meta.len() as usize);
        file.read_to_end(&mut data)?;

        let mut ops = Vec::new();
        let mut pos = 0;

        while pos < data.len() {
            match Self::decode_op(&data, &mut pos) {
                Some(op) => ops.push(op),
                None => break, // Truncated entry — stop replay
            }
        }

        Ok(ops)
    }

    /// Clear the ops log (after compaction).
    pub fn truncate(&mut self) -> io::Result<()> {
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        self.writer = BufWriter::with_capacity(65536, file);
        Ok(())
    }

    // ---- Internal ----

    fn write_op(&mut self, op: &SiloOp) -> io::Result<()> {
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
        self.writer.write_all(&buf)?;
        self.writer.write_all(&crc.to_le_bytes())?;
        Ok(())
    }

    fn decode_op(data: &[u8], pos: &mut usize) -> Option<SiloOp> {
        if *pos >= data.len() { return None; }
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
                // Verify CRC
                let expected_crc = u32::from_le_bytes(data[*pos..*pos + 4].try_into().ok()?);
                *pos += 4;
                let actual_crc = crc32fast::hash(&data[entry_start..payload_end]);
                if actual_crc != expected_crc {
                    return None;
                }
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
                if actual_crc != expected_crc {
                    return None;
                }
                Some(SiloOp::Delete { key })
            }
            _ => None,
        }
    }
}
