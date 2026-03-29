#![allow(unexpected_cfgs)]
//! ShardStore — Unified storage engine for BitDex.
//!
//! Unified storage engine. Replaces DocStore V2 with a single generic system that supports:
//! - Shard-local ops logs (append-only mutations)
//! - Materialized snapshots (compacted state)
//! - Generation management (LIFO fall-through reads)
//! - Pluggable codecs (doc vs bitmap) and sharding strategies
//!
//! # Type Parameters
//!
//! `ShardStore<S, O, Sh>` where:
//! - `S: SnapshotCodec` — how to serialize/deserialize the snapshot section
//! - `O: OpCodec<Snapshot = S::Snapshot>` — how to serialize/deserialize ops, tied to snapshot type
//! - `Sh: ShardingStrategy` — how to map keys to shard file paths

use std::fmt;
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

// ---------------------------------------------------------------------------
// Codec traits
// ---------------------------------------------------------------------------

/// Encodes and decodes the materialized snapshot section of a shard file.
///
/// The snapshot represents the full state at compaction time. For docs, this is
/// a flattened document. For bitmaps, this is a serialized roaring bitmap.
pub trait SnapshotCodec: Send + Sync + 'static {
    /// The in-memory representation of a snapshot.
    type Snapshot: Send + Sync + Clone + fmt::Debug;

    /// Serialize a snapshot into bytes.
    fn encode(snapshot: &Self::Snapshot, buf: &mut Vec<u8>);

    /// Deserialize a snapshot from bytes.
    fn decode(bytes: &[u8]) -> io::Result<Self::Snapshot>;

    /// Return an empty/default snapshot (used when no snapshot section exists).
    fn empty() -> Self::Snapshot;
}

/// Encodes and decodes ops log entries and applies them to snapshots.
///
/// The `Snapshot` associated type MUST match the `SnapshotCodec::Snapshot` —
/// enforced at the `ShardStore` level via `O: OpCodec<Snapshot = S::Snapshot>`.
pub trait OpCodec: Send + Sync + 'static {
    /// The in-memory representation of a single operation.
    type Op: Send + Sync + Clone + fmt::Debug;

    /// The snapshot type this codec operates on.
    type Snapshot: Send + Sync + Clone;

    /// Serialize an op into bytes (excluding the length prefix and CRC).
    fn encode_op(op: &Self::Op, buf: &mut Vec<u8>);

    /// Deserialize an op from bytes (excluding the length prefix and CRC).
    fn decode_op(bytes: &[u8]) -> io::Result<Self::Op>;

    /// Apply a single op to a snapshot in-place.
    fn apply(snapshot: &mut Self::Snapshot, op: &Self::Op);
}

/// Maps logical keys to shard file paths on disk.
///
/// Each ShardingStrategy defines how data is distributed across files.
/// For docs: slot_id → hex-bucketed shard path.
/// For bitmaps: (field, value) → field dir + hex-bucketed pack file.
pub trait ShardingStrategy: Send + Sync + 'static {
    /// The key type used to locate a shard.
    type Key: Send + Sync + Clone + fmt::Debug + Eq + std::hash::Hash;

    /// Given a key and a generation root directory, return the shard file path.
    fn shard_path(&self, key: &Self::Key, gen_root: &Path) -> PathBuf;

    /// List all shard keys that exist in a generation directory.
    /// Used for compaction and enumeration.
    fn list_shards(&self, gen_root: &Path) -> io::Result<Vec<Self::Key>>;
}

// ---------------------------------------------------------------------------
// Shard file format constants
// ---------------------------------------------------------------------------

/// Magic bytes identifying a ShardStore file.
const SHARD_MAGIC: [u8; 4] = *b"BDSS"; // BitDex ShardStore

/// Current shard file format version.
pub(crate) const SHARD_VERSION: u32 = 1;

/// Shard file header size in bytes.
/// Layout:
///   [4] magic "BDSS"
///   [4] version (u32 LE)
///   [8] ops_section_offset (u64 LE) — byte offset where ops log begins
///   [4] snapshot_len (u32 LE) — length of snapshot section in bytes
///   [4] ops_count (u32 LE) — number of ops entries in the log
///   [4] flags (u32 LE) — reserved for future use
///   = 28 bytes total
pub(crate) const HEADER_SIZE: usize = 28;

/// Per-op entry overhead: [4] length + [4] crc32 = 8 bytes wrapping each op.
#[allow(dead_code)]
const OP_ENTRY_OVERHEAD: usize = 8;

/// Byte offset of the ops_count field within the header.
/// magic(4) + version(4) + ops_section_offset(8) + snapshot_len(4) = 20.
const HEADER_OPS_COUNT_OFFSET: u64 = 20;

/// Default janitor compaction threshold: compact when ops_count exceeds this.
/// Based on Ollie's final microbench results: 2x read overhead at 1,000 ops
/// is acceptable. Configurable per-field: tagIds tolerates 50K+, low-cardinality
/// fields like nsfwLevel should compact at ~5K.
pub const DEFAULT_COMPACT_THRESHOLD: u32 = 500; // low-latency preset (was 1_000)

// ---------------------------------------------------------------------------
// Shard file header
// ---------------------------------------------------------------------------

/// Parsed shard file header.
#[derive(Debug, Clone)]
pub struct ShardHeader {
    pub version: u32,
    pub ops_section_offset: u64,
    pub snapshot_len: u32,
    pub ops_count: u32,
    pub flags: u32,
}

impl ShardHeader {
    /// Serialize the header to bytes.
    pub fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&SHARD_MAGIC);
        buf.extend_from_slice(&self.version.to_le_bytes());
        buf.extend_from_slice(&self.ops_section_offset.to_le_bytes());
        buf.extend_from_slice(&self.snapshot_len.to_le_bytes());
        buf.extend_from_slice(&self.ops_count.to_le_bytes());
        buf.extend_from_slice(&self.flags.to_le_bytes());
    }

    /// Deserialize a header from bytes.
    pub fn decode(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() < HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("shard header too short: {} bytes, need {}", bytes.len(), HEADER_SIZE),
            ));
        }
        if &bytes[0..4] != &SHARD_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid shard magic bytes",
            ));
        }
        let version = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
        if version != SHARD_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported shard version: {}, expected {}", version, SHARD_VERSION),
            ));
        }
        let ops_section_offset = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let snapshot_len = u32::from_le_bytes(bytes[16..20].try_into().unwrap());
        let ops_count = u32::from_le_bytes(bytes[20..24].try_into().unwrap());
        let flags = u32::from_le_bytes(bytes[24..28].try_into().unwrap());

        Ok(ShardHeader {
            version,
            ops_section_offset,
            snapshot_len,
            ops_count,
            flags,
        })
    }
}

// ---------------------------------------------------------------------------
// Op entry I/O (length-prefixed + CRC32)
// ---------------------------------------------------------------------------

/// Write a single op entry to a buffer: [u32 payload_len][payload bytes][u32 crc32]
fn write_op_entry<O: OpCodec>(op: &O::Op, buf: &mut Vec<u8>) {
    let mut payload = Vec::new();
    O::encode_op(op, &mut payload);

    let len = payload.len() as u32;
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(&payload);
    let crc = crc32_of(&payload);
    buf.extend_from_slice(&crc.to_le_bytes());
}

/// Read op entries from a byte slice (the ops section of a shard file).
/// Returns ops in file order (oldest first). Stops at first truncated/corrupt entry.
fn read_op_entries<O: OpCodec>(data: &[u8]) -> Vec<O::Op> {
    let mut ops = Vec::new();
    let mut pos = 0;

    while pos + 4 <= data.len() {
        let payload_len = u32::from_le_bytes(
            data[pos..pos + 4].try_into().unwrap()
        ) as usize;
        pos += 4;

        // Check if we have enough bytes for payload + CRC
        if pos + payload_len + 4 > data.len() {
            // Truncated entry — stop reading
            break;
        }

        let payload = &data[pos..pos + payload_len];
        pos += payload_len;

        let stored_crc = u32::from_le_bytes(
            data[pos..pos + 4].try_into().unwrap()
        );
        pos += 4;

        let computed_crc = crc32_of(payload);
        if stored_crc != computed_crc {
            // Corrupt entry — stop reading (don't trust anything after)
            break;
        }

        match O::decode_op(payload) {
            Ok(op) => ops.push(op),
            Err(_) => break, // Decode failure — stop
        }
    }

    ops
}

/// Public wrapper around `read_op_entries` for use by sibling modules
/// (e.g., `shard_store_bitmap` reading packed sort shard ops).
pub fn read_op_entries_pub<O: OpCodec>(data: &[u8]) -> Vec<O::Op> {
    read_op_entries::<O>(data)
}

/// Simple CRC32 (IEEE / CRC-32C via software). We use a basic lookup table.
fn crc32_of(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        let idx = ((crc ^ byte as u32) & 0xFF) as usize;
        crc = CRC32_TABLE[idx] ^ (crc >> 8);
    }
    crc ^ 0xFFFF_FFFF
}

/// CRC-32 lookup table (IEEE polynomial 0xEDB88320).
static CRC32_TABLE: [u32; 256] = {
    let mut table = [0u32; 256];
    let mut i = 0u32;
    while i < 256 {
        let mut crc = i;
        let mut j = 0;
        while j < 8 {
            if crc & 1 != 0 {
                crc = 0xEDB88320 ^ (crc >> 1);
            } else {
                crc >>= 1;
            }
            j += 1;
        }
        table[i as usize] = crc;
        i += 1;
    }
    table
};

// ---------------------------------------------------------------------------
// Shard file I/O (non-generic helpers to minimize monomorphization)
// ---------------------------------------------------------------------------

/// Read the full contents of a shard file. Returns (header, snapshot_bytes, ops_bytes).
fn read_shard_file_raw(path: &Path) -> io::Result<(ShardHeader, Vec<u8>, Vec<u8>)> {
    let data = fs::read(path)?;
    if data.len() < HEADER_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "shard file too small for header",
        ));
    }

    let header = ShardHeader::decode(&data[..HEADER_SIZE])?;

    let snapshot_start = HEADER_SIZE;
    let snapshot_end = snapshot_start + header.snapshot_len as usize;
    if snapshot_end > data.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "shard file truncated in snapshot section",
        ));
    }

    let snapshot_bytes = data[snapshot_start..snapshot_end].to_vec();
    let ops_offset = header.ops_section_offset as usize;
    let ops_bytes = if ops_offset <= data.len() {
        data[ops_offset..].to_vec()
    } else {
        Vec::new()
    };

    Ok((header, snapshot_bytes, ops_bytes))
}

/// Write a complete shard file atomically (tmp → fsync → rename).
pub(crate) fn write_shard_file_atomic(
    path: &Path,
    header: &ShardHeader,
    snapshot_bytes: &[u8],
    ops_bytes: &[u8],
) -> io::Result<()> {
    let tmp_path = path.with_extension("tmp");

    // Ensure parent directory exists
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut buf = Vec::with_capacity(HEADER_SIZE + snapshot_bytes.len() + ops_bytes.len());
    header.encode(&mut buf);
    buf.extend_from_slice(snapshot_bytes);
    buf.extend_from_slice(ops_bytes);

    let mut file = File::create(&tmp_path)?;
    file.write_all(&buf)?;
    file.sync_all()?;
    drop(file);

    fs::rename(&tmp_path, path)?;
    Ok(())
}

/// Append ops bytes to an existing shard file and update the header's ops_count.
fn append_ops_to_shard(path: &Path, new_ops_bytes: &[u8], additional_count: u32) -> io::Result<()> {
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;

    // Read current ops_count from header
    let mut header_buf = [0u8; HEADER_SIZE];
    file.read_exact(&mut header_buf)?;
    let mut header = ShardHeader::decode(&header_buf)?;

    // Append ops at end of file
    file.seek(SeekFrom::End(0))?;
    file.write_all(new_ops_bytes)?;

    // Update ops_count in header
    header.ops_count += additional_count;
    file.seek(SeekFrom::Start(HEADER_OPS_COUNT_OFFSET))?;
    file.write_all(&header.ops_count.to_le_bytes())?;

    file.sync_all()?;
    Ok(())
}

// ---------------------------------------------------------------------------
// ShardStore
// ---------------------------------------------------------------------------

/// The core unified storage engine.
///
/// Generic over snapshot codec, op codec, and sharding strategy.
/// Manages generations and provides read/write/compact operations.
pub struct ShardStore<S, O, Sh>
where
    S: SnapshotCodec,
    O: OpCodec<Snapshot = S::Snapshot>,
    Sh: ShardingStrategy,
{
    root: PathBuf,
    sharding: Sh,
    gen_counter: AtomicU64,
    _phantom_s: std::marker::PhantomData<S>,
    _phantom_o: std::marker::PhantomData<O>,
}

impl<S, O, Sh> ShardStore<S, O, Sh>
where
    S: SnapshotCodec,
    O: OpCodec<Snapshot = S::Snapshot>,
    Sh: ShardingStrategy,
{
    /// Create a new ShardStore rooted at the given directory.
    ///
    /// If the directory exists, scans for existing generations.
    /// If not, creates it with generation 0.
    pub fn new(root: PathBuf, sharding: Sh) -> io::Result<Self> {
        fs::create_dir_all(&root)?;

        // Scan for existing generations
        let gen = Self::find_latest_generation(&root)?;

        // Ensure at least gen 0 directory exists
        let gen_dir = root.join(format!("gen_{:03}", gen));
        fs::create_dir_all(&gen_dir)?;

        Ok(ShardStore {
            root,
            sharding,
            gen_counter: AtomicU64::new(gen),
            _phantom_s: std::marker::PhantomData,
            _phantom_o: std::marker::PhantomData,
        })
    }

    /// Current generation number.
    pub fn current_generation(&self) -> u64 {
        self.gen_counter.load(Ordering::Acquire)
    }

    /// Root directory of this store.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Get the directory path for a generation.
    pub fn gen_dir(&self, gen: u64) -> PathBuf {
        self.root.join(format!("gen_{:03}", gen))
    }

    /// Get the shard file path for a key in a specific generation.
    pub fn shard_path_in_gen(&self, key: &Sh::Key, gen: u64) -> PathBuf {
        self.sharding.shard_path(key, &self.gen_dir(gen))
    }

    // -----------------------------------------------------------------------
    // Read path
    // -----------------------------------------------------------------------

    /// Read a snapshot for a key, walking generations LIFO (newest → oldest).
    ///
    /// Walks newest → oldest collecting ops from each generation until finding
    /// a generation with a materialized snapshot (snapshot_len > 0). Then applies
    /// all collected ops chronologically (oldest gen first, newest last) on top
    /// of that base snapshot.
    ///
    /// This ensures that after a gen pin, ops in Gen N+1 (which have no snapshot)
    /// are correctly applied on top of Gen N's base snapshot.
    ///
    /// Returns `None` if no shard exists for this key in any generation.
    pub fn read(&self, key: &Sh::Key) -> io::Result<Option<S::Snapshot>> {
        let current_gen = self.current_generation();

        // Collect ops from newest → oldest until we find a snapshot.
        // Each entry: (gen, ops) — ops in file order (append-order within gen).
        let mut pending_ops: Vec<Vec<O::Op>> = Vec::new();
        let mut found_any = false;

        for gen in (0..=current_gen).rev() {
            let shard_path = self.shard_path_in_gen(key, gen);
            if !shard_path.exists() {
                continue;
            }
            found_any = true;

            let (header, snapshot_bytes, ops_bytes) = read_shard_file_raw(&shard_path)?;

            // Collect this gen's ops (will be applied later in chronological order)
            if header.ops_count > 0 {
                pending_ops.push(read_op_entries::<O>(&ops_bytes));
            }

            // If this gen has a snapshot, we've found our base — stop walking
            if header.snapshot_len > 0 {
                let mut snapshot = S::decode(&snapshot_bytes)?;

                // Apply ops chronologically: oldest gen first (reverse of collection order).
                // Within each gen, ops are already in append order (oldest first).
                for ops in pending_ops.iter().rev() {
                    for op in ops {
                        O::apply(&mut snapshot, op);
                    }
                }

                return Ok(Some(snapshot));
            }
        }

        // No snapshot found in any gen — apply all ops to empty if we found any shards
        if found_any && !pending_ops.is_empty() {
            let mut snapshot = S::empty();
            for ops in pending_ops.iter().rev() {
                for op in ops {
                    O::apply(&mut snapshot, op);
                }
            }
            return Ok(Some(snapshot));
        }

        if found_any {
            // Shards exist but all are empty (no snapshot, no ops)
            return Ok(Some(S::empty()));
        }

        Ok(None)
    }

    /// Read the raw ops count for a key in the current generation.
    /// Used by janitor to decide if compaction is needed.
    pub fn ops_count(&self, key: &Sh::Key) -> io::Result<Option<u32>> {
        let shard_path = self.shard_path_in_gen(key, self.current_generation());
        if !shard_path.exists() {
            return Ok(None);
        }

        let mut file = File::open(&shard_path)?;
        let mut header_buf = [0u8; HEADER_SIZE];
        file.read_exact(&mut header_buf)?;
        let header = ShardHeader::decode(&header_buf)?;
        Ok(Some(header.ops_count))
    }

    // -----------------------------------------------------------------------
    // Write path
    // -----------------------------------------------------------------------

    /// Append a single op to the current generation's shard for this key.
    ///
    /// If no shard exists yet in the current generation, creates one with
    /// an empty snapshot section. The snapshot will be populated on compaction.
    ///
    /// # Concurrency
    ///
    /// This method is NOT thread-safe for concurrent writes to the same shard.
    /// The caller must ensure single-writer access (e.g., flush thread only).
    /// Concurrent reads are safe — readers use snapshot + ops from completed writes.
    pub fn append_op(&self, key: &Sh::Key, op: &O::Op) -> io::Result<()> {
        let gen = self.current_generation();
        let shard_path = self.shard_path_in_gen(key, gen);

        let mut ops_buf = Vec::new();
        write_op_entry::<O>(op, &mut ops_buf);

        if shard_path.exists() {
            // Append to existing shard
            append_ops_to_shard(&shard_path, &ops_buf, 1)?;
        } else {
            // Create new shard with empty snapshot
            let header = ShardHeader {
                version: SHARD_VERSION,
                ops_section_offset: HEADER_SIZE as u64,
                snapshot_len: 0,
                ops_count: 1,
                flags: 0,
            };
            write_shard_file_atomic(&shard_path, &header, &[], &ops_buf)?;
        }

        Ok(())
    }

    /// Append multiple ops to the current generation's shard for this key.
    pub fn append_ops(&self, key: &Sh::Key, ops: &[O::Op]) -> io::Result<()> {
        if ops.is_empty() {
            return Ok(());
        }

        let gen = self.current_generation();
        let shard_path = self.shard_path_in_gen(key, gen);

        let mut ops_buf = Vec::new();
        for op in ops {
            write_op_entry::<O>(op, &mut ops_buf);
        }

        let count = ops.len() as u32;

        if shard_path.exists() {
            append_ops_to_shard(&shard_path, &ops_buf, count)?;
        } else {
            let header = ShardHeader {
                version: SHARD_VERSION,
                ops_section_offset: HEADER_SIZE as u64,
                snapshot_len: 0,
                ops_count: count,
                flags: 0,
            };
            write_shard_file_atomic(&shard_path, &header, &[], &ops_buf)?;
        }

        Ok(())
    }

    /// Write a full snapshot for a key in the current generation.
    ///
    /// This is the "bulk write" path — used during initial loading or compaction.
    /// Creates a shard with a materialized snapshot and zero ops.
    pub fn write_snapshot(&self, key: &Sh::Key, snapshot: &S::Snapshot) -> io::Result<()> {
        let gen = self.current_generation();
        let shard_path = self.shard_path_in_gen(key, gen);

        let mut snapshot_bytes = Vec::new();
        S::encode(snapshot, &mut snapshot_bytes);

        let ops_offset = HEADER_SIZE as u64 + snapshot_bytes.len() as u64;
        let header = ShardHeader {
            version: SHARD_VERSION,
            ops_section_offset: ops_offset,
            snapshot_len: snapshot_bytes.len() as u32,
            ops_count: 0,
            flags: 0,
        };

        write_shard_file_atomic(&shard_path, &header, &snapshot_bytes, &[])?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Generation management
    // -----------------------------------------------------------------------

    /// Pin the current generation: bump the counter so new writes go to gen N+1.
    /// Returns the old (now frozen) generation number.
    pub fn pin_generation(&self) -> io::Result<u64> {
        let old_gen = self.gen_counter.fetch_add(1, Ordering::AcqRel);
        let new_gen = old_gen + 1;

        // Create the new generation directory
        fs::create_dir_all(self.gen_dir(new_gen))?;

        Ok(old_gen)
    }

    /// Compact a shard: read snapshot + ops across generations, produce a
    /// fresh shard with a materialized snapshot and zero ops.
    ///
    /// Writes the compacted shard to `target_gen`.
    pub fn compact_shard(&self, key: &Sh::Key, target_gen: u64) -> io::Result<()> {
        // Read the full reconstructed state
        let snapshot = match self.read(key)? {
            Some(s) => s,
            None => return Ok(()), // Nothing to compact
        };

        // Write as a fresh snapshot in the target generation
        let shard_path = self.shard_path_in_gen(key, target_gen);

        let mut snapshot_bytes = Vec::new();
        S::encode(&snapshot, &mut snapshot_bytes);

        let ops_offset = HEADER_SIZE as u64 + snapshot_bytes.len() as u64;
        let header = ShardHeader {
            version: SHARD_VERSION,
            ops_section_offset: ops_offset,
            snapshot_len: snapshot_bytes.len() as u32,
            ops_count: 0,
            flags: 0,
        };

        write_shard_file_atomic(&shard_path, &header, &snapshot_bytes, &[])?;
        Ok(())
    }

    /// Compact all shards in a generation: merge all older generations into
    /// `target_gen` with zero ops.
    pub fn compact_generation(&self, target_gen: u64) -> io::Result<()> {
        // Collect all shard keys across all generations up to target_gen
        let mut all_keys = std::collections::HashSet::new();
        for gen in 0..=target_gen {
            let gen_dir = self.gen_dir(gen);
            if gen_dir.exists() {
                for key in self.sharding.list_shards(&gen_dir)? {
                    all_keys.insert(key);
                }
            }
        }

        // Compact each shard
        for key in &all_keys {
            self.compact_shard(key, target_gen)?;
        }

        Ok(())
    }

    /// Delete a generation directory and all its shard files.
    pub fn delete_generation(&self, gen: u64) -> io::Result<()> {
        let gen_dir = self.gen_dir(gen);
        if gen_dir.exists() {
            fs::remove_dir_all(&gen_dir)?;
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Janitor support
    // -----------------------------------------------------------------------

    /// Check if a shard needs compaction based on ops count threshold.
    ///
    /// Called by readers after scanning ops — zero overhead since the reader
    /// already iterated the ops. Returns true if ops_count > threshold.
    pub fn should_compact(&self, key: &Sh::Key, threshold: u32) -> io::Result<bool> {
        match self.ops_count(key)? {
            Some(count) => Ok(count > threshold),
            None => Ok(false),
        }
    }

    /// Check if a shard needs compaction using the default threshold (500 ops).
    /// Based on microbench results: knee at 500 ops, <2x overhead below that.
    pub fn needs_compaction(&self, key: &Sh::Key) -> io::Result<bool> {
        self.should_compact(key, DEFAULT_COMPACT_THRESHOLD)
    }

    /// Compact a shard in-place in the current generation.
    ///
    /// Reads the full state (snapshot + ops), writes back as a fresh snapshot
    /// with zero ops. This is the janitor's compaction path — called when
    /// ops_count exceeds the threshold.
    pub fn compact_current(&self, key: &Sh::Key) -> io::Result<()> {
        self.compact_shard(key, self.current_generation())
    }

    /// List all shard keys in the current generation.
    pub fn list_current_shards(&self) -> io::Result<Vec<Sh::Key>> {
        let gen_dir = self.gen_dir(self.current_generation());
        if gen_dir.exists() {
            self.sharding.list_shards(&gen_dir)
        } else {
            Ok(Vec::new())
        }
    }

    /// List all shard keys across all generations.
    pub fn list_all_shards(&self) -> io::Result<Vec<Sh::Key>> {
        let mut all_keys = std::collections::HashSet::new();
        let current_gen = self.current_generation();
        for gen in 0..=current_gen {
            let gen_dir = self.gen_dir(gen);
            if gen_dir.exists() {
                for key in self.sharding.list_shards(&gen_dir)? {
                    all_keys.insert(key);
                }
            }
        }
        Ok(all_keys.into_iter().collect())
    }

    /// Check if a shard exists in any generation.
    pub fn shard_exists(&self, key: &Sh::Key) -> bool {
        let current_gen = self.current_generation();
        for gen in (0..=current_gen).rev() {
            if self.shard_path_in_gen(key, gen).exists() {
                return true;
            }
        }
        false
    }

    /// Read only the header of a shard in the current generation.
    /// Useful for checking ops count without reading the full file.
    pub fn read_header(&self, key: &Sh::Key) -> io::Result<Option<ShardHeader>> {
        let shard_path = self.shard_path_in_gen(key, self.current_generation());
        if !shard_path.exists() {
            return Ok(None);
        }
        let mut file = File::open(&shard_path)?;
        let mut buf = [0u8; HEADER_SIZE];
        file.read_exact(&mut buf)?;
        Ok(Some(ShardHeader::decode(&buf)?))
    }

    // -----------------------------------------------------------------------
    // Bulk write path
    // -----------------------------------------------------------------------

    /// Write multiple snapshots in parallel using rayon.
    ///
    /// Groups keys by shard path, writes each shard file independently.
    /// Used during initial data loading for maximum throughput.
    /// The caller is responsible for ensuring no concurrent writes to the
    /// same shard (same invariant as append_op).
    #[cfg(feature = "rayon")]
    pub fn write_snapshots_parallel(
        &self,
        entries: Vec<(Sh::Key, S::Snapshot)>,
    ) -> io::Result<()> {
        use rayon::prelude::*;

        entries.into_par_iter().try_for_each(|(key, snapshot)| {
            self.write_snapshot(&key, &snapshot)
        })?;

        Ok(())
    }

    /// Write multiple snapshots sequentially.
    ///
    /// Non-rayon fallback for bulk writes. Same semantics as
    /// write_snapshots_parallel but single-threaded.
    pub fn write_snapshots_batch(
        &self,
        entries: &[(Sh::Key, S::Snapshot)],
    ) -> io::Result<()> {
        for (key, snapshot) in entries {
            self.write_snapshot(key, snapshot)?;
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Streaming save path
    // -----------------------------------------------------------------------

    /// Save a snapshot directly to a specific generation without going through
    /// the current generation counter. Used by save_and_unload to write
    /// directly from staging without cloning.
    pub fn write_snapshot_to_gen(
        &self,
        key: &Sh::Key,
        snapshot: &S::Snapshot,
        gen: u64,
    ) -> io::Result<()> {
        let shard_path = self.shard_path_in_gen(key, gen);

        let mut snapshot_bytes = Vec::new();
        S::encode(snapshot, &mut snapshot_bytes);

        let ops_offset = HEADER_SIZE as u64 + snapshot_bytes.len() as u64;
        let header = ShardHeader {
            version: SHARD_VERSION,
            ops_section_offset: ops_offset,
            snapshot_len: snapshot_bytes.len() as u32,
            ops_count: 0,
            flags: 0,
        };

        write_shard_file_atomic(&shard_path, &header, &snapshot_bytes, &[])?;
        Ok(())
    }

    /// Create a new generation and return its number, without advancing the
    /// current generation counter. Used for save_and_unload where we want
    /// to write to a fresh generation directory without affecting the active
    /// write path.
    pub fn create_save_generation(&self) -> io::Result<u64> {
        let save_gen = self.gen_counter.load(Ordering::Acquire) + 1;
        fs::create_dir_all(self.gen_dir(save_gen))?;
        Ok(save_gen)
    }

    /// Atomically advance the generation counter to a specific value.
    /// Used after save_and_unload completes to make the saved generation
    /// the current one.
    pub fn advance_generation_to(&self, gen: u64) {
        self.gen_counter.store(gen, Ordering::Release);
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Scan root directory for gen_NNN directories, return the highest N found (or 0).
    fn find_latest_generation(root: &Path) -> io::Result<u64> {
        let mut max_gen = 0u64;

        if !root.exists() {
            return Ok(0);
        }

        for entry in fs::read_dir(root)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if let Some(suffix) = name_str.strip_prefix("gen_") {
                if let Ok(gen) = suffix.parse::<u64>() {
                    max_gen = max_gen.max(gen);
                }
            }
        }

        Ok(max_gen)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // -- Test snapshot codec: simple key-value store --

    #[derive(Debug, Clone, PartialEq)]
    struct TestSnapshot {
        values: HashMap<String, String>,
    }

    struct TestSnapshotCodec;

    impl SnapshotCodec for TestSnapshotCodec {
        type Snapshot = TestSnapshot;

        fn encode(snapshot: &TestSnapshot, buf: &mut Vec<u8>) {
            // Simple encoding: [u32 num_entries] [u32 key_len][key][u32 val_len][val]...
            let count = snapshot.values.len() as u32;
            buf.extend_from_slice(&count.to_le_bytes());
            for (k, v) in &snapshot.values {
                buf.extend_from_slice(&(k.len() as u32).to_le_bytes());
                buf.extend_from_slice(k.as_bytes());
                buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
                buf.extend_from_slice(v.as_bytes());
            }
        }

        fn decode(bytes: &[u8]) -> io::Result<TestSnapshot> {
            let mut pos = 0;
            if bytes.len() < 4 {
                return Ok(TestSnapshot { values: HashMap::new() });
            }
            let count = u32::from_le_bytes(bytes[pos..pos+4].try_into().unwrap()) as usize;
            pos += 4;
            let mut values = HashMap::new();
            for _ in 0..count {
                let klen = u32::from_le_bytes(bytes[pos..pos+4].try_into().unwrap()) as usize;
                pos += 4;
                let key = String::from_utf8_lossy(&bytes[pos..pos+klen]).into_owned();
                pos += klen;
                let vlen = u32::from_le_bytes(bytes[pos..pos+4].try_into().unwrap()) as usize;
                pos += 4;
                let val = String::from_utf8_lossy(&bytes[pos..pos+vlen]).into_owned();
                pos += vlen;
                values.insert(key, val);
            }
            Ok(TestSnapshot { values })
        }

        fn empty() -> TestSnapshot {
            TestSnapshot { values: HashMap::new() }
        }
    }

    // -- Test op codec --

    #[derive(Debug, Clone)]
    enum TestOp {
        Set { key: String, value: String },
        Delete { key: String },
    }

    struct TestOpCodec;

    impl OpCodec for TestOpCodec {
        type Op = TestOp;
        type Snapshot = TestSnapshot;

        fn encode_op(op: &TestOp, buf: &mut Vec<u8>) {
            match op {
                TestOp::Set { key, value } => {
                    buf.push(0x01); // tag
                    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                    buf.extend_from_slice(key.as_bytes());
                    buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
                    buf.extend_from_slice(value.as_bytes());
                }
                TestOp::Delete { key } => {
                    buf.push(0x02); // tag
                    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                    buf.extend_from_slice(key.as_bytes());
                }
            }
        }

        fn decode_op(bytes: &[u8]) -> io::Result<TestOp> {
            if bytes.is_empty() {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "empty op"));
            }
            match bytes[0] {
                0x01 => {
                    let mut pos = 1;
                    let klen = u32::from_le_bytes(bytes[pos..pos+4].try_into().unwrap()) as usize;
                    pos += 4;
                    let key = String::from_utf8_lossy(&bytes[pos..pos+klen]).into_owned();
                    pos += klen;
                    let vlen = u32::from_le_bytes(bytes[pos..pos+4].try_into().unwrap()) as usize;
                    pos += 4;
                    let val = String::from_utf8_lossy(&bytes[pos..pos+vlen]).into_owned();
                    Ok(TestOp::Set { key, value: val })
                }
                0x02 => {
                    let klen = u32::from_le_bytes(bytes[1..5].try_into().unwrap()) as usize;
                    let key = String::from_utf8_lossy(&bytes[5..5+klen]).into_owned();
                    Ok(TestOp::Delete { key })
                }
                tag => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unknown op tag: {}", tag),
                )),
            }
        }

        fn apply(snapshot: &mut TestSnapshot, op: &TestOp) {
            match op {
                TestOp::Set { key, value } => {
                    snapshot.values.insert(key.clone(), value.clone());
                }
                TestOp::Delete { key } => {
                    snapshot.values.remove(key);
                }
            }
        }
    }

    // -- Test sharding strategy: single directory, key = string --

    struct FlatShard;

    impl ShardingStrategy for FlatShard {
        type Key = String;

        fn shard_path(&self, key: &String, gen_root: &Path) -> PathBuf {
            gen_root.join(format!("{}.shard", key))
        }

        fn list_shards(&self, gen_root: &Path) -> io::Result<Vec<String>> {
            let mut keys = Vec::new();
            if !gen_root.exists() {
                return Ok(keys);
            }
            for entry in fs::read_dir(gen_root)? {
                let entry = entry?;
                let name = entry.file_name().to_string_lossy().into_owned();
                if let Some(key) = name.strip_suffix(".shard") {
                    keys.push(key.to_string());
                }
            }
            Ok(keys)
        }
    }

    type TestStore = ShardStore<TestSnapshotCodec, TestOpCodec, FlatShard>;

    fn temp_store() -> (tempfile::TempDir, TestStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = TestStore::new(dir.path().to_path_buf(), FlatShard).unwrap();
        (dir, store)
    }

    #[test]
    fn test_write_snapshot_and_read() {
        let (_dir, store) = temp_store();

        let mut snap = TestSnapshot { values: HashMap::new() };
        snap.values.insert("name".into(), "bitdex".into());
        snap.values.insert("version".into(), "3".into());

        store.write_snapshot(&"doc1".to_string(), &snap).unwrap();
        let result = store.read(&"doc1".to_string()).unwrap().unwrap();
        assert_eq!(result, snap);
    }

    #[test]
    fn test_append_ops_and_read() {
        let (_dir, store) = temp_store();

        // Write base snapshot
        let snap = TestSnapshot { values: HashMap::new() };
        store.write_snapshot(&"doc1".to_string(), &snap).unwrap();

        // Append ops
        store.append_op(&"doc1".to_string(), &TestOp::Set {
            key: "name".into(), value: "bitdex".into()
        }).unwrap();
        store.append_op(&"doc1".to_string(), &TestOp::Set {
            key: "status".into(), value: "active".into()
        }).unwrap();

        // Read should reflect snapshot + ops
        let result = store.read(&"doc1".to_string()).unwrap().unwrap();
        assert_eq!(result.values.get("name").unwrap(), "bitdex");
        assert_eq!(result.values.get("status").unwrap(), "active");
    }

    #[test]
    fn test_ops_without_snapshot() {
        let (_dir, store) = temp_store();

        // Append ops without a base snapshot (creates shard with empty snapshot)
        store.append_op(&"doc1".to_string(), &TestOp::Set {
            key: "name".into(), value: "bitdex".into()
        }).unwrap();

        let result = store.read(&"doc1".to_string()).unwrap().unwrap();
        assert_eq!(result.values.get("name").unwrap(), "bitdex");
    }

    #[test]
    fn test_read_nonexistent_returns_none() {
        let (_dir, store) = temp_store();
        let result = store.read(&"nope".to_string()).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_delete_op() {
        let (_dir, store) = temp_store();

        let mut snap = TestSnapshot { values: HashMap::new() };
        snap.values.insert("name".into(), "bitdex".into());
        snap.values.insert("temp".into(), "remove_me".into());
        store.write_snapshot(&"doc1".to_string(), &snap).unwrap();

        store.append_op(&"doc1".to_string(), &TestOp::Delete {
            key: "temp".into()
        }).unwrap();

        let result = store.read(&"doc1".to_string()).unwrap().unwrap();
        assert_eq!(result.values.get("name").unwrap(), "bitdex");
        assert!(result.values.get("temp").is_none());
    }

    #[test]
    fn test_compact_shard() {
        let (_dir, store) = temp_store();

        // Write snapshot + ops
        let snap = TestSnapshot { values: HashMap::new() };
        store.write_snapshot(&"doc1".to_string(), &snap).unwrap();
        store.append_op(&"doc1".to_string(), &TestOp::Set {
            key: "a".into(), value: "1".into()
        }).unwrap();
        store.append_op(&"doc1".to_string(), &TestOp::Set {
            key: "b".into(), value: "2".into()
        }).unwrap();

        // Verify ops count before compaction
        assert_eq!(store.ops_count(&"doc1".to_string()).unwrap(), Some(2));

        // Compact into same generation
        store.compact_shard(&"doc1".to_string(), 0).unwrap();

        // After compaction: zero ops, data preserved
        assert_eq!(store.ops_count(&"doc1".to_string()).unwrap(), Some(0));
        let result = store.read(&"doc1".to_string()).unwrap().unwrap();
        assert_eq!(result.values.get("a").unwrap(), "1");
        assert_eq!(result.values.get("b").unwrap(), "2");
    }

    #[test]
    fn test_generation_pin_and_read() {
        let (_dir, store) = temp_store();

        // Write to gen 0
        store.write_snapshot(&"doc1".to_string(), &TestSnapshot {
            values: [("v".into(), "gen0".into())].into_iter().collect(),
        }).unwrap();

        // Pin → gen 0 frozen, gen 1 is current
        let frozen = store.pin_generation().unwrap();
        assert_eq!(frozen, 0);
        assert_eq!(store.current_generation(), 1);

        // Write to gen 1 (overwrites gen 0 for this key)
        store.write_snapshot(&"doc1".to_string(), &TestSnapshot {
            values: [("v".into(), "gen1".into())].into_iter().collect(),
        }).unwrap();

        // Read should find gen 1 (newest first)
        let result = store.read(&"doc1".to_string()).unwrap().unwrap();
        assert_eq!(result.values.get("v").unwrap(), "gen1");
    }

    #[test]
    fn test_generation_fallthrough() {
        let (_dir, store) = temp_store();

        // Write to gen 0
        store.write_snapshot(&"doc1".to_string(), &TestSnapshot {
            values: [("v".into(), "gen0".into())].into_iter().collect(),
        }).unwrap();

        // Pin → gen 1 is current
        store.pin_generation().unwrap();

        // Don't write to gen 1 for doc1
        // Write something else to gen 1
        store.write_snapshot(&"doc2".to_string(), &TestSnapshot {
            values: [("v".into(), "gen1_doc2".into())].into_iter().collect(),
        }).unwrap();

        // Read doc1 should fall through to gen 0
        let result = store.read(&"doc1".to_string()).unwrap().unwrap();
        assert_eq!(result.values.get("v").unwrap(), "gen0");

        // Read doc2 should find gen 1
        let result = store.read(&"doc2".to_string()).unwrap().unwrap();
        assert_eq!(result.values.get("v").unwrap(), "gen1_doc2");
    }

    #[test]
    fn test_cross_generation_ops_on_snapshot() {
        // Verifies that after a gen pin, ops in Gen N+1 (no snapshot)
        // are correctly applied on top of Gen N's base snapshot.
        let (_dir, store) = temp_store();

        // Write base snapshot in gen 0
        store.write_snapshot(&"doc1".to_string(), &TestSnapshot {
            values: [("a".into(), "1".into()), ("b".into(), "2".into())].into_iter().collect(),
        }).unwrap();

        // Pin → gen 1
        store.pin_generation().unwrap();

        // Append ops to gen 1 (no snapshot — this is what append_op does)
        store.append_op(&"doc1".to_string(), &TestOp::Set {
            key: "c".into(), value: "3".into(),
        }).unwrap();
        store.append_op(&"doc1".to_string(), &TestOp::Set {
            key: "a".into(), value: "updated".into(),
        }).unwrap();

        // Read should find gen 1 ops-only shard, walk back to gen 0 for
        // the base snapshot, then apply gen 1 ops on top.
        let result = store.read(&"doc1".to_string()).unwrap().unwrap();
        assert_eq!(result.values.get("a").unwrap(), "updated", "gen 1 op should override gen 0 snapshot");
        assert_eq!(result.values.get("b").unwrap(), "2", "gen 0 value should survive");
        assert_eq!(result.values.get("c").unwrap(), "3", "gen 1 new key should appear");
        assert_eq!(result.values.len(), 3);

        // Pin again → gen 2, add more ops
        store.pin_generation().unwrap();
        store.append_op(&"doc1".to_string(), &TestOp::Set {
            key: "d".into(), value: "4".into(),
        }).unwrap();

        // Read should walk gen 2 (ops) → gen 1 (ops) → gen 0 (snapshot)
        let result = store.read(&"doc1".to_string()).unwrap().unwrap();
        assert_eq!(result.values.len(), 4);
        assert_eq!(result.values.get("a").unwrap(), "updated");
        assert_eq!(result.values.get("d").unwrap(), "4", "gen 2 op should appear");
    }

    #[test]
    fn test_append_batch_ops() {
        let (_dir, store) = temp_store();

        let ops = vec![
            TestOp::Set { key: "a".into(), value: "1".into() },
            TestOp::Set { key: "b".into(), value: "2".into() },
            TestOp::Set { key: "c".into(), value: "3".into() },
        ];

        store.append_ops(&"doc1".to_string(), &ops).unwrap();

        let result = store.read(&"doc1".to_string()).unwrap().unwrap();
        assert_eq!(result.values.len(), 3);
        assert_eq!(result.values.get("a").unwrap(), "1");
        assert_eq!(result.values.get("c").unwrap(), "3");
    }

    #[test]
    fn test_crc32_detects_corruption() {
        // Verify that our CRC32 implementation produces consistent results
        let data = b"hello world";
        let crc1 = crc32_of(data);
        let crc2 = crc32_of(data);
        assert_eq!(crc1, crc2);

        // Different data → different CRC
        let crc3 = crc32_of(b"hello worl!");
        assert_ne!(crc1, crc3);
    }

    #[test]
    fn test_header_roundtrip() {
        let header = ShardHeader {
            version: SHARD_VERSION,
            ops_section_offset: 12345,
            snapshot_len: 678,
            ops_count: 42,
            flags: 0,
        };

        let mut buf = Vec::new();
        header.encode(&mut buf);
        assert_eq!(buf.len(), HEADER_SIZE);

        let decoded = ShardHeader::decode(&buf).unwrap();
        assert_eq!(decoded.version, header.version);
        assert_eq!(decoded.ops_section_offset, header.ops_section_offset);
        assert_eq!(decoded.snapshot_len, header.snapshot_len);
        assert_eq!(decoded.ops_count, header.ops_count);
        assert_eq!(decoded.flags, header.flags);
    }

    #[test]
    fn test_delete_generation() {
        let (_dir, store) = temp_store();

        // Write to gen 0, pin, write to gen 1
        store.write_snapshot(&"doc1".to_string(), &TestSnapshot {
            values: [("v".into(), "gen0".into())].into_iter().collect(),
        }).unwrap();
        store.pin_generation().unwrap();
        store.write_snapshot(&"doc1".to_string(), &TestSnapshot {
            values: [("v".into(), "gen1".into())].into_iter().collect(),
        }).unwrap();

        // Delete gen 0
        store.delete_generation(0).unwrap();

        // Read should still work (finds gen 1)
        let result = store.read(&"doc1".to_string()).unwrap().unwrap();
        assert_eq!(result.values.get("v").unwrap(), "gen1");
    }

    #[test]
    fn test_should_compact() {
        let (_dir, store) = temp_store();

        // No shard → should not compact
        assert!(!store.should_compact(&"doc1".to_string(), 5).unwrap());

        // Add 3 ops
        store.append_op(&"doc1".to_string(), &TestOp::Set {
            key: "a".into(), value: "1".into()
        }).unwrap();
        store.append_op(&"doc1".to_string(), &TestOp::Set {
            key: "b".into(), value: "2".into()
        }).unwrap();
        store.append_op(&"doc1".to_string(), &TestOp::Set {
            key: "c".into(), value: "3".into()
        }).unwrap();

        // Threshold 5 → should NOT compact (3 <= 5)
        assert!(!store.should_compact(&"doc1".to_string(), 5).unwrap());

        // Threshold 2 → SHOULD compact (3 > 2)
        assert!(store.should_compact(&"doc1".to_string(), 2).unwrap());
    }

    #[test]
    fn test_compact_current() {
        let (_dir, store) = temp_store();

        store.append_op(&"doc1".to_string(), &TestOp::Set {
            key: "x".into(), value: "42".into()
        }).unwrap();
        store.append_op(&"doc1".to_string(), &TestOp::Set {
            key: "y".into(), value: "99".into()
        }).unwrap();

        assert_eq!(store.ops_count(&"doc1".to_string()).unwrap(), Some(2));

        store.compact_current(&"doc1".to_string()).unwrap();

        assert_eq!(store.ops_count(&"doc1".to_string()).unwrap(), Some(0));
        let result = store.read(&"doc1".to_string()).unwrap().unwrap();
        assert_eq!(result.values.get("x").unwrap(), "42");
        assert_eq!(result.values.get("y").unwrap(), "99");
    }

    #[test]
    fn test_list_current_shards() {
        let (_dir, store) = temp_store();

        store.write_snapshot(&"a".to_string(), &TestSnapshot {
            values: HashMap::new(),
        }).unwrap();
        store.write_snapshot(&"b".to_string(), &TestSnapshot {
            values: HashMap::new(),
        }).unwrap();

        let mut shards = store.list_current_shards().unwrap();
        shards.sort();
        assert_eq!(shards, vec!["a", "b"]);
    }

    #[test]
    fn test_shard_exists() {
        let (_dir, store) = temp_store();

        assert!(!store.shard_exists(&"doc1".to_string()));

        store.write_snapshot(&"doc1".to_string(), &TestSnapshot {
            values: HashMap::new(),
        }).unwrap();

        assert!(store.shard_exists(&"doc1".to_string()));
    }

    #[test]
    fn test_read_header() {
        let (_dir, store) = temp_store();

        // No shard → None
        assert!(store.read_header(&"doc1".to_string()).unwrap().is_none());

        // Write snapshot + 2 ops
        store.write_snapshot(&"doc1".to_string(), &TestSnapshot {
            values: [("k".into(), "v".into())].into_iter().collect(),
        }).unwrap();
        store.append_op(&"doc1".to_string(), &TestOp::Set {
            key: "a".into(), value: "1".into()
        }).unwrap();
        store.append_op(&"doc1".to_string(), &TestOp::Set {
            key: "b".into(), value: "2".into()
        }).unwrap();

        let header = store.read_header(&"doc1".to_string()).unwrap().unwrap();
        assert_eq!(header.ops_count, 2);
        assert!(header.snapshot_len > 0);
    }

    #[test]
    fn test_list_all_shards_across_generations() {
        let (_dir, store) = temp_store();

        // Write to gen 0
        store.write_snapshot(&"doc_a".to_string(), &TestSnapshot {
            values: HashMap::new(),
        }).unwrap();

        // Pin → gen 1
        store.pin_generation().unwrap();

        // Write to gen 1 (different shard)
        store.write_snapshot(&"doc_b".to_string(), &TestSnapshot {
            values: HashMap::new(),
        }).unwrap();

        let mut all = store.list_all_shards().unwrap();
        all.sort();
        assert_eq!(all, vec!["doc_a", "doc_b"]);
    }

    #[test]
    fn test_write_snapshots_batch() {
        let (_dir, store) = temp_store();

        let entries: Vec<(String, TestSnapshot)> = (0..10).map(|i| {
            let key = format!("doc_{}", i);
            let snap = TestSnapshot {
                values: [(format!("k{}", i), format!("v{}", i))].into_iter().collect(),
            };
            (key, snap)
        }).collect();

        store.write_snapshots_batch(&entries).unwrap();

        for i in 0..10 {
            let result = store.read(&format!("doc_{}", i)).unwrap().unwrap();
            assert_eq!(result.values.get(&format!("k{}", i)).unwrap(), &format!("v{}", i));
        }
    }

    #[test]
    fn test_write_snapshot_to_gen() {
        let (_dir, store) = temp_store();

        // Write to gen 0 normally
        store.write_snapshot(&"doc1".to_string(), &TestSnapshot {
            values: [("v".into(), "gen0".into())].into_iter().collect(),
        }).unwrap();

        // Create save generation (gen 1) without advancing counter
        let save_gen = store.create_save_generation().unwrap();
        assert_eq!(save_gen, 1);
        assert_eq!(store.current_generation(), 0); // counter not advanced

        // Write directly to save generation
        store.write_snapshot_to_gen(&"doc1".to_string(), &TestSnapshot {
            values: [("v".into(), "saved".into())].into_iter().collect(),
        }, save_gen).unwrap();

        // Current generation still reads gen 0
        let result = store.read(&"doc1".to_string()).unwrap().unwrap();
        assert_eq!(result.values.get("v").unwrap(), "gen0");

        // Advance to save generation
        store.advance_generation_to(save_gen);
        assert_eq!(store.current_generation(), 1);

        // Now reads find save generation
        let result = store.read(&"doc1".to_string()).unwrap().unwrap();
        assert_eq!(result.values.get("v").unwrap(), "saved");
    }
}
