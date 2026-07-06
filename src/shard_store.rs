#![allow(unexpected_cfgs)]
//! ShardStore — Unified storage engine for BitDex.
//!
//! Flat, single-file-per-shard design. Each shard lives directly under the store
//! root with no generation directories. Compaction is batched-fsync: write `.new`,
//! fsync the `.new` file, rename over the existing shard. Orphan `.new` files from
//! a crash are swept on startup.
//!
//! # Type Parameters
//!
//! `ShardStore<S, O, Sh>` where:
//! - `S: SnapshotCodec` — how to serialize/deserialize the snapshot section
//! - `O: OpCodec<Snapshot = S::Snapshot>` — how to serialize/deserialize ops, tied to snapshot type
//! - `Sh: ShardingStrategy` — how to map keys to shard file paths

use dashmap::DashMap;
use parking_lot::RwLock;
use std::fmt;
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

// ---------------------------------------------------------------------------
// Test-only sync failure injection
// ---------------------------------------------------------------------------
//
// Set SYNC_INJECT_FAIL to `true` in a test to make `sync_all_opslogs` return
// an error immediately, simulating an OS-level fsync failure.  Always reset to
// `false` after the assertion so other tests are not affected.
//
// Compiled unconditionally so integration tests (which link the lib in
// non-`#[cfg(test)]` mode) can reach it.  In production it is always `false`
// and the AtomicBool load on the fast path will be eliminated by the optimizer.
#[doc(hidden)]
pub static SYNC_INJECT_FAIL: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

// ---------------------------------------------------------------------------
// Shard rewrite counters (diagnostic instrumentation, 2026-05-01)
//
// Every call to write_shard_file_atomic increments one of these counters based
// on the caller's intent:
//   SHARD_REWRITES_COMPACT    — merge-thread compaction (compact_shard)
//   SHARD_REWRITES_COLD       — first-write cold-path (shard file absent/invalid)
//   SHARD_REWRITES_SNAPSHOT   — full snapshot write (write_snapshot, write_filter_bucket*, write_sort_layers, etc.)
//
// Exposed via GET /metrics as `bitdex_shard_rewrites_total{source="compact|cold_create|snapshot"}`.
// These are global (not per-store) so they capture rewrites from all callers —
// the merge thread, the flush thread, dump processor, backfill, and save_snapshot.
// ---------------------------------------------------------------------------
pub static SHARD_REWRITES_COMPACT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub static SHARD_REWRITES_COLD: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub static SHARD_REWRITES_SNAPSHOT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Source label for `write_shard_file_atomic` — used to distinguish callers
/// in the `bitdex_shard_rewrites_total` Prometheus counter.
#[derive(Copy, Clone, Debug)]
pub enum ShardRewriteSource {
    /// merge-thread per-shard compaction (`compact_shard`)
    Compact,
    /// cold-path first write (shard file absent or invalid header)
    ColdCreate,
    /// explicit full snapshot write (save_snapshot, write_filter_bucket, dump, etc.)
    Snapshot,
}

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

    /// Given a key and the store root directory, return the shard file path.
    fn shard_path(&self, key: &Self::Key, root: &Path) -> PathBuf;

    /// List all shard keys that exist under the store root.
    /// Used for compaction and enumeration.
    fn list_shards(&self, root: &Path) -> io::Result<Vec<Self::Key>>;
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
pub(crate) const HEADER_OPS_COUNT_OFFSET: u64 = 20;

/// Default janitor compaction threshold: compact when ops_count exceeds this.
/// Based on Ollie's final microbench results: 2x read overhead at 1,000 ops
/// is acceptable. Configurable per-field: tagIds tolerates 50K+, low-cardinality
/// fields like nsfwLevel should compact at ~5K.
///
/// Bumped 500 → 100_000 in v1.0.196-jemalloc: prod observation (2026-05-01)
/// showed hot tagIds shards (200-300 MB snapshots) hitting the 500-op threshold
/// in <1s of steady-state pg-sync traffic, causing every 5s merge cycle to
/// full-rewrite ~250 MB shards. This produced 500 MB/s sustained disk writes
/// (1.82 TB in 1h pod uptime) and triggered kernel `balance_dirty_pages()`
/// throttling — observed as ~17s pod-wide freezes every ~70s
/// (`io.pressure full=0.39`). 100K ops = ~7-15% ops-log:snapshot ratio on hot
/// shards, keeping compaction cost vs read replay cost in balance. Cold shards
/// unaffected (they never hit even 500 ops/cycle anyway).
pub const DEFAULT_COMPACT_THRESHOLD: u32 = 100_000;

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

/// CRC32 using hardware acceleration when available (SSE4.2/ARM NEON),
/// falling back to optimized software tables. 10-50x faster than naive.
pub(crate) fn crc32_of(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

/// CRC-32 lookup table (IEEE polynomial 0xEDB88320).
#[allow(dead_code)]
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

/// Write a complete shard file atomically: write to `.new`, fsync, rename over `path`,
/// then fsync the parent directory for POSIX durability.
///
/// This is the single write path for both initial creation and compaction.
/// All writes are durable and atomic — no split-lock windows.
pub(crate) fn write_shard_file_atomic(
    path: &Path,
    header: &ShardHeader,
    snapshot_bytes: &[u8],
    ops_bytes: &[u8],
    source: ShardRewriteSource,
) -> io::Result<()> {
    // Increment per-source rewrite counter for production observability.
    match source {
        ShardRewriteSource::Compact => {
            SHARD_REWRITES_COMPACT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        ShardRewriteSource::ColdCreate => {
            SHARD_REWRITES_COLD.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        ShardRewriteSource::Snapshot => {
            SHARD_REWRITES_SNAPSHOT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
    let new_path = path.with_extension("new");

    // Ensure parent directory exists
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut buf = Vec::with_capacity(HEADER_SIZE + snapshot_bytes.len() + ops_bytes.len());
    header.encode(&mut buf);
    buf.extend_from_slice(snapshot_bytes);
    buf.extend_from_slice(ops_bytes);

    // Must open read-write: Windows requires write permission for sync_all().
    let mut file = OpenOptions::new()
        .read(true).write(true).create(true).truncate(true)
        .open(&new_path)?;
    file.write_all(&buf)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&new_path, path)?;
    // Fsync parent directory for POSIX durability (no-op on Windows).
    fsync_parent_dir(path)?;

    Ok(())
}

/// Fsync the parent directory of `path`.
///
/// Required on POSIX (ext4, xfs) for a rename to be durable across power loss.
/// On Windows this is a no-op: the OS durability model doesn't require it.
fn fsync_parent_dir(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        if let Some(parent) = path.parent() {
            let dir = File::open(parent)?;
            dir.sync_all()?;
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path; // suppress unused warning on Windows
    }
    Ok(())
}

/// Check if a shard file has at least a full header (28 bytes).
/// Returns false for undersized stubs (e.g., 4-byte PreCreator placeholders).
pub(crate) fn is_valid_shard_file(path: &Path) -> bool {
    fs::metadata(path)
        .map(|m| m.len() >= HEADER_SIZE as u64)
        .unwrap_or(false)
}

/// Append ops bytes to an existing shard file and update the header's ops_count.
#[allow(dead_code)]
fn append_ops_to_shard(path: &Path, new_ops_bytes: &[u8], additional_count: u32) -> io::Result<()> {
    append_ops_to_shard_opts(path, new_ops_bytes, additional_count, true)
}

/// Append ops bytes with optional fsync. Set `fsync=false` when durability is
/// guaranteed by an upstream layer (WAL + cursor-gated persistence) — writes
/// still land in OS page cache and get flushed later, but we skip the per-op
/// journal round-trip that serializes on NTFS.
fn append_ops_to_shard_opts(
    path: &Path,
    new_ops_bytes: &[u8],
    additional_count: u32,
    fsync: bool,
) -> io::Result<()> {
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;

    let mut header_buf = [0u8; HEADER_SIZE];
    file.read_exact(&mut header_buf)?;
    let mut header = ShardHeader::decode(&header_buf)?;

    file.seek(SeekFrom::End(0))?;
    file.write_all(new_ops_bytes)?;

    header.ops_count += additional_count;
    file.seek(SeekFrom::Start(HEADER_OPS_COUNT_OFFSET))?;
    file.write_all(&header.ops_count.to_le_bytes())?;

    if fsync {
        file.sync_all()?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// ShardStore
// ---------------------------------------------------------------------------

/// The core unified storage engine.
///
/// Generic over snapshot codec, op codec, and sharding strategy.
/// Flat, single-file-per-shard — no generation directories.
/// Provides read/write/compact operations.
///
/// # Concurrency
///
/// A per-shard `RwLock<()>` prevents compaction from racing with appends on
/// the same shard file. Writers (`append_op`, `append_ops`) and readers hold a
/// **shared** (read) lock so they run in parallel across different shards.
/// Compaction (`compact_shard`) holds an **exclusive** (write) lock for the
/// full read→encode→write→fsync→rename cycle so no appends can slip in between
/// the snapshot read and the rename.
pub struct ShardStore<S, O, Sh>
where
    S: SnapshotCodec,
    O: OpCodec<Snapshot = S::Snapshot>,
    Sh: ShardingStrategy,
{
    root: PathBuf,
    sharding: Sh,
    /// Per-shard RwLock. Shared for readers/writers; exclusive for compactors.
    shard_locks: DashMap<Sh::Key, Arc<RwLock<()>>>,
    /// Hot-tunable compaction threshold (ops_count > threshold triggers compaction).
    /// Initialized to `DEFAULT_COMPACT_THRESHOLD`. Wired to runtime PATCH /config
    /// `bitmap_compact_threshold` so prod can dial it without a restart.
    compact_threshold: AtomicU32,
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
    /// Creates the root directory if it does not exist.
    /// Sweeps any orphan `.new` files left from a crashed compaction.
    pub fn new(root: PathBuf, sharding: Sh) -> io::Result<Self> {
        fs::create_dir_all(&root)?;

        // Sweep orphan .new files from a crashed compaction.
        // Safe: a `.new` file that was never renamed is incomplete and must be discarded.
        Self::sweep_orphan_new_files(&root)?;

        Ok(ShardStore {
            root,
            sharding,
            shard_locks: DashMap::new(),
            compact_threshold: AtomicU32::new(DEFAULT_COMPACT_THRESHOLD),
            _phantom_s: std::marker::PhantomData,
            _phantom_o: std::marker::PhantomData,
        })
    }

    /// Root directory of this store.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Get (or lazily create) the per-shard RwLock for `key`.
    ///
    /// Shared lock — for readers and writers (concurrent across different shards, but
    /// serialized with the compactor on the same shard).
    /// Exclusive lock — for compactors (blocks all other accessors on this shard).
    pub(crate) fn shard_lock(&self, key: &Sh::Key) -> Arc<RwLock<()>> {
        if let Some(existing) = self.shard_locks.get(key) {
            return Arc::clone(&*existing);
        }
        self.shard_locks
            .entry(key.clone())
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone()
    }

    /// Get the shard file path for a key.
    pub fn shard_path(&self, key: &Sh::Key) -> PathBuf {
        self.sharding.shard_path(key, &self.root)
    }

    /// Sweep orphan `.new` files from the store root (recursively).
    ///
    /// Called on startup to recover from a crash during batched-fsync compaction.
    ///
    /// **Smart sweep:** For each `.new` file found:
    /// - If the header is valid (`snapshot_len > 0`, parseable magic/version):
    ///   **promote** by renaming `.new → shard_path` (completes the interrupted compact).
    /// - Otherwise: delete (file was truncated or corrupt before fsync completed).
    ///
    /// Returns `Err` if any promotion or deletion fails.
    fn sweep_orphan_new_files(root: &Path) -> io::Result<()> {
        let (promoted, deleted, failed) = Self::sweep_dir(root)?;
        if promoted > 0 || deleted > 0 {
            eprintln!(
                "shard_store: startup sweep — promoted={promoted}, deleted={deleted}, failed={failed}"
            );
        }
        if failed > 0 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("shard_store: sweep failed for {failed} orphan .new file(s)"),
            ));
        }
        Ok(())
    }

    /// Recursively sweep a directory for orphan `.new` files.
    /// Returns (promoted, deleted, failed) counts.
    fn sweep_dir(dir: &Path) -> io::Result<(u64, u64, u64)> {
        if !dir.is_dir() {
            return Ok((0, 0, 0));
        }
        let mut promoted = 0u64;
        let mut deleted = 0u64;
        let mut failed = 0u64;

        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                let (p, d, f) = Self::sweep_dir(&path)?;
                promoted += p;
                deleted += d;
                failed += f;
            } else if path.extension().map_or(false, |e| e == "new") {
                // Decide: promote (valid header) or delete (truncated/corrupt)
                let target = path.with_extension("shard");
                let should_promote = Self::new_file_is_promotable(&path);
                if should_promote {
                    // Complete the interrupted compaction: rename .new → .shard
                    match fs::rename(&path, &target) {
                        Ok(()) => {
                            if let Err(e) = fsync_parent_dir(&target) {
                                eprintln!(
                                    "shard_store: sweep: fsync parent after promote {}: {e}",
                                    target.display()
                                );
                            }
                            promoted += 1;
                        }
                        Err(e) => {
                            eprintln!(
                                "shard_store: sweep: failed to promote {}: {e}",
                                path.display()
                            );
                            failed += 1;
                        }
                    }
                } else {
                    // Truncated or corrupt — delete
                    match fs::remove_file(&path) {
                        Ok(()) => { deleted += 1; }
                        Err(e) => {
                            eprintln!(
                                "shard_store: sweep: failed to delete orphan {}: {e}",
                                path.display()
                            );
                            failed += 1;
                        }
                    }
                }
            }
        }
        Ok((promoted, deleted, failed))
    }

    /// Returns true if a `.new` file has a valid, complete header with `snapshot_len > 0`.
    /// A fully-fsynced compaction result will always have a non-zero snapshot section.
    fn new_file_is_promotable(path: &Path) -> bool {
        let mut file = match File::open(path) {
            Ok(f) => f,
            Err(_) => return false,
        };
        let mut buf = [0u8; HEADER_SIZE];
        if file.read_exact(&mut buf).is_err() {
            return false;
        }
        match ShardHeader::decode(&buf) {
            Ok(h) => h.snapshot_len > 0,
            Err(_) => false,
        }
    }

    // -----------------------------------------------------------------------
    // Read path
    // -----------------------------------------------------------------------

    /// Read a snapshot for a key.
    ///
    /// Reads the shard file, applies any ops on top of the snapshot section,
    /// and returns the fully-materialized snapshot.
    ///
    /// Returns `None` if no shard exists for this key.
    ///
    /// Holds a **shared** shard lock to prevent reading a half-renamed file
    /// during concurrent compaction.
    pub fn read(&self, key: &Sh::Key) -> io::Result<Option<S::Snapshot>> {
        let lock = self.shard_lock(key);
        let _guard = lock.read();

        let shard_path = self.shard_path(key);

        // Skip invalid shard stubs (e.g. PreCreator empty files)
        if shard_path.exists() && !is_valid_shard_file(&shard_path) {
            return Ok(None);
        }

        let (header, snapshot_bytes, ops_bytes) = match read_shard_file_raw(&shard_path) {
            Ok(result) => result,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };

        let mut snapshot = if header.snapshot_len > 0 {
            S::decode(&snapshot_bytes)?
        } else {
            S::empty()
        };

        if header.ops_count > 0 {
            for op in read_op_entries::<O>(&ops_bytes) {
                O::apply(&mut snapshot, &op);
            }
        }

        Ok(Some(snapshot))
    }

    /// Read the raw ops count for a key.
    /// Used by janitor to decide if compaction is needed.
    ///
    /// Tolerates NotFound (missing shard).
    pub fn ops_count(&self, key: &Sh::Key) -> io::Result<Option<u32>> {
        let shard_path = self.shard_path(key);
        let mut file = match File::open(&shard_path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        let mut header_buf = [0u8; HEADER_SIZE];
        file.read_exact(&mut header_buf)?;
        let header = ShardHeader::decode(&header_buf)?;
        Ok(Some(header.ops_count))
    }

    /// Read only the 28-byte header from a shard file path. Returns None if file not found.
    pub(crate) fn read_header_at(path: &Path) -> io::Result<Option<ShardHeader>> {
        let mut file = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        let mut header_buf = [0u8; HEADER_SIZE];
        file.read_exact(&mut header_buf)?;
        Ok(Some(ShardHeader::decode(&header_buf)?))
    }

    // -----------------------------------------------------------------------
    // Write path
    // -----------------------------------------------------------------------

    /// Append a single op to the shard for this key.
    ///
    /// If no shard exists yet, creates one with an empty snapshot section.
    /// The snapshot will be populated on compaction.
    ///
    /// Holds a **shared** shard lock — multiple callers can append to different
    /// shards concurrently, but a concurrent compactor on the same shard will
    /// block until this write completes.
    pub fn append_op(&self, key: &Sh::Key, op: &O::Op) -> io::Result<()> {
        self.append_op_opts(key, op, true)
    }

    /// Like `append_op` but optionally skips the per-shard fsync.
    ///
    /// Durability contract: callers passing `fsync=false` MUST ensure writes
    /// are durable before any externally-visible commit. For bitmap opslog,
    /// the WAL provides this — on crash, WAL replay re-applies ops from the
    /// last durable cursor position, which is only advanced after the merge
    /// thread successfully persists + fsyncs. Page-cache writes are visible
    /// to the merge thread's compaction reader in the same process.
    pub fn append_op_opts(&self, key: &Sh::Key, op: &O::Op, fsync: bool) -> io::Result<()> {
        let lock = self.shard_lock(key);
        let _guard = lock.read();

        let shard_path = self.shard_path(key);

        let mut ops_buf = Vec::new();
        write_op_entry::<O>(op, &mut ops_buf);

        if shard_path.exists() && is_valid_shard_file(&shard_path) {
            append_ops_to_shard_opts(&shard_path, &ops_buf, 1, fsync)?;
        } else {
            // Cold-path shard creation: always fsync to ensure the shard file
            // actually exists on disk. The cost here is once per shard lifetime.
            let header = ShardHeader {
                version: SHARD_VERSION,
                ops_section_offset: HEADER_SIZE as u64,
                snapshot_len: 0,
                ops_count: 1,
                flags: 0,
            };
            write_shard_file_atomic(&shard_path, &header, &[], &ops_buf, ShardRewriteSource::ColdCreate)?;
        }

        Ok(())
    }

    /// Append multiple ops to the shard for this key.
    ///
    /// Holds a **shared** shard lock — same semantics as `append_op`.
    pub fn append_ops(&self, key: &Sh::Key, ops: &[O::Op]) -> io::Result<()> {
        self.append_ops_opts(key, ops, true)
    }

    /// Like `append_ops` but optionally skips the per-shard fsync.
    ///
    /// Durability contract: callers passing `fsync=false` MUST ensure writes
    /// are durable before any externally-visible commit (e.g. before
    /// acknowledging a WAL cursor advance). The WAL itself is fsync'd on
    /// write, so an unfsynced docstore append will be re-applied on crash
    /// recovery from the WAL.
    pub fn append_ops_opts(&self, key: &Sh::Key, ops: &[O::Op], fsync: bool) -> io::Result<()> {
        if ops.is_empty() {
            return Ok(());
        }

        let lock = self.shard_lock(key);
        let _guard = lock.read();

        let shard_path = self.shard_path(key);

        let mut ops_buf = Vec::new();
        for op in ops {
            write_op_entry::<O>(op, &mut ops_buf);
        }

        let count = ops.len() as u32;

        if shard_path.exists() && is_valid_shard_file(&shard_path) {
            append_ops_to_shard_opts(&shard_path, &ops_buf, count, fsync)?;
        } else {
            // Cold-path shard creation: always fsync to ensure the shard file
            // actually exists on disk. The cost here is once per shard lifetime.
            let header = ShardHeader {
                version: SHARD_VERSION,
                ops_section_offset: HEADER_SIZE as u64,
                snapshot_len: 0,
                ops_count: count,
                flags: 0,
            };
            write_shard_file_atomic(&shard_path, &header, &[], &ops_buf, ShardRewriteSource::ColdCreate)?;
        }

        Ok(())
    }

    /// Write a full snapshot for a key.
    ///
    /// This is the "bulk write" path — used during initial loading or compaction.
    /// Creates a shard with a materialized snapshot and zero ops.
    ///
    /// Holds a **shared** shard lock.
    pub fn write_snapshot(&self, key: &Sh::Key, snapshot: &S::Snapshot) -> io::Result<()> {
        let lock = self.shard_lock(key);
        let _guard = lock.read();

        let shard_path = self.shard_path(key);

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

        write_shard_file_atomic(&shard_path, &header, &snapshot_bytes, &[], ShardRewriteSource::Snapshot)?;
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

    /// Check if a shard needs compaction using the store's current threshold.
    /// Threshold is initialized to `DEFAULT_COMPACT_THRESHOLD` and is hot-tunable
    /// at runtime via `set_compact_threshold` (wired to PATCH /config
    /// `bitmap_compact_threshold` from the server layer).
    pub fn needs_compaction(&self, key: &Sh::Key) -> io::Result<bool> {
        let threshold = self.compact_threshold.load(Ordering::Relaxed);
        // 0 = auto-compaction disabled. (The explicit `should_compact(key, 0)`
        // path stays "compact if any ops" — it's what the manual /compact
        // endpoint uses to force a full compaction.)
        if threshold == 0 {
            return Ok(false);
        }
        self.should_compact(key, threshold)
    }

    /// Set the compaction threshold (ops_count > threshold triggers compaction).
    /// Atomic, lock-free — safe to call from any thread including the runtime
    /// PATCH /config handler. Effects apply on the next merge cycle.
    pub fn set_compact_threshold(&self, threshold: u32) {
        self.compact_threshold.store(threshold, Ordering::Relaxed);
    }

    /// Read the current compaction threshold.
    pub fn compact_threshold(&self) -> u32 {
        self.compact_threshold.load(Ordering::Relaxed)
    }

    /// Compact a shard in-place: read snapshot + ops under an exclusive lock, write back
    /// as a fresh snapshot with zero ops. The write is fully atomic: write `.new`,
    /// fsync, rename over `shard_path`, fsync parent dir — all while holding the lock.
    ///
    /// **Skip-clean fast-path:** If the shard already has a snapshot with zero ops,
    /// returns `Ok(false)` immediately (no I/O, no lock contention).
    ///
    /// Returns `true` if compaction was performed.
    ///
    /// # Concurrency safety
    ///
    /// Holds an **exclusive** shard lock for the entire window: snapshot read →
    /// encode → write `.new` → fsync → rename. No writer can append to this shard
    /// between the snapshot read and the rename. This eliminates the split-lock
    /// race that existed when fsync+rename was deferred to a separate pass.
    pub fn compact_shard(&self, key: &Sh::Key) -> io::Result<bool> {
        let lock = self.shard_lock(key);
        let _guard = lock.write();

        let shard_path = self.shard_path(key);

        // Fast-path: read only the 28-byte header to check if already clean.
        if let Some(header) = Self::read_header_at(&shard_path)? {
            if header.snapshot_len > 0 && header.ops_count == 0 {
                return Ok(false);
            }
        }

        // Read snapshot holding the exclusive lock — no new ops can append during this window.
        let snapshot = match self.read_unlocked(key)? {
            Some(s) => s,
            None => return Ok(false),
        };

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

        // Atomic: write .new, fsync, rename, fsync parent. All under the exclusive lock.
        write_shard_file_atomic(&shard_path, &header, &snapshot_bytes, &[], ShardRewriteSource::Compact)?;
        Ok(true)
    }

    /// Internal read without acquiring the shard lock.
    ///
    /// Used by `compact_shard` which holds the exclusive lock for the entire
    /// compact window (read → encode → write → rename).
    fn read_unlocked(&self, key: &Sh::Key) -> io::Result<Option<S::Snapshot>> {
        let shard_path = self.shard_path(key);

        if shard_path.exists() && !is_valid_shard_file(&shard_path) {
            return Ok(None);
        }

        let (header, snapshot_bytes, ops_bytes) = match read_shard_file_raw(&shard_path) {
            Ok(result) => result,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };

        let mut snapshot = if header.snapshot_len > 0 {
            S::decode(&snapshot_bytes)?
        } else {
            S::empty()
        };

        if header.ops_count > 0 {
            for op in read_op_entries::<O>(&ops_bytes) {
                O::apply(&mut snapshot, &op);
            }
        }

        Ok(Some(snapshot))
    }

    /// Compact a shard in-place (alias for `compact_shard`).
    ///
    /// Provided for backward compatibility with callers that used the
    /// old `compact_current` name from the gen-based model.
    #[inline]
    pub fn compact_current(&self, key: &Sh::Key) -> io::Result<()> {
        self.compact_shard(key).map(|_| ())
    }

    /// List all shard keys in this store.
    pub fn list_shards(&self) -> io::Result<Vec<Sh::Key>> {
        self.sharding.list_shards(&self.root)
    }

    /// Compatibility alias for `list_shards`.
    /// Previously named `list_current_shards` in the gen-based model.
    #[inline]
    pub fn list_current_shards(&self) -> io::Result<Vec<Sh::Key>> {
        self.list_shards()
    }

    /// Fsync every shard file in this store.
    ///
    /// Called by the merge thread before persisting the WAL cursor to ensure
    /// all opslog entries appended with `fsync=false` by the flush thread are
    /// durable on disk.  Without this, an OS crash (not a process crash) between
    /// opslog append and cursor persist could advance the cursor past mutations
    /// that exist only in page cache, causing those WAL ops to be silently
    /// skipped on restart.
    ///
    /// Cost: one `sync_all()` per existing shard file, called once per merge
    /// interval (~60 s default).  Off the per-flush hot path.
    ///
    /// Returns the count of shards successfully synced.  On any failure the
    /// last error is returned so the caller can suppress cursor persistence.
    pub fn sync_all_opslogs(&self) -> io::Result<usize> {
        if SYNC_INJECT_FAIL.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "test: sync_all_opslogs injected failure",
            ));
        }
        let keys = self.list_shards()?;
        let mut synced = 0usize;
        let mut last_err: Option<io::Error> = None;
        for key in &keys {
            let shard_path = self.shard_path(key);
            // Acquire the shared shard lock BEFORE opening.  Do NOT call
            // shard_path.exists() first — that creates a classic TOCTOU race:
            // compaction holds the write lock to rename/replace the shard file,
            // and a gap between exists() and lock/open lets us see the old name
            // but open the post-rename file (or a NotFound).  By locking first
            // we ensure compaction has fully committed before we open.
            let lock = self.shard_lock(key);
            let _guard = lock.read();
            match fs::OpenOptions::new().read(true).write(true).open(&shard_path) {
                Ok(f) => {
                    if let Err(e) = f.sync_all() {
                        eprintln!(
                            "shard_store: sync_all_opslogs: fsync failed for {}: {e}",
                            shard_path.display()
                        );
                        last_err = Some(e);
                    } else {
                        synced += 1;
                    }
                }
                // File not found: shard key was listed but file was subsequently
                // removed (e.g. the shard was a placeholder with no content).
                // This is safe to skip — no opslog data to sync.
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => {
                    eprintln!(
                        "shard_store: sync_all_opslogs: open failed for {}: {e}",
                        shard_path.display()
                    );
                    last_err = Some(e);
                }
            }
        }
        if let Some(e) = last_err {
            return Err(e);
        }
        Ok(synced)
    }

    /// Check if a shard exists.
    pub fn shard_exists(&self, key: &Sh::Key) -> bool {
        self.shard_path(key).exists()
    }

    /// Read only the header of a shard.
    /// Useful for checking ops count without reading the full file.
    pub fn read_header(&self, key: &Sh::Key) -> io::Result<Option<ShardHeader>> {
        let shard_path = self.shard_path(key);
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
    pub fn write_snapshots_batch(
        &self,
        entries: &[(Sh::Key, S::Snapshot)],
    ) -> io::Result<()> {
        for (key, snapshot) in entries {
            self.write_snapshot(key, snapshot)?;
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
    use ahash::AHashMap as HashMap;

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

        fn shard_path(&self, key: &String, root: &Path) -> PathBuf {
            root.join(format!("{}.shard", key))
        }

        fn list_shards(&self, root: &Path) -> io::Result<Vec<String>> {
            let mut keys = Vec::new();
            if !root.exists() {
                return Ok(keys);
            }
            for entry in fs::read_dir(root)? {
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

        // Compact
        let did = store.compact_shard(&"doc1".to_string()).unwrap();
        assert!(did);

        // After compaction: zero ops, data preserved
        assert_eq!(store.ops_count(&"doc1".to_string()).unwrap(), Some(0));
        let result = store.read(&"doc1".to_string()).unwrap().unwrap();
        assert_eq!(result.values.get("a").unwrap(), "1");
        assert_eq!(result.values.get("b").unwrap(), "2");
    }

    #[test]
    fn test_compact_shard_skips_clean() {
        let (_dir, store) = temp_store();
        let mut snap = TestSnapshot { values: HashMap::new() };
        snap.values.insert("v".into(), "clean".into());
        store.write_snapshot(&"doc1".to_string(), &snap).unwrap();

        let did = store.compact_shard(&"doc1".to_string()).unwrap();
        assert!(!did, "should skip clean shard");
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
        let data = b"hello world";
        let crc1 = crc32_of(data);
        let crc2 = crc32_of(data);
        assert_eq!(crc1, crc2);

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
    fn test_compact_threshold_runtime_tunable() {
        let (_dir, store) = temp_store();

        // Default threshold = DEFAULT_COMPACT_THRESHOLD
        assert_eq!(store.compact_threshold(), DEFAULT_COMPACT_THRESHOLD);

        // 3 ops, default threshold (100K) → not compactable
        store.append_op(&"doc1".to_string(), &TestOp::Set { key: "a".into(), value: "1".into() }).unwrap();
        store.append_op(&"doc1".to_string(), &TestOp::Set { key: "b".into(), value: "2".into() }).unwrap();
        store.append_op(&"doc1".to_string(), &TestOp::Set { key: "c".into(), value: "3".into() }).unwrap();
        assert!(!store.needs_compaction(&"doc1".to_string()).unwrap());

        // Drop threshold to 2 — same shard now triggers
        store.set_compact_threshold(2);
        assert_eq!(store.compact_threshold(), 2);
        assert!(store.needs_compaction(&"doc1".to_string()).unwrap());

        // Bump back high — no longer triggers
        store.set_compact_threshold(1_000_000);
        assert!(!store.needs_compaction(&"doc1".to_string()).unwrap());
    }

    #[test]
    fn test_compact_shard_with_ops_only_no_snapshot() {
        let (_dir, store) = temp_store();

        // Write only ops (no snapshot) — shard should still compact
        store.append_op(&"doc1".to_string(), &TestOp::Set { key: "a".into(), value: "1".into() }).unwrap();
        store.append_op(&"doc1".to_string(), &TestOp::Set { key: "b".into(), value: "2".into() }).unwrap();

        let did = store.compact_shard(&"doc1".to_string()).unwrap();
        assert!(did);

        // After compaction, should be a clean snapshot with 0 ops
        let header = store.read_header(&"doc1".to_string()).unwrap().unwrap();
        assert_eq!(header.ops_count, 0);
        assert!(header.snapshot_len > 0);

        // Data should be preserved
        let result = store.read(&"doc1".to_string()).unwrap().unwrap();
        assert_eq!(result.values.get("a").unwrap(), "1");
        assert_eq!(result.values.get("b").unwrap(), "2");
    }

    #[test]
    fn test_compact_nonexistent_shard_returns_false() {
        let (_dir, store) = temp_store();
        let did = store.compact_shard(&"nope".to_string()).unwrap();
        assert!(!did);
    }

    #[test]
    fn test_list_shards() {
        let (_dir, store) = temp_store();

        store.write_snapshot(&"a".to_string(), &TestSnapshot {
            values: HashMap::new(),
        }).unwrap();
        store.write_snapshot(&"b".to_string(), &TestSnapshot {
            values: HashMap::new(),
        }).unwrap();

        let mut shards = store.list_shards().unwrap();
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
    fn test_append_ops_replaces_undersized_stub() {
        // Simulate PreCreator stub: file exists but only has 4 bytes (magic only).
        let dir = tempfile::tempdir().unwrap();
        let store = TestStore::new(dir.path().to_path_buf(), FlatShard).unwrap();

        // Manually create a 4-byte stub at the shard path
        let key = "stub_shard".to_string();
        let shard_path = store.shard_path(&key);
        if let Some(parent) = shard_path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(&shard_path, &SHARD_MAGIC).unwrap();
        assert_eq!(fs::metadata(&shard_path).unwrap().len(), 4);

        // append_ops should succeed by replacing the stub
        store.append_op(&key, &TestOp::Set {
            key: "name".into(), value: "test".into()
        }).unwrap();

        // Read should return the appended data
        let result = store.read(&key).unwrap().unwrap();
        assert_eq!(result.values.get("name").unwrap(), "test");
    }

    #[test]
    fn test_read_skips_undersized_stub() {
        let dir = tempfile::tempdir().unwrap();
        let store = TestStore::new(dir.path().to_path_buf(), FlatShard).unwrap();

        let key = "stub_shard".to_string();
        let shard_path = store.shard_path(&key);
        if let Some(parent) = shard_path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(&shard_path, &SHARD_MAGIC).unwrap();

        // read should return None (stub is skipped), not error
        let result = store.read(&key).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_orphan_new_file_swept_on_startup() {
        let dir = tempfile::tempdir().unwrap();

        // Create an orphan .new file
        let orphan = dir.path().join("orphan.new");
        fs::write(&orphan, b"garbage").unwrap();
        assert!(orphan.exists());

        // Creating a new store should sweep it
        let _store = TestStore::new(dir.path().to_path_buf(), FlatShard).unwrap();
        assert!(!orphan.exists(), "orphan .new file should be swept on startup");
    }

    #[test]
    fn test_compact_shard_is_atomic() {
        // Verifies that compact_shard write+fsync+rename happens atomically under lock:
        // after compact_shard returns Ok(true) the shard has ops_count=0 and is readable.
        let (_dir, store) = temp_store();

        store.append_op(&"doc1".to_string(), &TestOp::Set {
            key: "x".into(), value: "42".into()
        }).unwrap();
        store.append_op(&"doc1".to_string(), &TestOp::Set {
            key: "y".into(), value: "99".into()
        }).unwrap();

        // After compact_shard: no .new file on disk, live shard exists with 0 ops
        let did_compact = store.compact_shard(&"doc1".to_string()).unwrap();
        assert!(did_compact);

        let shard_path = store.shard_path(&"doc1".to_string());
        let new_path = shard_path.with_extension("new");
        assert!(!new_path.exists(), ".new file must not remain after compact_shard");
        assert!(shard_path.exists(), "live shard must exist");
        assert_eq!(store.ops_count(&"doc1".to_string()).unwrap(), Some(0));

        // Data is correct
        let result = store.read(&"doc1".to_string()).unwrap().unwrap();
        assert_eq!(result.values.get("x").unwrap(), "42");
        assert_eq!(result.values.get("y").unwrap(), "99");
    }

    #[test]
    fn test_ops_count_tolerates_not_found() {
        let (_dir, store) = temp_store();
        assert!(store.ops_count(&"nonexistent".to_string()).unwrap().is_none());
    }

    #[test]
    fn test_read_header_at() {
        let (_dir, store) = temp_store();

        // Non-existent file returns None
        let path = store.shard_path(&"nope".to_string());
        assert!(ShardStore::<TestSnapshotCodec, TestOpCodec, FlatShard>::read_header_at(&path).unwrap().is_none());

        // Write a snapshot + 2 ops, verify header
        let snap = TestSnapshot { values: HashMap::new() };
        store.write_snapshot(&"doc1".to_string(), &snap).unwrap();
        store.append_op(&"doc1".to_string(), &TestOp::Set { key: "a".into(), value: "b".into() }).unwrap();
        store.append_op(&"doc1".to_string(), &TestOp::Set { key: "c".into(), value: "d".into() }).unwrap();

        let path = store.shard_path(&"doc1".to_string());
        let header = ShardStore::<TestSnapshotCodec, TestOpCodec, FlatShard>::read_header_at(&path).unwrap().unwrap();
        assert_eq!(header.ops_count, 2);
        assert!(header.snapshot_len > 0);
    }
}
