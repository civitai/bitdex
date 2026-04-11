//! Typed append-only ops log backed by a memory-mapped file.
//!
//! Frame format per entry:
//!
//! ```text
//! [u32  op_bytes_len]
//! [N    op_bytes]     -- produced by OpCodec::encode_op
//! [u32  crc32]        -- over [op_bytes_len][op_bytes]
//! ```
//!
//! Unlike v3's original datasilo ops log (which stored opaque `Put(key, bytes)`
//! and shoved merge logic into a compaction callback), this log is generic
//! over an `OpCodec` — the same pattern used by v2 ShardStore. Ops are
//! typed first-class values. Compaction folds them into a snapshot via
//! `OpCodec::apply`. No per-write doc re-encoding.
//!
//! Two write modes:
//! - **Sequential** (`append`): single-thread, tight packing. Steady-state path.
//! - **Parallel** (via `ParallelOpsWriter`): lock-free 1MB thread-local regions.
//!   Used by bulk / dump writes.
//!
//! The log is mmap'd so reads are zero-copy through the page cache. Padding
//! (zero bytes) between thread-local regions is skipped transparently.

use std::fs::OpenOptions;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::traits::OpCodec;

/// Initial ops log file size (64 MB). Grows as needed.
const INITIAL_SIZE: u64 = 64 * 1024 * 1024;

/// Thread-local parallel write region size.
pub(crate) const REGION_SIZE: u64 = 1 << 20; // 1 MiB

/// Zero-copy view of a framed op in the mmap.
#[derive(Clone, Copy)]
pub struct OpSlice<'a> {
    pub bytes: &'a [u8],
}

/// Mmap'd append-only framed ops log.
///
/// The log is generic over `OpCodec` only at the call-site boundary — the
/// log itself stores `(len, bytes, crc)` frames and does not care what's
/// inside. Decoding happens in `for_each` / `get` via the codec.
pub struct OpsLog {
    path: PathBuf,
    mmap: Option<memmap2::MmapMut>,
    /// Current append cursor — parallel writers bump it atomically.
    cursor: AtomicU64,
    /// Total file size (capacity). Grows in place when cursor approaches it.
    capacity: u64,
}

// Safety: parallel writers claim disjoint regions via atomic cursor.
unsafe impl Send for OpsLog {}
unsafe impl Sync for OpsLog {}

impl OpsLog {
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
            let file = OpenOptions::new().read(true).write(true).open(&path)?;
            let mmap = unsafe { memmap2::MmapMut::map_mut(&file)? };
            #[cfg(unix)]
            let _ = mmap.advise(memmap2::Advice::Sequential);
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

    pub fn ensure_capacity(&mut self, min_size: u64) -> io::Result<()> {
        if self.capacity >= min_size && self.mmap.is_some() {
            return Ok(());
        }
        let new_size = min_size.max(INITIAL_SIZE).max(self.capacity * 2);
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&self.path)?;
        file.set_len(new_size)?;
        let mmap = unsafe { memmap2::MmapMut::map_mut(&file)? };
        #[cfg(unix)]
        let _ = mmap.advise(memmap2::Advice::Sequential);
        self.mmap = Some(mmap);
        self.capacity = new_size;
        Ok(())
    }

    /// Append a typed op to the log (sequential, single-thread).
    /// Auto-grows the file if needed. Caller must `flush()` when a batch is done.
    pub fn append<C: OpCodec>(&mut self, op: &C::Op) -> io::Result<()> {
        let mut frame = Vec::with_capacity(64);
        Self::encode_frame::<C>(&mut frame, op);
        let needed = self.cursor.load(Ordering::Relaxed) + frame.len() as u64;
        if needed > self.capacity || self.mmap.is_none() {
            self.ensure_capacity(needed + INITIAL_SIZE)?;
        }
        let offset = self
            .cursor
            .fetch_add(frame.len() as u64, Ordering::Relaxed) as usize;
        let mmap = self.mmap.as_ref().unwrap();
        if offset + frame.len() <= mmap.len() {
            unsafe {
                let dst = mmap.as_ptr().add(offset) as *mut u8;
                std::ptr::copy_nonoverlapping(frame.as_ptr(), dst, frame.len());
            }
        }
        Ok(())
    }

    /// Append many ops sequentially (reuses one encode buffer across ops).
    pub fn append_batch<C: OpCodec>(&mut self, ops: &[C::Op]) -> io::Result<()> {
        let mut frame = Vec::with_capacity(128);
        for op in ops {
            frame.clear();
            Self::encode_frame::<C>(&mut frame, op);
            let needed = self.cursor.load(Ordering::Relaxed) + frame.len() as u64;
            if needed > self.capacity || self.mmap.is_none() {
                self.ensure_capacity(needed + INITIAL_SIZE)?;
            }
            let offset = self
                .cursor
                .fetch_add(frame.len() as u64, Ordering::Relaxed) as usize;
            let mmap = self.mmap.as_ref().unwrap();
            if offset + frame.len() <= mmap.len() {
                unsafe {
                    let dst = mmap.as_ptr().add(offset) as *mut u8;
                    std::ptr::copy_nonoverlapping(frame.as_ptr(), dst, frame.len());
                }
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

    pub fn cursor(&self) -> &AtomicU64 {
        &self.cursor
    }

    pub fn mmap_ptr(&self) -> Option<*mut u8> {
        self.mmap.as_ref().map(|m| m.as_ptr() as *mut u8)
    }

    pub fn mmap_len(&self) -> usize {
        self.mmap.as_ref().map(|m| m.len()).unwrap_or(0)
    }

    pub fn data_size(&self) -> u64 {
        self.cursor.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.cursor.load(Ordering::Relaxed) == 0
    }

    /// Truncate the log (post-compaction). Drops mmap, truncates file to zero.
    pub fn truncate(&mut self) -> io::Result<()> {
        self.mmap = None;
        self.cursor = AtomicU64::new(0);
        self.capacity = 0;
        if self.path.exists() {
            let file = OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(&self.path)?;
            drop(file);
        }
        Ok(())
    }

    /// Iterate over every valid framed op in the log, in append order.
    /// `f` receives the raw op bytes. The caller decodes via `OpCodec::decode_op`
    /// — this lets the log stay generic-free at the iteration layer.
    ///
    /// Returns the number of valid ops consumed.
    pub fn for_each<F>(&self, mut f: F) -> io::Result<u64>
    where
        F: FnMut(&[u8]),
    {
        let mmap = match &self.mmap {
            Some(m) => m,
            None => return Ok(0),
        };
        let end = self.cursor.load(Ordering::Relaxed) as usize;
        if end == 0 {
            return Ok(0);
        }
        let data = &mmap[..end.min(mmap.len())];
        let mut pos = 0;
        let mut count = 0u64;

        while pos < data.len() {
            if data[pos] == 0 {
                // Padding between parallel regions.
                while pos < data.len() && data[pos] == 0 {
                    pos += 1;
                }
                continue;
            }
            let frame_start = pos;
            if pos + 4 > data.len() {
                break;
            }
            let op_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            if op_len == 0 || pos + op_len + 4 > data.len() {
                break;
            }
            let op_bytes = &data[pos..pos + op_len];
            pos += op_len;
            let expected_crc =
                u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
            pos += 4;
            let actual_crc = crc32fast::hash(&data[frame_start..pos - 4]);
            if actual_crc == expected_crc {
                f(op_bytes);
                count += 1;
            }
            // On CRC mismatch: skip and continue — treat the frame as corrupt padding.
        }

        Ok(count)
    }

    // ── Framing helpers ────────────────────────────────────────────────

    /// Encode one op frame into `buf`:
    /// `[op_len:u32][op_bytes][crc32:u32]` where crc is over the first two.
    #[inline]
    pub fn encode_frame<C: OpCodec>(buf: &mut Vec<u8>, op: &C::Op) {
        buf.clear();
        // Reserve length prefix.
        buf.extend_from_slice(&0u32.to_le_bytes());
        C::encode_op(op, buf);
        let op_len = (buf.len() - 4) as u32;
        buf[0..4].copy_from_slice(&op_len.to_le_bytes());
        let crc = crc32fast::hash(&buf[..]);
        buf.extend_from_slice(&crc.to_le_bytes());
    }

    /// Write a pre-encoded frame at a specific offset (parallel writers).
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

    /// Scan forward from start, decoding frames, to find the last valid end.
    /// Used when reopening a file to set the cursor correctly past truncated tails.
    fn find_data_end(mmap: &[u8]) -> usize {
        let mut pos = 0;
        let mut last_valid_end = 0;
        while pos < mmap.len() {
            if mmap[pos] == 0 {
                pos += 1;
                continue;
            }
            let frame_start = pos;
            if pos + 4 > mmap.len() {
                break;
            }
            let op_len =
                u32::from_le_bytes(mmap[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            if op_len == 0 || pos + op_len + 4 > mmap.len() {
                pos = frame_start + 1;
                continue;
            }
            let payload_end = pos + op_len;
            let expected_crc =
                u32::from_le_bytes(mmap[payload_end..payload_end + 4].try_into().unwrap());
            let actual_crc = crc32fast::hash(&mmap[frame_start..payload_end]);
            if actual_crc == expected_crc {
                pos = payload_end + 4;
                last_valid_end = pos;
            } else {
                pos = frame_start + 1;
            }
        }
        last_valid_end
    }
}

/// Lock-free parallel writer handle for the ops log mmap.
///
/// Each rayon thread claims 1 MiB regions via atomic cursor bump, writes
/// disjoint frames. Caller is responsible for not reallocating the mmap
/// while any `ParallelOpsWriter` is outstanding.
pub struct ParallelOpsWriter {
    cursor: *const AtomicU64,
    mmap_ptr: *mut u8,
    mmap_len: usize,
    pub overflow_count: AtomicU64,
}

unsafe impl Send for ParallelOpsWriter {}
unsafe impl Sync for ParallelOpsWriter {}

impl ParallelOpsWriter {
    /// Create a writer from the active ops log's cursor + mmap.
    /// Returns `None` if the mmap is not yet allocated.
    pub fn from_log(log: &OpsLog) -> Option<Self> {
        let mmap_ptr = log.mmap_ptr()?;
        Some(Self {
            cursor: log.cursor() as *const AtomicU64,
            mmap_ptr,
            mmap_len: log.mmap_len(),
            overflow_count: AtomicU64::new(0),
        })
    }

    /// Write an already-framed op at a claimed offset.
    /// Thread-local cursor / region-end bookkeeping is the caller's responsibility.
    #[inline]
    pub fn write_frame(
        &self,
        frame: &[u8],
        local_cursor: &mut usize,
        local_end: &mut usize,
    ) -> bool {
        let frame_len = frame.len();
        if *local_cursor + frame_len > *local_end {
            let cursor = unsafe { &*self.cursor };
            let start = cursor.fetch_add(REGION_SIZE, Ordering::Relaxed) as usize;
            *local_cursor = start;
            *local_end = start + REGION_SIZE as usize;
        }
        if *local_cursor + frame_len > self.mmap_len {
            self.overflow_count.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        unsafe {
            let dst = self.mmap_ptr.add(*local_cursor);
            std::ptr::copy_nonoverlapping(frame.as_ptr(), dst, frame_len);
        }
        *local_cursor += frame_len;
        true
    }
}
