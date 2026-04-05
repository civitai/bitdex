//! Open-addressed hash table stored in a memory-mapped file.
//!
//! # Design
//!
//! Each slot in the table is a 24-byte `HashEntry`:
//!
//! ```text
//! bytes  0– 7  key       u64  0 = empty, u64::MAX = tombstone
//! bytes  8–15  offset    u64  value location in the data file
//! bytes 16–19  length    u32  byte length of stored value
//! bytes 20–23  allocated u32  bytes allocated
//! ```
//!
//! The file layout is a fixed-size header followed by `capacity` consecutive
//! `HashEntry` slots:
//!
//! ```text
//! bytes  0– 7  magic     u64  0x4841534849445831 ("HASHIDX1")
//! bytes  8–15  capacity  u64  number of slots
//! bytes 16–23  count     u64  number of live (non-tombstone) entries
//! bytes 24–31  occupied  u64  number of used slots (live + tombstone)
//! bytes 32–..  slots     HashEntry[capacity]
//! ```
//!
//! # Collision resolution
//!
//! Linear probing.  The initial probe slot is `key % capacity`.  Keys `0` and
//! `u64::MAX` are reserved as sentinels; callers must ensure their u64 keys
//! avoid those values (e.g. by hashing through a bijection before calling
//! [`HashIndex`]).
//!
//! # Load factor
//!
//! All mutating operations check the load factor *before* inserting.  If
//! occupied slots (live + tombstone) would reach ≥ 75 % of capacity the
//! operation returns [`SiloError::TableFull`].  Callers should provision
//! `capacity ≈ 2× expected_entries` to stay comfortably below that limit.
//!
//! # Thread safety
//!
//! The mmap is shared via raw pointer arithmetic.  `HashIndex` is `Send +
//! Sync` (declared via `unsafe impl`) provided the caller serialises all
//! concurrent writers.  Concurrent *reads* are safe because Rust's shared-
//! reference rules are upheld at the entry-copy level.

use std::fs::OpenOptions;
use std::path::Path;

use memmap2::MmapMut;

use crate::{IndexEntry, Result, SiloError};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Magic number written to the first 8 bytes of every hash index file.
/// ASCII "HASHIDX1".
const MAGIC: u64 = 0x4841_5348_4944_5831;

/// Key value meaning "this slot is empty" — never a valid user key.
const KEY_EMPTY: u64 = 0;

/// Key value meaning "this slot held an entry that was removed" (tombstone).
const KEY_TOMBSTONE: u64 = u64::MAX;

/// Byte size of the file header.
const HEADER_SIZE: usize = 32; // magic(8) + capacity(8) + count(8) + occupied(8)

/// Byte size of one hash table slot on disk.
const ENTRY_SIZE: usize = 24; // key(8) + offset(8) + length(4) + allocated(4)

// Compile-time assertion so that any future layout changes are caught.
const _: () = assert!(ENTRY_SIZE == std::mem::size_of::<HashEntry>());

// ---------------------------------------------------------------------------
// On-disk entry layout
// ---------------------------------------------------------------------------

/// A single slot in the hash table (24 bytes, `#[repr(C)]`).
#[derive(Debug, Clone, Copy)]
#[repr(C)]
struct HashEntry {
    /// Lookup key. `KEY_EMPTY` (0) = unused; `KEY_TOMBSTONE` (u64::MAX) = deleted.
    key: u64,
    /// Byte offset into the data file.
    offset: u64,
    /// Byte length of the stored value.
    length: u32,
    /// Bytes allocated for the entry (>= length).
    allocated: u32,
}

// ---------------------------------------------------------------------------
// HashIndex
// ---------------------------------------------------------------------------

/// An mmap-backed open-addressed hash table mapping `u64` keys to
/// [`IndexEntry`] values.
///
/// See the [module-level documentation](self) for the full design.
pub struct HashIndex {
    mmap: MmapMut,
    /// Number of slots in the table (fixed at creation time).
    capacity: u64,
    /// Number of live (non-tombstone) entries.
    count: u64,
    /// Number of occupied slots (live + tombstone). Used for O(1) load-factor checks.
    occupied: u64,
}

// SAFETY: The mmap pointer is only dereferenced through methods that either
// hold `&mut self` (writes) or copy entry data out (reads).  No aliased
// mutable references are created.
unsafe impl Send for HashIndex {}
unsafe impl Sync for HashIndex {}

impl HashIndex {
    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /// Create a new hash index file at `path` with `capacity` slots.
    ///
    /// The file is zero-filled, which initialises every key to `KEY_EMPTY`.
    /// Returns an error if the file already exists.
    pub fn new(path: &Path, capacity: u64) -> Result<Self> {
        assert!(capacity > 0, "capacity must be > 0");

        let file_size = Self::file_size_for(capacity);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;

        file.set_len(file_size as u64)?;

        // SAFETY: The file was just created and set to the correct length.
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        // Random hint: hash table probes are uniformly distributed across the file.
        #[cfg(unix)] let _ = mmap.advise(memmap2::Advice::Random);

        // Write header.
        write_u64(&mut mmap, 0, MAGIC);
        write_u64(&mut mmap, 8, capacity);
        write_u64(&mut mmap, 16, 0); // count = 0
        write_u64(&mut mmap, 24, 0); // occupied = 0

        mmap.flush()?;

        Ok(Self { mmap, capacity, count: 0, occupied: 0 })
    }

    /// Open an existing hash index file at `path`.
    ///
    /// Reads the capacity and count from the file header.
    pub fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;

        // SAFETY: The file is open and we trust its contents (checked via magic).
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        // Random hint: hash table probes are uniformly distributed across the file.
        #[cfg(unix)] let _ = mmap.advise(memmap2::Advice::Random);

        if mmap.len() < HEADER_SIZE {
            return Err(SiloError::InvalidFile);
        }

        let magic = read_u64(&mmap, 0);
        if magic != MAGIC {
            return Err(SiloError::InvalidFile);
        }

        let capacity = read_u64(&mmap, 8);
        let count = read_u64(&mmap, 16);
        let occupied = read_u64(&mmap, 24);

        let expected_size = Self::file_size_for(capacity);
        if mmap.len() < expected_size {
            return Err(SiloError::InvalidFile);
        }

        Ok(Self { mmap, capacity, count, occupied })
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /// Look up `key` and return its [`IndexEntry`], or `None` if not found.
    ///
    /// Keys `0` and `u64::MAX` are reserved and will always return `None`.
    pub fn get(&self, key: u64) -> Option<IndexEntry> {
        if key == KEY_EMPTY || key == KEY_TOMBSTONE {
            return None;
        }

        let mut slot = self.probe_start(key);
        for _ in 0..self.capacity {
            let entry = self.read_entry(slot);
            match entry.key {
                KEY_EMPTY => return None,           // definitely not present
                KEY_TOMBSTONE => {}                 // skip — keep probing
                k if k == key => {
                    return Some(IndexEntry {
                        offset: entry.offset,
                        length: entry.length,
                        allocated: entry.allocated,
                    });
                }
                _ => {}                             // different key — keep probing
            }
            slot = self.next_slot(slot);
        }

        None // table is full of tombstones — key absent
    }

    /// Insert or update `key` with the given [`IndexEntry`].
    ///
    /// If `key` already exists its entry is updated in place.
    /// Returns [`SiloError::ReservedKey`] for keys `0` or `u64::MAX`.
    /// Returns [`SiloError::TableFull`] when the load factor would exceed 75 %.
    pub fn put(&mut self, key: u64, value: IndexEntry) -> Result<()> {
        if key == KEY_EMPTY {
            return Err(SiloError::ReservedKey);
        }
        if key == KEY_TOMBSTONE {
            return Err(SiloError::ReservedKey);
        }

        // Check load factor using the O(1) `occupied` counter (live + tombstone).
        // We gate on occupied+1 > 75% capacity so probing chains stay short.
        if self.occupied + 1 > self.capacity * 3 / 4 {
            return Err(SiloError::TableFull);
        }

        let mut slot = self.probe_start(key);
        let mut tombstone_slot: Option<u64> = None;

        for _ in 0..self.capacity {
            let entry = self.read_entry(slot);
            match entry.key {
                KEY_EMPTY => {
                    // Insert at the first tombstone we found (reuses the slot),
                    // or at this empty slot (claims a new slot).
                    let target = tombstone_slot.unwrap_or(slot);
                    let reusing_tombstone = tombstone_slot.is_some();
                    self.write_entry(target, HashEntry {
                        key,
                        offset: value.offset,
                        length: value.length,
                        allocated: value.allocated,
                    });
                    // live count always increases for a brand-new key
                    self.count += 1;
                    write_u64(&mut self.mmap, 16, self.count);
                    // occupied increases only when we claim a fresh empty slot
                    if !reusing_tombstone {
                        self.occupied += 1;
                        write_u64(&mut self.mmap, 24, self.occupied);
                    }
                    return Ok(());
                }
                KEY_TOMBSTONE => {
                    if tombstone_slot.is_none() {
                        tombstone_slot = Some(slot);
                    }
                }
                k if k == key => {
                    // Update existing entry in place — neither count nor occupied changes.
                    self.write_entry(slot, HashEntry {
                        key,
                        offset: value.offset,
                        length: value.length,
                        allocated: value.allocated,
                    });
                    return Ok(());
                }
                _ => {}
            }
            slot = self.next_slot(slot);
        }

        // All slots probed — can only happen when table is entirely tombstones.
        // Reuse the first tombstone slot.
        if let Some(ts) = tombstone_slot {
            self.write_entry(ts, HashEntry {
                key,
                offset: value.offset,
                length: value.length,
                allocated: value.allocated,
            });
            self.count += 1;
            write_u64(&mut self.mmap, 16, self.count);
            // occupied stays the same (reusing a tombstone, not claiming an empty slot)
            return Ok(());
        }

        Err(SiloError::TableFull)
    }

    /// Remove `key` by writing a tombstone.
    ///
    /// No-op if the key does not exist.  Returns `true` if the entry was
    /// found and removed, `false` if it was not present.
    pub fn remove(&mut self, key: u64) -> bool {
        if key == KEY_EMPTY || key == KEY_TOMBSTONE {
            return false;
        }

        let mut slot = self.probe_start(key);
        for _ in 0..self.capacity {
            let entry = self.read_entry(slot);
            match entry.key {
                KEY_EMPTY => return false,
                KEY_TOMBSTONE => {}
                k if k == key => {
                    // Overwrite with tombstone.
                    let ts = HashEntry {
                        key: KEY_TOMBSTONE,
                        offset: 0,
                        length: 0,
                        allocated: 0,
                    };
                    self.write_entry(slot, ts);
                    self.count = self.count.saturating_sub(1);
                    write_u64(&mut self.mmap, 16, self.count);
                    return true;
                }
                _ => {}
            }
            slot = self.next_slot(slot);
        }

        false
    }

    /// Return the number of live (non-tombstone) entries.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Return the table capacity (total number of slots).
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Flush all dirty mmap pages to the underlying file.
    pub fn flush(&self) -> Result<()> {
        self.mmap.flush()?;
        Ok(())
    }

    /// Iterate over all live entries in the table.
    ///
    /// Order is unspecified (hash table traversal order).
    pub fn iter(&self) -> impl Iterator<Item = (u64, IndexEntry)> + '_ {
        (0..self.capacity).filter_map(move |slot| {
            let entry = self.read_entry(slot);
            if entry.key != KEY_EMPTY && entry.key != KEY_TOMBSTONE {
                Some((entry.key, IndexEntry {
                    offset: entry.offset,
                    length: entry.length,
                    allocated: entry.allocated,
                }))
            } else {
                None
            }
        })
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Compute the initial probe slot for `key`.
    #[inline]
    fn probe_start(&self, key: u64) -> u64 {
        key % self.capacity
    }

    /// Advance to the next slot with wrap-around.
    #[inline]
    fn next_slot(&self, slot: u64) -> u64 {
        (slot + 1) % self.capacity
    }

    /// Byte offset of slot `i` in the mmap.
    #[inline]
    fn slot_offset(slot: u64) -> usize {
        HEADER_SIZE + slot as usize * ENTRY_SIZE
    }

    /// Read the `HashEntry` at `slot` by copying out of the mmap.
    fn read_entry(&self, slot: u64) -> HashEntry {
        let off = Self::slot_offset(slot);
        // SAFETY: `off` is within the mmap (checked by capacity).
        let key       = read_u64 (&self.mmap, off);
        let offset    = read_u64 (&self.mmap, off + 8);
        let length    = read_u32 (&self.mmap, off + 16);
        let allocated = read_u32 (&self.mmap, off + 20);
        HashEntry { key, offset, length, allocated }
    }

    /// Write `entry` to `slot` in the mmap.
    fn write_entry(&mut self, slot: u64, entry: HashEntry) {
        let off = Self::slot_offset(slot);
        write_u64(&mut self.mmap, off,      entry.key);
        write_u64(&mut self.mmap, off + 8,  entry.offset);
        write_u32(&mut self.mmap, off + 16, entry.length);
        write_u32(&mut self.mmap, off + 20, entry.allocated);
    }

    /// Total file size in bytes for a table with `capacity` slots.
    fn file_size_for(capacity: u64) -> usize {
        HEADER_SIZE + capacity as usize * ENTRY_SIZE
    }
}

// ---------------------------------------------------------------------------
// Little-endian read / write helpers
// ---------------------------------------------------------------------------

#[inline]
fn read_u64(mmap: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(mmap[off..off + 8].try_into().unwrap())
}

#[inline]
fn read_u32(mmap: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(mmap[off..off + 4].try_into().unwrap())
}

#[inline]
fn write_u64(mmap: &mut [u8], off: usize, val: u64) {
    mmap[off..off + 8].copy_from_slice(&val.to_le_bytes());
}

#[inline]
fn write_u32(mmap: &mut [u8], off: usize, val: u32) {
    mmap[off..off + 4].copy_from_slice(&val.to_le_bytes());
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    // Helper — create a throwaway HashIndex in a temp dir.
    fn make_index(capacity: u64) -> (HashIndex, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.hidx");
        let idx = HashIndex::new(&path, capacity).unwrap();
        (idx, dir)
    }

    fn entry(offset: u64, length: u32, allocated: u32) -> IndexEntry {
        IndexEntry { offset, length, allocated }
    }

    // ------------------------------------------------------------------
    // 1. Basic insert and lookup
    // ------------------------------------------------------------------
    #[test]
    fn test_hash_insert_and_lookup() {
        let (mut idx, _dir) = make_index(16);

        let e = entry(1024, 64, 64);
        idx.put(42, e).unwrap();

        let got = idx.get(42).unwrap();
        assert_eq!(got, e);

        // Key that was never inserted.
        assert!(idx.get(99).is_none());

        // Reserved sentinel keys.
        assert!(idx.get(0).is_none());
        assert!(idx.get(u64::MAX).is_none());
    }

    // ------------------------------------------------------------------
    // 2. Update an existing key
    // ------------------------------------------------------------------
    #[test]
    fn test_hash_update_existing() {
        let (mut idx, _dir) = make_index(16);

        idx.put(7, entry(100, 10, 16)).unwrap();
        assert_eq!(idx.count(), 1);

        // Overwrite — count must stay at 1.
        idx.put(7, entry(200, 20, 32)).unwrap();
        assert_eq!(idx.count(), 1);

        let got = idx.get(7).unwrap();
        assert_eq!(got.offset, 200);
        assert_eq!(got.length, 20);
        assert_eq!(got.allocated, 32);
    }

    // ------------------------------------------------------------------
    // 3. Collision handling
    // ------------------------------------------------------------------
    #[test]
    fn test_hash_collision_handling() {
        // capacity = 4, so keys 4, 8, 12 all hash to slot 0.
        let (mut idx, _dir) = make_index(4);

        idx.put(4,  entry(10, 1, 4)).unwrap();
        idx.put(8,  entry(20, 2, 4)).unwrap();
        idx.put(12, entry(30, 3, 4)).unwrap();

        assert_eq!(idx.get(4).unwrap().offset,  10);
        assert_eq!(idx.get(8).unwrap().offset,  20);
        assert_eq!(idx.get(12).unwrap().offset, 30);
    }

    // ------------------------------------------------------------------
    // 4. Remove / tombstone
    // ------------------------------------------------------------------
    #[test]
    fn test_hash_remove() {
        let (mut idx, _dir) = make_index(16);

        idx.put(55, entry(500, 50, 64)).unwrap();
        assert_eq!(idx.count(), 1);

        let removed = idx.remove(55);
        assert!(removed);
        assert_eq!(idx.count(), 0);

        // After removal the key should not be found.
        assert!(idx.get(55).is_none());

        // Removing again is a no-op.
        assert!(!idx.remove(55));
    }

    // ------------------------------------------------------------------
    // 4b. Insert after tombstone reuses the slot (lookup still works)
    // ------------------------------------------------------------------
    #[test]
    fn test_hash_insert_after_tombstone() {
        // capacity = 4; keys 4 and 8 both land on slot 0.
        let (mut idx, _dir) = make_index(8);

        idx.put(4,  entry(1, 1, 1)).unwrap();
        idx.put(8,  entry(2, 2, 2)).unwrap();  // probes to slot 1
        idx.remove(4);                          // slot 0 → tombstone
        idx.put(4,  entry(99, 9, 9)).unwrap(); // should reuse slot 0

        assert_eq!(idx.get(4).unwrap().offset, 99);
        assert_eq!(idx.get(8).unwrap().offset, 2);
    }

    // ------------------------------------------------------------------
    // 5. Load factor — insert up to ~70 % capacity
    // ------------------------------------------------------------------
    #[test]
    fn test_hash_load_factor() {
        // 100-slot table; 70 live entries ≈ 70 % load.
        // All puts must succeed; no infinite loops.
        let (mut idx, _dir) = make_index(100);

        for i in 1u64..=70 {
            idx.put(i, entry(i * 64, 64, 64)).unwrap();
        }

        assert_eq!(idx.count(), 70);

        // Every key must be retrievable.
        for i in 1u64..=70 {
            let got = idx.get(i).unwrap();
            assert_eq!(got.offset, i * 64, "key {} has wrong offset", i);
        }
    }

    // ------------------------------------------------------------------
    // 6. Persist across reopen
    // ------------------------------------------------------------------
    #[test]
    fn test_hash_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("persist.hidx");

        {
            let mut idx = HashIndex::new(&path, 32).unwrap();
            idx.put(1, entry(100, 10, 16)).unwrap();
            idx.put(2, entry(200, 20, 32)).unwrap();
            idx.put(3, entry(300, 30, 64)).unwrap();
            idx.flush().unwrap();
        }

        // Reopen and verify data survived.
        let idx = HashIndex::open(&path).unwrap();
        assert_eq!(idx.capacity(), 32);
        assert_eq!(idx.count(), 3);
        assert_eq!(idx.get(1).unwrap().offset, 100);
        assert_eq!(idx.get(2).unwrap().offset, 200);
        assert_eq!(idx.get(3).unwrap().offset, 300);
        assert!(idx.get(4).is_none());
    }

    // ------------------------------------------------------------------
    // 7. Iteration
    // ------------------------------------------------------------------
    #[test]
    fn test_hash_iter() {
        let (mut idx, _dir) = make_index(32);

        let pairs: Vec<(u64, IndexEntry)> = (1u64..=10)
            .map(|i| (i, entry(i * 100, i as u32, i as u32 * 2)))
            .collect();

        for &(k, v) in &pairs {
            idx.put(k, v).unwrap();
        }

        // Remove one to ensure tombstones are skipped.
        idx.remove(5);

        let mut collected: Vec<(u64, IndexEntry)> = idx.iter().collect();
        collected.sort_by_key(|&(k, _)| k);

        // Expect 9 entries (1–10 minus 5).
        assert_eq!(collected.len(), 9);
        for &(k, v) in &pairs {
            if k == 5 { continue; }
            let found = collected.iter().find(|&&(ck, _)| ck == k).unwrap();
            assert_eq!(found.1, v, "key {} has wrong value", k);
        }
    }

    // ------------------------------------------------------------------
    // 8. Reserved key errors
    // ------------------------------------------------------------------
    #[test]
    fn test_hash_reserved_keys() {
        let (mut idx, _dir) = make_index(16);

        assert!(matches!(idx.put(0, entry(1, 1, 1)), Err(SiloError::ReservedKey)));
        assert!(matches!(idx.put(u64::MAX, entry(1, 1, 1)), Err(SiloError::ReservedKey)));
    }

    // ------------------------------------------------------------------
    // 9. Invalid file detection
    // ------------------------------------------------------------------
    #[test]
    fn test_hash_invalid_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bad.hidx");

        // Write garbage.
        std::fs::write(&path, b"not a hash index file").unwrap();

        let result = HashIndex::open(&path);
        assert!(matches!(result, Err(SiloError::InvalidFile)));
    }

    // ------------------------------------------------------------------
    // 10. Probe correctness: key found after tombstone chain
    // ------------------------------------------------------------------
    #[test]
    fn test_hash_probe_through_tombstones() {
        // Table of 8; insert 3 keys all mapping to slot 0 (multiples of 8).
        let (mut idx, _dir) = make_index(8);

        idx.put(8,  entry(1, 1, 1)).unwrap(); // slot 0
        idx.put(16, entry(2, 2, 2)).unwrap(); // slot 1 (collision)
        idx.put(24, entry(3, 3, 3)).unwrap(); // slot 2 (collision)

        // Remove the first two — they become tombstones at slots 0 and 1.
        idx.remove(8);
        idx.remove(16);

        // Key 24 is still at slot 2; must be found by probing past tombstones.
        let got = idx.get(24).unwrap();
        assert_eq!(got.offset, 3);
    }

    // ------------------------------------------------------------------
    // 11. Throughput smoke test — 100K insert + 100K lookup
    //     Not a criterion benchmark (no per-iteration overhead), but gives
    //     a sanity-check number in `cargo test --release -- --nocapture`.
    // ------------------------------------------------------------------
    #[test]
    fn test_hash_throughput_100k() {
        use std::time::Instant;

        const N: u64 = 100_000;
        const CAP: u64 = N * 2; // ~50 % load factor

        let dir = tempdir().unwrap();
        let path = dir.path().join("throughput.hidx");
        let mut idx = HashIndex::new(&path, CAP).unwrap();

        // Insert phase.
        let t0 = Instant::now();
        for i in 1..=N {
            idx.put(i, entry(i * 64, 64, 64)).unwrap();
        }
        let insert_ms = t0.elapsed().as_secs_f64() * 1000.0;

        // Lookup phase (all hits).
        let t1 = Instant::now();
        let mut hits = 0u64;
        for i in 1..=N {
            if idx.get(i).is_some() { hits += 1; }
        }
        let lookup_ms = t1.elapsed().as_secs_f64() * 1000.0;

        assert_eq!(hits, N);
        assert_eq!(idx.count(), N);

        let insert_mops = N as f64 / insert_ms / 1000.0;
        let lookup_mops = N as f64 / lookup_ms / 1000.0;
        println!(
            "\n[throughput] insert {N}k: {insert_ms:.1}ms ({insert_mops:.1} Mop/s)  \
             lookup {N}k: {lookup_ms:.1}ms ({lookup_mops:.1} Mop/s)"
        );
    }
}
