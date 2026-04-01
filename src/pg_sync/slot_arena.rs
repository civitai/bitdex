//! Pre-allocated memory-mapped slot arena for bulk loading.
//!
//! Each document gets a fixed 512-byte slot in a memory-mapped file.
//! Multiple table streams write to different field offsets concurrently.
//! After all streams complete, a finalization pass reads populated slots,
//! serializes to msgpack, and writes to the docstore.
//!
//! Slot layout (512 bytes):
//! ```text
//! Offset  Size  Field
//! ------  ----  -----
//!   0       8   present_mask (AtomicU64 LE)
//!   8       8   image_id (u64 LE)
//!  16       1   nsfw_level (u8)
//!  17       8   user_id (u64 LE)
//!  25       1   image_type_enum (u8: 0=image, 1=video, 2=audio)
//!  26       8   sort_at (u64 LE, unix seconds)
//!  34       1   poi (u8 bool)
//!  35       1   minor (u8 bool)
//!  36      80   url ([u8; 80], first byte = length, rest = UTF-8)
//! 116      40   hash ([u8; 40], first byte = length)
//! 156       1   tag_count (u8, max 48 inline)
//! 157     192   tag_ids ([u32; 48] LE)
//! 349       1   mv_count (u8, max 8 inline)
//! 350      32   model_version_ids ([u32; 8] LE)
//! 382       1   tool_count (u8, max 8)
//! 383      32   tool_ids ([u32; 8] LE)
//! 415       1   technique_count (u8, max 4)
//! 416      16   technique_ids ([u32; 4] LE)
//! 432       1   has_meta (u8 bool)
//! 433       1   on_site (u8 bool)
//! 434       8   post_id (u64 LE)
//! 442       8   posted_to_id (u64 LE)
//! 450       1   availability_enum (u8: 0=Public, 1=Private, 2=Unsearchable)
//! 451       1   blocked_for_enum (u8: 0=none, 1=CSAM, 2=TOS, ...)
//! 452       4   reaction_count (u32 LE)
//! 456       4   comment_count (u32 LE)
//! 460       4   collected_count (u32 LE)
//! 464       1   mv_manual_count (u8, max 8)
//! 465      32   model_version_ids_manual ([u32; 8] LE)
//! 497       1   base_model_enum (u8)
//! 498       1   resource_poi (u8 bool, OR'd with image poi)
//! 499       8   published_at_unix (u64 LE, milliseconds)
//! 507       5   _padding
//! ---     ---
//! 512     total
//! ```

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use memmap2::MmapMut;
use roaring::RoaringBitmap;

use crate::shard_store_doc::ShardStoreBulkWriter as BulkWriter;
use crate::config::DataSchema;
use crate::error::Result;

// ---------------------------------------------------------------------------
// Constants — slot layout
// ---------------------------------------------------------------------------

pub const SLOT_SIZE: usize = 512;

// Field offsets
pub const OFF_PRESENT: usize = 0;
pub const OFF_IMAGE_ID: usize = 8;
pub const OFF_NSFW: usize = 16;
pub const OFF_USER_ID: usize = 17;
pub const OFF_IMAGE_TYPE: usize = 25;
pub const OFF_SORT_AT: usize = 26;
pub const OFF_POI: usize = 34;
pub const OFF_MINOR: usize = 35;
pub const OFF_URL: usize = 36;
pub const OFF_HASH: usize = 116;
pub const OFF_TAG_COUNT: usize = 156;
pub const OFF_TAG_IDS: usize = 157;
pub const OFF_MV_COUNT: usize = 349;
pub const OFF_MV_IDS: usize = 350;
pub const OFF_TOOL_COUNT: usize = 382;
pub const OFF_TOOL_IDS: usize = 383;
pub const OFF_TECH_COUNT: usize = 415;
pub const OFF_TECH_IDS: usize = 416;
pub const OFF_HAS_META: usize = 432;
pub const OFF_ON_SITE: usize = 433;
pub const OFF_POST_ID: usize = 434;
pub const OFF_POSTED_TO_ID: usize = 442;
pub const OFF_AVAILABILITY: usize = 450;
pub const OFF_BLOCKED_FOR: usize = 451;
pub const OFF_REACTION_COUNT: usize = 452;
pub const OFF_COMMENT_COUNT: usize = 456;
pub const OFF_COLLECTED_COUNT: usize = 460;
pub const OFF_MV_MANUAL_COUNT: usize = 464;
pub const OFF_MV_MANUAL_IDS: usize = 465;
pub const OFF_BASE_MODEL: usize = 497;
pub const OFF_RESOURCE_POI: usize = 498;
pub const OFF_PUBLISHED_AT: usize = 499;

// Inline capacity limits
pub const MAX_INLINE_TAGS: usize = 48;
pub const MAX_INLINE_MVS: usize = 8;
pub const MAX_INLINE_TOOLS: usize = 8;
pub const MAX_INLINE_TECHNIQUES: usize = 4;
pub const MAX_URL_LEN: usize = 79; // first byte = length
pub const MAX_HASH_LEN: usize = 39;

// Present mask bits
pub const MASK_IMAGE_ID: u64 = 1 << 0;
pub const MASK_NSFW: u64 = 1 << 1;
pub const MASK_USER_ID: u64 = 1 << 2;
pub const MASK_IMAGE_TYPE: u64 = 1 << 3;
pub const MASK_SORT_AT: u64 = 1 << 4;
pub const MASK_POI: u64 = 1 << 5;
pub const MASK_MINOR: u64 = 1 << 6;
pub const MASK_URL: u64 = 1 << 7;
pub const MASK_HASH: u64 = 1 << 8;
pub const MASK_TAGS: u64 = 1 << 9;
pub const MASK_MV: u64 = 1 << 10;
pub const MASK_TOOLS: u64 = 1 << 11;
pub const MASK_TECHNIQUES: u64 = 1 << 12;
pub const MASK_HAS_META: u64 = 1 << 13;
pub const MASK_ON_SITE: u64 = 1 << 14;
pub const MASK_POST_ID: u64 = 1 << 15;
pub const MASK_POSTED_TO_ID: u64 = 1 << 16;
pub const MASK_AVAILABILITY: u64 = 1 << 17;
pub const MASK_BLOCKED_FOR: u64 = 1 << 18;
pub const MASK_METRICS: u64 = 1 << 19;
pub const MASK_MV_MANUAL: u64 = 1 << 20;
pub const MASK_BASE_MODEL: u64 = 1 << 21;
pub const MASK_RESOURCE_POI: u64 = 1 << 22;
pub const MASK_PUBLISHED_AT: u64 = 1 << 23;

// ---------------------------------------------------------------------------
// Overflow for fields that exceed inline capacity
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowField {
    Tags,
    ModelVersionIds,
    ModelVersionIdsManual,
    Tools,
    Techniques,
}

#[derive(Debug)]
pub(crate) struct OverflowEntry {
    pub(crate) slot: u32,
    pub(crate) field: OverflowField,
    pub(crate) data: Vec<u32>,
}

// ---------------------------------------------------------------------------
// SlotData — assembled from slot + overflow for finalization
// ---------------------------------------------------------------------------

/// Complete data read from a slot, including overflow.
#[derive(Debug)]
pub struct SlotData {
    pub image_id: u64,
    pub nsfw_level: u8,
    pub user_id: u64,
    pub image_type: u8,
    pub sort_at: u64,
    pub poi: bool,
    pub minor: bool,
    pub url: Option<String>,
    pub hash: Option<String>,
    pub tag_ids: Vec<u32>,
    pub model_version_ids: Vec<u32>,
    pub model_version_ids_manual: Vec<u32>,
    pub tool_ids: Vec<u32>,
    pub technique_ids: Vec<u32>,
    pub has_meta: bool,
    pub on_site: bool,
    pub post_id: u64,
    pub posted_to_id: u64,
    pub availability: u8,
    pub blocked_for: u8,
    pub reaction_count: u32,
    pub comment_count: u32,
    pub collected_count: u32,
    pub base_model: u8,
    pub resource_poi: bool,
    pub published_at_unix: u64,
}

// ---------------------------------------------------------------------------
// SlotArena
// ---------------------------------------------------------------------------

/// Memory-mapped slot arena for bulk loading.
///
/// Pre-allocates a file with `max_slot * 512` bytes. Each table stream writes
/// to different field offsets concurrently using atomic present_mask updates.
/// After all streams finish, `finalize_to_docstore` reads populated slots and
/// writes compressed docstore shards.
pub struct SlotArena {
    mmap: MmapMut,
    overflow: Mutex<Vec<OverflowEntry>>,
    max_slot: u32,
    arena_path: PathBuf,
    _file: std::fs::File, // keep file handle alive for the mmap
}

impl SlotArena {
    /// Create a new slot arena backed by a memory-mapped file.
    ///
    /// The file is pre-allocated to `(max_slot + 1) * SLOT_SIZE` bytes and
    /// zero-initialized.
    pub fn new(max_slot: u32, path: &Path) -> Result<Self> {
        let file_size = (max_slot as u64 + 1) * SLOT_SIZE as u64;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .map_err(|e| crate::error::BitdexError::DocStore(
                format!("SlotArena: create file {}: {e}", path.display()),
            ))?;
        file.set_len(file_size).map_err(|e| {
            crate::error::BitdexError::DocStore(format!("SlotArena: set_len {file_size}: {e}"))
        })?;

        let mmap = unsafe {
            MmapMut::map_mut(&file).map_err(|e| {
                crate::error::BitdexError::DocStore(format!("SlotArena: mmap: {e}"))
            })?
        };

        eprintln!(
            "SlotArena: allocated {} MB for {} slots at {}",
            file_size / (1024 * 1024),
            max_slot + 1,
            path.display()
        );

        Ok(SlotArena {
            mmap,
            overflow: Mutex::new(Vec::new()),
            max_slot,
            arena_path: path.to_path_buf(),
            _file: file,
        })
    }

    /// Report memory usage: (arena_bytes, overflow_bytes).
    pub fn memory_usage(&self) -> (usize, usize) {
        let arena = (self.max_slot as usize + 1) * SLOT_SIZE;
        let overflow_entries = self.overflow.lock().unwrap();
        let overflow: usize = overflow_entries
            .iter()
            .map(|e| e.data.len() * 4 + std::mem::size_of::<OverflowEntry>())
            .sum();
        (arena, overflow)
    }

    // ---- Atomic present_mask helpers ----

    /// Atomically OR bits into the present_mask for a slot.
    /// Safe for concurrent access from multiple table streams.
    fn or_present_mask(&self, slot: u32, bits: u64) {
        let base = slot as usize * SLOT_SIZE + OFF_PRESENT;
        // SAFETY: We're treating the 8 bytes at the present_mask offset as an AtomicU64.
        // The mmap is aligned to page boundaries (4KB), and each slot is 512 bytes,
        // so the 8-byte present_mask is always naturally aligned.
        unsafe {
            let ptr = self.mmap.as_ptr().add(base) as *const AtomicU64;
            (*ptr).fetch_or(bits, Ordering::Release);
        }
    }

    // ---- Low-level write helpers ----

    #[inline]
    fn slot_base(&self, slot: u32) -> usize {
        slot as usize * SLOT_SIZE
    }

    #[inline]
    fn write_u64(&self, offset: usize, val: u64) {
        // SAFETY: We need &self (not &mut self) for concurrent access.
        // Different streams write to non-overlapping field offsets within each slot.
        unsafe {
            let ptr = self.mmap.as_ptr().add(offset) as *mut u8;
            std::ptr::copy_nonoverlapping(val.to_le_bytes().as_ptr(), ptr, 8);
        }
    }

    #[inline]
    fn write_u32(&self, offset: usize, val: u32) {
        unsafe {
            let ptr = self.mmap.as_ptr().add(offset) as *mut u8;
            std::ptr::copy_nonoverlapping(val.to_le_bytes().as_ptr(), ptr, 4);
        }
    }

    #[inline]
    fn write_u8(&self, offset: usize, val: u8) {
        unsafe {
            let ptr = self.mmap.as_ptr().add(offset) as *mut u8;
            *ptr = val;
        }
    }

    #[inline]
    fn write_inline_str(&self, offset: usize, max_len: usize, s: &[u8]) {
        let len = s.len().min(max_len);
        self.write_u8(offset, len as u8);
        if len > 0 {
            unsafe {
                let ptr = self.mmap.as_ptr().add(offset + 1) as *mut u8;
                std::ptr::copy_nonoverlapping(s.as_ptr(), ptr, len);
            }
        }
    }

    // ---- Read helpers (for finalization) ----

    #[inline]
    fn read_u64(&self, offset: usize) -> u64 {
        let bytes: [u8; 8] = self.mmap[offset..offset + 8].try_into().unwrap();
        u64::from_le_bytes(bytes)
    }

    #[inline]
    fn read_u32(&self, offset: usize) -> u32 {
        let bytes: [u8; 4] = self.mmap[offset..offset + 4].try_into().unwrap();
        u32::from_le_bytes(bytes)
    }

    #[inline]
    fn read_u8(&self, offset: usize) -> u8 {
        self.mmap[offset]
    }

    fn read_inline_str(&self, offset: usize) -> Option<String> {
        let len = self.read_u8(offset) as usize;
        if len == 0 {
            return None;
        }
        let bytes = &self.mmap[offset + 1..offset + 1 + len];
        Some(String::from_utf8_lossy(bytes).into_owned())
    }

    // ---- Public write methods (called by table streams) ----

    /// Write Image+Post scalar fields to a slot.
    ///
    /// Called by the image stream. Sets all scalar fields and their present bits.
    pub fn write_scalars(
        &self,
        slot: u32,
        image_id: u64,
        nsfw_level: u8,
        user_id: u64,
        image_type: u8,
        sort_at: u64,
        poi: bool,
        minor: bool,
        url: Option<&[u8]>,
        hash: Option<&[u8]>,
        has_meta: bool,
        on_site: bool,
        post_id: u64,
        posted_to_id: u64,
        availability: u8,
        blocked_for: u8,
        published_at_unix: u64,
    ) {
        let base = self.slot_base(slot);

        self.write_u64(base + OFF_IMAGE_ID, image_id);
        self.write_u8(base + OFF_NSFW, nsfw_level);
        self.write_u64(base + OFF_USER_ID, user_id);
        self.write_u8(base + OFF_IMAGE_TYPE, image_type);
        self.write_u64(base + OFF_SORT_AT, sort_at);
        self.write_u8(base + OFF_POI, poi as u8);
        self.write_u8(base + OFF_MINOR, minor as u8);

        let mut mask = MASK_IMAGE_ID | MASK_NSFW | MASK_USER_ID | MASK_IMAGE_TYPE
            | MASK_SORT_AT | MASK_POI | MASK_MINOR | MASK_POST_ID | MASK_POSTED_TO_ID
            | MASK_AVAILABILITY | MASK_PUBLISHED_AT;

        if let Some(url_bytes) = url {
            self.write_inline_str(base + OFF_URL, MAX_URL_LEN, url_bytes);
            mask |= MASK_URL;
        }
        if let Some(hash_bytes) = hash {
            self.write_inline_str(base + OFF_HASH, MAX_HASH_LEN, hash_bytes);
            mask |= MASK_HASH;
        }

        self.write_u8(base + OFF_HAS_META, has_meta as u8);
        self.write_u8(base + OFF_ON_SITE, on_site as u8);
        self.write_u64(base + OFF_POST_ID, post_id);
        self.write_u64(base + OFF_POSTED_TO_ID, posted_to_id);
        self.write_u8(base + OFF_AVAILABILITY, availability);
        self.write_u8(base + OFF_BLOCKED_FOR, blocked_for);
        self.write_u64(base + OFF_PUBLISHED_AT, published_at_unix);

        if has_meta { mask |= MASK_HAS_META; }
        if on_site { mask |= MASK_ON_SITE; }
        if blocked_for > 0 { mask |= MASK_BLOCKED_FOR; }

        // Metrics default to 0 — set mask so finalization knows they're present
        self.write_u32(base + OFF_REACTION_COUNT, 0);
        self.write_u32(base + OFF_COMMENT_COUNT, 0);
        self.write_u32(base + OFF_COLLECTED_COUNT, 0);
        mask |= MASK_METRICS;

        self.or_present_mask(slot, mask);
    }

    /// Append tag IDs to a slot. Inline up to 48, overflow the rest.
    ///
    /// Called by the tag stream. Tags arrive ordered by tagId, so the same
    /// slot may be written to multiple times as different tagIds are processed.
    /// Each call appends to the existing tag list in the slot.
    pub fn write_tags(&self, slot: u32, tag_ids: &[u32]) {
        if tag_ids.is_empty() {
            return;
        }
        let base = self.slot_base(slot);

        // Read current count atomically-enough (single byte, no torn reads)
        let current_count = self.read_u8(base + OFF_TAG_COUNT) as usize;
        let remaining_inline = MAX_INLINE_TAGS.saturating_sub(current_count);

        // Write as many as fit inline
        let inline_count = tag_ids.len().min(remaining_inline);
        for (i, &tag_id) in tag_ids[..inline_count].iter().enumerate() {
            self.write_u32(base + OFF_TAG_IDS + (current_count + i) * 4, tag_id);
        }
        self.write_u8(base + OFF_TAG_COUNT, (current_count + inline_count) as u8);

        // Overflow the rest
        if inline_count < tag_ids.len() {
            let overflow_data: Vec<u32> = tag_ids[inline_count..].to_vec();
            self.overflow.lock().unwrap().push(OverflowEntry {
                slot,
                field: OverflowField::Tags,
                data: overflow_data,
            });
        }

        self.or_present_mask(slot, MASK_TAGS);
    }

    /// Write model version IDs to a slot. Inline up to 8, overflow the rest.
    pub fn write_model_version_ids(&self, slot: u32, mv_ids: &[u32]) {
        if mv_ids.is_empty() {
            return;
        }
        let base = self.slot_base(slot);
        let current = self.read_u8(base + OFF_MV_COUNT) as usize;
        let remaining = MAX_INLINE_MVS.saturating_sub(current);
        let inline_n = mv_ids.len().min(remaining);

        for (i, &id) in mv_ids[..inline_n].iter().enumerate() {
            self.write_u32(base + OFF_MV_IDS + (current + i) * 4, id);
        }
        self.write_u8(base + OFF_MV_COUNT, (current + inline_n) as u8);

        if inline_n < mv_ids.len() {
            self.overflow.lock().unwrap().push(OverflowEntry {
                slot,
                field: OverflowField::ModelVersionIds,
                data: mv_ids[inline_n..].to_vec(),
            });
        }

        self.or_present_mask(slot, MASK_MV);
    }

    /// Write manual model version IDs to a slot.
    pub fn write_model_version_ids_manual(&self, slot: u32, mv_ids: &[u32]) {
        if mv_ids.is_empty() {
            return;
        }
        let base = self.slot_base(slot);
        let current = self.read_u8(base + OFF_MV_MANUAL_COUNT) as usize;
        let remaining = MAX_INLINE_MVS.saturating_sub(current);
        let inline_n = mv_ids.len().min(remaining);

        for (i, &id) in mv_ids[..inline_n].iter().enumerate() {
            self.write_u32(base + OFF_MV_MANUAL_IDS + (current + i) * 4, id);
        }
        self.write_u8(base + OFF_MV_MANUAL_COUNT, (current + inline_n) as u8);

        if inline_n < mv_ids.len() {
            self.overflow.lock().unwrap().push(OverflowEntry {
                slot,
                field: OverflowField::ModelVersionIdsManual,
                data: mv_ids[inline_n..].to_vec(),
            });
        }

        self.or_present_mask(slot, MASK_MV_MANUAL);
    }

    /// Write tool IDs to a slot.
    pub fn write_tools(&self, slot: u32, tool_ids: &[u32]) {
        if tool_ids.is_empty() {
            return;
        }
        let base = self.slot_base(slot);
        let current = self.read_u8(base + OFF_TOOL_COUNT) as usize;
        let remaining = MAX_INLINE_TOOLS.saturating_sub(current);
        let inline_n = tool_ids.len().min(remaining);

        for (i, &id) in tool_ids[..inline_n].iter().enumerate() {
            self.write_u32(base + OFF_TOOL_IDS + (current + i) * 4, id);
        }
        self.write_u8(base + OFF_TOOL_COUNT, (current + inline_n) as u8);

        if inline_n < tool_ids.len() {
            self.overflow.lock().unwrap().push(OverflowEntry {
                slot,
                field: OverflowField::Tools,
                data: tool_ids[inline_n..].to_vec(),
            });
        }

        self.or_present_mask(slot, MASK_TOOLS);
    }

    /// Write technique IDs to a slot.
    pub fn write_techniques(&self, slot: u32, technique_ids: &[u32]) {
        if technique_ids.is_empty() {
            return;
        }
        let base = self.slot_base(slot);
        let current = self.read_u8(base + OFF_TECH_COUNT) as usize;
        let remaining = MAX_INLINE_TECHNIQUES.saturating_sub(current);
        let inline_n = technique_ids.len().min(remaining);

        for (i, &id) in technique_ids[..inline_n].iter().enumerate() {
            self.write_u32(base + OFF_TECH_IDS + (current + i) * 4, id);
        }
        self.write_u8(base + OFF_TECH_COUNT, (current + inline_n) as u8);

        if inline_n < technique_ids.len() {
            self.overflow.lock().unwrap().push(OverflowEntry {
                slot,
                field: OverflowField::Techniques,
                data: technique_ids[inline_n..].to_vec(),
            });
        }

        self.or_present_mask(slot, MASK_TECHNIQUES);
    }

    /// Write base model enum to a slot.
    pub fn write_base_model(&self, slot: u32, base_model: u8) {
        let base = self.slot_base(slot);
        self.write_u8(base + OFF_BASE_MODEL, base_model);
        self.or_present_mask(slot, MASK_BASE_MODEL);
    }

    /// OR resource_poi into the slot's poi field.
    ///
    /// Image stream sets poi from Image.poi.
    /// Resource stream OR's in resource_poi if true (idempotent).
    pub fn set_resource_poi(&self, slot: u32) {
        let base = self.slot_base(slot);
        self.write_u8(base + OFF_RESOURCE_POI, 1);
        self.or_present_mask(slot, MASK_RESOURCE_POI);
    }

    // ---- Read methods (for finalization) ----

    /// Read a complete slot, merging any overflow data.
    ///
    /// Returns `None` if the slot has no present bits set (never written).
    pub(crate) fn read_slot(&self, slot: u32, overflow_map: &std::collections::HashMap<u32, Vec<&OverflowEntry>>) -> Option<SlotData> {
        let base = self.slot_base(slot);
        let mask = self.read_u64(base + OFF_PRESENT);

        if mask == 0 {
            return None;
        }

        // Read inline tag IDs
        let tag_count = self.read_u8(base + OFF_TAG_COUNT) as usize;
        let mut tag_ids: Vec<u32> = (0..tag_count)
            .map(|i| self.read_u32(base + OFF_TAG_IDS + i * 4))
            .collect();

        // Read inline MV IDs
        let mv_count = self.read_u8(base + OFF_MV_COUNT) as usize;
        let mut model_version_ids: Vec<u32> = (0..mv_count)
            .map(|i| self.read_u32(base + OFF_MV_IDS + i * 4))
            .collect();

        // Read inline manual MV IDs
        let mv_manual_count = self.read_u8(base + OFF_MV_MANUAL_COUNT) as usize;
        let mut model_version_ids_manual: Vec<u32> = (0..mv_manual_count)
            .map(|i| self.read_u32(base + OFF_MV_MANUAL_IDS + i * 4))
            .collect();

        // Read inline tool IDs
        let tool_count = self.read_u8(base + OFF_TOOL_COUNT) as usize;
        let mut tool_ids: Vec<u32> = (0..tool_count)
            .map(|i| self.read_u32(base + OFF_TOOL_IDS + i * 4))
            .collect();

        // Read inline technique IDs
        let tech_count = self.read_u8(base + OFF_TECH_COUNT) as usize;
        let mut technique_ids: Vec<u32> = (0..tech_count)
            .map(|i| self.read_u32(base + OFF_TECH_IDS + i * 4))
            .collect();

        // Merge overflow
        if let Some(entries) = overflow_map.get(&slot) {
            for entry in entries {
                match entry.field {
                    OverflowField::Tags => tag_ids.extend_from_slice(&entry.data),
                    OverflowField::ModelVersionIds => model_version_ids.extend_from_slice(&entry.data),
                    OverflowField::ModelVersionIdsManual => model_version_ids_manual.extend_from_slice(&entry.data),
                    OverflowField::Tools => tool_ids.extend_from_slice(&entry.data),
                    OverflowField::Techniques => technique_ids.extend_from_slice(&entry.data),
                }
            }
        }

        // Resolve poi: image poi OR resource_poi
        let image_poi = self.read_u8(base + OFF_POI) != 0;
        let resource_poi = self.read_u8(base + OFF_RESOURCE_POI) != 0;

        Some(SlotData {
            image_id: self.read_u64(base + OFF_IMAGE_ID),
            nsfw_level: self.read_u8(base + OFF_NSFW),
            user_id: self.read_u64(base + OFF_USER_ID),
            image_type: self.read_u8(base + OFF_IMAGE_TYPE),
            sort_at: self.read_u64(base + OFF_SORT_AT),
            poi: image_poi || resource_poi,
            minor: self.read_u8(base + OFF_MINOR) != 0,
            url: self.read_inline_str(base + OFF_URL),
            hash: self.read_inline_str(base + OFF_HASH),
            tag_ids,
            model_version_ids,
            model_version_ids_manual,
            tool_ids,
            technique_ids,
            has_meta: self.read_u8(base + OFF_HAS_META) != 0,
            on_site: self.read_u8(base + OFF_ON_SITE) != 0,
            post_id: self.read_u64(base + OFF_POST_ID),
            posted_to_id: self.read_u64(base + OFF_POSTED_TO_ID),
            availability: self.read_u8(base + OFF_AVAILABILITY),
            blocked_for: self.read_u8(base + OFF_BLOCKED_FOR),
            reaction_count: self.read_u32(base + OFF_REACTION_COUNT),
            comment_count: self.read_u32(base + OFF_COMMENT_COUNT),
            collected_count: self.read_u32(base + OFF_COLLECTED_COUNT),
            base_model: self.read_u8(base + OFF_BASE_MODEL),
            resource_poi,
            published_at_unix: self.read_u64(base + OFF_PUBLISHED_AT),
        })
    }

    /// Finalize all populated slots to the docstore.
    ///
    /// Iterates alive bitmap, reads each slot (with overflow merge), converts to
    /// JSON doc, encodes via BulkWriter, and writes to docstore shards.
    ///
    /// Uses rayon for parallel encoding + compression.
    pub fn finalize_to_docstore(
        &self,
        bulk_writer: &BulkWriter,
        schema: &DataSchema,
        alive: &RoaringBitmap,
    ) -> Result<(u64, u64)> {
        use rayon::prelude::*;

        let total = alive.len() as u64;
        eprintln!("SlotArena: finalizing {} docs to docstore...", total);

        // Build overflow lookup
        let overflow_entries = self.overflow.lock().unwrap();
        let mut overflow_map: std::collections::HashMap<u32, Vec<&OverflowEntry>> =
            std::collections::HashMap::new();
        for entry in overflow_entries.iter() {
            overflow_map.entry(entry.slot).or_default().push(entry);
        }
        let overflow_count = overflow_map.len();
        if overflow_count > 0 {
            eprintln!(
                "SlotArena: {} slots have overflow data ({:.1}%)",
                overflow_count,
                overflow_count as f64 / total as f64 * 100.0
            );
        }

        // Collect all slot IDs from alive bitmap
        let slots: Vec<u32> = alive.iter().collect();

        // Process in chunks matching docstore shard size (512 docs)
        // Parallel encode: read slots, convert to JSON, encode to msgpack
        let encoded: Vec<(u32, Vec<u8>)> = slots
            .par_iter()
            .filter_map(|&slot| {
                let slot_data = self.read_slot(slot, &overflow_map)?;
                let json = slot_data_to_json(&slot_data);
                let bytes = bulk_writer.encode_json(&json, schema);
                Some((slot, bytes))
            })
            .collect();

        let docs_written = encoded.len() as u64;
        let bytes_written: u64 = encoded.iter().map(|(_, b)| b.len() as u64).sum();

        // Write to docstore via BulkWriter (handles sharding + compression)
        bulk_writer.write_batch_encoded(encoded);

        eprintln!(
            "SlotArena: finalized {} docs, {} MB encoded",
            docs_written,
            bytes_written / (1024 * 1024)
        );

        Ok((docs_written, bytes_written))
    }

    /// Clean up the arena file.
    pub fn cleanup(self) -> std::io::Result<()> {
        drop(self.mmap);
        drop(self._file);
        std::fs::remove_file(&self.arena_path)
    }
}

// ---------------------------------------------------------------------------
// Enum encoding helpers
// ---------------------------------------------------------------------------

/// Encode image type string to enum byte.
pub fn encode_image_type(s: Option<&str>) -> u8 {
    match s {
        Some("video") => 1,
        Some("audio") => 2,
        _ => 0, // "image" or unknown
    }
}

/// Decode image type enum byte to string.
pub fn decode_image_type(v: u8) -> &'static str {
    match v {
        1 => "video",
        2 => "audio",
        _ => "image",
    }
}

/// Encode availability string to enum byte.
pub fn encode_availability(s: Option<&str>) -> u8 {
    match s {
        Some("Private") => 1,
        Some("Unsearchable") => 2,
        _ => 0, // "Public" or unknown
    }
}

/// Decode availability enum byte to string.
pub fn decode_availability(v: u8) -> &'static str {
    match v {
        1 => "Private",
        2 => "Unsearchable",
        _ => "Public",
    }
}

/// Encode blocked_for string to enum byte.
pub fn encode_blocked_for(s: Option<&str>) -> u8 {
    match s {
        None => 0,
        Some("") => 0,
        Some(_) => 1, // any non-empty value
    }
}

// Well-known base model strings → enum byte (for filter bitmaps).
// Not exhaustive — new base models get 0 (unknown).
pub fn encode_base_model(s: Option<&str>) -> u8 {
    match s {
        None | Some("") => 0,
        Some("SD 1.5") => 1,
        Some("SD 2.1") => 2,
        Some("SDXL 1.0") => 3,
        Some("Pony") => 4,
        Some("Flux.1 D") => 5,
        Some("Flux.1 S") => 6,
        Some("SD 3.5 Large") => 7,
        Some("Illustrious") => 8,
        Some("Hunyuan 1") => 9,
        Some("SD 3.5 Medium") => 10,
        Some(_) => 255, // known but unmapped
    }
}

pub fn decode_base_model(v: u8) -> &'static str {
    match v {
        0 => "",
        1 => "SD 1.5",
        2 => "SD 2.1",
        3 => "SDXL 1.0",
        4 => "Pony",
        5 => "Flux.1 D",
        6 => "Flux.1 S",
        7 => "SD 3.5 Large",
        8 => "Illustrious",
        9 => "Hunyuan 1",
        10 => "SD 3.5 Medium",
        _ => "Other",
    }
}

// ---------------------------------------------------------------------------
// SlotData → JSON for docstore encoding
// ---------------------------------------------------------------------------

/// Convert SlotData to a serde_json::Value matching the Bitdex data schema.
/// Used during finalization to produce docstore-compatible documents.
fn slot_data_to_json(slot: &SlotData) -> serde_json::Value {
    let mut doc = serde_json::json!({
        "id": slot.image_id as i64,
        "nsfwLevel": slot.nsfw_level as i64,
        "userId": slot.user_id as i64,
        "postId": slot.post_id as i64,
        "postedToId": slot.posted_to_id as i64,
        "type": decode_image_type(slot.image_type),
        "baseModel": decode_base_model(slot.base_model),
        "availability": decode_availability(slot.availability),
        "tagIds": slot.tag_ids.iter().map(|&t| t as i64).collect::<Vec<i64>>(),
        "modelVersionIds": slot.model_version_ids.iter().map(|&t| t as i64).collect::<Vec<i64>>(),
        "modelVersionIdsManual": slot.model_version_ids_manual.iter().map(|&t| t as i64).collect::<Vec<i64>>(),
        "toolIds": slot.tool_ids.iter().map(|&t| t as i64).collect::<Vec<i64>>(),
        "techniqueIds": slot.technique_ids.iter().map(|&t| t as i64).collect::<Vec<i64>>(),
        "reactionCount": slot.reaction_count as i64,
        "commentCount": slot.comment_count as i64,
        "collectedCount": slot.collected_count as i64,
        "sortAt": slot.sort_at as i64,
        "publishedAt": (slot.published_at_unix / 1000) as i64,
    });

    if let Some(obj) = doc.as_object_mut() {
        // Exists-boolean: isPublished = publishedAt is non-zero (matches outbox row_assembler)
        if slot.published_at_unix > 0 {
            obj.insert("isPublished".into(), serde_json::json!(true));
        }
        if slot.has_meta {
            obj.insert("hasMeta".into(), serde_json::json!(true));
        }
        if slot.on_site {
            obj.insert("onSite".into(), serde_json::json!(true));
        }
        if slot.poi {
            obj.insert("poi".into(), serde_json::json!(true));
        }
        if slot.minor {
            obj.insert("minor".into(), serde_json::json!(true));
        }
        if let Some(ref url) = slot.url {
            obj.insert("url".into(), serde_json::json!(url));
        }
        if let Some(ref hash) = slot.hash {
            obj.insert("hash".into(), serde_json::json!(hash));
        }
        if slot.blocked_for > 0 {
            obj.insert("blockedFor".into(), serde_json::json!("blocked"));
        }
    }

    doc
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_write_read_scalars() {
        let dir = tempdir().unwrap();
        let arena = SlotArena::new(100, &dir.path().join("slots.bin")).unwrap();

        arena.write_scalars(
            42, 12345, 16, 999, 0, 1700000000, true, false,
            Some(b"https://example.com/img.jpg"),
            Some(b"abc123hash"),
            true, false,
            100, 200, 0, 0, 1700000000000,
        );

        let overflow = std::collections::HashMap::new();
        let slot = arena.read_slot(42, &overflow).unwrap();

        assert_eq!(slot.image_id, 12345);
        assert_eq!(slot.nsfw_level, 16);
        assert_eq!(slot.user_id, 999);
        assert_eq!(slot.image_type, 0);
        assert_eq!(slot.sort_at, 1700000000);
        assert!(slot.poi);
        assert!(!slot.minor);
        assert_eq!(slot.url.as_deref(), Some("https://example.com/img.jpg"));
        assert_eq!(slot.hash.as_deref(), Some("abc123hash"));
        assert!(slot.has_meta);
        assert!(!slot.on_site);
        assert_eq!(slot.post_id, 100);
        assert_eq!(slot.posted_to_id, 200);
        assert_eq!(slot.availability, 0);
        assert_eq!(slot.published_at_unix, 1700000000000);
    }

    #[test]
    fn test_write_tags_inline() {
        let dir = tempdir().unwrap();
        let arena = SlotArena::new(10, &dir.path().join("slots.bin")).unwrap();

        let tags: Vec<u32> = (100..110).collect();
        arena.write_tags(5, &tags);

        let overflow = std::collections::HashMap::new();
        let slot = arena.read_slot(5, &overflow).unwrap();
        assert_eq!(slot.tag_ids, tags);
    }

    #[test]
    fn test_write_tags_overflow() {
        let dir = tempdir().unwrap();
        let arena = SlotArena::new(10, &dir.path().join("slots.bin")).unwrap();

        // Write 60 tags — 48 inline + 12 overflow
        let tags: Vec<u32> = (100..160).collect();
        arena.write_tags(3, &tags);

        // Build overflow map
        let overflow_entries = arena.overflow.lock().unwrap();
        let mut overflow_map: std::collections::HashMap<u32, Vec<&OverflowEntry>> =
            std::collections::HashMap::new();
        for entry in overflow_entries.iter() {
            overflow_map.entry(entry.slot).or_default().push(entry);
        }

        let slot = arena.read_slot(3, &overflow_map).unwrap();
        assert_eq!(slot.tag_ids.len(), 60);
        assert_eq!(slot.tag_ids, tags);
    }

    #[test]
    fn test_write_tags_incremental() {
        let dir = tempdir().unwrap();
        let arena = SlotArena::new(10, &dir.path().join("slots.bin")).unwrap();

        // Write tags incrementally (simulating tag stream)
        arena.write_tags(2, &[100, 101, 102]);
        arena.write_tags(2, &[200, 201]);

        let overflow = std::collections::HashMap::new();
        let slot = arena.read_slot(2, &overflow).unwrap();
        assert_eq!(slot.tag_ids, vec![100, 101, 102, 200, 201]);
    }

    #[test]
    fn test_concurrent_field_writes() {
        let dir = tempdir().unwrap();
        let arena = SlotArena::new(1000, &dir.path().join("slots.bin")).unwrap();

        std::thread::scope(|s| {
            // Thread 1: write scalars
            s.spawn(|| {
                for slot in 0..1000u32 {
                    arena.write_scalars(
                        slot, slot as u64, 16, slot as u64 * 7, 0,
                        1700000000, false, false,
                        Some(b"url"), Some(b"hash"),
                        false, false, 100, 0, 0, 0, 0,
                    );
                }
            });

            // Thread 2: write tags
            s.spawn(|| {
                for slot in 0..1000u32 {
                    arena.write_tags(slot, &[slot + 1000, slot + 2000]);
                }
            });

            // Thread 3: write tools
            s.spawn(|| {
                for slot in 0..1000u32 {
                    arena.write_tools(slot, &[slot + 5000]);
                }
            });
        });

        // Verify all fields present
        let overflow = std::collections::HashMap::new();
        for slot in 0..1000u32 {
            let data = arena.read_slot(slot, &overflow).unwrap();
            assert_eq!(data.image_id, slot as u64);
            assert_eq!(data.tag_ids.len(), 2);
            assert_eq!(data.tool_ids.len(), 1);
        }
    }

    #[test]
    fn test_present_mask_atomic() {
        let dir = tempdir().unwrap();
        let arena = SlotArena::new(10, &dir.path().join("slots.bin")).unwrap();

        // Multiple threads OR-ing different bits into the same slot
        std::thread::scope(|s| {
            for bit in 0..20u64 {
                let arena_ref = &arena;
                s.spawn(move || {
                    arena_ref.or_present_mask(5, 1 << bit);
                });
            }
        });

        let base = 5 * SLOT_SIZE + OFF_PRESENT;
        let mask = arena.read_u64(base);
        // All 20 bits should be set
        for bit in 0..20u64 {
            assert!(mask & (1 << bit) != 0, "bit {bit} not set");
        }
    }

    #[test]
    fn test_empty_slot_returns_none() {
        let dir = tempdir().unwrap();
        let arena = SlotArena::new(10, &dir.path().join("slots.bin")).unwrap();

        let overflow = std::collections::HashMap::new();
        assert!(arena.read_slot(7, &overflow).is_none());
    }

    #[test]
    fn test_memory_usage_reporting() {
        let dir = tempdir().unwrap();
        let arena = SlotArena::new(100, &dir.path().join("slots.bin")).unwrap();

        let (arena_bytes, overflow_bytes) = arena.memory_usage();
        assert_eq!(arena_bytes, 101 * SLOT_SIZE);
        assert_eq!(overflow_bytes, 0);

        // Add some overflow
        arena.write_tags(1, &vec![0u32; 60]); // 48 inline + 12 overflow
        let (_, overflow_bytes) = arena.memory_usage();
        assert!(overflow_bytes > 0);
    }
}
