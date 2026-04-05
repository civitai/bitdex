//! Deterministic u64 key encoding for BitmapSilo.
//!
//! Replaces the string-based manifest (`name_to_key` HashMap) with pure arithmetic.
//! Keys are computed from (field_id, value/bit_layer/bucket_id) and go directly
//! to DataSilo's mmap HashIndex. No heap allocation, no locks, no manifest file.
//!
//! ## Namespace layout (top 2 bits)
//!
//! | Prefix | Binary    | Use                          |
//! |--------|-----------|------------------------------|
//! | 0b00   | 00xx xxxx | Filter keys + system keys    |
//! | 0b01   | 01xx xxxx | Reserved                     |
//! | 0b10   | 10xx xxxx | Sort keys                    |
//! | 0b11   | 11xx xxxx | Bucket keys                  |
//!
//! ## System keys (literal small values)
//!
//! - `1` = alive bitmap
//! - `2` = metadata (slot_counter, cursors, etc.)
//!
//! These are safe because real filter keys have `field_id >= 1` in the upper bits,
//! so `(1u64 << 48) | anything` is always >= 2^48, far above 1 or 2.

/// Alive bitmap key — literal value 1.
pub const KEY_ALIVE: u64 = 1;

/// Metadata key — literal value 2.
pub const KEY_META: u64 = 2;

/// Sort namespace prefix (high bit set).
const SORT_PREFIX: u64 = 0x8000_0000_0000_0000;

/// Bucket namespace prefix (high 2 bits set).
const BUCKET_PREFIX: u64 = 0xC000_0000_0000_0000;

/// Maximum field_id that fits without colliding with namespace prefixes.
/// Filter keys use the top 2 bits as namespace (00), so field_id must fit in 14 bits.
/// With ~40 fields in practice, this is never a concern.
pub const MAX_FIELD_ID: u16 = 0x3FFF; // 16383

/// Encode a filter bitmap key: `(field_id << 48) | (value & 0xFFFF_FFFF_FFFF)`.
///
/// 14 bits for field_id (max 16383), 48 bits for value.
/// Top 2 bits are always 0b00 (filter namespace) since field_id <= MAX_FIELD_ID.
#[inline]
pub fn encode_filter_key(field_id: u16, value: u64) -> u64 {
    debug_assert!(field_id <= MAX_FIELD_ID, "field_id {field_id} exceeds MAX_FIELD_ID {MAX_FIELD_ID}");
    ((field_id as u64) << 48) | (value & 0x0000_FFFF_FFFF_FFFF)
}

/// Encode a sort bit-layer key: `0x8000... | (field_id << 32) | bit_layer`.
///
/// High bit = sort namespace. 14 bits field_id, 32 bits bit_layer index.
#[inline]
pub fn encode_sort_key(field_id: u16, bit_layer: u32) -> u64 {
    debug_assert!(field_id <= MAX_FIELD_ID, "field_id {field_id} exceeds MAX_FIELD_ID {MAX_FIELD_ID}");
    SORT_PREFIX | ((field_id as u64) << 32) | (bit_layer as u64)
}

/// Encode a time bucket key: `0xC000... | (field_id << 16) | bucket_id`.
///
/// High 2 bits = bucket namespace. 14 bits field_id, 16 bits bucket_id.
#[inline]
pub fn encode_bucket_key(field_id: u16, bucket_id: u16) -> u64 {
    debug_assert!(field_id <= MAX_FIELD_ID, "field_id {field_id} exceeds MAX_FIELD_ID {MAX_FIELD_ID}");
    BUCKET_PREFIX | ((field_id as u64) << 16) | (bucket_id as u64)
}

/// Decoded key with namespace and components.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodedKey {
    /// System key (alive=1, metadata=2).
    System(u64),
    /// Filter bitmap: (field_id, value).
    Filter { field_id: u16, value: u64 },
    /// Sort bit-layer: (field_id, bit_layer).
    Sort { field_id: u16, bit_layer: u32 },
    /// Time bucket: (field_id, bucket_id).
    Bucket { field_id: u16, bucket_id: u16 },
}

/// Decode a u64 silo key back to its components.
pub fn decode_key(key: u64) -> DecodedKey {
    if key <= 2 {
        return DecodedKey::System(key);
    }
    let top2 = key >> 62;
    match top2 {
        0b00 | 0b01 => {
            // Filter namespace (0b00). 0b01 is reserved but decode as filter for safety.
            let field_id = (key >> 48) as u16;
            let value = key & 0x0000_FFFF_FFFF_FFFF;
            DecodedKey::Filter { field_id, value }
        }
        0b10 => {
            // Sort namespace
            let field_id = ((key >> 32) & 0xFFFF) as u16;
            let bit_layer = (key & 0xFFFF_FFFF) as u32;
            DecodedKey::Sort { field_id, bit_layer }
        }
        0b11 => {
            // Bucket namespace
            let field_id = ((key >> 16) & 0xFFFF) as u16;
            let bucket_id = (key & 0xFFFF) as u16;
            DecodedKey::Bucket { field_id, bucket_id }
        }
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_keys_are_small() {
        assert_eq!(KEY_ALIVE, 1);
        assert_eq!(KEY_META, 2);
    }

    #[test]
    fn filter_key_roundtrip() {
        for field_id in [1u16, 5, 100, MAX_FIELD_ID] {
            for value in [0u64, 1, 42, 0x0000_FFFF_FFFF_FFFF] {
                let key = encode_filter_key(field_id, value);
                assert!(key > 2, "filter key must not collide with system keys");
                match decode_key(key) {
                    DecodedKey::Filter { field_id: fid, value: v } => {
                        assert_eq!(fid, field_id);
                        assert_eq!(v, value);
                    }
                    other => panic!("expected Filter, got {:?}", other),
                }
            }
        }
    }

    #[test]
    fn sort_key_roundtrip() {
        for field_id in [1u16, 5, 100] {
            for bit_layer in [0u32, 1, 31, 63] {
                let key = encode_sort_key(field_id, bit_layer);
                match decode_key(key) {
                    DecodedKey::Sort { field_id: fid, bit_layer: bl } => {
                        assert_eq!(fid, field_id);
                        assert_eq!(bl, bit_layer);
                    }
                    other => panic!("expected Sort, got {:?}", other),
                }
            }
        }
    }

    #[test]
    fn bucket_key_roundtrip() {
        for field_id in [1u16, 5, 100] {
            for bucket_id in [0u16, 1, 3, 0xFFFF] {
                let key = encode_bucket_key(field_id, bucket_id);
                match decode_key(key) {
                    DecodedKey::Bucket { field_id: fid, bucket_id: bid } => {
                        assert_eq!(fid, field_id);
                        assert_eq!(bid, bucket_id);
                    }
                    other => panic!("expected Bucket, got {:?}", other),
                }
            }
        }
    }

    #[test]
    fn no_namespace_collisions() {
        // Filter key with field_id=1, value=0 must differ from sort/bucket keys
        let filter = encode_filter_key(1, 0);
        let sort = encode_sort_key(1, 0);
        let bucket = encode_bucket_key(1, 0);
        assert_ne!(filter, sort);
        assert_ne!(filter, bucket);
        assert_ne!(sort, bucket);
        assert_ne!(filter, KEY_ALIVE);
        assert_ne!(filter, KEY_META);
    }

    #[test]
    fn filter_keys_never_collide_with_system() {
        // field_id starts at 1, so smallest filter key is (1 << 48) | 0 = 2^48
        let smallest = encode_filter_key(1, 0);
        assert!(smallest > KEY_META, "smallest filter key {} must exceed metadata key {}", smallest, KEY_META);
    }

    #[test]
    fn value_truncation() {
        // Values > 48 bits get truncated
        let full = 0xFFFF_FFFF_FFFF_FFFF_u64;
        let key = encode_filter_key(1, full);
        match decode_key(key) {
            DecodedKey::Filter { value, .. } => {
                assert_eq!(value, 0x0000_FFFF_FFFF_FFFF);
            }
            other => panic!("expected Filter, got {:?}", other),
        }
    }
}
