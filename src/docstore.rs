//! Document types and packed value encoding.
//!
//! This module contains the core types (`StoredDoc`, `PackedValue`) and
//! encoding helpers shared across the docstore and bitmap layers.
//!
//! Document persistence is handled by `DocStoreV3` in `shard_store_doc.rs`,
//! which stores documents in ShardStore format with CRC32 integrity,
//! generation pinning, and native compaction.

use std::collections::HashMap;

use crate::config::{FieldMapping, FieldValueType};
use crate::mutation::FieldValue;

/// Number of bits to shift slot_id right to get shard index.
/// 9 → 512 docs per shard.
pub const SHARD_SHIFT: u32 = 9;

/// Public accessor for SHARD_SHIFT (used by slot_arena finalization).
pub const SHARD_SHIFT_PUB: u32 = SHARD_SHIFT;

/// A stored document containing all field values.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StoredDoc {
    pub fields: HashMap<String, FieldValue>,
    /// Schema version this document was encoded with.
    /// Used to select correct defaults when reading elided fields.
    /// 0 = legacy (pre-versioning), 1+ = versioned.
    #[serde(skip, default)]
    pub schema_version: u8,
}

// ---------------------------------------------------------------------------
// Compact value encoding
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub enum PackedValue {
    I(i64),
    F(f64),
    B(bool),
    S(String),
    Mi(Vec<i64>),
    Mm(Vec<PackedValue>),
}

/// Convert a raw JSON value to PackedValue, with optional dictionary for LowCardinalityString.
pub fn json_to_packed_with_dict(
    raw: &serde_json::Value,
    mapping: &FieldMapping,
    ms_to_seconds: bool,
    dictionary: Option<&crate::dictionary::FieldDictionary>,
) -> Option<PackedValue> {
    match mapping.value_type {
        FieldValueType::Integer => {
            let n = raw
                .as_i64()
                .or_else(|| raw.as_u64().map(|u| u as i64))
                .or_else(|| raw.as_f64().map(|f| f as i64))?;
            let n = if ms_to_seconds {
                ((n / 1000) as u32) as i64
            } else {
                n
            };
            Some(PackedValue::I(n))
        }
        FieldValueType::Boolean => Some(PackedValue::B(raw.as_bool()?)),
        FieldValueType::String => Some(PackedValue::S(raw.as_str()?.to_string())),
        FieldValueType::MappedString => {
            let s = raw.as_str()?;
            let lookup = if mapping.case_sensitive {
                std::borrow::Cow::Borrowed(s)
            } else {
                std::borrow::Cow::Owned(s.to_lowercase())
            };
            let n = mapping
                .string_map
                .as_ref()
                .and_then(|m| m.get(lookup.as_ref()).copied())
                .unwrap_or(0);
            Some(PackedValue::I(n))
        }
        FieldValueType::LowCardinalityString => {
            let s = raw.as_str()?;
            if let Some(dict) = dictionary {
                let n = dict.get_or_insert(s);
                Some(PackedValue::I(n))
            } else {
                Some(PackedValue::I(0))
            }
        }
        FieldValueType::IntegerArray => {
            let arr = raw.as_array()?;
            if arr.is_empty() {
                return None;
            }
            let values: Vec<i64> = arr
                .iter()
                .filter_map(|v| v.as_i64().or_else(|| v.as_u64().map(|u| u as i64)))
                .collect();
            if values.is_empty() {
                None
            } else {
                Some(PackedValue::Mi(values))
            }
        }
        FieldValueType::ExistsBoolean => Some(PackedValue::B(true)),
    }
}


