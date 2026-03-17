//! Shared field mapper: converts raw JSON values to engine `FieldValue`s using schema metadata.
//!
//! This is the single source of truth for field conversion logic. All ingestion paths
//! (NDJSON loader, PG CSV single-pass, row assembler, HTTP upsert) call `map_field`
//! instead of maintaining their own conversion logic.

use crate::config::{FieldMapping, FieldValueType};
use crate::dictionary::FieldDictionary;
use crate::mutation::FieldValue;
use crate::query::Value;

/// Convert a raw `serde_json::Value` to an engine `FieldValue` using the given field mapping.
///
/// `apply_ms` controls whether ms→seconds conversion is applied (true when reading from
/// the primary source field, false when reading from a fallback that's already in seconds).
///
/// Returns `None` if the value cannot be converted (null, wrong type, empty array, etc.).
pub fn map_field(
    raw: &serde_json::Value,
    mapping: &FieldMapping,
    apply_ms: bool,
    dictionary: Option<&FieldDictionary>,
) -> Option<FieldValue> {
    match mapping.value_type {
        FieldValueType::Integer => {
            let n = if let Some(n) = raw.as_i64() {
                n
            } else if let Some(n) = raw.as_u64() {
                n as i64
            } else if let Some(n) = raw.as_f64() {
                n as i64
            } else {
                return None;
            };
            let n = if apply_ms {
                ((n / 1000) as u32) as i64
            } else {
                n
            };
            Some(FieldValue::Single(Value::Integer(n)))
        }
        FieldValueType::Boolean => {
            let b = raw.as_bool()?;
            Some(FieldValue::Single(Value::Bool(b)))
        }
        FieldValueType::String => {
            let s = raw.as_str()?;
            Some(FieldValue::Single(Value::String(s.to_string())))
        }
        FieldValueType::MappedString => {
            let s = raw.as_str()?;
            let map = mapping.string_map.as_ref()?;
            let lookup = if mapping.case_sensitive {
                std::borrow::Cow::Borrowed(s)
            } else {
                std::borrow::Cow::Owned(s.to_lowercase())
            };
            let n = map.get(lookup.as_ref()).copied().unwrap_or(0);
            Some(FieldValue::Single(Value::Integer(n)))
        }
        FieldValueType::LowCardinalityString => {
            let s = raw.as_str()?;
            if let Some(dict) = dictionary {
                let n = dict.get_or_insert(s);
                Some(FieldValue::Single(Value::Integer(n)))
            } else {
                // Without a dictionary, store as 0 (unknown)
                Some(FieldValue::Single(Value::Integer(0)))
            }
        }
        FieldValueType::IntegerArray => {
            let arr = raw.as_array()?;
            if arr.is_empty() {
                return None;
            }
            let values: Vec<Value> = arr
                .iter()
                .filter_map(|v| {
                    v.as_i64()
                        .or_else(|| v.as_u64().map(|n| n as i64))
                        .map(Value::Integer)
                })
                .collect();
            if values.is_empty() {
                None
            } else {
                Some(FieldValue::Multi(values))
            }
        }
        FieldValueType::ExistsBoolean => Some(FieldValue::Single(Value::Bool(true))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    fn int_mapping(source: &str, target: &str) -> FieldMapping {
        FieldMapping {
            source: source.into(),
            target: target.into(),
            value_type: FieldValueType::Integer,
            fallback: None,
            string_map: None,
            doc_only: false,
            filter_only: false,
            ms_to_seconds: false,
            truncate_u32: false,
            case_sensitive: false,
            default_value: None,
        }
    }

    #[test]
    fn test_integer_basic() {
        let m = int_mapping("x", "x");
        let v = map_field(&json!(42), &m, false, None).unwrap();
        assert!(matches!(v, FieldValue::Single(Value::Integer(42))));
    }

    #[test]
    fn test_integer_ms_to_seconds() {
        let m = int_mapping("x", "x");
        let v = map_field(&json!(1700000000000_i64), &m, true, None).unwrap();
        if let FieldValue::Single(Value::Integer(n)) = v {
            assert_eq!(n, ((1700000000000_i64 / 1000) as u32) as i64);
        } else {
            panic!("expected integer");
        }
    }

    #[test]
    fn test_boolean() {
        let m = FieldMapping {
            value_type: FieldValueType::Boolean,
            ..int_mapping("x", "x")
        };
        let v = map_field(&json!(true), &m, false, None).unwrap();
        assert!(matches!(v, FieldValue::Single(Value::Bool(true))));
    }

    #[test]
    fn test_string() {
        let m = FieldMapping {
            value_type: FieldValueType::String,
            ..int_mapping("x", "x")
        };
        let v = map_field(&json!("hello"), &m, false, None).unwrap();
        assert!(matches!(v, FieldValue::Single(Value::String(ref s)) if s == "hello"));
    }

    #[test]
    fn test_mapped_string_case_insensitive() {
        let mut map = HashMap::new();
        map.insert("draft".to_string(), 1);
        map.insert("published".to_string(), 2);
        let m = FieldMapping {
            value_type: FieldValueType::MappedString,
            string_map: Some(map),
            case_sensitive: false,
            ..int_mapping("x", "x")
        };
        let v = map_field(&json!("PUBLISHED"), &m, false, None).unwrap();
        assert!(matches!(v, FieldValue::Single(Value::Integer(2))));
    }

    #[test]
    fn test_integer_array() {
        let m = FieldMapping {
            value_type: FieldValueType::IntegerArray,
            ..int_mapping("x", "x")
        };
        let v = map_field(&json!([1, 2, 3]), &m, false, None).unwrap();
        if let FieldValue::Multi(vals) = v {
            assert_eq!(vals.len(), 3);
        } else {
            panic!("expected multi");
        }
    }

    #[test]
    fn test_integer_array_empty() {
        let m = FieldMapping {
            value_type: FieldValueType::IntegerArray,
            ..int_mapping("x", "x")
        };
        assert!(map_field(&json!([]), &m, false, None).is_none());
    }

    #[test]
    fn test_exists_boolean() {
        let m = FieldMapping {
            value_type: FieldValueType::ExistsBoolean,
            ..int_mapping("x", "x")
        };
        // ExistsBoolean always returns true regardless of input value
        let v = map_field(&json!("anything"), &m, false, None).unwrap();
        assert!(matches!(v, FieldValue::Single(Value::Bool(true))));
    }

    #[test]
    fn test_null_returns_none() {
        let m = int_mapping("x", "x");
        assert!(map_field(&json!(null), &m, false, None).is_none());
    }

    #[test]
    fn test_float_truncated_to_int() {
        let m = int_mapping("x", "x");
        let v = map_field(&json!(3.7), &m, false, None).unwrap();
        assert!(matches!(v, FieldValue::Single(Value::Integer(3))));
    }

    #[test]
    fn test_zero_timestamp_ms_to_seconds() {
        let m = int_mapping("x", "x");
        let v = map_field(&json!(0), &m, true, None).unwrap();
        assert!(matches!(v, FieldValue::Single(Value::Integer(0))));
    }

    #[test]
    fn test_low_cardinality_string_with_dict() {
        use crate::dictionary::FieldDictionary;
        let dict = FieldDictionary::new();
        let m = FieldMapping {
            value_type: FieldValueType::LowCardinalityString,
            ..int_mapping("x", "x")
        };
        let v = map_field(&json!("hello"), &m, false, Some(&dict)).unwrap();
        if let FieldValue::Single(Value::Integer(n)) = v {
            assert!(n > 0, "dictionary should assign a positive key");
        } else {
            panic!("expected integer");
        }
        // Same string should get the same key
        let v2 = map_field(&json!("hello"), &m, false, Some(&dict)).unwrap();
        assert_eq!(
            format!("{:?}", v),
            format!("{:?}", v2),
        );
    }

    #[test]
    fn test_low_cardinality_string_without_dict() {
        let m = FieldMapping {
            value_type: FieldValueType::LowCardinalityString,
            ..int_mapping("x", "x")
        };
        let v = map_field(&json!("hello"), &m, false, None).unwrap();
        // Without dict, defaults to 0
        assert!(matches!(v, FieldValue::Single(Value::Integer(0))));
    }
}
