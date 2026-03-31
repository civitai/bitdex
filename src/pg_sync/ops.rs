//! V2 ops data types for the ops-based sync pipeline.
//!
//! Ops are self-contained mutations: each carries the field name, old value (for removes),
//! and new value (for sets). This eliminates docstore reads on the write path.
//!
//! Op types:
//! - `set`: Set a scalar/sort field to a new value
//! - `remove`: Clear a slot from a field's bitmap (carries old value)
//! - `add`: Add a value to a multi-value field (tags, tools, etc.)
//! - `delete`: Delete a document (clears all bitmaps + alive bit)
//! - `queryOpSet`: Resolve slots via a BitDex query, apply nested ops to all matches

use serde::{Deserialize, Serialize};

/// A single operation within an ops array.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op")]
pub enum Op {
    /// Set a field to a value. For filter fields, sets the bit in the value's bitmap.
    /// For sort fields, decomposes to bit layers.
    #[serde(rename = "set")]
    Set {
        field: String,
        value: serde_json::Value,
    },

    /// Remove a slot from a field's bitmap (old value). Used in remove/set pairs
    /// for field changes: remove old value, then set new value.
    #[serde(rename = "remove")]
    Remove {
        field: String,
        value: serde_json::Value,
    },

    /// Add a value to a multi-value field (e.g., tagIds, toolIds).
    /// Used for join-table INSERTs.
    #[serde(rename = "add")]
    Add {
        field: String,
        value: serde_json::Value,
    },

    /// Delete a document. Clears all filter/sort bitmap bits + alive bit.
    /// Requires a docstore read to determine which bitmaps to clear.
    #[serde(rename = "delete")]
    Delete,

    /// Signal that this entity should have its alive bit set (creates the slot).
    /// Emitted by PG triggers on INSERT for `sets_alive: true` tables.
    /// When present, ops_poller sets `creates_slot: true` on the EntityOps.
    #[serde(rename = "alive")]
    Alive,

    /// Query-resolved bulk operation. Resolves slots via a BitDex query string,
    /// then applies the nested ops to all matching slots.
    /// Used for fan-out tables (ModelVersion, Post, Model).
    #[serde(rename = "queryOpSet")]
    QueryOpSet {
        /// BitDex query string (e.g., "modelVersionIds eq 456")
        query: String,
        /// Ops to apply to all slots matching the query
        ops: Vec<Op>,
    },
}

/// A row from the BitdexOps table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpsRow {
    /// Auto-incrementing ID (used as cursor position)
    pub id: i64,
    /// The entity (image) ID this op targets. For queryOpSet, this is the
    /// source entity ID (e.g., ModelVersion ID, Post ID).
    pub entity_id: i64,
    /// Array of operations to apply
    pub ops: Vec<Op>,
}

/// A batch of ops sent to the BitDex /ops endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpsBatch {
    /// Per-entity ops
    pub ops: Vec<EntityOps>,
    /// Optional sync source metadata (cursor position, lag, etc.)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<SyncMeta>,
}

/// Ops for a single entity within a batch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityOps {
    /// The entity (image) ID
    pub entity_id: i64,
    /// Operations to apply
    pub ops: Vec<Op>,
    /// If true, this entity should have its alive bit set (creates the slot if new).
    /// Only the primary entity table (e.g., Image with sets_alive: true) sets this.
    /// Join tables (tags, tools) leave this false — they only add multi-value bitmaps.
    #[serde(default)]
    pub creates_slot: bool,
}

impl EntityOps {
    /// Convenience constructor — creates_slot defaults to false.
    pub fn new(entity_id: i64, ops: Vec<Op>) -> Self {
        Self { entity_id, ops, creates_slot: false }
    }

    /// Constructor for primary entity ops that should create alive slots.
    pub fn with_alive(entity_id: i64, ops: Vec<Op>) -> Self {
        Self { entity_id, ops, creates_slot: true }
    }
}

/// Sync source metadata, bundled with ops payloads.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncMeta {
    /// Sync source identifier (e.g., "pg-sync-default", "clickhouse")
    pub source: String,
    /// Current cursor position in the ops table
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<i64>,
    /// Max ID in the ops table (for lag calculation)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_id: Option<i64>,
    /// Number of rows behind (max_id - cursor)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lag_rows: Option<i64>,
}

/// SQL for creating the BitdexOps table and index.
pub const SETUP_OPS_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS "BitdexOps" (
    id          BIGSERIAL PRIMARY KEY,
    entity_id   BIGINT NOT NULL,
    ops         JSONB NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_bitdex_ops_id ON "BitdexOps" (id);
"#;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_set_op_roundtrip() {
        let op = Op::Set {
            field: "nsfwLevel".into(),
            value: json!(16),
        };
        let json = serde_json::to_string(&op).unwrap();
        let parsed: Op = serde_json::from_str(&json).unwrap();
        assert_eq!(op, parsed);
    }

    #[test]
    fn test_remove_op_roundtrip() {
        let op = Op::Remove {
            field: "nsfwLevel".into(),
            value: json!(8),
        };
        let json = serde_json::to_string(&op).unwrap();
        let parsed: Op = serde_json::from_str(&json).unwrap();
        assert_eq!(op, parsed);
    }

    #[test]
    fn test_add_op_roundtrip() {
        let op = Op::Add {
            field: "tagIds".into(),
            value: json!(42),
        };
        let json = serde_json::to_string(&op).unwrap();
        let parsed: Op = serde_json::from_str(&json).unwrap();
        assert_eq!(op, parsed);
    }

    #[test]
    fn test_delete_op_roundtrip() {
        let op = Op::Delete;
        let json = serde_json::to_string(&op).unwrap();
        let parsed: Op = serde_json::from_str(&json).unwrap();
        assert_eq!(op, parsed);
    }

    #[test]
    fn test_query_op_set_roundtrip() {
        let op = Op::QueryOpSet {
            query: "modelVersionIds eq 456".into(),
            ops: vec![
                Op::Remove {
                    field: "baseModel".into(),
                    value: json!("SD 1.5"),
                },
                Op::Set {
                    field: "baseModel".into(),
                    value: json!("SDXL"),
                },
            ],
        };
        let json = serde_json::to_string(&op).unwrap();
        let parsed: Op = serde_json::from_str(&json).unwrap();
        assert_eq!(op, parsed);
    }

    #[test]
    fn test_ops_array_from_json() {
        let json = json!([
            {"op": "remove", "field": "nsfwLevel", "value": 8},
            {"op": "set", "field": "nsfwLevel", "value": 16},
            {"op": "add", "field": "tagIds", "value": 42},
            {"op": "delete"}
        ]);
        let ops: Vec<Op> = serde_json::from_value(json).unwrap();
        assert_eq!(ops.len(), 4);
        assert!(matches!(&ops[0], Op::Remove { field, .. } if field == "nsfwLevel"));
        assert!(matches!(&ops[1], Op::Set { field, .. } if field == "nsfwLevel"));
        assert!(matches!(&ops[2], Op::Add { field, .. } if field == "tagIds"));
        assert!(matches!(&ops[3], Op::Delete));
    }

    #[test]
    fn test_ops_batch_with_meta() {
        let batch = OpsBatch {
            ops: vec![EntityOps {
                entity_id: 123,
                creates_slot: false,
                ops: vec![Op::Set {
                    field: "nsfwLevel".into(),
                    value: json!(16),
                }],
            }],
            meta: Some(SyncMeta {
                source: "pg-sync-default".into(),
                cursor: Some(420_000_000),
                max_id: Some(500_000_000),
                lag_rows: Some(80_000_000),
            }),
        };
        let json = serde_json::to_string(&batch).unwrap();
        let parsed: OpsBatch = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.ops.len(), 1);
        assert_eq!(parsed.ops[0].entity_id, 123);
        assert!(parsed.meta.is_some());
        assert_eq!(parsed.meta.unwrap().source, "pg-sync-default");
    }

    #[test]
    fn test_ops_batch_without_meta() {
        let batch = OpsBatch {
            ops: vec![],
            meta: None,
        };
        let json = serde_json::to_string(&batch).unwrap();
        assert!(!json.contains("meta"));
    }

    #[test]
    fn test_image_insert_ops() {
        // Simulates what an Image INSERT trigger would produce
        let ops: Vec<Op> = vec![
            Op::Set { field: "nsfwLevel".into(), value: json!(1) },
            Op::Set { field: "type".into(), value: json!("image") },
            Op::Set { field: "userId".into(), value: json!(12345) },
            Op::Set { field: "postId".into(), value: json!(67890) },
            Op::Set { field: "existedAt".into(), value: json!(1711234567) },
        ];
        let json = serde_json::to_value(&ops).unwrap();
        let parsed: Vec<Op> = serde_json::from_value(json).unwrap();
        assert_eq!(parsed.len(), 5);
    }

    #[test]
    fn test_image_update_ops_with_old_values() {
        // Simulates Image UPDATE: nsfwLevel 8→16
        let ops: Vec<Op> = vec![
            Op::Remove { field: "nsfwLevel".into(), value: json!(8) },
            Op::Set { field: "nsfwLevel".into(), value: json!(16) },
        ];
        let json = serde_json::to_value(&ops).unwrap();
        let parsed: Vec<Op> = serde_json::from_value(json).unwrap();
        assert_eq!(parsed.len(), 2);
    }

    #[test]
    fn test_query_op_set_with_in_query() {
        // Model POI change: fan-out to all model versions
        let op = Op::QueryOpSet {
            query: "modelVersionIds in [101, 102, 103]".into(),
            ops: vec![Op::Set {
                field: "poi".into(),
                value: json!(true),
            }],
        };
        let json = serde_json::to_string(&op).unwrap();
        let parsed: Op = serde_json::from_str(&json).unwrap();
        if let Op::QueryOpSet { query, ops } = parsed {
            assert!(query.contains("in [101"));
            assert_eq!(ops.len(), 1);
        } else {
            panic!("Expected QueryOpSet");
        }
    }
}
