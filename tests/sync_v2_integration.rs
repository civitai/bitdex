//! Integration tests for the Sync V2 pipeline.
//!
//! Tests the ops → WAL → processor pipeline without PG.
//! Full E2E tests (PG triggers → poller → server) require a running
//! server and PG instance — see tests/e2e/ for those.

#![cfg(feature = "pg-sync")]

use serde_json::json;
use tempfile::TempDir;

use bitdex_v2::ops_wal::{WalReader, WalWriter};
use bitdex_v2::pg_sync::op_dedup::dedup_ops;
use bitdex_v2::pg_sync::ops::{EntityOps, Op, OpsBatch, SyncMeta};
use bitdex_v2::pg_sync::dump::{DumpRegistry, DumpStatus, dump_name, config_hash};
use bitdex_v2::pg_sync::trigger_gen::{SyncConfig, SyncSource, generate_trigger_sql};

// ── WAL Pipeline Integration ──

#[test]
fn test_ops_wal_roundtrip_with_dedup() {
    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("ops.wal");

    // Write ops with duplicates
    let writer = WalWriter::new(&wal_path);
    let batch = vec![
        EntityOps {
            entity_id: 1,
            creates_slot: false,
            ops: vec![
                Op::Set { field: "nsfwLevel".into(), value: json!(8) },
            ],
        },
        EntityOps {
            entity_id: 1,
            creates_slot: false,
            ops: vec![
                Op::Set { field: "nsfwLevel".into(), value: json!(16) }, // Overwrites first
            ],
        },
        EntityOps {
            entity_id: 2,
            creates_slot: false,
            ops: vec![
                Op::Add { field: "tagIds".into(), value: json!(42) },
            ],
        },
    ];
    writer.append_batch(&batch).unwrap();

    // Read back
    let mut reader = WalReader::new(&wal_path, 0);
    let result = reader.read_batch(100).unwrap();
    assert_eq!(result.entries.len(), 3);

    // Dedup
    let mut entries = result.entries;
    dedup_ops(&mut entries);

    // Entity 1: last set wins (nsfwLevel=16)
    let entity1 = entries.iter().find(|e| e.entity_id == 1).unwrap();
    let set_ops: Vec<_> = entity1.ops.iter()
        .filter(|op| matches!(op, Op::Set { field, .. } if field == "nsfwLevel"))
        .collect();
    assert_eq!(set_ops.len(), 1);
    if let Op::Set { value, .. } = &set_ops[0] {
        assert_eq!(*value, json!(16));
    }

    // Entity 2: add preserved
    let entity2 = entries.iter().find(|e| e.entity_id == 2).unwrap();
    assert_eq!(entity2.ops.len(), 1);
    assert!(matches!(&entity2.ops[0], Op::Add { field, .. } if field == "tagIds"));
}

#[test]
fn test_delete_absorbs_prior_ops_through_wal() {
    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("ops.wal");

    let writer = WalWriter::new(&wal_path);

    // First batch: set some fields
    writer.append_batch(&[EntityOps {
        entity_id: 1,
        creates_slot: false,
        ops: vec![
            Op::Set { field: "nsfwLevel".into(), value: json!(16) },
            Op::Add { field: "tagIds".into(), value: json!(42) },
        ],
    }]).unwrap();

    // Second batch: delete the entity
    writer.append_batch(&[EntityOps {
        entity_id: 1,
        creates_slot: false,
        ops: vec![Op::Delete],
    }]).unwrap();

    // Read all
    let mut reader = WalReader::new(&wal_path, 0);
    let result = reader.read_batch(100).unwrap();
    assert_eq!(result.entries.len(), 2);

    // Dedup should collapse to just delete
    let mut entries = result.entries;
    dedup_ops(&mut entries);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].ops.len(), 1);
    assert!(matches!(&entries[0].ops[0], Op::Delete));
}

#[test]
fn test_add_remove_cancellation_through_wal() {
    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("ops.wal");

    let writer = WalWriter::new(&wal_path);
    writer.append_batch(&[
        EntityOps {
            entity_id: 1,
            creates_slot: false,
            ops: vec![Op::Add { field: "tagIds".into(), value: json!(42) }],
        },
        EntityOps {
            entity_id: 1,
            creates_slot: false,
            ops: vec![Op::Remove { field: "tagIds".into(), value: json!(42) }],
        },
    ]).unwrap();

    let mut reader = WalReader::new(&wal_path, 0);
    let result = reader.read_batch(100).unwrap();
    let mut entries = result.entries;
    dedup_ops(&mut entries);

    // Net zero — entity should be dropped
    assert!(entries.is_empty() || entries[0].ops.is_empty());
}

#[test]
fn test_query_op_set_through_wal() {
    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("ops.wal");

    let writer = WalWriter::new(&wal_path);
    writer.append_batch(&[EntityOps {
        entity_id: 456,
        creates_slot: false,
        ops: vec![Op::QueryOpSet {
            query: "modelVersionIds eq 456".into(),
            ops: vec![
                Op::Remove { field: "baseModel".into(), value: json!("SD 1.5") },
                Op::Set { field: "baseModel".into(), value: json!("SDXL") },
            ],
        }],
    }]).unwrap();

    let mut reader = WalReader::new(&wal_path, 0);
    let result = reader.read_batch(100).unwrap();
    assert_eq!(result.entries.len(), 1);

    let entry = &result.entries[0];
    assert_eq!(entry.entity_id, 456);
    match &entry.ops[0] {
        Op::QueryOpSet { query, ops } => {
            assert_eq!(query, "modelVersionIds eq 456");
            assert_eq!(ops.len(), 2);
        }
        _ => panic!("Expected QueryOpSet"),
    }
}

// ── Cursor Resume Integration ──

#[test]
fn test_wal_cursor_resume_across_appends() {
    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("ops.wal");
    let writer = WalWriter::new(&wal_path);

    // Batch 1
    writer.append_batch(&[EntityOps {
        entity_id: 1,
        creates_slot: false,
        ops: vec![Op::Set { field: "a".into(), value: json!(1) }],
    }]).unwrap();

    // Read batch 1
    let mut reader = WalReader::new(&wal_path, 0);
    let r1 = reader.read_batch(100).unwrap();
    assert_eq!(r1.entries.len(), 1);
    let cursor = reader.cursor();

    // Batch 2 (appended after first read)
    writer.append_batch(&[EntityOps {
        entity_id: 2,
        creates_slot: false,
        ops: vec![Op::Set { field: "b".into(), value: json!(2) }],
    }]).unwrap();

    // Resume from cursor — should only get batch 2
    let mut reader2 = WalReader::new(&wal_path, cursor);
    let r2 = reader2.read_batch(100).unwrap();
    assert_eq!(r2.entries.len(), 1);
    assert_eq!(r2.entries[0].entity_id, 2);
}

// ── Dump Registry Integration ──

#[test]
fn test_dump_registry_full_workflow() {
    let dir = TempDir::new().unwrap();
    let dumps_path = dir.path().join("dumps.json");

    let mut reg = DumpRegistry::default();

    // Simulate boot: check if dumps are complete
    let image_hash = config_hash("table: Image\ntrack_fields: [nsfwLevel]");
    let tags_hash = config_hash("table: TagsOnImageNew\nfield: tagIds");
    let image_name = dump_name("Image", &image_hash);
    let tags_name = dump_name("TagsOnImageNew", &tags_hash);

    assert!(!reg.is_complete(&image_name));
    assert!(!reg.is_complete(&tags_name));

    // Register dumps
    reg.register(image_name.clone(), Some("dumps/image.wal".into()));
    reg.register(tags_name.clone(), Some("dumps/tags.wal".into()));
    reg.save(&dumps_path).unwrap();

    // Simulate pg-sync writing WAL and signaling loaded
    reg.mark_loaded(&image_name, 107_500_000);
    reg.mark_loaded(&tags_name, 375_000_000);

    // Simulate WAL reader completing
    reg.mark_complete(&image_name, 107_500_000);
    assert!(!reg.all_complete()); // Tags not done yet

    reg.mark_complete(&tags_name, 375_000_000);
    assert!(reg.all_complete());

    // Persist and reload
    reg.save(&dumps_path).unwrap();
    let loaded = DumpRegistry::load(&dumps_path);
    assert!(loaded.is_complete(&image_name));
    assert!(loaded.is_complete(&tags_name));
    assert!(loaded.all_complete());
}

#[test]
fn test_dump_config_change_detection() {
    let hash1 = config_hash("table: Image\ntrack_fields: [nsfwLevel]");
    let hash2 = config_hash("table: Image\ntrack_fields: [nsfwLevel, type]");
    let name1 = dump_name("Image", &hash1);
    let name2 = dump_name("Image", &hash2);

    let mut reg = DumpRegistry::default();
    reg.register(name1.clone(), None);
    reg.mark_loaded(&name1, 100);
    reg.mark_complete(&name1, 100);

    // After config change, the dump name is different
    assert!(reg.is_complete(&name1));
    assert!(!reg.is_complete(&name2)); // New hash → not loaded → needs re-dump
}

// ── Trigger Generation Integration ──

#[test]
fn test_full_civitai_config() {
    let yaml = r#"
sync_sources:
  - table: Image
    slot_field: id
    sets_alive: true
    track_fields:
      - nsfwLevel
      - type
      - userId
      - postId
      - minor
      - poi
      - blockedFor
      - "GREATEST({scannedAt}, {createdAt}) as existedAt"
      - "({flags} & (1 << 13)) != 0 AND ({flags} & (1 << 2)) = 0 as hasMeta"
      - "({flags} & (1 << 14)) != 0 as onSite"
    on_delete: delete_slot

  - table: TagsOnImageNew
    slot_field: imageId
    field: tagIds
    value_field: tagId

  - table: ImageTool
    slot_field: imageId
    field: toolIds
    value_field: toolId

  - table: ImageTechnique
    slot_field: imageId
    field: techniqueIds
    value_field: techniqueId

  - table: ModelVersion
    query: "modelVersionIds eq {id}"
    track_fields: [baseModel]

  - table: Post
    query: "postId eq {id}"
    track_fields: [publishedAt, availability]
"#;

    let config = SyncConfig::from_yaml(yaml).unwrap();
    assert_eq!(config.sync_sources.len(), 6);

    // Generate SQL for each and verify they're non-empty and contain expected patterns
    for source in &config.sync_sources {
        let sql = generate_trigger_sql(source);
        assert!(sql.contains("CREATE OR REPLACE FUNCTION"), "Missing function for {}", source.table);
        assert!(sql.contains("ENABLE ALWAYS"), "Missing ENABLE ALWAYS for {}", source.table);

        match source.table.as_str() {
            "Image" => {
                assert!(sql.contains("IS DISTINCT FROM"), "Image should use IS DISTINCT FROM");
                assert!(sql.contains("delete"), "Image should handle delete");
                assert!(sql.contains("GREATEST"), "Image should have existedAt expression");
            }
            "TagsOnImageNew" => {
                assert!(sql.contains("'add'"), "Tags should have add ops");
                assert!(sql.contains("'remove'"), "Tags should have remove ops");
            }
            "ModelVersion" => {
                assert!(sql.contains("queryOpSet"), "MV should use queryOpSet");
                assert!(sql.contains("modelVersionIds eq"), "MV should query by MV id");
            }
            "Post" => {
                assert!(sql.contains("queryOpSet"), "Post should use queryOpSet");
                assert!(sql.contains("postId eq"), "Post should query by postId");
            }
            _ => {}
        }
    }
}

// ── OpsBatch Serialization ──

#[test]
fn test_ops_batch_json_format() {
    let batch = OpsBatch {
        ops: vec![
            EntityOps {
                entity_id: 123,
                creates_slot: false,
                ops: vec![
                    Op::Remove { field: "nsfwLevel".into(), value: json!(8) },
                    Op::Set { field: "nsfwLevel".into(), value: json!(16) },
                ],
            },
            EntityOps {
                entity_id: 456,
                creates_slot: false,
                ops: vec![Op::QueryOpSet {
                    query: "modelVersionIds eq 456".into(),
                    ops: vec![
                        Op::Remove { field: "baseModel".into(), value: json!("SD 1.5") },
                        Op::Set { field: "baseModel".into(), value: json!("SDXL") },
                    ],
                }],
            },
        ],
        meta: Some(SyncMeta {
            source: "pg-sync-default".into(),
            cursor: Some(420_000_000),
            max_id: Some(500_000_000),
            lag_rows: Some(80_000_000),
        }),
    };

    // Round-trip through JSON
    let json_str = serde_json::to_string(&batch).unwrap();
    let parsed: OpsBatch = serde_json::from_str(&json_str).unwrap();
    assert_eq!(parsed.ops.len(), 2);
    assert_eq!(parsed.ops[0].entity_id, 123);
    assert_eq!(parsed.ops[1].entity_id, 456);
    let meta = parsed.meta.unwrap();
    assert_eq!(meta.source, "pg-sync-default");
    assert_eq!(meta.lag_rows, Some(80_000_000));
}
