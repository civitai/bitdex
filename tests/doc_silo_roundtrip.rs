//! Integration tests for `DocSilo` — lifted into `tests/` because the
//! lib test target has pre-existing unrelated compile rot (filter.rs /
//! query_metrics.rs / ingester.rs), and these tests only need the public API.

use bitdex_v2::doc_silo::{DocOp, DocSilo};
use bitdex_v2::mutation::FieldValue;
use bitdex_v2::query::Value;
use bitdex_v2::shard_store_doc::{PackedValue, StoredDoc};

fn make_doc(fields: &[(&str, FieldValue)]) -> StoredDoc {
    let mut map: ahash::AHashMap<String, FieldValue> = ahash::AHashMap::new();
    for (k, v) in fields {
        map.insert(k.to_string(), v.clone());
    }
    StoredDoc {
        fields: map,
        schema_version: 0u8,
    }
}

#[test]
fn put_then_get_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let mut silo = DocSilo::open(dir.path()).unwrap();
    let doc = make_doc(&[
        ("nsfwLevel", FieldValue::Single(Value::Integer(8))),
        ("isPublished", FieldValue::Single(Value::Bool(true))),
        (
            "tagIds",
            FieldValue::Multi(vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3),
            ]),
        ),
    ]);
    silo.put(42, &doc).unwrap();
    let got = silo.get(42).unwrap().unwrap();
    assert_eq!(got.fields.len(), 3);
    assert_eq!(
        got.fields.get("nsfwLevel"),
        Some(&FieldValue::Single(Value::Integer(8)))
    );
    assert!(matches!(got.fields.get("tagIds"), Some(FieldValue::Multi(_))));
}

#[test]
fn typed_set_op_updates_single_field() {
    let dir = tempfile::tempdir().unwrap();
    let mut silo = DocSilo::open(dir.path()).unwrap();
    silo.put(
        1,
        &make_doc(&[("nsfwLevel", FieldValue::Single(Value::Integer(8)))]),
    )
    .unwrap();
    let idx = silo.ensure_field_index("nsfwLevel");
    silo.apply_op(&DocOp::Set {
        slot: 1,
        field: idx,
        value: PackedValue::I(16),
    })
    .unwrap();
    let got = silo.get(1).unwrap().unwrap();
    assert_eq!(
        got.fields.get("nsfwLevel"),
        Some(&FieldValue::Single(Value::Integer(16)))
    );
}

#[test]
fn typed_append_op_unions_multi_int() {
    let dir = tempfile::tempdir().unwrap();
    let mut silo = DocSilo::open(dir.path()).unwrap();
    silo.put(
        1,
        &make_doc(&[(
            "tagIds",
            FieldValue::Multi(vec![Value::Integer(1), Value::Integer(2)]),
        )]),
    )
    .unwrap();
    let idx = silo.ensure_field_index("tagIds");
    silo.apply_ops_batch(&[
        DocOp::Append { slot: 1, field: idx, value: PackedValue::I(3) },
        DocOp::Append { slot: 1, field: idx, value: PackedValue::I(2) }, // dedup
    ])
    .unwrap();
    let got = silo.get(1).unwrap().unwrap();
    match got.fields.get("tagIds") {
        Some(FieldValue::Multi(vs)) => {
            let ints: Vec<i64> = vs
                .iter()
                .filter_map(|v| if let Value::Integer(i) = v { Some(*i) } else { None })
                .collect();
            assert_eq!(ints, vec![1, 2, 3]);
        }
        other => panic!("expected Multi, got {other:?}"),
    }
}

#[test]
fn remove_op_drops_value_from_multi_int() {
    let dir = tempfile::tempdir().unwrap();
    let mut silo = DocSilo::open(dir.path()).unwrap();
    silo.put(
        1,
        &make_doc(&[(
            "tagIds",
            FieldValue::Multi(vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3),
            ]),
        )]),
    )
    .unwrap();
    let idx = silo.ensure_field_index("tagIds");
    silo.apply_op(&DocOp::Remove {
        slot: 1,
        field: idx,
        value: PackedValue::I(2),
    })
    .unwrap();
    let got = silo.get(1).unwrap().unwrap();
    match got.fields.get("tagIds") {
        Some(FieldValue::Multi(vs)) => {
            let ints: Vec<i64> = vs
                .iter()
                .filter_map(|v| if let Value::Integer(i) = v { Some(*i) } else { None })
                .collect();
            assert_eq!(ints, vec![1, 3]);
        }
        other => panic!("expected Multi, got {other:?}"),
    }
}

#[test]
fn delete_op_hides_doc_on_read() {
    let dir = tempfile::tempdir().unwrap();
    let mut silo = DocSilo::open(dir.path()).unwrap();
    silo.put(
        1,
        &make_doc(&[("x", FieldValue::Single(Value::Integer(1)))]),
    )
    .unwrap();
    assert!(silo.get(1).unwrap().is_some());
    silo.apply_op(&DocOp::Delete { slot: 1 }).unwrap();
    assert!(silo.get(1).unwrap().is_none());
}

#[test]
fn compact_folds_ops_and_persists() {
    let dir = tempfile::tempdir().unwrap();
    let mut silo = DocSilo::open(dir.path()).unwrap();
    silo.put(
        1,
        &make_doc(&[("nsfwLevel", FieldValue::Single(Value::Integer(0)))]),
    )
    .unwrap();
    let idx = silo.ensure_field_index("nsfwLevel");
    silo.apply_ops_batch(&[
        DocOp::Set { slot: 1, field: idx, value: PackedValue::I(8) },
        DocOp::Set { slot: 1, field: idx, value: PackedValue::I(16) },
    ])
    .unwrap();
    assert!(silo.has_ops());
    silo.compact().unwrap();
    assert!(!silo.has_ops());
    let got = silo.get(1).unwrap().unwrap();
    assert_eq!(
        got.fields.get("nsfwLevel"),
        Some(&FieldValue::Single(Value::Integer(16)))
    );
}

#[test]
fn get_many_batched_read() {
    let dir = tempfile::tempdir().unwrap();
    let mut silo = DocSilo::open(dir.path()).unwrap();
    for i in 0..10u32 {
        silo.put(
            i,
            &make_doc(&[("x", FieldValue::Single(Value::Integer(i as i64)))]),
        )
        .unwrap();
    }
    let got = silo.get_many(&[0, 1, 2, 99]).unwrap();
    assert_eq!(got.len(), 4);
    assert!(got[0].is_some());
    assert!(got[1].is_some());
    assert!(got[2].is_some());
    assert!(got[3].is_none());
}

#[test]
fn bulk_load_then_ops_layer_on_top() {
    let dir = tempfile::tempdir().unwrap();
    let mut silo = DocSilo::open(dir.path()).unwrap();
    let docs: Vec<(u32, StoredDoc)> = (0..100u32)
        .map(|i| {
            (
                i,
                make_doc(&[("x", FieldValue::Single(Value::Integer(i as i64)))]),
            )
        })
        .collect();
    silo.bulk_load(&docs).unwrap();

    // Layer a typed Set on top of the bulk-loaded snapshot.
    let idx = silo.ensure_field_index("x");
    silo.apply_op(&DocOp::Set {
        slot: 50,
        field: idx,
        value: PackedValue::I(999),
    })
    .unwrap();

    let got = silo.get(50).unwrap().unwrap();
    assert_eq!(
        got.fields.get("x"),
        Some(&FieldValue::Single(Value::Integer(999)))
    );
    // Untouched entries still read straight from data.bin.
    let got = silo.get(51).unwrap().unwrap();
    assert_eq!(
        got.fields.get("x"),
        Some(&FieldValue::Single(Value::Integer(51)))
    );
}

#[test]
fn reopen_replays_ops_log() {
    let dir = tempfile::tempdir().unwrap();
    let idx;
    {
        let mut silo = DocSilo::open(dir.path()).unwrap();
        silo.put(
            1,
            &make_doc(&[("nsfwLevel", FieldValue::Single(Value::Integer(0)))]),
        )
        .unwrap();
        idx = silo.ensure_field_index("nsfwLevel");
        silo.apply_op(&DocOp::Set {
            slot: 1,
            field: idx,
            value: PackedValue::I(42),
        })
        .unwrap();
        silo.save_field_dict().unwrap();
    }
    let silo = DocSilo::open(dir.path()).unwrap();
    let got = silo.get(1).unwrap().unwrap();
    assert_eq!(
        got.fields.get("nsfwLevel"),
        Some(&FieldValue::Single(Value::Integer(42)))
    );
}
