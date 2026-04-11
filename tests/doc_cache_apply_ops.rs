//! Integration tests for `DocCache::apply_ops_in_place` — the v1.0.186
//! fix for the refresh-I/O regression in v1.0.185.
//!
//! These live in `tests/` instead of inside `doc_cache.rs` because the
//! lib test target has pre-existing unrelated compile rot we haven't
//! cleaned up (filter.rs / query_metrics.rs / ingester.rs). Integration
//! tests build against the lib's public API only and stay green.

#![cfg(feature = "pg-sync")]

use bitdex_v2::doc_cache::{ApplyOutcome, DocCache, DocCacheConfig};
use bitdex_v2::mutation::FieldValue;
use bitdex_v2::pg_sync::ops::Op;
use bitdex_v2::query::Value;
use bitdex_v2::shard_store_doc::StoredDoc;
use serde_json::json;

fn make_doc(fields: Vec<(&str, FieldValue)>) -> StoredDoc {
    let mut map = ahash::AHashMap::new();
    for (k, v) in fields {
        map.insert(k.to_string(), v);
    }
    StoredDoc {
        fields: map,
        schema_version: 0,
    }
}

fn seed_doc() -> StoredDoc {
    make_doc(vec![
        ("nsfwLevel", FieldValue::Single(Value::Integer(8))),
        ("isPublished", FieldValue::Single(Value::Bool(false))),
        (
            "tagIds",
            FieldValue::Multi(vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3),
            ]),
        ),
    ])
}

#[test]
fn set_scalar_replaces_value() {
    let cache = DocCache::new(DocCacheConfig::default());
    cache.insert(42, seed_doc());
    let out = cache.apply_ops_in_place(
        42,
        &[Op::Set {
            field: "nsfwLevel".into(),
            value: json!(16),
        }],
    );
    assert_eq!(out, ApplyOutcome::Applied);
    let doc = cache.get(42).unwrap();
    assert_eq!(
        doc.fields.get("nsfwLevel"),
        Some(&FieldValue::Single(Value::Integer(16)))
    );
}

#[test]
fn remove_then_set_scalar_is_equivalent_to_set() {
    // pg-sync emits Remove(old) + Set(new) for scalar updates.
    let cache = DocCache::new(DocCacheConfig::default());
    cache.insert(42, seed_doc());
    let out = cache.apply_ops_in_place(
        42,
        &[
            Op::Remove {
                field: "nsfwLevel".into(),
                value: json!(8),
            },
            Op::Set {
                field: "nsfwLevel".into(),
                value: json!(16),
            },
        ],
    );
    assert_eq!(out, ApplyOutcome::Applied);
    let doc = cache.get(42).unwrap();
    assert_eq!(
        doc.fields.get("nsfwLevel"),
        Some(&FieldValue::Single(Value::Integer(16)))
    );
}

#[test]
fn add_multi_appends_and_dedups() {
    let cache = DocCache::new(DocCacheConfig::default());
    cache.insert(42, seed_doc());
    cache.apply_ops_in_place(
        42,
        &[Op::Add {
            field: "tagIds".into(),
            value: json!(99),
        }],
    );
    cache.apply_ops_in_place(
        42,
        &[Op::Add {
            field: "tagIds".into(),
            value: json!(2),
        }],
    );
    let doc = cache.get(42).unwrap();
    match doc.fields.get("tagIds") {
        Some(FieldValue::Multi(vals)) => {
            let ints: Vec<i64> = vals
                .iter()
                .filter_map(|v| {
                    if let Value::Integer(i) = v {
                        Some(*i)
                    } else {
                        None
                    }
                })
                .collect();
            assert_eq!(ints, vec![1, 2, 3, 99], "99 appended, 2 deduped");
        }
        other => panic!("expected Multi, got {other:?}"),
    }
}

#[test]
fn remove_multi_retains_others() {
    let cache = DocCache::new(DocCacheConfig::default());
    cache.insert(42, seed_doc());
    let out = cache.apply_ops_in_place(
        42,
        &[Op::Remove {
            field: "tagIds".into(),
            value: json!(2),
        }],
    );
    assert_eq!(out, ApplyOutcome::Applied);
    let doc = cache.get(42).unwrap();
    match doc.fields.get("tagIds") {
        Some(FieldValue::Multi(vals)) => {
            assert_eq!(vals.len(), 2);
            assert!(!vals.contains(&Value::Integer(2)));
        }
        other => panic!("expected Multi, got {other:?}"),
    }
}

#[test]
fn add_on_absent_field_creates_multi() {
    let cache = DocCache::new(DocCacheConfig::default());
    cache.insert(42, seed_doc());
    let out = cache.apply_ops_in_place(
        42,
        &[Op::Add {
            field: "newMulti".into(),
            value: json!(42),
        }],
    );
    assert_eq!(out, ApplyOutcome::Applied);
    let doc = cache.get(42).unwrap();
    assert_eq!(
        doc.fields.get("newMulti"),
        Some(&FieldValue::Multi(vec![Value::Integer(42)]))
    );
}

#[test]
fn delete_removes_entry() {
    let cache = DocCache::new(DocCacheConfig::default());
    cache.insert(42, seed_doc());
    assert!(cache.contains(42));
    let out = cache.apply_ops_in_place(42, &[Op::Delete]);
    assert_eq!(out, ApplyOutcome::Deleted);
    assert!(!cache.contains(42));
    assert_eq!(cache.len(), 0);
}

#[test]
fn not_cached_slot_returns_noop() {
    let cache = DocCache::new(DocCacheConfig::default());
    // slot 99 was never inserted
    let out = cache.apply_ops_in_place(
        99,
        &[Op::Set {
            field: "nsfwLevel".into(),
            value: json!(16),
        }],
    );
    assert_eq!(out, ApplyOutcome::NotCached);
    // cache should remain empty — cache-on-read preserved
    assert_eq!(cache.len(), 0);
    assert!(!cache.contains(99));
}

#[test]
fn queryopset_needs_fallback() {
    let cache = DocCache::new(DocCacheConfig::default());
    cache.insert(42, seed_doc());
    let out = cache.apply_ops_in_place(
        42,
        &[Op::QueryOpSet {
            query: Some("modelVersionIds eq 456".into()),
            ops: vec![],
        }],
    );
    assert_eq!(out, ApplyOutcome::NeedsFallback);
    // Entry stays intact — caller will route to disk refresh.
    assert!(cache.contains(42));
}

#[test]
fn add_type_mismatch_falls_back() {
    // If Add targets a Single field, we can't safely apply.
    let cache = DocCache::new(DocCacheConfig::default());
    cache.insert(42, seed_doc());
    let out = cache.apply_ops_in_place(
        42,
        &[Op::Add {
            field: "nsfwLevel".into(),
            value: json!(99),
        }],
    );
    assert_eq!(out, ApplyOutcome::NeedsFallback);
}

#[test]
fn cross_generation_apply_finds_old_entry() {
    let config = DocCacheConfig {
        max_bytes: 1_073_741_824,
        generation_interval_secs: 60,
        max_generations: 30,
    };
    let cache = DocCache::new(config);
    cache.insert(42, seed_doc());
    // Push gen 0 back a few rotations so the entry lives deep.
    for _ in 0..5 {
        cache.push_new_generation();
    }
    assert_eq!(cache.generation_count(), 6);

    let out = cache.apply_ops_in_place(
        42,
        &[Op::Set {
            field: "nsfwLevel".into(),
            value: json!(32),
        }],
    );
    assert_eq!(out, ApplyOutcome::Applied);
    let doc = cache.get(42).unwrap();
    assert_eq!(
        doc.fields.get("nsfwLevel"),
        Some(&FieldValue::Single(Value::Integer(32)))
    );
}

#[test]
fn size_bytes_updates_on_multi_grow() {
    let cache = DocCache::new(DocCacheConfig::default());
    cache.insert(42, seed_doc());
    let before = cache.size_bytes();
    for v in 100..120 {
        let out = cache.apply_ops_in_place(
            42,
            &[Op::Add {
                field: "tagIds".into(),
                value: json!(v),
            }],
        );
        assert_eq!(out, ApplyOutcome::Applied);
    }
    let after = cache.size_bytes();
    assert!(
        after > before,
        "size should grow with tag additions: {before} -> {after}"
    );
}

#[test]
fn alive_op_is_noop_on_cached_entry() {
    let cache = DocCache::new(DocCacheConfig::default());
    cache.insert(42, seed_doc());
    let out = cache.apply_ops_in_place(42, &[Op::Alive]);
    assert_eq!(out, ApplyOutcome::Applied);
    // Doc unchanged
    let doc = cache.get(42).unwrap();
    assert_eq!(
        doc.fields.get("nsfwLevel"),
        Some(&FieldValue::Single(Value::Integer(8)))
    );
}

#[test]
fn bare_remove_on_scalar_falls_back() {
    // Gemini H1 — Apr 11 2026. A bare Remove on a scalar field without
    // a paired Set would previously drop the field silently, leaving
    // the cache divergent from disk. Expected: NeedsFallback so the
    // caller routes to the disk refresh path.
    let cache = DocCache::new(DocCacheConfig::default());
    cache.insert(42, seed_doc());
    let out = cache.apply_ops_in_place(
        42,
        &[Op::Remove {
            field: "nsfwLevel".into(),
            value: json!(8),
        }],
    );
    assert_eq!(out, ApplyOutcome::NeedsFallback);
    // Entry unchanged — the cached doc still has the old value, and
    // the caller refreshes from disk before the next read.
    let doc = cache.get(42).unwrap();
    assert_eq!(
        doc.fields.get("nsfwLevel"),
        Some(&FieldValue::Single(Value::Integer(8)))
    );
}

#[test]
fn remove_scalar_with_paired_set_applies_cleanly() {
    // Regression test for the Remove look-ahead: a Remove on a scalar
    // field followed by a Set on the SAME field should still apply
    // in-place (the paired case that was working before the H1 fix).
    let cache = DocCache::new(DocCacheConfig::default());
    cache.insert(42, seed_doc());
    let out = cache.apply_ops_in_place(
        42,
        &[
            Op::Remove {
                field: "nsfwLevel".into(),
                value: json!(8),
            },
            Op::Set {
                field: "nsfwLevel".into(),
                value: json!(16),
            },
        ],
    );
    assert_eq!(out, ApplyOutcome::Applied);
    let doc = cache.get(42).unwrap();
    assert_eq!(
        doc.fields.get("nsfwLevel"),
        Some(&FieldValue::Single(Value::Integer(16)))
    );
}

#[test]
fn mid_batch_fallback_leaves_entry_unchanged() {
    // GPT review H1 — Apr 11 2026. Earlier ops in a batch must NOT
    // partially mutate the cached entry when a later op returns
    // NeedsFallback. Clone-then-swap guarantees all-or-nothing
    // semantics so the caller's fallback refresh starts from the
    // real pre-batch state, not a half-applied mess.
    let cache = DocCache::new(DocCacheConfig::default());
    cache.insert(42, seed_doc());
    let size_before = cache.size_bytes();
    // [Set(nsfwLevel), Add(nsfwLevel)] — second op is a type-
    // mismatch (Add on Single) that returns NeedsFallback. The
    // earlier Set must NOT have mutated the cache.
    let out = cache.apply_ops_in_place(
        42,
        &[
            Op::Set {
                field: "nsfwLevel".into(),
                value: json!(16),
            },
            Op::Add {
                field: "nsfwLevel".into(),
                value: json!(99),
            },
        ],
    );
    assert_eq!(out, ApplyOutcome::NeedsFallback);
    let doc = cache.get(42).unwrap();
    // The pre-batch value must be intact — nsfwLevel is still 8,
    // not the 16 that the Set would have applied.
    assert_eq!(
        doc.fields.get("nsfwLevel"),
        Some(&FieldValue::Single(Value::Integer(8)))
    );
    // Size accounting should also be unchanged.
    assert_eq!(cache.size_bytes(), size_before);
}

#[test]
fn remove_scalar_followed_by_unrelated_set_falls_back() {
    // Look-ahead must match on the same field name, not just "any Set".
    // A Remove(nsfwLevel) followed by Set(isPublished) is NOT a paired
    // update — it's a bare Remove plus an independent Set, and we need
    // to fall back so disk reload preserves correctness.
    let cache = DocCache::new(DocCacheConfig::default());
    cache.insert(42, seed_doc());
    let out = cache.apply_ops_in_place(
        42,
        &[
            Op::Remove {
                field: "nsfwLevel".into(),
                value: json!(8),
            },
            Op::Set {
                field: "isPublished".into(),
                value: json!(true),
            },
        ],
    );
    assert_eq!(out, ApplyOutcome::NeedsFallback);
    // Entry must be unchanged — critically, isPublished should NOT
    // have been updated before the bail, because that would leave
    // the cache in a partially-mutated state.
    let doc = cache.get(42).unwrap();
    assert_eq!(
        doc.fields.get("nsfwLevel"),
        Some(&FieldValue::Single(Value::Integer(8)))
    );
    assert_eq!(
        doc.fields.get("isPublished"),
        Some(&FieldValue::Single(Value::Bool(false)))
    );
}
