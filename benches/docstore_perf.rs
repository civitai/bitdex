//! DocStore V1 vs V2 performance comparison benchmark.
//!
//! Compares the original compressed shard format (V1) against the new
//! append-only BitTuple log format (V2) across write, read, and compaction
//! workloads.

use std::collections::HashMap;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};

use bitdex_v2::docstore::{DocStore, PackedValue, StoredDoc};
use bitdex_v2::mutation::FieldValue;
use bitdex_v2::query::Value;

const SHARD_SIZE: u32 = 512;

// ---------------------------------------------------------------------------
// Helpers: generate realistic Civitai-like docs
// ---------------------------------------------------------------------------

/// Field names used in both V1 and V2 benchmarks.
const FIELD_NAMES: &[&str] = &[
    "id", "nsfwLevel", "userId", "type", "url", "poi", "minor", "sortAt",
];

fn make_image_doc(slot: u32) -> StoredDoc {
    let mut fields = HashMap::with_capacity(8);
    fields.insert("id".into(), FieldValue::Single(Value::Integer(slot as i64)));
    fields.insert(
        "nsfwLevel".into(),
        FieldValue::Single(Value::Integer((slot % 32) as i64)),
    );
    fields.insert(
        "userId".into(),
        FieldValue::Single(Value::Integer((slot * 7 + 13) as i64)),
    );
    fields.insert(
        "type".into(),
        FieldValue::Single(Value::String("image".into())),
    );
    fields.insert(
        "url".into(),
        FieldValue::Single(Value::String(format!(
            "xG1nkqKTMzGDvpLrqFT7WA/{:08x}/w=400",
            slot
        ))),
    );
    fields.insert("poi".into(), FieldValue::Single(Value::Bool(slot % 5 == 0)));
    fields.insert(
        "minor".into(),
        FieldValue::Single(Value::Bool(slot % 20 == 0)),
    );
    fields.insert(
        "sortAt".into(),
        FieldValue::Single(Value::Integer(1_700_000_000 + slot as i64)),
    );
    StoredDoc {
        fields,
        schema_version: 0,
    }
}

/// Encode a PackedValue to msgpack bytes (same as V2 tuple format uses).
fn pack_to_bytes(pv: &PackedValue) -> Vec<u8> {
    rmp_serde::to_vec(pv).unwrap_or_default()
}

/// Generate V2 tuples for a single doc: Vec<(field_idx, msgpack_bytes)>.
fn doc_to_v2_tuples(slot: u32, field_to_idx: &HashMap<String, u16>) -> Vec<(u16, Vec<u8>)> {
    let mut tuples = Vec::with_capacity(8);
    let emit = |name: &str, pv: PackedValue, out: &mut Vec<(u16, Vec<u8>)>| {
        if let Some(&idx) = field_to_idx.get(name) {
            out.push((idx, pack_to_bytes(&pv)));
        }
    };
    emit("id", PackedValue::I(slot as i64), &mut tuples);
    emit(
        "nsfwLevel",
        PackedValue::I((slot % 32) as i64),
        &mut tuples,
    );
    emit(
        "userId",
        PackedValue::I((slot * 7 + 13) as i64),
        &mut tuples,
    );
    emit(
        "type",
        PackedValue::S("image".into()),
        &mut tuples,
    );
    emit(
        "url",
        PackedValue::S(format!("xG1nkqKTMzGDvpLrqFT7WA/{:08x}/w=400", slot)),
        &mut tuples,
    );
    emit(
        "poi",
        PackedValue::B(slot % 5 == 0),
        &mut tuples,
    );
    emit(
        "minor",
        PackedValue::B(slot % 20 == 0),
        &mut tuples,
    );
    emit(
        "sortAt",
        PackedValue::I(1_700_000_000 + slot as i64),
        &mut tuples,
    );
    tuples
}

/// Open a DocStore with field dict populated, return (store, field_to_idx).
fn setup_store(dir: &std::path::Path) -> DocStore {
    let docs_dir = dir.join("docs");
    std::fs::create_dir_all(docs_dir.join("meta")).unwrap();
    std::fs::create_dir_all(docs_dir.join("shards")).unwrap();
    let mut store = DocStore::open(&docs_dir).unwrap();
    for name in FIELD_NAMES {
        store.ensure_field_idx_pub(name);
    }
    store
}

// ---------------------------------------------------------------------------
// 1. write_v1_shard: write 512 docs via write_batch_encoded (V1)
// ---------------------------------------------------------------------------

fn bench_write_v1_shard(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let mut store = setup_store(dir.path());
    let entries: Vec<(u32, Vec<u8>)> = (0..SHARD_SIZE)
        .map(|i| {
            let doc = make_image_doc(i);
            let raw = store.encode_doc_pub(&doc).unwrap();
            (i, raw)
        })
        .collect();

    let path = dir.path().join("v1_shard.bin");

    let mut group = c.benchmark_group("docstore_write");
    group.throughput(Throughput::Elements(SHARD_SIZE as u64));

    group.bench_function("v1_shard_512", |b| {
        b.iter(|| {
            DocStore::write_shard_file_pub(&path, &entries).unwrap();
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 2. write_v2_append: append 512 docs as tuples via append_tuple (V2)
// ---------------------------------------------------------------------------

fn bench_write_v2_append(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let store = setup_store(dir.path());
    let field_to_idx = store.field_to_idx().clone();

    // Pre-compute all tuples
    let all_tuples: Vec<Vec<(u16, Vec<u8>)>> = (0..SHARD_SIZE)
        .map(|i| doc_to_v2_tuples(i, &field_to_idx))
        .collect();

    let mut group = c.benchmark_group("docstore_write");
    group.throughput(Throughput::Elements(SHARD_SIZE as u64));

    group.bench_function("v2_append_512", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                // Fresh dir each iteration to avoid accumulating data
                let iter_dir = tempfile::tempdir().unwrap();
                let iter_store = setup_store(iter_dir.path());
                let start = std::time::Instant::now();
                for (slot, tuples) in all_tuples.iter().enumerate() {
                    for (field_idx, bytes) in tuples {
                        iter_store.append_tuple(slot as u32, *field_idx, bytes).unwrap();
                    }
                }
                total += start.elapsed();
            }
            total
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 3. read_v1_single: read 1 doc from V1 shard
// ---------------------------------------------------------------------------

fn bench_read_v1_single(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let mut store = setup_store(dir.path());

    // Write a shard with 512 docs
    let entries: Vec<(u32, Vec<u8>)> = (0..SHARD_SIZE)
        .map(|i| {
            let doc = make_image_doc(i);
            let raw = store.encode_doc_pub(&doc).unwrap();
            (i, raw)
        })
        .collect();
    let path = DocStore::shard_path_pub(store.path(), DocStore::shard_id_pub(0));
    DocStore::write_shard_file_pub(&path, &entries).unwrap();

    let mut group = c.benchmark_group("docstore_read");
    group.throughput(Throughput::Elements(1));

    group.bench_function("v1_single", |b| {
        b.iter(|| {
            let doc = store.get(42).unwrap();
            assert!(doc.is_some());
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 4. read_v2_single: read 1 doc from V2 shard
// ---------------------------------------------------------------------------

fn bench_read_v2_single(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let store = setup_store(dir.path());
    let field_to_idx = store.field_to_idx().clone();

    // Write 512 docs as V2 tuples
    for i in 0..SHARD_SIZE {
        for (field_idx, bytes) in doc_to_v2_tuples(i, &field_to_idx) {
            store.append_tuple(i, field_idx, &bytes).unwrap();
        }
    }

    let mut group = c.benchmark_group("docstore_read");
    group.throughput(Throughput::Elements(1));

    group.bench_function("v2_single", |b| {
        b.iter(|| {
            let doc = store.get_v2(42).unwrap();
            assert!(doc.is_some());
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 5. read_v1_batch_20: read 20 docs from V1 shard
// ---------------------------------------------------------------------------

fn bench_read_v1_batch_20(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let mut store = setup_store(dir.path());

    let entries: Vec<(u32, Vec<u8>)> = (0..SHARD_SIZE)
        .map(|i| {
            let doc = make_image_doc(i);
            let raw = store.encode_doc_pub(&doc).unwrap();
            (i, raw)
        })
        .collect();
    let path = DocStore::shard_path_pub(store.path(), DocStore::shard_id_pub(0));
    DocStore::write_shard_file_pub(&path, &entries).unwrap();

    let read_slots: Vec<u32> = (0..20).map(|i| i * 25).collect(); // spread across shard

    let mut group = c.benchmark_group("docstore_read");
    group.throughput(Throughput::Elements(20));

    group.bench_function("v1_batch_20", |b| {
        b.iter(|| {
            for &slot in &read_slots {
                let doc = store.get(slot).unwrap();
                assert!(doc.is_some());
            }
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 6. read_v2_batch_20: read 20 docs from V2 shard
// ---------------------------------------------------------------------------

fn bench_read_v2_batch_20(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let store = setup_store(dir.path());
    let field_to_idx = store.field_to_idx().clone();

    for i in 0..SHARD_SIZE {
        for (field_idx, bytes) in doc_to_v2_tuples(i, &field_to_idx) {
            store.append_tuple(i, field_idx, &bytes).unwrap();
        }
    }

    let read_slots: Vec<u32> = (0..20).map(|i| i * 25).collect();

    let mut group = c.benchmark_group("docstore_read");
    group.throughput(Throughput::Elements(20));

    group.bench_function("v2_batch_20", |b| {
        b.iter(|| {
            for &slot in &read_slots {
                let doc = store.get_v2(slot).unwrap();
                assert!(doc.is_some());
            }
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 7. compact_v2: compact a dirty V2 shard (30% stale entries)
// ---------------------------------------------------------------------------

fn bench_compact_v2(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let store = setup_store(dir.path());
    let field_to_idx = store.field_to_idx().clone();

    // Write initial 512 docs
    for i in 0..SHARD_SIZE {
        for (field_idx, bytes) in doc_to_v2_tuples(i, &field_to_idx) {
            store.append_tuple(i, field_idx, &bytes).unwrap();
        }
    }

    // Overwrite ~30% of docs with updated values (makes old tuples stale)
    let stale_count = (SHARD_SIZE as f64 * 0.3) as u32;
    for i in 0..stale_count {
        let slot = i * 3; // every 3rd slot
        if slot >= SHARD_SIZE {
            break;
        }
        // Write updated sortAt to create stale entries
        if let Some(&idx) = field_to_idx.get("sortAt") {
            let bytes = pack_to_bytes(&PackedValue::I(2_000_000_000 + slot as i64));
            store.append_tuple(slot, idx, &bytes).unwrap();
        }
    }

    let _shard_id = DocStore::shard_id_pub(0);

    let mut group = c.benchmark_group("docstore_compact");
    group.throughput(Throughput::Elements(SHARD_SIZE as u64));

    group.bench_function("v2_compact_30pct_stale", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                // Re-create the dirty shard each iteration
                let iter_dir = tempfile::tempdir().unwrap();
                let iter_store = setup_store(iter_dir.path());
                let ft = iter_store.field_to_idx().clone();
                for i in 0..SHARD_SIZE {
                    for (field_idx, bytes) in doc_to_v2_tuples(i, &ft) {
                        iter_store.append_tuple(i, field_idx, &bytes).unwrap();
                    }
                }
                for i in 0..stale_count {
                    let slot = i * 3;
                    if slot >= SHARD_SIZE { break; }
                    if let Some(&idx) = ft.get("sortAt") {
                        let bytes = pack_to_bytes(&PackedValue::I(2_000_000_000 + slot as i64));
                        iter_store.append_tuple(slot, idx, &bytes).unwrap();
                    }
                }
                let sid = DocStore::shard_id_pub(0);
                let start = std::time::Instant::now();
                iter_store.compact_shard(sid).unwrap();
                total += start.elapsed();
            }
            total
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_write_v1_shard,
    bench_write_v2_append,
    bench_read_v1_single,
    bench_read_v2_single,
    bench_read_v1_batch_20,
    bench_read_v2_batch_20,
    bench_compact_v2,
);
criterion_main!(benches);
