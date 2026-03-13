//! Docstore merge microbenchmark.
//!
//! Measures read-modify-write performance for shard files under three compression
//! strategies: (A) whole-shard zstd, (B) uncompressed, (C) per-doc zstd.
//! This informs the table-at-a-time bulk loader design.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use criterion::{criterion_group, criterion_main, Criterion, Throughput};

use bitdex_v2::docstore::{DocStore, StoredDoc};
use bitdex_v2::mutation::FieldValue;
use bitdex_v2::query::Value;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const SHARD_SIZE: u32 = 512;
const SHARD_VERSION: u32 = 1;

// ---------------------------------------------------------------------------
// Helpers: generate realistic docs
// ---------------------------------------------------------------------------

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
    StoredDoc { fields }
}

/// Pre-encode a full shard of 512 docs using DocStore encoding.
fn encode_shard_docs(store: &mut DocStore, base_slot: u32) -> Vec<(u32, Vec<u8>)> {
    (0..SHARD_SIZE)
        .map(|i| {
            let slot = base_slot + i;
            let doc = make_image_doc(slot);
            let raw = store.encode_doc_pub(&doc).unwrap();
            (slot, raw)
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Approach A: Compressed (current format) — uses DocStore methods
// ---------------------------------------------------------------------------

fn a_write_shard(path: &Path, entries: &[(u32, Vec<u8>)]) {
    DocStore::write_shard_file_pub(path, entries).unwrap();
}

fn a_read_shard(data: &[u8]) -> (Vec<(u32, u32, u32)>, Vec<u8>) {
    DocStore::read_shard_file_pub(data).unwrap()
}

// ---------------------------------------------------------------------------
// Approach B: Uncompressed — same format minus zstd
// ---------------------------------------------------------------------------

fn b_write_shard(path: &Path, entries: &[(u32, Vec<u8>)]) {
    let total_raw: usize = entries.iter().map(|(_, d)| d.len()).sum();
    let mut raw_data = Vec::with_capacity(total_raw);
    let mut offsets: Vec<(u32, u32, u32)> = Vec::with_capacity(entries.len());
    let mut offset: u32 = 0;
    for (slot_id, doc_bytes) in entries {
        offsets.push((*slot_id, offset, doc_bytes.len() as u32));
        raw_data.extend_from_slice(doc_bytes);
        offset += doc_bytes.len() as u32;
    }
    // header + index + raw (no compression, no uncompressed_size field)
    let header_size = 8 + entries.len() * 12;
    let mut buf = Vec::with_capacity(header_size + raw_data.len());
    buf.extend_from_slice(&SHARD_VERSION.to_le_bytes());
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for (slot_id, off, len) in &offsets {
        buf.extend_from_slice(&slot_id.to_le_bytes());
        buf.extend_from_slice(&off.to_le_bytes());
        buf.extend_from_slice(&len.to_le_bytes());
    }
    buf.extend_from_slice(&raw_data);

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    let tmp = path.with_extension("bin.tmp");
    std::fs::write(&tmp, &buf).unwrap();
    std::fs::rename(&tmp, path).unwrap();
}

fn b_read_shard(data: &[u8]) -> (Vec<(u32, u32, u32)>, Vec<u8>) {
    assert!(data.len() >= 8);
    let num = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
    let mut entries = Vec::with_capacity(num);
    for i in 0..num {
        let base = 8 + i * 12;
        let slot = u32::from_le_bytes([data[base], data[base + 1], data[base + 2], data[base + 3]]);
        let off = u32::from_le_bytes([data[base + 4], data[base + 5], data[base + 6], data[base + 7]]);
        let len = u32::from_le_bytes([data[base + 8], data[base + 9], data[base + 10], data[base + 11]]);
        entries.push((slot, off, len));
    }
    let data_start = 8 + num * 12;
    let raw = data[data_start..].to_vec();
    (entries, raw)
}

// ---------------------------------------------------------------------------
// Approach C: Per-doc compression
// ---------------------------------------------------------------------------

fn c_write_shard(path: &Path, entries: &[(u32, Vec<u8>)]) {
    // Each doc compressed independently, index points into compressed blob
    let mut compressed_data = Vec::with_capacity(entries.len() * 100);
    let mut offsets: Vec<(u32, u32, u32)> = Vec::with_capacity(entries.len());
    let mut offset: u32 = 0;
    for (slot_id, doc_bytes) in entries {
        let compressed = zstd::encode_all(doc_bytes.as_slice(), 1).unwrap();
        let clen = compressed.len() as u32;
        offsets.push((*slot_id, offset, clen));
        compressed_data.extend_from_slice(&compressed);
        offset += clen;
    }
    let header_size = 8 + entries.len() * 12;
    let mut buf = Vec::with_capacity(header_size + compressed_data.len());
    buf.extend_from_slice(&SHARD_VERSION.to_le_bytes());
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for (slot_id, off, len) in &offsets {
        buf.extend_from_slice(&slot_id.to_le_bytes());
        buf.extend_from_slice(&off.to_le_bytes());
        buf.extend_from_slice(&len.to_le_bytes());
    }
    buf.extend_from_slice(&compressed_data);

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    let tmp = path.with_extension("bin.tmp");
    std::fs::write(&tmp, &buf).unwrap();
    std::fs::rename(&tmp, path).unwrap();
}

fn c_read_shard(data: &[u8]) -> (Vec<(u32, u32, u32)>, Vec<u8>) {
    // Returns index + the raw compressed blob (NOT decompressed)
    assert!(data.len() >= 8);
    let num = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
    let mut entries = Vec::with_capacity(num);
    for i in 0..num {
        let base = 8 + i * 12;
        let slot = u32::from_le_bytes([data[base], data[base + 1], data[base + 2], data[base + 3]]);
        let off = u32::from_le_bytes([data[base + 4], data[base + 5], data[base + 6], data[base + 7]]);
        let len = u32::from_le_bytes([data[base + 8], data[base + 9], data[base + 10], data[base + 11]]);
        entries.push((slot, off, len));
    }
    let data_start = 8 + num * 12;
    let compressed_blob = data[data_start..].to_vec();
    (entries, compressed_blob)
}

/// Decompress a single doc from the per-doc-compressed blob.
fn c_decompress_one(blob: &[u8], offset: u32, length: u32) -> Vec<u8> {
    let start = offset as usize;
    let end = start + length as usize;
    zstd::decode_all(&blob[start..end]).unwrap()
}

// ---------------------------------------------------------------------------
// Merge helper: read shard, update docs, rewrite
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Setup: create a pre-populated shard file for each approach
// ---------------------------------------------------------------------------

struct ShardSetup {
    _dir: tempfile::TempDir,
    path_a: PathBuf,
    path_b: PathBuf,
    path_c: PathBuf,
    encoded_entries: Vec<(u32, Vec<u8>)>,
}

fn setup_shard(base_slot: u32) -> ShardSetup {
    let dir = tempfile::tempdir().unwrap();
    let docs_dir = dir.path().join("docs");
    std::fs::create_dir_all(docs_dir.join("meta")).unwrap();
    std::fs::create_dir_all(docs_dir.join("shards")).unwrap();

    let mut store = DocStore::open(&docs_dir).unwrap();
    let entries = encode_shard_docs(&mut store, base_slot);

    let path_a = dir.path().join("shard_a.bin");
    let path_b = dir.path().join("shard_b.bin");
    let path_c = dir.path().join("shard_c.bin");

    a_write_shard(&path_a, &entries);
    b_write_shard(&path_b, &entries);
    c_write_shard(&path_c, &entries);

    ShardSetup {
        _dir: dir,
        path_a,
        path_b,
        path_c,
        encoded_entries: entries,
    }
}

// ---------------------------------------------------------------------------
// W1: Single doc scalar merge
// ---------------------------------------------------------------------------

fn bench_w1_scalar_merge(c: &mut Criterion) {
    let setup = setup_shard(0);

    let mut group = c.benchmark_group("W1_scalar_merge");
    group.throughput(Throughput::Elements(1));

    // A: Compressed
    group.bench_function("A_compressed", |b| {
        // Reset shard before each iteration block
        a_write_shard(&setup.path_a, &setup.encoded_entries);
        b.iter(|| {
            let file_data = std::fs::read(&setup.path_a).unwrap();
            let (index, decompressed) = a_read_shard(&file_data);
            let target_slot = 42u32;
            let target_idx = index
                .binary_search_by_key(&target_slot, |e| e.0)
                .unwrap();
            let (_, off, len) = index[target_idx];
            let mut entries: Vec<(u32, Vec<u8>)> = index
                .iter()
                .map(|(s, o, l)| {
                    (*s, decompressed[*o as usize..(*o + *l) as usize].to_vec())
                })
                .collect();
            // Merge: decode, add field, re-encode
            let raw = &decompressed[off as usize..(off + len) as usize];
            let mut pairs: Vec<(u16, serde_json::Value)> =
                rmp_serde::from_slice(raw).unwrap_or_default();
            // Append availability field (using a high index as placeholder)
            pairs.push((99, serde_json::Value::from(1i64)));
            let new_raw = rmp_serde::to_vec(&pairs).unwrap();
            entries[target_idx] = (target_slot, new_raw);
            DocStore::write_shard_file_pub(&setup.path_a, &entries).unwrap();
        });
    });

    // B: Uncompressed
    group.bench_function("B_uncompressed", |b| {
        b_write_shard(&setup.path_b, &setup.encoded_entries);
        b.iter(|| {
            let file_data = std::fs::read(&setup.path_b).unwrap();
            let (index, data) = b_read_shard(&file_data);
            let target_slot = 42u32;
            let target_idx = index
                .binary_search_by_key(&target_slot, |e| e.0)
                .unwrap();
            let (_, off, len) = index[target_idx];
            let mut entries: Vec<(u32, Vec<u8>)> = index
                .iter()
                .map(|(s, o, l)| {
                    (*s, data[*o as usize..(*o + *l) as usize].to_vec())
                })
                .collect();
            let raw = &data[off as usize..(off + len) as usize];
            let mut pairs: Vec<(u16, serde_json::Value)> =
                rmp_serde::from_slice(raw).unwrap_or_default();
            pairs.push((99, serde_json::Value::from(1i64)));
            let new_raw = rmp_serde::to_vec(&pairs).unwrap();
            entries[target_idx] = (target_slot, new_raw);
            b_write_shard(&setup.path_b, &entries);
        });
    });

    // C: Per-doc compressed
    group.bench_function("C_per_doc", |b| {
        c_write_shard(&setup.path_c, &setup.encoded_entries);
        b.iter(|| {
            let file_data = std::fs::read(&setup.path_c).unwrap();
            let (index, blob) = c_read_shard(&file_data);
            let target_slot = 42u32;
            let target_idx = index
                .binary_search_by_key(&target_slot, |e| e.0)
                .unwrap();
            // Only decompress the target doc
            let (_, off, len) = index[target_idx];
            let raw = c_decompress_one(&blob, off, len);
            let mut pairs: Vec<(u16, serde_json::Value)> =
                rmp_serde::from_slice(&raw).unwrap_or_default();
            pairs.push((99, serde_json::Value::from(1i64)));
            let new_raw = rmp_serde::to_vec(&pairs).unwrap();
            // Rebuild: keep all other compressed docs, replace target
            let mut new_entries: Vec<(u32, Vec<u8>)> = Vec::with_capacity(index.len());
            for (i, (s, o, l)) in index.iter().enumerate() {
                if i == target_idx {
                    new_entries.push((*s, new_raw.clone()));
                } else {
                    let doc_raw = c_decompress_one(&blob, *o, *l);
                    new_entries.push((*s, doc_raw));
                }
            }
            c_write_shard(&setup.path_c, &new_entries);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// W2: Single doc multi-value append (tagIds)
// ---------------------------------------------------------------------------

fn bench_w2_tag_append(c: &mut Criterion) {
    // Create docs that already have a tagIds array
    let dir = tempfile::tempdir().unwrap();
    let docs_dir = dir.path().join("docs");
    std::fs::create_dir_all(docs_dir.join("meta")).unwrap();
    std::fs::create_dir_all(docs_dir.join("shards")).unwrap();

    let mut store = DocStore::open(&docs_dir).unwrap();
    let base_slot = 0u32;
    let entries: Vec<(u32, Vec<u8>)> = (0..SHARD_SIZE)
        .map(|i| {
            let slot = base_slot + i;
            let mut doc = make_image_doc(slot);
            // Add initial tagIds array (5 tags)
            doc.fields.insert(
                "tagIds".into(),
                FieldValue::Multi(
                    (0..5)
                        .map(|t| Value::Integer((slot * 100 + t) as i64))
                        .collect(),
                ),
            );
            let raw = store.encode_doc_pub(&doc).unwrap();
            (slot, raw)
        })
        .collect();

    let path_a = dir.path().join("shard_a.bin");
    let path_b = dir.path().join("shard_b.bin");
    let path_c = dir.path().join("shard_c.bin");

    let mut group = c.benchmark_group("W2_tag_append");
    group.throughput(Throughput::Elements(1));

    // A: Compressed
    group.bench_function("A_compressed", |b| {
        a_write_shard(&path_a, &entries);
        b.iter(|| {
            let file_data = std::fs::read(&path_a).unwrap();
            let (index, decompressed) = a_read_shard(&file_data);
            let target_slot = 42u32;
            let target_idx = index.binary_search_by_key(&target_slot, |e| e.0).unwrap();
            let mut all_entries: Vec<(u32, Vec<u8>)> = index
                .iter()
                .map(|(s, o, l)| (*s, decompressed[*o as usize..(*o + *l) as usize].to_vec()))
                .collect();
            // Decode target, append tag, re-encode
            let doc = store.decode_doc_pub(&all_entries[target_idx].1).unwrap();
            let mut new_doc = doc;
            if let Some(FieldValue::Multi(ref mut tags)) = new_doc.fields.get_mut("tagIds") {
                tags.push(Value::Integer(99999));
            }
            all_entries[target_idx].1 = store.encode_doc_pub(&new_doc).unwrap();
            DocStore::write_shard_file_pub(&path_a, &all_entries).unwrap();
        });
    });

    // B: Uncompressed
    group.bench_function("B_uncompressed", |b| {
        b_write_shard(&path_b, &entries);
        b.iter(|| {
            let file_data = std::fs::read(&path_b).unwrap();
            let (index, data) = b_read_shard(&file_data);
            let target_slot = 42u32;
            let target_idx = index.binary_search_by_key(&target_slot, |e| e.0).unwrap();
            let mut all_entries: Vec<(u32, Vec<u8>)> = index
                .iter()
                .map(|(s, o, l)| (*s, data[*o as usize..(*o + *l) as usize].to_vec()))
                .collect();
            let doc = store.decode_doc_pub(&all_entries[target_idx].1).unwrap();
            let mut new_doc = doc;
            if let Some(FieldValue::Multi(ref mut tags)) = new_doc.fields.get_mut("tagIds") {
                tags.push(Value::Integer(99999));
            }
            all_entries[target_idx].1 = store.encode_doc_pub(&new_doc).unwrap();
            b_write_shard(&path_b, &all_entries);
        });
    });

    // C: Per-doc compressed
    group.bench_function("C_per_doc", |b| {
        c_write_shard(&path_c, &entries);
        b.iter(|| {
            let file_data = std::fs::read(&path_c).unwrap();
            let (index, blob) = c_read_shard(&file_data);
            let target_slot = 42u32;
            let target_idx = index.binary_search_by_key(&target_slot, |e| e.0).unwrap();
            let (_, off, len) = index[target_idx];
            let raw = c_decompress_one(&blob, off, len);
            let doc = store.decode_doc_pub(&raw).unwrap();
            let mut new_doc = doc;
            if let Some(FieldValue::Multi(ref mut tags)) = new_doc.fields.get_mut("tagIds") {
                tags.push(Value::Integer(99999));
            }
            let new_raw = store.encode_doc_pub(&new_doc).unwrap();
            // Must rewrite all (index offsets change)
            let mut new_entries: Vec<(u32, Vec<u8>)> = Vec::with_capacity(index.len());
            for (i, (s, o, l)) in index.iter().enumerate() {
                if i == target_idx {
                    new_entries.push((*s, new_raw.clone()));
                } else {
                    new_entries.push((*s, c_decompress_one(&blob, *o, *l)));
                }
            }
            c_write_shard(&path_c, &new_entries);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// W3: Batch merge (N docs in same shard)
// ---------------------------------------------------------------------------

fn bench_w3_batch_merge(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let docs_dir = dir.path().join("docs");
    std::fs::create_dir_all(docs_dir.join("meta")).unwrap();
    std::fs::create_dir_all(docs_dir.join("shards")).unwrap();

    let mut store = DocStore::open(&docs_dir).unwrap();
    let entries = encode_shard_docs(&mut store, 0);

    let path_a = dir.path().join("shard_a.bin");
    let path_b = dir.path().join("shard_b.bin");
    let path_c = dir.path().join("shard_c.bin");

    for n in [1, 10, 50, 100, 512] {
        let mut group = c.benchmark_group(format!("W3_batch_merge_N{}", n));
        group.throughput(Throughput::Elements(n as u64));

        // Generate the merge payload: N docs with a new "availability" field
        let merge_slots: Vec<u32> = (0..n as u32).collect();

        // A: Compressed
        group.bench_function("A_compressed", |b| {
            a_write_shard(&path_a, &entries);
            b.iter(|| {
                let file_data = std::fs::read(&path_a).unwrap();
                let (index, decompressed) = a_read_shard(&file_data);
                let mut all_entries: Vec<(u32, Vec<u8>)> = index
                    .iter()
                    .map(|(s, o, l)| (*s, decompressed[*o as usize..(*o + *l) as usize].to_vec()))
                    .collect();
                // Merge N docs
                for &slot in &merge_slots {
                    let idx = index.binary_search_by_key(&slot, |e| e.0).unwrap();
                    let doc = store.decode_doc_pub(&all_entries[idx].1).unwrap();
                    let mut new_doc = doc;
                    new_doc.fields.insert(
                        "availability".into(),
                        FieldValue::Single(Value::Integer(1)),
                    );
                    all_entries[idx].1 = store.encode_doc_pub(&new_doc).unwrap();
                }
                DocStore::write_shard_file_pub(&path_a, &all_entries).unwrap();
            });
        });

        // B: Uncompressed
        group.bench_function("B_uncompressed", |b| {
            b_write_shard(&path_b, &entries);
            b.iter(|| {
                let file_data = std::fs::read(&path_b).unwrap();
                let (index, data) = b_read_shard(&file_data);
                let mut all_entries: Vec<(u32, Vec<u8>)> = index
                    .iter()
                    .map(|(s, o, l)| (*s, data[*o as usize..(*o + *l) as usize].to_vec()))
                    .collect();
                for &slot in &merge_slots {
                    let idx = index.binary_search_by_key(&slot, |e| e.0).unwrap();
                    let doc = store.decode_doc_pub(&all_entries[idx].1).unwrap();
                    let mut new_doc = doc;
                    new_doc.fields.insert(
                        "availability".into(),
                        FieldValue::Single(Value::Integer(1)),
                    );
                    all_entries[idx].1 = store.encode_doc_pub(&new_doc).unwrap();
                }
                b_write_shard(&path_b, &all_entries);
            });
        });

        // C: Per-doc compressed
        group.bench_function("C_per_doc", |b| {
            c_write_shard(&path_c, &entries);
            b.iter(|| {
                let file_data = std::fs::read(&path_c).unwrap();
                let (index, blob) = c_read_shard(&file_data);
                let merge_set: std::collections::HashSet<u32> =
                    merge_slots.iter().copied().collect();
                let mut new_entries: Vec<(u32, Vec<u8>)> = Vec::with_capacity(index.len());
                for (s, o, l) in &index {
                    let raw = c_decompress_one(&blob, *o, *l);
                    if merge_set.contains(s) {
                        let doc = store.decode_doc_pub(&raw).unwrap();
                        let mut new_doc = doc;
                        new_doc.fields.insert(
                            "availability".into(),
                            FieldValue::Single(Value::Integer(1)),
                        );
                        new_entries.push((*s, store.encode_doc_pub(&new_doc).unwrap()));
                    } else {
                        new_entries.push((*s, raw));
                    }
                }
                c_write_shard(&path_c, &new_entries);
            });
        });

        group.finish();
    }
}

// ---------------------------------------------------------------------------
// W4: First write to empty shard
// ---------------------------------------------------------------------------

fn bench_w4_fresh_write(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let docs_dir = dir.path().join("docs");
    std::fs::create_dir_all(docs_dir.join("meta")).unwrap();
    std::fs::create_dir_all(docs_dir.join("shards")).unwrap();

    let mut store = DocStore::open(&docs_dir).unwrap();
    let all_entries = encode_shard_docs(&mut store, 0);

    for n in [1, 100, 512] {
        let entries_subset: Vec<(u32, Vec<u8>)> =
            all_entries.iter().take(n).cloned().collect();

        let mut group = c.benchmark_group(format!("W4_fresh_write_N{}", n));
        group.throughput(Throughput::Elements(n as u64));

        let path_a = dir.path().join(format!("fresh_a_{}.bin", n));
        let path_b = dir.path().join(format!("fresh_b_{}.bin", n));
        let path_c = dir.path().join(format!("fresh_c_{}.bin", n));

        group.bench_function("A_compressed", |b| {
            b.iter(|| {
                a_write_shard(&path_a, &entries_subset);
            });
        });

        group.bench_function("B_uncompressed", |b| {
            b.iter(|| {
                b_write_shard(&path_b, &entries_subset);
            });
        });

        group.bench_function("C_per_doc", |b| {
            b.iter(|| {
                c_write_shard(&path_c, &entries_subset);
            });
        });

        group.finish();
    }
}

criterion_group!(
    benches,
    bench_w1_scalar_merge,
    bench_w2_tag_append,
    bench_w3_batch_merge,
    bench_w4_fresh_write,
);
criterion_main!(benches);
