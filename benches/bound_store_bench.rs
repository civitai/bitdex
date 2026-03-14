//! Microbenchmarks for BoundStore (unified cache persistence).
//!
//! Tests the core I/O and data structure operations in isolation:
//! 1. Shard serialize/deserialize round-trip (CPU packing cost)
//! 2. Meta file round-trip (registration persistence overhead)
//! 3. Meta-index lookup (entries_for_filter_field at scale)
//! 4. Fragmented shard rewrite (tombstone cleanup cost)
//!
//! Run: cargo bench --bench bound_store_bench

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use rand::Rng;
use roaring::RoaringBitmap;

use bitdex_v2::bound_store::{BoundStore, MetaEntry, MetaFile, ShardEntry, ShardKey};
use bitdex_v2::cache::CanonicalClause;
use bitdex_v2::meta_index::MetaIndex;
use bitdex_v2::query::SortDirection;

// ── Data Generation ─────────────────────────────────────────────────────────

fn make_clause(field: &str, value: u64) -> CanonicalClause {
    CanonicalClause {
        field: field.to_string(),
        op: "eq".to_string(),
        value_repr: value.to_string(),
    }
}

/// Generate a shard with `n` entries, each having a bitmap with `bitmap_card` slots.
fn generate_shard_entries(n: usize, bitmap_card: u32) -> Vec<ShardEntry> {
    let mut rng = rand::thread_rng();
    let mut entries = Vec::with_capacity(n);
    for i in 0..n {
        let mut bm = RoaringBitmap::new();
        for _ in 0..bitmap_card {
            bm.insert(rng.gen_range(0..10_000_000));
        }
        let nsfw = (i % 5) + 1;
        entries.push(ShardEntry {
            entry_id: i as u32,
            filter_clauses: vec![
                make_clause("nsfwLevel", nsfw as u64),
                make_clause("category", (i % 3 + 1) as u64),
            ],
            bitmap: bm,
        });
    }
    entries
}

/// Generate meta entries matching shard entries.
fn generate_meta_entries(n: usize) -> Vec<MetaEntry> {
    let sort_fields = ["reactionCount", "commentCount", "sortAt", "collectedCount"];
    let mut entries = Vec::with_capacity(n);
    for i in 0..n {
        let sf = sort_fields[i % sort_fields.len()];
        let dir = if i % 2 == 0 { SortDirection::Desc } else { SortDirection::Asc };
        entries.push(MetaEntry {
            entry_id: i as u32,
            sort_field: sf.to_string(),
            direction: dir,
            filter_clauses: vec![
                make_clause("nsfwLevel", (i % 5 + 1) as u64),
            ],
            capacity: 4000,
            max_capacity: 64000,
            min_tracked_value: 500,
            total_matched: 10000,
            has_more: true,
        });
    }
    entries
}

// ── Benchmarks ──────────────────────────────────────────────────────────────

fn bench_shard_round_trip(c: &mut Criterion) {
    let mut group = c.benchmark_group("shard_round_trip");

    for &entry_count in &[100, 1000, 5000] {
        let entries = generate_shard_entries(entry_count, 4000);
        let key = ShardKey::new("reactionCount".into(), SortDirection::Desc);

        group.bench_function(BenchmarkId::new("serialize", entry_count), |b| {
            let dir = tempfile::tempdir().unwrap();
            let store = BoundStore::new(&dir.path().join("bounds")).unwrap();
            b.iter(|| {
                store.write_shard(&key, &entries).unwrap();
            });
        });

        // Pre-write for deserialize bench
        let dir = tempfile::tempdir().unwrap();
        let store = BoundStore::new(&dir.path().join("bounds")).unwrap();
        store.write_shard(&key, &entries).unwrap();

        group.bench_function(BenchmarkId::new("deserialize", entry_count), |b| {
            b.iter(|| {
                store.load_shard(&key).unwrap().unwrap();
            });
        });

        // Report size
        let path = dir.path().join("bounds").join(key.filename());
        let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        println!(
            "  shard {entry_count} entries: {} bytes ({} bytes/entry)",
            size,
            size / entry_count as u64,
        );
    }
    group.finish();
}

fn bench_meta_round_trip(c: &mut Criterion) {
    let mut group = c.benchmark_group("meta_round_trip");

    for &entry_count in &[100, 1000, 10000] {
        let entries = generate_meta_entries(entry_count);
        let meta = MetaFile {
            entries: entries.clone(),
            tombstones: RoaringBitmap::new(),
            next_entry_id: entry_count as u32,
        };

        group.bench_function(BenchmarkId::new("write", entry_count), |b| {
            let dir = tempfile::tempdir().unwrap();
            let store = BoundStore::new(&dir.path().join("bounds")).unwrap();
            b.iter(|| {
                store.write_meta(&meta).unwrap();
            });
        });

        let dir = tempfile::tempdir().unwrap();
        let store = BoundStore::new(&dir.path().join("bounds")).unwrap();
        store.write_meta(&meta).unwrap();

        group.bench_function(BenchmarkId::new("load", entry_count), |b| {
            b.iter(|| {
                store.load_meta().unwrap().unwrap();
            });
        });

        // Report size
        let path = dir.path().join("bounds").join("meta.bin");
        let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        println!(
            "  meta {entry_count} entries: {} bytes ({} bytes/entry)",
            size,
            size / entry_count as u64,
        );
    }
    group.finish();
}

fn bench_meta_index_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("meta_index_lookup");

    // Build a meta-index with many registrations
    for &entry_count in &[1000, 10000, 100000] {
        let mut mi = MetaIndex::new();
        let filter_fields = ["nsfwLevel", "category", "onSite", "type", "baseModel"];
        let sort_fields = ["reactionCount", "commentCount", "sortAt", "collectedCount"];

        for i in 0..entry_count {
            let ff = filter_fields[i % filter_fields.len()];
            let sf = sort_fields[i % sort_fields.len()];
            let dir = if i % 2 == 0 { SortDirection::Desc } else { SortDirection::Asc };
            mi.register(
                &[make_clause(ff, (i % 10 + 1) as u64)],
                Some(sf),
                Some(dir),
            );
        }

        // Lookup by filter field (write-path operation)
        group.bench_function(
            BenchmarkId::new("entries_for_filter_field", entry_count),
            |b| {
                let mut idx = 0usize;
                b.iter(|| {
                    let field = filter_fields[idx % filter_fields.len()];
                    let _ = mi.entries_for_filter_field(field);
                    idx += 1;
                });
            },
        );

        // Lookup by sort field (write-path operation)
        group.bench_function(
            BenchmarkId::new("entries_for_sort_field", entry_count),
            |b| {
                let mut idx = 0usize;
                b.iter(|| {
                    let field = sort_fields[idx % sort_fields.len()];
                    let _ = mi.entries_for_sort_field(field);
                    idx += 1;
                });
            },
        );
    }
    group.finish();
}

fn bench_fragmented_shard_rewrite(c: &mut Criterion) {
    let mut group = c.benchmark_group("fragmented_shard_rewrite");

    let entry_count = 1000;
    let entries = generate_shard_entries(entry_count, 4000);
    let key = ShardKey::new("reactionCount".into(), SortDirection::Desc);

    // Benchmark rewrite at different tombstone ratios
    for &tombstone_pct in &[0, 25, 50, 75] {
        let dir = tempfile::tempdir().unwrap();
        let store = BoundStore::new(&dir.path().join("bounds")).unwrap();

        // Write initial shard
        store.write_shard(&key, &entries).unwrap();

        // Build the "live" subset (simulates read-modify-write)
        let live_count = entry_count * (100 - tombstone_pct) / 100;
        let live_entries: Vec<ShardEntry> = entries[..live_count].to_vec();

        group.bench_function(
            BenchmarkId::new("rewrite", format!("{tombstone_pct}%_dead")),
            |b| {
                b.iter(|| {
                    // Read old shard
                    let _old = store.load_shard(&key).unwrap().unwrap();
                    // Write merged (live-only) shard
                    store.write_shard(&key, &live_entries).unwrap();
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_shard_round_trip,
    bench_meta_round_trip,
    bench_meta_index_lookup,
    bench_fragmented_shard_rewrite,
);
criterion_main!(benches);
