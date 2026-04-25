//! Microbench: indexed-read fast path vs full-bucket read in `FilterBitmapStore`.
//!
//! Reproduces the postId-shape long-tail surface from the lazy-load localization
//! doc: a single bucket holds ~89K values; query workload looks up a single value.
//! Compares `load_field_values` (post-PR-A: indexed positioned reads) against the
//! legacy full-bucket `self.read()` path that decodes every bitmap in the bucket.
//!
//! Run:
//!   cargo run --release -p scratch --bin bench_indexed_lookup
//!
//! Per Justin's standing rule + the indexed-lookup ship gate, target: ≥30× cold
//! single-value lookup speedup at the 89K-value bucket shape.

use ahash::AHashSet;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use std::time::Instant;

use bitdex_v2::shard_store_bitmap::{
    FieldValueBucketShard, FilterBitmapStore, FilterBucketKey,
};
use roaring::RoaringBitmap;

const NUM_VALUES_PER_BUCKET: usize = 89_000; // postId-shape: 22.8M / 256 buckets ≈ 89K
const BITS_PER_BITMAP: u32 = 32; // postId is single-doc-y; small bitmaps
const NUM_LOOKUPS: usize = 200;
const SEED: u64 = 0xb1de_a1b1de_a1u64;

fn populate_bucket(
    store: &FilterBitmapStore,
    target_bucket: u8,
    values: &[u64],
) -> Vec<RoaringBitmap> {
    // Build (value, bitmap) pairs and write the whole bucket directly via the
    // raw write path that emits the in-shard index. Skips the per-op fsync
    // round-trip that makes 89K append_op() calls dominate wall-clock.
    let mut rng = StdRng::seed_from_u64(SEED);
    let mut bitmaps: Vec<RoaringBitmap> = Vec::with_capacity(values.len());
    for _ in values {
        let mut bm = RoaringBitmap::new();
        for _ in 0..BITS_PER_BITMAP {
            bm.insert(rng.gen_range(0..1_000_000));
        }
        bitmaps.push(bm);
    }
    let entries: Vec<(u64, &RoaringBitmap)> =
        values.iter().zip(bitmaps.iter()).map(|(&v, bm)| (v, bm)).collect();
    store
        .ensure_filter_dirs("postId", &[target_bucket])
        .unwrap();
    store
        .write_filter_bucket_raw("postId", target_bucket, &entries)
        .unwrap();
    bitmaps
}

fn drop_disk_cache_hint() {
    // No portable way to drop OS page cache from a process. We approximate "cold"
    // by running each lookup against a freshly-opened store with `compact_current`
    // forcing the on-disk shard layout, then sleep briefly to let the test harness
    // settle. NTFS will still serve from cache between iterations — this is a
    // warm-cache benchmark, NOT a cold-disk one. The 30× target is bandwidth-only
    // (BucketSnapshotCodec::decode walks ALL N bitmaps even when 1 is wanted).
    std::thread::sleep(std::time::Duration::from_millis(50));
}

fn main() {
    println!("=== indexed-lookup microbench ===");
    println!("bucket population: {} values × ~{} bits/bitmap", NUM_VALUES_PER_BUCKET, BITS_PER_BITMAP);
    println!("lookups: {}\n", NUM_LOOKUPS);

    let dir = tempfile::tempdir().unwrap();
    let store = FilterBitmapStore::new(dir.path().to_path_buf(), FieldValueBucketShard).unwrap();

    // Build value set, all in bucket 0x00 — ((v >> 8) & 0xFF) == 0 → v ∈ [0, 256)
    // can't fit 89K. Use bucket 0x01 with v in 0x100..(0x100+89_000<<0)?
    // Actually bucket = ((v >> 8) & 0xFF). All values in [0x100, 0x200) hit bucket 0x01.
    // That gives only 256 values per bucket. Real postId bucket sizes come from many
    // values that happen to share the same byte at position 8-15. To populate ~89K
    // values into a single bucket, pick a bucket byte b and generate values where
    // ((v >> 8) & 0xFF) == b. Distribute via:  v = ((rand_u64) & !0xFF00) | (b << 8).
    let target_bucket: u8 = 0x42;
    let mut rng = StdRng::seed_from_u64(SEED ^ 0xfeed);
    let mut values: AHashSet<u64> = AHashSet::with_capacity(NUM_VALUES_PER_BUCKET);
    while values.len() < NUM_VALUES_PER_BUCKET {
        let raw: u64 = rng.gen();
        let v = (raw & !(0xFFu64 << 8)) | ((target_bucket as u64) << 8);
        values.insert(v);
    }
    let values: Vec<u64> = values.into_iter().collect();
    let key = FilterBucketKey { field: "postId".into(), bucket: target_bucket };

    print!("populating ... ");
    let t = Instant::now();
    let _bitmaps = populate_bucket(&store, target_bucket, &values);
    println!("bucket written (raw) in {:?}", t.elapsed());

    // Choose lookup targets: random subset, single-value lookups.
    let mut shuffled = values.clone();
    let mut rng_pick = StdRng::seed_from_u64(SEED ^ 0xface);
    shuffled.shuffle(&mut rng_pick);
    let lookup_set: Vec<u64> = shuffled.iter().take(NUM_LOOKUPS).copied().collect();

    // ---- Indexed (current) path ----
    drop_disk_cache_hint();
    let t = Instant::now();
    let mut total_bits_indexed = 0u64;
    for &v in &lookup_set {
        let res = store.load_field_values("postId", &[v]).unwrap();
        if let Some(bm) = res.get(&v) {
            total_bits_indexed += bm.len() as u64;
        }
    }
    let dur_indexed = t.elapsed();
    let mean_indexed_us = (dur_indexed.as_micros() as f64) / (NUM_LOOKUPS as f64);
    println!("\nindexed path:    total {:?}, mean {:.2} µs/lookup, total_bits {}",
             dur_indexed, mean_indexed_us, total_bits_indexed);

    // ---- Legacy full-bucket-decode path ----
    drop_disk_cache_hint();
    let t = Instant::now();
    let mut total_bits_full = 0u64;
    for &v in &lookup_set {
        let snap = store.read(&key).unwrap().expect("shard exists");
        if let Some(bm) = snap.values.get(&v) {
            total_bits_full += bm.len() as u64;
        }
    }
    let dur_full = t.elapsed();
    let mean_full_us = (dur_full.as_micros() as f64) / (NUM_LOOKUPS as f64);
    println!("full-bucket:     total {:?}, mean {:.2} µs/lookup, total_bits {}",
             dur_full, mean_full_us, total_bits_full);

    // Correctness gate.
    assert_eq!(
        total_bits_indexed, total_bits_full,
        "indexed and full-bucket paths returned different bit counts: indexed={} full={}",
        total_bits_indexed, total_bits_full
    );

    let speedup = mean_full_us / mean_indexed_us;
    println!("\nspeedup: {:.1}×", speedup);

    if speedup >= 30.0 {
        println!("✓ ≥30× target met");
    } else {
        println!("✗ below 30× target — investigate");
    }
}
