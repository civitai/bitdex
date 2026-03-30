//! Experiment 4: Frozen bitmap replay harness with real Civitai data.
//!
//! Loads real filter bitmaps from ShardStore baseline-bench data,
//! converts to frozen format, and replays realistic query patterns
//! comparing: frozen direct ops vs to_owned() vs heap-resident.
//!
//! Run: cargo bench --bench frozen_replay --features data-silo

use roaring::RoaringBitmap;
use roaring::bitmap::frozen::FrozenRoaringBitmap;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Read as IoRead, Write as IoWrite};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const SHARD_MAGIC: &[u8; 4] = b"BDSS";
const HEADER_SIZE: usize = 28;

fn percentile(sorted: &[Duration], pct: f64) -> Duration {
    if sorted.is_empty() { return Duration::ZERO; }
    let idx = ((sorted.len() as f64 * pct / 100.0) as usize).min(sorted.len() - 1);
    sorted[idx]
}

/// Parse a BucketSnapshot from raw bytes (after ShardStore header).
/// Format: [u32 num_values][N × (u64 value_id, u32 offset, u32 length)][packed bitmaps]
fn parse_bucket_snapshot(data: &[u8]) -> Vec<(u64, RoaringBitmap)> {
    if data.len() < 4 { return Vec::new(); }
    let num_values = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    if num_values == 0 { return Vec::new(); }

    let index_size = num_values * 16; // 8 (value_id) + 4 (offset) + 4 (length) per entry
    if data.len() < 4 + index_size { return Vec::new(); }

    let bitmap_data_start = 4 + index_size;
    let mut result = Vec::with_capacity(num_values);

    for i in 0..num_values {
        let entry_start = 4 + i * 16;
        let value_id = u64::from_le_bytes(data[entry_start..entry_start+8].try_into().unwrap());
        let offset = u32::from_le_bytes(data[entry_start+8..entry_start+12].try_into().unwrap()) as usize;
        let length = u32::from_le_bytes(data[entry_start+12..entry_start+16].try_into().unwrap()) as usize;

        let bm_start = bitmap_data_start + offset;
        let bm_end = bm_start + length;
        if bm_end <= data.len() {
            if let Ok(bm) = RoaringBitmap::deserialize_from(&data[bm_start..bm_end]) {
                result.push((value_id, bm));
            }
        }
    }
    result
}

/// Load all bitmaps from a single shard file.
fn load_shard(path: &Path) -> Vec<(u64, RoaringBitmap)> {
    let mut data = Vec::new();
    File::open(path).unwrap().read_to_end(&mut data).unwrap();

    if data.len() < HEADER_SIZE { return Vec::new(); }
    if &data[0..4] != SHARD_MAGIC { return Vec::new(); }

    let snapshot_len = u32::from_le_bytes(data[16..20].try_into().unwrap()) as usize;
    let snapshot_end = HEADER_SIZE + snapshot_len;
    if snapshot_end > data.len() { return Vec::new(); }

    parse_bucket_snapshot(&data[HEADER_SIZE..snapshot_end])
}

/// Load all bitmaps for a filter field (may be split across multiple shard files).
fn load_field(base: &Path, field: &str) -> Vec<(String, RoaringBitmap)> {
    let field_dir = base.join(field);
    let mut bitmaps = Vec::new();

    if let Ok(entries) = fs::read_dir(&field_dir) {
        for entry in entries.flatten() {
            if entry.path().extension().map(|e| e == "shard").unwrap_or(false) {
                let shards = load_shard(&entry.path());
                for (value_id, bm) in shards {
                    if !bm.is_empty() {
                        bitmaps.push((format!("{}={}", field, value_id), bm));
                    }
                }
            }
        }
    }
    bitmaps
}

/// 32-byte aligned buffer for frozen serialization.
struct AlignedBuf {
    _backing: Vec<u8>,
    ptr: *mut u8,
    len: usize,
}

unsafe impl Send for AlignedBuf {}
unsafe impl Sync for AlignedBuf {}

impl AlignedBuf {
    fn new(size: usize) -> Self {
        let mut backing = vec![0u8; size + 32];
        let base = backing.as_ptr() as usize;
        let offset = (32 - (base % 32)) % 32;
        let ptr = backing[offset..].as_mut_ptr();
        assert!(ptr as usize % 32 == 0);
        AlignedBuf { _backing: backing, ptr, len: size }
    }

    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

/// Realistic Civitai query patterns (based on production trace analysis).
/// Each query is a list of field filters that get ANDed together.
fn build_query_plans(
    field_bitmaps: &HashMap<String, Vec<usize>>, // field -> indices into flat bitmap vec
    total_queries: usize,
) -> Vec<Vec<usize>> {
    // Realistic query patterns:
    // Pattern A (60%): nsfwLevel + type + isPublished (3 ANDs)
    // Pattern B (25%): nsfwLevel + type + isPublished + hasMeta + onSite (5 ANDs)
    // Pattern C (10%): nsfwLevel + type + userId + isPublished + minor (5 ANDs, includes sparse)
    // Pattern D (5%):  8-filter complex query

    let patterns: Vec<Vec<&str>> = vec![
        vec!["nsfwLevel", "type", "isPublished"],
        vec!["nsfwLevel", "type", "isPublished"],
        vec!["nsfwLevel", "type", "isPublished"],
        vec!["nsfwLevel", "type", "isPublished"],
        vec!["nsfwLevel", "type", "isPublished"],
        vec!["nsfwLevel", "type", "isPublished"],
        vec!["nsfwLevel", "type", "isPublished", "hasMeta", "onSite"],
        vec!["nsfwLevel", "type", "isPublished", "hasMeta", "onSite"],
        vec!["nsfwLevel", "type", "isPublished", "hasMeta"],
        vec!["nsfwLevel", "type", "userId", "isPublished", "minor"],
        vec!["nsfwLevel", "type", "isPublished", "hasMeta", "onSite", "minor", "poi", "availability"],
    ];

    let mut rng: u64 = 0xDEADBEEF;
    let mut plans = Vec::with_capacity(total_queries);

    for _ in 0..total_queries {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        let pattern = &patterns[(rng >> 32) as usize % patterns.len()];

        let mut plan = Vec::new();
        for &field in pattern {
            if let Some(indices) = field_bitmaps.get(field) {
                if !indices.is_empty() {
                    rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                    let idx = indices[(rng >> 32) as usize % indices.len()];
                    plan.push(idx);
                }
            }
        }

        if plan.len() >= 2 {
            plans.push(plan);
        }
    }
    plans
}

fn main() {
    // Try worktree-local first, then main repo
    let shard_base = {
        let local = PathBuf::from("data/baseline-bench/indexes/civitai/bitmaps/shardstore/filter/gen_000/filter");
        if local.exists() {
            local
        } else {
            // Worktree: data lives in main repo
            PathBuf::from("C:/Dev/Repos/open-source/bitdex-v2/data/baseline-bench/indexes/civitai/bitmaps/shardstore/filter/gen_000/filter")
        }
    };

    if !shard_base.exists() {
        eprintln!("ERROR: baseline-bench data not found at {}", shard_base.display());
        eprintln!("Run a baseline dump first to populate data/baseline-bench/");
        std::process::exit(1);
    }

    println!("=== Experiment 4: Frozen Bitmap Replay with Real Civitai Data ===");
    println!();

    // Load real bitmaps from ShardStore
    let fields_to_load = [
        "nsfwLevel", "type", "isPublished", "hasMeta", "onSite",
        "minor", "poi", "availability", "userId",
    ];

    let mut all_bitmaps: Vec<(String, RoaringBitmap)> = Vec::new();
    let mut field_indices: HashMap<String, Vec<usize>> = HashMap::new();

    print!("Loading bitmaps from shardstore...");
    for &field in &fields_to_load {
        let bitmaps = load_field(&shard_base, field);
        let start_idx = all_bitmaps.len();
        let count = bitmaps.len();
        let total_bits: u64 = bitmaps.iter().map(|(_, bm)| bm.len()).sum();

        let indices: Vec<usize> = (start_idx..start_idx + count).collect();
        field_indices.insert(field.to_string(), indices);
        all_bitmaps.extend(bitmaps);

        if count > 0 {
            println!("\n  {}: {} values, {} total bits", field, count, total_bits);
        }
    }

    let num_bitmaps = all_bitmaps.len();
    println!("\nTotal: {} bitmaps loaded", num_bitmaps);

    if num_bitmaps < 5 {
        eprintln!("ERROR: Too few bitmaps loaded. Check shard data.");
        std::process::exit(1);
    }

    // Convert to frozen format
    print!("Serializing to frozen format...");
    let frozen_dir = PathBuf::from("data/silo-experiment4");
    fs::create_dir_all(&frozen_dir).unwrap();

    let mut frozen_bufs: Vec<AlignedBuf> = Vec::with_capacity(num_bitmaps);
    let mut total_frozen_bytes: usize = 0;
    let mut total_heap_bytes: usize = 0;

    for (_, bm) in &all_bitmaps {
        let frozen_size = bm.frozen_serialized_size();
        let mut buf = AlignedBuf::new(frozen_size);
        bm.serialize_frozen_into(buf.as_mut_slice()).unwrap();
        total_frozen_bytes += frozen_size;
        total_heap_bytes += bm.serialized_size();
        frozen_bufs.push(buf);
    }
    println!(" done");
    println!("  Frozen total: {:.1} MB", total_frozen_bytes as f64 / 1_048_576.0);
    println!("  Heap total: {:.1} MB", total_heap_bytes as f64 / 1_048_576.0);
    println!();

    // Parse frozen views
    let frozen_views: Vec<FrozenRoaringBitmap<'_>> = frozen_bufs.iter()
        .map(|buf| FrozenRoaringBitmap::view(buf.as_slice()).unwrap())
        .collect();

    // Build query plans
    let total_queries = 1000;
    let query_plans = build_query_plans(&field_indices, total_queries);
    println!("Generated {} query plans", query_plans.len());

    // Analyze query complexity
    let mut complexity_hist: HashMap<usize, usize> = HashMap::new();
    for plan in &query_plans {
        *complexity_hist.entry(plan.len()).or_insert(0) += 1;
    }
    let mut sorted_hist: Vec<_> = complexity_hist.into_iter().collect();
    sorted_hist.sort();
    for (ands, count) in &sorted_hist {
        println!("  {} ANDs: {} queries", ands, count);
    }
    println!();

    // === Test 1: Frozen direct ops ===
    print!("Running frozen direct ops...");
    let frozen_latencies: Vec<Duration> = query_plans.iter().map(|plan| {
        let start = Instant::now();
        let mut result = &frozen_views[plan[0]] & &frozen_views[plan[1]];
        for &idx in &plan[2..] {
            result = &frozen_views[idx] & &result;
        }
        let _ = result.len();
        start.elapsed()
    }).collect();
    println!(" done");

    // === Test 2: Frozen to_owned() ===
    print!("Running frozen to_owned()...");
    let toowned_latencies: Vec<Duration> = query_plans.iter().map(|plan| {
        let start = Instant::now();
        let mut result = frozen_views[plan[0]].to_owned();
        for &idx in &plan[1..] {
            let owned = frozen_views[idx].to_owned();
            result &= owned;
        }
        let _ = result.len();
        start.elapsed()
    }).collect();
    println!(" done");

    // === Test 3: Heap-resident ===
    print!("Running heap-resident...");
    let heap_bitmaps: Vec<&RoaringBitmap> = all_bitmaps.iter().map(|(_, bm)| bm).collect();
    let heap_latencies: Vec<Duration> = query_plans.iter().map(|plan| {
        let start = Instant::now();
        let mut result = heap_bitmaps[plan[0]].clone();
        for &idx in &plan[1..] {
            result &= heap_bitmaps[idx];
        }
        let _ = result.len();
        start.elapsed()
    }).collect();
    println!(" done");

    // === Results ===
    println!();
    println!("=== Results (single-threaded, {} queries) ===", query_plans.len());
    println!();

    let mut sf = frozen_latencies.clone(); sf.sort();
    let mut st = toowned_latencies.clone(); st.sort();
    let mut sh = heap_latencies.clone(); sh.sort();

    println!("  {:>12}  {:>10}  {:>10}  {:>10}", "Method", "p50", "p95", "p99");
    println!("  {}", "-".repeat(48));
    println!("  {:>12}  {:>8.0}μs  {:>8.0}μs  {:>8.0}μs", "Direct",
        percentile(&sf, 50.0).as_nanos() as f64 / 1000.0,
        percentile(&sf, 95.0).as_nanos() as f64 / 1000.0,
        percentile(&sf, 99.0).as_nanos() as f64 / 1000.0,
    );
    println!("  {:>12}  {:>8.0}μs  {:>8.0}μs  {:>8.0}μs", "to_owned()",
        percentile(&st, 50.0).as_nanos() as f64 / 1000.0,
        percentile(&st, 95.0).as_nanos() as f64 / 1000.0,
        percentile(&st, 99.0).as_nanos() as f64 / 1000.0,
    );
    println!("  {:>12}  {:>8.0}μs  {:>8.0}μs  {:>8.0}μs", "Heap",
        percentile(&sh, 50.0).as_nanos() as f64 / 1000.0,
        percentile(&sh, 95.0).as_nanos() as f64 / 1000.0,
        percentile(&sh, 99.0).as_nanos() as f64 / 1000.0,
    );

    // Ratios
    let direct_p50 = percentile(&sf, 50.0).as_nanos() as f64;
    let toowned_p50 = percentile(&st, 50.0).as_nanos() as f64;
    let heap_p50 = percentile(&sh, 50.0).as_nanos() as f64;

    println!();
    println!("  Ratios (p50):");
    println!("    Direct vs to_owned: {:.2}x faster", toowned_p50 / direct_p50);
    println!("    Direct vs Heap:     {:.2}x", direct_p50 / heap_p50);
    println!("    to_owned vs Heap:   {:.2}x", toowned_p50 / heap_p50);

    // Per-complexity breakdown
    println!();
    println!("  Per-complexity breakdown (p50):");
    println!("  {:>6}  {:>10}  {:>10}  {:>10}", "ANDs", "Direct", "to_owned", "Heap");
    println!("  {}", "-".repeat(42));

    for &(num_ands, _) in &sorted_hist {
        let indices: Vec<usize> = query_plans.iter().enumerate()
            .filter(|(_, p)| p.len() == num_ands)
            .map(|(i, _)| i)
            .collect();

        if indices.is_empty() { continue; }

        let mut f: Vec<Duration> = indices.iter().map(|&i| frozen_latencies[i]).collect();
        let mut t: Vec<Duration> = indices.iter().map(|&i| toowned_latencies[i]).collect();
        let mut h: Vec<Duration> = indices.iter().map(|&i| heap_latencies[i]).collect();
        f.sort(); t.sort(); h.sort();

        println!("  {:>6}  {:>8.0}μs  {:>8.0}μs  {:>8.0}μs",
            num_ands,
            percentile(&f, 50.0).as_nanos() as f64 / 1000.0,
            percentile(&t, 50.0).as_nanos() as f64 / 1000.0,
            percentile(&h, 50.0).as_nanos() as f64 / 1000.0,
        );
    }

    println!();
    println!("  Memory: Frozen=0 heap  Heap={:.1} MB in-memory", total_heap_bytes as f64 / 1_048_576.0);
    println!("  Data: {} bitmaps from real Civitai shardstore", num_bitmaps);

    // Cleanup
    let _ = fs::remove_dir_all(&frozen_dir);
}
