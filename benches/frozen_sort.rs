//! Experiment 5: Frozen bitmap sort layer walk (bifurcate) benchmark.
//!
//! Loads real sort bit-layers from ShardStore baseline-bench data,
//! converts to frozen format, and compares the MSB→LSB bifurcation
//! walk using frozen layers vs heap-resident layers.
//!
//! This is the critical hot path — sort queries dominate production latency.
//!
//! Run: cargo bench --bench frozen_sort --features data-silo

use roaring::RoaringBitmap;
use roaring::bitmap::frozen::FrozenRoaringBitmap;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read as IoRead;
use std::path::PathBuf;
use std::time::{Duration, Instant};

const HEADER_SIZE: usize = 28;
const SHARD_MAGIC: &[u8; 4] = b"BDSS";

fn percentile(sorted: &[Duration], pct: f64) -> Duration {
    if sorted.is_empty() { return Duration::ZERO; }
    let idx = ((sorted.len() as f64 * pct / 100.0) as usize).min(sorted.len() - 1);
    sorted[idx]
}

/// Parse a SortFieldSnapshot from raw bytes (after ShardStore header).
/// Format: [u8 num_layers][N × (u8 bit_position, u32 offset, u32 length)][packed bitmaps]
fn parse_sort_snapshot(data: &[u8]) -> BTreeMap<u8, RoaringBitmap> {
    if data.is_empty() { return BTreeMap::new(); }

    let num_layers = data[0] as usize;
    if num_layers == 0 { return BTreeMap::new(); }

    let index_start = 1;
    let index_size = num_layers * 9;
    let data_start = index_start + index_size;

    if data.len() < data_start { return BTreeMap::new(); }

    let mut layers = BTreeMap::new();

    for i in 0..num_layers {
        let entry_offset = index_start + i * 9;
        let bit_position = data[entry_offset];
        let bm_offset = u32::from_le_bytes(
            data[entry_offset + 1..entry_offset + 5].try_into().unwrap(),
        ) as usize;
        let bm_length = u32::from_le_bytes(
            data[entry_offset + 5..entry_offset + 9].try_into().unwrap(),
        ) as usize;

        let bm_start = data_start + bm_offset;
        let bm_end = bm_start + bm_length;
        if bm_end <= data.len() {
            if let Ok(bm) = RoaringBitmap::deserialize_from(&data[bm_start..bm_end]) {
                layers.insert(bit_position, bm);
            }
        }
    }
    layers
}

fn load_sort_field(path: &std::path::Path) -> BTreeMap<u8, RoaringBitmap> {
    let mut data = Vec::new();
    File::open(path).unwrap().read_to_end(&mut data).unwrap();
    if data.len() < HEADER_SIZE { return BTreeMap::new(); }
    if &data[0..4] != SHARD_MAGIC { return BTreeMap::new(); }
    let snapshot_len = u32::from_le_bytes(data[16..20].try_into().unwrap()) as usize;
    let snapshot_end = HEADER_SIZE + snapshot_len;
    if snapshot_end > data.len() { return BTreeMap::new(); }
    parse_sort_snapshot(&data[HEADER_SIZE..snapshot_end])
}

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
    fn as_slice(&self) -> &[u8] { unsafe { std::slice::from_raw_parts(self.ptr, self.len) } }
    fn as_mut_slice(&mut self) -> &mut [u8] { unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) } }
}

/// MSB→LSB bifurcate walk using heap-resident bitmaps (current production code).
fn bifurcate_heap(
    candidates: &RoaringBitmap,
    layers: &[&RoaringBitmap],
    limit: usize,
    descending: bool,
) -> RoaringBitmap {
    let total = candidates.len() as usize;
    if total <= limit { return candidates.clone(); }

    let mut result = RoaringBitmap::new();
    let mut remaining = candidates.clone();
    let mut remaining_limit = limit;

    for layer in layers.iter().rev() {
        if remaining_limit == 0 || remaining.is_empty() { break; }

        let preferred = if descending {
            &remaining & *layer
        } else {
            &remaining - *layer
        };

        let preferred_count = preferred.len() as usize;
        if preferred_count == 0 {
            continue;
        } else if preferred_count >= remaining_limit {
            remaining = preferred;
        } else {
            result |= &preferred;
            remaining -= &preferred;
            remaining_limit -= preferred_count;
        }
    }

    if remaining_limit > 0 && !remaining.is_empty() {
        let mut taken = 0;
        for slot in remaining.iter() {
            if taken >= remaining_limit { break; }
            result.insert(slot);
            taken += 1;
        }
    }
    result
}

/// MSB→LSB bifurcate walk using frozen mmap'd bitmaps.
fn bifurcate_frozen(
    candidates: &RoaringBitmap,
    layers: &[&FrozenRoaringBitmap<'_>],
    limit: usize,
    descending: bool,
) -> RoaringBitmap {
    let total = candidates.len() as usize;
    if total <= limit { return candidates.clone(); }

    let mut result = RoaringBitmap::new();
    let mut remaining = candidates.clone();
    let mut remaining_limit = limit;

    for layer in layers.iter().rev() {
        if remaining_limit == 0 || remaining.is_empty() { break; }

        let preferred = if descending {
            &remaining & *layer  // RoaringBitmap & FrozenRoaringBitmap
        } else {
            &remaining - *layer  // RoaringBitmap - FrozenRoaringBitmap
        };

        let preferred_count = preferred.len() as usize;
        if preferred_count == 0 {
            continue;
        } else if preferred_count >= remaining_limit {
            remaining = preferred;
        } else {
            result |= &preferred;
            remaining -= &preferred;
            remaining_limit -= preferred_count;
        }
    }

    if remaining_limit > 0 && !remaining.is_empty() {
        let mut taken = 0;
        for slot in remaining.iter() {
            if taken >= remaining_limit { break; }
            result.insert(slot);
            taken += 1;
        }
    }
    result
}

fn main() {
    let sort_base = {
        let local = PathBuf::from("data/baseline-bench/indexes/civitai/bitmaps/shardstore/sort/gen_000/sort");
        if local.exists() { local }
        else { PathBuf::from("C:/Dev/Repos/open-source/bitdex-v2/data/baseline-bench/indexes/civitai/bitmaps/shardstore/sort/gen_000/sort") }
    };
    let filter_base = {
        let local = PathBuf::from("data/baseline-bench/indexes/civitai/bitmaps/shardstore/filter/gen_000/filter");
        if local.exists() { local }
        else { PathBuf::from("C:/Dev/Repos/open-source/bitdex-v2/data/baseline-bench/indexes/civitai/bitmaps/shardstore/filter/gen_000/filter") }
    };

    if !sort_base.exists() {
        eprintln!("ERROR: sort shardstore data not found at {}", sort_base.display());
        std::process::exit(1);
    }

    println!("=== Experiment 5: Frozen Sort Layer Walk (Bifurcate) ===");
    println!();

    // Load sortAt field (the primary production sort field)
    print!("Loading sortAt bit layers...");
    let sort_layers = load_sort_field(&sort_base.join("sortAt.shard"));
    let num_layers = sort_layers.len();
    println!(" {} layers loaded", num_layers);

    for (&bit, bm) in &sort_layers {
        if bit == 0 || bit == (num_layers as u8 - 1) || bit == num_layers as u8 / 2 {
            println!("  bit[{:>2}]: {:>10} set ({:.1}%)",
                bit, bm.len(), bm.len() as f64 / 108_000_000.0 * 100.0);
        }
    }

    // Load a few filter bitmaps for realistic candidate sets
    print!("Loading filter bitmaps for candidate sets...");
    let mut filter_data = Vec::new();
    File::open(&filter_base.join("nsfwLevel/00.shard")).unwrap().read_to_end(&mut filter_data).unwrap();
    let snapshot_len = u32::from_le_bytes(filter_data[16..20].try_into().unwrap()) as usize;
    let nsfw_bitmaps: Vec<(u64, RoaringBitmap)> = {
        let snap_data = &filter_data[HEADER_SIZE..HEADER_SIZE + snapshot_len];
        let num_values = u32::from_le_bytes(snap_data[0..4].try_into().unwrap()) as usize;
        let mut result = Vec::new();
        let index_size = num_values * 16;
        let bitmap_data_start = 4 + index_size;
        for i in 0..num_values {
            let entry_start = 4 + i * 16;
            let value_id = u64::from_le_bytes(snap_data[entry_start..entry_start+8].try_into().unwrap());
            let offset = u32::from_le_bytes(snap_data[entry_start+8..entry_start+12].try_into().unwrap()) as usize;
            let length = u32::from_le_bytes(snap_data[entry_start+12..entry_start+16].try_into().unwrap()) as usize;
            let bm_start = bitmap_data_start + offset;
            let bm_end = bm_start + length;
            if bm_end <= snap_data.len() {
                if let Ok(bm) = RoaringBitmap::deserialize_from(&snap_data[bm_start..bm_end]) {
                    result.push((value_id, bm));
                }
            }
        }
        result
    };
    println!(" done ({} nsfwLevel values)", nsfw_bitmaps.len());

    // Build candidate sets of varying sizes
    let candidate_sets: Vec<(&str, RoaringBitmap)> = {
        let mut sets = Vec::new();
        // Broad: single nsfwLevel filter (~60M+)
        if let Some((_, bm)) = nsfw_bitmaps.iter().max_by_key(|(_, b)| b.len()) {
            sets.push(("Broad (~60M+)", bm.clone()));
        }
        // Medium: AND two nsfwLevel values (~20-40M)
        if nsfw_bitmaps.len() >= 2 {
            let medium = &nsfw_bitmaps[0].1 & &nsfw_bitmaps[1].1;
            if medium.len() > 1_000_000 {
                sets.push(("Medium (~20M)", medium));
            }
        }
        // Narrow: small candidate set (~1M)
        if let Some((_, bm)) = nsfw_bitmaps.iter().min_by_key(|(_, b)| b.len()) {
            if bm.len() > 100_000 {
                sets.push(("Narrow (~1M)", bm.clone()));
            }
        }
        sets
    };

    // Convert sort layers to frozen format
    print!("Serializing sort layers to frozen...");
    let mut frozen_bufs: Vec<(u8, AlignedBuf)> = Vec::with_capacity(num_layers);
    let mut total_frozen_bytes = 0usize;

    let ordered_layers: Vec<(u8, &RoaringBitmap)> = sort_layers.iter()
        .map(|(&bit, bm)| (bit, bm))
        .collect();

    for &(bit, bm) in &ordered_layers {
        let size = bm.frozen_serialized_size();
        let mut buf = AlignedBuf::new(size);
        bm.serialize_frozen_into(buf.as_mut_slice()).unwrap();
        total_frozen_bytes += size;
        frozen_bufs.push((bit, buf));
    }
    println!(" done ({:.1} MB)", total_frozen_bytes as f64 / 1_048_576.0);

    // Parse frozen views
    let frozen_views: Vec<(u8, FrozenRoaringBitmap<'_>)> = frozen_bufs.iter()
        .map(|(bit, buf)| (*bit, FrozenRoaringBitmap::view(buf.as_slice()).unwrap()))
        .collect();

    // Build layer refs in order
    let heap_layer_refs: Vec<&RoaringBitmap> = ordered_layers.iter().map(|(_, bm)| *bm).collect();
    let frozen_layer_refs: Vec<&FrozenRoaringBitmap<'_>> = frozen_views.iter().map(|(_, fv)| fv).collect();

    println!();

    // Run benchmarks
    let limits = [20usize, 100, 500];
    let iterations = 50;

    for (set_name, candidates) in &candidate_sets {
        println!("--- Candidates: {} ({} docs) ---", set_name, candidates.len());
        println!("  {:>6} {:>5}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>6}",
            "Dir", "Limit", "Frz p50", "Frz p95", "Heap p50", "Heap p95", "Ratio", "Verify", "Match");
        println!("  {}", "-".repeat(86));

        for &limit in &limits {
            for descending in [true, false] {
                let dir = if descending { "DESC" } else { "ASC" };

                // Frozen walk
                let mut frozen_lats: Vec<Duration> = Vec::with_capacity(iterations);
                let mut frozen_result = RoaringBitmap::new();
                for _ in 0..iterations {
                    let start = Instant::now();
                    frozen_result = bifurcate_frozen(candidates, &frozen_layer_refs, limit, descending);
                    frozen_lats.push(start.elapsed());
                }

                // Heap walk
                let mut heap_lats: Vec<Duration> = Vec::with_capacity(iterations);
                let mut heap_result = RoaringBitmap::new();
                for _ in 0..iterations {
                    let start = Instant::now();
                    heap_result = bifurcate_heap(candidates, &heap_layer_refs, limit, descending);
                    heap_lats.push(start.elapsed());
                }

                frozen_lats.sort();
                heap_lats.sort();

                let fp50 = percentile(&frozen_lats, 50.0);
                let fp95 = percentile(&frozen_lats, 95.0);
                let hp50 = percentile(&heap_lats, 50.0);
                let hp95 = percentile(&heap_lats, 95.0);
                let ratio = fp50.as_nanos() as f64 / hp50.as_nanos() as f64;
                let results_match = frozen_result == heap_result;

                println!("  {:>6} {:>5}  {:>8.0}μs  {:>8.0}μs  {:>8.0}μs  {:>8.0}μs  {:>5.2}x  {:>8}  {:>6}",
                    dir, limit,
                    fp50.as_nanos() as f64 / 1000.0,
                    fp95.as_nanos() as f64 / 1000.0,
                    hp50.as_nanos() as f64 / 1000.0,
                    hp95.as_nanos() as f64 / 1000.0,
                    ratio,
                    if results_match { "OK" } else { "MISMATCH" },
                    frozen_result.len(),
                );
            }
        }
        println!();
    }

    println!("  Sort layers: {} bits, {:.1} MB frozen, {:.1} MB heap",
        num_layers,
        total_frozen_bytes as f64 / 1_048_576.0,
        ordered_layers.iter().map(|(_, bm)| bm.serialized_size()).sum::<usize>() as f64 / 1_048_576.0,
    );
}
