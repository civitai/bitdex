//! Microbenchmark: measure memory usage for different bitmap construction strategies.
//!
//! Run with: cargo test --test bench_memory --release -- --nocapture
//!
//! Tests:
//! 1. Random insert into HashMap<u64, RoaringBitmap> (current approach)
//! 2. Sorted insert via from_sorted_iter
//! 3. With vs without mmap arena simulation

use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::time::Instant;
use roaring::RoaringBitmap;

fn get_rss_mb() -> f64 {
    // Windows: use GetProcessMemoryInfo
    #[cfg(windows)]
    {
        use std::mem;
        extern "system" {
            fn GetCurrentProcess() -> isize;
            fn K32GetProcessMemoryInfo(
                process: isize,
                ppsmemcounters: *mut ProcessMemoryCounters,
                cb: u32,
            ) -> i32;
        }
        #[repr(C)]
        struct ProcessMemoryCounters {
            cb: u32,
            page_fault_count: u32,
            peak_working_set_size: usize,
            working_set_size: usize,
            quota_peak_paged_pool_usage: usize,
            quota_paged_pool_usage: usize,
            quota_peak_non_paged_pool_usage: usize,
            quota_non_paged_pool_usage: usize,
            pagefile_usage: usize,
            peak_pagefile_usage: usize,
        }
        unsafe {
            let mut pmc: ProcessMemoryCounters = mem::zeroed();
            pmc.cb = mem::size_of::<ProcessMemoryCounters>() as u32;
            K32GetProcessMemoryInfo(GetCurrentProcess(), &mut pmc, pmc.cb);
            pmc.working_set_size as f64 / (1024.0 * 1024.0)
        }
    }
    #[cfg(not(windows))]
    { 0.0 }
}

fn load_tags(path: &str) -> Vec<(u64, u32)> {
    let file = std::fs::File::open(path).expect("open tags csv");
    let reader = BufReader::with_capacity(4 * 1024 * 1024, file);
    let mut rows = Vec::new();
    for line in reader.lines() {
        let line = line.unwrap();
        if line.is_empty() { continue; }
        let mut parts = line.splitn(2, ',');
        let tag_id: u64 = parts.next().unwrap().parse().unwrap();
        let image_id: u32 = parts.next().unwrap().trim().parse().unwrap();
        rows.push((tag_id, image_id));
    }
    rows
}

#[test]
fn bench_tag_bitmap_memory() {
    let path = "tests/fixtures/tags_5m.csv";
    if !std::path::Path::new(path).exists() {
        eprintln!("Skipping: {} not found", path);
        return;
    }

    eprintln!("\n=== Loading tags CSV ===");
    let rss_before_load = get_rss_mb();
    let rows = load_tags(path);
    let rss_after_load = get_rss_mb();
    eprintln!("Loaded {} rows, RSS: {:.1} MB -> {:.1} MB (+{:.1} MB)",
        rows.len(), rss_before_load, rss_after_load, rss_after_load - rss_before_load);

    // Count distinct tags
    let mut tag_counts: HashMap<u64, usize> = HashMap::new();
    for &(tag_id, _) in &rows {
        *tag_counts.entry(tag_id).or_default() += 1;
    }
    eprintln!("Distinct tags: {}, avg images/tag: {:.0}",
        tag_counts.len(), rows.len() as f64 / tag_counts.len() as f64);

    // === Test 1: Random insert (current approach) ===
    eprintln!("\n=== Test 1: Random insert (current approach) ===");
    let rss_before = get_rss_mb();
    let start = Instant::now();
    let mut map1: HashMap<u64, RoaringBitmap> = HashMap::new();
    for &(tag_id, slot) in &rows {
        map1.entry(tag_id).or_insert_with(RoaringBitmap::new).insert(slot);
    }
    let elapsed1 = start.elapsed();
    let rss_after = get_rss_mb();
    let total_card: u64 = map1.values().map(|b| b.len()).sum();
    let total_bytes: usize = map1.values().map(|b| b.serialized_size()).sum();
    eprintln!("  Time: {:.1}ms, Rate: {:.0} rows/s",
        elapsed1.as_millis(), rows.len() as f64 / elapsed1.as_secs_f64());
    eprintln!("  RSS: {:.1} MB -> {:.1} MB (+{:.1} MB)",
        rss_before, rss_after, rss_after - rss_before);
    eprintln!("  Bitmaps: {} entries, total cardinality: {}, serialized: {:.1} MB",
        map1.len(), total_card, total_bytes as f64 / (1024.0 * 1024.0));
    drop(map1);

    // === Test 2: Pre-sort by tag_id, then from_sorted_iter ===
    eprintln!("\n=== Test 2: Sort by tagId + from_sorted_iter ===");
    let mut sorted = rows.clone();
    sorted.sort_unstable_by_key(|&(tag_id, slot)| (tag_id, slot));
    let rss_before = get_rss_mb();
    let start = Instant::now();
    let mut map2: HashMap<u64, RoaringBitmap> = HashMap::new();
    let mut i = 0;
    while i < sorted.len() {
        let tag_id = sorted[i].0;
        let start_idx = i;
        while i < sorted.len() && sorted[i].0 == tag_id {
            i += 1;
        }
        let slots: Vec<u32> = sorted[start_idx..i].iter().map(|&(_, s)| s).collect();
        map2.insert(tag_id, RoaringBitmap::from_sorted_iter(slots.into_iter()).unwrap());
    }
    let elapsed2 = start.elapsed();
    let rss_after = get_rss_mb();
    let total_card2: u64 = map2.values().map(|b| b.len()).sum();
    let total_bytes2: usize = map2.values().map(|b| b.serialized_size()).sum();
    eprintln!("  Time: {:.1}ms, Rate: {:.0} rows/s",
        elapsed2.as_millis(), rows.len() as f64 / elapsed2.as_secs_f64());
    eprintln!("  RSS: {:.1} MB -> {:.1} MB (+{:.1} MB)",
        rss_before, rss_after, rss_after - rss_before);
    eprintln!("  Bitmaps: {} entries, total cardinality: {}, serialized: {:.1} MB",
        map2.len(), total_card2, total_bytes2 as f64 / (1024.0 * 1024.0));
    drop(map2);

    // === Test 3: Simulate mmap arena random writes ===
    eprintln!("\n=== Test 3: Random mmap write simulation (64MB arena) ===");
    // Simulate arena with a Vec<u8> (64MB to represent a fraction of the 60GB arena)
    let arena_size = 64 * 1024 * 1024; // 64MB
    let rss_before = get_rss_mb();
    let mut arena = vec![0u8; arena_size];
    let rss_after_alloc = get_rss_mb();
    let start = Instant::now();
    // Random writes to simulate arena.write_tags(slot, &[tag_id])
    for &(tag_id, slot) in &rows {
        let offset = (slot as usize * 8) % arena_size;
        let bytes = tag_id.to_le_bytes();
        if offset + 8 <= arena_size {
            arena[offset..offset + 8].copy_from_slice(&bytes);
        }
    }
    let elapsed3 = start.elapsed();
    let rss_after = get_rss_mb();
    eprintln!("  Arena alloc RSS: +{:.1} MB", rss_after_alloc - rss_before);
    eprintln!("  After random writes RSS: +{:.1} MB (total {:.1} MB)",
        rss_after - rss_before, rss_after);
    eprintln!("  Time for random writes: {:.1}ms", elapsed3.as_millis());
    drop(arena);

    // === Summary ===
    eprintln!("\n=== Summary ===");
    eprintln!("  Random insert:    {:.0} rows/s, {:.1}ms", rows.len() as f64 / elapsed1.as_secs_f64(), elapsed1.as_millis());
    eprintln!("  Sorted iter:      {:.0} rows/s, {:.1}ms", rows.len() as f64 / elapsed2.as_secs_f64(), elapsed2.as_millis());
    eprintln!("  Speedup: {:.1}x", elapsed1.as_secs_f64() / elapsed2.as_secs_f64());
}
