//! Microbenchmark: measure mmap arena memory impact with random vs sequential writes.
//!
//! Run with: cargo test --test bench_arena_memory --release -- --nocapture
//!
//! Simulates the 60GB slot arena at reduced scale to measure page residency.

use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::time::Instant;
use roaring::RoaringBitmap;

fn get_rss_mb() -> f64 {
    #[cfg(windows)]
    {
        
        extern "system" {
            fn GetCurrentProcess() -> isize;
            fn K32GetProcessMemoryInfo(p: isize, c: *mut [u8; 80], s: u32) -> i32;
        }
        unsafe {
            let mut buf = [0u8; 80];
            K32GetProcessMemoryInfo(GetCurrentProcess(), &mut buf, 80);
            let wss = usize::from_ne_bytes(buf[24..24+8].try_into().unwrap());
            wss as f64 / (1024.0 * 1024.0)
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
fn bench_arena_random_vs_sequential() {
    let path = "tests/fixtures/tags_5m.csv";
    if !std::path::Path::new(path).exists() {
        eprintln!("Skipping: {} not found", path);
        return;
    }

    let rows = load_tags(path);
    eprintln!("Loaded {} rows", rows.len());

    // Find max slot to size the arena
    let max_slot = rows.iter().map(|&(_, s)| s).max().unwrap_or(0);
    eprintln!("Max slot: {}", max_slot);

    // Arena simulation: 512 bytes per slot, but we'll use the real max_slot range
    // At 512 bytes/slot, max_slot=124M would be 60GB. We'll use a smaller arena
    // that still shows the page residency pattern.
    //
    // For this test, use a 1GB arena to simulate the access pattern.
    let arena_slots = 2_000_000u32; // 2M slots * 512 = 1GB
    let arena_size = arena_slots as usize * 512;
    eprintln!("Arena size: {} MB ({} slots)", arena_size / (1024 * 1024), arena_slots);

    // === Test A: Random writes (tags ordered by tagId — current approach) ===
    eprintln!("\n=== Test A: Arena random writes (tagId order) ===");
    let mut arena_a = vec![0u8; arena_size];
    let rss_before = get_rss_mb();
    let start = Instant::now();
    for &(tag_id, image_id) in &rows {
        let slot = image_id % arena_slots;
        let base = slot as usize * 512;
        // Simulate writing tag_id at tag offset in slot
        let offset = base + 156; // OFF_TAG_COUNT position
        if offset + 4 <= arena_size {
            arena_a[offset..offset + 4].copy_from_slice(&(tag_id as u32).to_le_bytes());
        }
    }
    let elapsed_a = start.elapsed();
    let rss_after = get_rss_mb();
    eprintln!("  Time: {:.1}ms, RSS delta: +{:.1} MB", elapsed_a.as_millis(), rss_after - rss_before);
    drop(arena_a);

    // === Test B: Sequential writes (tags sorted by imageId) ===
    eprintln!("\n=== Test B: Arena sequential writes (imageId order) ===");
    let mut sorted_by_image = rows.clone();
    sorted_by_image.sort_unstable_by_key(|&(_, image_id)| image_id);

    let mut arena_b = vec![0u8; arena_size];
    let rss_before = get_rss_mb();
    let start = Instant::now();
    for &(tag_id, image_id) in &sorted_by_image {
        let slot = image_id % arena_slots;
        let base = slot as usize * 512;
        let offset = base + 156;
        if offset + 4 <= arena_size {
            arena_b[offset..offset + 4].copy_from_slice(&(tag_id as u32).to_le_bytes());
        }
    }
    let elapsed_b = start.elapsed();
    let rss_after = get_rss_mb();
    eprintln!("  Time: {:.1}ms, RSS delta: +{:.1} MB", elapsed_b.as_millis(), rss_after - rss_before);
    drop(arena_b);

    // === Test C: Combined — sort by imageId, sequential arena + random bitmap insert ===
    eprintln!("\n=== Test C: Sequential arena + random bitmap insert (best of both) ===");
    let mut arena_c = vec![0u8; arena_size];
    let mut map: HashMap<u64, RoaringBitmap> = HashMap::new();
    let rss_before = get_rss_mb();
    let start = Instant::now();
    for &(tag_id, image_id) in &sorted_by_image {
        // Sequential arena write
        let slot = image_id % arena_slots;
        let base = slot as usize * 512;
        let offset = base + 156;
        if offset + 4 <= arena_size {
            arena_c[offset..offset + 4].copy_from_slice(&(tag_id as u32).to_le_bytes());
        }
        // Random bitmap insert
        map.entry(tag_id).or_insert_with(RoaringBitmap::new).insert(image_id);
    }
    let elapsed_c = start.elapsed();
    let rss_after = get_rss_mb();
    let bitmaps_mb: f64 = map.values().map(|b| b.serialized_size()).sum::<usize>() as f64 / (1024.0 * 1024.0);
    eprintln!("  Time: {:.1}ms, RSS delta: +{:.1} MB, bitmaps: {:.1} MB serialized",
        elapsed_c.as_millis(), rss_after - rss_before, bitmaps_mb);
    drop(arena_c);
    drop(map);

    // === Test D: Two-pass — sorted bitmap (from_sorted_iter) + sequential arena ===
    eprintln!("\n=== Test D: Two-pass — sorted bitmap then sequential arena ===");
    // Pass 1: sort by tagId, build bitmaps
    let mut sorted_by_tag = rows.clone();
    sorted_by_tag.sort_unstable_by_key(|&(tag_id, slot)| (tag_id, slot));

    let rss_before = get_rss_mb();
    let start = Instant::now();

    let mut map2: HashMap<u64, RoaringBitmap> = HashMap::new();
    let mut i = 0;
    while i < sorted_by_tag.len() {
        let tag_id = sorted_by_tag[i].0;
        let start_idx = i;
        while i < sorted_by_tag.len() && sorted_by_tag[i].0 == tag_id { i += 1; }
        let slots: Vec<u32> = sorted_by_tag[start_idx..i].iter().map(|&(_, s)| s).collect();
        map2.insert(tag_id, RoaringBitmap::from_sorted_iter(slots.into_iter()).unwrap());
    }
    let rss_after_bitmaps = get_rss_mb();

    // Pass 2: sequential arena writes
    let mut arena_d = vec![0u8; arena_size];
    for &(tag_id, image_id) in &sorted_by_image {
        let slot = image_id % arena_slots;
        let base = slot as usize * 512;
        let offset = base + 156;
        if offset + 4 <= arena_size {
            arena_d[offset..offset + 4].copy_from_slice(&(tag_id as u32).to_le_bytes());
        }
    }
    let elapsed_d = start.elapsed();
    let rss_after = get_rss_mb();
    let bitmaps2_mb: f64 = map2.values().map(|b| b.serialized_size()).sum::<usize>() as f64 / (1024.0 * 1024.0);
    eprintln!("  Bitmap pass RSS: +{:.1} MB ({:.1} MB serialized)", rss_after_bitmaps - rss_before, bitmaps2_mb);
    eprintln!("  Total time: {:.1}ms, Final RSS delta: +{:.1} MB", elapsed_d.as_millis(), rss_after - rss_before);

    eprintln!("\n=== Comparison ===");
    eprintln!("  A (random arena):          {:.1}ms", elapsed_a.as_millis());
    eprintln!("  B (sequential arena):      {:.1}ms", elapsed_b.as_millis());
    eprintln!("  C (seq arena + rnd bitmap): {:.1}ms", elapsed_c.as_millis());
    eprintln!("  D (two-pass sorted):       {:.1}ms", elapsed_d.as_millis());
}
