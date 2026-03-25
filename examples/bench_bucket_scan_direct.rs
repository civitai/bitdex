//! Direct benchmark: full rebuild vs incremental scan on real 105M sort bitmaps.
//!
//! Usage: cargo run --release --example bench_bucket_scan_direct

use std::time::Instant;
use roaring::RoaringBitmap;

fn main() {
    let base = r"C:\Dev\Repos\open-source\bitdex-v2\data\indexes\civitai\bitmaps";
    let sort_path = format!("{}/sort/sortAt.sort", base);
    let alive_path = format!("{}/system/alive.roar", base);

    // Load alive bitmap
    eprintln!("Loading alive bitmap...");
    let alive_data = std::fs::read(&alive_path).expect("read alive.roar");
    let alive = RoaringBitmap::deserialize_from(&alive_data[..]).expect("deserialize alive");
    let alive_count = alive.len();
    eprintln!("  Alive: {} slots", alive_count);

    // Load sort layers (32 layers for u32)
    eprintln!("Loading sortAt sort layers...");
    let sort_data = std::fs::read(&sort_path).expect("read sortAt.sort");
    let layers = load_sort_layers(&sort_data, 32);
    eprintln!("  Loaded {} layers", layers.len());

    // Sample to find time range
    let now_secs: u64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // Reconstruct a few values to understand the range
    let mut sample_vals = Vec::new();
    let mut count = 0;
    for slot in alive.iter() {
        sample_vals.push(reconstruct_value(&layers, slot));
        count += 1;
        if count >= 100 { break; }
    }
    let max_val = *sample_vals.iter().max().unwrap_or(&0);
    eprintln!("  Sample max value: {} (now_secs: {})", max_val, now_secs);

    // Use the max sample value as "now" for the bucket
    let bucket_now = max_val as u64;
    let bucket_duration: u64 = 86400; // 24h
    let refresh_interval: u64 = 300;  // 5min
    let cutoff = bucket_now.saturating_sub(bucket_duration);

    eprintln!("\nBucket config: duration={}s, refresh={}s", bucket_duration, refresh_interval);
    eprintln!("  Cutoff: {} (bucket_now - duration)", cutoff);

    // ── Approach 1: Full rebuild ──
    // Iterate ALL alive slots, reconstruct value, build complete bucket bitmap
    eprintln!("\n=== Approach 1: Full Rebuild (all alive slots) ===");
    let t1 = Instant::now();
    let mut full_bucket = RoaringBitmap::new();
    for slot in alive.iter() {
        let val = reconstruct_value(&layers, slot) as u64;
        if val >= cutoff && val <= bucket_now {
            full_bucket.insert(slot);
        }
    }
    let full_elapsed = t1.elapsed();
    let bucket_count = full_bucket.len();
    eprintln!("  Time: {:?}", full_elapsed);
    eprintln!("  Bucket size: {} slots ({:.1}% of alive)",
        bucket_count, bucket_count as f64 / alive_count as f64 * 100.0);

    // ── Approach 2: Incremental scan ──
    // Iterate the BUCKET bitmap (from approach 1), find expired slots in [cutoff, cutoff+300)
    let old_cutoff = cutoff;
    let new_cutoff = cutoff + refresh_interval; // shifted by one refresh interval
    eprintln!("\n=== Approach 2: Incremental Scan (bucket bitmap only) ===");
    eprintln!("  Finding expired slots in [{}, {})", old_cutoff, new_cutoff);
    let t2 = Instant::now();
    let mut expired = RoaringBitmap::new();
    for slot in full_bucket.iter() {
        let val = reconstruct_value(&layers, slot) as u64;
        if val >= old_cutoff && val < new_cutoff {
            expired.insert(slot);
        }
    }
    let incr_elapsed = t2.elapsed();
    let expired_count = expired.len();
    eprintln!("  Time: {:?}", incr_elapsed);
    eprintln!("  Expired slots: {} ({:.4}% of bucket)",
        expired_count, expired_count as f64 / bucket_count as f64 * 100.0);

    // ── Approach 3: Apply diff ──
    // Subtract expired from bucket (the actual operation in production)
    eprintln!("\n=== Approach 3: Apply diff (bitmap AND-NOT) ===");
    let t3 = Instant::now();
    let mut updated_bucket = full_bucket.clone();
    updated_bucket -= &expired;
    let diff_elapsed = t3.elapsed();
    eprintln!("  Time: {:?}", diff_elapsed);
    eprintln!("  Updated bucket: {} slots", updated_bucket.len());

    // ── Summary ──
    eprintln!("\n========== SUMMARY ==========");
    eprintln!("  Alive slots:        {}", alive_count);
    eprintln!("  Bucket slots:       {} ({:.1}% of alive)", bucket_count,
        bucket_count as f64 / alive_count as f64 * 100.0);
    eprintln!("  Expired slots:      {} ({:.4}% of bucket)", expired_count,
        expired_count as f64 / bucket_count.max(1) as f64 * 100.0);
    eprintln!();
    eprintln!("  Full rebuild:       {:>10?}  (scan {} alive slots)", full_elapsed, alive_count);
    eprintln!("  Incremental scan:   {:>10?}  (scan {} bucket slots)", incr_elapsed, bucket_count);
    eprintln!("  Diff application:   {:>10?}  (AND-NOT {} expired)", diff_elapsed, expired_count);
    eprintln!();
    let speedup = full_elapsed.as_secs_f64() / incr_elapsed.as_secs_f64();
    eprintln!("  Speedup (scan):     {:.2}x", speedup);
    eprintln!("  Total incremental:  {:?}  (scan + apply)", incr_elapsed + diff_elapsed);
    let total_speedup = full_elapsed.as_secs_f64() / (incr_elapsed + diff_elapsed).as_secs_f64();
    eprintln!("  Total speedup:      {:.2}x", total_speedup);
}

fn load_sort_layers(data: &[u8], num_layers: usize) -> Vec<RoaringBitmap> {
    let stored_layers = data[0] as usize;
    let header_size = 1 + stored_layers * 9;
    let data_start = header_size;
    let mut layers = vec![RoaringBitmap::new(); num_layers];

    for i in 0..stored_layers {
        let idx_offset = 1 + i * 9;
        let bit_pos = data[idx_offset] as usize;
        let offset = u32::from_le_bytes([
            data[idx_offset + 1], data[idx_offset + 2],
            data[idx_offset + 3], data[idx_offset + 4],
        ]) as usize;
        let length = u32::from_le_bytes([
            data[idx_offset + 5], data[idx_offset + 6],
            data[idx_offset + 7], data[idx_offset + 8],
        ]) as usize;

        if bit_pos < num_layers {
            let start = data_start + offset;
            let end = start + length;
            if end <= data.len() {
                layers[bit_pos] = RoaringBitmap::deserialize_from(&data[start..end])
                    .unwrap_or_default();
            }
        }
    }
    layers
}

fn reconstruct_value(layers: &[RoaringBitmap], slot: u32) -> u32 {
    let mut value: u32 = 0;
    for (bit, layer) in layers.iter().enumerate() {
        if layer.contains(slot) {
            value |= 1 << bit;
        }
    }
    value
}
