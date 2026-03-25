//! Microbenchmark: time bucket full-rebuild vs incremental expired-slot scan.
//!
//! Compares two approaches for refreshing a 24h time bucket at 105M scale:
//!
//! **Full rebuild** (current): iterate ALL alive slots, reconstruct_value() on each,
//! build complete bucket bitmap. O(alive_count).
//!
//! **Incremental scan** (proposed): iterate only the EXISTING bucket bitmap,
//! reconstruct_value() on each, find slots in the narrow 300-second expired window
//! [cutoff - 300, cutoff). O(bucket_count) but returns O(expired_count).
//!
//! Usage:
//!   cargo run --release --example bench_bucket_scan -- --data-dir ./data
//!
//! The data-dir should point to a BitDex server data directory containing
//! `indexes/<name>/config.json` and `indexes/<name>/bitmaps/`.

use std::path::PathBuf;
use std::time::Instant;

use serde::Deserialize;

use bitdex_v2::concurrent_engine::ConcurrentEngine;
use bitdex_v2::config::Config;

/// Minimal deserialization target for config.json in an index directory.
/// We only need the engine Config; data_schema is ignored.
#[derive(Deserialize)]
struct IndexDefinition {
    #[allow(dead_code)]
    name: String,
    config: Config,
    // data_schema is present in the JSON but we don't need it for this benchmark.
    // serde will skip unknown fields by default.
}

fn arg_value(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|pos| args.get(pos + 1).cloned())
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let data_dir = PathBuf::from(
        arg_value(&args, "--data-dir")
            .or_else(|| arg_value(&args, "--data"))
            .unwrap_or_else(|| "data".into()),
    );

    let sort_field_name = arg_value(&args, "--sort-field")
        .unwrap_or_else(|| "sortAt".into());

    let bucket_duration_secs: u64 = arg_value(&args, "--bucket-duration")
        .and_then(|v| v.parse().ok())
        .unwrap_or(86400); // 24h

    let refresh_interval_secs: u64 = arg_value(&args, "--refresh-interval")
        .and_then(|v| v.parse().ok())
        .unwrap_or(300); // 5 min

    let num_runs: usize = arg_value(&args, "--runs")
        .and_then(|v| v.parse().ok())
        .unwrap_or(3);

    // --- Discover index ---
    let indexes_dir = data_dir.join("indexes");
    if !indexes_dir.exists() {
        eprintln!("ERROR: No indexes directory found at {}", indexes_dir.display());
        eprintln!("Expected: {}/indexes/<name>/config.json", data_dir.display());
        std::process::exit(1);
    }

    let index_dir = std::fs::read_dir(&indexes_dir)
        .expect("Failed to read indexes directory")
        .filter_map(|e| e.ok())
        .find(|e| e.path().join("config.json").exists())
        .map(|e| e.path());

    let index_dir = match index_dir {
        Some(d) => d,
        None => {
            eprintln!("ERROR: No index with config.json found in {}", indexes_dir.display());
            std::process::exit(1);
        }
    };

    let config_path = index_dir.join("config.json");
    eprintln!("Loading index from: {}", index_dir.display());

    // --- Load config ---
    let json = std::fs::read_to_string(&config_path)
        .unwrap_or_else(|e| {
            eprintln!("ERROR: Failed to read {}: {e}", config_path.display());
            std::process::exit(1);
        });

    let def: IndexDefinition = serde_json::from_str(&json)
        .unwrap_or_else(|e| {
            eprintln!("ERROR: Failed to parse config.json: {e}");
            std::process::exit(1);
        });

    // --- Build headless engine ---
    let bitmap_path = index_dir.join("bitmaps");
    let docstore_path = index_dir.join("docs");

    let mut config = def.config;
    config.storage.bitmap_path = Some(bitmap_path);
    config.headless = true;

    eprintln!("Creating headless engine...");
    let engine = ConcurrentEngine::new_with_path(config, &docstore_path)
        .unwrap_or_else(|e| {
            eprintln!("ERROR: Failed to create engine: {e}");
            std::process::exit(1);
        });

    let alive_count = engine.alive_count();
    eprintln!("Alive count: {}", alive_count);

    // --- Lazy-load the sort field ---
    eprintln!("Lazy-loading sort field '{sort_field_name}'...");
    let load_t0 = Instant::now();
    let _ = engine.ensure_fields_loaded(&[], Some(&sort_field_name));
    eprintln!("  loaded in {:.2}s", load_t0.elapsed().as_secs_f64());

    // --- Get snapshot data ---
    let snap = engine.snapshot_public();
    let sort_field = snap.sorts.get_field(&sort_field_name)
        .unwrap_or_else(|| {
            eprintln!("ERROR: Sort field '{sort_field_name}' not found in snapshot");
            eprintln!("Available sort fields:");
            // Can't list them easily, just exit
            std::process::exit(1);
        });

    let alive_bitmap = snap.slots.alive_bitmap();

    // --- Determine current time context ---
    // Use the max value in the sort field as "now" proxy (the most recent timestamp).
    // This avoids issues with the sort field storing seconds-since-epoch in u32.
    eprintln!("\nSampling sort field to determine time context...");
    let sample_t0 = Instant::now();
    let mut max_ts: u32 = 0;
    let mut min_ts: u32 = u32::MAX;
    // Sample first 10K alive slots to estimate min/max quickly
    let mut sample_count = 0u64;
    for slot in alive_bitmap.iter().take(10_000) {
        let val = sort_field.reconstruct_value(slot);
        if val > max_ts { max_ts = val; }
        if val > 0 && val < min_ts { min_ts = val; }
        sample_count += 1;
    }
    // Also sample from the end (high slot IDs tend to be recent)
    for slot in alive_bitmap.iter().rev().take(10_000) {
        let val = sort_field.reconstruct_value(slot);
        if val > max_ts { max_ts = val; }
        if val > 0 && val < min_ts { min_ts = val; }
        sample_count += 1;
    }
    eprintln!("  sampled {} slots in {:.1}ms", sample_count, sample_t0.elapsed().as_secs_f64() * 1000.0);
    eprintln!("  approx sort range: {} .. {} (span: {}s = {:.1} days)",
        min_ts, max_ts, max_ts.saturating_sub(min_ts),
        max_ts.saturating_sub(min_ts) as f64 / 86400.0);

    // Use max_ts as "now"
    let now = max_ts as u64;
    let bucket_cutoff = now.saturating_sub(bucket_duration_secs) as u32;
    let expired_window_start = bucket_cutoff.saturating_sub(refresh_interval_secs as u32);
    let expired_window_end = bucket_cutoff;

    eprintln!("\nBenchmark parameters:");
    eprintln!("  now (max_ts):          {now}");
    eprintln!("  bucket_duration:       {bucket_duration_secs}s ({:.1} days)", bucket_duration_secs as f64 / 86400.0);
    eprintln!("  refresh_interval:      {refresh_interval_secs}s");
    eprintln!("  bucket_cutoff:         {bucket_cutoff} (now - {bucket_duration_secs})");
    eprintln!("  expired_window:        [{expired_window_start}, {expired_window_end})  (300s window)");
    eprintln!("  alive_count:           {alive_count}");

    // =========================================================================
    // APPROACH 1: Full rebuild — scan all alive slots, build complete bucket bitmap
    // =========================================================================
    eprintln!("\n=== APPROACH 1: Full Rebuild (scan all alive slots) ===");

    let mut full_rebuild_times: Vec<f64> = Vec::new();
    let mut bucket_bitmap_card = 0u64;

    // Keep the bucket bitmap from the last run for approach 2
    let mut bucket_bitmap = roaring::RoaringBitmap::new();

    for run in 0..num_runs {
        let t0 = Instant::now();
        let mut bm = roaring::RoaringBitmap::new();
        for slot in alive_bitmap.iter() {
            let val = sort_field.reconstruct_value(slot);
            if val >= bucket_cutoff && (val as u64) <= now {
                bm.insert(slot);
            }
        }
        let elapsed = t0.elapsed().as_secs_f64();
        bucket_bitmap_card = bm.len();
        full_rebuild_times.push(elapsed);
        eprintln!("  run {}: {:.3}s  (bucket_size: {})", run + 1, elapsed, bucket_bitmap_card);

        if run == num_runs - 1 {
            bucket_bitmap = bm;
        }
    }

    // =========================================================================
    // APPROACH 2: Incremental scan — scan only bucket bitmap, find expired slots
    // =========================================================================
    eprintln!("\n=== APPROACH 2: Incremental Scan (scan bucket bitmap, find expired) ===");

    let mut incr_scan_times: Vec<f64> = Vec::new();
    let mut expired_count = 0u64;

    for run in 0..num_runs {
        let t0 = Instant::now();
        let expired = sort_field.slots_in_range(
            &bucket_bitmap,
            expired_window_start,
            expired_window_end,
        );
        let elapsed = t0.elapsed().as_secs_f64();
        expired_count = expired.len();
        incr_scan_times.push(elapsed);
        eprintln!("  run {}: {:.3}s  (expired_count: {})", run + 1, elapsed, expired_count);
    }

    // =========================================================================
    // APPROACH 2b: slots_in_range on alive bitmap (for reference)
    // =========================================================================
    eprintln!("\n=== APPROACH 2b: slots_in_range on alive bitmap (expired window) ===");

    let mut incr_alive_times: Vec<f64> = Vec::new();
    let mut expired_alive_count: u64;

    for run in 0..num_runs {
        let t0 = Instant::now();
        let expired = sort_field.slots_in_range(
            alive_bitmap,
            expired_window_start,
            expired_window_end,
        );
        let elapsed = t0.elapsed().as_secs_f64();
        expired_alive_count = expired.len();
        incr_alive_times.push(elapsed);
        eprintln!("  run {}: {:.3}s  (expired_count: {})", run + 1, elapsed, expired_alive_count);
    }

    // =========================================================================
    // Results
    // =========================================================================
    full_rebuild_times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    incr_scan_times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    incr_alive_times.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let full_median = full_rebuild_times[full_rebuild_times.len() / 2];
    let incr_median = incr_scan_times[incr_scan_times.len() / 2];
    let incr_alive_median = incr_alive_times[incr_alive_times.len() / 2];

    let speedup_vs_full = full_median / incr_median;
    let speedup_alive_vs_full = full_median / incr_alive_median;

    println!();
    println!("============================================================");
    println!("  TIME BUCKET REFRESH BENCHMARK RESULTS");
    println!("============================================================");
    println!();
    println!("  Dataset:");
    println!("    alive_count:        {:>12}", alive_count);
    println!("    bucket_count (24h): {:>12}", bucket_bitmap_card);
    println!("    expired_count:      {:>12}  (300s window)", expired_count);
    println!("    bucket/alive ratio: {:>11.1}%", bucket_bitmap_card as f64 / alive_count as f64 * 100.0);
    println!();
    println!("  Approach 1 — Full Rebuild (scan all alive):");
    println!("    median:  {:>8.3}s  ({} runs)", full_median, num_runs);
    println!("    min:     {:>8.3}s", full_rebuild_times[0]);
    println!("    max:     {:>8.3}s", full_rebuild_times[full_rebuild_times.len() - 1]);
    println!();
    println!("  Approach 2 — Incremental Scan (scan bucket bitmap):");
    println!("    median:  {:>8.3}s  ({} runs)", incr_median, num_runs);
    println!("    min:     {:>8.3}s", incr_scan_times[0]);
    println!("    max:     {:>8.3}s", incr_scan_times[incr_scan_times.len() - 1]);
    println!("    speedup: {:>7.1}x vs full rebuild", speedup_vs_full);
    println!();
    println!("  Approach 2b — slots_in_range on alive (expired window):");
    println!("    median:  {:>8.3}s  ({} runs)", incr_alive_median, num_runs);
    println!("    min:     {:>8.3}s", incr_alive_times[0]);
    println!("    max:     {:>8.3}s", incr_alive_times[incr_alive_times.len() - 1]);
    println!("    speedup: {:>7.1}x vs full rebuild", speedup_alive_vs_full);
    println!();
    println!("  Analysis:");
    println!("    Full rebuild scans {} slots (all alive).", alive_count);
    println!("    Incremental scans {} slots (bucket only, {:.1}% of alive).",
        bucket_bitmap_card, bucket_bitmap_card as f64 / alive_count as f64 * 100.0);
    println!("    Expired slots in 300s window: {}.", expired_count);

    if speedup_vs_full > 1.5 {
        println!("    --> Incremental scan is {:.1}x faster. Worth implementing.", speedup_vs_full);
    } else {
        println!("    --> Incremental scan shows minimal speedup ({:.1}x). May not be worth the complexity.", speedup_vs_full);
    }

    println!();
    println!("  Note: Both approaches call reconstruct_value() per slot (reads {} bit layers).",
        sort_field_name);
    println!("  The cost is proportional to the number of slots scanned,");
    println!("  NOT the number of expired slots found.");
    println!("============================================================");
}
