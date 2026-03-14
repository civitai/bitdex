//! Standalone test for the scatter-gather bulk loader.
//!
//! Usage: cargo run --release --features pg-sync --example test_scatter_gather -- [--threads N]
//!
//! Runs the scatter-gather pipeline against data/load_stage/ CSVs.
//! Watches RSS memory throughout.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use bitdex_v2::concurrent_engine::ConcurrentEngine;
use bitdex_v2::pg_sync::config::IndexDefinition;
use bitdex_v2::pg_sync::progress::LoadProgress;
use bitdex_v2::pg_sync::scatter_gather;

fn get_rss_mb() -> f64 {
    // Read /proc/self/status for VmRSS (Linux/WSL)
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if line.starts_with("VmRSS:") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if let Some(kb) = parts.get(1).and_then(|s| s.parse::<f64>().ok()) {
                    return kb / 1024.0;
                }
            }
        }
    }
    // Fallback: try tasklist on Windows (rough)
    if let Ok(output) = std::process::Command::new("wmic")
        .args(["process", "where", &format!("ProcessId={}", std::process::id()), "get", "WorkingSetSize"])
        .output()
    {
        let s = String::from_utf8_lossy(&output.stdout);
        for line in s.lines().skip(1) {
            if let Ok(bytes) = line.trim().parse::<u64>() {
                return bytes as f64 / (1024.0 * 1024.0);
            }
        }
    }
    0.0
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let _limit: Option<usize> = if let Some(pos) = args.iter().position(|a| a == "--limit") {
        args.get(pos + 1).and_then(|s| s.parse().ok())
    } else {
        None
    };
    let num_threads: usize = if let Some(pos) = args.iter().position(|a| a == "--threads") {
        args.get(pos + 1).and_then(|s| s.parse().ok()).unwrap_or(0)
    } else {
        0 // 0 = rayon default (num CPUs)
    };

    // Configure rayon thread pool if --threads specified
    if num_threads > 0 {
        rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build_global()
            .ok();
        eprintln!("Rayon thread pool: {} threads", num_threads);
    } else {
        eprintln!("Rayon thread pool: default ({} CPUs)", rayon::current_num_threads());
    }

    let stage_dir = PathBuf::from("data/load_stage");
    let index_dir = PathBuf::from("data/indexes/civitai");
    let data_dir = PathBuf::from("data/test-scatter-gather");

    // Clean previous test data
    let _ = std::fs::remove_dir_all(&data_dir);
    std::fs::create_dir_all(&data_dir).unwrap();

    eprintln!("=== Scatter-Gather Bulk Load Test ===");
    eprintln!("Stage dir: {}", stage_dir.display());
    eprintln!("Index dir: {}", index_dir.display());
    eprintln!("Data dir:  {}", data_dir.display());
    eprintln!("RSS at start: {:.0} MB", get_rss_mb());

    // Load index definition
    let index_def = IndexDefinition::from_dir(&index_dir).unwrap_or_else(|e| {
        eprintln!("Failed to load index definition: {e}");
        std::process::exit(1);
    });

    // Create engine — use indexes/civitai/ layout matching server expectations
    let index_dir_out = data_dir.join("indexes").join("civitai");
    let bitmap_dir = index_dir_out.join("bitmaps");
    let docs_dir = index_dir_out.join("docs");
    // Copy config.json so server can discover the index
    std::fs::create_dir_all(&index_dir_out).unwrap();
    let _ = std::fs::copy(
        index_dir.join("config.json"),
        index_dir_out.join("config.json"),
    );
    std::fs::create_dir_all(&bitmap_dir).unwrap();
    std::fs::create_dir_all(&docs_dir).unwrap();

    let mut engine_config = index_def.config.clone();
    engine_config.storage.bitmap_path = Some(bitmap_dir);

    let engine = ConcurrentEngine::new_with_path(engine_config, &docs_dir).unwrap_or_else(|e| {
        eprintln!("Failed to create engine: {e}");
        std::process::exit(1);
    });

    let phase2_only = args.iter().any(|a| a == "--phase2-only");

    engine.enter_loading_mode();

    let progress = Arc::new(LoadProgress::new());

    // Spawn RSS monitor thread
    let monitor_running = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let monitor_flag = Arc::clone(&monitor_running);
    let monitor = std::thread::spawn(move || {
        let mut peak_rss = 0.0f64;
        while monitor_flag.load(std::sync::atomic::Ordering::Relaxed) {
            let rss = get_rss_mb();
            if rss > peak_rss {
                peak_rss = rss;
            }
            std::thread::sleep(std::time::Duration::from_secs(5));
        }
        peak_rss
    });

    let start = Instant::now();
    let result = if phase2_only {
        eprintln!("=== Phase 2 only mode — using existing scratch shards ===");
        scatter_gather::run_gather_only(&engine, &index_def, &stage_dir, progress)
    } else {
        scatter_gather::run_bulk_load_scatter(&engine, &index_def, &stage_dir, progress)
    };

    monitor_running.store(false, std::sync::atomic::Ordering::Relaxed);
    let peak_rss = monitor.join().unwrap();

    match result {
        Ok(stats) => {
            eprintln!("\n=== Results ===");
            eprintln!("Records loaded: {}", stats.records_loaded);
            eprintln!("Elapsed: {:.1}s", stats.elapsed.as_secs_f64());
            eprintln!(
                "Rate: {:.0}/s",
                stats.records_loaded as f64 / stats.elapsed.as_secs_f64()
            );
            eprintln!("Peak RSS: {:.0} MB ({:.1} GB)", peak_rss, peak_rss / 1024.0);
            eprintln!("Final RSS: {:.0} MB", get_rss_mb());

            if peak_rss > 12.0 * 1024.0 {
                eprintln!("WARNING: Peak RSS exceeded 12 GB target!");
            } else {
                eprintln!("PASS: Peak RSS within 12 GB target.");
            }
        }
        Err(e) => {
            eprintln!("\nBulk load FAILED: {e}");
            std::process::exit(1);
        }
    }
}
