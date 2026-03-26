//! Validation harness for the Sync V2 ops pipeline.
//!
//! Tests both processing modes:
//! - **Dump mode** (default): AccumSink → direct bitmap accumulation (bulk loading path)
//! - **Steady-state mode** (--steady-state): CoalescerSink → coalescer channel (online path)
//!
//! Usage:
//!   cargo run --example validate_ops_pipeline --features pg-sync -- \
//!     --csv-dir C:\Dev\Repos\open-source\bitdex-v2\data\load_stage \
//!     --limit 100000
//!
//!   # Steady-state mode (slower, tests the online write path):
//!   cargo run --example validate_ops_pipeline --features pg-sync -- \
//!     --csv-dir data/load_stage --limit 10000 --steady-state

use std::path::PathBuf;
use std::time::Instant;

use bitdex_v2::concurrent_engine::ConcurrentEngine;
use bitdex_v2::config::Config;
use bitdex_v2::ops_processor::{apply_ops_batch, process_csv_dump_direct, process_wal_dump, FieldMeta};
use bitdex_v2::ops_wal::WalReader;
use bitdex_v2::pg_sync::csv_ops::run_csv_dump;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let csv_dir = get_arg(&args, "--csv-dir").unwrap_or_else(|| "data/load_stage".into());
    let limit: u64 = get_arg(&args, "--limit")
        .map(|s| s.parse().unwrap_or(100_000))
        .unwrap_or(100_000);
    let steady_state = args.iter().any(|a| a == "--steady-state");
    let direct = args.iter().any(|a| a == "--direct");

    eprintln!("=== Sync V2 Pipeline Validation ===");
    eprintln!("CSV dir:     {csv_dir}");
    eprintln!("Row limit:   {limit}");
    let mode_str = if direct { "direct (CSV → AccumSink, no WAL)" }
        else if steady_state { "steady-state (CoalescerSink)" }
        else { "dump (WAL → AccumSink)" };
    eprintln!("Mode:        {mode_str}");

    // Create a temp directory for this validation run
    let temp_dir = tempfile::TempDir::new().expect("Failed to create temp dir");
    let data_dir = temp_dir.path();
    let wal_path = data_dir.join("ops.wal");
    let bitmap_dir = data_dir.join("bitmaps");
    let docs_dir = data_dir.join("docs");
    std::fs::create_dir_all(&bitmap_dir).ok();
    std::fs::create_dir_all(&docs_dir).ok();

    // Phase 1: CSV → WAL
    eprintln!("\n--- Phase 1: CSV → WAL ---");
    let csv_start = Instant::now();
    let csv_results = run_csv_dump(
        &PathBuf::from(&csv_dir),
        &wal_path,
        10_000,
        Some(limit),
    )
    .expect("CSV dump failed");

    let csv_elapsed = csv_start.elapsed();
    let total_ops: u64 = csv_results.iter().map(|(_, s)| s.ops_written).sum();
    let total_rows: u64 = csv_results.iter().map(|(_, s)| s.rows_read).sum();
    eprintln!("\nCSV → WAL complete:");
    eprintln!("  Total rows:  {total_rows}");
    eprintln!("  Total ops:   {total_ops}");
    eprintln!("  Time:        {:.2}s", csv_elapsed.as_secs_f64());
    eprintln!("  Throughput:  {:.0} rows/s", total_rows as f64 / csv_elapsed.as_secs_f64().max(0.001));

    // Phase 2: WAL → Engine
    eprintln!("\n--- Phase 2: WAL → Engine ---");

    // Load index config
    let config_path = PathBuf::from(&csv_dir).parent().unwrap().join("indexes/civitai/config.json");
    let alt_config = PathBuf::from("data/indexes/civitai/config.json");
    let config_path = if config_path.exists() {
        config_path
    } else if alt_config.exists() {
        alt_config
    } else {
        eprintln!("ERROR: Could not find config.json. Skipping engine validation.");
        print_summary(&csv_results, None, None);
        return;
    };

    let config_str = std::fs::read_to_string(&config_path).expect("Failed to read config.json");
    let index_def: serde_json::Value = serde_json::from_str(&config_str).expect("Failed to parse config.json");
    let config: Config = serde_json::from_value(index_def["config"].clone()).expect("Failed to parse engine config");

    let meta = FieldMeta::from_config(&config);

    let mut engine_config = config.clone();
    engine_config.storage.bitmap_path = Some(bitmap_dir.clone());

    if direct {
        run_direct_mode(&engine_config, &docs_dir, &PathBuf::from(&csv_dir), limit, &csv_results);
    } else if steady_state {
        run_steady_state(&engine_config, &docs_dir, &wal_path, &meta, &csv_results, total_rows);
    } else {
        run_dump_mode(&engine_config, &config, &docs_dir, &wal_path, &meta, &csv_results, total_rows);
    }
}

fn run_direct_mode(
    engine_config: &Config,
    docs_dir: &std::path::Path,
    csv_dir: &std::path::Path,
    limit: u64,
    csv_results: &[(String, bitdex_v2::pg_sync::csv_ops::CsvOpsStats)],
) {
    let mut cfg = engine_config.clone();
    cfg.headless = true;
    let engine = ConcurrentEngine::new_with_path(cfg, docs_dir)
        .expect("Failed to create engine");

    eprintln!("  Processing CSV directly (no WAL)...");
    let (total_applied, total_errors, elapsed) =
        process_csv_dump_direct(&engine, csv_dir, 10_000, Some(limit));
    let alive = engine.alive_count();

    eprintln!("\nDirect dump complete:");
    eprintln!("  Ops applied: {total_applied}");
    eprintln!("  Errors:      {total_errors}");
    eprintln!("  Alive count: {alive}");
    eprintln!("  Time:        {:.2}s", elapsed);
    eprintln!("  Throughput:  {:.0} ops/s", total_applied as f64 / elapsed.max(0.001));

    validate_and_summarize(csv_results, alive, total_errors, 0);
}

fn run_dump_mode(
    engine_config: &Config,
    _config: &Config,
    docs_dir: &std::path::Path,
    wal_path: &std::path::Path,
    _meta: &FieldMeta,
    csv_results: &[(String, bitdex_v2::pg_sync::csv_ops::CsvOpsStats)],
    total_rows: u64,
) {
    // Headless is fine for dump mode — we apply directly to staging, no flush thread needed
    let mut cfg = engine_config.clone();
    cfg.headless = true;
    let engine = ConcurrentEngine::new_with_path(cfg, docs_dir)
        .expect("Failed to create engine");

    eprintln!("  Processing WAL via dump mode (AccumSink)...");
    let (total_applied, total_errors, elapsed) = process_wal_dump(&engine, wal_path, 10_000);
    let alive = engine.alive_count();

    eprintln!("\nWAL → Engine complete (dump mode):");
    eprintln!("  Ops applied: {total_applied}");
    eprintln!("  Errors:      {total_errors}");
    eprintln!("  Alive count: {alive}");
    eprintln!("  Time:        {:.2}s", elapsed);
    eprintln!("  Throughput:  {:.0} ops/s", total_applied as f64 / elapsed.max(0.001));

    validate_and_summarize(csv_results, alive, total_errors, total_rows);
}

fn run_steady_state(
    engine_config: &Config,
    docs_dir: &std::path::Path,
    wal_path: &std::path::Path,
    meta: &FieldMeta,
    csv_results: &[(String, bitdex_v2::pg_sync::csv_ops::CsvOpsStats)],
    total_rows: u64,
) {
    use bitdex_v2::ingester::CoalescerSink;

    // Non-headless — need flush thread to drain coalescer
    let mut cfg = engine_config.clone();
    cfg.headless = false;
    let engine = ConcurrentEngine::new_with_path(cfg, docs_dir)
        .expect("Failed to create engine");

    let wal_start = Instant::now();
    let mut reader = WalReader::new(wal_path, 0);
    let mut total_applied = 0u64;
    let mut total_errors = 0u64;

    loop {
        let batch = reader.read_batch(10_000).expect("WAL read failed");
        if batch.entries.is_empty() {
            break;
        }
        let sender = engine.mutation_sender();
        let mut sink = CoalescerSink::new(sender);
        let mut entries = batch.entries;
        let (applied, _skipped, errors) = apply_ops_batch(
            &mut sink, meta, &mut entries, Some(&engine),
        );
        total_applied += applied as u64;
        total_errors += errors as u64;
    }

    // Wait for flush thread to drain
    eprintln!("  Waiting for flush thread to drain...");
    let drain_start = Instant::now();
    loop {
        let pending = engine.flush_queue_depth();
        if pending == 0 { break; }
        if drain_start.elapsed().as_secs() > 30 {
            eprintln!("  WARN: flush thread still has {pending} pending ops after 30s");
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    eprintln!("  Flush drain: {:.1}s", drain_start.elapsed().as_secs_f64());

    let wal_elapsed = wal_start.elapsed();
    let alive = engine.alive_count();

    eprintln!("\nWAL → Engine complete (steady-state):");
    eprintln!("  Ops applied: {total_applied}");
    eprintln!("  Errors:      {total_errors}");
    eprintln!("  Alive count: {alive}");
    eprintln!("  Time:        {:.2}s", wal_elapsed.as_secs_f64());
    eprintln!("  Throughput:  {:.0} ops/s", total_applied as f64 / wal_elapsed.as_secs_f64().max(0.001));

    validate_and_summarize(csv_results, alive, total_errors, total_rows);
}

fn validate_and_summarize(
    csv_results: &[(String, bitdex_v2::pg_sync::csv_ops::CsvOpsStats)],
    alive: u64,
    total_errors: u64,
    total_rows: u64,
) {
    eprintln!("\n--- Phase 3: Validation ---");
    let mut pass = true;

    if alive == 0 {
        eprintln!("  FAIL: alive count is 0 — no documents loaded");
        pass = false;
    } else {
        eprintln!("  PASS: alive count = {alive}");
    }

    if total_errors > 0 {
        eprintln!("  WARN: {total_errors} errors during ops application");
    } else {
        eprintln!("  PASS: zero errors");
    }

    // Images make up a fraction of total rows
    let image_rows = csv_results.iter()
        .find(|(name, _)| name == "images")
        .map(|(_, s)| s.rows_read)
        .unwrap_or(0);
    let expected_min = (image_rows as f64 * 0.8) as u64;
    if alive < expected_min {
        eprintln!("  WARN: alive ({alive}) < 80% of image rows ({image_rows})");
    }

    print_summary(csv_results, Some(alive), Some(pass));
}

fn print_summary(
    csv_results: &[(String, bitdex_v2::pg_sync::csv_ops::CsvOpsStats)],
    alive: Option<u64>,
    pass: Option<bool>,
) {
    eprintln!("\n=== Summary ===");
    eprintln!("Table          | Rows      | Ops       | Time    | Rows/s");
    eprintln!("---------------|-----------|-----------|---------|--------");
    for (table, stats) in csv_results {
        eprintln!(
            "{:14} | {:>9} | {:>9} | {:>5.1}s  | {:>7.0}",
            table,
            stats.rows_read,
            stats.ops_written,
            stats.elapsed_secs,
            stats.rows_read as f64 / stats.elapsed_secs.max(0.001)
        );
    }
    if let Some(alive) = alive {
        eprintln!("\nAlive count: {alive}");
    }
    if let Some(pass) = pass {
        eprintln!("Result: {}", if pass { "PASS" } else { "FAIL" });
    }
}

fn get_arg(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .cloned()
}
