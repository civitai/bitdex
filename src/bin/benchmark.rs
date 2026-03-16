//! Bitdex V2 Benchmark Harness
//!
//! Loads real Civitai image data from an NDJSON file and measures insert, update,
//! and query performance at scale.
//!
//! Usage:
//!   cargo run --release --bin bitdex-benchmark -- [OPTIONS]
//!
//! Options:
//!   --data <PATH>     Path to images.ndjson (default: auto-detect)
//!   --limit <N>       Max records to load (default: all)
//!   --json            Output machine-readable JSON report
//!   --stages <LIST>   Comma-separated stages to run: insert,bulk,persist,restore,update,query,concurrent,mixed,contention,all (default: all)
//!   --threads <N>     Number of threads for concurrent benchmarks (default: 4)
//!   --in-memory-docstore  Use in-memory docstore instead of on-disk (default: on-disk)

// Use rpmalloc for better concurrent allocation performance.
// The default Windows CRT allocator fragments under heavy parallel load,
// causing parse times to degrade 5x+ as bitmap memory grows to 6+ GB.
#[global_allocator]
static ALLOC: rpmalloc::RpMalloc = rpmalloc::RpMalloc;

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use rand::Rng;
use rayon::prelude::*;

use bitdex_v2::concurrent_engine::ConcurrentEngine;
use bitdex_v2::config::{Config, FilterFieldConfig, SortFieldConfig};
use bitdex_v2::filter::FilterFieldType;
use bitdex_v2::mutation::{Document, FieldValue};
use bitdex_v2::query::{BitdexQuery, CursorPosition, FilterClause, SortClause, SortDirection, Value};

// ---------------------------------------------------------------------------
// NDJSON record definition
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct NdjsonRecord {
    id: u64,
    post_id: Option<u64>,
    user_id: Option<u64>,
    nsfw_level: Option<u64>,
    #[serde(rename = "type")]
    image_type: Option<String>,
    base_model: Option<String>,
    has_meta: Option<bool>,
    on_site: Option<bool>,
    poi: Option<bool>,
    minor: Option<bool>,
    prompt_nsfw: Option<bool>,
    sort_at: Option<u64>,
    published_at: Option<u64>,
    reaction_count: Option<u64>,
    comment_count: Option<u64>,
    collected_count: Option<u64>,
    tag_ids: Option<Vec<u64>>,
    model_version_ids: Option<Vec<u64>>,
    tool_ids: Option<Vec<u64>>,
    technique_ids: Option<Vec<u64>>,
    width: Option<u64>,
    height: Option<u64>,
}

impl NdjsonRecord {
    fn to_document(&self) -> Document {
        let mut fields = HashMap::new();

        if let Some(v) = self.nsfw_level {
            fields.insert("nsfwLevel".into(), FieldValue::Single(Value::Integer(v as i64)));
        }
        if let Some(v) = self.user_id {
            // Truncate userId to u32 range for filter bitmap keys
            fields.insert("userId".into(), FieldValue::Single(Value::Integer(v as i64)));
        }
        if let Some(ref v) = self.image_type {
            // Hash the type string to a u64 key for filter storage
            fields.insert("type".into(), FieldValue::Single(Value::Integer(type_to_int(v))));
        }
        if let Some(v) = self.has_meta {
            fields.insert("hasMeta".into(), FieldValue::Single(Value::Bool(v)));
        }
        if let Some(v) = self.on_site {
            fields.insert("onSite".into(), FieldValue::Single(Value::Bool(v)));
        }
        if let Some(v) = self.poi {
            fields.insert("poi".into(), FieldValue::Single(Value::Bool(v)));
        }
        if let Some(v) = self.minor {
            fields.insert("minor".into(), FieldValue::Single(Value::Bool(v)));
        }
        if let Some(ref tags) = self.tag_ids {
            if !tags.is_empty() {
                fields.insert(
                    "tagIds".into(),
                    FieldValue::Multi(tags.iter().map(|&t| Value::Integer(t as i64)).collect()),
                );
            }
        }
        if let Some(ref mv) = self.model_version_ids {
            if !mv.is_empty() {
                fields.insert(
                    "modelVersionIds".into(),
                    FieldValue::Multi(mv.iter().map(|&v| Value::Integer(v as i64)).collect()),
                );
            }
        }
        if let Some(ref t) = self.tool_ids {
            if !t.is_empty() {
                fields.insert(
                    "toolIds".into(),
                    FieldValue::Multi(t.iter().map(|&v| Value::Integer(v as i64)).collect()),
                );
            }
        }
        if let Some(ref t) = self.technique_ids {
            if !t.is_empty() {
                fields.insert(
                    "techniqueIds".into(),
                    FieldValue::Multi(t.iter().map(|&v| Value::Integer(v as i64)).collect()),
                );
            }
        }
        // Sort fields
        if let Some(v) = self.reaction_count {
            fields.insert("reactionCount".into(), FieldValue::Single(Value::Integer(v as i64)));
        }
        if let Some(v) = self.sort_at {
            // Truncate to u32 for sort layers (lower 32 bits preserves relative ordering
            // for recent timestamps within a reasonable window)
            fields.insert("sortAt".into(), FieldValue::Single(Value::Integer((v as u32) as i64)));
        }
        if let Some(v) = self.comment_count {
            fields.insert("commentCount".into(), FieldValue::Single(Value::Integer(v as i64)));
        }
        if let Some(v) = self.collected_count {
            fields.insert("collectedCount".into(), FieldValue::Single(Value::Integer(v as i64)));
        }
        // Use the record id itself as a sort field
        fields.insert("id".into(), FieldValue::Single(Value::Integer(self.id as i64)));

        Document { fields }
    }
}

fn type_to_int(t: &str) -> i64 {
    match t {
        "image" => 1,
        "video" => 2,
        "audio" => 3,
        _ => 0,
    }
}

// ---------------------------------------------------------------------------
// Byte utilities
// ---------------------------------------------------------------------------

/// Find the last newline in a byte slice (equivalent to memrchr for '\n').
fn memrchr_newline(data: &[u8]) -> Option<usize> {
    data.iter().rposition(|&b| b == b'\n')
}

// ---------------------------------------------------------------------------
// Config matching the Civitai schema
// ---------------------------------------------------------------------------

fn civitai_config() -> Config {
    Config {
        filter_fields: vec![
            FilterFieldConfig { name: "nsfwLevel".into(), field_type: FilterFieldType::SingleValue, behaviors: None, eviction: None, eager_load: false },
            FilterFieldConfig { name: "userId".into(), field_type: FilterFieldType::SingleValue, behaviors: None, eviction: None, eager_load: false },
            FilterFieldConfig { name: "type".into(), field_type: FilterFieldType::SingleValue, behaviors: None, eviction: None, eager_load: false },
            FilterFieldConfig { name: "hasMeta".into(), field_type: FilterFieldType::Boolean, behaviors: None, eviction: None, eager_load: false },
            FilterFieldConfig { name: "onSite".into(), field_type: FilterFieldType::Boolean, behaviors: None, eviction: None, eager_load: false },
            FilterFieldConfig { name: "poi".into(), field_type: FilterFieldType::Boolean, behaviors: None, eviction: None, eager_load: false },
            FilterFieldConfig { name: "minor".into(), field_type: FilterFieldType::Boolean, behaviors: None, eviction: None, eager_load: false },
            FilterFieldConfig { name: "tagIds".into(), field_type: FilterFieldType::MultiValue, behaviors: None, eviction: None, eager_load: false },
            FilterFieldConfig { name: "modelVersionIds".into(), field_type: FilterFieldType::MultiValue, behaviors: None, eviction: None, eager_load: false },
            FilterFieldConfig { name: "toolIds".into(), field_type: FilterFieldType::MultiValue, behaviors: None, eviction: None, eager_load: false },
            FilterFieldConfig { name: "techniqueIds".into(), field_type: FilterFieldType::MultiValue, behaviors: None, eviction: None, eager_load: false },
        ],
        sort_fields: vec![
            SortFieldConfig { name: "reactionCount".into(), source_type: "uint32".into(), encoding: "linear".into(), bits: 32, eager_load: false },
            SortFieldConfig { name: "sortAt".into(), source_type: "uint32".into(), encoding: "linear".into(), bits: 32, eager_load: false },
            SortFieldConfig { name: "commentCount".into(), source_type: "uint32".into(), encoding: "linear".into(), bits: 32, eager_load: false },
            SortFieldConfig { name: "collectedCount".into(), source_type: "uint32".into(), encoding: "linear".into(), bits: 32, eager_load: false },
            SortFieldConfig { name: "id".into(), source_type: "uint32".into(), encoding: "linear".into(), bits: 32, eager_load: false },
        ],
        max_page_size: 100,
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// CLI arg parsing (minimal, no extra dependencies)
// ---------------------------------------------------------------------------

struct Args {
    data_path: PathBuf,
    limit: Option<usize>,
    json_output: bool,
    stages: Vec<String>,
    threads: usize,
    channel_capacity: usize,
    flush_interval_us: u64,
    remap_ids: bool,
    in_memory_docstore: bool,
}

fn parse_args() -> Args {
    let args: Vec<String> = std::env::args().collect();
    let mut data_path: Option<PathBuf> = None;
    let mut limit: Option<usize> = None;
    let mut json_output = false;
    let mut stages = vec!["all".to_string()];
    let mut threads: usize = 4;
    let mut channel_capacity: usize = 0; // 0 = auto
    let mut flush_interval_us: u64 = 100;
    let mut remap_ids = false;
    let mut in_memory_docstore = false;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--data" => {
                i += 1;
                data_path = Some(PathBuf::from(&args[i]));
            }
            "--limit" => {
                i += 1;
                limit = Some(args[i].parse().expect("--limit must be a number"));
            }
            "--json" => {
                json_output = true;
            }
            "--stages" => {
                i += 1;
                stages = args[i].split(',').map(|s| s.trim().to_string()).collect();
            }
            "--threads" => {
                i += 1;
                threads = args[i].parse().expect("--threads must be a number");
                if threads == 0 { threads = 1; }
            }
            "--channel-capacity" => {
                i += 1;
                channel_capacity = args[i].parse().expect("--channel-capacity must be a number");
            }
            "--flush-interval-us" => {
                i += 1;
                flush_interval_us = args[i].parse().expect("--flush-interval-us must be a number");
            }
            "--remap-ids" => {
                remap_ids = true;
            }
            "--in-memory-docstore" => {
                in_memory_docstore = true;
            }
            other => {
                eprintln!("Unknown argument: {other}");
                std::process::exit(1);
            }
        }
        i += 1;
    }

    // Auto-detect data path
    let data_path = data_path.unwrap_or_else(|| {
        let candidates = [
            PathBuf::from(r"C:\Dev\Repos\open-source\bitdex\data\images.ndjson"),
            PathBuf::from("data/images.ndjson"),
            PathBuf::from("../bitdex/data/images.ndjson"),
        ];
        for c in &candidates {
            if c.exists() {
                return c.clone();
            }
        }
        eprintln!("Could not find images.ndjson. Use --data <PATH> to specify.");
        std::process::exit(1);
    });

    Args { data_path, limit, json_output, stages, threads, channel_capacity, flush_interval_us, remap_ids, in_memory_docstore }
}

fn should_run(stages: &[String], name: &str) -> bool {
    stages.iter().any(|s| s == "all" || s == name)
}

// ---------------------------------------------------------------------------
// Latency stats
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, serde::Serialize)]
struct LatencyStats {
    count: usize,
    total_ms: f64,
    min_ms: f64,
    max_ms: f64,
    mean_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
}

fn compute_stats(mut durations: Vec<Duration>) -> LatencyStats {
    assert!(!durations.is_empty());
    durations.sort();
    let count = durations.len();
    let total: Duration = durations.iter().sum();
    let total_ms = total.as_secs_f64() * 1000.0;
    let min_ms = durations[0].as_secs_f64() * 1000.0;
    let max_ms = durations[count - 1].as_secs_f64() * 1000.0;
    let mean_ms = total_ms / count as f64;

    let p = |pct: f64| -> f64 {
        let idx = ((pct / 100.0) * count as f64).ceil() as usize;
        let idx = idx.min(count).saturating_sub(1);
        durations[idx].as_secs_f64() * 1000.0
    };

    LatencyStats {
        count,
        total_ms,
        min_ms,
        max_ms,
        mean_ms,
        p50_ms: p(50.0),
        p95_ms: p(95.0),
        p99_ms: p(99.0),
    }
}

// ---------------------------------------------------------------------------
// Memory tracking
// ---------------------------------------------------------------------------

fn rss_bytes() -> u64 {
    #[cfg(target_os = "windows")]
    {
        use std::mem::MaybeUninit;
        // Use Windows API to get working set size
        unsafe {
            let process = windows_process_handle();
            let mut pmc: MaybeUninit<PROCESS_MEMORY_COUNTERS> = MaybeUninit::zeroed();
            if GetProcessMemoryInfo(process, pmc.as_mut_ptr(), std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32) != 0 {
                (*pmc.as_ptr()).working_set_size as u64
            } else {
                0
            }
        }
    }
    #[cfg(target_os = "linux")]
    {
        // Read from /proc/self/statm
        if let Ok(statm) = std::fs::read_to_string("/proc/self/statm") {
            if let Some(rss_pages) = statm.split_whitespace().nth(1) {
                if let Ok(pages) = rss_pages.parse::<u64>() {
                    return pages * 4096;
                }
            }
        }
        0
    }
    #[cfg(not(any(target_os = "windows", target_os = "linux")))]
    {
        0
    }
}

#[cfg(target_os = "windows")]
#[repr(C)]
#[allow(non_snake_case)]
struct PROCESS_MEMORY_COUNTERS {
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

#[cfg(target_os = "windows")]
extern "system" {
    fn GetCurrentProcess() -> isize;
}

#[cfg(target_os = "windows")]
#[link(name = "psapi")]
extern "system" {
    fn GetProcessMemoryInfo(process: isize, ppsmemCounters: *mut PROCESS_MEMORY_COUNTERS, cb: u32) -> i32;
}

#[cfg(target_os = "windows")]
unsafe fn windows_process_handle() -> isize {
    GetCurrentProcess()
}

fn dir_size(path: &std::path::Path) -> u64 {
    fn recurse(p: &std::path::Path) -> u64 {
        let mut total = 0u64;
        if let Ok(entries) = std::fs::read_dir(p) {
            for entry in entries.flatten() {
                let ft = entry.file_type().unwrap_or_else(|_| unreachable!());
                if ft.is_file() {
                    total += entry.metadata().map(|m| m.len()).unwrap_or(0);
                } else if ft.is_dir() {
                    total += recurse(&entry.path());
                }
            }
        }
        total
    }
    recurse(path)
}

fn format_bytes(b: u64) -> String {
    if b >= 1 << 30 {
        format!("{:.2} GB", b as f64 / (1u64 << 30) as f64)
    } else if b >= 1 << 20 {
        format!("{:.2} MB", b as f64 / (1u64 << 20) as f64)
    } else if b >= 1 << 10 {
        format!("{:.2} KB", b as f64 / (1u64 << 10) as f64)
    } else {
        format!("{b} B")
    }
}

fn _format_rate(count: usize, elapsed: Duration) -> String {
    let secs = elapsed.as_secs_f64();
    if secs == 0.0 {
        return "inf".to_string();
    }
    let rate = count as f64 / secs;
    if rate >= 1_000_000.0 {
        format!("{:.2}M/s", rate / 1_000_000.0)
    } else if rate >= 1_000.0 {
        format!("{:.1}K/s", rate / 1_000.0)
    } else {
        format!("{:.0}/s", rate)
    }
}

// ---------------------------------------------------------------------------
// Benchmark report structures (for JSON output)
// ---------------------------------------------------------------------------

#[derive(Debug, serde::Serialize)]
struct BenchmarkReport {
    dataset: DatasetInfo,
    insert_benchmarks: Vec<InsertBenchmark>,
    update_benchmark: Option<UpdateBenchmark>,
    query_benchmarks: Vec<QueryBenchmark>,
    concurrent_insert_benchmark: Option<ConcurrentInsertBenchmark>,
    mixed_rw_benchmark: Option<MixedRwBenchmark>,
    contention_benchmark: Option<ContentionBenchmark>,
    memory_snapshots: Vec<MemorySnapshot>,
}

#[derive(Debug, serde::Serialize)]
struct DatasetInfo {
    path: String,
    total_records: usize,
    records_loaded: usize,
    parse_time_ms: f64,
}

#[derive(Debug, serde::Serialize)]
struct InsertBenchmark {
    batch_label: String,
    record_count: usize,
    insert_ms: f64,
    wall_ms: f64,
    insert_rate_per_sec: f64,
    rss_before_bytes: u64,
    rss_after_bytes: u64,
    rss_delta_bytes: u64,
}

#[derive(Debug, serde::Serialize)]
struct UpdateBenchmark {
    record_count: usize,
    elapsed_ms: f64,
    rate_per_sec: f64,
}

#[derive(Debug, serde::Serialize)]
struct QueryBenchmark {
    name: String,
    description: String,
    iterations: usize,
    stats: LatencyStats,
}

#[derive(Debug, serde::Serialize)]
struct ConcurrentInsertBenchmark {
    threads: usize,
    record_count: usize,
    wall_ms: f64,
    total_docs_per_sec: f64,
    per_thread_docs_per_sec: f64,
    alive_after: u64,
    rss_before_bytes: u64,
    rss_after_bytes: u64,
}

#[derive(Debug, serde::Serialize)]
struct MixedRwBenchmark {
    writer_threads: usize,
    reader_threads: usize,
    records_inserted: usize,
    queries_executed: usize,
    wall_ms: f64,
    insert_rate_per_sec: f64,
    query_stats: LatencyStats,
}

#[derive(Debug, serde::Serialize)]
struct ContentionBenchmark {
    duration_secs: f64,
    reader_threads: usize,
    total_queries: usize,
    queries_per_sec: f64,
    query_stats: LatencyStats,
    total_inserts: usize,
    insert_rate_per_sec: f64,
    total_updates: usize,
    update_rate_per_sec: f64,
    alive_before: u64,
    alive_after: u64,
    rss_before_bytes: u64,
    rss_after_bytes: u64,
}

#[derive(Debug, serde::Serialize)]
struct MemorySnapshot {
    stage: String,
    rss_bytes: u64,
    rss_human: String,
    alive_count: u64,
}

// ---------------------------------------------------------------------------
// Streaming helpers — re-read the NDJSON file for each phase instead of
// holding millions of parsed records in RAM.
// ---------------------------------------------------------------------------

/// Count total records in the file (raw byte scan -- just counts newlines).
fn count_records(path: &PathBuf, limit: usize) -> usize {
    use std::io::Read;
    let file = File::open(path).expect("Failed to open data file");
    let mut reader = BufReader::with_capacity(256 * 1024, file);
    let mut buf = [0u8; 64 * 1024];
    let mut count = 0usize;
    loop {
        if count >= limit { break; }
        let bytes_read = reader.read(&mut buf).unwrap_or(0);
        if bytes_read == 0 { break; }
        for &b in &buf[..bytes_read] {
            if b == b'\n' {
                count += 1;
                if count >= limit { break; }
            }
        }
    }
    count
}

/// Stream records from the NDJSON file, calling `f` for each parsed record.
/// Stops after `limit` successful records. Returns (records_processed, parse_errors).
fn stream_records<F>(path: &PathBuf, limit: usize, mut f: F) -> (usize, usize)
where
    F: FnMut(&NdjsonRecord),
{
    let file = File::open(path).expect("Failed to open data file");
    let reader = BufReader::with_capacity(8 * 1024 * 1024, file);
    let mut count = 0usize;
    let mut errors = 0usize;
    for line_result in reader.lines() {
        if count >= limit { break; }
        let line = match line_result {
            Ok(l) => l,
            Err(_) => { errors += 1; continue; }
        };
        if line.is_empty() { continue; }
        match serde_json::from_str::<NdjsonRecord>(&line) {
            Ok(rec) => { f(&rec); count += 1; }
            Err(_) => { errors += 1; }
        }
    }
    (count, errors)
}

/// Load records into a Vec for concurrent benchmarks (needs pre-parsed data
/// so chunks can be distributed to threads).
fn load_records(path: &PathBuf, limit: usize, remap_ids: bool) -> Vec<(u32, Document)> {
    let mut records = Vec::new();
    let mut counter = 0u32;
    stream_records(path, limit, |rec| {
        let id = if remap_ids { counter } else { rec.id as u32 };
        counter += 1;
        records.push((id, rec.to_document()));
    });
    records
}

/// Print a detailed bitmap memory breakdown from the ConcurrentEngine.
fn print_bitmap_memory(engine: &ConcurrentEngine) {
    let (slot_bytes, filter_bytes, sort_bytes, _cache_entries, cache_bytes, filter_details, sort_details) =
        engine.bitmap_memory_report();
    let uc = engine.unified_cache_stats();
    let total = slot_bytes + filter_bytes + sort_bytes + cache_bytes;

    println!("--- Bitmap Memory (pure Bitdex, excludes docstore/allocator) ---");
    println!("  Slots (alive+clean):  {:>10}", format_bytes(slot_bytes as u64));
    println!("  Filter bitmaps:       {:>10}", format_bytes(filter_bytes as u64));
    for (name, count, bytes) in &filter_details {
        println!("    {:<22} {:>6} bitmaps  {:>10}", name, count, format_bytes(*bytes as u64));
    }
    println!("  Sort layer bitmaps:   {:>10}", format_bytes(sort_bytes as u64));
    for (name, bytes) in &sort_details {
        println!("    {:<22}              {:>10}", name, format_bytes(*bytes as u64));
    }
    println!("  Unified cache:        {:>10}  ({} entries, {} hits, {} misses)",
        format_bytes(uc.memory_bytes as u64), uc.entries, uc.hits, uc.misses);
    println!("  ----------------------------------------");
    println!("  Total bitmap memory:  {:>10}", format_bytes(total as u64));
    println!();
}

/// Create a ConcurrentEngine with on-disk or in-memory docstore based on the flag.
fn create_concurrent_engine(config: Config, bench_dir: &Path, label: &str, in_memory: bool) -> ConcurrentEngine {
    if in_memory {
        ConcurrentEngine::new(config).unwrap()
    } else {
        let db_path = bench_dir.join(format!("{}.redb", label));
        if db_path.exists() {
            std::fs::remove_file(&db_path).ok();
        }
        ConcurrentEngine::new_with_path(config, &db_path).unwrap()
    }
}

/// Wait for the ConcurrentEngine flush thread to catch up.
fn wait_for_flush(engine: &ConcurrentEngine, expected_alive: u64, max_ms: u64) {
    let deadline = Instant::now() + Duration::from_millis(max_ms);
    while Instant::now() < deadline {
        if engine.alive_count() >= expected_alive {
            thread::sleep(Duration::from_millis(5));
            return;
        }
        thread::sleep(Duration::from_millis(1));
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    let args = parse_args();

    println!("==========================================================");
    println!("  Bitdex V2 Benchmark Harness");
    println!("==========================================================");
    println!();
    println!("Data:       {}", args.data_path.display());
    println!("Limit:      {}", args.limit.map_or("all".to_string(), |n| n.to_string()));
    println!("Threads:    {}", args.threads);
    println!("Channel:    {}", if args.channel_capacity > 0 { args.channel_capacity.to_string() } else { "auto".to_string() });
    println!("Flush us:   {}", args.flush_interval_us);
    println!("Remap IDs:  {}", args.remap_ids);
    println!("Docstore:   {}", if args.in_memory_docstore { "in-memory" } else { "on-disk" });
    println!("Stages:     {:?}", args.stages);
    println!();

    // Set up on-disk docstore directory next to the executable (cleaned up at end)
    let bench_dir = std::env::current_exe()
        .expect("failed to get exe path")
        .parent()
        .expect("exe has no parent dir")
        .join("bitdex-bench-data");
    let needs_bench_dir = !args.in_memory_docstore
        || should_run(&args.stages, "persist")
        || should_run(&args.stages, "restore");
    if needs_bench_dir {
        if bench_dir.exists() && !should_run(&args.stages, "restore") {
            // Don't delete bench_dir if we're restoring from it
            std::fs::remove_dir_all(&bench_dir).ok();
        }
        std::fs::create_dir_all(&bench_dir).ok();
        println!("Bench dir: {}", bench_dir.display());
        println!();
    }
    let persist_path = bench_dir.join("bitmaps");

    let limit = args.limit.unwrap_or(usize::MAX);

    // -----------------------------------------------------------------------
    // Phase 1: Count records (quick scan, no full parse into memory)
    // Skip if only running bulk/persist/restore/query — the bulk path streams
    // in chunks and doesn't need an upfront count.
    // -----------------------------------------------------------------------
    let bulk_only_stages = ["bulk", "persist", "restore", "query"];
    let needs_count = args.stages.iter().any(|s| {
        s == "all" || !bulk_only_stages.contains(&s.as_str())
    });
    let (total_records, count_time_ms) = if needs_count {
        println!("--- Phase 1: Counting records ---");
        let count_start = Instant::now();
        let count = count_records(&args.data_path, limit);
        let count_elapsed = count_start.elapsed();
        println!("  {} records in {:.2}s", count, count_elapsed.as_secs_f64());
        println!("  RSS after count: {}", format_bytes(rss_bytes()));
        println!();
        (count, count_elapsed.as_secs_f64() * 1000.0)
    } else {
        // For bulk-only: use limit or a large sentinel (chunked streaming handles the actual limit)
        println!("--- Skipping record count (bulk-only mode) ---");
        println!();
        (limit, 0.0)
    };

    let mut report = BenchmarkReport {
        dataset: DatasetInfo {
            path: args.data_path.display().to_string(),
            total_records,
            records_loaded: total_records,
            parse_time_ms: count_time_ms,
        },
        insert_benchmarks: Vec::new(),
        update_benchmark: None,
        query_benchmarks: Vec::new(),
        concurrent_insert_benchmark: None,
        mixed_rw_benchmark: None,
        contention_benchmark: None,
        memory_snapshots: vec![MemorySnapshot {
            stage: "before_insert".into(),
            rss_bytes: rss_bytes(),
            rss_human: format_bytes(rss_bytes()),
            alive_count: 0,
        }],
    };

    // -----------------------------------------------------------------------
    // Phase 2: Insert benchmarks at varying batch sizes (ConcurrentEngine
    //          for batched docstore writes even in single-threaded mode)
    // -----------------------------------------------------------------------
    if should_run(&args.stages, "insert") {
        println!("--- Phase 2: Insert Benchmarks (ConcurrentEngine, single caller) ---");

        let batch_sizes: Vec<usize> = vec![1_000, 10_000, 100_000, 500_000, 1_000_000, total_records]
            .into_iter()
            .filter(|&s| s <= total_records)
            .collect();

        let batch_sizes: Vec<usize> = {
            let mut v = batch_sizes;
            v.dedup();
            v
        };

        for &batch_size in &batch_sizes {
            let label = if batch_size == total_records {
                format!("all ({})", total_records)
            } else {
                format!("{}", batch_size)
            };

            let rss_before = rss_bytes();
            let engine = create_concurrent_engine(civitai_config(), &bench_dir, &format!("insert_{}", batch_size), args.in_memory_docstore);
            engine.enter_loading_mode();
            let mut insert_time = Duration::ZERO;
            let mut id_counter = 0u32;

            let wall_start = Instant::now();
            stream_records(&args.data_path, batch_size, |rec| {
                let id = if args.remap_ids { let v = id_counter; id_counter += 1; v } else { rec.id as u32 };
                let doc = rec.to_document();
                let put_start = Instant::now();
                engine.put(id, &doc).unwrap();
                insert_time += put_start.elapsed();
            });
            engine.exit_loading_mode();
            // Wait for flush thread to apply all batched mutations
            wait_for_flush(&engine, batch_size as u64, 30_000);
            let wall_elapsed = wall_start.elapsed();
            let rss_after = rss_bytes();
            let rss_delta = rss_after.saturating_sub(rss_before);

            let insert_rate = batch_size as f64 / insert_time.as_secs_f64();

            println!("  [{:>12}] put: {:.2}s  wall: {:.2}s  ({:.0}/s)  RSS: {} (+{})  alive: {}",
                label,
                insert_time.as_secs_f64(),
                wall_elapsed.as_secs_f64(),
                insert_rate,
                format_bytes(rss_after),
                format_bytes(rss_delta),
                engine.alive_count()
            );

            report.insert_benchmarks.push(InsertBenchmark {
                batch_label: label.clone(),
                record_count: batch_size,
                insert_ms: insert_time.as_secs_f64() * 1000.0,
                wall_ms: wall_elapsed.as_secs_f64() * 1000.0,
                insert_rate_per_sec: insert_rate,
                rss_before_bytes: rss_before,
                rss_after_bytes: rss_after,
                rss_delta_bytes: rss_delta,
            });

            report.memory_snapshots.push(MemorySnapshot {
                stage: format!("insert_{}", label),
                rss_bytes: rss_after,
                rss_human: format_bytes(rss_after),
                alive_count: engine.alive_count(),
            });
        }
        println!();
    }

    // -----------------------------------------------------------------------
    // Phase 2b: Concurrent insert benchmark (ConcurrentEngine, N threads)
    // -----------------------------------------------------------------------
    if args.threads > 1 && should_run(&args.stages, "concurrent") {
        println!("--- Phase 2b: Concurrent Insert Benchmark ({} threads, ConcurrentEngine) ---", args.threads);
        println!("  Loading records into memory for thread distribution...");

        let load_start = Instant::now();
        let records = load_records(&args.data_path, total_records, args.remap_ids);
        let load_elapsed = load_start.elapsed();
        println!("  Loaded {} records in {:.2}s (parse + to_document)", records.len(), load_elapsed.as_secs_f64());

        let rss_before = rss_bytes();
        // Use tunable config for concurrent benchmarks
        let mut config = civitai_config();
        // Auto-size channel capacity: ~50 ops per doc * batch_count to avoid backpressure
        if args.channel_capacity > 0 {
            config.channel_capacity = args.channel_capacity;
        } else {
            config.channel_capacity = (records.len() * 50).max(100_000).min(10_000_000);
        }
        config.flush_interval_us = args.flush_interval_us;
        println!("  Channel capacity: {}, flush interval: {}us", config.channel_capacity, config.flush_interval_us);
        let engine = Arc::new(create_concurrent_engine(config, &bench_dir, "concurrent_insert", args.in_memory_docstore));
        engine.enter_loading_mode();

        // Split records into chunks for each thread
        let chunk_size = (records.len() + args.threads - 1) / args.threads;
        let chunks: Vec<Vec<(u32, Document)>> = records
            .chunks(chunk_size)
            .map(|c| c.to_vec())
            .collect();

        let total_inserted = Arc::new(AtomicUsize::new(0));

        println!("  Inserting with {} threads ({} records/thread avg, auto-coalesced)...", args.threads, chunk_size);
        let wall_start = Instant::now();

        let handles: Vec<_> = chunks
            .into_iter()
            .map(|chunk| {
                let engine = Arc::clone(&engine);
                let counter = Arc::clone(&total_inserted);
                thread::spawn(move || {
                    let mut count = 0usize;
                    // Simple put() calls — docstore writes are auto-coalesced by the flush thread
                    for (id, doc) in &chunk {
                        engine.put(*id, doc).unwrap();
                        count += 1;
                    }
                    counter.fetch_add(count, Ordering::Relaxed);
                    count
                })
            })
            .collect();

        let mut per_thread_counts = Vec::new();
        for h in handles {
            per_thread_counts.push(h.join().unwrap());
        }

        let wall_elapsed = wall_start.elapsed();
        let total_count = total_inserted.load(Ordering::Relaxed);

        // Exit loading mode and wait for all mutations to flush
        engine.exit_loading_mode();
        println!("  Waiting for flush thread to catch up...");
        wait_for_flush(&engine, total_count as u64, 30_000);
        let alive = engine.alive_count();

        let rss_after = rss_bytes();
        let total_rate = total_count as f64 / wall_elapsed.as_secs_f64();
        let per_thread_rate = total_rate / args.threads as f64;

        println!("  Concurrent insert complete:");
        println!("    Records:          {}", total_count);
        println!("    Wall time:        {:.2}s", wall_elapsed.as_secs_f64());
        println!("    Total throughput: {:.0} docs/s", total_rate);
        println!("    Per-thread avg:   {:.0} docs/s", per_thread_rate);
        println!("    Alive after:      {}", alive);
        println!("    RSS: {} (delta: {})", format_bytes(rss_after), format_bytes(rss_after.saturating_sub(rss_before)));
        for (i, count) in per_thread_counts.iter().enumerate() {
            println!("    Thread {}: {} records", i, count);
        }
        println!();

        report.concurrent_insert_benchmark = Some(ConcurrentInsertBenchmark {
            threads: args.threads,
            record_count: total_count,
            wall_ms: wall_elapsed.as_secs_f64() * 1000.0,
            total_docs_per_sec: total_rate,
            per_thread_docs_per_sec: per_thread_rate,
            alive_after: alive,
            rss_before_bytes: rss_before,
            rss_after_bytes: rss_after,
        });

        report.memory_snapshots.push(MemorySnapshot {
            stage: format!("concurrent_insert_{}t", args.threads),
            rss_bytes: rss_after,
            rss_human: format_bytes(rss_after),
            alive_count: alive,
        });
    }

    // -----------------------------------------------------------------------
    // Phase 2c: Bulk insert benchmark (put_bulk — parallel decompose + direct bitmap build)
    // -----------------------------------------------------------------------
    let mut bulk_engine: Option<ConcurrentEngine> = None;

    if should_run(&args.stages, "bulk") {
        println!("--- Phase 2c: Bulk Insert Benchmark (put_bulk, {} threads) ---", args.threads);

        // Process in chunks to avoid OOM at large scales.
        // Each chunk loads N records, calls put_bulk(), then frees the chunk.
        let chunk_size = 5_000_000.min(total_records);

        let rss_before = rss_bytes();
        let engine = create_concurrent_engine(civitai_config(), &bench_dir, "bulk_insert", args.in_memory_docstore);

        let wall_start = Instant::now();
        let mut total_inserted: usize = 0;
        let mut chunks_processed: usize = 0;
        let mut id_counter: u32 = 0;

        // Use loading mode: accumulate into a private staging InnerEngine
        // without publishing intermediate snapshots. This avoids the
        // Arc::make_mut deep-clone cascade that happens when the published
        // snapshot shares Arc references with the staging copy.
        let mut staging = engine.clone_staging();

        // Pipelined bulk loading with parallel parsing:
        //
        // 1. Reader thread reads raw lines in small batches (read_batch_size)
        //    and sends them to a channel (bounded, depth=2 for backpressure).
        // 2. Main thread receives line batches, parallel-parses with rayon,
        //    accumulates parsed docs into a bitmap chunk (chunk_size).
        // 3. When bitmap chunk is full, calls put_bulk_loading.
        //
        // This overlaps I/O with parsing with bitmap building.
        let remap_ids = args.remap_ids;
        let num_threads = args.threads;
        let read_batch_size = 500_000; // smaller read batches for better pipelining

        // Pipelined bulk loading:
        // 1. Reader thread reads large byte blocks (~300 MB each, ~500K lines)
        //    and sends complete-line buffers as Vec<u8>.
        // 2. Main thread receives byte buffers, splits lines + parses JSON in
        //    parallel with rayon, accumulates docs into bitmap chunks.
        // 3. When chunk is full, calls put_bulk_loading.
        //
        // All CPU work (newline splitting, JSON parsing, document construction)
        // happens in rayon on the main thread, while the reader thread does
        // pure I/O. This maximizes parallelism.
        let data_path = args.data_path.clone();
        let target_batch_bytes = read_batch_size * 600; // ~600 bytes/line × 500K lines ≈ 300 MB
        let (block_tx, block_rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(2);

        let reader_handle = thread::spawn(move || {
            use std::io::Read;
            let file = File::open(&data_path).expect("Failed to open data file");
            let mut reader = BufReader::with_capacity(16 * 1024 * 1024, file);
            let mut leftover = Vec::<u8>::new();
            let mut buf = vec![0u8; 4 * 1024 * 1024]; // 4 MB read buffer
            let mut accum = Vec::<u8>::with_capacity(target_batch_bytes + 4 * 1024 * 1024);
            let mut blocks_sent: usize = 0;

            loop {
                let bytes_read = reader.read(&mut buf).unwrap_or(0);
                if bytes_read == 0 {
                    // EOF — flush leftover + accumulator
                    if !leftover.is_empty() {
                        accum.extend_from_slice(&leftover);
                        leftover.clear();
                    }
                    if !accum.is_empty() {
                        let _ = block_tx.send(accum);
                        blocks_sent += 1;
                    }
                    break;
                }

                accum.extend_from_slice(&buf[..bytes_read]);

                // Once we have enough data, find the last newline and split
                if accum.len() >= target_batch_bytes {
                    match memrchr_newline(&accum) {
                        Some(last_nl) => {
                            // Everything up to (including) last newline is a complete batch
                            let remainder = accum[last_nl + 1..].to_vec();
                            accum.truncate(last_nl + 1);

                            // Prepend any leftover from previous split
                            if !leftover.is_empty() {
                                let mut combined = std::mem::take(&mut leftover);
                                combined.append(&mut accum);
                                accum = combined;
                            }

                            let batch = std::mem::replace(&mut accum, Vec::with_capacity(target_batch_bytes + 4 * 1024 * 1024));
                            leftover = remainder;

                            if block_tx.send(batch).is_err() { break; }
                            blocks_sent += 1;
                        }
                        None => {
                            // No newline in accumulated data — keep accumulating
                        }
                    }
                }
            }
            blocks_sent
        });

        // Main thread: receive byte blocks, parallel-parse with rayon, accumulate into bitmap chunks
        let mut doc_chunk: Vec<(u32, Document)> = Vec::with_capacity(chunk_size);
        let mut parse_time_accum = Duration::ZERO;

        while let Ok(raw_block) = block_rx.recv() {
            let parse_start = Instant::now();
            let base_id = id_counter;

            let block_str = std::str::from_utf8(&raw_block).expect("NDJSON block is not valid UTF-8");

            // Split into lines and parallel-parse with rayon
            let lines: Vec<&str> = block_str.split('\n')
                .map(|l| l.trim_end_matches('\r'))
                .filter(|l| !l.is_empty())
                .collect();
            let line_count = lines.len() as u32;

            let mut parsed: Vec<(u32, Document)> = lines.into_par_iter()
                .enumerate()
                .filter_map(|(i, line)| {
                    serde_json::from_str::<NdjsonRecord>(line).ok().map(|rec| {
                        let id = if remap_ids { base_id + i as u32 } else { rec.id as u32 };
                        (id, rec.to_document())
                    })
                })
                .collect();
            let parse_elapsed = parse_start.elapsed();
            parse_time_accum += parse_elapsed;
            id_counter += line_count;

            doc_chunk.append(&mut parsed);

            // When we have enough docs, run bitmap building
            if doc_chunk.len() >= chunk_size {
                let bitmap_start = Instant::now();
                let count = engine.put_bulk_loading(&mut staging, &doc_chunk, num_threads);
                let bitmap_elapsed = bitmap_start.elapsed();

                total_inserted += count;
                chunks_processed += 1;
                let alive = staging.slots.alive_count();
                let rate = count as f64 / (parse_time_accum + bitmap_elapsed).as_secs_f64();
                println!("  chunk {}: {} records  parse={:.2}s bitmap={:.2}s ({:.0}/s)  alive: {}",
                    chunks_processed, count,
                    parse_time_accum.as_secs_f64(), bitmap_elapsed.as_secs_f64(),
                    rate, alive);
                doc_chunk = Vec::with_capacity(chunk_size);
                parse_time_accum = Duration::ZERO;
            }
        }

        // Process remaining docs
        if !doc_chunk.is_empty() {
            let bitmap_start = Instant::now();
            let count = engine.put_bulk_loading(&mut staging, &doc_chunk, num_threads);
            let bitmap_elapsed = bitmap_start.elapsed();

            total_inserted += count;
            chunks_processed += 1;
            let alive = staging.slots.alive_count();
            let rate = count as f64 / (parse_time_accum + bitmap_elapsed).as_secs_f64();
            println!("  chunk {}: {} records  parse={:.2}s bitmap={:.2}s ({:.0}/s)  alive: {}",
                chunks_processed, count,
                parse_time_accum.as_secs_f64(), bitmap_elapsed.as_secs_f64(),
                rate, alive);
        }

        reader_handle.join().unwrap();

        // Publish the fully-built staging as the live snapshot
        let publish_start = Instant::now();
        engine.publish_staging(staging);
        let publish_elapsed = publish_start.elapsed();
        println!("  publish: {:.2}s  alive: {}", publish_elapsed.as_secs_f64(), engine.alive_count());

        let wall_elapsed = wall_start.elapsed();
        let rss_after = rss_bytes();
        let rss_delta = rss_after.saturating_sub(rss_before);

        let bulk_rate = total_inserted as f64 / wall_elapsed.as_secs_f64();

        println!("  [{:>12}] put_bulk total: {:.2}s  ({:.0}/s)  RSS: {} (+{})  alive: {}",
            format!("{}", total_inserted),
            wall_elapsed.as_secs_f64(),
            bulk_rate,
            format_bytes(rss_after),
            format_bytes(rss_delta),
            engine.alive_count()
        );

        // Bitmap memory breakdown
        print_bitmap_memory(&engine);

        report.insert_benchmarks.push(InsertBenchmark {
            batch_label: format!("bulk_{}", total_inserted),
            record_count: total_inserted,
            insert_ms: wall_elapsed.as_secs_f64() * 1000.0,
            wall_ms: wall_elapsed.as_secs_f64() * 1000.0,
            insert_rate_per_sec: bulk_rate,
            rss_before_bytes: rss_before,
            rss_after_bytes: rss_after,
            rss_delta_bytes: rss_delta,
        });

        report.memory_snapshots.push(MemorySnapshot {
            stage: format!("bulk_insert_{}", total_inserted),
            rss_bytes: rss_after,
            rss_human: format_bytes(rss_after),
            alive_count: engine.alive_count(),
        });
        println!();

        // Keep the bulk engine for query/update phases if those stages are also requested
        bulk_engine = Some(engine);
    }

    // -----------------------------------------------------------------------
    // Phase 3: Build the full engine (streaming from file)
    // If bulk was already run, reuse that engine instead of rebuilding.
    // -----------------------------------------------------------------------
    let mut engine = if let Some(be) = bulk_engine {
        println!("--- Reusing bulk-loaded engine for update/query benchmarks ---");
        println!("  Alive: {}", be.alive_count());
        println!("  RSS: {}", format_bytes(rss_bytes()));
        println!();

        report.memory_snapshots.push(MemorySnapshot {
            stage: "full_engine (from bulk)".into(),
            rss_bytes: rss_bytes(),
            rss_human: format_bytes(rss_bytes()),
            alive_count: be.alive_count(),
        });

        print_bitmap_memory(&be);
        be
    } else {
        println!("--- Building full engine for update/query benchmarks ---");
        let engine = create_concurrent_engine(civitai_config(), &bench_dir, "full_engine", args.in_memory_docstore);
        engine.enter_loading_mode();
        let build_start = Instant::now();
        let mut build_counter = 0u32;
        stream_records(&args.data_path, limit, |rec| {
            let id = if args.remap_ids { let v = build_counter; build_counter += 1; v } else { rec.id as u32 };
            let doc = rec.to_document();
            engine.put(id, &doc).unwrap();
        });
        engine.exit_loading_mode();
        wait_for_flush(&engine, total_records as u64, 60_000);
        let build_elapsed = build_start.elapsed();
        let rss = rss_bytes();
        println!("  Loaded {} records in {:.2}s", total_records, build_elapsed.as_secs_f64());
        println!("  Alive: {}", engine.alive_count());
        println!("  RSS: {}", format_bytes(rss));
        println!();

        report.memory_snapshots.push(MemorySnapshot {
            stage: "full_engine".into(),
            rss_bytes: rss,
            rss_human: format_bytes(rss),
            alive_count: engine.alive_count(),
        });

        // Bitmap memory breakdown (excludes redb, allocator, channels — pure Bitdex)
        print_bitmap_memory(&engine);
        engine
    };

    // -----------------------------------------------------------------------
    // Phase: Persist — save engine bitmap snapshot to disk
    // -----------------------------------------------------------------------
    if should_run(&args.stages, "persist") {
        println!("--- Phase: Persist (save bitmap snapshot) ---");
        let alive_before = engine.alive_count();
        let persist_start = Instant::now();
        engine.save_snapshot_to(&persist_path).unwrap();
        let persist_elapsed = persist_start.elapsed();

        let file_size = dir_size(&persist_path);
        println!("  Saved {} alive in {:.2}s (bitmaps dir: {})",
            alive_before, persist_elapsed.as_secs_f64(), format_bytes(file_size));
        println!("  RSS: {}", format_bytes(rss_bytes()));
        println!();
    }

    // -----------------------------------------------------------------------
    // Phase: Restore — drop engine, rebuild from bitmap snapshot
    // -----------------------------------------------------------------------
    if should_run(&args.stages, "restore") {
        println!("--- Phase: Restore (load from bitmap snapshot) ---");

        if !persist_path.exists() {
            eprintln!("  ERROR: no bitmaps dir found at {}. Run with 'persist' stage first.", persist_path.display());
        } else {
            let rss_before = rss_bytes();

            // Create a new engine with bitmap_path pointing to the snapshot
            let mut restore_config = civitai_config();
            restore_config.storage.bitmap_path = Some(persist_path.clone());

            let restore_start = Instant::now();
            let restored = if args.in_memory_docstore {
                ConcurrentEngine::new(restore_config).unwrap()
            } else {
                let db_path = bench_dir.join("restored_docs");
                ConcurrentEngine::new_with_path(restore_config, &db_path).unwrap()
            };
            let restore_elapsed = restore_start.elapsed();

            // Replace the old engine (implicitly drops it)
            engine = restored;

            let rss_restored = rss_bytes();
            println!("  Restored {} alive in {:.2}s (was {} RSS before)",
                engine.alive_count(), restore_elapsed.as_secs_f64(), format_bytes(rss_before));
            println!("  RSS: {}", format_bytes(rss_restored));
            println!();

            report.memory_snapshots.push(MemorySnapshot {
                stage: "restored_engine".into(),
                rss_bytes: rss_restored,
                rss_human: format_bytes(rss_restored),
                alive_count: engine.alive_count(),
            });

            print_bitmap_memory(&engine);
        }
    }

    // -----------------------------------------------------------------------
    // Phase 4: Update/re-insert benchmark (re-reads file from top)
    // -----------------------------------------------------------------------
    if should_run(&args.stages, "update") {
        println!("--- Phase 4: Update (Increment reactionCount) Benchmark ---");

        let update_count = total_records.min(100_000);
        let mut update_time = Duration::ZERO;
        let mut update_counter = 0u32;
        let wall_start = Instant::now();
        stream_records(&args.data_path, update_count, |rec| {
            let id = if args.remap_ids { let v = update_counter; update_counter += 1; v } else { rec.id as u32 };
            let mut doc = rec.to_document();
            // Increment reactionCount by 1 to exercise sort layer XOR diff
            if let Some(FieldValue::Single(Value::Integer(ref mut v))) = doc.fields.get_mut("reactionCount") {
                *v += 1;
            }
            let put_start = Instant::now();
            engine.put(id, &doc).unwrap();
            update_time += put_start.elapsed();
        });
        wait_for_flush(&engine, total_records as u64, 30_000);
        let wall_elapsed = wall_start.elapsed();
        let update_rate = update_count as f64 / update_time.as_secs_f64();

        println!("  Updated {} records in {:.2}s (wall: {:.2}s) ({:.0}/s)",
            update_count, update_time.as_secs_f64(), wall_elapsed.as_secs_f64(), update_rate);
        println!("  Alive after upsert: {} (should be unchanged)", engine.alive_count());
        println!();

        report.update_benchmark = Some(UpdateBenchmark {
            record_count: update_count,
            elapsed_ms: update_time.as_secs_f64() * 1000.0,
            rate_per_sec: update_rate,
        });
    }

    // -----------------------------------------------------------------------
    // Phase 5: Query benchmarks
    // -----------------------------------------------------------------------
    if should_run(&args.stages, "query") {
        println!("--- Phase 5: Query Benchmarks ---");
        println!();

        // Quick streaming pass to collect frequency stats for realistic queries.
        let mut user_freq: HashMap<i64, usize> = HashMap::new();
        let mut tag_freq: HashMap<i64, usize> = HashMap::new();
        let mut sample_tag_ids: Vec<i64> = Vec::new();
        let sample_limit = 100_000.min(total_records);

        stream_records(&args.data_path, sample_limit, |rec| {
            if let Some(uid) = rec.user_id {
                *user_freq.entry(uid as i64).or_default() += 1;
            }
            if let Some(ref tags) = rec.tag_ids {
                for &t in tags {
                    *tag_freq.entry(t as i64).or_default() += 1;
                    if sample_tag_ids.len() < 500 {
                        sample_tag_ids.push(t as i64);
                    }
                }
            }
        });

        let frequent_user_id = user_freq.iter()
            .max_by_key(|(_, &count)| count)
            .map(|(&uid, _)| uid)
            .unwrap_or(1);

        let popular_tag = tag_freq.iter()
            .max_by_key(|(_, &count)| count)
            .map(|(&tid, _)| tid)
            .unwrap_or(304);
        let medium_tag = tag_freq.iter()
            .find(|(_, &count)| count > 100 && count < 5000)
            .map(|(&tid, _)| tid)
            .unwrap_or(5133);

        let iterations = 200;

        struct QuerySpec {
            name: &'static str,
            description: &'static str,
            filters: Vec<FilterClause>,
            sort: Option<SortClause>,
            limit: usize,
        }

        let queries = vec![
            // --- Filter-only queries ---
            QuerySpec {
                name: "filter_eq_nsfwLevel_1",
                description: "Single eq filter on low-cardinality field (nsfwLevel=1)",
                filters: vec![FilterClause::Eq("nsfwLevel".into(), Value::Integer(1))],
                sort: None,
                limit: 50,
            },
            QuerySpec {
                name: "filter_eq_onSite_true",
                description: "Boolean filter (onSite=true)",
                filters: vec![FilterClause::Eq("onSite".into(), Value::Bool(true))],
                sort: None,
                limit: 50,
            },
            QuerySpec {
                name: "filter_eq_userId",
                description: "Single eq on high-cardinality field (userId)",
                filters: vec![FilterClause::Eq("userId".into(), Value::Integer(frequent_user_id))],
                sort: None,
                limit: 50,
            },
            QuerySpec {
                name: "filter_eq_tagId_popular",
                description: "Single tag filter on popular tag",
                filters: vec![FilterClause::Eq("tagIds".into(), Value::Integer(popular_tag))],
                sort: None,
                limit: 50,
            },
            QuerySpec {
                name: "filter_and_2_clauses",
                description: "AND of nsfwLevel + onSite",
                filters: vec![
                    FilterClause::Eq("nsfwLevel".into(), Value::Integer(1)),
                    FilterClause::Eq("onSite".into(), Value::Bool(true)),
                ],
                sort: None,
                limit: 50,
            },
            QuerySpec {
                name: "filter_and_3_clauses",
                description: "AND of nsfwLevel + onSite + popular tag",
                filters: vec![
                    FilterClause::Eq("nsfwLevel".into(), Value::Integer(1)),
                    FilterClause::Eq("onSite".into(), Value::Bool(true)),
                    FilterClause::Eq("tagIds".into(), Value::Integer(popular_tag)),
                ],
                sort: None,
                limit: 50,
            },
            QuerySpec {
                name: "filter_and_3_with_userId",
                description: "AND of nsfwLevel + onSite + userId (narrow result)",
                filters: vec![
                    FilterClause::Eq("nsfwLevel".into(), Value::Integer(1)),
                    FilterClause::Eq("onSite".into(), Value::Bool(true)),
                    FilterClause::Eq("userId".into(), Value::Integer(frequent_user_id)),
                ],
                sort: None,
                limit: 50,
            },
            QuerySpec {
                name: "filter_in_nsfwLevel",
                description: "IN filter on nsfwLevel with multiple values",
                filters: vec![FilterClause::In("nsfwLevel".into(), vec![
                    Value::Integer(1), Value::Integer(2), Value::Integer(4),
                ])],
                sort: None,
                limit: 50,
            },
            QuerySpec {
                name: "filter_not_eq_nsfwLevel",
                description: "NOT nsfwLevel=28 (large result set via andnot)",
                filters: vec![FilterClause::NotEq("nsfwLevel".into(), Value::Integer(28))],
                sort: None,
                limit: 50,
            },
            // --- Filter + sort queries ---
            QuerySpec {
                name: "sort_reactionCount_desc",
                description: "All records sorted by reactionCount descending",
                filters: vec![],
                sort: Some(SortClause { field: "reactionCount".into(), direction: SortDirection::Desc }),
                limit: 50,
            },
            QuerySpec {
                name: "filter_nsfw1_sort_reactions",
                description: "nsfwLevel=1 sorted by reactionCount desc",
                filters: vec![FilterClause::Eq("nsfwLevel".into(), Value::Integer(1))],
                sort: Some(SortClause { field: "reactionCount".into(), direction: SortDirection::Desc }),
                limit: 50,
            },
            QuerySpec {
                name: "filter_nsfw1_onSite_sort_reactions",
                description: "nsfwLevel=1 + onSite sorted by reactionCount desc",
                filters: vec![
                    FilterClause::Eq("nsfwLevel".into(), Value::Integer(1)),
                    FilterClause::Eq("onSite".into(), Value::Bool(true)),
                ],
                sort: Some(SortClause { field: "reactionCount".into(), direction: SortDirection::Desc }),
                limit: 50,
            },
            QuerySpec {
                name: "filter_tag_sort_reactions",
                description: "Popular tag sorted by reactionCount desc",
                filters: vec![FilterClause::Eq("tagIds".into(), Value::Integer(popular_tag))],
                sort: Some(SortClause { field: "reactionCount".into(), direction: SortDirection::Desc }),
                limit: 50,
            },
            QuerySpec {
                name: "filter_3_clauses_sort_reactions",
                description: "nsfwLevel=1 + onSite + tag sorted by reactionCount",
                filters: vec![
                    FilterClause::Eq("nsfwLevel".into(), Value::Integer(1)),
                    FilterClause::Eq("onSite".into(), Value::Bool(true)),
                    FilterClause::Eq("tagIds".into(), Value::Integer(popular_tag)),
                ],
                sort: Some(SortClause { field: "reactionCount".into(), direction: SortDirection::Desc }),
                limit: 50,
            },
            QuerySpec {
                name: "filter_sort_commentCount",
                description: "nsfwLevel=1 sorted by commentCount desc",
                filters: vec![FilterClause::Eq("nsfwLevel".into(), Value::Integer(1))],
                sort: Some(SortClause { field: "commentCount".into(), direction: SortDirection::Desc }),
                limit: 50,
            },
            QuerySpec {
                name: "filter_sort_id_asc",
                description: "nsfwLevel=1 sorted by id ascending (newest last)",
                filters: vec![FilterClause::Eq("nsfwLevel".into(), Value::Integer(1))],
                sort: Some(SortClause { field: "id".into(), direction: SortDirection::Asc }),
                limit: 50,
            },
            // --- Queries with repeated prefixes (cache testing) ---
            QuerySpec {
                name: "prefix_shared_A",
                description: "[Cache prefix] nsfwLevel=1 + onSite + tag(popular)",
                filters: vec![
                    FilterClause::Eq("nsfwLevel".into(), Value::Integer(1)),
                    FilterClause::Eq("onSite".into(), Value::Bool(true)),
                    FilterClause::Eq("tagIds".into(), Value::Integer(popular_tag)),
                ],
                sort: Some(SortClause { field: "reactionCount".into(), direction: SortDirection::Desc }),
                limit: 50,
            },
            QuerySpec {
                name: "prefix_shared_B",
                description: "[Cache prefix] nsfwLevel=1 + onSite + tag(medium)",
                filters: vec![
                    FilterClause::Eq("nsfwLevel".into(), Value::Integer(1)),
                    FilterClause::Eq("onSite".into(), Value::Bool(true)),
                    FilterClause::Eq("tagIds".into(), Value::Integer(medium_tag)),
                ],
                sort: Some(SortClause { field: "reactionCount".into(), direction: SortDirection::Desc }),
                limit: 50,
            },
            QuerySpec {
                name: "prefix_shared_C",
                description: "[Cache prefix] nsfwLevel=1 + onSite + userId",
                filters: vec![
                    FilterClause::Eq("nsfwLevel".into(), Value::Integer(1)),
                    FilterClause::Eq("onSite".into(), Value::Bool(true)),
                    FilterClause::Eq("userId".into(), Value::Integer(frequent_user_id)),
                ],
                sort: Some(SortClause { field: "reactionCount".into(), direction: SortDirection::Desc }),
                limit: 50,
            },
            // --- Wide OR query ---
            QuerySpec {
                name: "filter_or_3_tags",
                description: "OR of 3 different tags sorted by reactionCount",
                filters: vec![FilterClause::Or(vec![
                    FilterClause::Eq("tagIds".into(), Value::Integer(popular_tag)),
                    FilterClause::Eq("tagIds".into(), Value::Integer(medium_tag)),
                    FilterClause::Eq("tagIds".into(), Value::Integer(
                        sample_tag_ids.get(10).copied().unwrap_or(304)
                    )),
                ])],
                sort: Some(SortClause { field: "reactionCount".into(), direction: SortDirection::Desc }),
                limit: 50,
            },
        ];

        // Warm-up: run each query 10 times to populate unified cache
        let warmup_passes = 10;
        println!("  Warming up ({} passes x {} queries)...", warmup_passes, queries.len());
        for _ in 0..warmup_passes {
            for q in &queries {
                let _ = engine.query(&q.filters, q.sort.as_ref(), q.limit);
            }
        }
        println!();

        // Run benchmarks
        println!("  {:<40} {:>8} {:>8} {:>8} {:>8} {:>8}",
            "Query", "p50", "p95", "p99", "mean", "count");
        println!("  {}", "-".repeat(82));

        for q in &queries {
            let mut durations = Vec::with_capacity(iterations);
            for _ in 0..iterations {
                let start = Instant::now();
                let result = engine.query(&q.filters, q.sort.as_ref(), q.limit);
                let elapsed = start.elapsed();
                // Ensure the query succeeded
                let _ = result.unwrap();
                durations.push(elapsed);
            }

            let stats = compute_stats(durations);

            println!("  {:<40} {:>7.3} {:>7.3} {:>7.3} {:>7.3}ms {:>5}",
                q.name,
                stats.p50_ms,
                stats.p95_ms,
                stats.p99_ms,
                stats.mean_ms,
                stats.count,
            );

            report.query_benchmarks.push(QueryBenchmark {
                name: q.name.to_string(),
                description: q.description.to_string(),
                iterations,
                stats,
            });
        }
        println!();

        // Show bitmap memory after queries (unified cache populated)
        print_bitmap_memory(&engine);

        // -------------------------------------------------------------------
        // Phase 5b: Unified Cache Effectiveness (cold vs warm)
        //
        // Measures the speedup from unified cache on sort queries.
        // Clear the cache, run each sort query once (cold), then run it
        // again with the cache populated.
        // -------------------------------------------------------------------
        println!("--- Phase 5b: Unified Cache Effectiveness (cold vs warm) ---");
        println!();

        engine.clear_unified_cache();

        struct BoundTestSpec {
            name: &'static str,
            filters: Vec<FilterClause>,
            sort: SortClause,
            limit: usize,
        }

        let bound_tests = vec![
            BoundTestSpec {
                name: "all_sort_reactions",
                filters: vec![],
                sort: SortClause { field: "reactionCount".into(), direction: SortDirection::Desc },
                limit: 50,
            },
            BoundTestSpec {
                name: "nsfw1_sort_reactions",
                filters: vec![FilterClause::Eq("nsfwLevel".into(), Value::Integer(1))],
                sort: SortClause { field: "reactionCount".into(), direction: SortDirection::Desc },
                limit: 50,
            },
            BoundTestSpec {
                name: "nsfw1_onSite_sort_reactions",
                filters: vec![
                    FilterClause::Eq("nsfwLevel".into(), Value::Integer(1)),
                    FilterClause::Eq("onSite".into(), Value::Bool(true)),
                ],
                sort: SortClause { field: "reactionCount".into(), direction: SortDirection::Desc },
                limit: 50,
            },
            BoundTestSpec {
                name: "tag_sort_reactions",
                filters: vec![FilterClause::Eq("tagIds".into(), Value::Integer(popular_tag))],
                sort: SortClause { field: "reactionCount".into(), direction: SortDirection::Desc },
                limit: 50,
            },
            BoundTestSpec {
                name: "nsfw1_sort_commentCount",
                filters: vec![FilterClause::Eq("nsfwLevel".into(), Value::Integer(1))],
                sort: SortClause { field: "commentCount".into(), direction: SortDirection::Desc },
                limit: 50,
            },
            BoundTestSpec {
                name: "nsfw1_sort_id_asc",
                filters: vec![FilterClause::Eq("nsfwLevel".into(), Value::Integer(1))],
                sort: SortClause { field: "id".into(), direction: SortDirection::Asc },
                limit: 50,
            },
        ];

        println!("  {:<36} {:>8} {:>8} {:>8} {:>8}",
            "Query", "cold", "warm p50", "warm p95", "speedup");
        println!("  {}", "-".repeat(72));

        for bt in &bound_tests {
            // Cold: no cached results
            let cold_start = Instant::now();
            let _ = engine.query(&bt.filters, Some(&bt.sort), bt.limit).unwrap();
            let cold_ms = cold_start.elapsed().as_secs_f64() * 1000.0;

            // Warm: bound was just formed, subsequent queries benefit
            let warm_iters = 100;
            let mut warm_durations = Vec::with_capacity(warm_iters);
            for _ in 0..warm_iters {
                let start = Instant::now();
                let _ = engine.query(&bt.filters, Some(&bt.sort), bt.limit).unwrap();
                warm_durations.push(start.elapsed());
            }
            let warm_stats = compute_stats(warm_durations);
            let speedup = cold_ms / warm_stats.p50_ms;

            println!("  {:<36} {:>7.3} {:>7.3} {:>7.3}ms {:>7.1}x",
                bt.name, cold_ms, warm_stats.p50_ms, warm_stats.p95_ms, speedup);
        }
        println!();

        // Report unified cache stats after effectiveness test
        {
            let uc = engine.unified_cache_stats();
            println!("  Unified cache after effectiveness test: {} entries, {} hits, {} misses",
                uc.entries, uc.hits, uc.misses);
        }
        println!();

        // -------------------------------------------------------------------
        // Phase 5c: Deep Pagination Benchmark
        //
        // Tests cursor-based pagination through 10 pages. Measures per-page
        // latency to verify tiered bounds maintain performance at depth.
        // -------------------------------------------------------------------
        println!("--- Phase 5c: Deep Pagination (cursor through 10 pages) ---");
        println!();

        let pagination_filters = vec![
            FilterClause::Eq("nsfwLevel".into(), Value::Integer(1)),
            FilterClause::Eq("onSite".into(), Value::Bool(true)),
        ];
        let pagination_sort = SortClause {
            field: "reactionCount".into(),
            direction: SortDirection::Desc,
        };
        let page_size = 50;

        println!("  Filters: nsfwLevel=1 AND onSite=true");
        println!("  Sort: reactionCount DESC, page_size={}", page_size);
        println!();
        println!("  {:>6} {:>8} {:>10} {:>14}",
            "Page", "latency", "results", "cursor_value");
        println!("  {}", "-".repeat(44));

        let snap = engine.snapshot_public();
        let sort_field = snap.sorts.get_field("reactionCount").unwrap();
        let mut cursor: Option<CursorPosition> = None;

        for page in 1..=10 {
            let query = BitdexQuery {
                filters: pagination_filters.clone(),
                sort: Some(pagination_sort.clone()),
                limit: page_size,
                cursor: cursor.clone(),
                offset: None,
                skip_cache: false,
            };

            let start = Instant::now();
            let result = engine.execute_query(&query).unwrap();
            let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

            let result_count = result.ids.len();

            if let Some(&last_id) = result.ids.last() {
                let last_slot = last_id as u32;
                let sv = sort_field.reconstruct_value(last_slot);
                println!("  {:>6} {:>7.3}ms {:>10} {:>14}",
                    page, elapsed_ms, result_count, sv);
                cursor = Some(CursorPosition {
                    sort_value: sv as u64,
                    slot_id: last_slot,
                });
            } else {
                println!("  {:>6} {:>7.3}ms {:>10} {:>14}",
                    page, elapsed_ms, result_count, "-");
                break; // No more results
            }

            if result_count < page_size {
                break; // Partial page = end of results
            }
        }
        drop(snap);

        // Report unified cache stats after pagination
        {
            let uc = engine.unified_cache_stats();
            println!();
            println!("  Unified cache after pagination: {} entries, {} hits, {} misses",
                uc.entries, uc.hits, uc.misses);
        }
        println!();
    }

    // -----------------------------------------------------------------------
    // Phase 6: Mixed read/write benchmark (ConcurrentEngine)
    // Some threads insert while others query concurrently
    // -----------------------------------------------------------------------
    if args.threads > 1 && should_run(&args.stages, "mixed") {
        println!("--- Phase 6: Mixed Read/Write Benchmark ({} threads, ConcurrentEngine) ---", args.threads);

        // Use half threads for writing, half for reading (min 1 each)
        let writer_threads = (args.threads / 2).max(1);
        let reader_threads = (args.threads - writer_threads).max(1);

        // Load a subset of records for writing (use 50K or total if less)
        let mixed_record_count = total_records.min(50_000);
        println!("  Loading {} records for mixed benchmark...", mixed_record_count);
        let records = load_records(&args.data_path, mixed_record_count, args.remap_ids);

        let engine = Arc::new(create_concurrent_engine(civitai_config(), &bench_dir, "mixed_rw", args.in_memory_docstore));

        // Pre-populate with half the records so readers have data to query
        let prepop_count = records.len() / 2;
        for (id, doc) in &records[..prepop_count] {
            engine.put(*id, doc).unwrap();
        }
        wait_for_flush(&engine, prepop_count as u64, 10_000);
        println!("  Pre-populated {} records, alive: {}", prepop_count, engine.alive_count());

        // The remaining records will be inserted by writers during the mixed phase
        let write_records: Vec<(u32, Document)> = records[prepop_count..].to_vec();
        let write_chunk_size = (write_records.len() + writer_threads - 1) / writer_threads;
        let write_chunks: Vec<Vec<(u32, Document)>> = write_records
            .chunks(write_chunk_size)
            .map(|c| c.to_vec())
            .collect();

        let total_queries = Arc::new(AtomicUsize::new(0));
        let total_writes = Arc::new(AtomicUsize::new(0));
        let all_query_durations: Arc<parking_lot::Mutex<Vec<Duration>>> =
            Arc::new(parking_lot::Mutex::new(Vec::new()));

        let stop_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));

        println!("  Running mixed workload: {} writer threads, {} reader threads...", writer_threads, reader_threads);
        let wall_start = Instant::now();

        // Spawn writer threads
        let mut handles = Vec::new();
        for chunk in write_chunks {
            let engine = Arc::clone(&engine);
            let counter = Arc::clone(&total_writes);
            let stop = Arc::clone(&stop_flag);
            handles.push(thread::spawn(move || {
                for (id, doc) in &chunk {
                    if stop.load(Ordering::Relaxed) { break; }
                    engine.put(*id, doc).unwrap();
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }

        // Spawn reader threads
        for _ in 0..reader_threads {
            let engine = Arc::clone(&engine);
            let counter = Arc::clone(&total_queries);
            let durations = Arc::clone(&all_query_durations);
            let stop = Arc::clone(&stop_flag);
            handles.push(thread::spawn(move || {
                let query_patterns: Vec<Vec<FilterClause>> = vec![
                    vec![FilterClause::Eq("nsfwLevel".into(), Value::Integer(1))],
                    vec![FilterClause::Eq("onSite".into(), Value::Bool(true))],
                    vec![
                        FilterClause::Eq("nsfwLevel".into(), Value::Integer(1)),
                        FilterClause::Eq("onSite".into(), Value::Bool(true)),
                    ],
                    vec![FilterClause::Eq("hasMeta".into(), Value::Bool(true))],
                ];
                let sort = SortClause {
                    field: "reactionCount".into(),
                    direction: SortDirection::Desc,
                };
                let mut local_durations = Vec::new();
                let mut idx = 0;
                while !stop.load(Ordering::Relaxed) {
                    let filters = &query_patterns[idx % query_patterns.len()];
                    let start = Instant::now();
                    let result = engine.query(filters, Some(&sort), 50);
                    let elapsed = start.elapsed();
                    let _ = result; // query may return partial results during concurrent writes
                    local_durations.push(elapsed);
                    counter.fetch_add(1, Ordering::Relaxed);
                    idx += 1;
                }
                durations.lock().extend(local_durations);
            }));
        }

        // Wait for writer threads to finish (they're bounded by the chunk size)
        // Reader threads run until stop_flag is set
        // Wait for the first N handles (writers)
        for h in handles.drain(..writer_threads.min(handles.len())) {
            h.join().unwrap();
        }

        // Signal readers to stop
        stop_flag.store(true, Ordering::Relaxed);
        for h in handles {
            h.join().unwrap();
        }

        let wall_elapsed = wall_start.elapsed();
        let writes = total_writes.load(Ordering::Relaxed);
        let queries = total_queries.load(Ordering::Relaxed);

        // Wait for flush
        wait_for_flush(&engine, (prepop_count + writes) as u64, 10_000);

        let insert_rate = writes as f64 / wall_elapsed.as_secs_f64();

        let query_durations = Arc::try_unwrap(all_query_durations)
            .unwrap_or_else(|arc| arc.lock().clone().into())
            .into_inner();

        println!("  Mixed workload complete:");
        println!("    Wall time:     {:.2}s", wall_elapsed.as_secs_f64());
        println!("    Records inserted: {} ({:.0} docs/s)", writes, insert_rate);
        println!("    Queries executed: {}", queries);
        println!("    Alive after:   {}", engine.alive_count());

        if !query_durations.is_empty() {
            let stats = compute_stats(query_durations);
            println!("    Query latency under concurrent writes:");
            println!("      p50: {:.3}ms  p95: {:.3}ms  p99: {:.3}ms  mean: {:.3}ms",
                stats.p50_ms, stats.p95_ms, stats.p99_ms, stats.mean_ms);

            report.mixed_rw_benchmark = Some(MixedRwBenchmark {
                writer_threads,
                reader_threads,
                records_inserted: writes,
                queries_executed: queries,
                wall_ms: wall_elapsed.as_secs_f64() * 1000.0,
                insert_rate_per_sec: insert_rate,
                query_stats: stats,
            });
        }
        println!();
    }

    // -----------------------------------------------------------------------
    // Phase 7: Realistic contention benchmark
    //
    // Models production traffic: slow trickle of new docs, moderate update
    // rate on reactionCount, and readers hammering at max rate with
    // randomized filters/sorts so the cache doesn't just absorb everything.
    // -----------------------------------------------------------------------
    if should_run(&args.stages, "contention") {
        println!("--- Phase 7: Realistic Contention Benchmark ---");

        let duration_secs = 15;
        let insert_target_per_sec = 15.0_f64;  // slow trickle of new docs
        let update_target_per_sec = 150.0_f64; // moderate reaction count churn
        let reader_thread_count = 4.max(args.threads.saturating_sub(2));

        println!("  Duration:       {}s", duration_secs);
        println!("  Insert target:  {:.0}/s (new docs)", insert_target_per_sec);
        println!("  Update target:  {:.0}/s (reactionCount++)", update_target_per_sec);
        println!("  Reader threads: {}", reader_thread_count);
        println!();

        // Build a ConcurrentEngine loaded with the full dataset
        println!("  Building ConcurrentEngine with full dataset...");
        let mut conc_config = civitai_config();
        if args.channel_capacity > 0 {
            conc_config.channel_capacity = args.channel_capacity;
        } else {
            conc_config.channel_capacity = (total_records * 50).max(100_000).min(10_000_000);
        }
        conc_config.flush_interval_us = args.flush_interval_us;
        let conc_engine = Arc::new(create_concurrent_engine(conc_config, &bench_dir, "contention", args.in_memory_docstore));

        let load_start = Instant::now();
        stream_records(&args.data_path, limit, |rec| {
            let id = rec.id as u32;
            let doc = rec.to_document();
            conc_engine.put(id, &doc).unwrap();
        });
        // Wait for full flush before measuring
        wait_for_flush(&conc_engine, total_records as u64, 60_000);
        println!("  Loaded in {:.2}s, alive: {}", load_start.elapsed().as_secs_f64(), conc_engine.alive_count());

        // Collect sample values for randomized queries
        let mut sample_nsfw_levels: Vec<i64> = Vec::new();
        let mut sample_user_ids: Vec<i64> = Vec::new();
        let mut sample_tags: Vec<i64> = Vec::new();
        let mut max_id: u32 = 0;

        stream_records(&args.data_path, 100_000.min(total_records), |rec| {
            if rec.id as u32 > max_id { max_id = rec.id as u32; }
            if let Some(v) = rec.nsfw_level {
                if sample_nsfw_levels.len() < 50 && !sample_nsfw_levels.contains(&(v as i64)) {
                    sample_nsfw_levels.push(v as i64);
                }
            }
            if let Some(v) = rec.user_id {
                if sample_user_ids.len() < 200 {
                    sample_user_ids.push(v as i64);
                }
            }
            if let Some(ref tags) = rec.tag_ids {
                for &t in tags {
                    if sample_tags.len() < 500 {
                        sample_tags.push(t as i64);
                    }
                }
            }
        });

        if sample_nsfw_levels.is_empty() { sample_nsfw_levels.push(1); }
        if sample_user_ids.is_empty() { sample_user_ids.push(1); }
        if sample_tags.is_empty() { sample_tags.push(304); }

        let sample_nsfw_levels = Arc::new(sample_nsfw_levels);
        let sample_user_ids = Arc::new(sample_user_ids);
        let sample_tags = Arc::new(sample_tags);

        let sort_fields: Arc<Vec<&str>> = Arc::new(vec![
            "reactionCount", "sortAt", "commentCount", "collectedCount", "id",
        ]);

        let alive_before = conc_engine.alive_count();
        let rss_before = rss_bytes();
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let insert_count = Arc::new(AtomicUsize::new(0));
        let update_count = Arc::new(AtomicUsize::new(0));
        let query_count = Arc::new(AtomicUsize::new(0));
        let query_durations: Arc<parking_lot::Mutex<Vec<Duration>>> =
            Arc::new(parking_lot::Mutex::new(Vec::new()));

        let mut handles = Vec::new();

        // --- Insert thread: slow trickle of new documents ---
        {
            let engine = Arc::clone(&conc_engine);
            let stop = Arc::clone(&stop);
            let counter = Arc::clone(&insert_count);
            let sleep_per_insert = Duration::from_secs_f64(1.0 / insert_target_per_sec);
            let start_id = max_id + 1_000_000; // well beyond existing IDs

            handles.push(thread::spawn(move || {
                let mut rng = rand::thread_rng();
                let mut id = start_id;
                while !stop.load(Ordering::Relaxed) {
                    let mut fields = HashMap::new();
                    fields.insert("nsfwLevel".into(), FieldValue::Single(Value::Integer(
                        *[1i64, 2, 4, 8, 16, 28, 32].get(rng.gen_range(0..7)).unwrap()
                    )));
                    fields.insert("onSite".into(), FieldValue::Single(Value::Bool(rng.gen_bool(0.7))));
                    fields.insert("hasMeta".into(), FieldValue::Single(Value::Bool(rng.gen_bool(0.5))));
                    fields.insert("reactionCount".into(), FieldValue::Single(Value::Integer(
                        rng.gen_range(0..500)
                    )));
                    fields.insert("commentCount".into(), FieldValue::Single(Value::Integer(
                        rng.gen_range(0..50)
                    )));
                    fields.insert("id".into(), FieldValue::Single(Value::Integer(id as i64)));
                    let doc = Document { fields };
                    let _ = engine.put(id, &doc);
                    counter.fetch_add(1, Ordering::Relaxed);
                    id += 1;
                    thread::sleep(sleep_per_insert);
                }
            }));
        }

        // --- Update thread: moderate rate reactionCount increments ---
        {
            let engine = Arc::clone(&conc_engine);
            let stop = Arc::clone(&stop);
            let counter = Arc::clone(&update_count);
            let sleep_per_update = Duration::from_secs_f64(1.0 / update_target_per_sec);
            // Collect a set of existing IDs to update (re-read from file)
            let mut update_ids: Vec<u32> = Vec::new();
            stream_records(&args.data_path, 50_000.min(total_records), |rec| {
                update_ids.push(rec.id as u32);
            });

            handles.push(thread::spawn(move || {
                let mut rng = rand::thread_rng();
                while !stop.load(Ordering::Relaxed) {
                    let idx = rng.gen_range(0..update_ids.len());
                    let id = update_ids[idx];
                    // Minimal update: just bump reactionCount
                    let mut fields = HashMap::new();
                    fields.insert("reactionCount".into(), FieldValue::Single(Value::Integer(
                        rng.gen_range(1..10_000)
                    )));
                    fields.insert("id".into(), FieldValue::Single(Value::Integer(id as i64)));
                    let doc = Document { fields };
                    let _ = engine.put(id, &doc);
                    counter.fetch_add(1, Ordering::Relaxed);
                    thread::sleep(sleep_per_update);
                }
            }));
        }

        // --- Reader threads: max rate, randomized queries ---
        for _ in 0..reader_thread_count {
            let engine = Arc::clone(&conc_engine);
            let stop = Arc::clone(&stop);
            let counter = Arc::clone(&query_count);
            let durations = Arc::clone(&query_durations);
            let nsfw = Arc::clone(&sample_nsfw_levels);
            let users = Arc::clone(&sample_user_ids);
            let tags = Arc::clone(&sample_tags);
            let sorts = Arc::clone(&sort_fields);

            handles.push(thread::spawn(move || {
                let mut rng = rand::thread_rng();
                let mut local_durations = Vec::with_capacity(100_000);

                while !stop.load(Ordering::Relaxed) {
                    // Build a randomized query
                    let num_clauses = rng.gen_range(1..=3);
                    let mut filters: Vec<FilterClause> = Vec::new();

                    for _ in 0..num_clauses {
                        let clause_type = rng.gen_range(0..9);
                        let clause = match clause_type {
                            0 => {
                                // nsfwLevel eq
                                let v = nsfw[rng.gen_range(0..nsfw.len())];
                                FilterClause::Eq("nsfwLevel".into(), Value::Integer(v))
                            }
                            1 => {
                                // tagId eq
                                let v = tags[rng.gen_range(0..tags.len())];
                                FilterClause::Eq("tagIds".into(), Value::Integer(v))
                            }
                            2 => {
                                // userId eq
                                let v = users[rng.gen_range(0..users.len())];
                                FilterClause::Eq("userId".into(), Value::Integer(v))
                            }
                            3 => {
                                // boolean filters
                                let field = match rng.gen_range(0..3) {
                                    0 => "onSite",
                                    1 => "hasMeta",
                                    _ => "minor",
                                };
                                FilterClause::Eq(field.into(), Value::Bool(rng.gen_bool(0.5)))
                            }
                            4 => {
                                // IN on nsfwLevel (2-4 random values)
                                let count = rng.gen_range(2..=4);
                                let vals: Vec<Value> = (0..count)
                                    .map(|_| Value::Integer(nsfw[rng.gen_range(0..nsfw.len())]))
                                    .collect();
                                FilterClause::In("nsfwLevel".into(), vals)
                            }
                            5 => {
                                // NOT eq on nsfwLevel
                                let v = nsfw[rng.gen_range(0..nsfw.len())];
                                FilterClause::NotEq("nsfwLevel".into(), Value::Integer(v))
                            }
                            6 => {
                                // Or: nsfwLevel IN or userId eq (mirrors Civitai shadow queries)
                                let count = rng.gen_range(2..=4);
                                let nsfw_vals: Vec<Value> = (0..count)
                                    .map(|_| Value::Integer(nsfw[rng.gen_range(0..nsfw.len())]))
                                    .collect();
                                let uid = users[rng.gen_range(0..users.len())];
                                FilterClause::Or(vec![
                                    FilterClause::In("nsfwLevel".into(), nsfw_vals),
                                    FilterClause::Eq("userId".into(), Value::Integer(uid)),
                                ])
                            }
                            7 => {
                                // Not(And(nsfwLevel IN, boolean)) — the pattern that was buggy
                                let count = rng.gen_range(2..=3);
                                let nsfw_vals: Vec<Value> = (0..count)
                                    .map(|_| Value::Integer(nsfw[rng.gen_range(0..nsfw.len())]))
                                    .collect();
                                let field = match rng.gen_range(0..2) {
                                    0 => "onSite",
                                    _ => "hasMeta",
                                };
                                FilterClause::Not(Box::new(FilterClause::And(vec![
                                    FilterClause::In("nsfwLevel".into(), nsfw_vals),
                                    FilterClause::Eq(field.into(), Value::Bool(rng.gen_bool(0.5))),
                                ])))
                            }
                            _ => {
                                // And(boolean, boolean) — simple compound
                                FilterClause::And(vec![
                                    FilterClause::Eq("onSite".into(), Value::Bool(rng.gen_bool(0.7))),
                                    FilterClause::Eq("hasMeta".into(), Value::Bool(rng.gen_bool(0.5))),
                                ])
                            }
                        };
                        filters.push(clause);
                    }

                    // Random sort
                    let sort_field = sorts[rng.gen_range(0..sorts.len())];
                    let direction = if rng.gen_bool(0.5) {
                        SortDirection::Desc
                    } else {
                        SortDirection::Asc
                    };
                    let sort = SortClause {
                        field: sort_field.to_string(),
                        direction,
                    };

                    let limit = *[20, 50, 100].get(rng.gen_range(0..3)).unwrap();

                    let start = Instant::now();
                    let _ = engine.query(&filters, Some(&sort), limit);
                    local_durations.push(start.elapsed());
                    counter.fetch_add(1, Ordering::Relaxed);
                }

                durations.lock().extend(local_durations);
            }));
        }

        // Let it run for the configured duration
        println!("  Running for {}s...", duration_secs);
        let bench_start = Instant::now();

        // Print progress every 3 seconds
        for tick in 1..=(duration_secs / 3) {
            thread::sleep(Duration::from_secs(3));
            let elapsed = bench_start.elapsed().as_secs();
            println!("    [{:>2}s] inserts: {}  updates: {}  queries: {}",
                elapsed,
                insert_count.load(Ordering::Relaxed),
                update_count.load(Ordering::Relaxed),
                query_count.load(Ordering::Relaxed),
            );
            let _ = tick;
        }

        // Sleep remaining time if any
        let remaining = Duration::from_secs(duration_secs as u64).saturating_sub(bench_start.elapsed());
        if !remaining.is_zero() {
            thread::sleep(remaining);
        }

        // Signal stop
        stop.store(true, Ordering::Relaxed);
        for h in handles {
            h.join().unwrap();
        }

        let wall_elapsed = bench_start.elapsed();
        let total_inserts = insert_count.load(Ordering::Relaxed);
        let total_updates = update_count.load(Ordering::Relaxed);
        let total_queries = query_count.load(Ordering::Relaxed);

        // Wait for flush to settle
        thread::sleep(Duration::from_millis(200));
        let alive_after = conc_engine.alive_count();
        let rss_after = rss_bytes();

        let all_durations = Arc::try_unwrap(query_durations)
            .unwrap_or_else(|arc| arc.lock().clone().into())
            .into_inner();

        println!();
        println!("  Realistic contention results:");
        println!("    Wall time:       {:.2}s", wall_elapsed.as_secs_f64());
        println!("    Inserts:         {} ({:.1}/s)", total_inserts,
            total_inserts as f64 / wall_elapsed.as_secs_f64());
        println!("    Updates:         {} ({:.1}/s)", total_updates,
            total_updates as f64 / wall_elapsed.as_secs_f64());
        println!("    Queries:         {} ({:.0}/s)", total_queries,
            total_queries as f64 / wall_elapsed.as_secs_f64());
        println!("    Alive:           {} -> {} (+{})", alive_before, alive_after,
            alive_after - alive_before);
        println!("    RSS:             {} -> {} (delta: {})",
            format_bytes(rss_before), format_bytes(rss_after),
            format_bytes(rss_after.saturating_sub(rss_before)));

        if !all_durations.is_empty() {
            let stats = compute_stats(all_durations);
            println!("    Query latency under contention:");
            println!("      p50: {:.3}ms  p95: {:.3}ms  p99: {:.3}ms  max: {:.3}ms  mean: {:.3}ms",
                stats.p50_ms, stats.p95_ms, stats.p99_ms, stats.max_ms, stats.mean_ms);

            report.contention_benchmark = Some(ContentionBenchmark {
                duration_secs: wall_elapsed.as_secs_f64(),
                reader_threads: reader_thread_count,
                total_queries,
                queries_per_sec: total_queries as f64 / wall_elapsed.as_secs_f64(),
                query_stats: stats,
                total_inserts,
                insert_rate_per_sec: total_inserts as f64 / wall_elapsed.as_secs_f64(),
                total_updates,
                update_rate_per_sec: total_updates as f64 / wall_elapsed.as_secs_f64(),
                alive_before,
                alive_after,
                rss_before_bytes: rss_before,
                rss_after_bytes: rss_after,
            });
        }

        report.memory_snapshots.push(MemorySnapshot {
            stage: "contention".into(),
            rss_bytes: rss_after,
            rss_human: format_bytes(rss_after),
            alive_count: alive_after,
        });

        println!();
    }

    // -----------------------------------------------------------------------
    // Final memory snapshot
    // -----------------------------------------------------------------------
    let final_rss = rss_bytes();
    report.memory_snapshots.push(MemorySnapshot {
        stage: "final".into(),
        rss_bytes: final_rss,
        rss_human: format_bytes(final_rss),
        alive_count: engine.alive_count(),
    });

    println!("--- Final State ---");
    println!("  Alive documents: {}", engine.alive_count());
    println!("  Slot counter:    {}", engine.slot_counter());
    println!("  RSS:             {}", format_bytes(final_rss));
    println!();

    // -----------------------------------------------------------------------
    // JSON output
    // -----------------------------------------------------------------------
    if args.json_output {
        let json = serde_json::to_string_pretty(&report).unwrap();
        let out_path = PathBuf::from("benchmark_report.json");
        std::fs::write(&out_path, &json).expect("Failed to write JSON report");
        println!("JSON report written to: {}", out_path.display());
    }

    // Clean up on-disk data (drop engine first to release file handles)
    drop(engine);
    if needs_bench_dir && bench_dir.exists() {
        std::fs::remove_dir_all(&bench_dir).ok();
    }

    println!("==========================================================");
    println!("  Benchmark complete.");
    println!("==========================================================");
}
