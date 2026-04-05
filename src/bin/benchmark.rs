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
use bitdex_v2::engine::ConcurrentEngine;
use bitdex_v2::config::{Config, FilterFieldConfig, SortFieldConfig};
use bitdex_v2::engine::filter::FilterFieldType;
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
            FilterFieldConfig { name: "nsfwLevel".into(), field_type: FilterFieldType::SingleValue, behaviors: None, eviction: None, eager_load: false, per_value_lazy: false },
            FilterFieldConfig { name: "userId".into(), field_type: FilterFieldType::SingleValue, behaviors: None, eviction: None, eager_load: false, per_value_lazy: false },
            FilterFieldConfig { name: "type".into(), field_type: FilterFieldType::SingleValue, behaviors: None, eviction: None, eager_load: false, per_value_lazy: false },
            FilterFieldConfig { name: "hasMeta".into(), field_type: FilterFieldType::Boolean, behaviors: None, eviction: None, eager_load: false, per_value_lazy: false },
            FilterFieldConfig { name: "onSite".into(), field_type: FilterFieldType::Boolean, behaviors: None, eviction: None, eager_load: false, per_value_lazy: false },
            FilterFieldConfig { name: "poi".into(), field_type: FilterFieldType::Boolean, behaviors: None, eviction: None, eager_load: false, per_value_lazy: false },
            FilterFieldConfig { name: "minor".into(), field_type: FilterFieldType::Boolean, behaviors: None, eviction: None, eager_load: false, per_value_lazy: false },
            FilterFieldConfig { name: "tagIds".into(), field_type: FilterFieldType::MultiValue, behaviors: None, eviction: None, eager_load: false, per_value_lazy: false },
            FilterFieldConfig { name: "modelVersionIds".into(), field_type: FilterFieldType::MultiValue, behaviors: None, eviction: None, eager_load: false, per_value_lazy: false },
            FilterFieldConfig { name: "toolIds".into(), field_type: FilterFieldType::MultiValue, behaviors: None, eviction: None, eager_load: false, per_value_lazy: false },
            FilterFieldConfig { name: "techniqueIds".into(), field_type: FilterFieldType::MultiValue, behaviors: None, eviction: None, eager_load: false, per_value_lazy: false },
        ],
        sort_fields: vec![
            SortFieldConfig { name: "reactionCount".into(), source_type: "uint32".into(), encoding: "linear".into(), bits: 32, eager_load: false, computed: None },
            SortFieldConfig { name: "sortAt".into(), source_type: "uint32".into(), encoding: "linear".into(), bits: 32, eager_load: false, computed: None },
            SortFieldConfig { name: "commentCount".into(), source_type: "uint32".into(), encoding: "linear".into(), bits: 32, eager_load: false, computed: None },
            SortFieldConfig { name: "collectedCount".into(), source_type: "uint32".into(), encoding: "linear".into(), bits: 32, eager_load: false, computed: None },
            SortFieldConfig { name: "id".into(), source_type: "uint32".into(), encoding: "linear".into(), bits: 32, eager_load: false, computed: None },
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
    println!("  Cache (on-disk silo):  {:>10}", format_bytes(cache_bytes as u64));
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
    // Phase 2: Insert benchmarks — removed. Direct put() on ConcurrentEngine is no longer
    // supported; all writes flow through the ops pipeline. Use the dump processor instead.
    if should_run(&args.stages, "insert") {
        println!("--- Phase 2: Insert Benchmarks (removed — use dump processor for bulk loads) ---");
    }
    // Phase 2b: Concurrent insert benchmark — removed (put() no longer exists).
    // -----------------------------------------------------------------------
    // Phase 2c: Bulk insert benchmark (removed — put_bulk_loading was deleted in Phase 6)
    // -----------------------------------------------------------------------
    let bulk_engine: Option<ConcurrentEngine> = None;
    if should_run(&args.stages, "bulk") {
        println!("--- Phase 2c: Bulk Insert Benchmark (removed — put_bulk_loading no longer exists) ---");
        println!("  Use the loader (PUT /dumps) or put() in a loop for bulk inserts.");
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
        // Build engine from BitmapSilo snapshot if available, else create empty.
        // Insert stages were removed — use the dump processor to populate data.
        println!("--- Building full engine for update/query benchmarks ---");
        let engine = create_concurrent_engine(civitai_config(), &bench_dir, "full_engine", args.in_memory_docstore);
        let rss = rss_bytes();
        println!("  Alive: {}", engine.alive_count());
        println!("  RSS: {}", format_bytes(rss));
        println!();
        report.memory_snapshots.push(MemorySnapshot {
            stage: "full_engine".into(),
            rss_bytes: rss,
            rss_human: format_bytes(rss),
            alive_count: engine.alive_count(),
        });
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
    // Phase 4: Update/re-insert benchmark — removed (put() no longer exists).
    if should_run(&args.stages, "update") {
        println!("--- Phase 4: Update Benchmark (removed — writes via ops pipeline only) ---");
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
        engine.clear_cache();
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
        // Cache stats removed — CacheSilo has no in-memory stats tracking
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
                let sv = engine.reconstruct_sort_value("reactionCount", last_slot)
                    .unwrap_or(0);
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
        // Cache stats removed — CacheSilo has no in-memory stats tracking
        println!();
    }
    // -----------------------------------------------------------------------
    // Phase 6: Mixed read/write benchmark — removed (put() no longer exists).
    if args.threads > 1 && should_run(&args.stages, "mixed") {
        println!("--- Phase 6: Mixed Read/Write Benchmark (removed — writes via ops pipeline only) ---");
    }
    // Phase 7: Realistic contention benchmark — removed (put() no longer exists).
    if should_run(&args.stages, "contention") {
        println!("--- Phase 7: Contention Benchmark (removed — writes via ops pipeline only) ---");
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
