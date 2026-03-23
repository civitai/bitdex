#![cfg(feature = "replay")]
//! bitdex-replay: Replay captured production traffic against a running BitDex server.
//!
//! Loads a caplog file (captured by the snapshot system) and fires requests at
//! a target server, collecting latency data and comparing results against the
//! original captured responses.
//!
//! ## Usage
//!
//! ```bash
//! cargo run --release --features server --bin bitdex-replay -- \
//!   --url http://localhost:3001 \
//!   --caplog captures/session-123/traffic.caplog \
//!   --mode realtime \
//!   --output replay_results/
//! ```
//!
//! ## Modes
//!
//! - `realtime`: Fire requests at original timestamps (reproduces concurrency patterns)
//! - `saturate`: Fire all requests as fast as possible (find throughput ceiling)
//! - `sequential`: One request at a time (isolate per-query performance)
//! - `stepped`: Pause between requests (attach profiler, collect flamegraphs)
//! - `window`: Replay only requests from a time window (--window 120s-150s)

use std::collections::HashMap;
use std::io::Read;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

// Re-use the CaptureEntry struct from the main crate
use bitdex_v2::capture::CaptureEntry;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct CliArgs {
    /// Target server URL (e.g., http://localhost:3001)
    url: String,
    /// Path to the caplog file
    caplog: PathBuf,
    /// Replay mode
    mode: ReplayMode,
    /// Output directory for results
    output: PathBuf,
    /// Optional time window filter (e.g., "120s-150s")
    window: Option<(f64, f64)>,
    /// Scrape Prometheus metrics before/after replay
    scrape_metrics: bool,
    /// Number of concurrent workers for saturate mode
    concurrency: usize,
    /// Step delay in milliseconds for stepped mode
    step_delay_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum ReplayMode {
    Realtime,
    Saturate,
    Sequential,
    Stepped,
    Window,
}

fn parse_args() -> CliArgs {
    let args: Vec<String> = std::env::args().collect();

    let mut url = "http://localhost:3001".to_string();
    let mut caplog = PathBuf::new();
    let mut mode = ReplayMode::Realtime;
    let mut output = PathBuf::from("replay_results");
    let mut window = None;
    let mut scrape_metrics = false;
    let mut concurrency = 16;
    let mut step_delay_ms = 1000;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--url" => { i += 1; url = args[i].clone(); }
            "--caplog" => { i += 1; caplog = PathBuf::from(&args[i]); }
            "--mode" => {
                i += 1;
                mode = match args[i].as_str() {
                    "realtime" => ReplayMode::Realtime,
                    "saturate" => ReplayMode::Saturate,
                    "sequential" => ReplayMode::Sequential,
                    "stepped" => ReplayMode::Stepped,
                    "window" => ReplayMode::Window,
                    other => {
                        eprintln!("Unknown mode: {other}. Using realtime.");
                        ReplayMode::Realtime
                    }
                };
            }
            "--output" => { i += 1; output = PathBuf::from(&args[i]); }
            "--window" => {
                i += 1;
                window = parse_window(&args[i]);
                if window.is_some() { mode = ReplayMode::Window; }
            }
            "--scrape-metrics" => { scrape_metrics = true; }
            "--concurrency" => { i += 1; concurrency = args[i].parse().unwrap_or(16); }
            "--step-delay" => { i += 1; step_delay_ms = args[i].parse().unwrap_or(1000); }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            _ => {
                eprintln!("Unknown argument: {}", args[i]);
            }
        }
        i += 1;
    }

    if caplog.as_os_str().is_empty() {
        eprintln!("Error: --caplog is required");
        print_usage();
        std::process::exit(1);
    }

    CliArgs { url, caplog, mode, output, window, scrape_metrics, concurrency, step_delay_ms }
}

fn parse_window(s: &str) -> Option<(f64, f64)> {
    // Parse "120s-150s" or "120-150" (seconds from capture start)
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 2 { return None; }
    let start = parts[0].trim_end_matches('s').parse::<f64>().ok()?;
    let end = parts[1].trim_end_matches('s').parse::<f64>().ok()?;
    Some((start, end))
}

fn print_usage() {
    eprintln!("bitdex-replay: Replay captured production traffic\n");
    eprintln!("Usage: bitdex-replay --caplog <path> [options]\n");
    eprintln!("Options:");
    eprintln!("  --url <url>          Target server (default: http://localhost:3001)");
    eprintln!("  --caplog <path>      Path to .caplog file (required)");
    eprintln!("  --mode <mode>        realtime|saturate|sequential|stepped|window");
    eprintln!("  --output <dir>       Output directory (default: replay_results/)");
    eprintln!("  --window <range>     Time window filter, e.g. '120s-150s'");
    eprintln!("  --scrape-metrics     Scrape /metrics before and after replay");
    eprintln!("  --concurrency <n>    Worker count for saturate mode (default: 16)");
    eprintln!("  --step-delay <ms>    Delay between requests in stepped mode (default: 1000)");
}

// ---------------------------------------------------------------------------
// Caplog reader
// ---------------------------------------------------------------------------

/// Read all CaptureEntry records from a caplog file.
/// Format: [u32 len][msgpack bytes] repeated.
fn read_caplog(path: &std::path::Path) -> std::io::Result<Vec<CaptureEntry>> {
    let data = std::fs::read(path)?;
    let mut entries = Vec::new();
    let mut offset = 0;

    while offset + 4 <= data.len() {
        let len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        if offset + len > data.len() {
            eprintln!("Warning: truncated caplog entry at offset {}, skipping rest", offset);
            break;
        }

        match rmp_serde::from_slice::<CaptureEntry>(&data[offset..offset + len]) {
            Ok(entry) => entries.push(entry),
            Err(e) => {
                eprintln!("Warning: failed to deserialize caplog entry at offset {}: {e}", offset);
            }
        }
        offset += len;
    }

    Ok(entries)
}

// ---------------------------------------------------------------------------
// Request execution
// ---------------------------------------------------------------------------

/// Result of replaying a single request.
#[derive(Debug, serde::Serialize)]
struct ReplayResult {
    /// Index of the request in the caplog.
    request_index: usize,
    /// Original arrival timestamp (ns from epoch).
    original_arrived_ns: u64,
    /// Original latency in microseconds.
    original_latency_us: u64,
    /// Replay latency in microseconds.
    replay_latency_us: u64,
    /// Whether HTTP status codes matched.
    status_match: bool,
    /// Original status code.
    original_status: u16,
    /// Replay status code.
    replay_status: u16,
    /// Whether response bodies matched (for queries: same IDs).
    result_match: bool,
    /// Request method + path for categorization.
    endpoint: String,
}

/// Fire a single HTTP request and measure latency.
fn execute_request(
    agent: &ureq::Agent,
    base_url: &str,
    entry: &CaptureEntry,
) -> Result<(u16, Vec<u8>, Duration), String> {
    let url = if entry.query_string.is_empty() {
        format!("{}{}", base_url, entry.path)
    } else {
        format!("{}{}?{}", base_url, entry.path, entry.query_string)
    };

    let start = Instant::now();
    let result = match entry.method.as_str() {
        "GET" => agent.get(&url).call(),
        "POST" => agent.post(&url)
            .set("Content-Type", "application/json")
            .send_bytes(&entry.body),
        "PATCH" => agent.patch(&url)
            .set("Content-Type", "application/json")
            .send_bytes(&entry.body),
        "DELETE" => {
            if entry.body.is_empty() {
                agent.delete(&url).call()
            } else {
                agent.delete(&url)
                    .set("Content-Type", "application/json")
                    .send_bytes(&entry.body)
            }
        }
        "PUT" => agent.put(&url)
            .set("Content-Type", "application/json")
            .send_bytes(&entry.body),
        other => return Err(format!("Unsupported method: {other}")),
    };

    let elapsed = start.elapsed();

    match result {
        Ok(resp) => {
            let status = resp.status();
            let mut body = Vec::new();
            resp.into_reader().read_to_end(&mut body).unwrap_or(0);
            Ok((status, body, elapsed))
        }
        Err(ureq::Error::Status(code, resp)) => {
            let mut body = Vec::new();
            resp.into_reader().read_to_end(&mut body).unwrap_or(0);
            Ok((code, body, elapsed))
        }
        Err(e) => Err(format!("Request failed: {e}")),
    }
}

// ---------------------------------------------------------------------------
// Replay modes
// ---------------------------------------------------------------------------

/// Replay in realtime mode: fire requests at their original timestamps.
fn replay_realtime(
    entries: &[CaptureEntry],
    base_url: &str,
) -> Vec<ReplayResult> {
    if entries.is_empty() {
        return Vec::new();
    }

    let agent = ureq::AgentBuilder::new()
        .timeout_connect(Duration::from_secs(10))
        .timeout_read(Duration::from_secs(30))
        .build();

    let base_ns = entries[0].arrived_at_ns;
    let replay_start = Instant::now();
    let mut results = Vec::with_capacity(entries.len());

    for (i, entry) in entries.iter().enumerate() {
        // Wait until the right time
        let target_offset = Duration::from_nanos(entry.arrived_at_ns.saturating_sub(base_ns));
        let now_offset = replay_start.elapsed();
        if target_offset > now_offset {
            let wait = target_offset - now_offset;
            // Use spin-wait for sub-ms precision
            if wait < Duration::from_millis(1) {
                let deadline = Instant::now() + wait;
                while Instant::now() < deadline {
                    std::hint::spin_loop();
                }
            } else {
                std::thread::sleep(wait - Duration::from_micros(500));
                // Spin for the remaining sub-ms
                let deadline = replay_start + target_offset;
                while Instant::now() < deadline {
                    std::hint::spin_loop();
                }
            }
        }

        let result = fire_and_compare(&agent, base_url, entry, i);
        if (i + 1) % 100 == 0 {
            let elapsed = replay_start.elapsed();
            eprintln!(
                "  [{}/{}] {:.1}s elapsed, last replay_us={}",
                i + 1, entries.len(), elapsed.as_secs_f64(),
                result.replay_latency_us
            );
        }
        results.push(result);
    }

    results
}

/// Replay in saturate mode: fire as fast as possible with concurrent workers.
fn replay_saturate(
    entries: &[CaptureEntry],
    base_url: &str,
    concurrency: usize,
) -> Vec<ReplayResult> {
    use std::sync::Mutex;

    let results = Arc::new(Mutex::new(Vec::with_capacity(entries.len())));
    let index = Arc::new(AtomicU64::new(0));
    let total = entries.len();

    let handles: Vec<_> = (0..concurrency)
        .map(|_| {
            let entries = entries.to_vec();
            let base_url = base_url.to_string();
            let results = Arc::clone(&results);
            let index = Arc::clone(&index);

            std::thread::spawn(move || {
                let agent = ureq::AgentBuilder::new()
                    .timeout_connect(Duration::from_secs(10))
                    .timeout_read(Duration::from_secs(30))
                    .build();

                loop {
                    let i = index.fetch_add(1, Ordering::Relaxed) as usize;
                    if i >= total {
                        break;
                    }
                    let result = fire_and_compare(&agent, &base_url, &entries[i], i);
                    results.lock().unwrap().push(result);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let mut results = Arc::try_unwrap(results).unwrap().into_inner().unwrap();
    results.sort_by_key(|r| r.request_index);
    results
}

/// Replay in sequential mode: one at a time, no waiting.
fn replay_sequential(
    entries: &[CaptureEntry],
    base_url: &str,
) -> Vec<ReplayResult> {
    let agent = ureq::AgentBuilder::new()
        .timeout_connect(Duration::from_secs(10))
        .timeout_read(Duration::from_secs(30))
        .build();

    entries.iter().enumerate().map(|(i, entry)| {
        let result = fire_and_compare(&agent, base_url, entry, i);
        if (i + 1) % 100 == 0 {
            eprintln!("  [{}/{}] replay_us={}", i + 1, entries.len(), result.replay_latency_us);
        }
        result
    }).collect()
}

/// Replay in stepped mode: pause between each request.
fn replay_stepped(
    entries: &[CaptureEntry],
    base_url: &str,
    step_delay: Duration,
) -> Vec<ReplayResult> {
    let agent = ureq::AgentBuilder::new()
        .timeout_connect(Duration::from_secs(10))
        .timeout_read(Duration::from_secs(30))
        .build();

    entries.iter().enumerate().map(|(i, entry)| {
        let result = fire_and_compare(&agent, base_url, entry, i);
        eprintln!(
            "  [{}/{}] {}: replay={}μs original={}μs status={}→{}",
            i + 1, entries.len(), result.endpoint,
            result.replay_latency_us, result.original_latency_us,
            result.original_status, result.replay_status
        );
        std::thread::sleep(step_delay);
        result
    }).collect()
}

/// Fire a request and compare with the original captured response.
fn fire_and_compare(
    agent: &ureq::Agent,
    base_url: &str,
    entry: &CaptureEntry,
    index: usize,
) -> ReplayResult {
    let original_latency_us = (entry.responded_at_ns - entry.arrived_at_ns) / 1_000;
    let endpoint = format!("{} {}", entry.method, entry.path);

    match execute_request(agent, base_url, entry) {
        Ok((status, body, elapsed)) => {
            let replay_latency_us = elapsed.as_micros() as u64;
            let status_match = status == entry.response_status;
            let result_match = body == entry.response_body;

            ReplayResult {
                request_index: index,
                original_arrived_ns: entry.arrived_at_ns,
                original_latency_us,
                replay_latency_us,
                status_match,
                original_status: entry.response_status,
                replay_status: status,
                result_match,
                endpoint,
            }
        }
        Err(e) => {
            eprintln!("  Request {} failed: {e}", index);
            ReplayResult {
                request_index: index,
                original_arrived_ns: entry.arrived_at_ns,
                original_latency_us,
                replay_latency_us: 0,
                status_match: false,
                original_status: entry.response_status,
                replay_status: 0,
                result_match: false,
                endpoint,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Analysis & output
// ---------------------------------------------------------------------------

/// Summary statistics for a replay run.
#[derive(Debug, serde::Serialize)]
struct ReplaySummary {
    mode: String,
    total_requests: usize,
    successful_requests: usize,
    status_match_pct: f64,
    result_match_pct: f64,
    original_latency: LatencyStats,
    replay_latency: LatencyStats,
    stalls: Vec<StallEvent>,
    wall_time_seconds: f64,
    effective_qps: f64,
}

#[derive(Debug, serde::Serialize)]
struct LatencyStats {
    p50_us: u64,
    p95_us: u64,
    p99_us: u64,
    max_us: u64,
    mean_us: f64,
}

#[derive(Debug, serde::Serialize)]
struct StallEvent {
    /// Seconds from replay start when the stall began.
    start_offset_s: f64,
    /// Duration of the stall in seconds.
    duration_s: f64,
    /// Number of requests that were in-flight or queued during the stall.
    requests_queued: usize,
}

fn compute_summary(
    results: &[ReplayResult],
    mode: &str,
    wall_time: Duration,
    entries: &[CaptureEntry],
) -> ReplaySummary {
    let total = results.len();
    let successful = results.iter().filter(|r| r.replay_status > 0).count();
    let status_matches = results.iter().filter(|r| r.status_match).count();
    let result_matches = results.iter().filter(|r| r.result_match).count();

    let mut original_latencies: Vec<u64> = results.iter().map(|r| r.original_latency_us).collect();
    let mut replay_latencies: Vec<u64> = results.iter().map(|r| r.replay_latency_us).collect();
    original_latencies.sort_unstable();
    replay_latencies.sort_unstable();

    let stalls = detect_stalls(results, entries);

    ReplaySummary {
        mode: mode.to_string(),
        total_requests: total,
        successful_requests: successful,
        status_match_pct: if total > 0 { status_matches as f64 / total as f64 * 100.0 } else { 0.0 },
        result_match_pct: if total > 0 { result_matches as f64 / total as f64 * 100.0 } else { 0.0 },
        original_latency: compute_latency_stats(&original_latencies),
        replay_latency: compute_latency_stats(&replay_latencies),
        stalls,
        wall_time_seconds: wall_time.as_secs_f64(),
        effective_qps: if wall_time.as_secs_f64() > 0.0 { total as f64 / wall_time.as_secs_f64() } else { 0.0 },
    }
}

fn compute_latency_stats(sorted: &[u64]) -> LatencyStats {
    if sorted.is_empty() {
        return LatencyStats { p50_us: 0, p95_us: 0, p99_us: 0, max_us: 0, mean_us: 0.0 };
    }
    let n = sorted.len();
    let sum: u64 = sorted.iter().sum();
    LatencyStats {
        p50_us: sorted[n * 50 / 100],
        p95_us: sorted[n * 95 / 100],
        p99_us: sorted[n * 99 / 100],
        max_us: sorted[n - 1],
        mean_us: sum as f64 / n as f64,
    }
}

/// Detect periods where no responses came back for >1 second.
fn detect_stalls(results: &[ReplayResult], entries: &[CaptureEntry]) -> Vec<StallEvent> {
    if entries.is_empty() || results.is_empty() {
        return Vec::new();
    }

    // Sort results by original arrival time
    let mut by_time: Vec<&ReplayResult> = results.iter().collect();
    by_time.sort_by_key(|r| r.original_arrived_ns);

    let base_ns = entries[0].arrived_at_ns;
    let mut stalls = Vec::new();
    let stall_threshold = Duration::from_secs(1);

    // Look for gaps in response completions > 1s
    let mut response_times: Vec<(u64, u64)> = results
        .iter()
        .filter(|r| r.replay_latency_us > 0)
        .map(|r| (r.original_arrived_ns, r.original_arrived_ns + r.replay_latency_us * 1000))
        .collect();
    response_times.sort_by_key(|&(_, end)| end);

    for window in response_times.windows(2) {
        let gap_ns = window[1].1.saturating_sub(window[0].1);
        if gap_ns > stall_threshold.as_nanos() as u64 {
            let start_offset = Duration::from_nanos(window[0].1 - base_ns);
            let duration = Duration::from_nanos(gap_ns);
            // Count requests that arrived during the gap
            let gap_start = window[0].1;
            let gap_end = window[1].1;
            let queued = results
                .iter()
                .filter(|r| r.original_arrived_ns >= gap_start && r.original_arrived_ns <= gap_end)
                .count();

            stalls.push(StallEvent {
                start_offset_s: start_offset.as_secs_f64(),
                duration_s: duration.as_secs_f64(),
                requests_queued: queued,
            });
        }
    }

    stalls
}

/// Write per_request.csv
fn write_per_request_csv(results: &[ReplayResult], output_dir: &std::path::Path) -> std::io::Result<()> {
    let path = output_dir.join("per_request.csv");
    let mut writer = std::io::BufWriter::new(std::fs::File::create(&path)?);
    use std::io::Write;
    writeln!(writer, "request_index,endpoint,original_us,replay_us,status_match,result_match,original_status,replay_status")?;
    for r in results {
        writeln!(
            writer,
            "{},{},{},{},{},{},{},{}",
            r.request_index, r.endpoint, r.original_latency_us, r.replay_latency_us,
            r.status_match, r.result_match, r.original_status, r.replay_status
        )?;
    }
    Ok(())
}

/// Scrape Prometheus metrics endpoint.
fn scrape_metrics(base_url: &str) -> Option<String> {
    let url = format!("{}/metrics", base_url.trim_end_matches('/'));
    let url = url.replace("/api/indexes", ""); // metrics is at root
    match ureq::get(&url).call() {
        Ok(resp) => {
            let mut body = String::new();
            resp.into_reader().read_to_string(&mut body).ok();
            Some(body)
        }
        Err(e) => {
            eprintln!("Warning: failed to scrape metrics: {e}");
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    let args = parse_args();

    println!("=== bitdex-replay ===");
    println!("  URL:    {}", args.url);
    println!("  Caplog: {}", args.caplog.display());
    println!("  Mode:   {:?}", args.mode);
    println!("  Output: {}", args.output.display());
    if let Some((start, end)) = args.window {
        println!("  Window: {start}s - {end}s");
    }
    println!();

    // Read caplog
    println!("Reading caplog...");
    let mut entries = match read_caplog(&args.caplog) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("Error reading caplog: {e}");
            std::process::exit(1);
        }
    };
    println!("  Loaded {} entries", entries.len());

    if entries.is_empty() {
        eprintln!("Error: caplog is empty");
        std::process::exit(1);
    }

    // Sort by arrival time
    entries.sort_by_key(|e| e.arrived_at_ns);

    // Apply window filter if specified
    if let Some((start_s, end_s)) = args.window {
        let base_ns = entries[0].arrived_at_ns;
        let start_ns = base_ns + (start_s * 1_000_000_000.0) as u64;
        let end_ns = base_ns + (end_s * 1_000_000_000.0) as u64;
        let before = entries.len();
        entries.retain(|e| e.arrived_at_ns >= start_ns && e.arrived_at_ns <= end_ns);
        println!("  Window filter: {before} → {} entries", entries.len());
    }

    // Print caplog stats
    let duration_ns = entries.last().unwrap().arrived_at_ns - entries[0].arrived_at_ns;
    let duration_s = duration_ns as f64 / 1_000_000_000.0;
    println!(
        "  Capture duration: {:.1}s, avg rate: {:.0} req/s",
        duration_s,
        if duration_s > 0.0 { entries.len() as f64 / duration_s } else { 0.0 }
    );

    // Categorize requests
    let mut categories: HashMap<String, usize> = HashMap::new();
    for e in &entries {
        *categories.entry(format!("{} {}", e.method, e.path)).or_default() += 1;
    }
    println!("  Request breakdown:");
    let mut cats: Vec<_> = categories.into_iter().collect();
    cats.sort_by(|a, b| b.1.cmp(&a.1));
    for (endpoint, count) in cats.iter().take(10) {
        println!("    {count:>6} {endpoint}");
    }
    println!();

    // Create output directory
    let run_id = chrono_run_id();
    let run_dir = args.output.join(&run_id);
    std::fs::create_dir_all(&run_dir).expect("Failed to create output directory");

    // Scrape metrics before
    let metrics_before = if args.scrape_metrics {
        println!("Scraping pre-replay metrics...");
        let m = scrape_metrics(&args.url);
        if let Some(ref text) = m {
            std::fs::write(run_dir.join("metrics_before.prom"), text).ok();
        }
        m
    } else {
        None
    };

    // Execute replay
    let mode_str = format!("{:?}", args.mode).to_lowercase();
    println!("Starting replay ({mode_str})...");
    let wall_start = Instant::now();

    let results = match args.mode {
        ReplayMode::Realtime | ReplayMode::Window => {
            replay_realtime(&entries, &args.url)
        }
        ReplayMode::Saturate => {
            replay_saturate(&entries, &args.url, args.concurrency)
        }
        ReplayMode::Sequential => {
            replay_sequential(&entries, &args.url)
        }
        ReplayMode::Stepped => {
            replay_stepped(&entries, &args.url, Duration::from_millis(args.step_delay_ms))
        }
    };

    let wall_time = wall_start.elapsed();
    println!("\nReplay complete in {:.1}s", wall_time.as_secs_f64());

    // Scrape metrics after
    if args.scrape_metrics {
        println!("Scraping post-replay metrics...");
        if let Some(text) = scrape_metrics(&args.url) {
            std::fs::write(run_dir.join("metrics_after.prom"), &text).ok();
        }
    }

    // Compute summary
    let summary = compute_summary(&results, &mode_str, wall_time, &entries);

    // Print summary
    println!("\n=== Summary ===");
    println!("  Requests: {} total, {} successful", summary.total_requests, summary.successful_requests);
    println!("  Status match: {:.1}%", summary.status_match_pct);
    println!("  Result match: {:.1}%", summary.result_match_pct);
    println!("  QPS: {:.0}", summary.effective_qps);
    println!();
    println!("  Original latency:  p50={}μs  p95={}μs  p99={}μs  max={}μs",
        summary.original_latency.p50_us, summary.original_latency.p95_us,
        summary.original_latency.p99_us, summary.original_latency.max_us);
    println!("  Replay latency:    p50={}μs  p95={}μs  p99={}μs  max={}μs",
        summary.replay_latency.p50_us, summary.replay_latency.p95_us,
        summary.replay_latency.p99_us, summary.replay_latency.max_us);

    if !summary.stalls.is_empty() {
        println!("\n  Stall events (>1s gaps):");
        for stall in &summary.stalls {
            println!(
                "    at {:.1}s: {:.2}s stall, {} requests queued",
                stall.start_offset_s, stall.duration_s, stall.requests_queued
            );
        }
    } else {
        println!("\n  No stall events detected (>1s gaps).");
    }

    // Write outputs
    let summary_path = run_dir.join("summary.json");
    let summary_json = serde_json::to_string_pretty(&summary).unwrap();
    std::fs::write(&summary_path, &summary_json).expect("Failed to write summary.json");
    println!("\nOutput: {}", run_dir.display());
    println!("  summary.json: latency comparison + stall detection");

    if let Err(e) = write_per_request_csv(&results, &run_dir) {
        eprintln!("Warning: failed to write per_request.csv: {e}");
    } else {
        println!("  per_request.csv: per-request latency comparison");
    }
}

fn chrono_run_id() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    format!("run-{}", now.as_secs())
}
