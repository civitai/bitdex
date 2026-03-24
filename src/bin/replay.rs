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
    /// Speed multiplier for realtime/window mode (1.0 = realtime, 2.0 = 2x faster)
    speed: f64,
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
    let mut speed = 1.0f64;

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
            "--speed" => {
                i += 1;
                speed = args[i].trim_end_matches('x').parse().unwrap_or(1.0);
                if speed <= 0.0 { speed = 1.0; }
            }
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

    CliArgs { url, caplog, mode, output, window, scrape_metrics, concurrency, step_delay_ms, speed }
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
    eprintln!("  --speed <mult>       Speed multiplier for realtime/window mode (default: 1.0)");
    eprintln!("                       Examples: 2.0 or 2x = 2x faster, 0.5 = half speed");
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
    /// Replay latency in microseconds (client-side: includes HTTP + thread pool overhead).
    replay_latency_us: u64,
    /// Server-reported processing time in microseconds (from X-BitDex-Duration-Us header).
    /// 0 if header not present (non-query endpoints or older server versions).
    server_duration_us: u64,
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
/// Returns (status, body, client_elapsed, server_duration_us).
fn execute_request(
    agent: &ureq::Agent,
    base_url: &str,
    entry: &CaptureEntry,
) -> Result<(u16, Vec<u8>, Duration, u64), String> {
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
            let server_us = resp.header("X-BitDex-Duration-Us")
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0);
            let mut body = Vec::new();
            resp.into_reader().read_to_end(&mut body).unwrap_or(0);
            Ok((status, body, elapsed, server_us))
        }
        Err(ureq::Error::Status(code, resp)) => {
            let server_us = resp.header("X-BitDex-Duration-Us")
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0);
            let mut body = Vec::new();
            resp.into_reader().read_to_end(&mut body).unwrap_or(0);
            Ok((code, body, elapsed, server_us))
        }
        Err(e) => Err(format!("Request failed: {e}")),
    }
}

// ---------------------------------------------------------------------------
// Replay modes
// ---------------------------------------------------------------------------

/// Replay in realtime mode: fire requests at their original timestamps with
/// concurrent dispatch. A scheduler thread sleeps until each entry's scheduled
/// time, then submits the request to a thread pool (non-blocking). Worker
/// threads execute requests in parallel and push results to a shared collector.
///
/// `speed` controls the replay rate: 1.0 = realtime, 2.0 = 2x faster, etc.
fn replay_realtime(
    entries: &[CaptureEntry],
    base_url: &str,
    speed: f64,
) -> Vec<ReplayResult> {
    use std::sync::Mutex;

    if entries.is_empty() {
        return Vec::new();
    }

    let speed = if speed <= 0.0 { 1.0 } else { speed };
    let base_ns = entries[0].arrived_at_ns;
    let replay_start = Instant::now();
    let results = Arc::new(Mutex::new(Vec::with_capacity(entries.len())));
    let in_flight = Arc::new(AtomicU64::new(0));
    let completed = Arc::new(AtomicU64::new(0));
    let total = entries.len();

    // Thread pool sized for peak concurrency. Production captures show ~91 req/s
    // with tails up to ~30s, so 64 threads covers worst-case in-flight count.
    let pool_size = 64usize;
    let (tx, rx) = crossbeam_channel::bounded::<(usize, CaptureEntry)>(pool_size * 2);

    // Spawn worker threads — each gets its own ureq Agent (connection pool).
    let handles: Vec<_> = (0..pool_size)
        .map(|_| {
            let rx = rx.clone();
            let base_url = base_url.to_string();
            let results = Arc::clone(&results);
            let in_flight = Arc::clone(&in_flight);
            let completed = Arc::clone(&completed);

            std::thread::spawn(move || {
                let agent = ureq::AgentBuilder::new()
                    .timeout_connect(Duration::from_secs(10))
                    .timeout_read(Duration::from_secs(30))
                    .build();

                while let Ok((index, entry)) = rx.recv() {
                    in_flight.fetch_add(1, Ordering::Relaxed);
                    let result = fire_and_compare(&agent, &base_url, &entry, index);
                    in_flight.fetch_sub(1, Ordering::Relaxed);
                    completed.fetch_add(1, Ordering::Relaxed);
                    results.lock().unwrap().push(result);
                }
            })
        })
        .collect();

    // Scheduler: iterate entries, wait until each entry's scheduled time, then
    // submit to the pool without blocking on the response.
    for (i, entry) in entries.iter().enumerate() {
        let target_offset_ns = entry.arrived_at_ns.saturating_sub(base_ns);
        let target_offset = Duration::from_nanos((target_offset_ns as f64 / speed) as u64);
        let now_offset = replay_start.elapsed();

        if target_offset > now_offset {
            let wait = target_offset - now_offset;
            if wait > Duration::from_millis(1) {
                // Sleep for the bulk, then spin-wait for sub-ms precision.
                std::thread::sleep(wait - Duration::from_micros(500));
            }
            // Spin-wait for the remainder.
            while replay_start.elapsed() < target_offset {
                std::hint::spin_loop();
            }
        }

        // Submit to thread pool (blocks only if the bounded channel is full,
        // which means 128 requests are already queued — back-pressure).
        let _ = tx.send((i, entry.clone()));

        // Progress every 500 requests.
        if (i + 1) % 500 == 0 {
            let elapsed = replay_start.elapsed();
            let done = completed.load(Ordering::Relaxed);
            let flying = in_flight.load(Ordering::Relaxed);
            eprintln!(
                "  [{}/{}] {:.1}s elapsed, {} completed, {} in-flight",
                i + 1, total, elapsed.as_secs_f64(), done, flying
            );
        }
    }

    // Drop sender to signal workers to drain remaining work and exit.
    drop(tx);

    for h in handles {
        h.join().unwrap();
    }

    let mut results = Arc::try_unwrap(results).unwrap().into_inner().unwrap();
    results.sort_by_key(|r| r.request_index);
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
///
/// On network errors, the actual elapsed time is still recorded so stall
/// detection and latency analysis reflect reality rather than showing 0.
fn fire_and_compare(
    agent: &ureq::Agent,
    base_url: &str,
    entry: &CaptureEntry,
    index: usize,
) -> ReplayResult {
    let original_latency_us = entry.responded_at_ns.saturating_sub(entry.arrived_at_ns) / 1_000;
    let endpoint = format!("{} {}", entry.method, entry.path);

    let start = Instant::now();
    match execute_request(agent, base_url, entry) {
        Ok((status, body, elapsed, server_us)) => {
            let replay_latency_us = elapsed.as_micros() as u64;
            let status_match = status == entry.response_status;
            let result_match = body == entry.response_body;

            ReplayResult {
                request_index: index,
                original_arrived_ns: entry.arrived_at_ns,
                original_latency_us,
                replay_latency_us,
                server_duration_us: server_us,
                status_match,
                original_status: entry.response_status,
                replay_status: status,
                result_match,
                endpoint,
            }
        }
        Err(e) => {
            let elapsed_us = start.elapsed().as_micros() as u64;
            eprintln!("  Request {} failed ({}us): {e}", index, elapsed_us);
            ReplayResult {
                request_index: index,
                original_arrived_ns: entry.arrived_at_ns,
                original_latency_us,
                replay_latency_us: elapsed_us,
                server_duration_us: 0,
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
    server_latency: LatencyStats,
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
    let mut server_latencies: Vec<u64> = results.iter().map(|r| r.server_duration_us).collect();
    original_latencies.sort_unstable();
    replay_latencies.sort_unstable();
    server_latencies.sort_unstable();

    let stalls = detect_stalls(results, entries);

    ReplaySummary {
        mode: mode.to_string(),
        total_requests: total,
        successful_requests: successful,
        status_match_pct: if total > 0 { status_matches as f64 / total as f64 * 100.0 } else { 0.0 },
        result_match_pct: if total > 0 { result_matches as f64 / total as f64 * 100.0 } else { 0.0 },
        original_latency: compute_latency_stats(&original_latencies),
        replay_latency: compute_latency_stats(&replay_latencies),
        server_latency: compute_latency_stats(&server_latencies),
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
    writeln!(writer, "request_index,endpoint,original_us,replay_us,server_us,client_overhead_us,status_match,result_match,original_status,replay_status")?;
    for r in results {
        let overhead = r.replay_latency_us.saturating_sub(r.server_duration_us);
        writeln!(
            writer,
            "{},{},{},{},{},{},{},{},{},{}",
            r.request_index, r.endpoint, r.original_latency_us, r.replay_latency_us,
            r.server_duration_us, overhead,
            r.status_match, r.result_match, r.original_status, r.replay_status
        )?;
    }
    Ok(())
}

/// Validate that the target server has real data loaded.
/// Returns (alive_count, bitmap_mb) on success, or error message on failure.
fn validate_server_data(base_url: &str) -> Result<(u64, f64), String> {
    // Try to find an index by listing indexes first
    let list_url = format!("{}/api/indexes", base_url.trim_end_matches('/'));
    let resp = ureq::get(&list_url).call()
        .map_err(|e| format!("Failed to reach server: {e}"))?;
    let mut body = String::new();
    resp.into_reader().read_to_string(&mut body)
        .map_err(|e| format!("Failed to read index list: {e}"))?;
    let list: serde_json::Value = serde_json::from_str(&body)
        .map_err(|e| format!("Failed to parse index list: {e}"))?;

    let indexes = list["indexes"].as_array()
        .ok_or("No indexes found on server")?;
    if indexes.is_empty() {
        return Err("No indexes loaded on server".to_string());
    }

    let index_name = indexes[0]["name"].as_str()
        .ok_or("Index has no name field")?;

    // Get stats for the first index
    let stats_url = format!("{}/api/indexes/{}/stats", base_url.trim_end_matches('/'), index_name);
    let resp = ureq::get(&stats_url).call()
        .map_err(|e| format!("Failed to get stats: {e}"))?;
    let mut body = String::new();
    resp.into_reader().read_to_string(&mut body)
        .map_err(|e| format!("Failed to read stats: {e}"))?;
    let stats: serde_json::Value = serde_json::from_str(&body)
        .map_err(|e| format!("Failed to parse stats: {e}"))?;

    let alive = stats["alive_count"].as_u64().unwrap_or(0);
    let filter_bytes = stats["filter_bitmap_bytes"].as_u64().unwrap_or(0);
    let sort_bytes = stats["sort_bitmap_bytes"].as_u64().unwrap_or(0);
    let bitmap_mb = (filter_bytes + sort_bytes) as f64 / (1024.0 * 1024.0);

    if alive == 0 {
        return Err(format!("Index '{}' has 0 alive documents — no data loaded", index_name));
    }
    if bitmap_mb < 1.0 {
        return Err(format!(
            "Index '{}' has {} alive docs but only {:.1} MB bitmaps — data may be incomplete",
            index_name, alive, bitmap_mb
        ));
    }

    Ok((alive, bitmap_mb))
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
    if args.speed != 1.0 {
        println!("  Speed:  {}x", args.speed);
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

    // Validate server has real data before running a benchmark
    println!("Validating server data...");
    match validate_server_data(&args.url) {
        Ok((alive, bitmap_mb)) => {
            println!("  Server OK: {} alive documents, {:.1} MB bitmaps", alive, bitmap_mb);
        }
        Err(e) => {
            eprintln!("ERROR: Server validation failed: {e}");
            eprintln!("The server may not have loaded data, or bitmap migration may be incomplete.");
            eprintln!("Check /api/indexes/{{name}}/stats for alive_count and bitmap memory.");
            std::process::exit(1);
        }
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
            replay_realtime(&entries, &args.url, args.speed)
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
    if summary.server_latency.max_us > 0 {
        println!("  Server latency:    p50={}μs  p95={}μs  p99={}μs  max={}μs",
            summary.server_latency.p50_us, summary.server_latency.p95_us,
            summary.server_latency.p99_us, summary.server_latency.max_us);
    }

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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Write CaptureEntry records to a file in caplog format:
    /// [u32 LE length][rmp_serde msgpack bytes] per entry.
    fn write_synthetic_caplog(
        entries: &[CaptureEntry],
        path: &std::path::Path,
    ) -> std::io::Result<()> {
        let mut f = std::io::BufWriter::new(std::fs::File::create(path)?);
        for entry in entries {
            let data = rmp_serde::to_vec(entry)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            let len = data.len() as u32;
            f.write_all(&len.to_le_bytes())?;
            f.write_all(&data)?;
        }
        f.flush()?;
        Ok(())
    }

    /// Generate a POST /api/indexes/civitai/query entry with a realistic JSON body.
    fn make_query_entry(arrived_ns: u64, latency_us: u64) -> CaptureEntry {
        let body = br#"{"filters":[{"eq":{"nsfwLevel":1}},{"eq":{"type":"image"}}],"sort":{"field":"reactionCount","direction":"desc"},"limit":100}"#;
        let response_body = br#"{"hits":[1,2,3],"total":3}"#;
        CaptureEntry {
            arrived_at_ns: arrived_ns,
            method: "POST".to_string(),
            path: "/api/indexes/civitai/query".to_string(),
            query_string: String::new(),
            body: body.to_vec(),
            response_status: 200,
            response_body: response_body.to_vec(),
            responded_at_ns: arrived_ns + latency_us * 1_000,
        }
    }

    /// Generate a GET request entry at an arbitrary path.
    fn make_get_entry(arrived_ns: u64, path: &str, latency_us: u64) -> CaptureEntry {
        CaptureEntry {
            arrived_at_ns: arrived_ns,
            method: "GET".to_string(),
            path: path.to_string(),
            query_string: String::new(),
            body: Vec::new(),
            response_status: 200,
            response_body: br#"{"status":"ok"}"#.to_vec(),
            responded_at_ns: arrived_ns + latency_us * 1_000,
        }
    }

    /// Generate a POST /api/indexes/civitai/documents/upsert entry.
    fn make_upsert_entry(arrived_ns: u64, latency_us: u64) -> CaptureEntry {
        let body = br#"[{"id":42,"nsfwLevel":1,"type":"image"}]"#;
        CaptureEntry {
            arrived_at_ns: arrived_ns,
            method: "POST".to_string(),
            path: "/api/indexes/civitai/documents/upsert".to_string(),
            query_string: String::new(),
            body: body.to_vec(),
            response_status: 200,
            response_body: br#"{"upserted":1}"#.to_vec(),
            responded_at_ns: arrived_ns + latency_us * 1_000,
        }
    }

    // -------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------

    #[test]
    fn test_parse_empty_caplog() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.caplog");
        write_synthetic_caplog(&[], &path).unwrap();

        let entries = read_caplog(&path).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_parse_single_entry() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("single.caplog");
        let entry = make_query_entry(1_000_000_000, 500);
        write_synthetic_caplog(&[entry.clone()], &path).unwrap();

        let entries = read_caplog(&path).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].arrived_at_ns, 1_000_000_000);
        assert_eq!(entries[0].method, "POST");
        assert_eq!(entries[0].path, "/api/indexes/civitai/query");
        assert_eq!(entries[0].response_status, 200);
        assert_eq!(entries[0].body, entry.body);
        assert_eq!(entries[0].response_body, entry.response_body);
        assert_eq!(
            entries[0].responded_at_ns,
            1_000_000_000 + 500 * 1_000
        );
    }

    #[test]
    fn test_parse_multiple_entries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("multi.caplog");

        let base_ns = 1_700_000_000_000_000_000u64; // ~2023 epoch ns
        let entries_in: Vec<CaptureEntry> = (0..10)
            .map(|i| make_query_entry(base_ns + i * 1_000_000, 100 + i * 10))
            .collect();

        write_synthetic_caplog(&entries_in, &path).unwrap();
        let entries_out = read_caplog(&path).unwrap();

        assert_eq!(entries_out.len(), 10);
        for (i, (inp, out)) in entries_in.iter().zip(entries_out.iter()).enumerate() {
            assert_eq!(inp.arrived_at_ns, out.arrived_at_ns, "mismatch at index {i}");
            assert_eq!(inp.method, out.method, "method mismatch at index {i}");
            assert_eq!(inp.body, out.body, "body mismatch at index {i}");
        }
    }

    #[test]
    fn test_parse_preserves_order() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("order.caplog");

        let base_ns = 1_700_000_000_000_000_000u64;
        let entries_in: Vec<CaptureEntry> = (0..50)
            .map(|i| make_query_entry(base_ns + i * 500_000, 200))
            .collect();

        write_synthetic_caplog(&entries_in, &path).unwrap();
        let entries_out = read_caplog(&path).unwrap();

        assert_eq!(entries_out.len(), 50);
        // Verify order is preserved — read_caplog returns entries in file order
        for (i, out) in entries_out.iter().enumerate() {
            let expected_ns = base_ns + (i as u64) * 500_000;
            assert_eq!(out.arrived_at_ns, expected_ns, "order broken at index {i}");
        }
    }

    #[test]
    fn test_parse_mixed_methods() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mixed.caplog");

        let base_ns = 1_700_000_000_000_000_000u64;
        let entries_in = vec![
            make_query_entry(base_ns, 300),
            make_get_entry(base_ns + 1_000_000, "/api/indexes/civitai/stats", 50),
            make_upsert_entry(base_ns + 2_000_000, 1500),
            make_get_entry(base_ns + 3_000_000, "/api/indexes/civitai/traces", 80),
            make_query_entry(base_ns + 4_000_000, 250),
        ];

        write_synthetic_caplog(&entries_in, &path).unwrap();
        let entries_out = read_caplog(&path).unwrap();

        assert_eq!(entries_out.len(), 5);
        assert_eq!(entries_out[0].method, "POST");
        assert_eq!(entries_out[0].path, "/api/indexes/civitai/query");
        assert_eq!(entries_out[1].method, "GET");
        assert_eq!(entries_out[1].path, "/api/indexes/civitai/stats");
        assert_eq!(entries_out[2].method, "POST");
        assert_eq!(entries_out[2].path, "/api/indexes/civitai/documents/upsert");
        assert_eq!(entries_out[3].method, "GET");
        assert_eq!(entries_out[3].path, "/api/indexes/civitai/traces");
        assert_eq!(entries_out[4].method, "POST");
        assert_eq!(entries_out[4].path, "/api/indexes/civitai/query");
    }

    #[test]
    fn test_parse_truncated_caplog_recovers() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("truncated.caplog");

        // Write 5 valid entries, then truncate midway through a 6th
        let base_ns = 1_700_000_000_000_000_000u64;
        let entries_in: Vec<CaptureEntry> = (0..6)
            .map(|i| make_query_entry(base_ns + i * 1_000_000, 100))
            .collect();

        // Write all 6 to get the full bytes, then truncate
        let mut full_bytes = Vec::new();
        for entry in &entries_in {
            let data = rmp_serde::to_vec(entry).unwrap();
            let len = data.len() as u32;
            full_bytes.extend_from_slice(&len.to_le_bytes());
            full_bytes.extend_from_slice(&data);
        }

        // Chop off last 10 bytes — this makes the 6th entry's payload incomplete
        let truncated = &full_bytes[..full_bytes.len() - 10];
        std::fs::write(&path, truncated).unwrap();

        let entries_out = read_caplog(&path).unwrap();
        // Should recover the first 5 entries and skip the truncated 6th
        assert_eq!(entries_out.len(), 5);
        for (i, out) in entries_out.iter().enumerate() {
            assert_eq!(
                out.arrived_at_ns,
                base_ns + (i as u64) * 1_000_000,
                "recovered entry {i} has wrong timestamp"
            );
        }
    }

    #[test]
    fn test_roundtrip_with_realistic_traffic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("realistic.caplog");

        let base_ns = 1_711_000_000_000_000_000u64; // ~March 2024
        let mut entries_in = Vec::with_capacity(100);
        let mut rng_state = 42u64; // deterministic pseudo-random

        // Simple LCG for deterministic test — avoids pulling in rand for cfg(test)
        let mut next_u64 = || -> u64 {
            rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            rng_state
        };

        for i in 0u64..100 {
            let arrived = base_ns + i * 10_000_000; // 10ms apart
            let bucket = next_u64() % 100;
            let entry = if bucket < 70 {
                // 70% POST query
                make_query_entry(arrived, 200 + (next_u64() % 5000))
            } else if bucket < 80 {
                // 10% GET stats
                make_get_entry(arrived, "/api/indexes/civitai/stats", 20 + (next_u64() % 100))
            } else if bucket < 90 {
                // 10% POST upsert
                make_upsert_entry(arrived, 500 + (next_u64() % 3000))
            } else {
                // 10% GET traces
                make_get_entry(arrived, "/api/indexes/civitai/traces", 30 + (next_u64() % 200))
            };
            entries_in.push(entry);
        }

        write_synthetic_caplog(&entries_in, &path).unwrap();
        let entries_out = read_caplog(&path).unwrap();

        assert_eq!(entries_out.len(), 100);

        // Verify every field round-trips exactly
        for (i, (inp, out)) in entries_in.iter().zip(entries_out.iter()).enumerate() {
            assert_eq!(inp.arrived_at_ns, out.arrived_at_ns, "arrived_at_ns mismatch at {i}");
            assert_eq!(inp.method, out.method, "method mismatch at {i}");
            assert_eq!(inp.path, out.path, "path mismatch at {i}");
            assert_eq!(inp.query_string, out.query_string, "query_string mismatch at {i}");
            assert_eq!(inp.body, out.body, "body mismatch at {i}");
            assert_eq!(inp.response_status, out.response_status, "response_status mismatch at {i}");
            assert_eq!(inp.response_body, out.response_body, "response_body mismatch at {i}");
            assert_eq!(inp.responded_at_ns, out.responded_at_ns, "responded_at_ns mismatch at {i}");
        }

        // Verify traffic distribution is mixed (at least 2 methods, at least 2 paths)
        let methods: std::collections::HashSet<&str> =
            entries_out.iter().map(|e| e.method.as_str()).collect();
        assert!(methods.contains("GET"), "expected GET requests in mix");
        assert!(methods.contains("POST"), "expected POST requests in mix");

        let paths: std::collections::HashSet<&str> =
            entries_out.iter().map(|e| e.path.as_str()).collect();
        assert!(paths.len() >= 3, "expected at least 3 distinct paths, got {}", paths.len());
    }

    #[test]
    fn test_window_filter() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("window.caplog");

        // Generate entries spanning 60 seconds (one per second)
        let base_ns = 1_711_000_000_000_000_000u64;
        let ns_per_sec = 1_000_000_000u64;
        let entries_in: Vec<CaptureEntry> = (0..60)
            .map(|i| make_query_entry(base_ns + i * ns_per_sec, 100))
            .collect();

        write_synthetic_caplog(&entries_in, &path).unwrap();
        let mut all_entries = read_caplog(&path).unwrap();
        assert_eq!(all_entries.len(), 60);

        // Sort by arrival time (as main() does before window filtering)
        all_entries.sort_by_key(|e| e.arrived_at_ns);

        // Apply window filter: 20s-30s (should include seconds 20..=30 → 11 entries)
        let (start_s, end_s) = (20.0f64, 30.0f64);
        let start_ns = base_ns + (start_s * 1_000_000_000.0) as u64;
        let end_ns = base_ns + (end_s * 1_000_000_000.0) as u64;
        let windowed: Vec<&CaptureEntry> = all_entries
            .iter()
            .filter(|e| e.arrived_at_ns >= start_ns && e.arrived_at_ns <= end_ns)
            .collect();

        assert_eq!(windowed.len(), 11, "window 20s-30s should include 11 entries (20,21,...,30)");

        // Verify the timestamps are correct
        for (j, entry) in windowed.iter().enumerate() {
            let expected_second = 20 + j as u64;
            let expected_ns = base_ns + expected_second * ns_per_sec;
            assert_eq!(
                entry.arrived_at_ns, expected_ns,
                "window entry {j} should be at second {expected_second}"
            );
        }

        // Verify entries outside the window are excluded
        let before_window: Vec<&CaptureEntry> = all_entries
            .iter()
            .filter(|e| e.arrived_at_ns < start_ns)
            .collect();
        assert_eq!(before_window.len(), 20, "20 entries should be before the window");

        let after_window: Vec<&CaptureEntry> = all_entries
            .iter()
            .filter(|e| e.arrived_at_ns > end_ns)
            .collect();
        assert_eq!(after_window.len(), 29, "29 entries should be after the window");
    }
}
