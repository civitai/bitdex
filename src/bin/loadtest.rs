//! BitDex Concurrent Load Tester
//!
//! Tests query throughput and latency at various concurrency levels.
//! Two modes:
//!   --mode direct   Embeds the engine, restores from disk, queries directly (tests bitmap layer)
//!   --mode http     Sends HTTP requests to a running server (tests full stack)
//!
//! Usage:
//!   cargo run --release --bin bitdex-loadtest --features loadtest -- --mode direct --data-dir ./data
//!   cargo run --release --bin bitdex-loadtest --features loadtest -- --mode http --url http://localhost:3001
//!
//! Options:
//!   --concurrency <LIST>   Comma-separated concurrency levels (default: 1,4,8,16,32,64)
//!   --duration <SECS>      Seconds to run at each level (default: 10)
//!   --warmup <SECS>        Warmup seconds before measuring (default: 3)
//!   --no-warmup            Skip warmup phase
//!   --workload <PATH>      JSON workload file (default: built-in Civitai queries)
//!   --index <NAME>         Index name (default: civitai)

// Use rpmalloc for better concurrent allocation performance.
#[global_allocator]
static ALLOC: rpmalloc::RpMalloc = rpmalloc::RpMalloc;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::Rng;

use bitdex_v2::query::{FilterClause, SortClause};

// ---------------------------------------------------------------------------
// Query runner trait — abstracts direct vs HTTP execution
// ---------------------------------------------------------------------------

trait QueryRunner: Send + Sync {
    /// Execute a query, return (total_matched, elapsed).
    fn run_query(
        &self,
        filters: &[FilterClause],
        sort: Option<&SortClause>,
        limit: usize,
    ) -> Result<(u64, Duration), String>;

    /// Name for display.
    fn mode_name(&self) -> &str;

    /// Alive count (for stats).
    fn alive_count(&self) -> u64;
}

// ---------------------------------------------------------------------------
// Direct runner — embeds the engine
// ---------------------------------------------------------------------------

struct DirectRunner {
    engine: Arc<bitdex_v2::concurrent_engine::ConcurrentEngine>,
}

impl QueryRunner for DirectRunner {
    fn run_query(
        &self,
        filters: &[FilterClause],
        sort: Option<&SortClause>,
        limit: usize,
    ) -> Result<(u64, Duration), String> {
        let start = Instant::now();
        let result = self.engine.query(filters, sort, limit).map_err(|e| e.to_string())?;
        Ok((result.total_matched, start.elapsed()))
    }

    fn mode_name(&self) -> &str {
        "direct"
    }

    fn alive_count(&self) -> u64 {
        self.engine.alive_count()
    }
}

// ---------------------------------------------------------------------------
// HTTP runner — sends requests to server
// ---------------------------------------------------------------------------

struct HttpRunner {
    query_url: String,
    stats_url: String,
}

impl HttpRunner {
    fn new(base_url: &str, index: &str) -> Self {
        Self {
            query_url: format!("{}/api/indexes/{}/query", base_url, index),
            stats_url: format!("{}/api/indexes/{}/stats", base_url, index),
        }
    }

    /// Create a thread-local ureq Agent (avoids Mutex contention on shared pool).
    fn make_agent() -> ureq::Agent {
        ureq::AgentBuilder::new()
            .timeout(Duration::from_secs(30))
            .build()
    }
}

thread_local! {
    static THREAD_AGENT: ureq::Agent = HttpRunner::make_agent();
}

impl QueryRunner for HttpRunner {
    fn run_query(
        &self,
        filters: &[FilterClause],
        sort: Option<&SortClause>,
        limit: usize,
    ) -> Result<(u64, Duration), String> {
        let body = serde_json::json!({
            "filters": filters,
            "sort": sort,
            "limit": limit,
        });

        let start = Instant::now();
        let resp = THREAD_AGENT.with(|agent| {
            agent.post(&self.query_url)
                .set("Content-Type", "application/json")
                .send_string(&body.to_string())
                .map_err(|e| e.to_string())
        })?;

        let text = resp.into_string().map_err(|e| e.to_string())?;
        let elapsed = start.elapsed();

        let parsed: serde_json::Value =
            serde_json::from_str(&text).map_err(|e| e.to_string())?;
        let total = parsed["total_matched"].as_u64().unwrap_or(0);
        Ok((total, elapsed))
    }

    fn mode_name(&self) -> &str {
        "http"
    }

    fn alive_count(&self) -> u64 {
        let resp = THREAD_AGENT.with(|agent| agent.get(&self.stats_url).call());
        match resp {
            Ok(r) => {
                let text = r.into_string().unwrap_or_default();
                let parsed: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
                parsed["alive_count"].as_u64().unwrap_or(0)
            }
            Err(_) => 0,
        }
    }
}

// ---------------------------------------------------------------------------
// Query workload — representative production-like queries
// ---------------------------------------------------------------------------

/// A single query in the workload file.
#[derive(Clone, serde::Deserialize)]
struct WorkloadQuery {
    label: String,
    filters: Vec<FilterClause>,
    #[serde(default)]
    sort: Option<SortClause>,
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_limit() -> usize {
    20
}

/// The workload file format.
#[derive(serde::Deserialize)]
struct WorkloadFile {
    queries: Vec<WorkloadQuery>,
}

#[derive(Clone)]
struct QueryWorkload {
    queries: Vec<WorkloadQuery>,
}

impl QueryWorkload {
    /// Load from a JSON file.
    fn from_file(path: &str) -> Self {
        let json = std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("Failed to read workload file {}: {}", path, e));
        let wf: WorkloadFile = serde_json::from_str(&json)
            .unwrap_or_else(|e| panic!("Failed to parse workload file {}: {}", path, e));
        Self { queries: wf.queries }
    }

    /// Built-in Civitai workload (used when no file is specified).
    fn civitai_default() -> Self {
        let json = r#"{"queries":[
            {"label":"homepage_sortAt","filters":[{"Eq":["nsfwLevel",{"Integer":1}]},{"Not":{"Eq":["availability",{"String":"Private"}]}}],"sort":{"field":"sortAt","direction":"Desc"}},
            {"label":"homepage_reactionCount","filters":[{"Eq":["nsfwLevel",{"Integer":1}]},{"Not":{"Eq":["availability",{"String":"Private"}]}}],"sort":{"field":"reactionCount","direction":"Desc"}},
            {"label":"homepage_commentCount","filters":[{"Eq":["nsfwLevel",{"Integer":1}]},{"Not":{"Eq":["availability",{"String":"Private"}]}}],"sort":{"field":"commentCount","direction":"Desc"}},
            {"label":"homepage_collectedCount","filters":[{"Eq":["nsfwLevel",{"Integer":1}]},{"Not":{"Eq":["availability",{"String":"Private"}]}}],"sort":{"field":"collectedCount","direction":"Desc"}},
            {"label":"user_1","filters":[{"Eq":["userId",{"Integer":1}]}],"sort":{"field":"sortAt","direction":"Desc"}},
            {"label":"user_4","filters":[{"Eq":["userId",{"Integer":4}]}],"sort":{"field":"sortAt","direction":"Desc"}},
            {"label":"user_100","filters":[{"Eq":["userId",{"Integer":100}]}],"sort":{"field":"sortAt","direction":"Desc"}},
            {"label":"user_5000","filters":[{"Eq":["userId",{"Integer":5000}]}],"sort":{"field":"sortAt","direction":"Desc"}},
            {"label":"user_50000","filters":[{"Eq":["userId",{"Integer":50000}]}],"sort":{"field":"sortAt","direction":"Desc"}},
            {"label":"mixed_type_image","filters":[{"Eq":["nsfwLevel",{"Integer":1}]},{"Eq":["type",{"String":"image"}]},{"Not":{"Eq":["availability",{"String":"Private"}]}}],"sort":{"field":"sortAt","direction":"Desc"}},
            {"label":"mixed_hasMeta","filters":[{"Eq":["nsfwLevel",{"Integer":1}]},{"Eq":["hasMeta",{"Bool":true}]},{"Not":{"Eq":["availability",{"String":"Private"}]}}],"sort":{"field":"reactionCount","direction":"Desc"}},
            {"label":"filter_only_dense","filters":[{"Eq":["nsfwLevel",{"Integer":1}]}]},
            {"label":"paginated","filters":[{"Eq":["nsfwLevel",{"Integer":1}]},{"Not":{"Eq":["availability",{"String":"Private"}]}}],"sort":{"field":"sortAt","direction":"Desc"}}
        ]}"#;
        let wf: WorkloadFile = serde_json::from_str(json).expect("parse built-in workload");
        Self { queries: wf.queries }
    }

    fn random_query(&self, rng: &mut impl Rng) -> &WorkloadQuery {
        &self.queries[rng.gen_range(0..self.queries.len())]
    }
}

// ---------------------------------------------------------------------------
// Stats computation
// ---------------------------------------------------------------------------

struct LatencyStats {
    count: usize,
    qps: f64,
    p50_us: u64,
    p95_us: u64,
    p99_us: u64,
    p999_us: u64,
    max_us: u64,
    mean_us: u64,
}

fn compute_latency_stats(mut durations_us: Vec<u64>, wall_secs: f64) -> LatencyStats {
    durations_us.sort_unstable();
    let n = durations_us.len();
    if n == 0 {
        return LatencyStats {
            count: 0, qps: 0.0, p50_us: 0, p95_us: 0, p99_us: 0, p999_us: 0, max_us: 0, mean_us: 0,
        };
    }
    let sum: u64 = durations_us.iter().sum();
    LatencyStats {
        count: n,
        qps: n as f64 / wall_secs,
        p50_us: durations_us[n * 50 / 100],
        p95_us: durations_us[n * 95 / 100],
        p99_us: durations_us[n * 99 / 100],
        p999_us: durations_us[(n as f64 * 0.999) as usize],
        max_us: durations_us[n - 1],
        mean_us: sum / n as u64,
    }
}

// ---------------------------------------------------------------------------
// Load test runner
// ---------------------------------------------------------------------------

fn run_load_test(
    runner: &Arc<dyn QueryRunner>,
    concurrency: usize,
    duration_secs: u64,
    warmup_secs: u64,
    workload: &QueryWorkload,
) -> LatencyStats {
    let stop = Arc::new(AtomicBool::new(false));
    let measuring = Arc::new(AtomicBool::new(false));
    let query_count = Arc::new(AtomicU64::new(0));
    let error_count = Arc::new(AtomicU64::new(0));

    // Each thread collects its own latencies to avoid lock contention
    let all_latencies: Arc<parking_lot::Mutex<Vec<Vec<u64>>>> =
        Arc::new(parking_lot::Mutex::new(Vec::new()));

    let mut handles = Vec::new();

    for _ in 0..concurrency {
        let runner = Arc::clone(runner);
        let stop = Arc::clone(&stop);
        let measuring = Arc::clone(&measuring);
        let counter = Arc::clone(&query_count);
        let errors = Arc::clone(&error_count);
        let latencies = Arc::clone(&all_latencies);
        let workload = workload.clone();

        handles.push(std::thread::spawn(move || {
            let mut rng = rand::thread_rng();
            let mut local_latencies: Vec<u64> = Vec::with_capacity(100_000);

            while !stop.load(Ordering::Relaxed) {
                let q = workload.random_query(&mut rng);

                match runner.run_query(&q.filters, q.sort.as_ref(), q.limit) {
                    Ok((_total, elapsed)) => {
                        if measuring.load(Ordering::Relaxed) {
                            local_latencies.push(elapsed.as_micros() as u64);
                            counter.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(_) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }

            latencies.lock().push(local_latencies);
        }));
    }

    // Warmup phase
    if warmup_secs > 0 {
        std::thread::sleep(Duration::from_secs(warmup_secs));
    }

    // Start measuring
    measuring.store(true, Ordering::Relaxed);
    let measure_start = Instant::now();
    std::thread::sleep(Duration::from_secs(duration_secs));
    let wall_secs = measure_start.elapsed().as_secs_f64();

    // Stop
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }

    let errors = error_count.load(Ordering::Relaxed);
    if errors > 0 {
        eprintln!("  Warning: {} errors during test", errors);
    }

    // Flatten all thread-local latencies
    let all = all_latencies.lock();
    let flat: Vec<u64> = all.iter().flat_map(|v| v.iter().copied()).collect();

    compute_latency_stats(flat, wall_secs)
}

// ---------------------------------------------------------------------------
// Warmup — trigger lazy loads and seed bound cache
// ---------------------------------------------------------------------------

fn warmup_runner(runner: &Arc<dyn QueryRunner>, workload: &QueryWorkload) {
    eprint!("Warming up (lazy loads + bound cache)...");
    let start = Instant::now();

    // Run each workload query twice:
    // 1st pass triggers lazy field loading
    // 2nd pass seeds the bound cache
    for pass in 0..2 {
        for q in &workload.queries {
            let _ = runner.run_query(&q.filters, q.sort.as_ref(), q.limit);
        }
        if pass == 0 {
            eprint!(" fields loaded...");
        }
    }

    eprintln!(" done ({:.1}s)", start.elapsed().as_secs_f64());
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let mut mode = "direct".to_string();
    let mut data_dir = "./data".to_string();
    let mut url = "http://localhost:3001".to_string();
    let mut index = "civitai".to_string();
    let mut concurrency_levels: Vec<usize> = vec![1, 4, 8, 16, 32, 64];
    let mut duration_secs: u64 = 10;
    let mut warmup_secs: u64 = 3;
    let mut skip_warmup = false;
    let mut workload_path: Option<String> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--mode" => {
                i += 1;
                mode = args[i].clone();
            }
            "--data-dir" => {
                i += 1;
                data_dir = args[i].clone();
            }
            "--url" => {
                i += 1;
                url = args[i].clone();
            }
            "--index" => {
                i += 1;
                index = args[i].clone();
            }
            "--concurrency" => {
                i += 1;
                concurrency_levels = args[i]
                    .split(',')
                    .filter_map(|s| s.trim().parse().ok())
                    .collect();
            }
            "--duration" => {
                i += 1;
                duration_secs = args[i].parse().unwrap_or(10);
            }
            "--warmup" => {
                i += 1;
                warmup_secs = args[i].parse().unwrap_or(3);
            }
            "--no-warmup" => {
                skip_warmup = true;
            }
            "--workload" => {
                i += 1;
                workload_path = Some(args[i].clone());
            }
            "--help" | "-h" => {
                eprintln!("Usage: loadtest [OPTIONS]");
                eprintln!("  --mode <direct|http>        Query mode (default: direct)");
                eprintln!("  --data-dir <PATH>            Data directory for direct mode (default: ./data)");
                eprintln!("  --url <URL>                  Server URL for HTTP mode (default: http://localhost:3001)");
                eprintln!("  --index <NAME>               Index name (default: civitai)");
                eprintln!("  --concurrency <1,4,8,...>    Concurrency levels (default: 1,4,8,16,32,64)");
                eprintln!("  --duration <SECS>            Seconds per level (default: 10)");
                eprintln!("  --warmup <SECS>              Warmup before measuring (default: 3)");
                eprintln!("  --no-warmup                  Skip warmup phase");
                eprintln!("  --workload <PATH>            JSON workload file (default: tests/loadtest/workload.json)");
                std::process::exit(0);
            }
            _ => {
                eprintln!("Unknown argument: {}", args[i]);
                std::process::exit(1);
            }
        }
        i += 1;
    }

    eprintln!("BitDex Load Test");
    eprintln!("  mode: {}", mode);
    eprintln!("  workload: {}", workload_path.as_deref().unwrap_or("auto-detect"));
    eprintln!("  concurrency: {:?}", concurrency_levels);
    eprintln!("  duration: {}s per level ({}s warmup)", duration_secs, warmup_secs);

    // Build the runner
    let runner: Arc<dyn QueryRunner> = match mode.as_str() {
        "direct" => {
            eprintln!("  data-dir: {}", data_dir);
            eprintln!();
            eprintln!("Restoring engine from disk...");

            let restore_start = Instant::now();
            let engine = restore_engine(&data_dir, &index);
            eprintln!(
                "  Restored in {:.2}s ({} records)",
                restore_start.elapsed().as_secs_f64(),
                engine.alive_count()
            );
            Arc::new(DirectRunner {
                engine: Arc::new(engine),
            })
        }
        "http" => {
            eprintln!("  url: {}", url);
            eprintln!("  index: {}", index);
            let runner = Arc::new(HttpRunner::new(&url, &index));
            let alive = runner.alive_count();
            if alive == 0 {
                eprintln!("  WARNING: server returned 0 alive count — is the server running?");
            } else {
                eprintln!("  Server has {} records", alive);
            }
            runner
        }
        _ => {
            eprintln!("Unknown mode: {}. Use 'direct' or 'http'.", mode);
            std::process::exit(1);
        }
    };

    let workload = if let Some(ref path) = workload_path {
        QueryWorkload::from_file(path)
    } else {
        // Default to real traffic workload if available, fall back to built-in
        let default_workload = std::path::Path::new("tests/loadtest/workload.json");
        if default_workload.exists() {
            eprintln!("  workload file: tests/loadtest/workload.json (2,516 real traffic queries)");
            QueryWorkload::from_file(default_workload.to_str().unwrap())
        } else {
            eprintln!("  workload: built-in (13 stress-test queries — use --workload for baselines)");
            QueryWorkload::civitai_default()
        }
    };

    // Warmup: trigger lazy loads and seed bound cache
    if !skip_warmup {
        warmup_runner(&runner, &workload);
    }

    eprintln!();
    println!("=== BitDex Load Test ({} mode, {} records) ===", runner.mode_name(), runner.alive_count());
    println!();
    println!(
        "{:>12}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}",
        "concurrency", "queries", "QPS", "p50", "p95", "p99", "p99.9", "max"
    );
    println!("{}", "-".repeat(92));

    for &c in &concurrency_levels {
        eprint!("  Running c={:>3}...", c);
        let stats = run_load_test(&runner, c, duration_secs, warmup_secs, &workload);

        println!(
            "\r{:>12}  {:>10}  {:>10.0}  {:>8.2}ms  {:>8.2}ms  {:>8.2}ms  {:>8.2}ms  {:>8.2}ms",
            c,
            stats.count,
            stats.qps,
            stats.p50_us as f64 / 1000.0,
            stats.p95_us as f64 / 1000.0,
            stats.p99_us as f64 / 1000.0,
            stats.p999_us as f64 / 1000.0,
            stats.max_us as f64 / 1000.0,
        );
    }

    println!();
    println!("Done.");
}

// ---------------------------------------------------------------------------
// Engine restore (same logic as server.rs)
// ---------------------------------------------------------------------------

fn restore_engine(data_dir: &str, index_name: &str) -> bitdex_v2::concurrent_engine::ConcurrentEngine {
    use bitdex_v2::concurrent_engine::ConcurrentEngine;
    use std::path::Path;

    let index_path = Path::new(data_dir).join("indexes").join(index_name);
    let config_path = match bitdex_v2::server::find_index_config(&index_path) {
        Some(p) => p,
        None => {
            eprintln!("Config not found in: {}", index_path.display());
            std::process::exit(1);
        }
    };

    let def = bitdex_v2::server::IndexDefinition::from_file(&config_path)
        .unwrap_or_else(|e| { eprintln!("Failed to parse config: {e}"); std::process::exit(1); });

    let docstore_path = index_path.join("docs");
    let mut config = def.config;
    config.storage.bitmap_path = Some(index_path.join("bitmaps"));

    let mut engine = ConcurrentEngine::new_with_path(config, &docstore_path)
        .expect("restore engine");

    // Set up string maps from data schema
    let (string_maps, cs_fields) = bitdex_v2::server::build_string_maps_with_dicts(&def.data_schema, None);
    if !string_maps.is_empty() {
        engine.set_string_maps(string_maps);
    }
    if !cs_fields.is_empty() {
        engine.set_case_sensitive_fields(cs_fields);
    }

    engine
}
