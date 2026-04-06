/// csv_mmap_real_bench.rs
///
/// Real-world benchmark using actual Civitai data:
///   posts.csv       — 619 MB, 23M rows, comma-separated, columns: id,publishedAtSecs,availability,modelVersionId
///   images-small.csv — 2 GB,  14.6M rows, columns: id,url,...,postId (col 10),...
///
/// Compares two approaches for building the posts enrichment index:
///   A) Full parse → HashMap<i64, PostRow>
///   D) Dense Vec<u64> offset index + mmap on-demand parse  (max post id ~27.6M → 221 MB Vec)
///
/// Then simulates the enrichment join: iterate images-small.csv, extract postId, look up in
/// the posts index, measure per-lookup latency and total wall time.
///
/// Run with:
///   cargo run -p scratch --release --bin csv_mmap_real_bench

use std::collections::HashMap;
use std::time::Instant;

use memmap2::Mmap;

// ── paths ─────────────────────────────────────────────────────────────────────
const POSTS_PATH: &str = "C:/Dev/Repos/open-source/bitdex-v2/data/load_stage/posts.csv";
const IMAGES_PATH: &str = "C:/Dev/Repos/open-source/bitdex-v2/data/load_stage/images-small.csv";

// posts.csv column indices (0-based, comma-separated)
const POSTS_COL_ID: usize = 0;
const POSTS_COL_PUBLISHED_AT: usize = 1;
const POSTS_COL_AVAILABILITY: usize = 2;
const POSTS_COL_MODEL_VERSION: usize = 3;

// images-small.csv column indices
const IMAGES_COL_POST_ID: usize = 10;

// Dense Vec max capacity — post IDs go up to ~27.6M
const POST_ID_MAX: usize = 28_000_000;

// ── helpers ───────────────────────────────────────────────────────────────────

fn sep() {
    println!("{}", "─".repeat(70));
}

#[derive(Debug, Clone)]
struct PostRow {
    published_at: i64,
    availability: u8, // 0=unknown, 1=Public, 2=Private, 3=Unsearchable
    model_version_id: Option<i64>,
}

/// Availability string → compact byte
#[inline]
fn parse_availability(s: &[u8]) -> u8 {
    match s {
        b"Public" => 1,
        b"Private" => 2,
        b"Unsearchable" => 3,
        _ => 0,
    }
}

/// Minimal byte-slice memchr
#[inline]
fn memchr(haystack: &[u8], needle: u8) -> Option<usize> {
    haystack.iter().position(|&b| b == needle)
}

/// Parse the key (i64) from the start of a CSV line (up to first comma).
#[inline]
fn parse_key(line: &[u8]) -> Option<i64> {
    let end = memchr(line, b',').unwrap_or(line.len());
    let s = &line[..end];
    if s.is_empty() {
        return None;
    }
    fast_parse_i64(s)
}

/// Fast ASCII decimal i64 parser — avoids UTF-8 validation overhead.
#[inline]
fn fast_parse_i64(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    let (neg, digits) = if s[0] == b'-' { (true, &s[1..]) } else { (false, s) };
    if digits.is_empty() {
        return None;
    }
    let mut v: i64 = 0;
    for &b in digits {
        if b < b'0' || b > b'9' {
            return None;
        }
        v = v.wrapping_mul(10).wrapping_add((b - b'0') as i64);
    }
    Some(if neg { -v } else { v })
}

/// Extract a specific comma-delimited column from a line (0-based).
/// Returns the raw bytes of the field (empty slice if column is absent or empty).
#[inline]
fn get_column(line: &[u8], col: usize) -> &[u8] {
    let mut current = 0usize;
    let mut start = 0usize;
    let mut i = 0usize;
    loop {
        let end = if i < line.len() && line[i] != b',' {
            i += 1;
            continue;
        } else {
            let e = i;
            i += 1;
            e
        };
        if current == col {
            return &line[start..end];
        }
        current += 1;
        start = i;
        if i > line.len() {
            break;
        }
    }
    // Last column without trailing comma
    if current == col && start <= line.len() {
        &line[start..]
    } else {
        b""
    }
}

/// Parse all four fields from a posts CSV line (already past key column).
#[inline]
fn parse_post_line(line: &[u8]) -> Option<PostRow> {
    let published_at_bytes = get_column(line, POSTS_COL_PUBLISHED_AT);
    let avail_bytes = get_column(line, POSTS_COL_AVAILABILITY);
    let mv_bytes = get_column(line, POSTS_COL_MODEL_VERSION);

    let published_at = fast_parse_i64(published_at_bytes)?;
    let availability = parse_availability(avail_bytes);
    let model_version_id = if mv_bytes.is_empty() {
        None
    } else {
        fast_parse_i64(mv_bytes)
    };

    Some(PostRow { published_at, availability, model_version_id })
}

/// Read the line at `offset` from a mmap. Returns bytes excluding newline.
#[inline]
fn line_at(mmap: &Mmap, offset: u64) -> &[u8] {
    let start = offset as usize;
    let data = &mmap[start..];
    // handle CRLF
    let end = memchr(data, b'\n').unwrap_or(data.len());
    let slice = &data[..end];
    // strip trailing CR
    if slice.last() == Some(&b'\r') { &slice[..slice.len() - 1] } else { slice }
}

/// Format bytes as human-readable size
fn fmt_bytes(b: usize) -> String {
    if b >= 1 << 30 {
        format!("{:.2} GB", b as f64 / (1 << 30) as f64)
    } else if b >= 1 << 20 {
        format!("{:.1} MB", b as f64 / (1 << 20) as f64)
    } else {
        format!("{} KB", b / 1024)
    }
}

// ── Approach A: Full HashMap ──────────────────────────────────────────────────

struct ApproachA {
    map: HashMap<i64, PostRow>,
}

impl ApproachA {
    fn build() -> (Self, std::time::Duration, usize) {
        println!("  Reading {}...", POSTS_PATH);
        let t = Instant::now();

        let data = std::fs::read(POSTS_PATH).expect("posts.csv not found");
        let file_bytes = data.len();
        let mut map: HashMap<i64, PostRow> = HashMap::with_capacity(24_000_000);

        let mut lines = data.split(|&b| b == b'\n');
        lines.next(); // skip header

        let mut parsed = 0u64;
        for line in lines {
            // strip CR
            let line = if line.last() == Some(&b'\r') { &line[..line.len()-1] } else { line };
            if line.is_empty() { continue; }
            let Some(key) = parse_key(line) else { continue; };
            let Some(row) = parse_post_line(line) else { continue; };
            map.insert(key, row);
            parsed += 1;
        }

        let elapsed = t.elapsed();
        let map_len = map.len();

        // Memory estimate: HashMap bucket overhead + per-entry (key i64 + PostRow)
        // PostRow = i64 + u8 + Option<i64> = 8+1+9 = ~24 bytes with alignment → 24
        // HashMap slot: key(8) + value(24) + hash overhead ≈ 48 bytes/slot @ 50% load factor
        let estimated_mem = map.capacity() * (8 + 24 + 8) // key+value+metadata
            + file_bytes; // we read into heap via std::fs::read
        // (we'll drop the file bytes after build, so net = just the HashMap)
        let net_mem = map.capacity() * (8 + 24 + 8);

        println!("  Parsed {} rows in {:.0} ms", parsed, elapsed.as_millis());
        println!("  map.len()={} map.capacity()={}", map_len, map.capacity());
        println!("  Estimated HashMap heap: {} (excl. input buffer {})",
            fmt_bytes(net_mem), fmt_bytes(file_bytes));

        (Self { map }, elapsed, net_mem)
    }

    #[inline]
    fn lookup(&self, key: i64) -> Option<&PostRow> {
        self.map.get(&key)
    }
}

// ── Approach D: Dense Vec<u64> offset index + mmap ───────────────────────────

struct ApproachD {
    offsets: Vec<u64>,
    mmap: Mmap,
}

impl ApproachD {
    fn build() -> (Self, std::time::Duration, usize) {
        println!("  Mmapping {}...", POSTS_PATH);
        let t = Instant::now();

        let file = std::fs::File::open(POSTS_PATH).expect("posts.csv not found");
        let mmap = unsafe { Mmap::map(&file).unwrap() };
        let file_bytes = mmap.len();

        let mut offsets = vec![u64::MAX; POST_ID_MAX];
        let data = &mmap[..];

        let mut pos = 0usize;
        // skip header
        if let Some(nl) = memchr(&data[pos..], b'\n') { pos += nl + 1; }

        let mut indexed = 0u64;
        let mut skipped = 0u64;

        while pos < data.len() {
            let line_start = pos as u64;
            let slice = &data[pos..];
            let nl = memchr(slice, b'\n').unwrap_or(slice.len());
            let line = {
                let raw = &slice[..nl];
                if raw.last() == Some(&b'\r') { &raw[..raw.len()-1] } else { raw }
            };

            if !line.is_empty() {
                if let Some(key) = parse_key(line) {
                    if key >= 0 && (key as usize) < POST_ID_MAX {
                        offsets[key as usize] = line_start;
                        indexed += 1;
                    } else {
                        skipped += 1;
                    }
                }
            }

            pos += nl + 1;
        }

        let elapsed = t.elapsed();
        // Memory: Vec<u64> + mmap is page-cache backed (not heap)
        let vec_heap = offsets.len() * 8;

        println!("  Indexed {} rows ({} skipped out-of-range) in {:.0} ms",
            indexed, skipped, elapsed.as_millis());
        println!("  offsets vec: {} (mmap is OS page-cache, not heap: {})",
            fmt_bytes(vec_heap), fmt_bytes(file_bytes));

        (Self { offsets, mmap }, elapsed, vec_heap)
    }

    #[inline]
    fn lookup(&self, key: i64) -> Option<PostRow> {
        if key <= 0 || (key as usize) >= POST_ID_MAX {
            return None;
        }
        let offset = self.offsets[key as usize];
        if offset == u64::MAX {
            return None;
        }
        let line = line_at(&self.mmap, offset);
        parse_post_line(line)
    }
}

// ── Enrichment join simulation ────────────────────────────────────────────────

/// Iterate images-small.csv, extract postId (col 10), call lookup_fn for each.
/// Returns (total_lookups, hits, elapsed).
fn simulate_enrichment_join<F>(label: &str, lookup: F) -> (u64, u64, std::time::Duration)
where
    F: Fn(i64) -> bool,
{
    println!("  Streaming {}...", IMAGES_PATH);
    let data = std::fs::read(IMAGES_PATH).expect("images-small.csv not found");

    let t = Instant::now();
    let mut lines = data.split(|&b| b == b'\n');
    lines.next(); // skip header

    let mut total = 0u64;
    let mut hits = 0u64;
    let mut missing_post_id = 0u64;

    for line in lines {
        let line = if line.last() == Some(&b'\r') { &line[..line.len()-1] } else { line };
        if line.is_empty() { continue; }

        let post_id_bytes = get_column(line, IMAGES_COL_POST_ID);
        if post_id_bytes.is_empty() {
            missing_post_id += 1;
            continue;
        }
        let Some(post_id) = fast_parse_i64(post_id_bytes) else {
            missing_post_id += 1;
            continue;
        };

        total += 1;
        if lookup(post_id) { hits += 1; }
    }

    let elapsed = t.elapsed();
    let ns_per = if total > 0 {
        elapsed.as_nanos() as f64 / total as f64
    } else { 0.0 };

    println!("  {} — {} lookups, {} hits ({:.1}% hit rate), {} missing postId",
        label, total, hits, hits as f64 / total as f64 * 100.0, missing_post_id);
    println!("  Total: {:.3}s  |  Avg: {:.1} ns/lookup  |  {:.0} lookups/sec",
        elapsed.as_secs_f64(), ns_per, total as f64 / elapsed.as_secs_f64());

    (total, hits, elapsed)
}

// ── main ──────────────────────────────────────────────────────────────────────

fn main() {
    println!("=== csv_mmap_real_bench ===");
    println!("posts.csv path:        {}", POSTS_PATH);
    println!("images-small.csv path: {}", IMAGES_PATH);
    sep();

    // ── Build Approach A ──────────────────────────────────────────────────────
    println!("APPROACH A: Full parse → HashMap<i64, PostRow>");
    let (a, a_build, a_mem) = ApproachA::build();
    sep();

    // ── Build Approach D ──────────────────────────────────────────────────────
    println!("APPROACH D: Dense Vec<u64> offset index + mmap");
    let (d, d_build, d_mem) = ApproachD::build();
    sep();

    // ── Build summary ─────────────────────────────────────────────────────────
    println!("BUILD SUMMARY");
    println!("  A  HashMap:    {:.0} ms  |  {} heap", a_build.as_millis(), fmt_bytes(a_mem));
    println!("  D  Dense Vec:  {:.0} ms  |  {} heap  (+mmap in OS page cache)", d_build.as_millis(), fmt_bytes(d_mem));
    println!("  Build speedup: {:.1}x faster for D", a_build.as_secs_f64() / d_build.as_secs_f64());
    println!("  Memory ratio:  {:.1}x less heap for D", a_mem as f64 / d_mem as f64);
    sep();

    // ── Enrichment join — Approach A ─────────────────────────────────────────
    println!("ENRICHMENT JOIN — Approach A (HashMap lookup)");
    // Note: this loads images-small.csv into heap for both passes.
    // We do A first while the posts HashMap is hot in cache.
    let (a_total, a_hits, a_elapsed) = simulate_enrichment_join("A", |pid| a.lookup(pid).is_some());
    sep();

    // ── Enrichment join — Approach D ─────────────────────────────────────────
    println!("ENRICHMENT JOIN — Approach D (dense Vec + mmap)");
    let (d_total, d_hits, d_elapsed) = simulate_enrichment_join("D", |pid| d.lookup(pid).is_some());
    sep();

    // ── Final summary ─────────────────────────────────────────────────────────
    println!("FINAL SUMMARY");
    println!("  {:>35}  {:>12}  {:>12}", "", "Approach A", "Approach D");
    println!("  {:>35}  {:>12}  {:>12}", "Build time (ms)", a_build.as_millis(), d_build.as_millis());
    println!("  {:>35}  {:>12}  {:>12}", "Index heap", fmt_bytes(a_mem), fmt_bytes(d_mem));
    let a_ns = if a_total > 0 { a_elapsed.as_nanos() as f64 / a_total as f64 } else { 0.0 };
    let d_ns = if d_total > 0 { d_elapsed.as_nanos() as f64 / d_total as f64 } else { 0.0 };
    println!("  {:>35}  {:>12}  {:>12}", "Lookup rows", a_total, d_total);
    println!("  {:>35}  {:>11.1}  {:>11.1}", "Total lookup wall time (s)", a_elapsed.as_secs_f64(), d_elapsed.as_secs_f64());
    println!("  {:>35}  {:>11.1}  {:>11.1}", "Avg ns/lookup (lookup only)", a_ns, d_ns);
    println!("  {:>35}  {:>11.1}  {:>11.1}", "Hit rate %", a_hits as f64 / a_total as f64 * 100.0, d_hits as f64 / d_total as f64 * 100.0);
    sep();

    println!("NOTE: 'lookup wall time' includes images-small.csv streaming + column extraction");
    println!("      for both approaches equally. Lookup delta = D_total - A_total wall time.");
    let csv_overhead_s = a_elapsed.as_secs_f64().min(d_elapsed.as_secs_f64());
    println!("  Estimated pure lookup overhead for D: {:.2}s above A baseline",
        (d_elapsed.as_secs_f64() - a_elapsed.as_secs_f64()).max(0.0));
    println!("  Lookup-only ns/call delta: {:.1} ns", (d_ns - a_ns).max(0.0));
    let _ = csv_overhead_s;
    sep();
}
