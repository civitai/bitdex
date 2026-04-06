/// csv_mmap_bench.rs
///
/// Compares four approaches to enrichment lookups over a large synthetic CSV:
///
///   A) Full parse → HashMap<i64, Vec<Option<String>>>
///   B) Offset index (HashMap<i64, u64>) + mmap on-demand parse
///   C) Offset index (sorted Vec<(i64, u64)>) + mmap binary-search + on-demand parse
///   D) Dense offset Vec (Vec<u64> indexed by key) + mmap on-demand parse
///
/// Run with:
///   cargo run -p scratch --release --bin csv_mmap_bench

use std::collections::HashMap;
use std::io::Write;
use std::time::Instant;

use memmap2::Mmap;
use rand::Rng;
use rand::SeedableRng as _;
use tempfile::NamedTempFile;

// ── tunables ──────────────────────────────────────────────────────────────────
const NUM_ROWS: i64 = 1_000_000;
const NUM_COLS: usize = 10;
const NUM_LOOKUPS: usize = 100_000;
const SEED: u64 = 0xDEAD_BEEF;
// ─────────────────────────────────────────────────────────────────────────────

fn sep() {
    println!("{}", "─".repeat(70));
}

// ── CSV generation ────────────────────────────────────────────────────────────

/// Write a synthetic CSV to a temp file. Returns the file and its byte size.
/// Columns: id (i64), col1..col9 (random alphanumeric strings, 4-16 chars).
fn generate_csv() -> (NamedTempFile, u64) {
    println!("Generating synthetic CSV ({} rows × {} cols)…", NUM_ROWS, NUM_COLS);
    let mut rng = rand::rngs::StdRng::seed_from_u64(SEED);
    let mut file = NamedTempFile::new().expect("tempfile");

    // header
    let mut header = String::from("id");
    for i in 1..NUM_COLS {
        header.push_str(&format!(",col{}", i));
    }
    header.push('\n');
    file.write_all(header.as_bytes()).unwrap();

    // rows
    const CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut buf = String::with_capacity(256);
    for id in 0..NUM_ROWS {
        buf.clear();
        buf.push_str(&id.to_string());
        for _ in 1..NUM_COLS {
            buf.push(',');
            let len = rng.gen_range(4..=16usize);
            for _ in 0..len {
                buf.push(CHARS[rng.gen_range(0..CHARS.len())] as char);
            }
        }
        buf.push('\n');
        file.write_all(buf.as_bytes()).unwrap();
    }
    file.flush().unwrap();

    let size = file.as_file().metadata().unwrap().len();
    println!("CSV written: {:.1} MB", size as f64 / 1_048_576.0);
    (file, size)
}

// ── helpers ───────────────────────────────────────────────────────────────────

/// Parse a single CSV line (already located in the mmap) into its fields,
/// skipping the key column. Returns `Vec<Option<String>>` where None = empty.
#[inline]
fn parse_line_fields(line: &[u8]) -> Vec<Option<String>> {
    // skip key column (first field)
    let mut fields = Vec::with_capacity(NUM_COLS - 1);
    let mut col = 0;
    let mut start = 0;
    for i in 0..=line.len() {
        let ch = if i < line.len() { line[i] } else { b',' };
        if ch == b',' || i == line.len() {
            if col > 0 {
                let s = &line[start..i];
                if s.is_empty() {
                    fields.push(None);
                } else {
                    fields.push(Some(String::from_utf8_lossy(s).into_owned()));
                }
            }
            col += 1;
            start = i + 1;
        }
    }
    fields
}

/// Find the byte span of the line at `offset` within the mmap.
/// Returns the slice of bytes for that line (excluding the newline).
#[inline]
fn line_at(mmap: &Mmap, offset: u64) -> &[u8] {
    let start = offset as usize;
    let data = &mmap[start..];
    let end = memchr(data, b'\n').unwrap_or(data.len());
    &data[..end]
}

/// Minimal memchr replacement (std doesn't expose it for slices directly).
#[inline]
fn memchr(haystack: &[u8], needle: u8) -> Option<usize> {
    haystack.iter().position(|&b| b == needle)
}

// ── random lookup keys ────────────────────────────────────────────────────────

fn random_keys(n: usize) -> Vec<i64> {
    let mut rng = rand::rngs::StdRng::seed_from_u64(SEED ^ 0x1234);
    (0..n).map(|_| rng.gen_range(0..NUM_ROWS)).collect()
}

// ── Approach A ────────────────────────────────────────────────────────────────

struct ApproachA {
    map: HashMap<i64, Vec<Option<String>>>,
}

impl ApproachA {
    fn build(path: &std::path::Path) -> (Self, std::time::Duration) {
        let t = Instant::now();
        let content = std::fs::read(path).unwrap();
        let mut map = HashMap::with_capacity(NUM_ROWS as usize);
        let mut lines = content.split(|&b| b == b'\n');
        lines.next(); // skip header
        for line in lines {
            if line.is_empty() {
                continue;
            }
            // parse key
            let comma = memchr(line, b',').unwrap_or(line.len());
            let key: i64 = std::str::from_utf8(&line[..comma])
                .unwrap()
                .parse()
                .unwrap();
            let fields = parse_line_fields(line);
            map.insert(key, fields);
        }
        (Self { map }, t.elapsed())
    }

    #[inline]
    fn lookup(&self, key: i64) -> Option<&Vec<Option<String>>> {
        self.map.get(&key)
    }
}

// ── Approach B ────────────────────────────────────────────────────────────────

struct ApproachB {
    index: HashMap<i64, u64>, // key → byte offset
    mmap: Mmap,
}

impl ApproachB {
    fn build(path: &std::path::Path) -> (Self, std::time::Duration) {
        let t = Instant::now();
        let file = std::fs::File::open(path).unwrap();
        let mmap = unsafe { Mmap::map(&file).unwrap() };

        let mut index = HashMap::with_capacity(NUM_ROWS as usize);
        let data = &mmap[..];

        let mut pos: usize = 0;
        // skip header line
        if let Some(nl) = memchr(&data[pos..], b'\n') {
            pos += nl + 1;
        }

        while pos < data.len() {
            let line_start = pos as u64;
            let slice = &data[pos..];
            let nl = memchr(slice, b'\n').unwrap_or(slice.len());
            let line = &slice[..nl];
            if !line.is_empty() {
                let comma = memchr(line, b',').unwrap_or(line.len());
                let key: i64 = std::str::from_utf8(&line[..comma])
                    .unwrap()
                    .parse()
                    .unwrap();
                index.insert(key, line_start);
            }
            pos += nl + 1;
        }

        (Self { index, mmap }, t.elapsed())
    }

    #[inline]
    fn lookup(&self, key: i64) -> Option<Vec<Option<String>>> {
        let offset = *self.index.get(&key)?;
        let line = line_at(&self.mmap, offset);
        Some(parse_line_fields(line))
    }
}

// ── Approach C ────────────────────────────────────────────────────────────────

struct ApproachC {
    index: Vec<(i64, u64)>, // sorted by key
    mmap: Mmap,
}

impl ApproachC {
    fn build(path: &std::path::Path) -> (Self, std::time::Duration) {
        let t = Instant::now();
        let file = std::fs::File::open(path).unwrap();
        let mmap = unsafe { Mmap::map(&file).unwrap() };

        let mut index = Vec::with_capacity(NUM_ROWS as usize);
        let data = &mmap[..];

        let mut pos: usize = 0;
        if let Some(nl) = memchr(&data[pos..], b'\n') {
            pos += nl + 1;
        }

        while pos < data.len() {
            let line_start = pos as u64;
            let slice = &data[pos..];
            let nl = memchr(slice, b'\n').unwrap_or(slice.len());
            let line = &slice[..nl];
            if !line.is_empty() {
                let comma = memchr(line, b',').unwrap_or(line.len());
                let key: i64 = std::str::from_utf8(&line[..comma])
                    .unwrap()
                    .parse()
                    .unwrap();
                index.push((key, line_start));
            }
            pos += nl + 1;
        }

        index.sort_unstable_by_key(|&(k, _)| k);
        (Self { index, mmap }, t.elapsed())
    }

    #[inline]
    fn lookup(&self, key: i64) -> Option<Vec<Option<String>>> {
        let pos = self.index.partition_point(|&(k, _)| k < key);
        if pos < self.index.len() && self.index[pos].0 == key {
            let offset = self.index[pos].1;
            let line = line_at(&self.mmap, offset);
            Some(parse_line_fields(line))
        } else {
            None
        }
    }
}

// ── Approach D ────────────────────────────────────────────────────────────────
// Assumes keys are dense 0-based i64 (slot IDs). O(1) direct-index lookup.

struct ApproachD {
    offsets: Vec<u64>, // index = key, value = byte offset (u64::MAX = absent)
    mmap: Mmap,
}

impl ApproachD {
    fn build(path: &std::path::Path) -> (Self, std::time::Duration) {
        let t = Instant::now();
        let file = std::fs::File::open(path).unwrap();
        let mmap = unsafe { Mmap::map(&file).unwrap() };

        let mut offsets = vec![u64::MAX; NUM_ROWS as usize];
        let data = &mmap[..];

        let mut pos: usize = 0;
        if let Some(nl) = memchr(&data[pos..], b'\n') {
            pos += nl + 1;
        }

        while pos < data.len() {
            let line_start = pos as u64;
            let slice = &data[pos..];
            let nl = memchr(slice, b'\n').unwrap_or(slice.len());
            let line = &slice[..nl];
            if !line.is_empty() {
                let comma = memchr(line, b',').unwrap_or(line.len());
                let key: i64 = std::str::from_utf8(&line[..comma])
                    .unwrap()
                    .parse()
                    .unwrap();
                if key >= 0 && key < NUM_ROWS {
                    offsets[key as usize] = line_start;
                }
            }
            pos += nl + 1;
        }

        (Self { offsets, mmap }, t.elapsed())
    }

    #[inline]
    fn lookup(&self, key: i64) -> Option<Vec<Option<String>>> {
        if key < 0 || key >= NUM_ROWS {
            return None;
        }
        let offset = self.offsets[key as usize];
        if offset == u64::MAX {
            return None;
        }
        let line = line_at(&self.mmap, offset);
        Some(parse_line_fields(line))
    }
}

// ── memory size estimates ─────────────────────────────────────────────────────

fn approx_hashmap_a_bytes(map: &HashMap<i64, Vec<Option<String>>>) -> usize {
    // HashMap overhead + per-entry: key (8) + Vec ptr/len/cap (24) + heap strings
    // We sample a few entries and extrapolate.
    let base = map.capacity() * (8 + 24 + 8); // key + Vec header + pointer overhead
    // rough string content: ~10 chars avg per field, 9 fields, 1M rows
    let avg_string_bytes = 10 * (NUM_COLS - 1) * map.len();
    base + avg_string_bytes
}

fn approx_hashmap_b_bytes(map: &HashMap<i64, u64>) -> usize {
    map.capacity() * 16 // 8 (key) + 8 (value)
}

fn approx_vec_c_bytes(v: &[(i64, u64)]) -> usize {
    v.len() * 16
}

fn approx_vec_d_bytes(v: &[u64]) -> usize {
    v.len() * 8
}

// ── benchmark runner ──────────────────────────────────────────────────────────

fn bench_lookups<F: Fn(i64) -> bool>(label: &str, keys: &[i64], lookup: F) -> f64 {
    // warm up
    for &k in &keys[..1000.min(keys.len())] {
        let _ = lookup(k);
    }

    let t = Instant::now();
    let mut found = 0usize;
    for &k in keys {
        if lookup(k) {
            found += 1;
        }
    }
    let elapsed = t.elapsed();
    let ns_per_lookup = elapsed.as_nanos() as f64 / keys.len() as f64;
    let throughput = keys.len() as f64 / elapsed.as_secs_f64();
    println!(
        "  {} — {:.1} ns/lookup  |  {:.0} lookups/sec  |  {}/{} found",
        label,
        ns_per_lookup,
        throughput,
        found,
        keys.len()
    );
    ns_per_lookup
}

// ── main ──────────────────────────────────────────────────────────────────────

fn main() {
    println!("=== csv_mmap_bench ===");
    println!(
        "Rows: {}  Cols: {}  Lookups: {}",
        NUM_ROWS, NUM_COLS, NUM_LOOKUPS
    );
    sep();

    // Generate CSV
    let (tmp, file_bytes) = generate_csv();
    let path = tmp.path();
    sep();

    // Random lookup keys (same set for all approaches)
    let lookup_keys = random_keys(NUM_LOOKUPS);

    // ── Approach A ────────────────────────────────────────────────────────────
    println!("Approach A: Full parse → HashMap<i64, Vec<Option<String>>>");
    let (a, a_build) = ApproachA::build(path);
    let a_mem = approx_hashmap_a_bytes(&a.map);
    println!(
        "  Build: {:.0} ms  |  ~{:.1} MB estimated heap",
        a_build.as_millis(),
        a_mem as f64 / 1_048_576.0
    );
    let a_ns = bench_lookups("Lookup", &lookup_keys, |k| a.lookup(k).is_some());
    sep();

    // ── Approach B ────────────────────────────────────────────────────────────
    println!("Approach B: HashMap<i64, u64> offset index + mmap on-demand parse");
    let (b, b_build) = ApproachB::build(path);
    let b_mem = approx_hashmap_b_bytes(&b.index);
    println!(
        "  Build: {:.0} ms  |  ~{:.1} MB estimated index heap  |  file mmap: {:.1} MB",
        b_build.as_millis(),
        b_mem as f64 / 1_048_576.0,
        file_bytes as f64 / 1_048_576.0,
    );
    let b_ns = bench_lookups("Lookup", &lookup_keys, |k| b.lookup(k).is_some());
    sep();

    // ── Approach C ────────────────────────────────────────────────────────────
    println!("Approach C: sorted Vec<(i64, u64)> offset index + mmap binary search");
    let (c, c_build) = ApproachC::build(path);
    let c_mem = approx_vec_c_bytes(&c.index);
    println!(
        "  Build: {:.0} ms  |  ~{:.1} MB estimated index heap  |  file mmap: {:.1} MB",
        c_build.as_millis(),
        c_mem as f64 / 1_048_576.0,
        file_bytes as f64 / 1_048_576.0,
    );
    let c_ns = bench_lookups("Lookup", &lookup_keys, |k| c.lookup(k).is_some());
    sep();

    // ── Approach D ────────────────────────────────────────────────────────────
    println!("Approach D: dense Vec<u64> offset index (O(1)) + mmap on-demand parse");
    let (d, d_build) = ApproachD::build(path);
    let d_mem = approx_vec_d_bytes(&d.offsets);
    println!(
        "  Build: {:.0} ms  |  ~{:.1} MB estimated index heap  |  file mmap: {:.1} MB",
        d_build.as_millis(),
        d_mem as f64 / 1_048_576.0,
        file_bytes as f64 / 1_048_576.0,
    );
    let d_ns = bench_lookups("Lookup", &lookup_keys, |k| d.lookup(k).is_some());
    sep();

    // ── Summary ───────────────────────────────────────────────────────────────
    println!("SUMMARY");
    println!(
        "  A  Full parse HashMap:   build={:>5} ms  index~={:>6.0} MB  lookup={:>7.1} ns",
        a_build.as_millis(),
        approx_hashmap_a_bytes(&a.map) as f64 / 1_048_576.0,
        a_ns
    );
    println!(
        "  B  HashMap offset index: build={:>5} ms  index~={:>6.0} MB  lookup={:>7.1} ns",
        b_build.as_millis(),
        b_mem as f64 / 1_048_576.0,
        b_ns
    );
    println!(
        "  C  Vec offset index:     build={:>5} ms  index~={:>6.0} MB  lookup={:>7.1} ns",
        c_build.as_millis(),
        c_mem as f64 / 1_048_576.0,
        c_ns
    );
    println!(
        "  D  Dense Vec (O(1)):     build={:>5} ms  index~={:>6.0} MB  lookup={:>7.1} ns",
        d_build.as_millis(),
        d_mem as f64 / 1_048_576.0,
        d_ns
    );
    sep();

    // ── Projection: 14.6M lookups ─────────────────────────────────────────────
    let scale: f64 = 14_600_000.0;
    println!("Projection for {:.1}M lookups (14.6M image rows):", scale / 1e6);
    for (label, ns) in [("A", a_ns), ("B", b_ns), ("C", c_ns), ("D", d_ns)] {
        let total_s = ns * scale / 1e9;
        println!("  {} → {:.2} seconds", label, total_s);
    }
    sep();
}
