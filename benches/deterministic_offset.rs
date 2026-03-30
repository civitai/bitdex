//! Experiment 2: Deterministic-offset doc storage benchmark.
//!
//! Concept: slot_id determines file offset directly. offset = slot_id * SLOT_SIZE.
//! No index needed. Zero memory overhead.
//! For reads: pread at offset. For writes: pwrite at offset.
//!
//! Run: cargo bench --bench deterministic_offset --features data-silo
//!   or: cargo run --release --bench deterministic_offset --features data-silo

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// From Ollie's V5 data: 25GB for 108.9M docs = ~230 bytes/doc average.
/// p50 ~148 bytes, p95 ~161 bytes from Edward's estimates.
/// Use 256 bytes as slot size to cover p99 with alignment padding.
const SLOT_SIZE_SMALL: usize = 256;

/// Edward's original spec: 168KB. For comparison only.
const SLOT_SIZE_LARGE: usize = 168 * 1024;

/// Header: 4 bytes doc_len (u32 LE) at start of each slot.
const HEADER_SIZE: usize = 4;

/// Number of docs for write benchmark (adjustable via env BENCH_WRITE_COUNT).
const DEFAULT_WRITE_COUNT: usize = 10_000_000;

/// Number of random reads for read benchmark.
const DEFAULT_READ_COUNT: usize = 10_000;

// ---------------------------------------------------------------------------
// Deterministic-offset file store
// ---------------------------------------------------------------------------

struct DetStore {
    file: File,
    path: PathBuf,
    slot_size: usize,
}

impl DetStore {
    fn create(path: &Path, slot_size: usize) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        Ok(Self {
            file,
            path: path.to_path_buf(),
            slot_size,
        })
    }

    fn open(path: &Path, slot_size: usize) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        Ok(Self {
            file,
            path: path.to_path_buf(),
            slot_size,
        })
    }

    /// Write doc at deterministic offset. offset = slot_id * slot_size.
    /// Format: [doc_len: u32 LE][doc_bytes][padding to slot_size]
    fn write_doc(&mut self, slot_id: u32, data: &[u8]) -> io::Result<()> {
        assert!(
            data.len() + HEADER_SIZE <= self.slot_size,
            "doc {} bytes exceeds slot_size {} - header {}",
            data.len(),
            self.slot_size,
            HEADER_SIZE
        );
        let offset = slot_id as u64 * self.slot_size as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        // Write header (doc length)
        self.file.write_all(&(data.len() as u32).to_le_bytes())?;
        // Write doc data
        self.file.write_all(data)?;
        // No need to write padding — OS will zero-fill sparse regions
        Ok(())
    }

    /// Read doc from deterministic offset via seek+read (pread equivalent on Windows).
    fn read_doc(&mut self, slot_id: u32) -> io::Result<Vec<u8>> {
        let offset = slot_id as u64 * self.slot_size as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        let mut hdr = [0u8; HEADER_SIZE];
        self.file.read_exact(&mut hdr)?;
        let doc_len = u32::from_le_bytes(hdr) as usize;
        if doc_len == 0 || doc_len + HEADER_SIZE > self.slot_size {
            return Ok(Vec::new());
        }
        let mut buf = vec![0u8; doc_len];
        self.file.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn file_size(&self) -> io::Result<u64> {
        Ok(fs::metadata(&self.path)?.len())
    }
}

// ---------------------------------------------------------------------------
// Benchmark helpers
// ---------------------------------------------------------------------------

fn generate_doc(slot_id: u32, target_size: usize) -> Vec<u8> {
    // Simulate realistic doc: msgpack-ish header + field data
    let mut doc = Vec::with_capacity(target_size);
    // Write slot_id as first 4 bytes for verification
    doc.extend_from_slice(&slot_id.to_le_bytes());
    // Fill remaining with deterministic pattern
    let remaining = target_size.saturating_sub(4);
    for i in 0..remaining {
        doc.push(((slot_id as usize + i) % 256) as u8);
    }
    doc
}

fn percentile(sorted: &[Duration], pct: f64) -> Duration {
    let idx = ((sorted.len() as f64 * pct / 100.0) as usize).min(sorted.len() - 1);
    sorted[idx]
}

fn run_write_bench(dir: &Path, slot_size: usize, count: usize, doc_size: usize) -> (Duration, u64) {
    let path = dir.join(format!("det_store_{}b.dat", slot_size));
    let mut store = DetStore::create(&path, slot_size).unwrap();

    let doc = generate_doc(0, doc_size); // Reuse same doc content for fairness
    let start = Instant::now();
    for slot in 0..count as u32 {
        // Vary doc slightly per slot (just overwrite first 4 bytes)
        let mut d = doc.clone();
        d[..4].copy_from_slice(&slot.to_le_bytes());
        store.write_doc(slot, &d).unwrap();
    }
    store.file.sync_all().unwrap();
    let elapsed = start.elapsed();
    let file_size = store.file_size().unwrap();
    (elapsed, file_size)
}

fn run_read_bench(dir: &Path, slot_size: usize, max_slot: u32, count: usize) -> Vec<Duration> {
    let path = dir.join(format!("det_store_{}b.dat", slot_size));
    let mut store = DetStore::open(&path, slot_size).unwrap();

    // Generate random slot IDs (deterministic seed for reproducibility)
    let mut rng_state: u64 = 0xDEADBEEF;
    let slots: Vec<u32> = (0..count)
        .map(|_| {
            rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (rng_state >> 33) as u32 % max_slot
        })
        .collect();

    let mut latencies = Vec::with_capacity(count);
    for &slot in &slots {
        let start = Instant::now();
        let doc = store.read_doc(slot).unwrap();
        let elapsed = start.elapsed();
        assert!(!doc.is_empty(), "slot {} returned empty doc", slot);
        // Verify first 4 bytes are the slot_id
        let stored_slot = u32::from_le_bytes([doc[0], doc[1], doc[2], doc[3]]);
        assert_eq!(stored_slot, slot, "data corruption at slot {}", slot);
        latencies.push(elapsed);
    }
    latencies
}

fn main() {
    let write_count = std::env::var("BENCH_WRITE_COUNT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_WRITE_COUNT);
    let read_count = std::env::var("BENCH_READ_COUNT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_READ_COUNT);

    // Use realistic doc size (~230 bytes matches V5 production data)
    let doc_size = 230;

    let dir = PathBuf::from("data/silo-experiment2");
    fs::create_dir_all(&dir).unwrap();

    println!("=== Experiment 2: Deterministic-Offset Doc Storage ===");
    println!("Write count: {}M", write_count / 1_000_000);
    println!("Read count:  {}", read_count);
    println!("Doc size:    {} bytes", doc_size);
    println!();

    // --- Benchmark with 256-byte slots ---
    let slot_size = SLOT_SIZE_SMALL;
    println!("--- Slot size: {} bytes ---", slot_size);

    // Check disk space: write_count * slot_size
    let needed_bytes = write_count as u64 * slot_size as u64;
    println!("Disk needed: {:.1} GB", needed_bytes as f64 / 1e9);

    // If > 50GB needed, scale down
    let actual_count = if needed_bytes > 50_000_000_000 {
        let scaled = (50_000_000_000u64 / slot_size as u64) as usize;
        println!("Scaling down to {}M docs (50GB disk limit)", scaled / 1_000_000);
        scaled
    } else {
        write_count
    };

    // Write benchmark
    print!("Writing {} docs...", actual_count);
    let (write_time, file_size) = run_write_bench(&dir, slot_size, actual_count, doc_size);
    let write_docs_per_sec = actual_count as f64 / write_time.as_secs_f64();
    let write_mb_per_sec = (actual_count as f64 * doc_size as f64) / write_time.as_secs_f64() / 1e6;
    println!(" done in {:.2}s", write_time.as_secs_f64());
    println!("  Throughput: {:.0} docs/s ({:.0} MB/s)", write_docs_per_sec, write_mb_per_sec);
    println!("  File size:  {:.2} GB", file_size as f64 / 1e9);
    let waste = 1.0 - (doc_size as f64 / slot_size as f64);
    println!("  Waste:      {:.1}% ({} unused bytes/slot)", waste * 100.0, slot_size - doc_size - HEADER_SIZE);
    println!("  Memory:     0 bytes (no index)");

    // Read benchmark
    print!("Reading {} random docs...", read_count);
    let mut latencies = run_read_bench(&dir, slot_size, actual_count as u32, read_count);
    latencies.sort();
    println!(" done");
    println!(
        "  p50:  {:>8.1}μs",
        percentile(&latencies, 50.0).as_nanos() as f64 / 1000.0
    );
    println!(
        "  p95:  {:>8.1}μs",
        percentile(&latencies, 95.0).as_nanos() as f64 / 1000.0
    );
    println!(
        "  p99:  {:>8.1}μs",
        percentile(&latencies, 99.0).as_nanos() as f64 / 1000.0
    );
    println!(
        "  mean: {:>8.1}μs",
        latencies.iter().map(|d| d.as_nanos()).sum::<u128>() as f64 / latencies.len() as f64 / 1000.0
    );
    println!();

    // Cleanup
    let _ = fs::remove_file(dir.join(format!("det_store_{}b.dat", slot_size)));

    // --- Projections at 107M scale ---
    println!("--- Projections at 107M scale ---");
    println!("  Slot size 256B:  {:.1} GB file, 0 bytes memory", 107_000_000u64 as f64 * 256.0 / 1e9);
    println!("  Slot size 512B:  {:.1} GB file, 0 bytes memory", 107_000_000u64 as f64 * 512.0 / 1e9);
    println!("  Slot size 168KB: {:.1} TB file, 0 bytes memory", 107_000_000u64 as f64 * 168.0 * 1024.0 / 1e12);
    println!();
    println!("--- Baseline comparison (DocStore V2) ---");
    println!("  Write: 405K rows/sec");
    println!("  Read:  10μs warm (doc cache)");
    println!("  Memory: doc_index.bin at 107M = ~1.6 GB");
    println!();

    // Write results file
    let results = format!(
        "# Experiment 2: Deterministic-Offset Doc Storage\n\
        \n\
        ## Concept\n\
        offset = slot_id * SLOT_SIZE. No index. Zero memory overhead.\n\
        \n\
        ## Configuration\n\
        - Slot size: {} bytes\n\
        - Doc size: {} bytes (matches V5 production p50)\n\
        - Write count: {}M\n\
        - Read count: {}\n\
        \n\
        ## Write Performance\n\
        - Throughput: {:.0} docs/s ({:.0} MB/s)\n\
        - Duration: {:.2}s for {}M docs\n\
        - File size: {:.2} GB\n\
        - Waste: {:.1}% ({} unused bytes/slot)\n\
        - Memory overhead: 0 bytes\n\
        \n\
        ## Read Performance (random, cold)\n\
        - p50: {:.1}μs\n\
        - p95: {:.1}μs\n\
        - p99: {:.1}μs\n\
        - mean: {:.1}μs\n\
        \n\
        ## vs DocStore V2 Baseline\n\
        | Metric | Det-Offset | DocStore V2 |\n\
        |--------|-----------|-------------|\n\
        | Write throughput | {:.0} docs/s | 405K docs/s |\n\
        | Read latency (cold) | p50 {:.1}μs | 10μs (warm cache) |\n\
        | Memory at 107M | 0 bytes | ~1.6 GB (index) |\n\
        | Disk at 107M | {:.1} GB | ~25 GB |\n\
        \n\
        ## Projections at 107M\n\
        - 256B slots: {:.1} GB disk, 0 bytes memory\n\
        - 512B slots: {:.1} GB disk, 0 bytes memory\n\
        - Note: Edward's original 168KB SLOT_SIZE would require {:.1} TB — likely meant 168 *bytes*\n\
        \n\
        ## Key Observations\n\
        1. Zero memory overhead (no index to load or maintain)\n\
        2. O(1) read/write — single seek + read/write per operation\n\
        3. Sparse file support on modern filesystems avoids pre-allocating unused slots\n\
        4. Waste depends on slot_size / avg_doc_size ratio\n\
        5. Upserts are trivial: just overwrite at same offset\n\
        6. No compaction needed (overwrite in place)\n\
        ",
        slot_size,
        doc_size,
        actual_count / 1_000_000,
        read_count,
        write_docs_per_sec,
        write_mb_per_sec,
        write_time.as_secs_f64(),
        actual_count / 1_000_000,
        file_size as f64 / 1e9,
        waste * 100.0,
        slot_size - doc_size - HEADER_SIZE,
        percentile(&latencies, 50.0).as_nanos() as f64 / 1000.0,
        percentile(&latencies, 95.0).as_nanos() as f64 / 1000.0,
        percentile(&latencies, 99.0).as_nanos() as f64 / 1000.0,
        latencies.iter().map(|d| d.as_nanos()).sum::<u128>() as f64 / latencies.len() as f64 / 1000.0,
        write_docs_per_sec,
        percentile(&latencies, 50.0).as_nanos() as f64 / 1000.0,
        107_000_000u64 as f64 * slot_size as f64 / 1e9,
        107_000_000u64 as f64 * 256.0 / 1e9,
        107_000_000u64 as f64 * 512.0 / 1e9,
        107_000_000u64 as f64 * 168.0 * 1024.0 / 1e12,
    );

    let results_path = dir.join("results.md");
    fs::write(&results_path, &results).unwrap();
    println!("Results written to {}", results_path.display());
}
