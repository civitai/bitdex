use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

const NUM_ROWS: u64 = 107_000_000;
const DOC_SIZE: usize = 200;

fn run_variant(label: &str, threads: usize, buf_size: usize) {
    let silo_dir = PathBuf::from("bench_silo_tmp");
    let _ = fs::remove_dir_all(&silo_dir);
    fs::create_dir_all(&silo_dir).unwrap();

    let pool = rayon::ThreadPoolBuilder::new().num_threads(threads).build().unwrap();
    let doc: Vec<u8> = vec![0x41u8; DOC_SIZE];
    let rows_per_thread = NUM_ROWS / threads as u64;
    let total_written = AtomicU64::new(0);

    let t = Instant::now();
    pool.scope(|s| {
        for tid in 0..threads {
            let doc = &doc;
            let total_written = &total_written;
            let silo_dir = &silo_dir;
            s.spawn(move |_| {
                let path = silo_dir.join(format!("silo_{:02}.dat", tid));
                let file = File::create(&path).unwrap();
                let mut w = BufWriter::with_capacity(buf_size, file);
                for i in 0..rows_per_thread {
                    let slot = (tid as u64 * rows_per_thread + i) as u32;
                    w.write_all(&slot.to_le_bytes()).unwrap();
                    w.write_all(&(DOC_SIZE as u32).to_le_bytes()).unwrap();
                    w.write_all(doc).unwrap();
                    if i % 1_000_000 == 0 && i > 0 {
                        total_written.fetch_add(1_000_000, Ordering::Relaxed);
                    }
                }
                w.flush().unwrap();
            });
        }
    });
    let elapsed = t.elapsed();
    let rate = NUM_ROWS as f64 / elapsed.as_secs_f64();
    let bw = (NUM_ROWS as f64 * (DOC_SIZE + 8) as f64) / elapsed.as_secs_f64() / 1e9;
    let pass = rate >= 5_000_000.0;
    println!("  {}: {:.1}M/s ({:.1} GB/s) in {:.1}s — {}", 
        label, rate / 1e6, bw, elapsed.as_secs_f64(), if pass { "✓" } else { "✗" });
    let _ = fs::remove_dir_all(&silo_dir);
}

fn main() {
    println!("=== Benchmark 0 Variants ===");
    println!("  107M rows × 208 bytes = 21.4 GB\n");
    run_variant("A: 28 threads, 8MB buf  ", 28, 8 * 1024 * 1024);
    run_variant("B:  4 threads, 8MB buf  ", 4, 8 * 1024 * 1024);
    run_variant("C:  8 threads, 8MB buf  ", 8, 8 * 1024 * 1024);
}
