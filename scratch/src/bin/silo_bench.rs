/// Benchmark DataSilo bulk load + ops append + read throughput.
use rayon::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

fn main() {
    println!("=== DataSilo Benchmark ===\n");

    let dir = tempfile::tempdir().unwrap();
    let mut silo = datasilo::DataSilo::open(
        dir.path(),
        datasilo::SiloConfig { buffer_ratio: 1.0 }, // no buffer for benchmark
    ).unwrap();

    // Simulate doc entries: ~230 bytes each (avg doc size)
    let num_entries = 10_000_000u32;
    let doc_size = 230;
    let doc_data: Vec<u8> = (0..doc_size).map(|i| (i % 256) as u8).collect();

    // Bench 1a: Bulk load (BufWriter path)
    println!("--- Bulk Load - BufWriter ({} entries, {}B each) ---", num_entries, doc_size);
    let t = Instant::now();
    let entries = (0..num_entries).map(|i| (i, doc_data.clone()));
    let count = silo.bulk_load(entries).unwrap();
    let bulk_elapsed = t.elapsed();
    let bulk_rate = count as f64 / bulk_elapsed.as_secs_f64();
    println!("  {} entries in {:.2}s ({:.1}M entries/sec)\n",
        count, bulk_elapsed.as_secs_f64(), bulk_rate / 1e6);

    // Bench 1b: Bulk load (presized mmap path)
    let dir2 = tempfile::tempdir().unwrap();
    let mut silo2 = datasilo::DataSilo::open(
        dir2.path(),
        datasilo::SiloConfig { buffer_ratio: 1.0 },
    ).unwrap();
    println!("--- Bulk Load - Presized mmap ({} entries, {}B each) ---", num_entries, doc_size);
    let total_bytes = num_entries as u64 * doc_size as u64;
    let t = Instant::now();
    let entries2 = (0..num_entries).map(|i| (i, doc_data.clone()));
    let count2 = silo2.bulk_load_presized(num_entries - 1, total_bytes, entries2).unwrap();
    let mmap_elapsed = t.elapsed();
    let mmap_rate = count2 as f64 / mmap_elapsed.as_secs_f64();
    println!("  {} entries in {:.2}s ({:.1}M entries/sec)\n",
        count2, mmap_elapsed.as_secs_f64(), mmap_rate / 1e6);
    // Bench 1c: Parallel mmap (multiple threads writing concurrently)
    let dir3 = tempfile::tempdir().unwrap();
    let mut silo3 = datasilo::DataSilo::open(
        dir3.path(),
        datasilo::SiloConfig { buffer_ratio: 1.0 },
    ).unwrap();
    println!("--- Bulk Load - Parallel mmap with thread regions ({} entries, {}B, {} threads) ---",
        num_entries, doc_size, rayon::current_num_threads());
    // Overallocate 20% to account for region padding
    let total_bytes = (num_entries as u64 * doc_size as u64) * 6 / 5;
    let t = Instant::now();
    let writer = silo3.prepare_parallel_writer(num_entries - 1, total_bytes).unwrap();
    let chunk_size = (num_entries as usize / rayon::current_num_threads()).max(1);
    // Each rayon thread gets a ThreadWriter with 1MB regions — sequential within region
    (0..num_entries).into_par_iter().with_min_len(chunk_size).for_each_init(
        || writer.thread_writer(),
        |tw, i| { tw.write(i, &doc_data); },
    );
    let par_count = silo3.finish_parallel_write(writer).unwrap();
    let par_elapsed = t.elapsed();
    let par_rate = par_count as f64 / par_elapsed.as_secs_f64();
    println!("  {} entries in {:.2}s ({:.1}M entries/sec)\n",
        par_count, par_elapsed.as_secs_f64(), par_rate / 1e6);

    // Use silo3 for subsequent benchmarks
    let mut silo = silo3;

    // Bench 2: Random reads
    println!("--- Random Reads (100K lookups) ---");
    let t = Instant::now();
    let mut found = 0u64;
    for i in 0..100_000u32 {
        let key = (i * 7 + 13) % num_entries;
        if silo.get(key).is_some() { found += 1; }
    }
    let read_elapsed = t.elapsed();
    let read_rate = 100_000.0 / read_elapsed.as_secs_f64();
    println!("  {} found in {:.2}ms ({:.1}M reads/sec)\n",
        found, read_elapsed.as_secs_f64() * 1000.0, read_rate / 1e6);

    // Bench 3: Append ops (simulating phase 2+ Merge writes)
    println!("--- Append Ops (100K ops) ---");
    let t = Instant::now();
    for i in 0..100_000u32 {
        silo.append_op(i, doc_data.clone()).unwrap();
    }
    let ops_elapsed = t.elapsed();
    let ops_rate = 100_000.0 / ops_elapsed.as_secs_f64();
    println!("  100K ops in {:.2}ms ({:.1}K ops/sec)\n",
        ops_elapsed.as_secs_f64() * 1000.0, ops_rate / 1e3);

    // Bench 4: Batch ops (simulating phase 2+ bulk Merge)
    println!("--- Batch Ops (1M ops in batches of 10K) ---");
    let t = Instant::now();
    let batch_size = 10_000;
    let num_batches = 100;
    for batch_idx in 0..num_batches {
        let batch: Vec<(u32, Vec<u8>)> = (0..batch_size)
            .map(|i| {
                let key = batch_idx * batch_size + i;
                (key, doc_data.clone())
            })
            .collect();
        silo.append_ops_batch(&batch).unwrap();
    }
    let batch_elapsed = t.elapsed();
    let total_ops = (num_batches * batch_size) as f64;
    let batch_rate = total_ops / batch_elapsed.as_secs_f64();
    println!("  {}M ops in {:.2}s ({:.1}M ops/sec)\n",
        total_ops / 1e6, batch_elapsed.as_secs_f64(), batch_rate / 1e6);

    // Bench 5: Read with pending ops
    println!("--- Reads with Pending Ops ({} pending) ---", silo.pending_count());
    let t = Instant::now();
    found = 0;
    for i in 0..100_000u32 {
        if silo.get(i).is_some() { found += 1; }
    }
    let pending_read_elapsed = t.elapsed();
    let pending_read_rate = 100_000.0 / pending_read_elapsed.as_secs_f64();
    println!("  {} found in {:.2}ms ({:.1}M reads/sec)\n",
        found, pending_read_elapsed.as_secs_f64() * 1000.0, pending_read_rate / 1e6);

    println!("=== Summary ===");
    println!("  Bulk load:     {:.1}M entries/sec", bulk_rate / 1e6);
    println!("  Random reads:  {:.1}M reads/sec", read_rate / 1e6);
    println!("  Single ops:    {:.1}K ops/sec", ops_rate / 1e3);
    println!("  Batch ops:     {:.1}M ops/sec", batch_rate / 1e6);
    println!("  Pending reads: {:.1}M reads/sec", pending_read_rate / 1e6);
    println!("");
    println!("  At 109M entries: {:.1}s bulk load",
        109e6 / bulk_rate);
}
