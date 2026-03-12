//! Microbench: Can readers do relaxed AtomicU64 stores on values inside a
//! HashMap that lives behind Arc + ArcSwap?
//!
//! This validates the idle-eviction design where FilterField contains
//! `HashMap<u64, AtomicU64>` for last-touched stamps. Reader threads load a
//! snapshot via `ArcSwap::load()`, traverse to the HashMap, and do
//! `store(cycle, Relaxed)` on the AtomicU64 values. The writer thread
//! periodically clones the struct (loading each AtomicU64 to copy it),
//! modifies it, and publishes a new snapshot via `ArcSwap::store()`.

use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

/// Simulates the relevant part of InnerEngine / FilterField.
/// The HashMap maps field-value keys to last-touched cycle stamps.
struct Snapshot {
    stamps: HashMap<u64, AtomicU64>,
    generation: u64,
}

impl Snapshot {
    fn new(num_keys: u64, generation: u64) -> Self {
        let mut stamps = HashMap::with_capacity(num_keys as usize);
        for k in 0..num_keys {
            stamps.insert(k, AtomicU64::new(0));
        }
        Self { stamps, generation }
    }

    /// Manual clone that loads each AtomicU64 — this is what the writer
    /// (flush thread) would do when creating a new snapshot.
    fn clone_loading(&self) -> Self {
        let mut stamps = HashMap::with_capacity(self.stamps.len());
        for (&k, v) in &self.stamps {
            stamps.insert(k, AtomicU64::new(v.load(Ordering::Relaxed)));
        }
        Self {
            stamps,
            generation: self.generation,
        }
    }
}

#[test]
fn readers_can_relaxed_store_through_arcswap() {
    const NUM_KEYS: u64 = 1000;
    const NUM_READERS: usize = 4;
    const DURATION_MS: u64 = 500;
    const WRITER_PUBLISH_INTERVAL_US: u64 = 500; // publish new snapshot every 500μs

    let snap = Arc::new(ArcSwap::from_pointee(Snapshot::new(NUM_KEYS, 0)));
    let barrier = Arc::new(Barrier::new(NUM_READERS + 1 + 1)); // readers + writer + main
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // --- Reader threads ---
    // Each reader loads the snapshot, picks keys, and does relaxed stores
    // of an increasing cycle counter on the AtomicU64 values.
    let mut reader_handles = Vec::new();
    for reader_id in 0..NUM_READERS {
        let snap = snap.clone();
        let barrier = barrier.clone();
        let stop = stop.clone();

        reader_handles.push(std::thread::spawn(move || {
            barrier.wait();
            let mut ops: u64 = 0;
            let mut cycle: u64 = 1;

            while !stop.load(Ordering::Relaxed) {
                // Load snapshot — this is what query threads do
                let guard = snap.load();

                // Touch a subset of keys (simulating query touching filter values)
                let start_key = (reader_id as u64 * 7 + cycle) % NUM_KEYS;
                for offset in 0..50 {
                    let key = (start_key + offset) % NUM_KEYS;
                    if let Some(stamp) = guard.stamps.get(&key) {
                        // THE KEY OPERATION: relaxed store on AtomicU64 inside
                        // a HashMap behind Arc<ArcSwap<Snapshot>>
                        stamp.store(cycle, Ordering::Relaxed);
                        ops += 1;
                    }
                }
                cycle += 1;
            }

            ops
        }));
    }

    // --- Writer thread ---
    // Periodically clones the snapshot (loading AtomicU64s), bumps generation,
    // and publishes via ArcSwap::store(). This simulates the flush thread.
    let writer_snap = snap.clone();
    let writer_barrier = barrier.clone();
    let writer_stop = stop.clone();

    let writer_handle = std::thread::spawn(move || {
        writer_barrier.wait();
        let mut publishes: u64 = 0;

        while !writer_stop.load(Ordering::Relaxed) {
            // Clone with manual AtomicU64 loading (like flush thread would)
            let current = writer_snap.load();
            let mut new_snap = current.clone_loading();

            // Bump generation (simulates applying mutations)
            new_snap.generation = current.generation + 1;

            // Publish new snapshot atomically
            writer_snap.store(Arc::new(new_snap));
            publishes += 1;

            std::thread::sleep(Duration::from_micros(WRITER_PUBLISH_INTERVAL_US));
        }

        publishes
    });

    // --- Main: start everyone, let them run, then stop ---
    barrier.wait();
    let start = Instant::now();
    std::thread::sleep(Duration::from_millis(DURATION_MS));
    stop.store(true, Ordering::Relaxed);

    let elapsed = start.elapsed();

    let mut total_reader_ops = 0u64;
    for h in reader_handles {
        let ops = h.join().expect("reader thread panicked");
        total_reader_ops += ops;
    }
    let publishes = writer_handle.join().expect("writer thread panicked");

    // --- Verify stamps are visible ---
    // After all threads stop, load the final snapshot and check that at least
    // some stamps are non-zero (readers wrote to them).
    let final_snap = snap.load();
    let mut nonzero_stamps = 0u64;
    let mut max_stamp = 0u64;
    for (_, v) in &final_snap.stamps {
        let val = v.load(Ordering::Relaxed);
        if val > 0 {
            nonzero_stamps += 1;
        }
        if val > max_stamp {
            max_stamp = val;
        }
    }

    println!("\n=== Eviction Atomics Microbench ===");
    println!("Duration:        {:.1}ms", elapsed.as_secs_f64() * 1000.0);
    println!("Reader threads:  {NUM_READERS}");
    println!("Total reader ops: {total_reader_ops}");
    println!(
        "Reader throughput: {:.1}M ops/s",
        total_reader_ops as f64 / elapsed.as_secs_f64() / 1_000_000.0
    );
    println!("Writer publishes: {publishes}");
    println!(
        "Final snapshot gen: {}",
        final_snap.generation
    );
    println!("Non-zero stamps: {nonzero_stamps}/{NUM_KEYS}");
    println!("Max stamp value: {max_stamp}");
    println!("===================================\n");

    // Assertions
    assert!(
        total_reader_ops > 0,
        "readers must have performed some ops"
    );
    assert!(
        publishes > 0,
        "writer must have published some snapshots"
    );
    assert!(
        nonzero_stamps > 0,
        "some stamps must be non-zero (readers wrote them)"
    );
    // The final snapshot was published by the writer, which cloned-and-loaded
    // the AtomicU64 values. If stamps survived the clone, they're visible.
    // The max stamp should be reasonably large (readers ran many cycles).
    assert!(
        max_stamp > 10,
        "stamps should reflect many reader cycles, got max={max_stamp}"
    );
}

/// Verify that old snapshots' AtomicU64 stores don't affect new snapshots
/// (i.e., the clone_loading copies values, doesn't share AtomicU64 cells).
#[test]
fn old_snapshot_stores_dont_leak_to_new_snapshot() {
    const NUM_KEYS: u64 = 100;

    let snap = Arc::new(ArcSwap::from_pointee(Snapshot::new(NUM_KEYS, 0)));

    // Load old snapshot
    let old_guard = snap.load();

    // Write stamp=42 on key 0 in old snapshot
    old_guard.stamps.get(&0).unwrap().store(42, Ordering::Relaxed);

    // Writer clones and publishes new snapshot
    let new_inner = old_guard.clone_loading();
    // At this point, key 0 should have value 42 (copied from old)
    assert_eq!(
        new_inner.stamps.get(&0).unwrap().load(Ordering::Relaxed),
        42,
        "clone_loading should copy the stamp value"
    );
    snap.store(Arc::new(new_inner));

    // Now write stamp=999 on key 0 in the OLD snapshot
    old_guard.stamps.get(&0).unwrap().store(999, Ordering::Relaxed);

    // Load the new snapshot — key 0 should still be 42, not 999
    let new_guard = snap.load();
    let val = new_guard.stamps.get(&0).unwrap().load(Ordering::Relaxed);
    assert_eq!(
        val, 42,
        "new snapshot must not see old snapshot's post-clone writes, got {val}"
    );
}
