//! Tests for the "stamp gap" race condition in idle eviction.
//!
//! The race scenario:
//! 1. Flush thread evicts value V (removes from bitmaps AND from eviction_stamps)
//! 2. Flush thread publishes new snapshot (without V)
//! 3. Query arrives for V, triggers lazy reload — V added back + stamped at current_cycle
//! 4. Readers on OLD snapshot might query V and stamp it. Can the reload stamp get lost
//!    if eviction sweep runs again quickly?
//!
//! These tests simulate the eviction lifecycle using ArcSwap + DashMap to verify
//! that stamp-based idle eviction is safe against rapid evict-reload-evict cycles.

use arc_swap::ArcSwap;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

/// Simulates FilterField bitmaps — a snapshot of which values are "loaded".
struct Snapshot {
    /// value_id -> data (simulating bitmap presence)
    bitmaps: HashMap<u64, String>,
}

/// Runs an eviction sweep: removes entries from `stamps` where
/// `current_cycle - stamp > idle_threshold`. Returns evicted keys.
///
/// Two-pass approach: collect keys to evict, then remove them.
/// This avoids holding shard locks during the decision phase.
fn eviction_sweep(
    stamps: &DashMap<u64, AtomicU64>,
    current_cycle: u64,
    idle_threshold: u64,
) -> Vec<u64> {
    let cutoff = current_cycle.saturating_sub(idle_threshold);
    let mut to_evict = Vec::new();

    // Pass 1: identify idle entries
    for entry in stamps.iter() {
        let s = entry.value().load(Ordering::Relaxed);
        if s < cutoff {
            to_evict.push(*entry.key());
        }
    }

    // Pass 2: remove them
    for key in &to_evict {
        stamps.remove(key);
    }

    to_evict
}

/// Publishes a new snapshot with the given bitmaps.
fn publish_snapshot(swap: &ArcSwap<Snapshot>, bitmaps: HashMap<u64, String>) {
    swap.store(Arc::new(Snapshot { bitmaps }));
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 1: Eviction + reload + re-eviction race (deterministic)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn evict_reload_no_false_reeviction() {
    // Setup: 100 values, all stamped at cycle 0
    let stamps: DashMap<u64, AtomicU64> = DashMap::new();
    let mut initial_bitmaps = HashMap::new();
    for i in 0..100u64 {
        stamps.insert(i, AtomicU64::new(0));
        initial_bitmaps.insert(i, format!("bitmap_{i}"));
    }

    let swap = ArcSwap::from_pointee(Snapshot {
        bitmaps: initial_bitmaps,
    });

// Step 1: Eviction sweep at cycle=1000, idle_threshold=100
    // cutoff = 1000 - 100 = 900. All stamps are 0, so 0 < 900 → all evicted.
    let evicted = eviction_sweep(&stamps, 1000, 100);
    assert_eq!(evicted.len(), 100, "all 100 values should be evicted");
    assert_eq!(stamps.len(), 0, "stamps should be empty after eviction");

// Step 2: Publish empty snapshot (simulates flush thread removing bitmaps)
    publish_snapshot(&swap, HashMap::new());
    let snap = swap.load();
    assert!(snap.bitmaps.is_empty(), "snapshot should be empty after eviction");

// Step 3: Simulate lazy reload of value 42 — add it back, stamp at cycle=1000
    stamps.insert(42, AtomicU64::new(1000));
    let mut reloaded_bitmaps = HashMap::new();
    reloaded_bitmaps.insert(42, "bitmap_42_reloaded".to_string());
    publish_snapshot(&swap, reloaded_bitmaps);

    // Verify reload
    let snap = swap.load();
    assert!(snap.bitmaps.contains_key(&42), "value 42 should be reloaded");
    assert_eq!(stamps.len(), 1, "only value 42 should be stamped");

// Step 4: Run eviction sweep again at cycle=1001, idle_threshold=100
    // cutoff = 1001 - 100 = 901. Value 42 stamp = 1000. 1000 >= 901 → NOT evicted.
    let evicted_2 = eviction_sweep(&stamps, 1001, 100);
    assert!(
        evicted_2.is_empty(),
        "value 42 should NOT be re-evicted (stamp=1000, cutoff=901), but got evicted: {:?}",
        evicted_2
    );
    assert_eq!(stamps.len(), 1, "value 42 stamp should survive");
    {
        let stamp_42 = stamps.get(&42).unwrap();
        assert_eq!(stamp_42.load(Ordering::Relaxed), 1000);
    } // drop Ref before next sweep to avoid shard deadlock

    // Also verify it survives right up to the edge: cycle=1100, threshold=100 → cutoff=1000
    // stamp=1000, cutoff=1000 → 1000 < 1000 is FALSE → NOT evicted
    let evicted_edge = eviction_sweep(&stamps, 1100, 100);
    assert!(
        evicted_edge.is_empty(),
        "value 42 should NOT be evicted at exact boundary (stamp=1000, cutoff=1000)"
    );

    // One more cycle: cycle=1101, threshold=100 → cutoff=1001
    // stamp=1000, cutoff=1001 → 1000 < 1001 is TRUE → evicted
    let evicted_past = eviction_sweep(&stamps, 1101, 100);
    assert_eq!(
        evicted_past.len(),
        1,
        "value 42 SHOULD be evicted once past threshold"
    );
    assert_eq!(evicted_past[0], 42);

    println!("\n=== evict_reload_no_false_reeviction: PASSED ===\n");
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 2: Rapid evict-reload-evict stress test (concurrent)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn rapid_evict_reload_stress_no_false_evictions() {
    const NUM_VALUES: u64 = 200;
    const HOT_KEYS: u64 = 50; // readers only touch keys 0..49
    const NUM_READERS: usize = 4;
    const DURATION_MS: u64 = 1000;
    const IDLE_THRESHOLD: u64 = 100;
    const FALSE_EVICTION_WINDOW: u64 = 10; // "within 10 cycles of being stamped"

    // Shared state
    let stamps: Arc<DashMap<u64, AtomicU64>> = Arc::new(DashMap::new());
    let cycle: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
    let false_evictions: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(NUM_READERS + 1 + 1)); // readers + flush + main

    // Initialize: all values present, stamped at cycle 0
    let mut initial_bitmaps = HashMap::new();
    for i in 0..NUM_VALUES {
        stamps.insert(i, AtomicU64::new(0));
        initial_bitmaps.insert(i, format!("v{i}"));
    }
    let swap = Arc::new(ArcSwap::from_pointee(Snapshot {
        bitmaps: initial_bitmaps,
    }));

    // --- Reader threads: continuously query HOT keys and stamp them ---
    // Keys 0..HOT_KEYS are "hot" (frequently queried), keys HOT_KEYS..NUM_VALUES are "cold"
    // (never queried by readers, so they go idle and get evicted by the flush thread).
    let mut reader_handles = Vec::new();
    for reader_id in 0..NUM_READERS {
        let stamps = stamps.clone();
        let cycle = cycle.clone();
        let swap = swap.clone();
        let stop = stop.clone();
        let barrier = barrier.clone();

        reader_handles.push(std::thread::spawn(move || {
            barrier.wait();
            let mut ops = 0u64;
            let mut key_offset = reader_id as u64 * 7;

            while !stop.load(Ordering::Relaxed) {
                let snap = swap.load();
                let c = cycle.load(Ordering::Relaxed);

                // Touch 10 hot keys per iteration (only keys 0..HOT_KEYS)
                for _ in 0..10 {
                    let key = key_offset % HOT_KEYS;
                    key_offset = key_offset.wrapping_add(13);

                    // If the value exists in the snapshot, stamp it
                    if snap.bitmaps.contains_key(&key) {
                        if let Some(entry) = stamps.get(&key) {
                            entry.value().store(c, Ordering::Relaxed);
                        }
                        ops += 1;
                    }
                }
            }
            ops
        }));
    }

    // --- Flush thread: evict idle → reload recently-queried → evict again ---
    let flush_stamps = stamps.clone();
    let flush_cycle = cycle.clone();
    let flush_swap = swap.clone();
    let flush_stop = stop.clone();
    let flush_barrier = barrier.clone();
    let flush_false_evictions = false_evictions.clone();

    let flush_handle = std::thread::spawn(move || {
        flush_barrier.wait();
        let mut evict_rounds = 0u64;
        let mut total_evicted = 0u64;
        let mut total_reloaded = 0u64;

        while !flush_stop.load(Ordering::Relaxed) {
            let c = flush_cycle.fetch_add(1, Ordering::Relaxed) + 1;

            // --- Eviction sweep ---
            let cutoff = c.saturating_sub(IDLE_THRESHOLD);
            let mut evicted_keys = Vec::new();
            let mut evicted_with_stamps: Vec<(u64, u64)> = Vec::new();

            for entry in flush_stamps.iter() {
                let s = entry.value().load(Ordering::Relaxed);
                if s < cutoff {
                    evicted_keys.push(*entry.key());
                    evicted_with_stamps.push((*entry.key(), s));
                }
            }

            // Check for false evictions BEFORE removing
            for &(key, stamp) in &evicted_with_stamps {
                if c.saturating_sub(stamp) <= FALSE_EVICTION_WINDOW {
                    flush_false_evictions.fetch_add(1, Ordering::Relaxed);
                    eprintln!(
                        "FALSE EVICTION: key={key}, stamp={stamp}, cycle={c}, gap={}",
                        c - stamp
                    );
                }
            }

            // Remove evicted
            for key in &evicted_keys {
                flush_stamps.remove(key);
            }

            total_evicted += evicted_keys.len() as u64;

            // Publish snapshot without evicted values
            if !evicted_keys.is_empty() {
                let snap = flush_swap.load();
                let mut new_bitmaps = snap.bitmaps.clone();
                for k in &evicted_keys {
                    new_bitmaps.remove(k);
                }
                publish_snapshot(&flush_swap, new_bitmaps);
            }

            // --- Simulate lazy reload of recently-queried values ---
            // Reload the first few evicted keys (simulating queries triggering reload)
            let reload_count = evicted_keys.len().min(5);
            if reload_count > 0 {
                let snap = flush_swap.load();
                let mut new_bitmaps = snap.bitmaps.clone();
                for &key in &evicted_keys[..reload_count] {
                    // Reload: add back to bitmaps + stamp at current cycle
                    new_bitmaps.insert(key, format!("reloaded_{key}"));
                    flush_stamps.insert(key, AtomicU64::new(c));
                }
                publish_snapshot(&flush_swap, new_bitmaps);
                total_reloaded += reload_count as u64;
            }

            evict_rounds += 1;

            // Small yield to let readers run
            std::thread::yield_now();
        }

        (evict_rounds, total_evicted, total_reloaded)
    });

    // --- Main: start, run, stop ---
    barrier.wait();
    let start = Instant::now();
    std::thread::sleep(Duration::from_millis(DURATION_MS));
    stop.store(true, Ordering::Relaxed);

    let elapsed = start.elapsed();

    let mut total_reader_ops = 0u64;
    for h in reader_handles {
        total_reader_ops += h.join().expect("reader panicked");
    }
    let (evict_rounds, total_evicted, total_reloaded) =
        flush_handle.join().expect("flush thread panicked");

    let false_count = false_evictions.load(Ordering::Relaxed);

    println!("\n=== rapid_evict_reload_stress ===");
    println!("Duration:         {:.1}ms", elapsed.as_secs_f64() * 1000.0);
    println!("Reader threads:   {NUM_READERS}");
    println!("Total reader ops: {total_reader_ops}");
    println!("Eviction rounds:  {evict_rounds}");
    println!("Total evicted:    {total_evicted}");
    println!("Total reloaded:   {total_reloaded}");
    println!("False evictions:  {false_count}");
    println!(
        "Final cycle:      {}",
        cycle.load(Ordering::Relaxed)
    );
    println!(
        "Stamps remaining: {}",
        stamps.len()
    );
    println!("================================\n");

    assert_eq!(
        false_count, 0,
        "there must be zero false evictions (values evicted within {FALSE_EVICTION_WINDOW} cycles of being stamped)"
    );
    assert!(evict_rounds > 0, "flush thread must have run eviction rounds");
    assert!(total_reader_ops > 0, "readers must have done work");
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 3: Grace period test (deterministic edge cases)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn grace_period_freshly_loaded_values_protected() {
    let stamps: DashMap<u64, AtomicU64> = DashMap::new();

    // Scenario: idle_threshold = 50, current_cycle = 200
    let idle_threshold: u64 = 50;
    let current_cycle: u64 = 200;
    // cutoff = 200 - 50 = 150

    // Value A: stamp = 200 (just loaded at current cycle) → NOT evicted
    stamps.insert(1, AtomicU64::new(200));

    // Value B: stamp = 190 (loaded 10 cycles ago) → NOT evicted (190 >= 150)
    stamps.insert(2, AtomicU64::new(190));

    // Value C: stamp = 151 (loaded 49 cycles ago) → NOT evicted (151 >= 150)
    stamps.insert(3, AtomicU64::new(151));

    // Value D: stamp = 150 (exactly at boundary: current - threshold) → NOT evicted (150 < 150 is false)
    stamps.insert(4, AtomicU64::new(150));

    // Value E: stamp = 149 (one past boundary) → EVICTED (149 < 150)
    stamps.insert(5, AtomicU64::new(149));

    // Value F: stamp = 0 (never touched) → EVICTED (0 < 150)
    stamps.insert(6, AtomicU64::new(0));

    let evicted = eviction_sweep(&stamps, current_cycle, idle_threshold);

    // Check which were evicted
    assert!(
        stamps.contains_key(&1),
        "Value A (stamp=200, current cycle) must NOT be evicted"
    );
    assert!(
        stamps.contains_key(&2),
        "Value B (stamp=190, recent) must NOT be evicted"
    );
    assert!(
        stamps.contains_key(&3),
        "Value C (stamp=151, within threshold) must NOT be evicted"
    );
    assert!(
        stamps.contains_key(&4),
        "Value D (stamp=150, exact boundary) must NOT be evicted"
    );
    assert!(
        !stamps.contains_key(&5),
        "Value E (stamp=149, past boundary) MUST be evicted"
    );
    assert!(
        !stamps.contains_key(&6),
        "Value F (stamp=0, never touched) MUST be evicted"
    );

    assert_eq!(evicted.len(), 2, "exactly 2 values should be evicted");
    assert!(evicted.contains(&5));
    assert!(evicted.contains(&6));

    // Remaining stamps should be exactly 4
    assert_eq!(stamps.len(), 4, "4 values should survive the sweep");

    println!("\n=== grace_period_freshly_loaded_values_protected: PASSED ===");
    println!("Evicted: {:?}", evicted);
    println!(
        "Surviving stamps: {:?}",
        stamps
            .iter()
            .map(|e| (*e.key(), e.value().load(Ordering::Relaxed)))
            .collect::<Vec<_>>()
    );
    println!("===\n");
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 4: Boundary precision — stamp == cutoff is safe
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn boundary_stamp_equals_cutoff_not_evicted() {
    let stamps: DashMap<u64, AtomicU64> = DashMap::new();

    // Sweep at every cycle from 100 to 110, threshold=100
    // Value stamped at cycle 5 → cutoff ranges from 0 to 10
    stamps.insert(1, AtomicU64::new(5));

    // cutoff=0 (cycle=100, threshold=100): 5 < 0 → false → kept
    let evicted = eviction_sweep(&stamps, 100, 100);
    assert!(
        evicted.is_empty(),
        "cycle=100: stamp=5 >= cutoff=0, should survive"
    );

    // cutoff=4 (cycle=104, threshold=100): 5 < 4 → false → kept
    let evicted = eviction_sweep(&stamps, 104, 100);
    assert!(
        evicted.is_empty(),
        "cycle=104: stamp=5 >= cutoff=4, should survive"
    );

    // cutoff=5 (cycle=105, threshold=100): 5 < 5 → false → kept (BOUNDARY)
    let evicted = eviction_sweep(&stamps, 105, 100);
    assert!(
        evicted.is_empty(),
        "cycle=105: stamp=5 == cutoff=5, should NOT be evicted (boundary)"
    );

    // cutoff=6 (cycle=106, threshold=100): 5 < 6 → true → EVICTED
    let evicted = eviction_sweep(&stamps, 106, 100);
    assert_eq!(
        evicted.len(),
        1,
        "cycle=106: stamp=5 < cutoff=6, should be evicted"
    );

    println!("\n=== boundary_stamp_equals_cutoff_not_evicted: PASSED ===\n");
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 5: Stamp survival across snapshot transitions
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn stamp_survives_snapshot_transition() {
    // Stamps live in DashMap (outside the snapshot), so they must survive
    // even when the ArcSwap snapshot is replaced.
    let stamps: Arc<DashMap<u64, AtomicU64>> = Arc::new(DashMap::new());

    let swap = Arc::new(ArcSwap::from_pointee(Snapshot {
        bitmaps: {
            let mut m = HashMap::new();
            m.insert(42, "bitmap_42".to_string());
            m
        },
    }));

    // Stamp value 42 at cycle 500
    stamps.insert(42, AtomicU64::new(500));

    // Reader loads old snapshot
    let old_snap = swap.load();
    assert!(old_snap.bitmaps.contains_key(&42));

    // Flush thread publishes NEW snapshot (still has 42)
    let mut new_bitmaps = old_snap.bitmaps.clone();
    new_bitmaps.insert(42, "bitmap_42_v2".to_string());
    publish_snapshot(&swap, new_bitmaps);

    // Reader on OLD snapshot stamps 42 at cycle 600
    if let Some(entry) = stamps.get(&42) {
        entry.value().store(600, Ordering::Relaxed);
    }

    // Verify stamp is visible to flush thread
    let stamp = stamps.get(&42).unwrap().load(Ordering::Relaxed);
    assert_eq!(stamp, 600, "stamp from old-snapshot reader must be visible");

    // Eviction at cycle 650, threshold=100 → cutoff=550
    // stamp=600 >= 550 → NOT evicted
    let evicted = eviction_sweep(&stamps, 650, 100);
    assert!(
        evicted.is_empty(),
        "recently-stamped value must survive eviction"
    );

    // Publish yet another snapshot
    publish_snapshot(&swap, HashMap::new());

    // Stamp is STILL in DashMap even though snapshot is now empty
    let stamp = stamps.get(&42).unwrap().load(Ordering::Relaxed);
    assert_eq!(
        stamp, 600,
        "stamp must persist independently of snapshot content"
    );

    println!("\n=== stamp_survives_snapshot_transition: PASSED ===\n");
}
