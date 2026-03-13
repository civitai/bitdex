# Bitdex Concurrency & Bulk Loading Design Conversation

**Original conversation:** 2/22/2026
**Link:** [Claude chat](https://claude.ai/chat/2fe1687a-8613-484c-a136-8e86f17065ac)
**Status:** Design decisions from this conversation are fully implemented.

This transcript was condensed to Bitdex-specific design decisions only. Generic Rust/Tokio concurrency explanations were removed.

---

## 1. ArcSwap Double-Buffer for Lock-Free Reads

**Problem:** Cloning large roaring bitmaps every flush cycle is expensive. Readers need lock-free access while writers apply diffs.

**Solution:** ArcSwap + double-buffer with `try_unwrap` reclaim:

1. Apply diff (additions + removals) to staging bitmap — O(diff), not O(main)
2. `Arc::new(staging)` and swap into live via ArcSwap — readers instantly see new version
3. `Arc::try_unwrap(old_live)` to reclaim the retired bitmap without cloning
4. Apply same diff to retired bitmap to bring it current — it becomes new staging
5. Clear diffs

Key insight: Two diff applications is O(2 x diff), whereas cloning is O(entire bitmap). The clone fallback only happens if a reader is actively holding a guard at swap time, which is rare since `ArcSwap::load()` guards are short-lived.

**Alternative considered and rejected:** RwLock on a single bitmap. Simpler and half the memory, but readers block during write lock (even if briefly). Chosen for loading mode instead (see below).

**Alternative considered and rejected:** COW overlay where readers check main + diff inline. Still requires locks on the diff bitmaps, moving contention from flush time to read time (worse, since reads are the hot path).

---

## 2. Loading Mode: Direct Writes During Bulk Load

**Problem:** During bulk loads, the diff → flush → swap cycle doubles write work and the ArcSwap clone cascade kills throughput.

**Solution:** Adaptive mode switching:

- **Normal mode (steady state):** Write to diff bitmaps, periodic flush merges into main via ArcSwap swap. Contention-free reads.
- **Loading mode (bulk load):** Skip diffs entirely, write directly to main bitmap via RwLock. Readers briefly contend but write throughput is maximized.

Explicit `enter_bulk_mode()` / `exit_bulk_mode()` rather than heuristic-based switching — the system knows when it's bulk loading.

During loading mode, apply workers write batches directly to the target bitmap. One write lock per batch (amortized over many bits). With hundreds of bitmaps, any individual bitmap's write lock is held briefly, so reader contention is minimal.

---

## 3. Dual-Endpoint Design: put vs put_bulk

**Problem:** During bulk load, long-tail bitmaps with few pending changes starve because the priority queue always favors the biggest batch. Standard traffic needs timely application guarantees.

**Solution:** Two separate ingestion endpoints with different guarantees:

- **Standard put:** "Visible to readers within X seconds" — gets priority in the apply queue
- **Bulk put:** "Get this in eventually, maximize throughput" — biggest-batch-first scheduling

Both channels share the same apply worker pool. Standard channel always wins if anything is waiting. The channel itself carries the intent — no global mode flag needed.

If a standard put arrives for a bitmap that already has bulk changes queued, combine them and promote to standard priority (avoids applying to the same bitmap twice).

---

## 4. Sort-Based Bulk Loading (Eliminate Per-Bitmap Overhead)

**Problem:** With 600K distinct bitmaps across 1M records (high cardinality), the average bitmap has < 2 bits. Bottleneck is infrastructure overhead per bitmap (HashMap lookups, memory allocation, cache misses), not bitmap operations. The priority queue breaks down because there are no big batches.

**Solution:** Replace the two-pool decompose/apply architecture with a sort-based pipeline during bulk load:

1. **Parse (parallel):** Stream JSON, emit flat `(bitmap_id, row_id)` tuples into a Vec
2. **Sort:** `par_sort_unstable` on the tuples — cache-friendly on contiguous memory
3. **Build (parallel):** Group by bitmap_id, `RoaringBitmap::from_sorted_iter` per group
4. **Merge:** Single `|=` per bitmap into main store

`from_sorted_iter` is dramatically faster than individual inserts because roaring can build optimal containers in one pass (knows density upfront, picks array vs bitmap vs run containers perfectly).

**Chunked pipeline** for 104M+ records: process 1M records at a time. Peak memory is one chunk of tuples (~240MB) plus the main bitmap store. Stream input with `BufReader`, no spilling to disk needed.

**Throughput estimate:** Per 1M chunk, a few seconds end to end. 104M records in ~5-10 minutes. Bottleneck shifts to disk read speed on NVMe (3-5GB/s).

---

## 5. Worker Pool Architecture

The decompose/apply pool structure remains the same across modes. Decomposition is CPU-bound (parsing, figuring out which bitmaps to touch), application is lock-bound (contending on bitmap access). Different bottlenecks → different pools, independently tunable.

The priority slot mechanism provides automatic adaptive batching: under heavy load, batches naturally grow because changes accumulate while workers are busy. Fewer lock acquisitions per bit. Under light load, batches are smaller but contention is low anyway.
