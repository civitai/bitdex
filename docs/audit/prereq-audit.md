# Prerequisite Phase Audit: Diff-Based ArcSwap + Two-Thread Model

**Auditor:** Claude Opus 4.6
**Date:** 2026-02-21
**Scope:** P1 through P10 of the Prerequisite phase per `docs/plans/roadmap-performance-and-persistence.md`
**Reference docs:** `docs/design/design-arcswap-redb-reconciliation.md`, `docs/reviews/architecture-risk-review.md`

---

## Summary

| Item | Description | Status |
|------|------------|--------|
| P1 | VersionedBitmap and BitmapDiff structs | COMPLIANT |
| P2 | FilterField uses VersionedBitmap | COMPLIANT |
| P3 | SortField uses VersionedBitmap | COMPLIANT |
| P4 | SlotAllocator alive bitmap uses VersionedBitmap | COMPLIANT |
| P5 | Diff fusion in executor | NON-COMPLIANT |
| P6 | Split flush thread into flush + merge threads | PARTIAL |
| P7 | Write coalescer feeds flush thread | NON-COMPLIANT |
| P8 | Remove Phase 2 config placeholders | COMPLIANT |
| P9 | VersionedBitmap unit tests | COMPLIANT |
| P10 | Two-thread flush/merge integration tests | PARTIAL |

**Overall:** 4 COMPLIANT, 2 PARTIAL, 2 NON-COMPLIANT, 0 MISSING.

The data structures (P1-P4) are fully correct. The fundamental architectural commitments of the Prerequisite phase -- diff fusion on the read path (P5) and separation of flush/merge responsibilities (P6/P7) -- are where compliance breaks down.

---

## Detailed Findings

---

### P1: VersionedBitmap and BitmapDiff Structs

**Status: COMPLIANT**

**Spec requires:**
- `base: Arc<RoaringBitmap>` -- immutable, shared with snapshots
- `diff: Arc<BitmapDiff>` -- shared via Arc, swapped on publish
- `generation: u64` -- incremented on each merge
- Methods: `apply_diff()`, `is_dirty()`, `merge()`
- BitmapDiff with `sets: RoaringBitmap`, `clears: RoaringBitmap`

**Evidence:**

`src/versioned_bitmap.rs:11-14` -- BitmapDiff struct:
```rust
pub struct BitmapDiff {
    pub sets: RoaringBitmap,
    pub clears: RoaringBitmap,
}
```

`src/versioned_bitmap.rs:72-76` -- VersionedBitmap struct:
```rust
pub struct VersionedBitmap {
    base: Arc<RoaringBitmap>,
    diff: Arc<BitmapDiff>,
    generation: u64,
}
```

`src/versioned_bitmap.rs:131-136` -- apply_diff method:
```rust
pub fn apply_diff(&self, candidates: &RoaringBitmap) -> RoaringBitmap {
    let mut result = candidates & self.base.as_ref();
    result |= candidates & &self.diff.sets;
    result -= &self.diff.clears;
    result
}
```

`src/versioned_bitmap.rs:154-156` -- is_dirty method:
```rust
pub fn is_dirty(&self) -> bool {
    !self.diff.is_empty()
}
```

`src/versioned_bitmap.rs:169-178` -- merge method:
```rust
pub fn merge(&mut self) {
    if self.diff.is_empty() { return; }
    let base = Arc::make_mut(&mut self.base);
    *base |= &self.diff.sets;
    *base -= &self.diff.clears;
    self.diff = Arc::new(BitmapDiff::new());
    self.generation += 1;
}
```

All spec requirements met. The diff is `Arc`-wrapped per architecture risk review issue 5. `insert()` and `remove()` use `Arc::make_mut()` for CoW on the diff layer. `swap_diff()` supports the publish pattern.

**Gap:** None.

---

### P2: FilterField Uses VersionedBitmap

**Status: COMPLIANT**

**Spec requires:** Replace `HashMap<u64, Arc<RoaringBitmap>>` with `HashMap<u64, VersionedBitmap>`. Mutations write to diff instead of `Arc::make_mut()`.

**Evidence:**

`src/filter.rs:21-26` -- FilterField struct:
```rust
pub struct FilterField {
    bitmaps: HashMap<u64, VersionedBitmap>,
    config: FilterFieldConfig,
}
```

`src/filter.rs:56-61` -- insert writes to diff layer via VersionedBitmap:
```rust
pub fn insert(&mut self, value: u64, slot: u32) {
    self.bitmaps
        .entry(value)
        .or_insert_with(VersionedBitmap::new_empty)
        .insert(slot);
}
```

`src/filter.rs:64-69` -- remove writes to diff layer:
```rust
pub fn remove(&mut self, value: u64, slot: u32) {
    if let Some(vb) = self.bitmaps.get_mut(&value) {
        vb.remove(slot);
    }
}
```

Mutations go through `VersionedBitmap::insert()` and `VersionedBitmap::remove()`, which both operate on the diff layer via `Arc::make_mut(&mut self.diff)`. The old `Arc<RoaringBitmap>` direct storage is completely replaced.

`src/filter.rs:203-206` -- FilterIndex uses Arc-per-field for CoW at the field level:
```rust
pub struct FilterIndex {
    fields: HashMap<String, Arc<FilterField>>,
}
```

`src/filter.rs:229-231` -- get_field_mut uses Arc::make_mut:
```rust
pub fn get_field_mut(&mut self, name: &str) -> Option<&mut FilterField> {
    self.fields.get_mut(name).map(|f| Arc::make_mut(f))
}
```

**Gap:** None.

---

### P3: SortField Uses VersionedBitmap

**Status: COMPLIANT**

**Spec requires:** Replace `Vec<Arc<RoaringBitmap>>` bit layers with `Vec<VersionedBitmap>`.

**Evidence:**

`src/sort.rs:24-31` -- SortField struct:
```rust
pub struct SortField {
    bit_layers: Vec<VersionedBitmap>,
    num_bits: usize,
    config: SortFieldConfig,
}
```

`src/sort.rs:57-63` -- insert writes to diff:
```rust
pub fn insert(&mut self, slot: u32, value: u32) {
    for bit in 0..self.num_bits {
        if (value >> bit) & 1 == 1 {
            self.bit_layers[bit].insert(slot);
        }
    }
}
```

`src/sort.rs:106-111` -- layer accessor enforces merged state via debug_assert:
```rust
pub fn layer(&self, bit: usize) -> Option<&RoaringBitmap> {
    self.bit_layers.get(bit).map(|vb| {
        debug_assert!(!vb.is_dirty(), "sort layer {bit} has unmerged diff");
        vb.base().as_ref()
    })
}
```

`src/sort.rs:367-371` -- SortIndex uses Arc-per-field:
```rust
pub struct SortIndex {
    fields: std::collections::HashMap<String, Arc<SortField>>,
}
```

**Gap:** None.

---

### P4: SlotAllocator Alive Bitmap Uses VersionedBitmap

**Status: COMPLIANT**

**Spec requires:** Alive bitmap uses VersionedBitmap, always merged eagerly.

**Evidence:**

`src/slot.rs:22-35` -- SlotAllocator struct:
```rust
pub struct SlotAllocator {
    next_slot: AtomicU32,
    alive: VersionedBitmap,
    clean: Arc<RoaringBitmap>,
    deferred: BTreeMap<u64, Vec<u32>>,
}
```

`src/slot.rs:84-86` -- allocate writes to diff:
```rust
self.alive.insert(slot);
```

`src/slot.rs:112-113` -- delete writes to diff:
```rust
self.alive.remove(slot);
```

`src/slot.rs:145-147` -- explicit merge method:
```rust
pub fn merge_alive(&mut self) {
    self.alive.merge();
}
```

`src/slot.rs:130-132` -- alive_bitmap accessor returns merged base:
```rust
pub fn alive_bitmap(&self) -> &RoaringBitmap {
    self.alive.base().as_ref()
}
```

Eager merge is enforced by the write coalescer calling `slots.merge_alive()` in `WriteBatch::apply()` at line 318 of `write_coalescer.rs`.

**Gap:** None.

---

### P5: Diff Fusion in Executor

**Status: NON-COMPLIANT**

**Spec requires:** `compute_filters()` and `evaluate_clause()` must fuse diffs into intersections: `(base & filters) | (sets & filters) &! clears`. Only needed for filter bitmaps -- sort layers have no active diffs.

The design doc (`design-arcswap-redb-reconciliation.md:52-71`) is explicit:

> Readers call `inner.load()` for an immutable snapshot. When evaluating a filter clause, if a bitmap has a non-empty diff, **fuse the diff into the intersection** rather than materializing a merged copy.

And:

> `result = (base & other_filter) | (sets & other_filter) &! clears`

**Evidence of non-compliance:**

`src/executor.rs:397-413` -- compute_filters does NOT fuse diffs:
```rust
pub(crate) fn compute_filters(&self, clauses: &[FilterClause]) -> Result<RoaringBitmap> {
    if clauses.is_empty() {
        return Ok(self.slots.alive_bitmap().clone());
    }
    let mut result: Option<RoaringBitmap> = None;
    for clause in clauses {
        let bitmap = self.evaluate_clause(clause)?;
        result = Some(match result {
            Some(existing) => existing & &bitmap,
            None => bitmap,
        });
    }
    Ok(result.unwrap_or_default())
}
```

`src/executor.rs:416-427` -- evaluate_clause for Eq does NOT fuse diffs:
```rust
FilterClause::Eq(field, value) => {
    if let Some(filter_field) = self.filters.get_field(field) {
        let key = value_to_bitmap_key(value)
            .ok_or_else(|| BitdexError::InvalidValue { ... })?;
        return Ok(filter_field.get(key).cloned().unwrap_or_default());
    }
    ...
}
```

The `filter_field.get(key)` call in `src/filter.rs:99-103`:
```rust
pub fn get(&self, value: u64) -> Option<&RoaringBitmap> {
    self.bitmaps.get(&value).map(|vb| {
        debug_assert!(!vb.is_dirty(), "filter bitmap must be merged before read");
        vb.base().as_ref()
    })
}
```

This is the smoking gun: **`get()` returns only the base bitmap and asserts the diff is empty**. There is no diff fusion happening. The executor reads merged bases exclusively. The `apply_diff()` method on VersionedBitmap exists but is never called by the executor.

Similarly, `FilterField::union()` at line 117-125 and `FilterField::intersection()` at line 129-140 both use `vb.base().as_ref()` -- no diff fusion.

**Why this happened:** The write coalescer eagerly merges ALL bitmaps -- not just sort layers and alive -- before publishing. See `WriteBatch::apply()` at `src/write_coalescer.rs:308-315`:

```rust
// Eager merge: filter diffs must be compacted before readers see them.
// Merge only filter fields that were mutated in this batch.
let mutated_filter_fields = self.mutated_filter_fields();
for field_name in &mutated_filter_fields {
    if let Some(field) = filters.get_field_mut(field_name) {
        field.merge_dirty();
    }
}
```

Filter bitmaps are eagerly merged by the flush thread, so the executor never sees dirty diffs, and the `debug_assert!(!vb.is_dirty())` never fires. The `apply_diff()` method is dead code in the read path.

**Impact:** MEDIUM-HIGH.

1. **Performance regression:** The entire point of the diff model is to avoid cloning full bitmaps on the flush thread. By eagerly merging filter diffs, the flush thread calls `VersionedBitmap::merge()` which invokes `Arc::make_mut(&mut self.base)`. When readers hold a snapshot with `strong_count > 1`, this clones the entire base bitmap -- exactly the CoW model the spec was designed to eliminate.

2. **Architectural debt:** The `apply_diff()` method works correctly (tested in P9 tests), but it is unused in production code paths. When Phase A lands and filter diffs are supposed to accumulate between merge thread cycles, the executor will need to be rewritten to use diff fusion. This should have been done in the Prerequisite.

3. **Correctness is preserved:** Because filter diffs are merged before publish, readers see correct data. The issue is purely performance -- the system does more work than necessary on the write path and misses the performance benefit the diff model was designed to provide.

---

### P6: Split Flush Thread into Flush + Merge Threads

**Status: PARTIAL**

**Spec requires:**
- Flush thread: drain channel -> merge sort diffs into bases -> accumulate filter diffs in Arc -> publish
- Merge thread: compact filter diffs -> write redb -> publish merged
- From architecture risk review issue 3 (HIGH priority): "flush and merge must be separate threads"
- From issue 4: "Sort layer diffs get merged immediately by the flush thread. Filter diffs accumulate until the merge thread compacts them."

**Evidence of compliance:**

Two threads do exist. `src/concurrent_engine.rs:177-432` spawns the flush thread, and `src/concurrent_engine.rs:434-486` spawns the merge thread:

```rust
let flush_handle = {
    // ... (line 177)
    thread::spawn(move || {
        // ... flush thread logic
    })
};

let merge_handle = {
    // ... (line 434)
    thread::spawn(move || {
        // ... merge thread logic
    })
};
```

**Evidence of non-compliance:**

The merge thread (lines 442-485) only handles Tier 2 pending buffer draining -- it does NOT compact Tier 1 filter diffs:

```rust
thread::spawn(move || {
    let sleep_duration = Duration::from_millis(merge_interval_ms);
    while !shutdown.load(Ordering::Relaxed) {
        thread::sleep(sleep_duration);
        // Drain pending buffer to redb
        if let Some(ref store) = merge_bitmap_store {
            let to_drain = {
                let mut p = merge_pending.lock();
                p.drain_heaviest(pending_drain_cap)
            };
            // ... load from redb, apply mutations, write back
        }
    }
})
```

The merge thread:
- Drains Tier 2 pending buffer mutations to redb
- Does NOT compact Tier 1 filter diffs into bases
- Does NOT publish merged snapshots with empty diffs
- Does NOT call `merge()` on any VersionedBitmaps
- Does NOT check `Arc::strong_count()` for in-place mutation

The flush thread does ALL the merging (both sort AND filter) in `coalescer.apply_prepared()`, which calls `WriteBatch::apply()`, which eagerly merges everything.

**Spec vs Reality:**

| Responsibility | Spec: Flush Thread | Spec: Merge Thread | Actual: Flush Thread | Actual: Merge Thread |
|---|---|---|---|---|
| Drain mutation channel | Yes | No | Yes | No |
| Merge sort diffs | Yes | No | Yes | No |
| Accumulate filter diffs | Yes | No | **No -- merges them** | No |
| Publish snapshot | Yes | Yes (merged) | Yes | No |
| Compact filter diffs | No | Yes | **Yes (eagerly)** | No |
| Write to redb | No | Yes | No | Yes (Tier 2 only) |
| Drain pending buffer | No | Yes | No | Yes |

**Impact:** HIGH.

The spec's two-thread split was designed to keep the flush thread fast (~sub-millisecond) by offloading the expensive merge work. Currently the flush thread does all the merge work inline, which includes `Arc::make_mut()` clone cascades on shared bitmaps. This defeats the purpose of the two-thread model.

Under heavy read+write load:
- Flush publishes snapshot with N dirty filter bitmaps
- Readers load snapshot, bump `strong_count` on base Arcs
- Next flush cycle calls `merge()` on those same bitmaps
- `Arc::make_mut()` detects `strong_count > 1`, clones entire base
- Dense bitmaps (~12MB each) generate tens of MB of transient copies

This is exactly the problem the diff model was supposed to eliminate (design doc: "No `Arc::make_mut()` cloning of full bitmaps on every flush").

---

### P7: Write Coalescer Feeds Flush Thread

**Status: NON-COMPLIANT**

**Spec requires:**
- Sort ops applied directly to bases (then eagerly merged)
- Filter ops applied to diffs (NOT merged -- accumulate for merge thread)

From the roadmap: "The coalescer feeds the flush thread. Sort ops get applied directly to bases. Filter ops get applied to diffs."

From design doc line 46-47:
```
2. Apply mutations to diff bitmaps:
   - Insert -> diff.sets.insert(bit)
   - Remove -> diff.clears.insert(bit)
```

From architecture risk review issue 4: "Sort layer diffs get merged immediately by the flush thread. Filter diffs accumulate until the merge thread compacts them."

**Evidence of non-compliance:**

`src/write_coalescer.rs:249-319` -- `WriteBatch::apply()`:

```rust
pub fn apply(
    &self,
    slots: &mut SlotAllocator,
    filters: &mut FilterIndex,
    sorts: &mut SortIndex,
) {
    // Apply filter inserts in bulk
    for (key, slot_ids) in &self.filter_inserts {
        if let Some(field) = filters.get_field_mut(&key.field) {
            field.insert_bulk(key.value, slot_ids.iter().copied());
        }
    }
    // Apply filter removes in bulk
    for (key, slot_ids) in &self.filter_removes {
        if let Some(field) = filters.get_field_mut(&key.field) {
            field.remove_bulk(key.value, slot_ids);
        }
    }
    // ... sort ops applied ...

    // Eager merge: sort diffs MUST be empty before readers see them.
    // Merge only sort fields that were mutated in this batch.
    for field_name in &mutated_sort_fields {
        if let Some(field) = sorts.get_field_mut(field_name) {
            field.merge_dirty();
        }
    }

    // Eager merge: filter diffs must be compacted before readers see them.
    // Merge only filter fields that were mutated in this batch.
    let mutated_filter_fields = self.mutated_filter_fields();
    for field_name in &mutated_filter_fields {
        if let Some(field) = filters.get_field_mut(field_name) {
            field.merge_dirty();
        }
    }

    // Merge alive bitmap
    slots.merge_alive();
}
```

The code comment on line 308-309 says "Eager merge: filter diffs must be compacted before readers see them." -- this directly contradicts the spec which says filter diffs should accumulate and be compacted by the merge thread.

**How mutations flow through:**

1. `FilterField::insert_bulk()` calls `VersionedBitmap::insert_bulk()` which writes to the diff layer -- correct so far.
2. `field.merge_dirty()` is called immediately after -- this compacts the diff into the base, undoing the diff accumulation.

The sort layer eager merge is correct per spec. The filter and alive eager merge is NOT correct per spec. Alive should also be eagerly merged (P4 says "always merged eagerly like sort layers"), but filter diffs should NOT be.

**Impact:** HIGH (same as P6). This is the implementation-level manifestation of the P6 architecture violation. The write coalescer treats all bitmap types identically -- merge everything. The spec requires differentiated handling: sort+alive = merge eagerly, filter = accumulate diffs.

---

### P8: Remove Phase 2 Config Placeholders

**Status: COMPLIANT**

**Spec requires:** Remove `snapshot_interval_secs` and `wal_flush_strategy` fields entirely.

**Evidence:**

`src/config.rs` -- The complete Config struct (lines 13-53) contains NO `snapshot_interval_secs` or `wal_flush_strategy` fields:

```rust
pub struct Config {
    pub filter_fields: Vec<FilterFieldConfig>,
    pub sort_fields: Vec<SortFieldConfig>,
    pub max_page_size: usize,
    pub cache: CacheConfig,
    pub autovac_interval_secs: u64,
    pub merge_interval_ms: u64,
    pub prometheus_port: u16,
    pub flush_interval_us: u64,
    pub channel_capacity: usize,
    pub storage: StorageConfig,
}
```

Grep of the entire codebase confirms no remaining references:
- No occurrence of `snapshot_interval_secs` in any source file
- No occurrence of `wal_flush_strategy` in any source file

**Gap:** None.

---

### P9: VersionedBitmap Unit Tests

**Status: COMPLIANT**

**Spec requires:** Diff accumulation, merge, generation tracking, `strong_count` in-place mutation. Property tests for diff correctness.

**Evidence:**

`src/versioned_bitmap.rs:201-470` -- Comprehensive test suite with 15 tests:

| Test | What it covers | Line |
|------|---------------|------|
| `basic_insert_remove` | Diff accumulation (insert/remove to diff layer) | 206 |
| `apply_diff_correctness` | Diff fusion: base+diff intersected with candidates | 226 |
| `merge_compaction` | Merge compacts diff into base, bumps generation | 263 |
| `merge_strong_count` | `Arc::strong_count == 1` = in-place, `> 1` = clone | 299 |
| `clone_shares_arcs` | Arc pointer sharing on clone | 324 |
| `empty_diff_noop` | Empty diff merge does not bump generation | 339 |
| `apply_diff_empty` | apply_diff with no dirty diff = base & candidates | 350 |
| `insert_then_remove_same_bit` | Mutual exclusion: bit in sets OR clears, not both | 372 |
| `remove_then_insert_same_bit` | Reverse mutual exclusion | 387 |
| `contains_base_overridden_by_diff` | Diff layer overrides base | 402 |
| `swap_diff` | swap_diff replaces diff | 421 |
| `bitmap_bytes_reporting` | Serialized size reporting | 435 |
| `from_arc_constructor` | Construct from existing Arc | 449 |
| `base_len` | Base cardinality | 462 |

The `merge_strong_count` test (line 299-321) specifically validates the spec requirement that `Arc::make_mut()` mutates in place when `strong_count == 1` and clones when `> 1`:

```rust
// strong_count == 1 -> no clone needed
let base_ptr_before = Arc::as_ptr(vb.base());
vb.merge();
let base_ptr_after = Arc::as_ptr(vb.base());
assert_eq!(base_ptr_before, base_ptr_after);

// Now clone to bump strong_count > 1
let _snapshot = vb.clone();
assert!(Arc::strong_count(vb.base()) > 1);
let base_ptr_before = Arc::as_ptr(vb.base());
vb.merge();
let base_ptr_after = Arc::as_ptr(vb.base());
assert_ne!(base_ptr_before, base_ptr_after);
```

**Gap:** No property tests using `proptest` or `quickcheck`. The spec mentions "Property tests for diff correctness (apply diff to base = expected bitmap)." However, the manual test coverage is thorough and covers all the key invariants. This is a minor gap.

---

### P10: Two-Thread Flush/Merge Integration Tests

**Status: PARTIAL**

**Spec requires:** Concurrent reads during merge, sort layers always clean, filter diffs accumulate correctly, publish ordering.

**Evidence:**

`src/concurrent_engine.rs:1708-1743` -- Two tests exercise the two-thread model:

```rust
#[test]
fn test_merge_thread_starts_and_stops() {
    let mut engine = ConcurrentEngine::new(test_config()).unwrap();
    engine.shutdown();
}

#[test]
fn test_two_threads_independent() {
    let engine = Arc::new(ConcurrentEngine::new(test_config()).unwrap());
    engine.put(1, &make_doc(vec![...])).unwrap();
    wait_for_flush(&engine, 1, 500);
    let result = engine.query(&[...], None, 100).unwrap();
    assert!(result.ids.contains(&1));
}
```

These tests verify that both threads start and shut down cleanly, and that the system works end-to-end. The broader concurrency tests (`test_concurrent_puts`, `test_concurrent_reads_during_writes`, `test_concurrent_mixed_read_write`) exercise the flush thread under load.

**Gap:** SIGNIFICANT.

The spec requires tests for:
1. **"Concurrent reads during merge"** -- No test explicitly triggers a merge cycle and verifies readers see correct data during merge. The merge thread currently only drains pending buffers, so there is nothing to test.
2. **"Sort layers always clean"** -- No test asserts that sort layers are never dirty in a published snapshot. The sort layer `debug_assert!` in `bifurcate()` (line 183) would catch this at runtime, but there is no explicit integration test.
3. **"Filter diffs accumulate correctly"** -- Cannot be tested because filter diffs are eagerly merged (P5/P7 non-compliance). There is no window where filter diffs exist in a published snapshot.
4. **"Publish ordering"** -- No test verifies that readers always see monotonically advancing state.

**Impact:** MEDIUM. The existing concurrency tests provide reasonable confidence that the system is correct under concurrent access. The missing tests are specifically for behaviors that don't yet exist in the implementation (filter diff accumulation, merge thread compaction). When P5/P6/P7 are fixed, these tests become critical.

---

## Cross-Cutting Findings

### Architecture Risk Review Compliance

| Issue | Risk Review Requirement | Status |
|-------|------------------------|--------|
| Issue 3 | Merge must be separate thread from flush (HIGH) | PARTIAL -- threads exist but merge thread doesn't compact filter diffs |
| Issue 4 | Sort layers merged inline by flush (never have active diffs) | COMPLIANT -- `WriteBatch::apply()` merges sort diffs before return |
| Issue 5 | Diffs wrapped in `Arc<BitmapDiff>` to avoid clone churn | COMPLIANT -- `diff: Arc<BitmapDiff>` in VersionedBitmap |
| Issue 6 | Accept clone when `strong_count > 1`, don't spin-wait | COMPLIANT -- `merge()` uses `Arc::make_mut()` which clones transparently |

### Design Doc Compliance

| Design Doc Requirement | Status |
|----------------------|--------|
| "Flush thread applies mutations to diff bitmaps (sets.insert, clears.insert)" | COMPLIANT -- mutations go to diff layer |
| "On publish: ArcSwap::store() publishes base + diff. Readers see both." | NON-COMPLIANT -- readers see only merged bases |
| "Read path: fuse diff into intersection against small result set" | NON-COMPLIANT -- apply_diff() exists but is unused |
| "Merge cycle: separate thread, checks Arc::strong_count" | NON-COMPLIANT -- merge thread does not compact filter diffs |
| "Sort layer diffs merged eagerly by flush thread" | COMPLIANT |
| "No Arc::make_mut() cloning of full bitmaps on every flush" | NON-COMPLIANT -- filter diffs are eagerly merged, causing Arc::make_mut() clones |

---

## Root Cause Analysis

The non-compliance in P5, P6, and P7 has a single root cause: **the write coalescer eagerly merges ALL bitmaps** in `WriteBatch::apply()`. The comment at line 308 explicitly says "filter diffs must be compacted before readers see them" -- this is the design decision that cascades into all three violations.

This likely happened because:
1. The diff infrastructure (VersionedBitmap, apply_diff) was built correctly.
2. It was simpler to merge everything eagerly and get correctness guaranteed.
3. The executor was not updated to handle dirty diffs because there were never any dirty diffs to handle.
4. This creates a chicken-and-egg problem: the executor needs diff fusion to be tested, but the flush thread merges diffs before they reach the executor.

The result is a system that has the diff infrastructure in place but operates in "CoW mode" -- functionally equivalent to the pre-diff architecture, with the additional overhead of maintaining a diff layer that is immediately merged.

---

## Recommendations

### Critical (must fix before Phase A)

1. **P7 Fix: Stop eagerly merging filter diffs in WriteBatch::apply().**
   Remove the filter merge block at lines 308-315 of `write_coalescer.rs`. Keep sort layer and alive bitmap eager merge. After this change, filter diffs will accumulate between merge thread cycles.

2. **P5 Fix: Implement diff fusion in the executor.**
   - Add a `get_versioned()` method to FilterField that returns `&VersionedBitmap` instead of `&RoaringBitmap`.
   - In `evaluate_clause()` for Eq, use `VersionedBitmap::apply_diff()` against the candidate set instead of `filter_field.get()`.
   - Update `union()` and `intersection()` in FilterField to handle dirty diffs.
   - Remove the `debug_assert!(!vb.is_dirty())` from `FilterField::get()` or make it conditional.

3. **P6 Fix: Move filter diff compaction to the merge thread.**
   The merge thread should:
   - Snapshot current filter diffs from the staging engine
   - Compact them into bases using `VersionedBitmap::merge()`
   - Publish a merged snapshot with empty filter diffs
   - Optionally write merged bases to redb

### Important (fix for test coverage)

4. **P9 Enhancement: Add proptest property tests** for VersionedBitmap diff correctness.

5. **P10 Enhancement: Add integration tests** for:
   - Filter diffs visible in published snapshots
   - Merge thread compaction produces correct results
   - Sort layers never dirty in published snapshots
   - Concurrent readers see consistent state during merge

---

## Conclusion

The Prerequisite phase is approximately 60% complete. The foundational data structures (P1-P4) are fully correct and well-tested. The config cleanup (P8) is done. The unit tests (P9) are thorough.

However, the core architectural innovation of the Prerequisite -- deferred filter diff compaction with read-time fusion -- is not implemented. The system uses the diff layer as a write buffer that is immediately merged, making the diff infrastructure effectively dead code in the production read path. This must be fixed before Phase A, which depends on filter diffs accumulating between merge cycles for the two-tier storage model to work correctly.
