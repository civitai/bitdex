# Memory & Resource Management Audit

**Auditor:** Agent C
**Date:** 2026-03-13
**Scope:** Memory lifecycle, Arc/clone patterns, unbounded growth, resource leaks in a long-running 105M-record process (~14.5 GB RSS)

---

## Files Examined

- `src/concurrent_engine.rs` -- ArcSwap snapshots, flush thread, merge thread, lazy loading, eviction sweep, existence set updates
- `src/write_coalescer.rs` -- WriteBatch accumulation, grouping, apply
- `src/unified_cache.rs` -- Cache entries, LRU eviction, live maintenance, byte tracking
- `src/meta_index.rs` -- MetaIndex bitmaps, registration/deregistration
- `src/versioned_bitmap.rs` -- Arc-per-bitmap CoW, diff layers, merge
- `src/filter.rs` -- FilterField/FilterIndex, Arc-per-field CoW, HashMap<u64, VersionedBitmap>
- `src/sort.rs` -- SortField bit layers
- `src/slot.rs` -- SlotAllocator, deferred alive map
- `src/docstore.rs` -- Sharded doc store, shard read/write
- `src/bitmap_fs.rs` -- Bitmap persistence, pack files
- `src/radix_sort.rs` -- RadixSortIndex 256-bucket structure
- `src/concurrency.rs` -- InFlightTracker (DashSet)
- `src/config.rs` -- Configuration defaults
- `src/dictionary.rs` -- FieldDictionary (DashMap)
- `src/time_buckets.rs` -- TimeBucketManager
- `src/meta_index.rs` -- MetaIndex free list and registration maps

---

## Findings

### F1: Existence Set Full-Clone on Every New Distinct Value (HIGH)

**File:** `src/concurrent_engine.rs:462-470`

```rust
let mut updated = (**current).clone();  // clones entire HashSet<u64>
updated.insert(fgk.value);
ek.store(Arc::new(updated));
```

On every flush cycle, for each new distinct value in any lazy-value field, the entire `HashSet<u64>` is cloned. For tagIds with ~31K distinct values, this clones a ~31K-entry HashSet per new value. During initial loading or bulk ingestion with many new tag values, this could clone the set thousands of times in quick succession.

**Severity:** High
**Impact:** During bulk upserts that introduce new distinct values, each new value triggers a full 31K-entry HashSet clone + Arc allocation. With 100 new tags in one batch, that is 100 full clones of a ~500 KB HashSet. The old Arc'd HashSets pile up until all reader Guards release them.
**Suggested fix:** Batch all new values from a single flush cycle into one clone+insert+store operation instead of clone-per-value. Accumulate new keys into a local Vec, then do one clone, insert all, store once.

---

### F2: Unbounded Lazy Load Channel (HIGH)

**File:** `src/concurrent_engine.rs:295-296`

```rust
let (lazy_tx, lazy_rx): (Sender<LazyLoad>, Receiver<LazyLoad>) =
    crossbeam_channel::unbounded();
```

The lazy load channel is unbounded. A `LazyLoad::FilterField` message for tagIds carries a `HashMap<u64, RoaringBitmap>` with up to 31K entries, each bitmap potentially millions of bits. If multiple query threads trigger lazy loads simultaneously (e.g., at startup when all fields are pending), the unbounded channel could buffer multiple multi-GB payloads before the flush thread drains them.

**Severity:** High
**Impact:** Under load at startup, multiple concurrent queries could each trigger a lazy load for tagIds-scale fields, buffering several GB of bitmap data in the channel before the flush thread processes them. This is a transient spike but could push RSS well above steady-state.
**Suggested fix:** Use a bounded channel (capacity 4-8) with backpressure, or use a mutex-guarded staging area that deduplicates loads. Alternatively, serialize lazy loads with a per-field load lock so only one thread loads a given field at a time (the second thread waits for the first's result).

---

### F3: Merge Thread Full Snapshot Clone for Persistence (MEDIUM)

**File:** `src/concurrent_engine.rs:904-905`

```rust
let snap = merge_inner.load_full();
let mut compacted = (*snap).clone();
```

Every time the merge thread persists (triggered by dirty flag, default merge interval), it clones the entire `InnerEngine` snapshot. While Arc-per-bitmap makes this cheap in theory (refcount bumps only), `fields_mut()` at line 921 then calls `Arc::make_mut()` on each FilterField, which deep-clones any field with refcount > 1 (which is all of them, since the flush thread also holds staging). This means the merge thread clones every non-lazy FilterField's HashMap on every persist cycle.

At 105M records with multiple filter fields, the non-tagIds fields (nsfwLevel ~7 values, boolean fields) are small, but the clone of the `HashMap<String, Arc<FilterField>>` itself and each FilterField's `HashMap<u64, VersionedBitmap>` is still wasteful.

**Severity:** Medium
**Impact:** Each persist cycle deep-clones all non-lazy filter fields' inner HashMaps. For low-cardinality fields this is small (7 entries for nsfwLevel). But `merge_dirty()` on line 925 triggers `Arc::make_mut` on each VersionedBitmap's base, which clones any bitmap still shared with readers. At 105M, a single bitmap like nsfwLevel=1 could be ~50 MB serialized. The cloned data is dropped after persist completes, but during the window both copies exist in memory.
**Suggested fix:** Read bitmap data directly from the Arc without cloning -- serialize directly from the snapshot's `base()` references without calling `fields_mut()`. Use `fused_cow()` for serialization, which borrows when clean (no diff).

---

### F4: WriteBatch HashMap Capacity Retention (LOW)

**File:** `src/write_coalescer.rs:118-124`

```rust
self.filter_inserts.clear();
self.filter_removes.clear();
self.sort_sets.clear();
self.sort_clears.clear();
```

`HashMap::clear()` retains allocated capacity. The WriteBatch is reused across flush cycles. During a burst (e.g., 100K ops in one cycle with many distinct field/value combinations), the HashMaps grow large. After the burst, the HashMaps retain their peak capacity forever. The `ops` Vec similarly retains its high-water-mark capacity.

**Severity:** Low
**Impact:** A few MB at most. During bulk loading, the batch could grow to handle hundreds of thousands of distinct (field, value) keys. After loading, steady-state traffic is much smaller but the HashMaps keep their large bucket arrays. Not a real concern at the scale of other memory usage (14.5 GB).
**Suggested fix:** Periodically shrink the HashMaps if their len-to-capacity ratio drops below a threshold (e.g., `if cap > 8 * len { self.filter_inserts = HashMap::new(); }`). Or simply accept this as acceptable overhead.

---

### F5: Eviction Stamps DashMap Grows Without Bound for Non-Evicted Values (MEDIUM)

**File:** `src/concurrent_engine.rs:1402-1407`

```rust
let field_arc: Arc<str> = Arc::from(field_name.as_str());
for &value in values {
    self.eviction_stamps
        .entry((field_arc.clone(), value))
        .or_insert_with(|| AtomicU64::new(now_ms))
        .store(now_ms, Ordering::Relaxed);
}
```

Every query that touches an eviction-enabled field inserts a stamp for each queried value. Stamps are only removed when a value is actually evicted (line 691). If the working set is large (e.g., queries touch 10K distinct tag IDs over weeks), the DashMap accumulates entries for all ever-queried values, even ones that were evicted and re-loaded multiple times. The stamp is removed on eviction (line 691) but re-created on the next query.

More critically, each entry key is `(Arc<str>, u64)`. The `Arc<str>` is allocated fresh from `Arc::from(field_name.as_str())` on every query invocation (line 1402), creating a new Arc allocation per query per field. These Arcs are short-lived for the query thread but the DashMap holds its own clone.

**Severity:** Medium
**Impact:** At 31K distinct tagIds values, if all are eventually queried, the DashMap holds 31K entries of `((Arc<str>, u64), AtomicU64)` -- approximately 31K * 40 bytes = ~1.2 MB, plus DashMap overhead. Not huge, but the per-query `Arc::from()` allocation for the field name is wasteful. Over months, if the set of queried values shifts, the DashMap grows monotonically (entries are only removed on eviction, and eviction only targets idle values).
**Suggested fix:** (1) Cache the `Arc<str>` for field names using the existing `FieldRegistry` instead of allocating a new `Arc::from()` per query. (2) Periodically sweep the DashMap to remove stamps for values that are no longer loaded in the filter field (i.e., values that were evicted but whose stamps were re-created by a query before eviction ran).

---

### F6: MetaIndex String Allocations in Hot Lookup Path (LOW)

**File:** `src/meta_index.rs:210, 218, 228-237, 258, 275, 291-296`

Multiple MetaIndex lookup methods allocate `String` from `&str` on every call:

```rust
self.field_bitmaps.get(&FieldKey(field.to_string()))   // line 210
self.sort_bitmaps.get(&SortKey { field: field.to_string(), ... })  // line 218-221
ClauseKey::from_canonical(clause)  // clones 3 Strings per clause
```

These are called from `maintain_filter_changes()` and `maintain_sort_changes()` on every flush cycle, and from `find_matching_entries()` on every cache lookup. The canonical clause creation in `from_canonical()` clones `field`, `op`, and `value_repr` every time.

**Severity:** Low
**Impact:** Allocator pressure from transient String allocations. Each cache lookup allocates 3 Strings per filter clause, and each flush cycle allocates Strings per mutated field. At steady-state query rates (1000+ QPS), this is thousands of small String allocations per second, though each is tiny (field names are ~10-20 bytes).
**Suggested fix:** Use `Arc<str>` or interned strings for ClauseKey/SortKey/FieldKey fields. Or use `Borrow` trait impls to allow lookup with `&str` keys without allocation (e.g., a wrapper newtype implementing Hash+Eq that borrows).

---

### F7: ensure_fields_loaded Clones Entire InnerEngine for Per-Value Loads (MEDIUM)

**File:** `src/concurrent_engine.rs:1424-1425`

```rust
let current: Arc<InnerEngine> = self.inner.load_full();
let mut updated = (*current).clone();
```

Every time a query triggers a lazy per-value load (e.g., loading a new tagId value from disk), the entire InnerEngine snapshot is cloned. This is Arc-per-bitmap so it is cheap (O(num_fields) refcount bumps), but it happens on the query thread, adding latency. More importantly, the cloned `updated` is then mutated (applying the loaded values) and published via `inner.store()`, racing with the flush thread's own `inner.store()`.

The code loads the snapshot, modifies it, and publishes it -- but between `load_full()` and `store()`, the flush thread may have published a newer snapshot. The load's publish would clobber the flush thread's mutations.

**Severity:** Medium
**Impact:** This is more of a correctness concern than memory, but the clone itself adds O(num_fields) time to the query hot path during lazy loads. At 105M with ~10 fields, this is ~10 Arc refcount bumps -- negligible per load but compounds when multiple concurrent queries trigger loads simultaneously. The race condition could also cause lost mutations (flush thread publishes, then query thread overwrites with stale snapshot + loaded field).
**Suggested fix:** Send loaded data to the flush thread exclusively (via the lazy_tx channel) and let only the flush thread publish snapshots. Remove the query-thread publish path. The query thread already sends via lazy_tx; it just also publishes directly to avoid waiting for the flush thread. A semaphore or condition variable could let the query thread wait for the flush thread to incorporate the load.

---

### F8: No Shrink-to-Fit on FilterField HashMap After Eviction (LOW)

**File:** `src/filter.rs:134-136`

```rust
pub fn remove_value(&mut self, value: u64) {
    self.bitmaps.remove(&value);
}
```

When idle eviction removes values from a FilterField's `bitmaps` HashMap, the HashMap's bucket array is never shrunk. For tagIds with 31K values, if eviction removes 25K values, the HashMap still holds bucket arrays sized for 31K entries. This is standard HashMap behavior but worth noting for a field that cycles between 5K and 31K loaded values over time.

**Severity:** Low
**Impact:** At 31K entries, HashMap overhead is ~1-2 MB (bucket array + metadata). After evicting down to 5K entries, this 1-2 MB is wasted but not significant vs. the 5+ GB of bitmap data.
**Suggested fix:** After an eviction sweep that removes a large fraction (>50%) of entries, replace the HashMap with a new one via `HashMap::from_iter()` or `shrink_to_fit()`. Only worth doing if eviction cycles are frequent.

---

### F9: Deferred Alive Map Linear Scan in `is_deferred()` (LOW)

**File:** `src/slot.rs:300-302`

```rust
pub fn is_deferred(&self, slot: u32) -> bool {
    self.deferred.values().any(|slots| slots.contains(&slot))
}
```

This is a linear scan over all deferred values (BTreeMap<u64, Vec<u32>>). Each Vec is scanned linearly. If many slots are deferred (e.g., thousands of scheduled publishes), this becomes O(total_deferred) per call. Not a memory issue per se but a scalability concern that could cause flush thread stalls.

**Severity:** Low
**Impact:** In normal operation, deferred count is small (0-100). If a batch schedule pushes thousands of deferred slots, each is_deferred check scans all of them. Currently is_deferred is only used in tests, so this is a latent issue.
**Suggested fix:** Add a `HashSet<u32>` alongside the BTreeMap for O(1) `is_deferred()` lookups if this method is ever used in production paths.

---

### F10: Snapshot Clone on Every Flush Publish (DESIGN - INFO)

**File:** `src/concurrent_engine.rs:551`

```rust
inner.store(Arc::new(staging.clone()));
```

This is the fundamental ArcSwap publish path. The `staging.clone()` is O(num_fields) Arc refcount bumps (not deep copies) thanks to Arc-per-bitmap CoW. This is well-documented and intentional. However, each publish creates a new `Arc<InnerEngine>` allocation. At the default 100us flush interval, under sustained write load, this is 10K publishes/second, each allocating an Arc. The old Arcs are deallocated when the last reader Guard drops.

**Severity:** Info (by design)
**Impact:** The staging.clone() is cheap (~10 Arc refcount bumps for ~10 fields). The Arc<InnerEngine> allocation is ~200 bytes. At 10K/s, this is 2 MB/s of allocator churn, which is negligible. Old snapshots are freed promptly since Guards are short-lived (query scope). No memory leak concern.
**Suggested fix:** None needed. This is the correct architecture. The adaptive sleep (min_sleep to max_sleep) already reduces publish rate during idle periods.

---

### F11: FieldDictionary DashMap Never Shrinks (LOW)

**File:** `src/dictionary.rs:18-26`

FieldDictionary uses two DashMaps (`map` and `originals`). These are append-only -- values are never removed. For LowCardinalityString fields, this is fine by definition (low cardinality). But if a field is misconfigured as LowCardinalityString when it is actually high cardinality, the DashMaps would grow without bound.

**Severity:** Low
**Impact:** LowCardinalityString fields typically have <1000 values. DashMap overhead at that scale is ~50 KB. Config validation should catch misconfigured fields.
**Suggested fix:** Add a config-level max_distinct warning or limit for LowCardinalityString fields. Log a warning if distinct count exceeds 10K.

---

### F12: InFlightTracker DashSet Never Shrinks (INFO)

**File:** `src/concurrency.rs:15`

```rust
in_flight: DashSet<u32>,
```

DashSet uses sharded internal HashMaps that never shrink. Under normal operation, in-flight count is 0-10 concurrent writes. The DashSet's shard allocation is minimal (~4 KB). No concern.

**Severity:** Info
**Impact:** Negligible. DashSet overhead is fixed at shard count * minimum bucket size.

---

## Prioritized Top 5

| Priority | Finding | Severity | Impact | Effort |
|----------|---------|----------|--------|--------|
| 1 | **F1: Existence set clone-per-value** | High | 100s of 500KB HashSet clones during bulk ingestion bursts | Low -- batch new values into one clone |
| 2 | **F2: Unbounded lazy load channel** | High | Multi-GB transient RSS spike at startup under concurrent queries | Medium -- add bounding or per-field load lock |
| 3 | **F3: Merge thread full snapshot clone** | Medium | Deep-clones all loaded filter fields on every persist cycle; doubles bitmap memory during persist | Medium -- serialize from Arc refs without fields_mut() |
| 4 | **F7: Query-thread snapshot clone + race** | Medium | Correctness risk: lazy load publish can clobber flush thread mutations | Medium -- remove query-thread publish, rely on lazy_tx |
| 5 | **F5: Eviction stamps DashMap unbounded + per-query Arc alloc** | Medium | Monotonic DashMap growth + wasteful Arc<str> allocation per query | Low -- cache Arc<str>, periodic stamp cleanup |
