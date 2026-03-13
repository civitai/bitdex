# Hot Path & Query Performance Audit

**Auditor:** Agent A — Hot Path & Query Performance Auditor
**Date:** 2026-03-13
**Scope:** Query execution hot path, bitmap operations, sort traversal, cache lookup, snapshot reads

---

## Files Examined

| File | Purpose |
|------|---------|
| `src/executor.rs` | Query execution: filter evaluation, sort traversal, pagination |
| `src/planner.rs` | Cardinality-based clause reordering |
| `src/concurrent_engine.rs` | ArcSwap snapshot reads, cache integration, lazy loading, post-validation |
| `src/unified_cache.rs` | Unified cache: lookup, store, maintenance |
| `src/cache.rs` | Cache key canonicalization |
| `src/sort.rs` | Sort layer bitmap traversal (top_n, bifurcate, cursor filtering) |
| `src/slot.rs` | Slot allocator, alive bitmap |
| `src/filter.rs` | Filter field bitmap storage, versioned reads |
| `src/versioned_bitmap.rs` | Base+diff bitmap with fused reads |
| `src/radix_sort.rs` | 8-bit radix buckets for expanded cache entries |
| `src/concurrency.rs` | InFlightTracker for post-validation |

---

## What is Already Well-Optimized

Before the findings, credit where due:

- **ArcSwap snapshot reads** (`concurrent_engine.rs:1146-1148`): The `inner.load()` returns a Guard with zero refcount ops. This is textbook lock-free.
- **Arc-per-bitmap CoW**: Snapshot cloning is O(num_fields), not O(data). Only mutated bitmaps pay the clone cost.
- **Sort traversal** (`sort.rs:174-240`): The MSB-to-LSB bifurcation is clean and correct. Pure bitmap AND operations, no allocations in the inner loop until the final `order_results`.
- **Sorted keys fast path** (`executor.rs:576-625`): Binary search on packed `(value<<32)|slot` keys is excellent for the common first-page case (~55ns).
- **Radix bucket skip**: O(1) prefix skip for deep pagination vs O(n) scan.
- **`has_in_flight()` fast exit** (`concurrent_engine.rs:2153`): Post-validation is entirely skipped when no writes are in flight.
- **Versioned bitmap `fused()` fast path** (`versioned_bitmap.rs:163-164`): When diff is empty, returns `base.clone()` which is just an Arc refcount bump. Since sort layers are always eagerly merged, this fast path hits reliably.
- **`all_slots_alive` flag** (`slot.rs:143`): Allows skipping the alive AND entirely when no deletes have occurred.

---

## Findings

### Finding 1: FilterClause Cloning in the Planner

**Severity:** MEDIUM
**File:** `src/planner.rs:132-138`
**Impact:** Every query clones all FilterClause objects during planning

The `plan_query` function clones every FilterClause to build `clause_estimates`:

```rust
let mut clause_estimates: Vec<(FilterClause, u64)> = clauses
    .iter()
    .map(|c| {
        let est = estimate_cardinality(c, filters, alive_count);
        (c.clone(), est)  // <-- clones every clause
    })
    .collect();
```

FilterClause contains `String` field names and `Vec<Value>` for In/NotIn variants. At typical query sizes (2-5 clauses), this is a few hundred bytes, but it runs on every single query. The `optimize_and_clause` function (`planner.rs:167-176`) does the same.

**Suggested fix:** Return a `Vec<usize>` of reordered indices instead of cloned clauses. The executor can then index into the original slice. Alternatively, use `Arc<str>` for field names in FilterClause to make cloning cheap.

---

### Finding 2: Alive Bitmap Clone on Every Empty-Filter Query

**Severity:** HIGH
**File:** `src/executor.rs:293`
**Impact:** Clones the full alive bitmap (104M bits = ~13MB serialized) when filters are empty

```rust
if clauses.is_empty() {
    return Ok(self.slots.alive_bitmap().clone());
}
```

A "show all documents sorted by X" query with no filters hits this path. At 105M records, the alive bitmap is ~13MB. This is a full deep clone on every such query.

**Suggested fix:** Return an `Arc<RoaringBitmap>` or `Cow<RoaringBitmap>` from `compute_filters` to avoid the clone. Since the alive bitmap is already Arc-wrapped inside VersionedBitmap, this could be zero-copy. Alternatively, track this as a sentinel "full scan" and pass the alive bitmap by reference through the sort path.

---

### Finding 3: Redundant Alive Bitmap Clone in NotEq/NotIn/Not Evaluation

**Severity:** MEDIUM
**File:** `src/executor.rs:337-339, 367-370, 377-379`
**Impact:** Each negation clause clones the full alive bitmap

```rust
// NotEq
let alive = self.slots.alive_bitmap();
let mut result = alive.clone();  // full 13MB clone
result -= &eq_bitmap;

// Not
let inner_bitmap = self.evaluate_clause(inner)?;
let alive = self.slots.alive_bitmap();
let mut result = alive.clone();  // full 13MB clone
result -= &inner_bitmap;
```

NotEq, NotIn, and Not all clone the alive bitmap to compute the complement. When these appear in AND chains, the planner should order them last, and the executor could use `apply_diff` style in-place operations on the accumulated result instead. But even ordered last, the first negation still needs the full clone.

**Suggested fix:** For NotEq/Not when there is already an accumulated AND result, compute `accumulated &! negated_bitmap` directly instead of `(alive.clone() - negated) & accumulated`. This avoids cloning alive entirely. The roaring-rs crate supports `andnot_inplace` patterns via `-=`.

---

### Finding 4: Cache Key Canonicalization Allocates Strings on Every Query

**Severity:** MEDIUM
**File:** `src/cache.rs:20-114`
**Impact:** Every cache lookup allocates multiple Strings for the canonical key

`CanonicalClause::from_filter` allocates:
- `field.clone()` (String clone)
- `"eq".to_string()`, `"neq".to_string()`, etc. (new String allocation)
- `value_to_string(value)` (formats integers into Strings)
- For In/NotIn: allocates a Vec of String values, sorts them, joins with ","

This runs on every `execute_query` call to build the `UnifiedKey` for cache lookup (`concurrent_engine.rs:1676-1681`). At high QPS, this is thousands of small allocations per second, all for HashMap key construction.

**Suggested fix:** Pre-intern operator strings as `&'static str`. Use a pre-hashed key approach: hash the clauses incrementally without materializing intermediate Strings. Or switch CanonicalClause fields to `Arc<str>` / `&'static str` for the op field. For the value_repr, consider storing the raw integer value directly instead of formatting to String.

---

### Finding 5: Unified Cache Lookup Takes a Mutex Lock

**Severity:** HIGH
**File:** `src/concurrent_engine.rs:1683-1696`
**Impact:** Every query with a sort clause acquires a mutex for cache lookup

```rust
let cache_data = {
    let mut uc = self.unified_cache.lock();
    uc.lookup(&ukey).map(|entry| {
        let bm = entry.bitmap().as_ref().clone();
        // ... clone 6 more fields
    })
};
```

The unified cache is protected by `parking_lot::Mutex`. Even though `parking_lot` is fast, this serializes all query threads through a single lock for both the lookup AND the bitmap clone. At high concurrency (100+ concurrent queries), this becomes a bottleneck.

The bitmap clone inside the lock is especially bad: `entry.bitmap().as_ref().clone()` clones a RoaringBitmap (potentially 10K+ entries) while holding the mutex. The sorted_keys `.to_vec()` is also done inside the lock.

**Suggested fix:** Two options:
1. Use `RwLock` instead of `Mutex` — lookups are reads and can proceed concurrently. Only `form_and_store`/`maintain_*` need write access.
2. Store cache entry data behind `Arc` so lookup returns `Arc<UnifiedEntry>` without cloning the bitmap. The `touch()` timestamp update needs interior mutability (AtomicU64).

---

### Finding 6: Double Cache Lookup on Slow Path

**Severity:** LOW
**File:** `src/concurrent_engine.rs:1676 + 1868`
**Impact:** Cache miss queries canonicalize and look up twice

When the fast path at line 1676 succeeds, great. But when it misses (falls through to `execute_query_slow_path`), the slow path re-canonicalizes the clauses (`cache::canonicalize` at line 1868) and does another `uc.lookup` (line 1874). The canonicalization result from the fast path is not passed through.

**Suggested fix:** Pass the already-computed `UnifiedKey` from the fast path into `execute_query_slow_path`. The `cached` parameter already carries some data but the key itself is not reused for the miss case.

---

### Finding 7: `fused()` Clones Base Bitmap Even When Clean

**Severity:** LOW (but frequent)
**File:** `src/versioned_bitmap.rs:162-164`
**Impact:** `base.as_ref().clone()` does a full RoaringBitmap deep copy

```rust
pub fn fused(&self) -> RoaringBitmap {
    if self.diff.is_empty() {
        return self.base.as_ref().clone();  // deep copy, not Arc clone
    }
```

When the diff is empty (common case — sort layers are always merged), `fused()` still does a full `RoaringBitmap::clone()`, not an `Arc::clone()`. This is because `fused()` returns an owned `RoaringBitmap`, not an `Arc`. This is called in the filter evaluation path (`executor.rs:327`: `vb.fused()`) for every Eq clause.

In the filter evaluation hot path, the returned bitmap is immediately ANDed with the accumulator (`executor.rs:301`: `existing & &bitmap`). If `fused()` returned `Cow<RoaringBitmap>`, the AND could borrow instead of requiring ownership.

**Suggested fix:** Add a `fused_or_base(&self) -> &RoaringBitmap` method that returns a reference to the base when clean (panics or falls back when dirty). Since filter bitmaps in the snapshot are always clean (compaction runs every 50 cycles), this would eliminate the clone entirely for filter evaluation. The AND operation in `compute_filters` can work with a reference.

---

### Finding 8: `compute_filters` AND Chain Always Owns Both Operands

**Severity:** MEDIUM
**File:** `src/executor.rs:296-306`
**Impact:** Each AND step consumes one owned bitmap and produces a new one

```rust
let mut result: Option<RoaringBitmap> = None;
for clause in clauses {
    let bitmap = self.evaluate_clause(clause)?;
    result = Some(match result {
        Some(existing) => existing & &bitmap,  // owned & borrowed = new allocation
        None => bitmap,
    });
}
```

The `existing & &bitmap` pattern allocates a new RoaringBitmap for the intersection. But `roaring-rs` supports in-place intersection: `existing &= &bitmap` which modifies `existing` in place. This avoids allocating a new bitmap at each AND step.

**Suggested fix:** Use `&=` (BitAndAssign) instead of `&`:
```rust
result = Some(match result {
    Some(mut existing) => { existing &= &bitmap; existing },
    None => bitmap,
});
```

---

### Finding 9: Cursor-Path Bitmap Clone in `slot_order_paginate_dir`

**Severity:** LOW
**File:** `src/executor.rs:500-501`
**Impact:** Clones the full candidates bitmap on cursor path

```rust
if let Some(cursor) = cursor {
    let mut narrowed = candidates.clone();  // full clone of filter result
    narrowed.remove_range(0..=cursor.slot_id);
```

When paginating with a cursor, the entire candidates bitmap is cloned just to remove a range. For a 21M-entry filter result, this is a non-trivial allocation.

**Suggested fix:** Use `candidates.iter().rev().take_while(|&s| s < cursor.slot_id).take(limit)` for descending, avoiding the clone entirely. Or use a temporary bitmap created from the range intersection: `candidates & RoaringBitmap::from_range(0..cursor.slot_id)`. roaring-rs range operations are very fast.

---

### Finding 10: Eviction Stamp Arc::from() on Every Query with Lazy Value Fields

**Severity:** MEDIUM
**File:** `src/concurrent_engine.rs:1392-1410`
**Impact:** Allocates Arc<str> per field per query for eviction stamping

```rust
for (field_name, values) in &needed_values {
    if self.config.filter_fields.iter()
        .any(|fc| fc.name == *field_name && fc.eviction.is_some())
    {
        let field_arc: Arc<str> = Arc::from(field_name.as_str());  // allocation
        for &value in values {
            self.eviction_stamps
                .entry((field_arc.clone(), value))  // Arc clone per value
```

Every query that touches a lazy-value field (tagIds) does:
1. A linear scan of `config.filter_fields` to check eviction config
2. `Arc::from(field_name.as_str())` allocation per field
3. `field_arc.clone()` per value (cheap Arc bump, but still atomic ops)

The linear scan of `config.filter_fields` is O(num_fields) per query, per field. At 10 fields, this is ~100 iterations per query just for eviction config lookup.

**Suggested fix:** Pre-compute a `HashSet<String>` of eviction-enabled field names at engine construction time. Store it as a member of ConcurrentEngine. Also pre-intern the field name `Arc<str>` values at construction, so the hot path just does a HashSet lookup + Arc clone (no allocation).

---

### Finding 11: `time_buckets` Mutex Lock on Every Query

**Severity:** LOW
**File:** `src/concurrent_engine.rs:1644, 1300`
**Impact:** Every query acquires a parking_lot::Mutex lock on TimeBucketManager

```rust
let tb_guard = self.time_buckets.as_ref().map(|tb| tb.lock());
```

The time bucket manager is locked for the entire query execution. While `parking_lot::Mutex` is fast, this serializes reads through a single lock point. The time bucket bitmaps are read-only from the query thread's perspective.

**Suggested fix:** Wrap TimeBucketManager in `ArcSwap` instead of `Mutex`. The flush thread can update it atomically. Query threads load it lock-free. Alternatively, use `RwLock` since queries only read.

---

### Finding 12: Planner Clones FilterClause in `optimize_and_clause`

**Severity:** LOW
**File:** `src/planner.rs:167-176`
**Impact:** Called recursively for every And node in the filter tree

`optimize_and_clause` clones all sub-clauses of an And node, same pattern as `plan_query`. Since this is called from `evaluate_clause` (`executor.rs:385-389`) for every nested And, deeply nested filters multiply the cloning cost.

**Suggested fix:** Same as Finding 1 — return indices or use borrow-based approach.

---

### Finding 13: `NotIn` Creates a Full FilterClause::In Clone for Recursive Evaluation

**Severity:** LOW
**File:** `src/executor.rs:367`
**Impact:** Clones field name + entire values Vec to delegate to In evaluation

```rust
let in_bitmap = self.evaluate_clause(&FilterClause::In(field.clone(), values.clone()))?;
```

NotIn delegates to In by constructing a temporary FilterClause::In, cloning the field name and the entire Vec<Value>. For a NotIn with 50 tag IDs, this clones 50 Value objects just to reuse the In code path.

**Suggested fix:** Extract the In evaluation logic into a helper function that takes `(&str, &[Value])` directly, avoiding the clone. Both In and NotIn can call the helper.

---

### Finding 14: `post_validate` Linear Scan for Invalid Slot Removal

**Severity:** LOW
**File:** `src/concurrent_engine.rs:2172-2176`
**Impact:** O(n*m) where n=result size, m=invalid slots

```rust
result.ids.retain(|id| !invalid_slots.contains(&(*id as u32)));
```

`invalid_slots` is a Vec, so `contains` is O(m). With result size 100 and 1-2 invalid slots, this is fine. But the `retain` + `contains` pattern is O(n*m). If write contention increases (e.g., bulk updates), this could become noticeable.

**Suggested fix:** Use a HashSet for `invalid_slots` if the count exceeds a threshold (e.g., >4).

---

### Finding 15: `snap_range_clauses` HashMap Allocation Per Query

**Severity:** LOW
**File:** `src/concurrent_engine.rs:2127-2128`
**Impact:** Allocates a HashMap on every query that has time buckets

```rust
let mut managers = std::collections::HashMap::new();
managers.insert(tb.field_name().to_string(), tb);
```

A single-entry HashMap is created on every query with time buckets, just to match the `BucketSnapContext` interface. This involves a heap allocation for the HashMap + a String clone for the key.

**Suggested fix:** Change `BucketSnapContext` to accept a single `(&str, &TimeBucketManager)` since there is only ever one time bucket manager. Or use a small inline map (e.g., `SmallVec` or `arrayvec`).

---

## Prioritized Top 5

| Rank | Finding | Severity | Impact at 500M+ Scale | Effort |
|------|---------|----------|----------------------|--------|
| **1** | **#5: Unified cache mutex serializes all query threads** | HIGH | At 500M, cache entries are larger, lock hold time increases. With 100+ concurrent readers, this becomes the primary bottleneck. | Medium — requires interior mutability for `touch()` and `RwLock` or `ArcSwap` for the cache |
| **2** | **#2: Alive bitmap clone on empty-filter queries** | HIGH | 500M alive bitmap = ~60MB. Every "browse all" query pays this. These are common in infinite scroll UIs. | Low — return `Arc` or `Cow` from `compute_filters` |
| **3** | **#8: AND chain allocates new bitmap per step** | MEDIUM | At 500M with 3-5 filter clauses, each intersection produces a new multi-MB bitmap. In-place `&=` eliminates intermediate allocations. | Low — change `&` to `&=` in `compute_filters` |
| **4** | **#4: Cache key canonicalization String allocations** | MEDIUM | At high QPS (10K+), thousands of small String allocations per second pressure the allocator. Affects p99 latency via jemalloc contention. | Medium — requires changing CanonicalClause field types |
| **5** | **#3: Alive clone in every negation clause** | MEDIUM | 500M alive = ~60MB clone per negation. Queries like "NOT nsfwLevel=28" are extremely common. Eliminating this when an accumulated result exists cuts 50%+ of the allocation. | Low — change `compute_filters` to pass accumulated result into negation evaluation |

### Honorable Mentions

- **#7 (fused() deep copy)** would be #3 priority if filter bitmaps were frequently dirty, but since compaction runs every 50 flush cycles, this is usually in the fast path. Still worth fixing for correctness under load.
- **#10 (eviction stamp allocation)** compounds with #5 since the DashMap operations add atomic contention on top of the cache mutex.
- **#11 (time bucket mutex)** is a serialization point but the hold time is very short (just a pointer dereference), so it is lower priority than the cache mutex.
