# Shape-Grouping Cache Maintenance

**Status:** Draft for review  
**Branch:** `ivy/watcher-shape-model`  
**Problem:** Phase B of cache maintenance calls `slot_matches_filter` once per (cache entry, mutated slot) pair. At 47K affected entries × ~200 slots, that is ~9.4M calls per flush cycle — each doing 8+ bitmap lookups plus `value_repr.parse::<u64>()` string parsing.  
**Insight:** Those 47K entries share ~100 unique filter clause vectors ("shapes"). Evaluating the same shape 470 times is pure waste.

---

## The Core Insight

A "shape" is the canonical `filter_clauses` vector of a `UnifiedKey` — already stable because `cache.rs::canonicalize()` sorts clauses before key construction. Two entries with identical filter clauses have identical shape. The shape hash is just `AHasher(filter_clauses)`.

In prod today: 47K affected entries / ~100 unique shapes = ~470 entries per shape. The shape-grouping model calls `slot_matches_filter` once per shape per slot instead of once per entry per slot. At the observed prod ratio, that is a **470x reduction** in `slot_matches_filter` calls.

---

## Data Structures

### Two new indexes inside `UnifiedCache`

```rust
/// Maps shape_hash → set of UnifiedKeys sharing that canonical filter clause vector.
/// Key is a 64-bit ahash of filter_clauses (same hasher already used elsewhere).
shape_to_keys: HashMap<u64, Vec<UnifiedKey>>,

/// Maps filter field name → set of shape_hashes that reference that field.
/// Used to narrow affected shapes to only those touching a mutated filter field.
field_to_shapes: HashMap<String, HashSet<u64>>,
```

Both indexes live inside `UnifiedCache` alongside the existing `entries`, `meta`, and `meta_id_to_key`. They share the same `parking_lot::Mutex` — no new concurrency primitive needed.

### Shape hash computation

```rust
fn shape_hash(clauses: &[CanonicalClause]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = ahash::AHasher::default();
    clauses.hash(&mut h);
    h.finish()
}
```

This is the same 64-bit hash used by the existing observability code in `concurrent_engine.rs` (lines 1564–1574), so we know it is fast and adequate at <100K shapes.

### Shape-level work item

```rust
pub struct ShapeWorkItem {
    pub shape_hash: u64,
    pub filter_clauses: Vec<CanonicalClause>,   // single clone per shape, not per entry
    pub slots: Vec<u32>,
    /// All entries under this shape, each with their own min_tracked_value/direction/bitmap.
    pub entries: Vec<ShapeEntryRef>,
}

pub struct ShapeEntryRef {
    pub key: UnifiedKey,
    pub min_tracked_value: u32,
    pub direction: SortDirection,
    pub bitmap: Option<Arc<RoaringBitmap>>,  // for sort-only fast path
}
```

### Shape-level result

```rust
pub struct ShapeResult {
    pub shape_hash: u64,
    /// For each entry under this shape: add/remove lists.
    pub entry_results: Vec<CacheMaintenanceResult>,
}
```

---

## Lifecycle

### On cache insert (`insert_entry`)

After the existing `meta.register()` call:

```rust
let hash = shape_hash(&key.filter_clauses);
self.shape_to_keys.entry(hash).or_default().push(key.clone());
for clause in &key.filter_clauses {
    self.field_to_shapes
        .entry(clause.field.clone())
        .or_default()
        .insert(hash);
}
```

### On cache evict / invalidate / clear

When a `UnifiedKey` is removed from `entries`:

```rust
let hash = shape_hash(&key.filter_clauses);
if let Some(keys) = self.shape_to_keys.get_mut(&hash) {
    keys.retain(|k| k != key);
    if keys.is_empty() {
        self.shape_to_keys.remove(&hash);
        // Clean up field_to_shapes entries that now have no shapes
        for clause in &key.filter_clauses {
            if let Some(shapes) = self.field_to_shapes.get_mut(&clause.field) {
                shapes.remove(&hash);
                if shapes.is_empty() {
                    self.field_to_shapes.remove(&clause.field);
                }
            }
        }
    }
}
```

All existing eviction code paths call a single helper `remove_entry_from_indexes(key)` so deregistration is never missed.

### On server restart with existing entries

At restart, entries are restored from BoundStore shards via `insert_restored_entry()` — the same paths that call `meta.register()`. Adding shape index registration to those same call sites means zero migration code: entries register themselves during the existing lazy-load / preload process.

---

## Phase A/B/C under Shape Grouping

### Phase A: `collect_by_shape` (holds brief lock)

1. Collect `changed_slots_per_field` from coalescer (same as today).
2. Look up `field_to_shapes` for each mutated filter field → `affected_hashes`.
3. For each affected hash: look up `shape_to_keys`, collect all live non-`needs_rebuild` entries, build a `ShapeWorkItem` with the union of relevant slots.
4. Return `Vec<ShapeWorkItem>` and over-budget keys (same budget logic as today).

This replaces `collect_filter_work`. The key difference: we emit one work item per shape, not one per entry.

### Phase B: `evaluate_by_shape` (lock-free, as today)

1. Precompute sort values once per `(sort_field, slot)` — same `precompute_sort_values` function, but now the slots come from shape work items (already deduplicated at shape level).
2. For each `ShapeWorkItem`:
   a. For each slot in `item.slots`: call `slot_matches_filter(slot, &item.filter_clauses, ...)` **once**. Record result as `bool`.
   b. Build per-entry results by checking each `ShapeEntryRef`:
      - If the slot matches the filter: check `sort_qualifies(sort_value, direction)` per entry.
      - If it doesn't match: add to removes for all entries currently containing it (`bitmap.contains(slot)` per entry — O(1) roaring lookup).
3. The sort-only fast path from PR #182 applies at the per-entry-ref level, exactly as today.

### Phase C: `apply_by_shape` (holds brief lock)

Same as `apply_maintenance_results` but takes `Vec<ShapeResult>`. Iterates `entry_results` inside each `ShapeResult`, applies adds/removes per entry.

---

## Sort-Only Fast Path Compatibility

The sort-only fast path (PR #182 / commit fa3b609) checks whether a slot is in `filter_changed_slots`. If not, it uses `bitmap.contains(slot)` to short-circuit filter evaluation.

Under shape grouping, this check happens at the `ShapeEntryRef` level, not the shape level:

```
For each slot in shape work item:
  slot_matches = slot_matches_filter(slot, shape_clauses, ...)
    UNLESS slot is sort-only (not in filter_changed_slots)
        AND bitmap.contains(slot)  -- then we already know it matches, skip full eval
  
  For each entry_ref:
    if slot_matches (proven) OR sort-only short-circuit applies:
      check sort_qualifies per entry_ref
```

The critical invariant is preserved: `bitmap.contains(slot) == false` for a sort-only slot never implies "doesn't match filters" — it falls through to full filter eval. This logic moves from per-entry to per-shape-per-slot, but the semantics are identical.

**Important:** For sort-only work items (no filter change), shape grouping is not applicable — those items only need one slot_matches_filter call per (shape, slot) anyway, and the sort-only fast path already skips most of them. The existing `evaluate_sort_work` remains untouched.

---

## Concurrency

No new concurrency primitives needed. The `shape_to_keys` and `field_to_shapes` indexes are owned by `UnifiedCache` and protected by the existing `parking_lot::Mutex`. Phase A holds the lock briefly (same as today), Phase B is lock-free, Phase C holds briefly. The invariant is: the indexes are always consistent with `entries` at any point where the lock is held.

**No TOCTOU concern:** Phase B operates on a snapshot of work items collected under Phase A's lock. Even if new entries are inserted or evicted between Phase A and Phase C, Phase C's `apply_maintenance_results` already handles missing keys with a `continue` guard.

---

## Feature Flag

A new field on `CacheConfig`:

```rust
/// Use shape-grouping for filter maintenance (call slot_matches_filter once
/// per shape per slot instead of once per entry per slot).
/// Default: false. Enable to A/B test before making permanent.
#[serde(default)]
pub cache_maintenance_by_shape: bool,
```

In `collect_filter_work`, a branch selects old path vs new path:

```rust
if self.config.cache_maintenance_by_shape {
    self.collect_by_shape(...)
} else {
    self.collect_filter_work_legacy(...)
}
```

Phase C always calls the same `apply_maintenance_results` (shape results are converted to `Vec<CacheMaintenanceResult>` before Phase C, or Phase C accepts both).

The old Phase A/B/C paths stay intact as fallback. The flag is off by default — requires explicit opt-in to activate.

---

## What Does NOT Change

- `evaluate_sort_work` and `collect_sort_work` — untouched (sort-only path already has fast-reject via max/min_per_field)
- `apply_maintenance_results` — unchanged (shape results reduced to the same `CacheMaintenanceResult` format before apply)
- `slot_matches_filter` / `slot_matches_clause` — unchanged
- `MetaIndex` — unchanged (still used for budget estimation and non-Eq entries)
- `remove_slots_from_all_batch` — unchanged
- All existing tests — must pass with flag off

---

## Invariants to Document

1. **Filter fields and sort fields are disjoint** in the Civitai prod config. `shape_to_keys` is keyed on filter clauses; `mutated_sort_slots` does not interact with it. Code should not assume this invariant holds generally, but can assert it in debug mode.

2. **Shape hash collisions** are negligible at <1K unique shapes. If two different clause vectors collide (64-bit hash), the entries would be grouped into the same shape work item. `slot_matches_filter` would still be called with the correct per-shape clauses because we store `filter_clauses: Vec<CanonicalClause>` in each `ShapeWorkItem` — not just the hash. A collision only inflates work, never corrupts results.

3. **Conservative operators** (`bucket`, compound `and`/`or`) return `true` from `slot_matches_filter`. Shape grouping does not change this — the shape's work item will produce `matches = true` for those slots, and the per-entry adds still respect `sort_qualifies`. This is correct behavior (bloat control handles cleanup).

---

## Expected Impact

- Phase B CPU: ~470x fewer `slot_matches_filter` calls (47K → ~100 per cycle per slot)
- Phase A: O(shapes × slots) instead of O(entries × slots) — same asymptotic, smaller constant
- Memory: two small HashMaps (~100 shapes × ~4 keys each = negligible)
- Correctness: identical to legacy path (shape eval is logically equivalent to per-entry eval because filter_clauses are the same)

---

## Implementation Order

1. Add `shape_to_keys` and `field_to_shapes` to `UnifiedCache` struct
2. Add `shape_hash()` helper function
3. Wire registration into `insert_entry` and deregistration into all eviction paths via a single `remove_entry_from_indexes()` helper
4. Implement `collect_by_shape()` and `evaluate_by_shape()`
5. Add `cache_maintenance_by_shape` config flag
6. Wire flag in `collect_filter_work` dispatch
7. Add unit tests for registration, multi-entry-per-shape correctness, and flag behavior
8. External review (GPT + Gemini)
