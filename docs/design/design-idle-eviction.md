# Design: Idle-Time Bitmap Eviction

## Problem

Multi-value filter fields like `tagIds` use per-value lazy loading: bitmaps are loaded from BitmapFs on first query. But once loaded, they stay in memory forever. At 105M records, `tagIds` has ~31K distinct values consuming ~5.2 GB (79% of all filter bitmap memory). Real query patterns touch maybe 50-100 popular tags regularly — the rest is dead weight after the initial query that loaded them.

The popular tags can't be compressed further. Tags like "anime" have 50M+ bits set — roaring bitmaps can't compress dense data. The memory cost is inherent to the data. The win is not keeping bitmaps in memory that nobody is querying anymore.

## Solution: Per-Value Idle Eviction

Add a configurable idle timeout to multi-value filter fields. Values untouched for N flush cycles get evicted from the in-memory `FilterField` HashMap. The next query referencing that value re-loads it from BitmapFs via the existing `ensure_fields_loaded()` per-value lazy loading path.

### Why idle time, not memory budget

- **Self-regulating.** Steady-state memory equals the working set — the tags people are actually querying. No tuning required beyond the idle threshold.
- **Per-field configurable.** A field with expensive-to-load bitmaps (many values, large bitmaps) can keep them longer. A field with cheap reloads can evict aggressively.
- **No scanning needed.** Each value tracks its own last-touched cycle. The eviction sweep is a simple filter over the HashMap during the flush loop — values that expired remove themselves.
- A memory budget solves a problem we're unlikely to have. If the working set is genuinely too large for RAM, that's a capacity planning problem, not an eviction policy problem.

## Design

### Config: `eviction` block on `FilterFieldConfig`

```json
{
  "name": "tagIds",
  "field_type": "multi_value",
  "eviction": {
    "idle_cycles": 50000
  }
}
```

| Field | Type | Description |
|---|---|---|
| `idle_cycles` | `u32` | Evict values untouched for this many flush cycles. At 100us flush interval with adaptive backoff, 50,000 cycles is roughly 5-50 seconds of wall time depending on load. |

Fields without an `eviction` block never evict (current behavior). Only meaningful on `multi_value` fields — `single_value` and `boolean` fields have low cardinality and should always stay resident.

Validation: reject `eviction` on non-`multi_value` fields (or just ignore it with a warning).

### Access tracking: generation stamps — OUTSIDE FilterField

> **Microbench result (2026-03-12):** Cloning `HashMap<u64, AtomicU64>` inside `FilterField` costs **963μs at 31K entries** (borderline) and **38ms at 553K entries** (infeasible). Since `FilterField` is cloned on every snapshot publish via `Arc::make_mut()`, the `last_touched` map **must not live inside FilterField**. See `tests/eviction_clone_bench.rs`.

**Revised approach: side structure on `ConcurrentEngine`**

Store access stamps in a separate `Arc<DashMap<(Arc<str>, u64), AtomicU64>>` on `ConcurrentEngine`, keyed by `(field_name, value)`. This structure is never cloned — readers and the flush thread share it directly.

```rust
pub struct ConcurrentEngine {
    // ... existing fields ...
    /// Per-value last-accessed flush cycle. Shared between readers and flush thread.
    /// Key: (field_name, value_id). Value: flush cycle when last touched.
    /// Only contains entries for eviction-enabled fields.
    eviction_stamps: Arc<DashMap<(Arc<str>, u64), AtomicU64>>,
    /// Global flush cycle counter, incremented by flush thread.
    flush_cycle: Arc<AtomicU64>,
}
```

`FilterField` stays unchanged (no `last_touched`, no manual `Clone`):

```rust
pub struct FilterField {
    bitmaps: HashMap<u64, VersionedBitmap>,
    config: FilterFieldConfig,
    // No eviction state here — it lives on ConcurrentEngine
}
```

**Reader stamping:** When a query accesses a value in an eviction-enabled field, it stamps the DashMap entry:

```rust
// In the query path, after looking up a filter value:
if field.config.eviction.is_some() {
    let cycle = self.flush_cycle.load(Ordering::Relaxed);
    self.eviction_stamps
        .entry((field_name.clone(), value_id))
        .or_insert_with(|| AtomicU64::new(cycle))
        .store(cycle, Ordering::Relaxed);
}
```

DashMap provides lock-free concurrent reads and sharded writes — no contention between reader threads.

**Eviction sweep:** The flush thread reads from the same DashMap:

```rust
fn evict_idle(&mut self, field_name: &str, field: &mut FilterField, current_cycle: u64) {
    let idle_cycles = match &field.config.eviction {
        Some(e) => e.idle_cycles as u64,
        None => return,
    };

    let to_evict: Vec<u64> = field.bitmaps.keys()
        .filter(|&&value| {
            self.eviction_stamps
                .get(&(field_name.into(), value))
                .map(|entry| current_cycle - entry.load(Ordering::Relaxed) > idle_cycles)
                .unwrap_or(true) // no stamp = never touched = evict
        })
        .copied()
        .collect();

    for value in &to_evict {
        field.bitmaps.remove(value);
        self.eviction_stamps.remove(&(field_name.into(), *value));
    }
}
```

**Why DashMap over alternatives:**
- `Arc<Mutex<HashMap>>` would require readers to lock on every query — unacceptable contention.
- Channel-based side reporting adds latency and complexity.
- DashMap gives us concurrent lock-free reads (stamp lookups) with sharded writes (stamp updates). At 31K entries, each shard holds ~500 entries — no contention.

> **Microbench confirmed (2026-03-12):** AtomicU64 relaxed stores through `Arc`/`ArcSwap` shared references work correctly — 225M ops/sec across 4 threads, clone isolation verified. See `tests/eviction_atomics_test.rs`.

**Cost:** DashMap adds a dependency but it's already widely used in the Rust ecosystem. Memory overhead is ~31K * (16 bytes key + 8 bytes AtomicU64 + DashMap overhead) ≈ ~1 MB. Negligible.

### Eviction sweep in the flush loop

Add an eviction pass to the flush thread, running every `EVICTION_INTERVAL` cycles (e.g., every 1,000 flush cycles — roughly every 0.1-1s):

```rust
const EVICTION_INTERVAL: u64 = 1000;

if flush_cycle % EVICTION_INTERVAL == 0 {
    for (name, field) in staging.filters.fields_mut() {
        field.evict_idle(flush_cycle);
    }
}
```

`FilterField::evict_idle()`:

```rust
pub fn evict_idle(&mut self, current_cycle: u64) {
    let idle_cycles = match &self.config.eviction {
        Some(e) => e.idle_cycles as u64,
        None => return, // no eviction configured
    };

    let last_touched = match &self.last_touched {
        Some(lt) => lt,
        None => return,
    };

    let to_evict: Vec<u64> = self.bitmaps.keys()
        .filter(|value| {
            last_touched.get(value)
                .map(|lt| current_cycle - lt.load(Ordering::Relaxed) > idle_cycles)
                .unwrap_or(true) // no stamp = never touched = evict
        })
        .copied()
        .collect();

    for value in &to_evict {
        self.bitmaps.remove(value);
        if let Some(lt) = &mut self.last_touched {
            lt.remove(value);
        }
    }

    if !to_evict.is_empty() {
        eprintln!(
            "Evicted {} idle values from filter '{}'",
            to_evict.len(), self.config.name
        );
    }
}
```

### Re-load on demand

Already works. When a query references `tagIds=42` and value 42 has been evicted:

1. `ensure_fields_loaded()` checks if the value exists in the snapshot via `get_versioned()` → returns `None`
2. Calls `store.load_field_values("tagIds", &[42])` → reads from BitmapFs (NVMe, ~microseconds)
3. Inserts into snapshot, stamps `eviction_stamps`, publishes via ArcSwap

No changes needed to the lazy loading path. Eviction makes the value "pending" again naturally.

> **Measured reload latency (2026-03-12, 105M Civitai dataset):** Tags that exist in the index: **4.7ms cold → 21μs warm** (subsequent queries). Popular tags: **11μs** steady state. Eviction + reload is viable for all existing tag values. See `tools/measure-reload.mjs`.
>
> **Finding:** Zero-result tag queries (nonexistent tag IDs) are consistently 30-50ms and never speed up, even warm. Root cause: no negative cache in per-value lazy loading — `ensure_fields_loaded()` opens `.fpack` from disk every time for nonexistent values. **Solution: Positive Existence Set** — see dedicated section below.

### Interaction with mutations

When a document is upserted with `tagIds: [42, 99]`, the write coalescer sends `MutationOp::SetFilter` for each value. The flush thread's `apply_prepared()` inserts the bitmap for value 42 even if it was evicted. This is fine — the value re-enters the HashMap via mutation, gets stamped, and is subject to eviction again later.

**Edge case:** An evicted value receives a mutation (set bit for slot X), but the bitmap is gone. `FilterField::insert()` creates a new `VersionedBitmap` via `entry().or_insert_with()`, so the bit is set in a fresh bitmap. But this bitmap is missing all the other bits from disk.

**Fix:** When `evict_idle()` removes a value, it does NOT remove it from disk (BitmapFs always has the full bitmap). The value must be re-loaded from disk before mutations can be applied. Two options:

1. **Reload-before-mutate:** In `apply_prepared()`, if a value has eviction enabled and the value's bitmap is missing, load from disk first, then apply the mutation. This adds a disk read on the mutation path but only for evicted values receiving writes (rare).
2. **Never evict dirty values:** Track which values have pending mutations. Only evict clean values. Mutations implicitly keep values resident.

**Recommendation:** Option 1 (reload-before-mutate). It's simpler and the case is rare — most mutations hit popular values that are already resident. The disk read is microseconds on NVMe.

### Interaction with snapshot publish

The eviction sweep runs on staging (flush thread's private copy). When it removes values and publishes, the new snapshot simply doesn't contain those values. In-flight readers hold their own `Arc<InnerEngine>` snapshot — they still see the old bitmaps until they finish. No correctness issues.

### Interaction with cache invalidation

Evicting a bitmap doesn't change any query results — it's just removing the in-memory copy. The trie cache and unified cache are keyed by filter clauses, not bitmap presence. When a query hits a cache entry, the result is already computed and correct. When a cache miss occurs and the bitmap needs to be loaded from disk, the query proceeds normally.

No cache invalidation needed on eviction.

### Interaction with bound cache

Bound cache entries reference filter field bitmaps for invalidation tracking. Evicting a bitmap doesn't invalidate bounds — the bound is still valid because the underlying data hasn't changed. When a bound needs rebuilding, it will trigger bitmap loads as needed via `ensure_fields_loaded()`.

No changes needed to bound cache.

### Interaction with merge thread

The merge thread periodically compacts `VersionedBitmap` diffs. It must not crash on values that were evicted between merge scheduling and execution. Since merge operates on the staging engine (same as flush thread, single-threaded), and eviction also runs on staging, there's no race. Evicted values are simply absent from the HashMap — the merge loop's `values_mut()` iterator skips them.

### Interaction with bitmap persistence (save_snapshot)

`save_snapshot()` iterates all in-memory filter bitmaps and writes them to BitmapFs. With eviction, some values are only on disk (evicted). This is fine — `save_snapshot()` only needs to persist what's in memory (which may be newer than disk due to mutations). BitmapFs already has the evicted values' bitmaps from the last save.

**Edge case:** A value is loaded, mutated, evicted, and `save_snapshot()` runs. The mutation is lost because the bitmap was evicted before being persisted.

**Fix:** Don't evict dirty (unmutated) bitmaps. `evict_idle()` should skip values with `is_dirty() == true`. Since compaction runs every 50 cycles and eviction runs on much longer timescales (50K+ cycles), dirty bitmaps will be compacted and persisted long before eviction considers them.

```rust
.filter(|value| {
    // Don't evict dirty bitmaps — they have unpersisted mutations
    if let Some(vb) = self.bitmaps.get(value) {
        if vb.is_dirty() { return false; }
    }
    // ... idle check
})
```

### Prometheus metrics

Add gauges to track eviction behavior:

| Metric | Type | Description |
|---|---|---|
| `bitdex_eviction_total` | Counter (per field) | Total values evicted since startup |
| `bitdex_eviction_resident_values` | Gauge (per field) | Currently resident value count |
| `bitdex_eviction_reloads_total` | Counter (per field) | Times a value was re-loaded after eviction |

These let operators tune `idle_cycles` per field based on actual eviction/reload rates.

## Worked Example: Civitai tagIds

Current state (no eviction):
- 31K distinct tag values, ~5.2 GB in memory
- All loaded on first query touching each value, never freed

With eviction (`idle_cycles: 50000`):
- Startup: 0 tag bitmaps in memory (per-value lazy loading)
- User queries `tagIds=42` (anime): loaded from disk, stamped
- Over time, ~100 popular tags accumulate: ~100-500 MB resident (popular tags have large bitmaps)
- Rare tag queried once, untouched for 50K cycles: evicted
- Steady state: ~100-200 values resident, ~500 MB-1 GB instead of 5.2 GB

Estimated memory savings: **~4 GB** (80% reduction in tag memory).

## Config Example (Civitai)

```json
{
  "filter_fields": [
    {
      "name": "nsfwLevel",
      "field_type": "single_value"
    },
    {
      "name": "tagIds",
      "field_type": "multi_value",
      "eviction": {
        "idle_cycles": 50000
      }
    },
    {
      "name": "modelVersionIds",
      "field_type": "multi_value",
      "eviction": {
        "idle_cycles": 100000
      }
    },
    {
      "name": "toolIds",
      "field_type": "multi_value"
    }
  ]
}
```

Here `tagIds` evicts after 50K idle cycles, `modelVersionIds` keeps values longer (100K cycles), and `toolIds` has no eviction (low cardinality, not worth the complexity).

## Implementation Plan

### Step 1: Config
- Add `EvictionConfig { idle_cycles: u32 }` struct
- Add `eviction: Option<EvictionConfig>` to `FilterFieldConfig`
- Validation: warn if set on non-`multi_value` field

### Step 2: Access tracking (revised — stamps outside FilterField)
- Add `eviction_stamps: Arc<DashMap<(Arc<str>, u64), AtomicU64>>` to `ConcurrentEngine`
- Add `flush_cycle: Arc<AtomicU64>` to `ConcurrentEngine`
- Add `dashmap` dependency to Cargo.toml
- Stamp in query path: after filter value lookup, if field has eviction config, update DashMap entry
- Stamp on `insert` / `load_from` (value entering memory should be considered "touched")
- No changes to `FilterField` struct or its `Clone` impl

### Step 3: Eviction sweep
- Add `evict_idle(current_cycle: u64)` to `FilterField`
- Skip dirty bitmaps (unpersisted mutations)
- Call from flush loop every `EVICTION_INTERVAL` cycles
- Log eviction count per field

### Step 4: Reload-before-mutate
- In `FilterField::insert()` / `or_bitmap()`, if eviction is enabled and the value is missing, the caller must ensure reload from disk first
- This requires access to `BitmapFs` from the flush thread — already available via the coalescer/mutation path
- Alternative: accept that fresh-inserted values start empty and rely on the next `save_snapshot` + load cycle to catch up. This is only correct if mutations are complete (all bits for the value are set in the mutation batch). For single-doc upserts this is true, but for bulk loading it may not be.

### Step 5: Metrics
- Add Prometheus counters/gauges for eviction stats

### Step 6: Tests
- Unit test: evict_idle removes values past threshold
- Unit test: dirty bitmaps are not evicted
- Unit test: re-load after eviction via ensure_fields_loaded
- Integration test: query → evict → re-query returns same results
- Benchmark: measure eviction sweep overhead at 31K values

## Microbench Validation Results (2026-03-12)

Three assumptions were tested before implementation. Test files are checked in.

| Assumption | Test | Result | Verdict |
|---|---|---|---|
| AtomicU64 stores through ArcSwap | `tests/eviction_atomics_test.rs` | 225M ops/sec, clone isolation confirmed | **PASS** |
| Clone cost at 31K entries (tagIds) | `tests/eviction_clone_bench.rs` | 963μs p50 | **BORDERLINE** — must move stamps outside FilterField |
| Clone cost at 553K entries (userId) | `tests/eviction_clone_bench.rs` | 38ms p50 | **FAIL** — rules out stamps inside FilterField |
| Eviction sweep cost at 31K | `tests/eviction_clone_bench.rs` | 44-73μs | **PASS** |
| Single-value reload latency | `tools/measure-reload.mjs` | 4.7ms cold → 21μs warm | **PASS** — eviction+reload viable |
| Zero-result tag queries | `tools/measure-reload.mjs` | 30-50ms, never caches | **FINDING** — separate optimization needed |

**Key design change driven by benchmarks:** `last_touched` stamps moved from `FilterField` (cloned every snapshot publish) to a shared `DashMap` on `ConcurrentEngine` (never cloned). See revised "Access tracking" section.

---

## Review Comments

> **Review by Claude Opus (2026-03-12)** — notes from design review session with Justin.

### idle_cycles is opaque — use seconds instead

50,000 cycles maps to "5-50 seconds of wall time depending on load" — that's a 10x range. Operators can't reason about this when tuning. Suggest exposing `idle_seconds` in config and having the flush thread convert internally using its observed cycle rate:

```json
{
  "name": "tagIds",
  "field_type": "multi_value",
  "eviction": {
    "idle_seconds": 30
  }
}
```

The flush thread tracks its own cycle timing already. On each eviction sweep, it can compute `threshold_cycles = idle_seconds / avg_cycle_duration`. This makes the config human-readable and stable across different hardware/load profiles. At minimum, log the effective idle timeout in seconds on startup so operators can correlate.

### AtomicU64 stamp gap after eviction + reload — VALIDATED SAFE

~~Reader threads hold immutable `&FilterField` via ArcSwap snapshots. The `relaxed store` through `&AtomicU64` is fine. But when a value is evicted and re-loaded, the new entry only exists in staging. If the eviction sweep runs before readers pick up the new snapshot, the value could be immediately re-evicted.~~

**Resolved (2026-03-12):** With stamps in DashMap (outside FilterField), stamps persist across all snapshot transitions. The concurrent stress test (4 readers + flush thread, 142K eviction rounds, 41M reader ops) confirmed **zero false evictions**. Values stamped at reload time are protected by the idle threshold window. No grace period or special stamping needed. See `tests/eviction_stamp_gap_test.rs`.

### Verify BitmapFs is plumbed to flush thread

Step 4 (reload-before-mutate) says "requires access to `BitmapFs` from the flush thread — already available." Before implementing, verify the flush thread actually has a handle to `BitmapFs` for the right index. The flush thread has the coalescer and staging engine, but the `BitmapFs` store path may need to be threaded through from `ConcurrentEngine::new_with_path()`. This is a small plumbing task but easy to miss.

### Eviction sweep cost at scale

The sweep scans all values in eviction-enabled fields — 31K atomic loads for tagIds every `EVICTION_INTERVAL` (1,000 cycles, ~0.1s). This is cheap, but worth a quick benchmark. If it shows up in flush thread cycle time at higher cardinalities (e.g., userId at 553K values if eviction were ever enabled there), bump `EVICTION_INTERVAL` or skip fields where `last_touched.len() < 100`.

### Unified cache interaction on expansion

Missing from the interaction analysis: when a unified cache entry needs to **expand** (cursor past boundary, fetch more sorted slots), the expansion path calls `execute_from_bitmap` which intersects filter bitmaps. If the relevant tag bitmap was evicted, this triggers a lazy reload from disk during the expansion — adding latency to what's supposed to be a fast cache path.

This isn't a correctness issue, but it means expansion of cache entries for recently-evicted tags will be slower (~5-100ms disk read instead of ~0ms). Worth noting explicitly. Could also consider pinning bitmaps referenced by active unified cache entries, but that's probably over-engineering it — the cache already handles slow expansions gracefully.

### Idea: eviction as a Prometheus-driven feedback loop

The design already includes `bitdex_eviction_reloads_total` as a metric. Consider building on this: if the reload rate for a field exceeds a threshold (e.g., >10 reloads/sec sustained), automatically increase the effective idle threshold for that field. This turns eviction into a self-tuning system where the idle timeout adapts to actual traffic patterns rather than requiring manual config changes.

This could be as simple as: if `reloads_in_last_minute > 60`, double the effective `idle_seconds` (capped at some max). Reset when reload rate drops. No config change needed — just runtime adaptation.

### Idea: batch eviction with snapshot coalescing

Rather than evicting values one-at-a-time across multiple flush cycles, consider batching: the eviction sweep collects all candidates, then removes them all in a single staging mutation + snapshot publish. This ensures readers see a single atomic transition rather than a series of partial evictions. The current design already does this (sweep collects `to_evict` vec, then removes all), so this is just reinforcing that the batch approach is correct.

### Idea: warm-set pinning for production deployments

For production, consider a `pinned_values` list in config — values that are never evicted regardless of idle time. For Civitai, this would be the top ~20 tags (anime, realistic, etc.) that are guaranteed to be in every feed query. These tags have the largest bitmaps (50M+ bits) and the longest reload times. Pinning them avoids the worst-case reload latency entirely.

```json
{
  "name": "tagIds",
  "field_type": "multi_value",
  "eviction": {
    "idle_seconds": 30,
    "pinned_values": [5, 42, 99, 1337]
  }
}
```

This is optional sugar — the idle threshold alone handles it if set high enough. But explicit pinning makes the intent clear and avoids surprises.

---

## Stamp Gap Race Condition: Validated Safe (2026-03-12)

The concern raised in "AtomicU64 stamp gap after eviction + reload" was tested with 5 dedicated tests (`tests/eviction_stamp_gap_test.rs`):

| Test | What it validates | Result |
|---|---|---|
| `evict_reload_no_false_reeviction` | Deterministic evict→reload→re-evict cycle | Value stamped at reload survives next sweep |
| `rapid_evict_reload_stress` | 4 readers + flush thread, 1s sustained | 0 false evictions across 142K eviction rounds, 41M reader ops |
| `grace_period_freshly_loaded_values_protected` | Boundary conditions on stamp vs cutoff | All edge cases correct |
| `boundary_stamp_equals_cutoff_not_evicted` | stamp == cutoff is NOT evicted | Confirmed: `<` not `<=` |
| `stamp_survives_snapshot_transition` | DashMap stamps persist across ArcSwap publishes | Stamps live outside snapshot, always visible |

**DashMap contention validated:** 109ns single-thread, 155ns under 4-thread contention. Well under 500ns hot-path budget. See `tests/eviction_dashmap_bench.rs`.

**Conclusion:** The DashMap + AtomicU64 stamp approach is safe. No grace period or special stamping needed — the fundamental design is correct. Values stamped at reload time are protected by the idle threshold window.

---

## Positive Existence Set: Solving Zero-Result Queries (2026-03-12)

### Problem

The `tools/measure-reload.mjs` investigation found that queries for **nonexistent** tag IDs cost 30-50ms each and never improve — `ensure_fields_loaded()` opens the `.fpack` file from disk, finds nothing, and remembers nothing. The workload has 379 such queries (15% of all tag queries). This is orthogonal to eviction but compounds with it: evicted values reload in 4.7ms, but nonexistent values waste 30-50ms every time.

### Recommendation: In-Memory Key Dictionary (not a negative cache)

Both Gemini and GPT were consulted independently. **Both converged on the same answer**: maintain an exact in-memory set of all existing value IDs per field.

For tagIds with ~31K distinct values:

```rust
/// On ConcurrentEngine, one per eviction-enabled multi_value field
existing_keys: Arc<ArcSwap<FxHashSet<u64>>>
```

**Query path** (in `ensure_fields_loaded()`, before disk lookup):
```rust
if !self.existing_keys[field_name].load().contains(&value_id) {
    // Value doesn't exist in the index — skip disk entirely
    return; // or cache as empty bitmap
}
// Value exists but bitmap not loaded → proceed with lazy load from .fpack
```

**Memory:** 31K × 16 bytes = ~500 KB. Negligible.

**Lookup:** <20ns (FxHashSet, no allocation, no I/O).

**Invalidation:** When a flush introduces new distinct values for a field, insert them into the set and publish via `ArcSwap::store()`. Single writer (flush thread), many readers (query threads). Same pattern as the rest of the ArcSwap architecture.

### Why This Beats Alternatives

| Approach | Verdict | Reason |
|---|---|---|
| **Positive Existence Set** | **Best** | Exact, bounded (= distinct count), <20ns, trivial invalidation |
| Bounded negative cache + generation | Second best | Works but unbounded if queries reference arbitrary u64s; needs eviction policy |
| Bloom filter for known-missing | **Rejected** | False positives on "known-missing" = correctness bug (skip disk for value that exists) |
| Unbounded HashSet of misses | **Rejected** | Millions of unique nonexistent IDs → unbounded memory growth |

### Where to Get the Key List

The `.fpack` file header already contains the list of value IDs stored in each bucket. During initial lazy-load setup (or eagerly at startup), scan `.fpack` headers to collect all existing keys without loading any bitmap payloads. This is a metadata-only read — fast and small.

### Implementation

1. Add `existing_keys: HashMap<Arc<str>, Arc<ArcSwap<FxHashSet<u64>>>>` to `ConcurrentEngine`
2. On startup / first field access: scan `.fpack` headers to build initial key set
3. In `ensure_fields_loaded()`: check `existing_keys` before disk lookup
4. On flush with new distinct values: insert into set, publish new `Arc`
5. Zero-result queries now short-circuit in <20ns instead of 30-50ms disk I/O

**Impact on workload:** Eliminates 379 × 30-50ms = 11-19 seconds of cumulative wasted I/O per workload pass. At c=1, this directly improves p99 tail latency.

---

## Non-Goals

- **Memory budget / hard cap.** Not needed. Idle eviction is self-regulating.
- **LFU / frequency-based eviction.** Over-engineered. Idle time is simpler and handles the same workload pattern (hot/cold split).
- **Eviction of sort bitmaps.** Sort layers are always needed for the full dataset and can't be partially loaded by value.
- **Eviction of single_value / boolean fields.** Low cardinality, always cheap to keep resident.
