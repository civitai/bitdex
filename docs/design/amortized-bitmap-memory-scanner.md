---
status: APPROVED
updated: 2026-03-28
---

# Amortized Bitmap Memory Scanner

## Problem

The `bitmap_memory` metric group calls `serialized_size()` on every bitmap across every field on each Prometheus `/metrics` scrape. At 107M records with tagIds having 31K+ distinct values, this takes **52 seconds** — blocking the HTTP handler and effectively killing the server.

The current workaround is `enabled_metrics: []` in production, which disables bitmap memory reporting entirely. This means operators have no visibility into per-field bitmap memory consumption.

## Root Cause

Roaring bitmap `serialized_size()` iterates the bitmap's internal container list to compute the exact byte size. This is O(containers), not O(1). With 31K bitmaps in tagIds alone, each potentially containing thousands of containers at 107M scale, the aggregate cost is prohibitive on the scrape path.

Moving the computation to the flush thread doesn't help — it would stall flush cycles, which are on the query-serving hot path (50us interval).

Delta tracking on mutation also doesn't work because roaring compresses internally — a single bit flip can trigger container type changes (array to bitset to run-length) that unpredictably change the serialized size. You can't estimate the size change from the mutation; you have to call `serialized_size()`.

## Solution: Amortized Background Scanner

Decouple bitmap memory measurement from both the scrape path and the flush path. A background scanner processes a bounded number of dirty fields per tick, updating atomic counters that the scrape handler reads instantly.

### Architecture

```
Flush Thread                    Memory Scanner Thread           Scrape Handler
    |                                 |                              |
    | -- marks field dirty -->        |                              |
    |                           [tick: 100ms]                        |
    |                           pick N dirty fields                  |
    |                           call per_field_bytes()               |
    |                           update AtomicU64 totals              |
    |                           clear dirty flags                    |
    |                                 |                              |
    |                                 |        <-- read atomics --   |
    |                                 |             (O(1))           |
```

### Components

**1. Dirty Set**
A `DashSet<String>` (or `Mutex<HashSet<String>>`) of field names that have been mutated since their last memory scan. The flush thread inserts field names after applying mutations. The scanner removes them after processing.

This naturally dedupes — if tagIds is mutated on every flush cycle, it only appears once in the dirty set.

**2. Cached Memory Totals**
Per-field `AtomicU64` values stored in a `DashMap<String, (AtomicU64, AtomicU64)>` mapping field name to (byte_count, bitmap_count). Updated by the scanner, read by the scrape handler.

Separate totals for:
- Filter fields: per-field bytes and bitmap count
- Sort fields: per-field bytes
- Slot bitmaps: single value (cheap, can stay on scrape path)

**3. Scanner Thread**
Spawned alongside the flush thread (not inside it). Runs on its own interval.

Per tick:
1. Drain up to `scan_batch_size` entries from the dirty set
2. Load the current ArcSwap snapshot (zero-cost, same as query readers)
3. For each dirty field, call `per_field_bytes()` on just that field
4. Update the atomic counters
5. Sleep until next tick

The snapshot load means the scanner reads immutable data — no lock contention with the flush thread.

**4. Initial Population**
After index load completes (same point where `startup_duration_seconds` is set), run one full scan of all fields to populate the baseline. This happens once at startup and is acceptable since the server isn't serving traffic yet.

### Runtime Configuration

All scanner parameters should be runtime-patchable via `PATCH /api/indexes/{name}/config`:

| Setting | Default | What It Controls |
|---------|---------|-----------------|
| `memory_scanner.enabled` | `true` | Master toggle. When false, scanner sleeps and scrape returns stale/zero values. |
| `memory_scanner.interval_ms` | `100` | Scanner tick interval in milliseconds |
| `memory_scanner.batch_size` | `3` | Max fields processed per tick |

These should be persisted with the index config (same as cache settings).

### Staleness Analysis

With 20 filter+sort fields, `batch_size=3`, and `interval_ms=100`:
- Worst case full scan: 20/3 * 100ms = ~700ms
- Typical case (1-3 dirty fields): 100ms
- Scrape handler: O(num_fields) atomic loads = sub-microsecond

For 15-second Prometheus scrape intervals, even worst-case 700ms staleness is negligible.

### tagIds Concern

tagIds has 31K distinct values and dominates bitmap memory (79-80%). Its `per_field_bytes()` call iterates all 31K bitmaps. At steady state this is the most expensive single-field scan.

Mitigation: the scanner processes it as a single field in one tick. If `per_field_bytes()` on tagIds takes N milliseconds, that's the scanner's cost for that tick. This happens at most once per full dirty-set cycle (~700ms), not on every flush or every scrape.

If tagIds alone proves too expensive for a single tick, we can add sub-field batching later (process 1000 bitmaps per tick within a field). But start simple — measure first.

## Changes to enabled_metrics

### Replace with disabled_metrics

Invert the current `enabled_metrics` whitelist to a `disabled_metrics` blacklist:

**Current (whitelist):** `enabled_metrics: ["bitmap_memory", "eviction_stats"]` — only listed groups are collected. Empty = nothing.

**Proposed (blacklist):** `disabled_metrics: ["bitmap_memory"]` — all groups enabled by default, only listed groups are disabled. Empty = everything on.

This is more intuitive and safer — new metric groups are automatically enabled unless explicitly opted out.

### bitmap_memory Group Behavior Change

With the scanner in place, the `bitmap_memory` group no longer iterates bitmaps on scrape. It reads cached atomics instead. The `disabled_metrics` toggle for `bitmap_memory` would disable the scanner thread entirely (it sleeps) and zero out the atomic counters.

The other two groups (`eviction_stats`, `boundstore_disk`) remain gated as before — they have their own cost profiles.

## Scrape Handler Changes

Replace the current gated `bitmap_memory_report()` call:

```rust
// BEFORE (52s at 107M):
if state.metrics_bitmap_memory.load(Ordering::Relaxed) {
    let (slot_bytes, _, _, _, _, filter_details, sort_details) =
        engine.bitmap_memory_report();
    // ... set gauges from filter_details/sort_details
}

// AFTER (sub-microsecond):
if state.metrics_bitmap_memory.load(Ordering::Relaxed) {
    // Slot bitmaps are cheap (just alive + clean), keep on scrape path
    let slot_bytes = engine.slot_bitmap_bytes();
    m.slot_bitmap_bytes.with_label_values(&[name]).set(slot_bytes as i64);

    // Filter + sort from cached scanner totals
    for (field, bytes, count) in engine.cached_filter_memory() {
        m.filter_bitmap_bytes.with_label_values(&[name, &field]).set(bytes as i64);
        m.filter_bitmap_count.with_label_values(&[name, &field]).set(count as i64);
    }
    for (field, bytes) in engine.cached_sort_memory() {
        m.sort_bitmap_bytes.with_label_values(&[name, &field]).set(bytes as i64);
    }
}
```

## Testing

1. **Unit test:** Verify dirty set population on mutation and clearing after scan
2. **Integration test:** Start engine, insert documents, verify cached totals converge to `bitmap_memory_report()` values within a few seconds
3. **Benchmark:** Compare scrape latency before/after at 1M records locally. Should drop from O(all bitmaps) to O(num_fields) atomic loads.

## Migration

1. Implement scanner + cached totals
2. Update scrape handler to read cached values
3. Add `disabled_metrics` config (parallel to `enabled_metrics` during transition)
4. Deploy with `disabled_metrics: []` (everything on, including bitmap_memory)
5. Remove old `enabled_metrics` config support in a follow-up release

## Risk

- **tagIds scan cost:** If 31K bitmaps takes >1s to size, the scanner tick will be slow. Mitigate by measuring in production and adding sub-field batching if needed.
- **Stale data during bulk load:** In loading mode the scanner should be paused (same as snapshot publishing). Memory totals update after loading completes via the initial population scan.
- **Thread overhead:** One additional thread. Minimal — it sleeps most of the time.
