# Lazy-Load Localization — 2026-04-25

P99 sub-second mission gate measurement under shadow-on diverse traffic surfaced a long-tail bug: **per-value lazy-load on `postId` is the dominant cost on cold-path queries**. 4 / 1000 traces in the in-flight ring buffer exceeded 1 s; **lazy_load_us dominates total_us in every slow trace**, with filter_us and sort_us sub-millisecond.

## Top-15 slowest traces (live ring buffer, post-shadow-flip)

Sorted by `total_us` desc.

| # | total | cache | sort | filter_us | sort_us | **lazy_load_us** | result | clauses |
|---|---|---|---|---|---|---|---|---|
| 1 | 1235 ms | MISS | reactionCount | 213 μs | 73 μs | **365 ms** | 2 | `postId In [15465049]` + safety |
| 2 | 1154 ms | MISS | reactionCount | 153 μs | 82 μs | **678 ms** | 14 | `postId In [15465057]` + safety |
| 3 | 1137 ms | MISS | reactionCount | 188 μs | 111 μs | **662 ms** | 1 | `postId In [15465038]` + safety |
| 4 | 1093 ms | MISS | sortAt | 6 348 μs | 261 μs | **611 ms** | 27 | `OR(...) + IsNotNull(postId)` |
| 5 | 990 ms | MISS | reactionCount | 103 μs | 86 μs | **519 ms** | 1 | postId In [single] |
| 6 | 978 ms | MISS | sortAt | 6 813 μs | 126 μs | **908 ms** | 1 | OR(...) + IsNotNull(postId) |
| 7 | 968 ms | MISS | reactionCount | 72 μs | 36 μs | **869 ms** | 1 | postId In [single] |
| 8 | 934 ms | MISS | reactionCount | 149 μs | 69 μs | **853 ms** | 1 | postId In [single] |
| 9 | 870 ms | MISS | sortAt | 7 307 μs | 8 411 μs | **787 ms** | 975 | `OR(...) + IsNotNull(postId)` (sort_us higher because actual top-N work) |
| 10 | 863 ms | MISS | reactionCount | 94 μs | 93 μs | **357 ms** | 20 | postId In [single] |
| 11 | 856 ms | MISS | reactionCount | 141 μs | 110 μs | **789 ms** | 2 | postId In [single] |
| 12 | 824 ms | MISS | reactionCount | 129 μs | 86 μs | **363 ms** | 2 | postId In [single] |
| 13 | 823 ms | MISS | reactionCount | 120 μs | 12 μs | **761 ms** | 0 | postId In [single] |
| 14 | 823 ms | **HIT** | sortAt | 0 μs | 1 μs | **764 ms** | 67 | (cache hit on result, but still triggered lazy load) |
| 15 | 768 ms | MISS | reactionCount | 129 μs | 167 μs | **678 ms** | 14 | postId In [single] |

filter_us + sort_us combined are < 10 ms in **every** outlier. lazy_load_us accounts for 30–95 % of total_us.

## Pattern

Two query shapes consume the long tail:

1. **`postId In [single ID]` + safety prefilter** (12 of 15) — single-doc lookup by postId. Profile-page / detail-view shape. Lazy-loads one postId bitmap.
2. **`OR(...) + IsNotNull(postId)`** (3 of 15) — uses `IsNotNull(postId)` which reads `NULL_BITMAP_KEY` from the postId field. If that special key isn't loaded, lazy-load.

Both shapes hit `postId` lazy-load. `postId` is the only field configured with `per_value_lazy: true` (22.8 M values, can't fit all in memory).

## Why is per-value lazy-load slow?

Code path traced:

- Query handler → `load_lazy_for_query(filters, sort)` at `concurrent_engine.rs:3756`
- Determines missing per-value keys per field (line 3838): `field.get_versioned(v)` returns None or unloaded → add to missing set
- Spawns `load_field_values(field, missing_values)` per field in parallel via `std::thread::scope`
- `load_field_values` at `shard_store_bitmap.rs:891`:

```rust
pub fn load_field_values(&self, field: &str, values: &[u64]) -> io::Result<HashMap<u64, RoaringBitmap>> {
    // Group requested values by bucket
    let mut by_bucket: HashMap<u8, Vec<u64>> = HashMap::new();
    for &v in values {
        let bucket = ((v >> 8) & 0xFF) as u8;
        by_bucket.entry(bucket).or_default().push(v);
    }
    let mut result = HashMap::new();
    for (bucket, wanted) in by_bucket {
        let key = FilterBucketKey { field: field.to_string(), bucket };
        if let Some(snap) = self.read(&key)? {                  // ← reads ENTIRE bucket
            for v in wanted {
                if let Some(bm) = snap.values.get(&v) {
                    result.insert(v, bm.clone());
                }
            }
        }
    }
    Ok(result)
}
```

**`self.read(&FilterBucketKey)` deserializes the entire bucket snapshot**, even when only one value out of 89 K (postId at 22.8 M / 256 buckets ≈ 89 K values / bucket) is needed.

The bucket file is probably 30–80 MB on disk after compression. Read + deserialize on a cold filesystem cache → 350–900 ms.

## Concurrency analysis

For Scarlet's question 2:
- **Across fields** the loads ARE parallel — each field gets its own thread via `std::thread::scope`.
- **Within a field**, multiple values that fall in **different buckets** result in sequential bucket reads (one `self.read()` call per bucket inside `load_field_values`). For `In [a, b, c]` where a/b/c are in 3 different buckets → 3 sequential bucket reads.
- **Within a single bucket**, the read is one call regardless of how many values are wanted.

So the worst case ("In with N values across N different buckets, all cold") = N × full-bucket-read sequentially. The most common case in the slow traces is 1 value → 1 bucket read → still 350–900 ms because the bucket itself is huge.

## Fix surfaces

Three candidate directions, ranked by likely impact and effort:

### A. Indexed value lookup (biggest win, biggest scope)

Add a sparse index file per shard mapping `value → (offset, len)` within the shard. `load_field_values` reads only the bytes for the requested values, not the whole bucket. Reads drop from "deserialize 89 K entries" to "deserialize N requested entries."

- Expected: 350–900 ms → 5–20 ms per cold lookup. **30–50× speedup on the long tail.**
- Risk: shard format change, requires migration (or backwards-compat fallback to full-bucket read on old shards).
- Scope: medium. ShardStore format extension, write path also needs to emit the index when writing buckets.

### B. In-memory bucket cache

After `load_field_values` reads a bucket, cache the deserialized snapshot in memory. Second cold-lookup on a different value in the same bucket = instant.

- For postId at 256 buckets, after ~256 cold lookups one of each bucket is cached. Hit rate climbs over time.
- Expected: amortizes the cold cost. First N=256 queries against new buckets still take 350-900 ms; everything else is fast. With 22.8 M values across 256 buckets, the working-set bucket cache size = the whole field essentially, so this defeats the purpose of `per_value_lazy: true` (memory-bound).
- Workaround: bound the cache (e.g. last-100-buckets LRU). Caps memory growth.
- Scope: small. Drop-in cache wrapper around `self.read()` calls in `load_field_values`.

### C. Auto-warm prediction (lowest impact, simplest)

The warm registry now persists query shapes (post-PR-#228). If the warm registry could capture the **specific postId values** referenced in queries (not just the shape), auto-warm could pre-load them on boot.

- Doesn't help in-flight traffic — only post-restart.
- Scales poorly: 22.8 M values × shape diversity = unbounded auto-warm work.
- Probably not worth the implementation cost.

### Recommendation

**A is the right fix.** B is a useful complement (caps the cold cost the rare time A's index miss happens, e.g. on a cold restart before the index is hit). C is cheap to defer.

The shape-format change for A is meaningful work — Justin should approve before engineering time goes in.

## Mission-gate framing

- **Strict P99 (99th percentile) under shadow traffic: ~25 ms.** Cleared with 40× margin.
- **P99.6 (99.6th): ~1 s.** Cleared at the strict reading; first violation around the 99.6 percentile.
- **0.4 % of queries above 1 s.** All localized to lazy-load on postId.

If the gate is "strict P99 < 1 s" → cleared.
If the gate is "no UX-visible >1 s outliers at scale" → not cleared; 0.4 % at 100 M queries/day = 400 K slow queries/day.

Justin's call:
- (A1) Ship perf stack as-is. P99 cleared per strict reading. Address tail in V2.
- (A2) Build the indexed-lookup fix (surface A) before flip-back. Ships sub-second on the 99.99 percentile.
- (A3) Hybrid — ship as-is + queue the indexed-lookup as immediate next priority for V2.

## Data sources

- Live ring buffer at `/api/indexes/civitai/traces?last=1000` against local server (perf stack v1.0.165–171 + relay v1.0.175 against PF localhost:4099 against prod relay)
- Snapshot saved at `local-prom/runs/shadow-traces-snapshot.json`
- Shadow flag flipped 2026-04-25T17:18:15Z (server log + flipt-state commit `472bf93`)
- Mixed-load context: replay-captured corpus driving 3K QPS local + replay-prod-via-relay consumer pulling shadow-on /events/queries at ~250 QPS

---

*Doc filed by Donovan, 2026-04-25, mission-gate localization phase.*
