# Performance Tuning Roadmap

Current state after the March 13-16 optimization sessions. These are the remaining opportunities ordered by estimated impact.

## Current Performance (105M records)

| Tier | Latency |
|------|---------|
| Hot cache (memory) | 12-16μs |
| Warm cache (disk shard restore) | ~337μs |
| Cold miss (full filter + sort + cache seed) | 37-70ms broad, 1.6ms sparse |
| Eager field loading (startup) | ~1.4s total (parallel) |

## High Impact

### 1. staging.clone() cascade on lazy load publish

**Problem:** When a lazy-loaded field is published via ArcSwap, the `staging.clone()` triggers an Arc refcount cascade. `Arc::make_mut()` on the next mutation deep-clones all FilterField HashMaps. Single lazy field load can cost 5+ seconds in publish overhead.

**Current mitigation:** Loading mode skips publishing during bulk inserts. Eager loading avoids lazy loads for common fields.

**Fix options:**
- **Delta publishing**: Instead of cloning the entire InnerEngine, publish only the changed field. Requires restructuring how ArcSwap snapshots work — currently it's all-or-nothing.
- **Field-level ArcSwap**: Each field gets its own ArcSwap instead of one for the entire InnerEngine. Lazy loads swap one field without touching others.
- **Copy-on-write InnerEngine**: Use a persistent data structure (like `im::HashMap`) for the field maps so cloning is O(1) structural sharing instead of deep copy.

**Estimated improvement:** Eliminates the 5s lazy load cliff entirely.

**Complexity:** High — touches the core concurrency architecture.

### 2. Dynamic cache warming

**Problem:** First query for any new filter+sort combination pays 37-70ms cold miss.

**Fix:** `POST /api/indexes/{name}/warm` endpoint (built) + external agent that analyzes traffic patterns and pre-warms on startup. See `docs/design/dynamic-cache-warming.md`.

**Estimated improvement:** Eliminates cold misses for the top ~50 query patterns.

**Complexity:** Low for the endpoint (done). Medium for the analysis agent.

### 3. Config serialization roundtrip

**Problem:** The server rewrites `config.json` and drops fields with default values (like `eager_load: false`) because `#[serde(default)]` is set without `#[serde(skip_serializing_if)]`. Manual config edits get lost on next save.

**Fix:** Add `#[serde(skip_serializing_if = "is_default")]` or always serialize all fields. Alternatively, separate the on-disk config (user-editable) from the runtime config (server-managed).

**Estimated improvement:** Prevents config drift, reduces ops friction.

**Complexity:** Low.

## Medium Impact

### 4. fused() clone on first Eq clause

**Problem:** The first filter clause in `compute_filters` needs an owned bitmap for the accumulator. `fused()` clones the base bitmap (~11ms for nsfwLevel at 28M bits). Subsequent clauses use the reference-AND fast path and avoid cloning.

**Fix:** Use `fused_cow()` for the first clause too, and restructure the accumulator to work with a `Cow<RoaringBitmap>` — borrow when possible, own when needed for AND.

**Estimated improvement:** ~11ms → ~1ms for the first clause (saves one 28M-bit bitmap clone).

**Complexity:** Medium — requires changing the accumulator pattern in `compute_filters`.

### 5. Parallel existence set loading

**Problem:** Existence sets for multi_value fields load sequentially on startup (~2s for 5 fields). Each reads fpack headers to build a HashSet of known keys.

**Fix:** Load existence sets in parallel with rayon (same pattern as parallel fpack loading).

**Estimated improvement:** 2s → ~0.5s startup.

**Complexity:** Low.

### 6. Per-value stampede protection

**Problem:** Concurrent queries for the same missing multi_value key (e.g., tagIds=42) can both trigger disk reads. No in-flight sentinel for per-value loads.

**Fix:** Add an in-flight set (like `pending_filter_loads` but per value) that prevents duplicate disk reads. Second requester waits or proceeds without cache.

**Estimated improvement:** Prevents wasted I/O under concurrent traffic for cold values.

**Complexity:** Low-medium.

## Low Impact (Nice to Have)

### 7. Shard compression

**Problem:** .ucpack files are uncompressed. The reactionCount shard is 53KB.

**Fix:** zstd compression on shard write, decompression on read. At these sizes (<100KB), compression adds microseconds.

**Estimated improvement:** Marginal — shards are already small. Would matter more if caches grow to MB scale.

**Complexity:** Low.

### 8. Merge interval tuning

**Problem:** Cache persistence writes every 5s (merge_interval_ms). A new cache entry created at t=0 isn't persisted until t=5. If the server crashes in that window, the entry is lost.

**Fix:** Reduce merge_interval_ms or add a flush-on-demand for important cache entries.

**Estimated improvement:** Faster persistence of new cache entries.

**Complexity:** Low (config change), but tradeoff: more disk writes.

### 9. Parallel preload batch

**Problem:** `preload_eager_fields` loads sort fields first, then filter fields. Could load both in one parallel batch.

**Fix:** Combine sort and filter loading into a single `std::thread::scope` block.

**Estimated improvement:** Maybe 200-400ms off startup if NVMe can saturate more parallel reads.

**Complexity:** Low.

### 10. Zero-copy bitmap format

**Problem:** `RoaringBitmap::deserialize_from()` copies bitmap data from the file buffer into heap-allocated containers. This is 82% of load time.

**Fix:** Memory-mapped I/O with a zero-copy roaring format — bitmaps reference the mmap'd file directly. Roaring-rs doesn't support this natively; would require a custom format or contributing upstream.

**Estimated improvement:** Could make all bitmap loading nearly instant (just page faults on access).

**Complexity:** Very high — custom bitmap format, careful lifetime management.

## Already Optimized (Don't Touch)

These were the major wins from the March 13-16 sessions:

| Optimization | Before | After | Speedup |
|---|---|---|---|
| Planner string resolution | 5,198ms sort seed | 4.7ms | 1,100x |
| Double sort traversal elimination | 2x sort cost | 1x | 2x |
| Reference-AND with distributed In | 18.4ms sparse filter | 1.0ms | 18x |
| Arc-wrap time bucket bitmaps | 1-3ms snap | 3ns | 1,000,000x |
| Cache key snapping before lookup | 0% hit rate | ~100% | ∞ |
| Cursor tiebreaker range ops | 492ms | 3.6ms | 136x |
| Parallel fpack loading | 2.5s userId | 732ms | 3.4x |
| Persisted sorted_keys (ucpack v2) | 10.7ms shard restore | 337μs | 32x |
| Not-narrowing against accumulator | 44ms Not clause | 20ms | 2.2x |
| Always-snap to nearest bucket | Zero results for gaps | Correct results | — |
| Preload reorder (bounds before listen) | Cold miss on first query | 12μs cache hit | — |
