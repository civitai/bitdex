# Generational Doc Cache Eviction

## Problem

The current doc cache eviction (`doc_cache.rs:evict()`) does an O(n) scan + O(n log n) sort of ALL cached entries to find the oldest 20%. This runs inline on the query thread, blocking the requesting query for seconds at 100K+ entries.

With PR #58's conditional write-through fix, eviction triggers less often (cold pg-sync docs no longer fill the cache). But when it does trigger — under sustained query traffic that naturally fills the cache — the inline sweep still blocks queries.

## Design: Generational Buckets

Replace the flat DashMap + LRU timestamp scan with **generational buckets**. Each bucket represents a time window. Reads promote entries to the current bucket. An eviction thread drops the oldest bucket wholesale — no scanning required.

### Data Structures

```
DocCache
├── generations: ArcSwap<Vec<Arc<Generation>>>  // lock-free read access, [0]=current, [N]=oldest
└── config: DocCacheConfig

Generation
├── entries: DashMap<u32, CachedEntry>      // the actual cached docs
├── size_bytes: AtomicU64                   // byte total for this generation
└── created_at: Instant                     // when this generation was created (for merging)
```

@adam: No entry index — readers scan each generation's DashMap (hash lookup per gen). With generation count capped at ~30, that's 30 hash lookups worst case (~3μs). Negligible compared to disk I/O on a miss.

Current generation is always `generations[0]` (front of vec). Oldest is at the back.

Each `Generation` tracks its own `size_bytes` via AtomicU64. Total bytes for Prometheus = sum of all generation sizes (iterate the vec, trivial). `created_at` enables the eviction thread to merge old generations (see below).

@adam: CachedEntry simplified — `last_accessed_ms` dropped entirely. The generation IS the recency signal. CachedEntry becomes just `{ doc: StoredDoc, size_bytes: u64 }`. Removes code and per-entry overhead.

### Operations

**Read hit** (query thread):
1. Load generations vec via `ArcSwap::load()` (lock-free)
2. Scan `generations[0]` first (current — most likely hit due to temporal locality)
3. If found → return doc (no move needed, already current)
4. If not in [0], scan [1], [2], ... until found
5. If found in older generation → call `promote(slot_id, from_gen, to_gen)` to move to current
6. This is O(num_generations): a few DashMap hash lookups. Cheap.

@adam: Yes, promotion is a dedicated `fn promote(&self, slot_id: u32, from: &Generation, to: &Generation)` — removes from `from.entries`, subtracts from `from.size_bytes`, inserts into `to.entries`, adds to `to.size_bytes`. Keeps the read path clean.

**Read miss** (query thread):
1. Read doc from disk (docstore)
2. Insert into `generations[0]` (current)
3. Bump `generations[0].size_bytes`
4. No eviction check — eviction thread handles this

**Insert/update** (flush thread, conditional write-through from PR #58):
1. Scan generations for slot_id
2. If present → update doc in-place in whichever generation it's in (don't promote — writes aren't reads)
3. If not present → skip (PR #58 behavior)

**Eviction** (dedicated eviction thread):
1. Sleep for `check_interval` (e.g., 5s — just checking, not necessarily evicting)
2. Compute total bytes: sum `gen.size_bytes` across all generations (trivial)
3. If over budget → drop `generations.last()` (oldest). Repeat until under budget.
4. Rotation: push a new empty `Generation` to front every `generation_interval` (e.g., 60s)
5. Merge: if generation count exceeds `max_generations` (~30), merge the two oldest into one (see below)

@adam: Re Rust memory — yes, dropping a Generation (via `vec.pop()`) frees all entries automatically. Rust's ownership: when the `Arc<Generation>` refcount hits zero, the `Generation` is dropped, which drops the `DashMap`, which drops all `(u32, CachedEntry)` entries, which drops all `StoredDoc` data. No manual cleanup. `generations.pop()` = instant memory reclaim (once no reader holds a ref to that Arc via the old ArcSwap snapshot).

@adam: Re bucket count scaling — at 60s intervals, 5 hours = 300 generations. Each read scans all of them (300 DashMap lookups). That's ~30μs per read — at 2000 reads/sec (100 queries × 20 docs), that's 60ms/sec of overhead. Not terrible, but not great either.

**Solution: cap + merge.** Cap at `max_generations` (~30). When the eviction thread would push a new generation that exceeds the cap, it merges the two oldest generations into one first. Merging = iterate the smaller DashMap, insert each entry into the larger one, drop the smaller. This keeps scan count bounded at ~30 regardless of how long the server runs. The merged generation's `created_at` takes the older timestamp so eviction ordering is preserved.

At 30 generations × 60s = entries survive ~30 min without access before being in the oldest mergeable bucket. Combined with memory-pressure-only eviction, cold entries can survive much longer if there's room.

### Bucket Rotation (revised)

Buckets accumulate until memory pressure triggers eviction. Merging prevents unbounded growth.

```
Time 0:      [gen0: active, 50MB]
Time 60s:    [gen1: active, 0MB] [gen0: 200MB]
Time 120s:   [gen2: active, 0MB] [gen1: 180MB] [gen0: 350MB]
Time 180s:   [gen3: active, 0MB] [gen2: 150MB] [gen1: 280MB] [gen0: 400MB]
             total = 830MB < 1GB max → no eviction, keep all
Time 240s:   [gen4: active, 0MB] ... [gen0: 450MB]
             total = 1060MB > 1GB max → drop gen0 (oldest, 450MB)
             total = 610MB → under budget, stop

... after 30 min with no memory pressure:
             30 generations accumulated. Next rotation triggers merge:
             merge gen29 + gen28 → combined gen28. Count stays at 30.
```

Entries survive as long as there's memory room. Active docs get promoted to the current generation on read, so they're never in the oldest bucket when it's dropped.

### Eviction Thread

```rust
fn eviction_thread(cache: Arc<DocCache>, shutdown: Arc<AtomicBool>) {
    let check_interval = Duration::from_secs(5);
    let mut last_rotation = Instant::now();
    let gen_interval = Duration::from_secs(cache.config.generation_interval_secs);

    while !shutdown.load(Ordering::Relaxed) {
        thread::sleep(check_interval);

        // Rotate: push new generation to front periodically
        if last_rotation.elapsed() >= gen_interval {
            cache.push_new_generation();  // also merges oldest if over max_generations
            last_rotation = Instant::now();
        }

        // Evict: drop oldest generations until under budget
        while cache.total_bytes() > cache.config.max_bytes {
            if cache.generation_count() <= 1 {
                break; // Never drop the current generation
            }
            let evicted = cache.drop_oldest_generation();
            tracing::info!("doc cache: dropped oldest generation ({evicted} entries)");
        }
    }
}
```

### Read Path (lock-free via ArcSwap)

```rust
pub fn get(&self, slot_id: u32) -> Option<StoredDoc> {
    let gens = self.generations.load();  // ArcSwap::load() — lock-free, ~1ns

    // Scan from current (front) to oldest (back)
    for (i, gen) in gens.iter().enumerate() {
        if let Some(entry) = gen.entries.get(&slot_id) {
            let doc = entry.doc.clone();
            if i == 0 {
                // Already in current generation — fast path
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Some(doc);
            }
            // Promote: move from old gen to current
            let size = entry.size_bytes;
            drop(entry); // release DashMap ref before remove
            self.promote(slot_id, gen, &gens[0], size, doc.clone());
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Some(doc);
        }
    }
    self.misses.fetch_add(1, Ordering::Relaxed);
    None
}

fn promote(&self, slot_id: u32, from: &Generation, to: &Generation, size: u64, doc: StoredDoc) {
    from.entries.remove(&slot_id);
    from.size_bytes.fetch_sub(size, Ordering::Relaxed);
    to.entries.insert(slot_id, CachedEntry { doc, size_bytes: size });
    to.size_bytes.fetch_add(size, Ordering::Relaxed);
}
```

@adam: Read path is fully lock-free. `ArcSwap::load()` returns a Guard (~1ns, no refcount bump). The DashMap `.get()` is also lock-free for reads. Promotion does DashMap remove + insert which takes shard-level locks inside DashMap, but those are fine-grained and brief.

@adam: Re concurrent promotion — if two threads promote the same slot_id simultaneously, one `remove()` succeeds and the other gets None. The second thread's promotion is a no-op (entry already moved). No corruption, no duplication. DashMap handles this safely.

The eviction thread mutates the generations vec via ArcSwap: builds a new `Vec<Arc<Generation>>` (push/pop/merge), then `store()` atomically. Readers using the old vec via their Guard continue safely — they hold a ref to the old Arc. Once all readers finish, the old vec is dropped.

### CachedEntry (simplified)

```rust
struct CachedEntry {
    doc: StoredDoc,
    size_bytes: u64,
    // No more last_accessed_ms — the generation IS the recency signal
}
```

### Memory Overhead

- **Per-generation DashMap**: ~1 KB empty. Capped at ~30 = 30 KB. Negligible.
- **No entry index**: zero overhead beyond the DashMaps themselves.
- **Per-entry**: smaller than before (dropped `last_accessed_ms` AtomicU64 = -8 bytes/entry).
- **No duplication**: each doc exists in exactly one generation at any time.

### Configuration

```rust
pub struct DocCacheConfig {
    pub max_bytes: u64,                    // existing: 1 GB default
    pub generation_interval_secs: u64,     // new: 60s default (how often to rotate)
    pub max_generations: usize,            // new: 30 default (cap before merging oldest)
}
```

### Complexity Comparison

| Operation | Current (LRU scan) | Generational |
|-----------|-------------------|--------------|
| Read hit (current gen) | O(1) | O(1) |
| Read hit (promote) | O(1) | O(num_gens) scan + O(1) move |
| Read miss + insert | O(1) | O(num_gens) scan + O(1) insert |
| Eviction | O(n log n) inline | O(1) drop oldest gen, background |
| Memory overhead | ~16 bytes/entry | ~8 bytes/entry (smaller!) |

### Migration Path

1. Replace `DocCache` internals with generational structure + ArcSwap
2. Simplify `CachedEntry` (drop `last_accessed_ms`)
3. Spawn eviction thread during `ConcurrentEngine::new()`
4. Remove inline `needs_eviction()` / `evict()` calls from `get_document()`
5. Prometheus metrics: `bitdex_doc_cache_generations` (count), per-gen size via existing `size_bytes` endpoint

@adam: Agreed — this simplifies the overall code. The inline eviction path, the LRU timestamp tracking, and the `needs_eviction()` check all get removed. Net code reduction.

### Open Questions

1. **Generation interval tuning**: 60s feels right — fine-grained enough for precise eviction, coarse enough to keep generation count low. Configurable via `DocCacheConfig`. Thoughts?
2. **Max generations**: 30 default (30 min of history at 60s intervals). Higher = more scan per read but finer eviction. Lower = less scan but coarser drops. 30 seems balanced?
3. **Merge strategy**: When over `max_generations`, merge the two oldest. Alternative: merge all generations older than N into one "cold" bucket. Simpler two-at-a-time merge keeps it predictable.
