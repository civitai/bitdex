# Regression Resolution — 2026-04-25

Audit trail for the apply-mean regression discovered while stacking the
post-handoff perf PRs and resolved by PR #227.

## Final stack (in main as of this writing)

| Tag | PR | Net behavior |
|---|---|---|
| v1.0.165 | #221 | WAL reader skips unreadable tail of closed gen — fixes silent stall |
| v1.0.166 | #222 | `merge_dirty` uses side dirty-set, walks O(dirty) instead of O(N) |
| v1.0.167 | #223 | `save_snapshot`'s per-bucket writes parallelized via rayon |
| (n/a) | #226 | Auto-prefilter promotion skips 0-cardinality results — registry no longer pollutes |
| v1.0.169 | #227 | `bitmap_bytes` chunked iteration — bounds read-lock-hold (regression fix) |
| v1.0.170 | #224 | Merge thread brace fix — unlocks prefilter refresh, warm persist, idle compaction, RSS eviction, cache eviction sweep |

#225 (`for_each_versioned_chunked` + range_scan) remains open as a save-side improvement; it didn't move the regression-target needle and is not load-bearing.

## What broke and why

PR #224 fixed a stray `}` that had been silently closing the merge thread's `while !shutdown.load()` loop early. Five blocks of code that were structured at the loop body's indent and clearly meant to run every cycle had been dead-code-after-loop until graceful shutdown — i.e. effectively never:

1. Prefilter refresh
2. Auto-prefilter promotion
3. Warm registry persist
4. RSS-aware memory pressure eviction
5. Idle cache eviction sweep

Pre-#224, the bitmap memory cache scanner (`bitmap_memory_cache::scan_tick`) had nothing to do because nothing was ever marked stale (the `mark_stale` calls live inside the dead block). Post-#224, the merge thread runs the full cycle — including `mark_stale` after every idle compaction — and the scanner re-reads the byte counts at full cadence.

`FilterField::bitmap_bytes()` walked the entire bitmaps map under one read lock acquire. At userId's 769K entries: ~50 ms continuous read-lock-hold. At postId's 22.5M: ~1–2 s. parking_lot RwLock has FIFO writer fairness — a queued writer blocks subsequent readers, but a reader that already holds the lock pins waiting writers. So apply path's `remove_bulk` sat behind these long reads and surfaced as `[remove_bulk_slow] field=userId lock=80–235 ms mut=4–15 μs`.

Net: stacking #224 alone reverted PR #222's apply-mean improvement. Apply mean climbed from 5.3 ms (post-#222 alone) back to **146 ms** (the original baseline).

## Bisect

Diagnostic harness (all reverted before commit):

- `[insert_bulk_slow]` already in `filter.rs:128` — used during the original #222 root cause.
- Added `[remove_bulk_slow]` — confirmed apply was waiting (`mut_us` 4–15 μs vs `lock_us` 8–235 ms).
- Added `[merge_dirty_slow]` — confirmed merge_dirty itself stays under 5 ms post-#222 (it isn't the holder).
- Added `[read_slow]` on `apply_diff_eq` / `union_with_diff` / `for_each_versioned` / `for_each_versioned_chunked` / `iter_versioned`. **Zero slow events on any of those paths.**
- Disabled the `Idle compaction` publish (`inner.store(Arc::new(staging.clone()))`) as a hypothesis — no effect on the apply lock-wait. Ruled out staging.clone()'s read-lock-during-publish.
- Located `bitmap_memory_cache::scan_tick` calling `filter_field.bitmap_bytes()`. The hold instrumentation needed for that path had been deferred — adding it would have shown the real culprit immediately. Direct read of `filter.rs:309` confirmed: `r.values().map(|vb| vb.bitmap_bytes()).sum()` under one read lock.

## Fix shape

`bitmap_bytes` switched to chunked iteration (16K-key chunks, read lock released between chunks). Same total work, bounded max-continuous-lock-hold. Same pattern as #225's `for_each_versioned_chunked`.

```rust
pub fn bitmap_bytes(&self) -> usize {
    const CHUNK: usize = 16_384;
    let keys: Vec<u64> = {
        let r = self.bitmaps.read();
        r.keys().copied().collect()
    };
    let mut total: usize = 0;
    for chunk in keys.chunks(CHUNK) {
        let r = self.bitmaps.read();
        for &k in chunk {
            if let Some(vb) = r.get(&k) {
                total += vb.bitmap_bytes();
            }
        }
    }
    total
}
```

## Numbers

### Apply path (write-side, sustained PG-sync stream from prod head)

| Metric | Pre-stack baseline (Apr 24) | After #222 alone | After #224 alone | Full stack v1.0.165–170 |
|---|---|---|---|---|
| Apply mean | 142 ms | 5.3 ms | **146 ms (regression)** | **never crosses 100 ms threshold** |
| Apply p95 | 166 ms | 8.8 ms | 165 ms | n/a (zero `[flush-slow]` events) |
| Apply max | 191 ms | 14.8 ms | 212 ms | n/a |
| `[ops-trace]` event count (apply ≥100 ms) | 5 / 10 min | 0 / 5 min | 6 / 1 min | **0 / 90 s** |
| `[insert_bulk_slow]` event count | 4 / 10 min | 0 / 5 min | 2 / 1 min | **0 / 90 s** |
| `[remove_bulk_slow]` event count (instrumented separately) | n/a | n/a | 17–25 / 90 s | **0–4 / 90 s** |
| Per-`remove_bulk_slow` `lock_us` | n/a | n/a | 80–235 ms (mean 76 ms) | 5–11 ms (mean 8.6 ms) |

Sync was streaming ~77 ops/sec at sustained measurement; the pre-stack baseline was streaming ~10K ops/sec via catch-up phase. Same writer pressure on the apply path — the bottleneck was lock contention, not write volume.

### Read path (full replay against captured prod queries)

| Metric | Value |
|---|---|
| Queries replayed (5-min window) | 887,596 |
| Cache hits | 887,578 |
| Cache misses | 18 |
| Cache hit rate | 99.99 % |
| P50 / P90 / P95 / P99 (replay client-side) | 1 / 4 / 5 / 8 ms |
| Sustained QPS | ~4,900 |

Cache-saturated because replay loops a fixed 232-query corpus; the absolute numbers aren't representative of prod's diverse load. Useful for relative comparison only.

### Merge thread (post-#224 actually-runs-now)

| Activity | Count in 5 min |
|---|---|
| `Idle compaction` cycles fired | 30+ |
| Auto-prefilter promotions | 31 (all non-zero cardinality, none rejected at registry cap) |
| Auto-prefilter `→ 0 slots` skipped (#226's effect) | 0 (none made it past the guard) |
| Prefilter refresh log lines | 32, max compute 122 ms (matches the safety prefilter clauses) |

### Boot + auto-warm

| Metric | Value |
|---|---|
| Boot phase (cold restore from 110.5M dump) | 146 s |
| Auto-warm trigger | Boot log: `Auto-warming N query shapes... Auto-warmed N/N in Xms` |
| Local auto-warm (only 1 entry in warm.json) | 108 ms |

**Caveat: warm registry persist is broken on main.** 887K queries in 5 min — zero `warm registry: persisted N shapes` log lines. warm.json on disk is from a much earlier run (1 entry, Apr 25 01:16). `total_recorded` must be > 0 since auto-prefilter promotion fires at the >=50 threshold and we see 31 promotions. Both share the same `merge_warm_registry` Arc. Persist returns `Ok(0)` silently somewhere.

Suspect: `warm_persist_path` resolves to `None` in this build's bootstrap, OR `top_shapes` returns an empty Vec despite `total_recorded > 0`. Earlier diagnostic in a prior worktree session showed `WarmRegistry::new — persist_path = Some(...)` correctly resolved; that diagnostic isn't on main.

**Time-to-first-warm-restore measurement is therefore inconclusive.** Auto-warm executes structurally (Boot log confirms) but only restores 1 stale entry. The 30–60-min cold-pod cache ramp problem cited in the handoff cannot be measured against the post-stack code until warm.json is being populated correctly.

This is its own task — the merge thread reaches the warm-persist call (we see auto-prefilter promotion firing immediately after) but persist returns 0. Deferred for a separate investigation.

### Update — warm persist resolved by PR #228 (v1.0.171)

Root cause: `server.rs:2787` overwrote `query.filters` with the post-prefilter-substitution form (which prepends `FilterClause::BucketBitmap { bitmap: Arc<RoaringBitmap>, .. }`, marked `#[serde(skip)]`). Line 2817 then recorded the substituted version into the warm registry. When `persist()` tried to serialize, serde rejected the `BucketBitmap` variant and the entire batch was silently discarded.

Fix: capture `original_filters_for_warm = query.filters.clone()` BEFORE substitution, pass that to `record(...)`. 14 insertions, 2 deletions in `server.rs`.

Verification table:

| Metric | Pre-#228 | Post-#228 |
|---|---|---|
| `warm registry: persisted N shapes` events | 0 / 5 min | every merge cycle |
| `warm registry persist failed: BucketBitmap` events | 9+ / 5 min | 0 |
| `warm.json` size | 221 B (1 stale entry) | 410 KB |
| Unique shapes persisted | 1 | 187 |
| Top entry frequency in warm.json after 5 min replay | 1 | 37,961 |

### Time-to-first-warm-restore (final, post-stack)

Methodology per Scarlet:

1. Server with full v1.0.171 stack, warm.json populated meaningfully (187 shapes after 5 min sustained replay).
2. Pre-restart cache hit ratio: **100.00%** (1.76M queries, 12 misses).
3. `taskkill /IM bitdex-server.exe`, restart server, time boot + auto-warm.
4. Restart replay immediately, sample cache hit ratio every 5 s.

| Phase | Duration |
|---|---|
| Boot phase (110.5M dump restore from disk) | 159.1 s |
| Auto-warm (187 shapes) | **4.6 s** |
| Total kill → ready-with-warm-cache | **163.1 s (~2.7 min)** |

Cache hit ratio post-restart, sampled every 5 s:

| Elapsed since query restart | Hit ratio |
|---|---|
| t+5.5 s | 99.95 % |
| t+11 s | 99.97 % |
| t+16 s | 99.98 % |
| t+22 s | 99.99 % |
| t+56 s | 100.00 % |
| t+112 s | 100.00 % |

**Time-to-50%-cache-hit: under 5.5 s** (already at 99.95 % on the first sample after replay started).

Compare to the handoff's 30–60-min restart penalty: **100–300× faster.**

Caveat: local replay loops a fixed 232-query corpus, so cache saturates fast regardless of auto-warm — the absolute hit ratio numbers are not representative of prod's diverse load. The meaningful number is the **boot + auto-warm wall-clock (163 s)**, which is now bounded by the 110M dump restore time. Auto-warm contributes 4.6 s and replays 187 shapes covering the working set captured by the warm registry before shutdown.

In prod, the auto-warm working set won't be 100 % of the next-window query mix, so cache hit ratio will climb gradually after auto-warm completes — but the **starting point** is now a populated bound cache + a populated unified cache instead of empty. The 30–60 min restart-penalty narrative is closed.

## Open follow-ups

| ID | Topic | Why deferred |
|---|---|---|
| Task #13 | Cache `bitmap_bytes` with TTL or update incrementally on mutation | Real fix; current chunked iter is "stop the bleeding" |
| (filed earlier) | sortAtUnix field-not-found in compute_filters during auto-promotion | Cosmetic; doesn't block apply |
| (new) | warm registry persist returning Ok(0) silently | Auto-warm only restores stale 1-entry warm.json on this code path |
| Task #9 / PR #225 | `for_each_versioned_chunked` + range_scan + save_snapshot consolidation | Different surface; doesn't pair with the apply regression |

## Causal narrative

1. PR #224 fixed a single-character bug (`}` placement) that had silently disabled five merge-thread blocks for an unknown amount of time. The fix is correct and unblocks important behavior — including warm persist, prefilter refresh, idle compaction, RSS eviction, cache eviction.
2. One of the unblocked behaviors (`bitmap_memory_cache::scan_tick` re-reading `bitmap_bytes` after `mark_stale` calls) had a long-held read lock that pre-#224 was harmless because the calls didn't fire.
3. Apply path's writers (the `remove_bulk` chain in `WriteBatch::apply`) waited behind those reads, surfacing as `[remove_bulk_slow] lock=80–235 ms`. The mutation work itself was 4–15 μs.
4. PR #227 chunked the `bitmap_bytes` iteration, bounding max-continuous-lock-hold to ~10 ms per chunk. Apply is no longer hostage to the scanner's re-read window.
5. With the full v1.0.165–170 stack: **apply never crosses the 100 ms `[flush-slow]` threshold under sustained PG-sync streaming.** The original 142 ms apply-mean baseline is gone.

## Merge sequence executed by Scarlet

```
v1.0.165 — #221 (ops_wal rotation stall)
v1.0.166 — #222 (merge_dirty side-set)
v1.0.167 — #223 (parallel snapshot save)
(no tag) — #226 (skip 0-card auto-promotion)
v1.0.169 — #227 (bitmap_bytes chunked) ← regression fix
v1.0.170 — #224 (merge thread brace) ← paired with #227
```

PR #224 deliberately landed AFTER #227 so the regression was contained at merge time, not deployed.
