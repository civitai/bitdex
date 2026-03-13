# Architecture Risk Review: Diff-Based ArcSwap + Two-Tier redb

_Review of the proposed concurrency and storage model for memory growth, contention, and latency risks. Reviewed and refined with feedback._

---

## 1. Thundering Herd on Cold Tier 2 Cache Misses

**Scenario:** A popular tag bitmap (e.g., `tag=anime`) gets evicted from moka, then 50 concurrent readers all miss the cache simultaneously. Each loads the same bitmap from redb independently — 50 parallel deserializations of the same multi-MB bitmap.

**Severity:** Medium-high. redb read transactions are cheap but deserializing roaring bitmaps is CPU-bound. 50 copies of a 2MB bitmap = 100MB of transient allocations that all get thrown away except one.

**Fix:** Use moka's `get_with()` / `try_get_with()` instead of `get()` + manual insert. These coalesce concurrent misses for the same key — the first caller loads from redb, others block on a future and share the result. One load, one allocation. One line change, problem eliminated.

**Status:** Resolved in design. Apply during Phase A implementation.

---

## 2. Pending Mutation Drain at Merge Time

**Scenario:** Between merges (5 seconds), mutations arrive for thousands of cold Tier 2 bitmaps (tagIds alone has ~500k distinct values). At merge time, the design says "load base from redb, apply pending, write back" for every cold bitmap with pending mutations.

**Severity:** High. If 10,000 distinct tags get mutations in a 5-second window, the merge cycle loads and writes 10,000 bitmaps from/to redb. At ~1-5ms per read, that's 10-50 seconds — far longer than the 5-second merge interval. Merges pile up.

**Fix:** Drain lazily, not eagerly. Cap the pending drain to N bitmaps per merge cycle (e.g., 100). Prioritize by pending mutation count (bitmaps with the most accumulated changes first). The pending buffer is correct indefinitely — it's applied on next read or next merge, whichever comes first. There's no urgency to flush everything at once.

**Addition:** Track a metric for pending buffer depth so you can observe whether it grows unbounded over time. Unbounded growth means the drain rate isn't keeping up with write volume and the cap needs tuning.

**Status:** Resolved in design. Architectural — must be in the design before Phase A implementation.

---

## 3. Merge Cycle Blocking the Flush Thread

**Scenario:** The merge cycle runs on the flush thread. While it's compacting diffs, serializing bitmaps, and writing to redb, no new diffs can be published. A large merge (many dirty Tier 1 bitmaps + redb batch write) could take hundreds of milliseconds, stalling read freshness.

**Severity:** High (upgraded from medium). This is the most important issue. The flush thread's only job is draining the mutation channel and publishing diffs via ArcSwap — sub-millisecond, never blocks. If merge work contaminates the flush path, read freshness degrades proportionally.

**Fix:** Decouple merge from flush into separate threads:

- **Flush thread:** Drains mutation channel, applies sort layer diffs directly to bases (see issue 4 — there are few and they're fast), accumulates filter diffs, publishes via ArcSwap. Sub-millisecond, never blocks.
- **Merge thread:** Independently compacts filter diffs, writes to redb, publishes merged bases. Takes a snapshot of current diffs, works on its own copy. The flush thread keeps publishing new diffs in the meantime.

Coordination between them is minimal: the merge thread snapshots current diffs, works independently, then publishes. Readers always see the latest diffs regardless of whether a merge is in progress.

**Status:** Resolved in design. Architectural — must be in the design before implementation.

---

## 4. Sort Layer Diff Fusion Overhead (Pre-Phase D)

**Scenario:** Until bound caches land (Phase D), sort traversal walks 32 bit layers × the full alive set. Each layer with a non-empty diff requires 3 operations instead of 1: `(base & candidates)`, then `| (sets & candidates)`, then `&! clears`. At 32 layers, that's 96 bitmap operations instead of 32.

**Severity:** Medium. This is a 3× multiplier on the already-slow sort path. The sort path is what's hitting 20ms+ at 100M docs. Making it 3× slower before Phase D lands would be brutal.

**Fix:** Sort layers should never have active diffs in the read path. Merge them eagerly — the flush thread merges sort layer diffs inline before publishing. Sort layers are Tier 1 (always in memory), there are only ~160 of them (5 sort fields × 32 layers), and merging a small diff into a dense bitmap is trivial. This keeps sort traversal at 1 operation per layer, not 3.

**Rule:** Sort layer diffs get merged immediately by the flush thread. Filter diffs accumulate until the merge thread compacts them.

**Interaction with Issue 3:** This creates a clean separation of responsibilities. The flush thread handles the fast path: drain mutations, merge sort diffs into bases (cheap, ~160 bitmaps), accumulate filter diffs, publish. The merge thread handles the slow path: compact filter diffs, drain pending cold mutations, write to redb. Fast-path work stays fast. Slow-path work never blocks it.

**Status:** Resolved in design. Apply during prereq implementation.

---

## 5. Snapshot Clone Copies All Diffs

**Scenario:** Every 100ms, the flush thread publishes via `ArcSwap::store()`. The current contention-bench code does `inner.store(Arc::new(staging.clone()))`. With the diff model, `staging` contains `VersionedBitmap`s where `base` is `Arc` (cheap clone) but `diff` is owned `BitmapDiff { sets, clears }` — these are deep-cloned.

**Severity:** Low-medium. Individual diffs are small (KB), but there are 160+ sort layer diffs + all filter field diffs. At 10 publishes/second, that's meaningful allocation churn. Under heavy write load where diffs accumulate quickly, each publish clones more data.

**Fix:** Wrap diffs in `Arc` too: `diff: Arc<BitmapDiff>`. Publishing clones the Arc (pointer copy), not the bitmap data.

**Pattern:** The flush thread maintains a mutable working diff, applies mutations to it, then wraps it in a new `Arc` for publishing. The old `Arc` stays alive until readers release their Guards. Clean, no deep clone.

**Note:** With the issue 4 fix, sort layer diffs are merged by the flush thread before publishing — so sort layers publish with empty diffs regardless. The `Arc<BitmapDiff>` optimization primarily benefits filter diffs that accumulate between merge cycles.

**Status:** Resolved in design. Apply during prereq implementation.

---

## 6. Merge Blocked by Long-Running Reader Guards

**Scenario:** Merge wants to mutate a base in place (`Arc::strong_count == 1`). But a reader is holding an ArcSwap Guard from a slow query (complex multi-filter + full sort traversal, 20ms+). The strong count stays >1, so the merge can't mutate in place.

**Severity:** Low. Queries complete in microseconds to low milliseconds for cached paths. Even worst-case 20ms sort queries are brief. With N concurrent slow queries holding Guards to different generations, you might have 2-3 copies of a dense bitmap coexisting briefly.

**Fix:** Accept it. Clone-and-replace rather than waiting — waiting blocks the merge thread. One 12MB clone every few seconds is negligible compared to the CoW model which cloned on every flush cycle. Don't spin-wait on `strong_count`. Don't add complexity to avoid an already-rare event.

**Status:** No action needed. The diff model already makes this negligible by design.

---

## Priority

| # | Issue | When it Matters | Fix Complexity |
|---|---|---|---|
| 3 | Merge blocking flush | Immediately (prereq) | Medium — separate threads + coordination |
| 2 | Pending drain storm | Phase A | Low — cap + prioritize + metric |
| 1 | Thundering herd | Phase A | Low — `moka::get_with()` |
| 4 | Sort diff fusion | Prereq | Low — flush thread merges sort diffs inline |
| 5 | Diff clone churn | Prereq | Low — `Arc<BitmapDiff>` |
| 6 | Reader guard blocking merge | Prereq | None — accept clone, don't spin |

Issues 3 and 2 are architectural — they must be in the design before implementation starts. Issues 4 and 5 are straightforward implementation details that slot in naturally during prereq work. Issue 1 is a one-line fix during Phase A. Issue 6 requires no action.

---

## Flush Thread vs Merge Thread: Final Model

The interaction between issues 3 and 4 yields a clean two-thread model:

```
Flush Thread (every 100ms, never blocks):
  1. Drain mutation channel, group ops
  2. Apply sort layer mutations directly to bases (~160 bitmaps, trivial)
  3. Apply filter mutations to diffs (accumulate, wrap in Arc)
  4. Publish snapshot via ArcSwap::store()

Merge Thread (every 5s or diff threshold):
  1. Snapshot current filter diffs
  2. Compact filter diffs into bases (in-place when Arc::strong_count == 1, clone otherwise)
  3. Drain pending cold Tier 2 mutations (capped, prioritized by mutation count)
  4. Batch write all merged bases to redb
  5. Publish merged snapshot with empty filter diffs
  6. Report pending buffer depth metric
```

Sort diffs never reach readers. Filter diffs accumulate between merges but are small (KB). redb writes never block the flush path. Readers always see the latest state.
