---
status: BRAINSTORM
created: 2026-03-29
author: Bitmap Architect
context: v3-variable-size-brainstorm.md (Approach 2 selected), v3-unified-mmap-architecture.md
---

# V3 Compaction Strategies for Variable-Size mmap'd Document Store

> How do we reclaim dead space in a ~27GB append-only data file without blocking queries?

---

## The Problem

V3 variable-size document storage (the recommended Approach 2 from
`v3-variable-size-brainstorm.md`) uses two mmap'd files:

```
slot_table.dat:  108M x 12 bytes = 1.3 GB  (u64 offset, u32 length per slot)
data.dat:        ~27 GB                      (packed variable-size docs, append-only)
```

On upsert, the new doc version is appended to `data.dat` and `slot_table.dat` is
updated to point to the new location. The old doc bytes at the original offset
become dead space. At 72 ops/sec with ~230 bytes average doc size:

```
Dead space growth:  72 ops/sec x 230 bytes = 16.6 KB/sec
                    = 1.43 GB/day
                    = 5.3% of data file per day
```

Over a week without compaction: ~10 GB dead space, file grows to ~37 GB. On a
32 GB pod, this pushes mmap residency uncomfortably close to the cgroup limit
(though the OS should page out cold dead regions, the file size itself is a
concern for NVMe space on the 100 GB PV).

The compaction strategy must:

1. Not rewrite the entire 27 GB file (takes ~30s at 0.9 GB/s NVMe, blocks or degrades reads)
2. Keep mmap reads sub-microsecond (slot_table deref -> data deref, ~84ns hot)
3. Handle growth (50K new docs/day, plus updates)
4. Be crash-safe (no data loss on mid-compaction crash)
5. Minimize write amplification (don't rewrite data that has not changed)

---

## Production Context

| Parameter | Value | Source |
|-----------|-------|--------|
| Records | 107.8M, growing ~50K/day | Aidan prod metrics |
| Data file size | ~27 GB (108M x 230 bytes avg) | silo benchmarks |
| Slot table size | 1.3 GB (108M x 12 bytes) | design doc |
| Steady-state ops | 72/sec | Aidan prod metrics |
| Burst ops | 275K/sec (backfill) | Aidan prod metrics |
| Average doc size | ~230 bytes (msgpack tuples) | Ollie Phase 1 baseline |
| NVMe write ceiling | 0.9 GB/s sequential | silo benchmarks at 107M |
| Pod memory limit | 32 GB | K8s config |
| Bitmap memory | 6.5 GB | production v1.0.99 |
| Deploy frequency | ~daily (pod restart) | Aidan deploy cadence |
| Platform | Kubernetes, single replica, NVMe PV | production |

---

## Approach 1: Append + Periodic Full Repack

**How it works:** All upserts append to `data.dat`. When dead space exceeds a
threshold (e.g., 20% of file size), rewrite the entire data file: scan the slot
table, copy every live doc sequentially to a new file, rebuild the slot table,
atomic rename.

**Numbers at 108M:**
- Full rewrite: 27 GB at 0.9 GB/s = 30 seconds
- Read source (27 GB) + write dest (27 GB) = 54 GB of I/O
- Dead space at 20% threshold: 5.4 GB, triggers after ~3.6 days at 72 ops/sec
- During rewrite: reads continue against old mmap (pinned by generation). New
  mmap takes over after rename. Two copies of data file briefly coexist = ~54 GB
  disk peak.

**Crash safety:** Old file survives until rename completes. Crash mid-rewrite
loses only the new file (incomplete). Restart resumes from old file + ops log.

| Criterion | Score | Notes |
|-----------|-------|-------|
| Compaction latency | D | 30 seconds, blocks NVMe bandwidth |
| Disk growth rate | B | Bounded by threshold. 5.4 GB max dead before trigger. |
| Implementation complexity | A+ | Simplest possible. ~50 lines of code. |
| Crash safety | A | Atomic rename. Old file is always valid. |
| Read path impact | A | Zero impact. Readers on old mmap until swap. |
| Write amplification | F | Rewrites all 27 GB to reclaim ~5.4 GB dead. 5:1 amplification. |
| Disk peak | C | 54 GB (two full copies) during compaction. |

**Verdict:** The baseline. Simple, correct, but the 30-second rewrite and 5:1
write amplification are the cost of simplicity.

---

## Approach 2: Sharded Data Files

**How it works:** Instead of one monolithic `data.dat`, split into N shards:
`data_00.dat` through `data_31.dat`. Slot assignment: `slot_id % 32 = shard`.
Slot table entry adds a `u8 shard_id` (or it is implicit from `slot_id % N`).
Each shard compacted independently.

**Numbers at 108M, 32 shards:**
- Per-shard size: 27 GB / 32 = ~844 MB
- Per-shard rewrite: 844 MB at 0.9 GB/s = ~0.94 seconds
- Dead space per shard at 20% threshold: ~169 MB, triggers after ~3.6 days
- Round-robin compaction: compact one shard every ~2.7 hours (32 shards / 3.6 days)
- I/O per compaction: ~1.7 GB (read 844 MB + write 844 MB)

**Crash safety:** Same as Approach 1, per shard. Old shard file survives until rename.

**Slot table modification:** If `shard_id` is implicit (`slot_id % 32`), no change
to the 12-byte slot table format. The offset is relative to the shard file. Reads:
`shard_files[slot_id % 32][slot_table[slot_id].offset..+length]`.

| Criterion | Score | Notes |
|-----------|-------|-------|
| Compaction latency | B+ | ~1 second per shard. Compaction is bounded and short. |
| Disk growth rate | B | Same as Approach 1 (20% threshold), but each shard triggers independently. |
| Implementation complexity | B | 32 mmap'd files instead of 1. Implicit sharding is simple. |
| Crash safety | A | Same atomic rename pattern, per shard. |
| Read path impact | A- | Extra array index for shard selection. Negligible. |
| Write amplification | C | Still rewrites full shard (844 MB) to reclaim ~169 MB. 5:1 per shard. |
| Disk peak | A | Only 1.7 GB peak (one shard copy) vs 54 GB for full repack. |

**Verdict:** Strictly better than Approach 1 in every dimension except a tiny
read-path complexity increase. The key win is bounding compaction to ~1 second
and disk peak to ~1.7 GB.

---

## Approach 3: Generational Segments

**How it works:**
- `gen0.dat` = bulk load snapshot (never modified, ~27 GB)
- `gen1.dat` = steady-state appends (grows over time)
- Slot table points to (gen_id, offset, length). When gen1 exceeds a threshold,
  merge gen0 + gen1 into a new gen0, reset gen1.
- Multiple active generations: gen0 (base), gen1 (recent writes), gen2 (older writes not yet merged).

**Numbers at 108M:**
- gen0: 27 GB (initial load)
- gen1 after 1 day: ~1.43 GB (dead space) + ~1.43 GB (new versions) = ~2.86 GB
- gen1 after 1 week: ~20 GB. Merge needed.
- Merge: read gen0 (27 GB) + gen1 (20 GB), write new gen0 (27 GB) = 74 GB I/O.
  At 0.9 GB/s = ~82 seconds. Worse than full repack because gen1 must also be read.
- Alternative: compact gen1 only (remove its dead space): 20 GB, keeps only live
  overwrites. But gen0 still has dead entries (original versions of updated docs).
  Eventually gen0 must be rebuilt.

**Slot table modification:** Need `u8 gen_id` per entry (or 2 bits for 4 gens).
12 bytes -> 13 bytes = 1.4 GB slot table. Or pack gen_id into the high bits of
offset (2 bits = 4 gens, still leaves 62-bit offset = 4 exabytes addressable).

| Criterion | Score | Notes |
|-----------|-------|-------|
| Compaction latency | C | Small compactions (gen1 only) are fast. Full merge is 82s. |
| Disk growth rate | B- | gen0 dead space is never reclaimed until merge. Merge doubles disk briefly. |
| Implementation complexity | C | Multiple read paths per generation. Generation tracking. Merge state machine. |
| Crash safety | B+ | Each gen is append-only. Merge uses atomic rename. |
| Read path impact | B- | Must check gen_id, index into correct mmap. Extra branch per read. |
| Write amplification | B | gen1 compaction is efficient. gen0 merge has high amplification. |
| Disk peak | D | During merge: gen0 (27 GB) + gen1 (20 GB) + new gen0 (27 GB) = 74 GB. |

**Verdict:** Interesting idea but the math does not work out. The bulk load
snapshot (gen0) accumulates dead entries that can only be cleaned by a full merge,
which is worse than a full repack. The generational model adds complexity without
avoiding the fundamental problem: eventually, you must rewrite the base.

The one case where generations shine is if gen0 is read-only (bulk data never
updated) and only new inserts/updates go to gen1. At Civitai, this is not the
case: 72 ops/sec include updates to existing records (resources get new reactions,
images change NSFW level, etc.).

---

## Approach 4: Free-List Reuse

**How it works:** When a doc is updated, the old space (offset, length) goes on
a free list. New writes try to reuse a free region of sufficient size before
appending to the end. Exact-fit or best-fit allocation.

**Numbers at 108M:**
- Average doc: 230 bytes. Size distribution is tight (p50/p95/p99 close per Ollie).
- On update, new doc is usually ~230 bytes. Old slot is ~230 bytes. High reuse rate
  if sizes are stable.
- Free list size: at steady state, 72 ops/sec with near-perfect reuse, the free
  list stays small (< 1000 entries). Memory: ~16 KB. Negligible.
- If sizes drift (fields added over time), reuse drops. Small free regions accumulate.
  Fragmentation grows. Eventually need a full compaction anyway.

**Implementation complexity:**
- Free list sorted by size for best-fit lookup.
- Must handle coalescing adjacent free regions (otherwise 230-byte fragments
  never combine into larger regions).
- Coalescing requires tracking region boundaries — essentially a memory allocator
  for a file.
- Must persist the free list for crash recovery, or rebuild by scanning slot table
  vs data file on startup.

| Criterion | Score | Notes |
|-----------|-------|-------|
| Compaction latency | A+ | No compaction needed if reuse is high. |
| Disk growth rate | A (stable sizes) / D (drifting sizes) | Perfect when sizes match. Degrades with size variation. |
| Implementation complexity | D | Building a file allocator (fragmentation, coalescing, persistence). |
| Crash safety | C | Free list must be persisted or rebuilt. Rebuilding = full scan at startup. |
| Read path impact | A | No change. Slot table still points to (offset, length). |
| Write amplification | A+ | Overwrites dead space. Near-zero amplification. |
| Disk peak | A+ | File never grows beyond initial size if reuse is sufficient. |

**Verdict:** Beautiful in theory, fragile in practice. Civitai's tight size
distribution makes this appealing — near-perfect reuse for updates of similar-size
docs. But the moment doc sizes change (new fields added in a schema migration,
tags list grows, URL format changes), the allocator fragments and you need a
fallback compaction strategy anyway.

The allocator itself is non-trivial. We would be writing malloc for a file.
Given that the sharded approach (Approach 2) achieves ~1 second compaction
with vastly less code, the complexity budget is not justified.

**Potential as a hybrid:** Use free-list reuse as an optimization ON TOP of
sharded compaction. If a free region of sufficient size exists in the same shard,
reuse it. Otherwise append. This reduces dead space growth without the full
allocator complexity (no coalescing needed — just track freed regions).

---

## Approach 5: Page-Granular Compaction

**How it works:** Divide `data.dat` into fixed-size pages (e.g., 64 KB). Track
liveness per page (what fraction of bytes are live). When a page drops below
a threshold (e.g., 50% live), compact it: move live docs to a fresh page, update
slot table entries, free the old page.

**Numbers at 108M, 64 KB pages:**
- Total pages: 27 GB / 64 KB = ~421,875 pages
- Docs per page: 64 KB / 230 bytes = ~278 docs per page
- Page tracking: 421K pages x 8 bytes (live_bytes + doc_count) = ~3.4 MB
- Compaction unit: 64 KB read + 64 KB write = 128 KB per page compaction.
  At 0.9 GB/s = ~0.14 ms. Imperceptible.
- Pages to compact per day: 72 ops/sec = ~6.2K updates/day. If updates are
  uniformly distributed across pages, each page gets ~0.015 updates/day.
  Very slow page degradation. At 50% liveness threshold, a page with 278 docs
  needs 139 deletions/updates to trigger — ~25 years of uniform updates.
- If updates are skewed (popular images updated frequently), hot pages compact
  sooner. But most pages remain pristine.

**The boundary problem:** A 230-byte doc does not align to a 64 KB page
boundary. Docs span pages. A doc starting at byte 65,500 occupies bytes in
page N (36 bytes) and page N+1 (194 bytes). Compacting page N must handle
cross-boundary docs — either forbid them (waste space at page boundaries),
or track which docs span which pages (additional metadata).

**Forbidding boundary spans:** Pad docs to not cross boundaries. At 230-byte
avg docs and 64 KB pages, this wastes up to 229 bytes per page at the boundary
= 0.35% waste. Acceptable. But requires checking `if offset + length > page_end`
on every append and inserting padding.

| Criterion | Score | Notes |
|-----------|-------|-------|
| Compaction latency | A+ | Sub-millisecond per page. Best possible granularity. |
| Disk growth rate | B+ | Compact only dirty pages. Most pages stay clean forever. |
| Implementation complexity | C | Page boundary handling, per-page liveness tracking, doc-to-page mapping. |
| Crash safety | B | Compact to new page, update slot entries, free old page. Crash = old page survives. |
| Read path impact | A | No change. Slot table still absolute (offset, length). |
| Write amplification | A | Rewrite only 64 KB at a time. Only pages with dead space. |
| Disk peak | A+ | 64 KB overhead per compaction. Negligible. |

**Verdict:** Elegant and efficient, but the complexity of page boundary handling
and per-page liveness tracking is significant. The math also reveals a key
insight: at 72 ops/sec with uniform distribution, individual pages barely degrade.
Compaction at this granularity is solving a problem that manifests extremely
slowly per-page. The sharded approach (Approach 2) with 32 shards already bounds
compaction to ~1 second per shard, which is "good enough" without the page
tracking machinery.

Where page-granular compaction shines: if updates are extremely skewed (a few
thousand images get updated constantly). In that case, a few pages accumulate
dead space quickly and benefit from targeted compaction. But if the workload is
skewed, those hot docs also tend to be in the same shard, so sharded compaction
handles it naturally.

---

## Approach 6: Copy-on-Write Regions

**How it works:** Divide the data file into fixed-size regions (e.g., 1 MB).
Track dead space per region. On compaction: write a new region with live docs
packed, update slot table pointers, replace old region.

**Numbers at 108M, 1 MB regions:**
- Total regions: 27 GB / 1 MB = ~27,648 regions
- Docs per region: 1 MB / 230 bytes = ~4,348 docs per region
- Region tracking: 27K regions x 8 bytes = ~216 KB. Negligible.
- Compaction unit: 1 MB read + 1 MB write = 2 MB I/O. At 0.9 GB/s = ~2.2 ms.
- Slot table updates per compaction: ~4,348 entries (12 bytes each) = ~52 KB
  of random writes. At NVMe random write speed this is sub-millisecond.

**This is functionally identical to Approach 5 with larger pages.** The region
boundary problem is the same. The crash safety model is the same. The tracking
overhead is lower (27K regions vs 421K pages) but the compaction unit is larger.

| Criterion | Score | Notes |
|-----------|-------|-------|
| Compaction latency | A | ~2 ms per region. |
| Disk growth rate | B+ | Compact only dirty regions. |
| Implementation complexity | C+ | Same boundary issues as Approach 5, but fewer units to track. |
| Crash safety | A- | Old region stays until new region is fully written and slot table updated. |
| Read path impact | A | No change to read path. |
| Write amplification | A- | 1 MB per compaction. Slightly worse than page-granular for sparse dead space. |
| Disk peak | A+ | 1 MB overhead per compaction. |

**Verdict:** A coarser-grained version of Approach 5. Simpler to implement
(fewer units to track), but the fundamental analysis is the same. At 72 ops/sec,
regions degrade slowly. The extra machinery over sharded compaction is not
clearly justified.

---

## Approach 7: Buddy Allocator / Size-Class Regions

**How it works:** Divide the data file into size-class zones (128B, 256B, 512B,
1KB, 4KB). Docs go into the smallest zone that fits. Updates within the same
size class overwrite in place. Docs that change size class are relocated.

**Numbers at 108M, Civitai docs (~230 bytes avg):**
- Most docs fall in the 256-byte class. 108M x 256 = 27.6 GB zone.
- Internal fragmentation: 230/256 = 10% waste. Better than 512-byte fixed slots (55%).
- Updates where doc stays in 256-byte class: overwrite in place. Zero dead space.
- Updates where doc exceeds 256 bytes: relocate to 512-byte zone. Old 256-byte
  slot goes on a free list within the zone.

**This is the slab allocator from `v3-variable-size-brainstorm.md` Approach 5,
already evaluated and eliminated.** Quoting that analysis: "Strictly worse than
[slot table + packed data] for general-purpose use. The slab model adds complexity
without reducing the indirection that the slot table already handles cleanly."

| Criterion | Score | Notes |
|-----------|-------|-------|
| Compaction latency | A (within-class) | In-place overwrites need no compaction. |
| Disk growth rate | A (stable sizes) / C (varying) | Same as free-list: degrades with size variation. |
| Implementation complexity | D- | Multiple zones, free lists per zone, cross-zone relocation. |
| Crash safety | C | In-place overwrites need careful ordering. Cross-zone moves need atomicity. |
| Read path impact | B | Must determine size class to find the right zone file. Extra indirection. |
| Write amplification | A (in-place) | No amplification for same-class updates. |
| Disk peak | B | Multiple zone files, each potentially oversized. |

**Verdict:** Eliminated. Same conclusion as the variable-size brainstorm.

---

## Approach 8: Accept Growth + Repack on Restart

**How it works:** Do nothing. Dead space accumulates. On pod restart (which
happens roughly daily for deploys), repack the data file as part of startup.

**Numbers:**
- Dead space per day: 1.43 GB (72 ops/sec x 230 bytes x 86400 sec)
- File growth: 27 GB -> 28.4 GB after one day. 5.3% growth.
- Weekly without restart: 27 GB -> 37 GB. 37% growth.
- Repack on startup: 27 GB rewrite at 0.9 GB/s = 30 seconds added to boot time.
- Current boot time: ~200 us mmap + 22 seconds lazy bitmap load (V2) or
  ~200 us mmap (V3). Adding 30 seconds is a significant regression for V3
  but comparable to V2's lazy load time.

**The critical insight:** At daily deploys, max dead space is 1.43 GB (~5.3%).
This is less than the 20% compaction threshold of Approach 1. The file barely
grows between restarts. If deploys stay daily, compaction is essentially free
because the pod restart already creates a natural compaction window.

**Risk:** If deploy frequency decreases (e.g., stable period with no releases),
dead space grows. After a week: +10 GB. After a month: +43 GB (file doubles).
This requires either manual compaction or an admin endpoint to trigger repack.

| Criterion | Score | Notes |
|-----------|-------|-------|
| Compaction latency | A+ (steady state) / D (restart) | Zero during operation. 30 seconds on boot. |
| Disk growth rate | B+ (daily restart) / D (no restart) | 5.3%/day. Fine with daily restarts. Unbounded without. |
| Implementation complexity | A++ | Zero code. Do nothing. |
| Crash safety | A++ | Nothing to crash. |
| Read path impact | A++ | No compaction, no impact. |
| Write amplification | A++ (steady state) / F (restart) | Zero during operation. Full rewrite on boot. |
| Disk peak | B+ | 30 GB peak after 1 day. Manageable on 100 GB PV. |

**Verdict:** Surprisingly viable given the deploy cadence. The question is
whether to rely on operational behavior (daily deploys) as an architectural
assumption. If the answer is yes, this is the simplest possible approach. If
the answer is no, it needs a fallback.

---

## Approach 9: Sharded Data Files + Incremental Compaction (Novel Hybrid)

**How it works:** Combine Approach 2 (sharded files) with a lightweight
background compactor that compacts ONE shard when its dead space exceeds a
threshold. The janitor thread (already planned in V3 architecture) handles this
alongside bitmap compaction.

```
data/v3/docs/
  shard_00.dat  ... shard_31.dat     (32 data shards, ~844 MB each)
  slot_table.dat                      (1.3 GB, 12 bytes/slot)
  shard_meta.json                     (per-shard dead_bytes counter)
```

**Write path:**
1. Compute shard: `shard_id = slot_id % 32`
2. Append new doc to `shard_{shard_id}.dat`
3. Update `slot_table[slot_id]` with new (offset, length)
4. Increment `shard_meta[shard_id].dead_bytes` by old doc's length

**Compaction (per shard):**
1. Janitor checks: `if shard.dead_bytes > shard.file_size * 0.20`
2. Create `shard_{id}.dat.new`
3. Scan slot_table for all slots where `slot_id % 32 == shard_id`
   AND `alive_bitmap.contains(slot_id)`.
   Collect (slot_id, offset, length) tuples sorted by offset (sequential read).
4. Read each live doc from old shard, write sequentially to new shard.
   Record new offsets.
5. Update slot_table entries for this shard to new offsets.
6. Atomic rename `shard_{id}.dat.new` -> `shard_{id}.dat`.
7. Re-mmap the shard for readers.
8. Reset `shard_meta[shard_id].dead_bytes = 0`.

**Numbers at 108M, 32 shards:**
- Per-shard: 3.375M slots, ~844 MB data
- Dead space per shard per day: 1.43 GB / 32 = ~45 MB
- Threshold at 20%: 169 MB dead, triggers after ~3.8 days per shard
- Compaction time: 844 MB read + 844 MB write at 0.9 GB/s = ~1.9 seconds
- Frequency: ~8.5 shard compactions per month (32 shards / 3.8 days each)
- Average: one compaction every ~3.5 days. 1.9 seconds of I/O every 3.5 days.
- With daily deploys: compaction almost never triggers (dead space < threshold).

**Slot table update atomicity:** During compaction, slot table entries must be
updated after the new shard is written but before it is swapped in. The
janitor thread is the only writer, so no concurrency issue. Readers see a
brief inconsistency window where slot_table points to the new shard's offsets
but the old shard's mmap is still active. Two options:

- **Option A (generation pinning):** Pin readers to old generation during
  compaction. After swap, new readers see new generation. Old readers finish
  on old gen. Same pattern as V3 bitmap compaction.
- **Option B (atomic swap order):** Write new shard, mmap it, update slot_table
  entries, then unmap old shard. Since readers hold an Arc to the mmap, old
  readers complete against the old mmap even after the file is renamed/deleted
  (file stays open until last fd closes on Linux). Simpler but relies on OS
  fd semantics.

**Crash safety during compaction:**
- Step 2-4: only `shard_{id}.dat.new` is written. Original survives.
- Step 5: slot_table updates. If crash here, slot_table points to new offsets
  but new shard is not yet renamed. On restart: detect orphan `.dat.new` files,
  delete them, rebuild slot_table from the original shard (scan and match).
- Step 6: atomic rename. After this point, the new shard IS the shard.
- Alternative: write a small journal file before step 5 that records the shard
  being compacted. On startup, if journal exists, roll back slot_table entries
  for that shard. Adds ~50 lines of code but makes crash recovery deterministic.

| Criterion | Score | Notes |
|-----------|-------|-------|
| Compaction latency | A | 1.9 seconds, bounded, predictable. |
| Disk growth rate | A | Bounded by per-shard threshold. Max 169 MB dead per shard. |
| Implementation complexity | B+ | Sharding is implicit (modulo). Compaction is straightforward. Journal for crash safety. |
| Crash safety | A- | Atomic rename + journal. Deterministic recovery. |
| Read path impact | A | No change during normal operation. Brief mmap re-creation on compaction. |
| Write amplification | B | Rewrites full shard (844 MB) to reclaim ~169 MB. 5:1 per shard. But only 1.9s of I/O every 3.5 days. Amortized: ~6 MB/hour of write amplification. |
| Disk peak | A | 844 MB peak (one shard copy). |

**The write amplification concern:** 5:1 per compaction sounds bad, but the
amortized cost is what matters. At 8.5 compactions/month x 844 MB = 7.2 GB/month
of compaction writes. Steady-state appends = 1.43 GB/day x 30 = 42.9 GB/month.
Total writes = 50.1 GB/month. Without compaction: 42.9 GB/month. Compaction adds
17% write overhead. For NVMe with ~600 TBW endurance, this is decades of lifetime.

---

## Approach 10: Hybrid Sharded + Free-List (Novel)

**How it works:** Build on Approach 9 (sharded files) but add a lightweight
free-list per shard. When a doc is updated, the old region goes on the shard's
free list. New writes check the free list first.

The free list is intentionally simple: no coalescing, no splitting, no
best-fit search. Just a Vec of (offset, length) pairs per shard, filtered for
regions >= new doc size. First-fit within the shard.

**Numbers at 108M, 32 shards:**
- Free list per shard: at 72 ops/sec / 32 shards = 2.25 ops/shard/sec.
  If 80% of updates reuse a free region (Civitai's tight size distribution),
  free list stays under ~500 entries. Memory: 12 bytes x 500 = 6 KB per shard.
  Total: 192 KB. Negligible.
- Dead space growth with 80% reuse: 0.2 x 1.43 GB/day = 286 MB/day.
  At 20% threshold per shard: ~3.4 GB total before any shard triggers.
  Time to first compaction: ~12 days (vs 3.8 days without reuse). 3x longer.
- With 95% reuse (very tight size distribution): 71.5 MB/day dead.
  First compaction: ~47 days. Effectively never triggers with daily deploys.

**Why not full free-list (Approach 4)?** This hybrid avoids the full allocator
complexity by keeping sharded compaction as the fallback. The free list is
best-effort: if no suitable region exists, append. No coalescing. No
fragmentation crisis — sharded compaction cleans up periodically. The free list
just reduces how often compaction triggers.

| Criterion | Score | Notes |
|-----------|-------|-------|
| Compaction latency | A+ | Rarely triggers (12+ days with 80% reuse). |
| Disk growth rate | A+ | Near-zero with high reuse. Bounded by shard compaction otherwise. |
| Implementation complexity | B | Shard compaction + simple Vec per shard. ~150 extra lines. |
| Crash safety | A- | Same as Approach 9. Free list rebuilt from slot_table scan on startup. |
| Read path impact | A | No change. |
| Write amplification | A | Reuse eliminates most dead space. Residual handled by shard compaction. |
| Disk peak | A | 844 MB max (rare shard compaction). |

**The question:** Is the free list worth 150 lines of code? If Civitai's doc
size distribution is truly tight (p95/p99 within 20% of p50), reuse is very
high and the free list meaningfully delays compaction. If doc sizes vary more
than expected, the free list helps less and shard compaction handles it anyway.

---

## Ranking

| Rank | Approach | Why |
|------|----------|-----|
| 1 | **9: Sharded + Incremental (Recommended)** | Best balance of simplicity, bounded compaction, and crash safety. 1.9 seconds per compaction, one shard at a time, janitor-driven. Matches V3's unified ops log + janitor pattern. |
| 2 | **8: Accept Growth + Restart Repack** | If daily deploys continue, this is free. Zero code. But fragile — depends on operational cadence. Use as the Phase 1 strategy before building Approach 9. |
| 3 | **10: Sharded + Free-List Hybrid** | Strictly better than #1 if doc sizes are tight. But adds complexity for a marginal improvement (3.8 days -> 12 days between compactions). Defer as a post-ship optimization. |
| 4 | **2: Sharded Data Files (simple repack)** | Same as #1 without the janitor integration. Fine as a manual-trigger approach. |
| 5 | **5: Page-Granular Compaction** | Theoretically optimal (sub-ms compaction units) but page boundary handling adds significant complexity for a workload that barely degrades per-page. |
| 6 | **6: CoW Regions** | Coarser Approach 5. Same analysis, slightly simpler, slightly less precise. |
| 7 | **1: Full Repack** | The baseline. Simple but 30 seconds is too long and 54 GB disk peak is tight on 100 GB PV. |
| 8 | **3: Generational Segments** | Does not solve the problem — base generation still accumulates dead space. Full merge is worse than full repack. |
| 9 | **4: Free-List Only** | Beautiful theory, fragile practice. Building a file allocator is not justified when shard compaction achieves the same goal in 50 lines. |
| 10 | **7: Buddy/Size-Class** | Already eliminated in v3-variable-size-brainstorm.md. Strictly worse than slot table. |

---

## Recommendation

### Phase 1: Ship with Approach 8 (Do Nothing)

At daily deploy cadence, dead space is 1.43 GB/day = 5.3%. The pod restarts
before it matters. On startup, the bulk dump pipeline rewrites the data file
from scratch (it re-processes CSVs), so there is no accumulated dead space
across restarts.

**Cost:** Zero lines of code.
**Risk:** If deploys stop for a week, file grows by ~10 GB. Acceptable on a
100 GB PV (37 GB total, well under limit).
**Escape hatch:** Add an admin endpoint (`POST /admin/compact`) that triggers
a full repack on demand. ~50 lines of code. Covers the "no deploy for a week"
scenario.

### Phase 2: Build Approach 9 (Sharded + Incremental) When Needed

Build the janitor-driven shard compaction when any of these triggers occur:

1. Deploy frequency drops below weekly (dead space > 10 GB)
2. PV usage exceeds 70% (headroom concern)
3. Steady-state ops increase beyond 200/sec (3.9 GB/day dead space)
4. A second deployment or customer with different doc size distributions arrives

The sharded compaction integrates naturally with V3's janitor thread. It follows
the same pattern as bitmap compaction: threshold check, generation pin, rewrite,
atomic swap. Implementation estimate: ~200 lines in `src/v3/janitor.rs`.

### Phase 3: Evaluate Free-List (Approach 10) If Compaction Is Too Frequent

If sharded compaction triggers more than once per day (indicating higher ops
volume or larger doc sizes), add the simple free-list per shard. This delays
compaction by 3-5x with minimal code (~150 lines). Only worth doing if the
measurement shows compaction is a real operational burden.

---

## What This Analysis Assumes

1. **Doc size distribution is tight.** If Civitai docs have high variance (e.g.,
   some docs are 50 bytes, others are 2 KB), the free-list reuse rate drops and
   shard compaction triggers more often. Measure the actual distribution before
   committing to Phase 2 design.

2. **Updates are somewhat uniformly distributed across slots.** If a small set
   of slots are updated disproportionately (e.g., trending images), dead space
   concentrates in a few shards. Sharded compaction handles this naturally
   (hot shards compact more often), but the per-shard threshold may need tuning.

3. **NVMe write bandwidth is not contended during compaction.** If the janitor
   thread compacts a shard while bulk backfill is running (275K ops/sec),
   the 1.9 seconds could stretch to 5-10 seconds. The janitor should skip
   compaction during bulk operations (same `is_loading_mode()` check used
   elsewhere).

4. **Daily deploys continue.** If the operational model changes (e.g., long-lived
   pods with no restarts), Phase 2 becomes necessary sooner.

---

## Scaling Projections

| Records | Shard Size (32) | Dead/Day | Compaction Time | Days to Trigger (20%) |
|---------|----------------|----------|----------------|----------------------|
| 10M | 80 MB | 134 MB | 0.18s | 0.12 days |
| 100M | 800 MB | 1.34 GB | 1.8s | 1.2 days |
| 500M | 4 GB | 6.7 GB | 8.9s | 5.9 days |
| 1B | 8 GB | 13.4 GB | 17.8s | 11.8 days |

At 500M+ records, shard compaction time approaches 10 seconds. Options:
- Increase shard count to 128: 500M / 128 = ~1 GB/shard, 2.2s compaction.
- Accept 9 seconds (still bounded, still background, happens every 6 days).
- The shard count should be configurable, defaulting to 32.

At 1B records with 72 ops/sec, dead space per shard per day is only 418 MB
(13.4 GB / 32). Each 8 GB shard takes 12 days to hit 20% threshold. Compaction
is rare. The per-shard compaction model scales well because dead space growth
is constant (determined by ops/sec, not dataset size) while shard size grows
linearly with dataset size, so the time-to-threshold grows linearly too.

---

## Appendix: Interaction with V3 Ops Log

The V3 architecture (`v3-unified-mmap-architecture.md`) specifies an ops log
per-file for crash recovery. For document storage, the ops log records upserts:

```
[u8 op_type = UPSERT]
[u32 slot_id]
[u32 doc_length]
[doc_length bytes: new doc]
[u32 crc32]
```

On crash recovery, replay the ops log against the data file + slot table.

**Compaction interaction:** After shard compaction, the ops log for that shard
is truncated (all ops are now reflected in the snapshot). The janitor writes
the new shard, updates the slot table, then truncates the ops log. If it
crashes between shard write and ops log truncation, the ops are replayed
on restart — they are idempotent (re-append doc at same offset).

**The ops log is separate from dead space tracking.** The ops log records
what happened (for crash recovery). Dead space tracking records how much
space is reclaimable (for compaction triggering). Both are maintained by
the janitor. The ops log is per-file. Dead space counters are per-shard
(stored in `shard_meta.json` or in-memory with periodic flush).
