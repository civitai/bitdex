---
status: BRAINSTORM
created: 2026-03-29
author: Bitmap Architect (brainstorm session with Justin)
---

# V3 Variable-Size Document Storage — Design Brainstorm

> How do we support variable-size documents while keeping mmap performance?

---

## The Problem

V3's current design uses fixed 512-byte slots: `offset = slot_id * 512`. This gives
O(1) lookup, zero indirection, 6.49M writes/sec (32 threads), and 30ns hot reads.

But 512 bytes is a policy choice baked into the file format. Civitai docs average ~230
bytes (55% waste). A future dataset with 2KB docs would not fit at all. Justin's vision
is a general-purpose instant index for any data — fixed slots limit that.

**The tension:** Variable-size data requires *some* form of indirection or metadata.
Every approach trades away one of: simplicity, read speed, write speed, or memory overhead.
The question is which trade-off is cheapest at 108M+ scale.

---

## Known Constraints (from experiments + production)

| Constraint | Source |
|-----------|--------|
| K8s cgroups counts dirty mmap pages against container memory limit | deployment-saga.md: 60GB mmap OOMKilled the pod |
| TLB pressure at 107M: mmap random reads go from 7ns (1M) to 42ns (107M) | Ollie Benchmark 3 |
| NVMe sequential write ceiling: ~0.9 GB/s on current hardware | silo benchmarks at 107M |
| Civitai doc size: mean ~230 bytes, tight distribution (p50/p95/p99 close) | Ollie Phase 1 baseline |
| Production steady-state: 72 ops/sec, 275K/sec burst | Aidan prod metrics |
| 32 GB pod limit, 6.5 GB for bitmaps, need headroom | production v1.0.99 |
| Ops log + janitor compaction pattern is the V3 standard | v3-unified-mmap-architecture.md |

---

## Approach 1: Fixed Slots with Configurable Size (Current Design)

`offset = slot_id * SLOT_SIZE`, SLOT_SIZE set at index creation time.

**Memory math at 108M:**
- 512 bytes/slot: 55.3 GB file. ~55% waste for Civitai (230-byte avg docs).
- 256 bytes/slot: 27.6 GB file. ~10% waste for Civitai but cannot fit docs > 256 bytes.
- 1024 bytes/slot: 110.6 GB file. Works for larger docs but 4x waste for Civitai.

**Pros:**
- Zero indirection. Pointer arithmetic only. 30ns reads.
- Zero metadata memory. The offset formula IS the index.
- Simplest possible code. No fragmentation, no compaction of data layout.
- Bulk writes are memcpy into pre-allocated mmap region — maximum throughput.
- Compatible with ops log pattern (overwrite slot in-place during compaction).

**Cons:**
- Documents must fit in SLOT_SIZE. Hard upper bound per index.
- Waste is proportional to (SLOT_SIZE - avg_doc_size) * num_docs. At 108M this adds up.
- Changing SLOT_SIZE requires full rebuild (offline migration).
- Not general-purpose: a user must know their max doc size at index creation time.

**Verdict:** Best for known, tight distributions. Civitai is the perfect case (tight
distribution, predictable max). But fails the "any data" criterion.

---

## Approach 2: Thin Slot Table (8 bytes/slot) + Packed Data Region

Two mmap'd regions: a fixed-size slot table and a variable-size data file.

```
slot_table.dat:  [slot_0: u32 offset, u32 length] [slot_1: ...] ...
data.dat:        [doc_0 bytes][doc_1 bytes][doc_2 bytes]...
```

Lookup: `slot_table[slot_id * 8]` -> `(offset, length)` -> `data[offset..offset+length]`.

**Memory math at 108M:**
- Slot table: 108M * 8 bytes = 864 MB file. mmap'd, so only hot pages resident.
- Data file: 108M * 230 bytes (avg) = 24.8 GB. Same as the actual data — zero waste.
- Total: 25.7 GB vs 55.3 GB for 512-byte fixed slots. 53% less disk.

**Read path (two mmap derefs):**
1. Deref slot_table at `slot_id * 8` -> 8 bytes (offset, length). ~42ns (TLB pressure).
2. Deref data at `offset` for `length` bytes. ~42ns (different file, different TLB entry).
3. Total: ~84ns for a cache-hot read. 2.8x the fixed-slot 30ns but still sub-microsecond.

**Write path (bulk):**
- Data file: append-only, sequential. Same throughput as silo writes (4.77M/s BufWriter).
- Slot table: random writes (slot_id determines position). With mmap, this is memcpy to
  a pre-sized file — fast, but creates dirty pages scattered across 864MB.
- K8s cgroup concern: 864MB of potentially dirty slot table pages is manageable (unlike
  the 60GB arena that OOMKilled us). The data file is sequential so dirty pages flush
  efficiently.

**Write path (steady-state):**
- Append new doc version to data file end.
- Update slot_table[slot_id] to new (offset, length).
- Old data in data file is dead space until compaction.
- Compaction: rewrite data file sequentially, update all slot_table entries.

**Pros:**
- Any document size. No upper bound. No waste.
- Slot table is itself mmap'd — no heap memory for the index.
- Two-deref read is still sub-microsecond when hot.
- Append-only data region has excellent sequential I/O characteristics.
- Slot table has perfect spatial locality for sequential scans (8 bytes/entry, ~500 entries
  per 4KB page).

**Cons:**
- One extra deref per read (84ns vs 30ns). At 72 ops/sec production rate this adds 3.9us/sec
  total — invisible. At high QPS with include_docs on many results it adds up, but those queries
  are already dominated by bitmap ops (5-30ms).
- Compaction is harder: must rewrite data file AND update slot table atomically. Fixed-slot
  compaction overwrites in-place.
- Slot table dirty pages during bulk load: 864MB of random writes. Not catastrophic (the old
  60GB arena was 70x larger) but worth monitoring in K8s.
- Two files instead of one. Slightly more operational complexity.

**Variant 2a: u64 offsets instead of u32.**
If data file exceeds 4GB (it will at 108M * 230 bytes = 24.8GB), u32 offsets are insufficient.
Use `(u48 offset, u16 length)` for 8 bytes total — 256TB max file, 64KB max doc. Or use full
`(u64 offset, u32 length)` = 12 bytes/slot = 1.3 GB slot table. The 12-byte variant handles
arbitrarily large docs and files but costs 50% more slot table space.

**Recommendation for variant:** Use 12 bytes/slot `(u64 offset, u32 length)`. At 108M this is
1.3 GB — still manageable as an mmap'd file. The 4GB offset limit of u32 would bite immediately
for Civitai-scale data, and 64KB doc limit of the packed variant is unnecessarily restrictive
for a general-purpose engine.

With 12-byte slots: 108M * 12 = 1.296 GB slot table + 24.8 GB data = 26.1 GB total.

---

## Approach 3: Page-Aligned Variable Slots (4KB pages)

Each slot gets one or more 4KB pages. Small docs waste the remainder of the page.

```
offset = slot_id * 4096  (for single-page docs)
Large docs: slot_id's page contains a continuation pointer to overflow pages.
```

**Memory math at 108M:**
- 108M * 4096 = 442.4 GB. For 230-byte average docs this is 94% waste. Non-starter at scale.

Even if we pack multiple docs per page (e.g., 4 docs per 4KB page = 1024 bytes/doc effective),
that requires a sub-page index, which is just Approach 2 with alignment constraints.

**Pros:**
- Page-aligned reads/writes are optimal for the OS VM subsystem.
- No fragmentation within a page.

**Cons:**
- 94% waste for small docs. At 108M, 442 GB of disk for 25 GB of data.
- Overflow mechanism adds complexity without clear benefit over Approach 2.
- The 4KB minimum is 17x the average Civitai doc. For an index engine storing compact
  field tuples, this is grotesquely wasteful.

**Verdict:** Eliminated. The waste ratio is disqualifying for any dataset with sub-1KB docs,
which is the primary use case for a bitmap index engine.

---

## Approach 4: Log-Structured Merge (LSM-Style)

Append-only log for writes. Periodic compaction sorts by slot_id and writes a sorted run.
Reads check the active log, then sorted runs (newest first).

```
Write: append (slot_id, doc_bytes) to active_log.dat
Read:  check active_log (in-memory index) -> sorted_run_N -> sorted_run_N-1 -> ...
Compact: merge active_log + sorted_runs -> new sorted_run
```

**Memory math at 108M:**
- Data: ~24.8 GB (same as actual data, eventually compacted).
- In-memory index for active log: HashMap<u32, u64> for recent writes.
  At 72 ops/sec, 1K compaction threshold = ~1K entries = ~24 KB. Negligible.
- During bulk load: all 108M entries in the active log before first compaction.
  In-memory index = 108M * 12 bytes = 1.3 GB during load (same as Approach 2 slot table).

**Write path:** Sequential append. Best possible I/O pattern. Matches or exceeds silo
write throughput.

**Read path:** If the doc is in the latest sorted run, one mmap deref + binary search
over the sorted run's index. If it was recently written, check in-memory index first
(HashMap lookup, ~50ns). Worst case: check multiple runs.

**Pros:**
- Fastest writes (pure sequential append, no random I/O at all).
- Any document size.
- Natural fit with the ops log pattern V3 already uses.
- Compaction produces a sorted, packed file — optimal for sequential scans.

**Cons:**
- Read amplification: must check multiple runs until doc is found. With 72 ops/sec and
  compaction every ~14 seconds (1K ops), there is typically 1 sorted run + active log.
  So 2 lookups worst case — acceptable.
- Compaction rewrites the entire data file. At 24.8 GB, this takes ~27 seconds at
  0.9 GB/s NVMe write. During compaction, reads must handle the old + new run.
- The sorted run needs its own index (binary search over slot_ids, or a slot table).
  This converges toward Approach 2 after compaction.
- More complex than Approach 2: two different read paths (active log vs sorted run),
  compaction state machine, generation management.

**Verdict:** Functionally equivalent to Approach 2 after compaction, but with higher
steady-state complexity. The append-only write path is attractive for burst writes
(275K/sec), but the 16x mmap write speed from Experiment 2 already exceeds that.
Not worth the complexity premium.

---

## Approach 5: Slab Allocator Model

Size classes with dedicated files per class. A slot table maps slot_id to (class, offset).

```
slab_256.dat   — docs 1-256 bytes, packed at 256-byte boundaries
slab_1024.dat  — docs 257-1024 bytes, packed at 1024-byte boundaries
slab_4096.dat  — docs 1025-4096 bytes
slab_big.dat   — docs > 4096 bytes, variable-length with offset table

slot_table.dat — [slot_id * 4] -> (u2 class, u30 offset_within_class)
```

**Memory math at 108M (Civitai, ~230 byte docs):**
- Most docs fall in slab_256: 108M * 256 = 27.6 GB. ~10% waste (same as 256-byte fixed slots).
- Slot table: 108M * 4 bytes = 432 MB.
- Total: ~28 GB. Better than 512-byte fixed, worse than packed (Approach 2).

**Memory math for mixed dataset (50% 100-byte, 30% 500-byte, 20% 2KB docs):**
- slab_256: 54M * 256 = 13.8 GB
- slab_1024: 32.4M * 1024 = 33.2 GB
- slab_4096: 21.6M * 4096 = 88.5 GB
- Total: ~135 GB for 47 GB of actual data. 65% waste.
- Approach 2 would store 47 GB of data + 1.3 GB slot table = 48.3 GB. 3% waste.

**Pros:**
- Within a slab, reads are O(1) with deterministic offsets (like fixed slots).
- Low waste for datasets that cluster tightly in one size class.
- Deletes within a slab can be tracked with a free-list bitmap (roaring!) for slot reuse.

**Cons:**
- Multiple files (4+) with coordination between them.
- Slot table still needed (now with class tag), so no advantage over Approach 2.
- Waste increases sharply when doc sizes span multiple classes.
- Slab class boundaries are another configuration knob. "What size classes should I use?"
  is a question users should not have to answer.
- Compaction must handle each slab independently.
- If a doc changes size class on update (e.g., tags added), it must move between slabs
  and the slot table entry must be updated atomically.

**Verdict:** Strictly worse than Approach 2 for general-purpose use. The slab model adds
complexity without reducing the indirection that Approach 2 already handles cleanly. Only
wins if >95% of docs are the same size — and in that case, fixed slots (Approach 1) are
simpler.

---

## Approach 6: Hybrid Fixed Primary + Variable Overflow

Two regions: a fixed-size primary slot for hot fields, and a variable-size overflow region
for everything else.

```
primary.dat:   [slot_id * 256] -> 256 bytes: nsfwLevel, type, sort values, etc.
overflow.dat:  variable-length region for url, hash, full tagIds list, etc.
primary slot includes: u32 overflow_offset, u16 overflow_length (6 bytes of the 256)
```

**Memory math at 108M:**
- Primary: 108M * 256 = 27.6 GB. Always allocated, even for docs that fit entirely.
- Overflow: only for docs > 250 bytes (256 minus 6-byte pointer). If 20% of docs overflow
  with avg 200 bytes extra: 21.6M * 200 = 4.3 GB.
- Total: ~31.9 GB.

For Civitai (230-byte avg docs): most docs fit in 250 bytes with zero overflow. Maybe 10-15%
overflow for docs with long URLs or many tags. Effective waste ~10%.

**Read path:**
- Hot fields (used for filtering/sorting): single mmap deref at `slot_id * 256`. 30ns.
  These fields are already IN the bitmaps — this is redundant for filter/sort.
- Full doc (include_docs response): primary deref + conditional overflow deref. 30-84ns.

**Pros:**
- Hot path stays O(1) with zero indirection.
- Overflow is append-only with its own compaction cycle.
- Natural separation of "index fields" vs "payload fields."

**Cons:**
- **The primary slot duplicates data already in bitmaps.** Filter values are in filter
  bitmaps. Sort values are in sort layer bitmaps. The only reason to store docs at all
  is for `include_docs` responses and for upsert diffing. Neither needs O(1) latency —
  the query already took 5-30ms of bitmap operations.
- Two read paths: primary-only vs primary+overflow. Branching complexity.
- Primary slot format must be defined per-schema: which fields go in primary vs overflow?
  This is another configuration burden and makes the format schema-dependent.
- Overflow region has all the same problems as Approach 2 (indirection, compaction)
  but ALSO requires the fixed primary — strictly more complex.
- The 256-byte primary is wasted for any dataset where docs are all < 100 bytes or all
  > 500 bytes.

**Verdict:** Tempting but misguided for a bitmap index engine. The "hot fields" are already
accessible via bitmap lookup (the bitmaps ARE the index). Document storage exists for
`include_docs` and upsert diffing — both cold paths where 84ns vs 30ns is irrelevant
against the 5-30ms query time. The complexity of maintaining two storage regions with
schema-dependent field assignment is not justified.

---

## Approach 7: Chunked Slot Table (Novel)

A variation on Approach 2 that eliminates the per-slot random writes during bulk load.

```
Bulk load:
  Write data.dat sequentially (append-only, like current silos).
  Build slot_table.dat AFTER data is written, in one sequential pass.

Steady-state:
  Same as Approach 2: slot_table[slot_id] -> (offset, length) -> data[offset..].

Compaction:
  Rewrite data.dat packed, rebuild slot_table.dat from scratch.
```

The key insight: during bulk load, we do not need the slot table at all. The dump processor
knows each doc's slot_id as it writes. It can buffer (slot_id, offset, length) tuples in
memory and write the slot table in one sequential pass after the data file is complete.

At 108M: buffering 12 bytes per entry = 1.3 GB. This is identical to the silo index merge
step that already works (data-silo-architecture.md section 6). Same pattern, same cost.

For steady-state writes, the slot table is mmap'd and updated in-place (8-byte or 12-byte
write per upsert). At 72 ops/sec, this is ~864 bytes/sec of dirty pages — trivial.

**Pros over vanilla Approach 2:**
- Bulk load writes ZERO random pages to the slot table. All sequential I/O.
- Eliminates the K8s dirty-page concern during bulk load entirely.
- Steady-state random writes to slot table are negligible (72/sec).

**Cons vs vanilla Approach 2:**
- Requires 1.3 GB RAM during bulk load for buffered entries. Acceptable — it is the same
  cost as the current silo index merge step.
- Slightly more complex bulk load path (buffer + flush vs direct mmap write).

---

## Approach 8: Tiered Slot Table with Inline Small Docs (Novel)

Combine fixed-slot simplicity for small docs with Approach 2 for large docs, using the
slot table itself as storage for small documents.

```
slot_table.dat:  108M entries, each 64 bytes.
  If doc fits in 52 bytes:
    [u8 flags=INLINE] [u8 length] [up to 52 bytes of doc] [padding]  = 64 bytes
  If doc exceeds 52 bytes:
    [u8 flags=EXTERNAL] [u8 reserved] [u48 offset] [u32 length] [padding] = 64 bytes
    Actual doc in data.dat at the given offset.
```

**Memory math at 108M:**
- Slot table: 108M * 64 = 6.9 GB. Always allocated.
- Data file: only for docs > 52 bytes. At Civitai (230-byte avg), ALL docs overflow.
  Data file = 24.8 GB. Total = 31.7 GB.
- For a dataset with 40-byte docs: everything inline. Total = 6.9 GB, zero data file.

**The problem:** 64 bytes per slot is too small to inline Civitai docs (230 bytes avg).
Increasing to 256 bytes per slot to inline most Civitai docs = 27.6 GB slot table + data
file for outliers, which is just Approach 1 (fixed slots) with extra complexity for the
overflow case.

**Verdict:** Only interesting if most docs are tiny (< 50 bytes). For the target use case
(100-500 byte index field tuples), the inline threshold is too low to help and the slot
table overhead is too high. The inline optimization is not worth the branching complexity
in the read path.

---

## Ranked Recommendation

### Tier 1: Build This

**Approach 2 with Chunked Bulk Load (Approaches 2 + 7 combined)**

12-byte slot table (u64 offset, u32 length) + packed data region. Slot table built
sequentially after bulk load completes. mmap'd for steady-state reads and writes.

| Criterion | Score | Notes |
|-----------|-------|-------|
| Write throughput | A | Sequential append to data file during bulk load. Same I/O pattern as proven 4.77M/s silos. Slot table written in one pass after. |
| Read latency (hot) | A- | Two mmap derefs: ~84ns at 108M (2.8x the 30ns fixed-slot baseline). Sub-microsecond. Irrelevant against 5-30ms query times. |
| Memory overhead | A+ | Zero heap. Slot table (1.3 GB) + data (24.8 GB) both mmap'd, OS manages residency. 1.3 GB buffer during bulk load only. |
| Flexibility | A+ | Any document size up to 4 GB (u32 length). No configuration needed. |
| Simplicity | A | Two files, one indirection. Slot table is a flat array — trivial to reason about. |
| Ops log compatibility | A | Append new doc to data, update slot_table entry, log the op. Janitor rewrites data + rebuilds slot table. Same pattern as all V3 silos. |
| Disk efficiency | A+ | 26.1 GB vs 55.3 GB for 512-byte fixed slots (53% reduction for Civitai). Zero waste for the data itself. |
| K8s safety | A | Bulk load: zero dirty mmap pages (sequential BufWriter + post-build slot table). Steady-state: 72 slot_table writes/sec = ~864 bytes/sec dirty. |

**Implementation sketch:**

```rust
// Slot table: mmap'd flat array
struct SlotTable {
    mmap: MmapMut,          // 12 bytes per slot
    capacity: u32,          // max slot_id + 1
}

impl SlotTable {
    fn get(&self, slot_id: u32) -> Option<(u64, u32)> {
        let base = slot_id as usize * 12;
        let offset = u64::from_le_bytes(self.mmap[base..base+8]);
        let length = u32::from_le_bytes(self.mmap[base+8..base+12]);
        if length == 0 { None } else { Some((offset, length)) }
    }

    fn set(&mut self, slot_id: u32, offset: u64, length: u32) {
        let base = slot_id as usize * 12;
        self.mmap[base..base+8].copy_from_slice(&offset.to_le_bytes());
        self.mmap[base+8..base+12].copy_from_slice(&length.to_le_bytes());
    }
}

// Data file: append-only
struct DataFile {
    mmap: Mmap,             // read-only mmap for serving reads
    append_fd: File,        // append-only fd for writes
    tail: AtomicU64,        // next write offset
}
```

**Compaction strategy:**
1. Scan slot table for all live entries (alive bitmap tells us which slots are live).
2. Read each live doc from data file, write sequentially to new data file.
3. Build new slot table from the sequential write offsets.
4. Atomic swap: rename new files over old files.
5. Frequency: when dead space exceeds 20% of data file (configurable).

At steady state (72 ops/sec, ~16KB/sec of new data), dead space grows at ~16KB/sec.
At 24.8 GB data file, 20% threshold = 5 GB dead space = ~86 hours before first compaction.
Compaction rewrites 24.8 GB at 0.9 GB/s = ~28 seconds. Acceptable.

**Concurrency:**
- Reads: mmap is read-only after bulk load. Multiple readers, no coordination.
- Writes: single mutation thread appends to data file, updates slot table. Same as V3's
  ops log pattern. Readers see stale slot_table entries until the write completes — this
  is fine because the ops log + in-memory buffer provides read-after-write consistency.
- Compaction: generation pinning (same as bitmap compaction). Readers on old gen see old
  files. New gen takes over atomically.

### Tier 2: Keep as Fallback

**Approach 1: Fixed Slots (Current Design)**

If benchmarking shows the 84ns two-deref read is a problem for some workload we have not
anticipated, fixed slots remain the simplest option. The 55% waste is acceptable for Civitai
specifically. Make SLOT_SIZE configurable at index creation time (already planned).

The key advantage of fixed slots is that compaction is trivial (overwrite in-place) and
there is no slot table to manage. For single-dataset deployments where doc size is known,
this is the right choice.

**Consider offering both:** fixed-slot mode for known-size deployments, slot-table mode
for general-purpose. The V3Engine selects the storage backend based on config. Two
implementations, same trait interface.

### Tier 3: Interesting but Not Worth the Complexity

**Approach 4 (LSM):** Converges to Approach 2 after compaction. The append-only write
path is elegant but the read amplification and compaction state machine add complexity
without measurable benefit given that mmap bulk writes already hit 6.49M/s.

**Approach 6 (Hybrid):** Solves a problem that does not exist. The "hot fields" are in
bitmaps. Document reads are cold-path only.

### Tier 4: Eliminated

**Approach 3 (4KB pages):** 94% waste. Non-starter.
**Approach 5 (Slabs):** Strictly worse than Approach 2. More files, more complexity, more waste.
**Approach 8 (Inline):** Inline threshold too low for the target doc size range.

---

## The 84ns Question

The core objection to Approach 2 is that reads go from 30ns (one deref) to ~84ns (two
derefs). Is this a real concern?

**At production query volume (72 ops/sec):**
- Extra 54ns per doc read = 3.9 us/sec total. Not measurable.

**At include_docs with limit=200:**
- Extra 54ns * 200 = 10.8 us per query. Query time is 5-30ms. This is 0.04-0.2% overhead.

**At burst (275K ops/sec) with include_docs:**
- Extra 54ns * 275K = 14.9 ms/sec. Spread across threads, ~0.5ms per thread. Still negligible.

**The only scenario where 84ns hurts:**
- A query that returns 100K+ docs with include_docs. 54ns * 100K = 5.4ms extra.
  But such queries are already 30ms+ in bitmap operations. 18% overhead.
  These queries are rare (production p99 limit is 200 per Aidan's traces).

**Conclusion:** The 84ns read latency is not a practical concern for any realistic workload.
The flexibility and disk savings of variable-size storage are worth 2.8x on a metric that
is never the bottleneck.

---

## What About the Frozen Bitmap Pattern?

V3 frozen bitmaps use `FrozenRoaringBitmap::view(&mmap_slice)` for zero-copy reads. Can we
do something similar for documents?

Documents are not bitmaps — they do not have a canonical binary format that supports
zero-copy operations. But the slot table approach IS the document equivalent of frozen
bitmaps: the data file is the "frozen" snapshot, the slot table is the offset index, and
both are mmap'd for zero-copy reads. The analogy holds.

The ops log + janitor pattern unifies both:

| | Bitmaps | Documents |
|---|---------|-----------|
| Snapshot | .frozen file (CRoaring format) | data.dat (packed docs) + slot_table.dat |
| Read | `view(&mmap_slice)` | `slot_table[id]` -> `&data[offset..offset+length]` |
| Write | In-memory diff + ops.log | Append to data + update slot_table + ops.log |
| Compact | `to_owned()` + apply diff + `serialize_frozen_into()` | Rewrite data sequentially + rebuild slot_table |

One pattern. Two data types. Clean symmetry.

---

## Appendix: Scaling Projections

| Records | Slot Table (12B) | Data File (230B avg) | Total | Fixed 512B | Savings |
|---------|-----------------|---------------------|-------|-----------|---------|
| 10M | 120 MB | 2.3 GB | 2.4 GB | 5.1 GB | 53% |
| 100M | 1.2 GB | 23 GB | 24.2 GB | 51.2 GB | 53% |
| 500M | 6 GB | 115 GB | 121 GB | 256 GB | 53% |
| 1B | 12 GB | 230 GB | 242 GB | 512 GB | 53% |

At 1B records, the slot table is 12 GB — large but still mmap-friendly. The OS will page
in only the hot portions. Sequential access patterns (bulk load, compaction) will prefetch
efficiently.

The savings percentage is constant because it depends only on (SLOT_SIZE - avg_doc_size) /
SLOT_SIZE, which is dataset-dependent, not scale-dependent. For any dataset where
avg_doc_size < SLOT_SIZE, the slot table approach saves disk proportional to the waste.
