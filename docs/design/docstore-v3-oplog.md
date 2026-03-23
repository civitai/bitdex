# DocStore V3: Generation-Based Op-Log Architecture

> Evolve the append-only tuple log (V2) into a generation-aware operations log with in-memory document caching. Generations enable point-in-time snapshots without copying. Op-log format enables incremental bitmap persistence (append deltas instead of rewriting full bitmaps).

**Status**: PROPOSED
**Builds on**: DocStore V2 (`docs/design/docstore-v2-bittuple-log.md`)
**Enables**: Snapshot system (`docs/design/snapshot-system.md`), incremental bitmap persistence

---

## Problem

DocStore V2 is fast (21us/doc reads, 512MB/s writes) but has gaps:

1. **No in-memory caching** — Every read hits disk. Different request patterns = different shard reads = unique disk I/O per request. Hypothesis: this is a major contributor to the 6-second production stalls.
2. **No point-in-time snapshots** — Can't freeze state without stopping writes. Needed for the snapshot/benchmark system.
3. **Full bitmap rewrites** — BitmapFs saves rewrite entire roaring bitmaps to disk. At 105M records, this is ~7GB of bitmap data per save. Could instead append deltas.
4. **No schema awareness** — V2 tuples are raw (slot, field_idx, value). No knowledge of field types, no ability to express operations like "append to multi-value" vs "replace scalar."

---

## Design

### Generation Model

```
data/
  docstore/
    master.bin              ← generation index: [gen_id, path, status, tuple_count]
    gen_000/                ← base generation (full state, compacted)
      schema.bin            ← field dictionary + field types + defaults
      shards/               ← shard files (same hex-bucketed layout as V2)
    gen_001/                ← ops generation (deltas only)
      ops.binlog            ← operation tuples
      index.bin             ← slot_id → offset index (for random access)
    gen_002/                ← another ops generation
      ops.binlog
      index.bin
```

**Base generation (Gen 0)**: Full document state. Created by compacting all prior generations or from initial bulk load. Same shard format as V2 but with schema header.

**Ops generations (Gen 1+)**: Append-only operation logs. Each entry is a typed operation, not just a raw tuple.

@justin: [shard-local ops logs, unified file format, generation fall-through, bitmap ops on top of snapshots, janitor compaction, LIFO generation traversal, unified storage system]

@sky: This is a much better architecture than what I proposed. Let me respond to the key points:

**Shard-local ops logs**: Agreed — this is strictly better than a single ops.binlog per generation. It preserves V2's parallel write advantage (per-shard mutexes, near-zero contention), avoids a single bottleneck file, and means we don't need the `index.bin` at all since slot→shard mapping is already deterministic. The file format you described (header → snapshot → ops log) is clean and keeps everything self-contained per shard.

**Unified file format for both docs and bitmaps**: I like this direction. The pattern is identical:
```
--- HEADER (schema/config, snapshot pointers, oplog pointer)
--- SNAPSHOT (doc state OR roaring bitmap)
--- OPS LOG (append-only, LIFO read)
```
The difference is just what the snapshot and ops encode. For docs: snapshot = flattened field values, ops = Set/Append/Remove/Delete. For bitmaps: snapshot = serialized roaring bitmap, ops = SetBit/ClearBit. A trait-based approach could unify the I/O layer while keeping the domain logic separate. I'd propose we call the unified system **`ShardStore`** (or your `BitStore` name) and have it parameterized by a `SnapshotCodec` and `OpCodec` trait. Adam can prototype this — it's a clean abstraction that earns its keep.
@justin: Like it all. We should probably outline the full list of codecs of both types and ensure we implement it in a way that's easily extensible

@sky: Agreed. Here's the initial codec inventory:

**SnapshotCodecs** (how to read/write the snapshot section):
| Codec | Used by | Snapshot data |
|-------|---------|---------------|
| `DocSnapshotCodec` | DocStore shards | Flattened document fields (schema-aware, per-slot) |
| `BitmapSnapshotCodec` | Filter/sort bitmaps | Serialized roaring bitmap bytes |
| `AliveSnapshotCodec` | Alive bitmap | Serialized roaring bitmap (single per store) |

**OpCodecs** (how to read/write ops log entries):
| Codec | Used by | Op types |
|-------|---------|----------|
| `DocOpCodec` | DocStore shards | Set, Append, Remove, Delete, Create |
| `BitmapOpCodec` | Filter/sort bitmaps | SetBit, ClearBit, BatchSet, BatchClear |

**ShardingStrategy** (how to locate the right file):
| Strategy | Used by | Mapping |
|----------|---------|---------|
| `SlotHexShard` | DocStore | slot_id → hex-bucketed directory + shard file (existing V2 layout) |
| `FieldFileShard` | Filter bitmaps | (field_name, value) → one .fpack file per field, multiple values inside |
| `SortLayerShard` | Sort bitmaps | (field_name, bit_position) → one .sort file per sort field |
| `SingletonShard` | Alive bitmap | One file, no sharding |

The trait interfaces would be:
```rust
trait SnapshotCodec: Send + Sync {
    type Item;  // StoredDoc for docs, RoaringBitmap for bitmaps
    fn encode(item: &Self::Item, buf: &mut Vec<u8>);
    fn decode(bytes: &[u8]) -> Result<Self::Item>;
}

trait OpCodec: Send + Sync {
    type Op;  // DocOp for docs, BitmapOp for bitmaps
    fn encode_op(op: &Self::Op, buf: &mut Vec<u8>);
    fn decode_op(bytes: &[u8]) -> Result<Self::Op>;
    fn apply(item: &mut <associated SnapshotCodec>::Item, op: &Self::Op);
}

trait ShardingStrategy: Send + Sync {
    type Key;  // u32 (slot) for docs, (field, value) for bitmaps
    fn shard_path(&self, key: &Self::Key, gen_dir: &Path) -> PathBuf;
}
```

Adding a new storage type = implement these three traits. The `ShardStore<S, O, Sh>` is generic over all three. Extensible by design.

@justin: [tags sharding — do we shard by value too, not just field?]

@sky: You're right — it's more nuanced than "one file per field." I checked `src/bitmap_fs.rs`. Current layout:

**Filter bitmaps** use **hex-bucketed .fpack files per field**. Each field gets a directory (`filter/tagIds/`, `filter/nsfwLevel/`, etc.), and within that, values are distributed across up to 256 bucket files (`00.fpack` through `ff.fpack`) based on `(value >> 8) & 0xFF`. Each .fpack has an index table (value → offset/length) so you can read individual bitmaps without loading the whole file. For tagIds with 31K values, that's ~120 values per bucket file.

So the sharding strategies need a fourth entry:

| Strategy | Used by | Mapping |
|----------|---------|---------|
| `SlotHexShard` | DocStore | slot_id → hex-bucketed shard file |
| `FieldValueBucketShard` | Filter bitmaps | (field, value) → field dir + hex-bucketed .fpack |
| `SortLayerShard` | Sort bitmaps | (field, bit_position) → one .sort file per field |
| `SingletonShard` | Alive bitmap | One file, no sharding |

The `FieldFileShard` I originally proposed was wrong — it's not one file per field. It's one *directory* per field with *bucketed files per value range*. The `FieldValueBucketShard` captures the real behavior. Same trait interface, just the correct mapping.

**Bitmap ops cost**: This is the key assumption to validate. Applying 100 SetBit/ClearBit ops to a deserialized roaring bitmap should be sub-microsecond (it's just flipping bits in containers). The question is whether the ops log I/O during lazy load adds meaningful time. I'll have Ollie run a microbench: deserialize bitmap → apply N ops → measure, for N = 10, 100, 1000, 10000. My expectation is it's negligible up to ~10K ops, at which point the janitor should have compacted anyway.
@justin: Great. Important test.

**Janitor triggering**: Length-based makes sense. Readers already scan the shard — they can cheaply check ops count and flag for compaction. Time-based would require wall-clock tracking per shard which is messy. I'd say: flag when ops count > threshold (configurable, maybe 500-1000 ops per shard), janitor picks up flagged shards in background.
@justin: Right, it's not like they even need to necessarily check ops count, right? They'll be looping through all of the ops anyways, and so they'll just keep a counter.

@sky: Exactly — zero overhead. The reader is already iterating ops for the LIFO scan. Just increment a local counter. If `counter > threshold` at the end, set an atomic flag on the shard for the janitor. The janitor periodically checks flagged shards and picks them up. No separate "check ops count" pass needed.

@justin: [existing janitor already has queue infrastructure, just feed into it]

@sky: Correct — the janitor in `src/docstore.rs` already does reader-triggered compaction (no polling). Readers flag dirty shards, janitor picks them up. The V3 change is just extending the "dirty" criteria from "too many stale tuples" to "too many ops since last snapshot." Same queue, same trigger mechanism, different threshold. Will note in the implementation plan that this is an extension of existing infra, not a new system.

**LIFO generation fall-through**: Yes, this makes perfect sense. On read, walk generations newest→oldest, stop at first snapshot that covers your slot. On startup/lazy load, same thing. This is elegant because compaction just means "flatten everything into a snapshot at this generation and delete older gens."

**Unified storage system**: I think this is the right call architecturally. The I/O layer (header parsing, ops appending, compaction scheduling, generation management) is identical. Only the snapshot/ops encoding differs. Two concerns:
1. Roaring bitmaps are variable-size blobs (some are 12 bytes, tagId bitmaps are megabytes). Doc shards are fixed-slot-count. We'd need the ShardStore to handle both sharding strategies — hex-bucketed directories for docs (slot-based), and per-field files for bitmaps (one "shard" per bitmap).
@justin: [sharding strategy as config-driven, deterministic file location]

@sky: Yes — the `ShardingStrategy` trait I outlined above is exactly this. The schema/config for each store instance says "use SlotHexShard" or "use FieldFileShard" and the ShardStore uses that to deterministically locate files. The config-driven approach you're describing maps cleanly to having the `ShardStore` constructor take a `ShardingStrategy` impl, which could even be loaded from the schema header at runtime. So you could theoretically change sharding strategies across generations (though you'd rarely want to).
2. Timeline: unifying the systems is a refactor on top of V3. I'd recommend building V3 with the unified pattern in mind (shared header format, shared generation logic) but keeping doc and bitmap as separate ShardStore instances for now. Unifying the code can happen once both are working.
@justin: [unify both systems as part of this refactor, not later]

@sky: Fair point — you're right that if we're already ripping out the storage layer, doing it piecemeal just means two rounds of disruption. Let me revise the plan: Adam builds `ShardStore<S, O, Sh>` as the unified system from day one. DocStore V3 and BitmapFs V2 are both instantiations of ShardStore with different codecs and sharding strategies. One codebase, one set of tests for the I/O layer, two sets of codec tests. This does increase the scope of the initial PR, but it means we only build and debug the generation logic, compaction, and ops appending once. I'll update the work breakdown accordingly.

**Doc caching granularity**: You asked whether to cache full shards or individual doc IDs. I'd go with individual IDs. At 105M records with 512 docs/shard, caching full shards would mean pulling 512 docs when you only need 1. Individual ID caching lets the working set naturally form around what's actually being requested. And as you said, slot→shard is deterministic, so cache misses know exactly where to look.
@justin: [single-ID fetch re-scans ops log, need batch fetch from same shard]

@sky: Good catch — this is a real concern. If 10 cache misses hit the same shard in rapid succession, you'd scan the ops log 10 times. The fix is a **batch shard read**: when a query result needs docs from multiple slots, group the slot IDs by shard, then do a single shard read per shard that collects all requested slots in one pass through the ops log. The LIFO scan already touches every op — extracting multiple slots from one pass is basically free (just check `op.slot` against a small HashSet of requested slots).

Implementation: the cache miss path collects all missing slot IDs, groups by shard, issues batch reads, caches all results. Single-ID fetches still work as the degenerate case (batch of 1).

For Ollie's microbench: we should test both single-slot and multi-slot shard reads to quantify the per-slot overhead. If it's significant, that validates the batch approach. If it's negligible (which I suspect for shards with <500 ops), then even individual reads are fine.

@justin: [microbench compaction threshold, cross-thread read batching with configurable flush interval]

@sky: Both great points. Let me address each:

**Compaction threshold microbench**: Yes — Ollie's benchmark list should include: "read latency vs ops count" curve. Measure single-slot read at 10, 50, 100, 250, 500, 1000, 2500 ops per shard. Find the knee where latency jumps noticeably. That becomes the default compaction threshold. I'd guess the knee is somewhere around 500-1000 ops based on the shard size (few KB of ops data at that point), but we need the numbers.

**Cross-thread read batching**: This is essentially a **singleflight pattern** for shard reads — a well-known pattern in Go, less common in Rust but totally doable. Here's how I'd implement it:

```
Thread A needs doc 42 from shard 0x3a → checks batch collector for shard 0x3a
  - No pending batch → creates one, becomes the "leader" for this batch
  - Sets a short deadline (configurable, e.g. 1-2ms)
Thread B needs doc 78 from shard 0x3a → checks batch collector
  - Pending batch exists → adds slot 78 to the batch, gets a oneshot receiver
Thread C needs doc 15 from shard 0x3a → same, adds to batch
... deadline expires or batch hits a size limit ...
Thread A (leader) executes the batch: one shard read, extracts all requested slots
Thread A sends results back through oneshot channels to B and C
```

The key properties:
- First requester becomes the leader (no separate worker needed)
- Configurable coalescing window (1-2ms default — basically invisible to users)
- Per-shard batching so different shards execute in parallel
- Degrades gracefully: if only one thread needs a shard, batch of 1, zero overhead

This is the same pattern we discussed for the unified cache (singleflight would help cache p95 too — noted in CLAUDE.md). DashMap keyed by shard ID, value is a batch collector with a deadline. Definitely worth building as part of ShardStore since it benefits both doc and bitmap reads.

### Operation Format

```rust
enum DocOp {
    /// Set a scalar field to a value (replaces previous)
    Set { slot: u32, field: u16, value: PackedValue },

    /// Append a value to a multi-value field (e.g., add tag)
    Append { slot: u32, field: u16, value: PackedValue },

    /// Remove a value from a multi-value field (e.g., remove tag)
    Remove { slot: u32, field: u16, value: PackedValue },

    /// Delete entire document
    Delete { slot: u32 },

    /// Create document with full field set
    Create { slot: u32, fields: Vec<(u16, PackedValue)> },
}
```

Wire format: msgpack-encoded, prepended with u32 LE byte length for random access.

### Schema Header

Each base generation includes a schema:

```rust
struct DocSchema {
    fields: Vec<FieldDef>,
}

struct FieldDef {
    idx: u16,
    name: Arc<str>,
    field_type: FieldType,  // Scalar, MultiValue, Boolean
    default: Option<PackedValue>,
}
```

The schema defines what operations are valid per field. `Append`/`Remove` only valid on `MultiValue` fields. `Set` only valid on `Scalar`/`Boolean`.

@justin: [pluggable field type registry]

@sky: Not difficult at all in Rust — this is a natural fit for a trait. Something like:

```rust
trait FieldHandler: Send + Sync {
    fn valid_ops(&self) -> &[OpKind];  // Set, Append, Remove, etc.
    fn apply_op(&self, current: &mut PackedValue, op: &DocOp) -> Result<()>;
    fn default_value(&self) -> PackedValue;
}
```

Then the schema maps each field_idx to a `Box<dyn FieldHandler>`. Built-in handlers: `ScalarHandler`, `MultiValueHandler`, `BooleanHandler`. Adding a new field type = implement the trait, register it. The ops log doesn't need to change — it just stores the op, and the handler interprets it.

This also cleanly solves validation — if someone sends an `Append` op for a scalar field, the handler rejects it before it hits the ops log. Good separation of concerns and no cost at read time (it's just a vtable dispatch, <1ns).

### Read Path (Revised)

@sky: Reworked per your feedback. No more `index.bin`, everything is shard-local with generation fall-through.

To reconstruct a document for slot S:

1. **Fast path**: Check in-memory doc cache → if hit, return (ns).
2. **Cache miss**: Determine shard from slot (deterministic mapping).
3. Walk generations LIFO (newest → oldest):
   a. Open shard file for this slot's shard in current generation
   b. If shard exists: read header → check if snapshot covers slot S
      - If snapshot covers S: load snapshot state for S
      - Read ops log section (LIFO), apply ops for slot S on top of snapshot
      - Stop — don't fall through to older generations
   c. If shard doesn't exist in this generation: fall through to prior generation
4. Cache the reconstructed document.
5. If slot not found in any generation → document doesn't exist.

**Key property**: Once you find a shard containing a snapshot for your slot, you're done. The snapshot + that shard's ops log is the complete state. No cross-generation merging needed.

### Write Path (Revised)

1. Validate op against field handler (pluggable registry)
2. Determine target shard from slot (deterministic mapping)
3. Append `DocOp` to the **current generation's** shard file ops log section
   - If shard doesn't exist yet in current gen: create it (header only, no snapshot — snapshot comes from compaction)
4. If slot S is in doc cache → apply op directly to cached doc (no disk read)
5. Ops flow through the same crossbeam channel as bitmap mutations — the flush thread handles ordered writes

@justin: [rename DocOp → ShardOp for unified naming]

@sky: Agreed on the naming. In the unified model:
- The generic op type is `ShardOp<O>` where `O` is the codec's op type
- For docs: `ShardOp<DocOp>` — carries Set/Append/Remove/Delete/Create
- For bitmaps: `ShardOp<BitmapOp>` — carries SetBit/ClearBit/BatchSet/BatchClear
- The shard file format doesn't care about the op type — it just stores bytes. The codec handles serialization.

On disk, the ops log entries are: `[u32 length][u8 op_type_tag][payload bytes]`. The `op_type_tag` is codec-specific. This way the ShardStore I/O layer is truly generic — it reads/writes byte blobs and delegates interpretation to the codec.

### In-Memory Document Cache

```rust
struct DocCache {
    /// slot_id → cached document
    entries: DashMap<u32, CachedDoc>,
    /// LRU tracking
    access_times: DashMap<u32, Instant>,
    /// max entries (configurable)
    capacity: usize,
}

struct CachedDoc {
    fields: SmallVec<[(u16, PackedValue); 16]>,
    generation: u64,  // which gen this was built from
}
```

**Population**: Cache-on-read. When a query result includes document content, the reconstructed doc enters the cache.

**Invalidation**: Op-log watchers. When a write arrives for slot S:
- If S is in cache → apply the op directly to the cached doc (no disk read needed)
- Writes do NOT count as reads for LRU purposes (avoids write amplification of cache)

**Eviction**: LRU by `access_times`. Configurable capacity. Read-heavy slots stay hot; write-only slots get evicted.

**Size estimate**: At ~500 bytes/doc average, 100K cached docs = 50MB. Well within budget.

@justin: This all looks good, though. Even though the read/write path is slightly different.

@sky: Agreed — the cache layer stays the same regardless of the read/write path changes above. The key insight from your feedback is that cache updates from writes are eventually consistent and non-blocking, which means the cache can lag slightly behind the ops log and that's fine. The ops log is the source of truth; the cache is just an optimization.

---

## Bitmap Op-Log (Extension)

The same generation model applies to bitmap persistence:

### Current (BitmapFs)

Full roaring bitmap files rewritten on save. At 105M, this is ~7GB per save cycle.

### Proposed (BitmapFs V2)

```
bitmaps/
  gen_000/                  ← base: full roaring bitmap files
    filters/
    sorts/
    alive.roar
  gen_001/                  ← delta: bit changes only
    ops.bitlog              ← (bitmap_id, bit_position, set/clear)
```

**Base generation**: Same `.fpack` and `.sort` files as current BitmapFs.

**Ops generation**: Append-only log of bit changes:

```rust
enum BitmapOp {
    SetBit { bitmap_id: u32, bit: u32 },
    ClearBit { bitmap_id: u32, bit: u32 },
    /// Batch: set/clear multiple bits on same bitmap
    BatchSet { bitmap_id: u32, bits: Vec<u32> },
    BatchClear { bitmap_id: u32, bits: Vec<u32> },
}
```

**Benefits**:
- Writes become appends (fast, no rewrite)
- Reads: load base bitmap, apply ops (ops are tiny — a few bytes each)
- Save cycle goes from seconds to milliseconds
- Generations align with DocStore generations (same pin = same snapshot)

**Compaction**: Periodically merge ops into base (rewrite bitmap). Configurable threshold: compact when ops log exceeds N entries or M bytes.

@justin: [unify shard store now, bitmap ops as second codec application]

@sky: Yes — with the unified ShardStore, this entire "Bitmap Op-Log" section becomes just another ShardStore instantiation. The section above already defines the `BitmapOp` enum and `BitmapSnapshotCodec`. In the unified model:

```rust
// DocStore = ShardStore with doc codecs
type DocStore = ShardStore<DocSnapshotCodec, DocOpCodec, SlotHexShard>;

// BitmapStore = ShardStore with bitmap codecs
type FilterStore = ShardStore<BitmapSnapshotCodec, BitmapOpCodec, FieldFileShard>;
type SortStore = ShardStore<BitmapSnapshotCodec, BitmapOpCodec, SortLayerShard>;
type AliveStore = ShardStore<BitmapSnapshotCodec, BitmapOpCodec, SingletonShard>;
```

Each gets the same generation management, ops appending, compaction, and janitor triggering — for free. The bitmap-specific section here becomes a description of the `BitmapSnapshotCodec` and `BitmapOpCodec` implementations, not a separate system. I'll restructure this doc to reflect the unified model once we finalize the trait interfaces. This is exactly the "formalize the standard by demonstrating it" you're after — two concrete codec pairs proving the abstraction works.

---

## Generation Lifecycle

```
                    pin
Active Gen ──────────────► Frozen Gen
    │                          │
    │ new writes               │ compactable
    ▼                          ▼
New Active Gen            Merge into base
```

**Pin**: Called by snapshot system or manual trigger. Freezes current gen, starts new one. No data copying — just bumps the generation counter.

**Compact**: Merge frozen gens into a new base. Rewrites shards with all ops applied. Old gens deleted after. Can run in background.

**Configurable retention**:
- `max_generations`: compact when exceeded (default: 4)
- `max_ops_bytes`: compact when ops log grows past threshold
- `on_demand`: only compact when explicitly triggered

@justin: [shard-specific ops logs vs unified ops log, compaction flattens to snapshot, atomic swaps, generation compression]

@sky: To clarify — I think we're actually in agreement. The approach IS shard-specific ops logs (your design), NOT a single unified ops log (my original proposal). Each shard file has its own ops section at the tail. So compaction is per-shard: the janitor reads the shard (snapshot + ops), applies ops to produce a new snapshot, atomic-swaps the file. No cross-shard coordination needed.

For generation-level compaction: when you want to collapse Gen 0 + Gen 1 into a single generation, the janitor walks each shard that exists in Gen 1, merges it with its counterpart in Gen 0 (if any), writes the merged shard to the compacted gen. Shards that only exist in Gen 0 can just be moved/linked. Then delete the old gens. This is embarrassingly parallel — each shard is independent.

The compression-of-entire-generation idea (tar.zst the whole gen directory) makes sense as a separate concern — that's for the snapshot export/download system, not for normal operation.

---

## Relation to Existing Systems

| System | Current | V3 |
|--------|---------|-----|
| DocStore reads | Disk every time (V2 tuple scan) | Memory cache, disk fallback |
| DocStore writes | Append tuple to shard | Append typed op to generation |
| Bitmap save | Full rewrite (~7GB) | Append delta (~KB per flush) |
| Snapshot | Not possible | Pin generation |
| Compaction | Janitor (background) | Generation merge (background) |

### Migration Path

V3 is backward-compatible with V2 at the shard level. A V2 shard directory becomes a Gen 0 base. New ops go into Gen 1+. No migration step needed — just start writing ops.

@justin: [V2 = op log only, V3 = snapshot + op log]

@sky: Exactly right. V2 shards are purely append-only tuple logs — no snapshot section, everything is ops. V3 adds the snapshot section as an optimization: pre-computed state so you don't have to replay the full op log from scratch. A V2 shard is just a V3 shard with an empty snapshot section and the op log starting at the beginning. This means migration is zero-cost: V3 reads V2 shards as "all ops, no snapshot," and the janitor's first compaction pass produces the snapshot section. No separate migration step.

@justin: [snapshot should be actual document data, not ops replay]

@sky: Yes, the snapshot should absolutely be the materialized document, not ops. Three reasons:

1. **Read speed**: Snapshot read = deserialize document. No iteration. If there are zero pending ops (common after compaction), you skip the ops log entirely.
2. **Size**: A materialized doc with 15 fields is probably ~200-500 bytes. The equivalent ops to build it from scratch (15 Create/Set ops) would be larger due to per-op headers and slot IDs repeated in each op.
3. **Consistency with bitmaps**: Bitmap snapshots are the serialized roaring bitmap, not "the ops that built it." Same pattern for docs.

So the read layers are exactly as you described:
```
1. Schema defaults (from header)
2. Merge snapshot doc (if present — covers full state at compaction time)
3. Apply ops log LIFO (only ops since last snapshot)
```

If no snapshot exists (new shard, pre-compaction), you just skip step 2 and build from ops only. The janitor's job is to periodically produce snapshots so step 3 is short.

---

## Open Questions

1. ~~**Index size**~~ **RESOLVED** — No longer needed. Shard-local ops logs mean slot→shard mapping is deterministic (same as V2). No separate index file.

2. **Cache sizing** — @sky: The dual-system idea is smart. Here's a concrete mechanism: track a `cache_update_backlog` counter (incremented when a write op is queued for cache update, decremented when applied). If `backlog / backlog_drain_rate` exceeds a threshold (say 100ms of lag), start evicting least-recently-read entries to shrink the cache. This is essentially a feedback loop: cache grows with reads (hot entries), shrinks when write pressure makes updates too expensive. The key invariant is that write-to-cache updates are never blocking — they're fire-and-forget via a channel, and the cache tolerates being slightly stale. We just need the Prometheus metric (`bitdex_doc_cache_backlog`) to know if it's healthy.
@justin: This is exactly what I want. Perfect. Good thinking.

3. ~~**Bitmap ops log size**~~ **RESOLVED** — Per-shard ops logs. Janitor handles compaction per-shard when ops count exceeds threshold. Same pattern as existing V2 janitor, just extended to handle the snapshot+ops format.

4. ~~**Concurrent generation access**~~ **RESOLVED** — @sky: You're right, Arc per generation is unnecessary. Readers hit the doc cache (live, no generation concept). On cache miss, they know which generations exist (a small atomic list that rarely changes), read the shard files directly, and cache the result. The generation list only changes on pin/compact, which is rare. A simple `AtomicU64` generation counter + a small vec of generation paths (behind a read-mostly lock or ArcSwap) is sufficient. No per-reader Arc needed.

5. ~~**Schema evolution**~~ **RESOLVED** — @sky: Agreed. Schema evolution = new generation with updated schema in the shard headers. New ops use new schema. Old generations keep their original schema. On read fall-through, the reader applies the schema from whichever generation's shard it's reading. Compaction merges old schema + new schema into the compacted shard's header. No special migration step — it's just ops.
