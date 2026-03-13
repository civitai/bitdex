# Bitdex V2 — Project Brief & Development Guide

## What is Bitdex?

Bitdex is a purpose-built, in-memory bitmap index engine written in Rust. Its only job is to take filter predicates and sort parameters and return an ordered list of integer IDs. It stores no documents. It serves no content. It is bitmaps all the way down.

**In:** Filter predicates + sort field + sort direction + limit
**Out:** Ordered `Vec<i64>` of IDs

Everything else—document retrieval, content serving, full-text search—happens downstream.

---

## Why Bitdex Exists

We previously used Meilisearch to index ~150M image documents for filtering and sorting. It collapsed under the write load—300 document inserts were taking 4 minutes. OpenSearch would be an improvement but requires a multi-node cluster with 64GB+ RAM per node, costs thousands per month, and delivers 50-200ms query times.

Bitdex V1 proved the bitmap filtering concept works (0.2ms queries) but hit memory and write performance walls due to Vec-based column storage, skip lists, and forward/reverse indexes.

Bitdex V2 eliminates all of that. The entire engine is roaring bitmaps. Nothing else.

---

## Core Architecture

### The One Rule

**Everything is a bitmap. There are no other data structures for indexed data.**

No Vecs for column storage. No skip lists. No sorted arrays. No forward maps. No reverse indexes. Documents are stored on disk via redb (not in-memory). The only non-bitmap in-memory structures are: HashMap for the trie cache, redb file handles for the docstore, and Prometheus metrics counters.

### Slot Model

- Each document is assigned a **slot** which is its position in every bitmap
- For integer ID users: the Postgres ID IS the slot. No mapping needed.
- For GUID users (future): a HashMap<GUID, u32> maps to slots. Not in V2 scope.
- Slots are monotonically assigned via atomic counter on insert
- Deleted slots are NOT immediately recycled. The alive bitmap hides them.
- An **autovac** process periodically produces a **clean bitmap** of recycled slots
- New inserts check the clean bitmap first (grab first set bit), append only if none available

### Bitmap Categories

#### 1. Alive Bitmap
- One bitmap tracking all active documents
- ANDed into every query implicitly
- Delete = clear one bit here. That's the entire delete operation.
- Dead bits in other bitmaps are harmless noise filtered out by this gate

#### 2. Filter Bitmaps
- One roaring bitmap per distinct value per filterable field
- `tag=anime` → bitmap of all slots with that tag
- `nsfwLevel=1` → bitmap of all slots with nsfwLevel 1
- Boolean fields: one bitmap per boolean (hasMeta, onSite, etc.)
- Multi-value fields (tags, modelVersionIds, toolIds, techniqueIds): one bitmap per distinct value, a document can appear in multiple bitmaps for the same field
- Filtering is AND/OR/NOT operations across bitmaps

#### 3. Sort Layer Bitmaps
- Each sortable numeric field is decomposed into N bitmaps, one per bit position of the value
- A u32 field = 32 bitmaps (bit 0 through bit 31)
- Bitmap `sort_reactionCount_bit5` has a 1 for every slot where bit 5 of reaction_count is set
- **Top-N retrieval**: start at the most significant bit layer, AND with the filter result. If the intersection has >= N results, narrow to that set and descend. If < N, keep both groups and continue. This is binary search across all documents simultaneously.
- All sort operations are bitmap AND operations. No sorting algorithms, no comparisons, no heaps.

### Configuration

Each sort field is configured with:

```yaml
sort_fields:
  - name: reactionCount
    source_type: uint32  # determines default bit depth
    encoding: linear     # linear or log
    bits: 32             # number of bitmap layers, defaults to full bit space

  - name: sortAt
    source_type: uint32
    encoding: linear
    bits: 32

  - name: commentCount
    source_type: uint32
    encoding: linear
    bits: 32

  - name: collectedCount
    source_type: uint32
    encoding: linear
    bits: 32

  - name: id
    source_type: uint32
    encoding: linear
    bits: 32
```

**Build with full precision first.** Encoding optimizations (log encoding for power-law fields, reduced bit depths) are a future optimization. Do not implement log encoding in V2 unless sort performance proves to be a bottleneck.

Each filter field is configured with:

```yaml
filter_fields:
  - name: nsfwLevel
    field_type: single_value
  - name: tagIds
    field_type: multi_value
  - name: userId
    field_type: single_value
  - name: modelVersionIds
    field_type: multi_value
  - name: onSite
    field_type: boolean
  # ... etc
```

Global configuration:

```yaml
max_page_size: 100          # hard cap on results per query
cache_max_entries: 10000    # trie cache size limit
cache_decay_rate: 0.95      # exponential decay for hit stats
autovac_interval_secs: 3600 # how often autovac runs
snapshot_interval_secs: 300  # how often snapshots are taken
wal_flush_strategy: "fsync"  # fsync or async
prometheus_port: 9090
```

All configuration should be hot-reloadable via config file watch or admin API endpoint.

---

## Mutation API

### PUT(id, document)
Full replace with upsert semantics.
1. If slot exists for this ID and is alive: treat as full update (clear all old bits, set all new bits)
2. If slot does not exist: assign new slot (check clean bitmap first, then atomic counter)
3. Set alive bit
4. Set filter bitmaps based on document fields
5. Encode sort field values and set sort layer bitmaps
6. Write WAL entry

### PATCH(id, partial_fields)
Merge only provided fields.
1. For each changed filter field: clear old bitmap bit, set new bitmap bit. The WAL event provides old and new values—do NOT look up old values from stored state.
2. For each changed sort field: XOR old and new encoded values, flip only changed bit layers
3. Write WAL entry

### DELETE(id)
1. Clear the alive bit for this slot. Done.
2. Write WAL entry
3. All other bitmaps retain stale bits—they're invisible behind the alive gate

### DELETE WHERE(predicate)
1. Resolve the predicate using filter bitmaps (this is just a query)
2. Get the matching slot IDs from the result bitmap
3. Clear the alive bit for each matching slot
4. Write WAL entry for each

---

## Query Path

### Step 1: Parse
Plugin query parser converts the incoming request to an internal filter/sort representation.

### Step 2: Cache Check
Normalize filter clauses into canonical sorted order. Walk the trie cache:
- Exact hit with valid generations → use cached bitmap, skip to step 4
- Prefix hit → use cached partial bitmap as starting point for step 3
- Miss → compute from scratch in step 3

### Step 3: Filter Computation
Evaluate filter clauses ordered by estimated cardinality (smallest first).
- AND: bitmap intersection
- OR: bitmap union
- NOT: bitmap complement (andnot operation)
- Always AND with alive bitmap as final step
- Store result in trie cache

### Step 4: Sort
Traverse sort layer bitmaps from most significant bit to least significant:
- At each layer, AND the working set with the sort layer bitmap
- If intersection has >= limit results, narrow to intersection (for descending) or its complement (for ascending)
- If < limit results, bifurcate and continue
- After all layers: result is the top N slots in sort order
- Tiebreaker: slot ID (use bit positions in the result bitmap)

### Step 5: Cursor Pagination
Cursor is two values: `(sort_value, slot_id)`.
For next page: modify the sort traversal to find values <= sort_value (descending) or >= sort_value (ascending), with slot_id as tiebreaker for equal values.

### Step 6: Return
Extract set bit positions from the final bitmap. These ARE the Postgres IDs (since slot = ID). Return as ordered Vec<i64>.

---

## Query Planning

### Cardinality-Based Filter Ordering
Maintain a count alongside each filter bitmap (increment on insert, decrement on delete). When computing filter intersections, sort clauses by cardinality ascending—intersect the smallest bitmap first.

### Filter-First vs Sort-First Decision
If the estimated filter result is very small (< 1000 based on cardinality estimates of the smallest filter), the sort traversal through 32 layers might be overkill. For very small result sets, it may be faster to extract all matching IDs and sort them via a simple in-memory sort. Benchmark both paths and choose based on estimated cardinality.

### NOT Filter Optimization
For `NOT nsfwLevel=28` where the negated set is small but the matching set is huge: compute the small set and use `andnot` against the working set rather than computing the complement.

---

## Trie Cache

### Structure
A trie keyed by canonically sorted filter clauses. Each node may hold:
- A cached result bitmap
- Hit count (exponential moving average)
- Last access timestamp
- Generation counters for each filter field involved

### Prefix Matching
Query `{nsfwLevel=1, tag=anime, userId=10103}` checks the trie for the longest matching prefix. If `{nsfwLevel=1, tag=anime}` exists as a cached node, use that bitmap and AND it with the userId bitmap. Avoids recomputing common filter combinations.

### Canonical Ordering
Filter clauses are sorted by field name, then by value. This ensures the same logical query always maps to the same trie path regardless of the order the caller specified filters.

### Promotion/Demotion
A background loop runs periodically:
- **Promote**: nodes with high hit counts but no cached bitmap get their bitmap computed and cached
- **Demote**: nodes with cached bitmaps that haven't been accessed recently get evicted
- Hit counts decay by `cache_decay_rate` each cycle to prevent stale promotions

### Invalidation
Lazy generation counter per filter field. When any document's nsfwLevel changes, bump the nsfwLevel generation. On cache hit, check if any field generation has advanced since the entry was created. If so, treat as miss. Slightly over-invalidates but cheap and correct.

**Important note**: sort fields (reactionCount, etc.) change frequently but are NOT part of cache keys. Cache keys are filter-only. Sort is applied after cache lookup. So reaction count churn does NOT invalidate cached filter bitmaps.

---

## Concurrency Model

### No Locks

Readers and writers operate on shared bitmaps simultaneously. No RwLock, no Mutex.

### Optimistic Validation
- Writers atomically mark their target slot ID in a small in-flight set (concurrent HashMap or atomic bitmap) BEFORE mutating bitmaps
- Writers clear the in-flight mark AFTER mutation is complete
- Readers execute their full query without coordination
- After computing results, readers check if any of their result IDs overlap with the in-flight set
- If no overlap: return results (99.99% of the time)
- If overlap: revalidate only the overlapping IDs (re-check their filter/sort bits)

### Why This Works
Write operations take microseconds. The probability of a read's 50 result IDs colliding with a concurrent microsecond write is negligible. The revalidation path exists for correctness but almost never executes.

---

## Persistence

### WAL (Write-Ahead Log)
- Every mutation writes a WAL entry before modifying bitmaps
- WAL entries contain: operation type, slot ID, old values (for updates), new values
- WAL is append-only, fsync'd per write or batched based on config
- WAL is the source of truth for crash recovery

### Snapshots
- Built by a **sidecar process** that reads the WAL
- Sidecar maintains its own copy of bitmap state in memory
- As sidecar processes each WAL entry, it applies the mutation to its state and marks that entry as consumed
- Periodically serializes full state to a binary snapshot file
- Snapshot format: roaring bitmaps serialize natively, write them contiguously with a header containing metadata (version, slot counter, field definitions, bitmap counts)
- **Expected snapshot size**: 1-3GB for 150M documents (roaring native serialization + zstd compression)
- Sidecar truncates consumed WAL entries after successful snapshot

### Startup Sequence
1. Load most recent snapshot (seconds off NVMe)
2. Replay any WAL entries newer than the snapshot
3. Begin serving queries
4. Begin consuming new WAL entries from Postgres

---

## Autovac

### Purpose
Clean stale bits from dead slots and recycle slots for reuse.

### Process
1. Runs on a configurable interval (default: hourly)
2. Finds dead slots: `NOT alive AND NOT clean AND slot < counter`
3. For each dead slot: iterate all filter bitmaps and clear that bit position. For low-cardinality fields this is cheap (check ~20 bitmaps). For high-cardinality fields (tags) it's a scan but still fast since each bit check is O(1).
4. Clear sort layer bits for dead slots
5. Set the slot as clean in the clean bitmap
6. Clean bitmap is used by insert path: check for first set bit, reuse that slot

### Performance Impact
Cleaning 10,000 dead slots across a few thousand bitmaps takes seconds. Background operation, no impact on query performance. Even daily runs are sufficient—stale bit accumulation from deletes is negligible relative to bitmap size.

---

## Prometheus Metrics

### Query Performance
- `bitdex_query_duration_seconds` — histogram with p50/p95/p99
- `bitdex_queries_total` — counter
- `bitdex_query_results_count` — histogram of result set sizes
- `bitdex_cache_hit_total` — counter (label: exact|partial|miss)
- `bitdex_cache_size` — gauge (number of cached bitmaps)
- `bitdex_cache_evictions_total` — counter

### Mutation Performance
- `bitdex_mutations_total` — counter (label: put|patch|delete|delete_where)
- `bitdex_mutation_duration_seconds` — histogram
- `bitdex_inflight_writes` — gauge

### WAL
- `bitdex_wal_depth` — gauge (unconsumed entries) ← ALERT ON THIS
- `bitdex_wal_bytes` — gauge (WAL size on disk)
- `bitdex_wal_consumer_lag_seconds` — gauge ← ALERT ON THIS

### Snapshots
- `bitdex_snapshot_age_seconds` — gauge (time since last successful snapshot) ← ALERT ON THIS
- `bitdex_snapshot_duration_seconds` — gauge
- `bitdex_snapshot_size_bytes` — gauge

### Resources
- `bitdex_memory_bytes` — gauge (total RSS)
- `bitdex_bitmap_count` — gauge (label: filter|sort|system)
- `bitdex_bitmap_memory_bytes` — gauge (label: per field)
- `bitdex_alive_count` — gauge (active documents)
- `bitdex_dead_count` — gauge (deleted but not vacuumed)
- `bitdex_clean_count` — gauge (recycled slots available)
- `bitdex_slot_counter` — gauge (total slots ever assigned)

---

## Query API — Plugin Architecture

### Trait Definition

```rust
pub struct BitdexQuery {
    pub filters: Vec<FilterClause>,
    pub sort: Option<SortClause>,
    pub limit: usize,
    pub cursor: Option<CursorPosition>,
}

pub enum FilterClause {
    Eq(String, Value),
    In(String, Vec<Value>),
    Not(Box<FilterClause>),
    And(Vec<FilterClause>),
    Or(Vec<FilterClause>),
    Gt(String, Value),
    Lt(String, Value),
    Gte(String, Value),
    Lte(String, Value),
}

pub struct SortClause {
    pub field: String,
    pub direction: SortDirection,
}

pub enum SortDirection {
    Asc,
    Desc,
}

pub struct CursorPosition {
    pub sort_value: u64,
    pub slot_id: u32,
}

pub trait QueryParser: Send + Sync {
    fn parse(&self, input: &[u8]) -> Result<BitdexQuery, ParseError>;
    fn content_type(&self) -> &str;
}
```

### V2 Scope: JSON Query Parser Only

```json
{
  "filters": {
    "AND": [
      {"field": "nsfwLevel", "op": "eq", "value": 1},
      {"field": "tagIds", "op": "in", "value": [456, 789]},
      {"field": "onSite", "op": "eq", "value": true},
      {"field": "userId", "op": "not_eq", "value": 999}
    ]
  },
  "sort": {"field": "reactionCount", "direction": "desc"},
  "limit": 50,
  "cursor": {"sort_value": 342, "slot_id": 7042}
}
```

OpenSearch DSL and Meilisearch query syntax parsers are future plugins. Do not build in V2.

---

## Data Flow — Civitai Integration

### Ingestion Path
1. Postgres WAL emits change events via logical replication
2. A sidecar WAL consumer translates Postgres row changes into Bitdex mutations:
   - INSERT → PUT
   - UPDATE → PATCH (diff old/new values from WAL event, send only changed fields)
   - DELETE → DELETE
3. Bitdex applies mutations, writes its own WAL, updates bitmaps

### Query Path
1. API server receives user request (e.g., "show me SFW anime images sorted by reactions")
2. API server translates to Bitdex JSON query
3. Bitdex returns ordered list of Postgres IDs
4. API server fetches document data from Redis cache (already exists) or Postgres
5. API server returns full response to user

### Backfill Path
1. Query all image records from Postgres read replica (same data center as Bitdex)
2. Stream in batches of 10k
3. PUT each record into Bitdex
4. After backfill completes, switch WAL consumer to real-time mode

---

## Measured Memory — Civitai Image Index (104.6M records, remapped IDs, 4 threads)

| Scale | Bitmap Memory | RSS | Worst Query p50 |
|------:|-------------:|----:|----------------:|
| 5M | 328 MB | 1.20 GB | 0.83ms |
| 50M | 2.95 GB | 6.09 GB | 13.5ms |
| 100M | 6.19 GB | 11.66 GB | 18.7ms |
| 104.6M | 6.49 GB | 12.14 GB | 21.1ms |

tagIds dominates filter memory at 79-80% across all scales (4.48 GB at 104.6M).
Full benchmark details: `docs/benchmark-report.md`

### Extrapolation to 150M

| Component | Estimated Size |
|---|---|
| Filter bitmaps | ~8.1 GB |
| Sort bitmaps | ~1.1 GB |
| Trie cache | ~160 MB |
| **Total bitmap memory** | **~9.3 GB** |
| **Total RSS** | **~17.4 GB** |

Within the original 7-11 GB bitmap target. RSS overhead ~48% from redb + allocator.
Document store on disk (redb): ~6 GB at 100M records.

---

## V1 Reference — What to Reuse

The [V1 codebase exists as a reference](C:\Dev\Repos\open-source\bitdex\). Here is what to carry forward and what to discard:

### Reuse
- Roaring bitmap filter logic (the filtering worked perfectly in V1)
- WAL consumer / Postgres logical replication integration
- Server scaffolding (HTTP server, config parsing)
- Any Prometheus setup
- Query parser trait if it was started

### Discard — Do NOT bring these into V2
- Vec-based column storage
- Skip lists
- Sorted vectors or arrays
- Forward maps
- Reverse indexes
- Any document/value storage
- Slot recycling via free lists (replaced by clean bitmap)

---

## Development Phases

### Phase 1: Core Engine — COMPLETE ✓ (commit 7bc60fd)
**Goal**: Bitmaps work, mutations work, queries return correct results.

Build: ✓
- ✓ Slot allocation with atomic counter
- ✓ Alive bitmap
- ✓ Filter bitmaps (insert, update, delete operations)
- ✓ Sort layer bitmaps (insert, update with XOR diff)
- ✓ PUT, PATCH, DELETE, DELETE WHERE operations
- ✓ Basic query execution: filter intersection + sort layer traversal
- ✓ JSON query parser
- ✓ Config loading

Test: ✓
- ✓ Insert N documents, verify every filter returns correct results vs brute-force scan
- ✓ Insert, update, delete documents—verify ALL bitmaps are consistent after each op
- ✓ Sort correctness: verify top-N results match a naive sort of the full dataset
- ✓ Cursor pagination: verify page 2 starts exactly where page 1 ended with no gaps or duplicates
- ✓ DELETE WHERE: verify predicate resolution and alive bitmap clearing
- ✓ PATCH: verify only changed fields update, unchanged fields retain correct bits

### Phase 2: Persistence — PARTIAL (commit 8e3c54a)
**Goal**: Crash and restart without data loss.

Build:
- ✓ On-disk document store via redb (embedded key-value store keyed by slot ID)
- ✓ Upsert diffing: read old doc from disk, diff old vs new, update only changed bitmaps
- WAL: append-only log with configurable fsync
- Snapshot serialization (roaring native serialization + zstd)
- Snapshot deserialization and startup loading
- Sidecar snapshot builder that consumes WAL
- WAL truncation after successful snapshot
- Startup sequence: load snapshot → replay WAL → serve

Test:
- ✓ Docstore read/write correctness verified at 104.6M scale
- Write documents, snapshot, restart, verify state matches
- Write documents, kill process mid-mutation, restart from snapshot + WAL replay, verify consistency
- Verify WAL replay is idempotent (replay same entries twice, state is correct)
- Verify sidecar correctly tracks WAL consumption position
- Benchmark snapshot load time (target: seconds for full dataset)
- Benchmark snapshot size (target: 1-3GB for 150M docs)

### Phase 3: Performance — COMPLETE ✓ (commits 95df2a5 through 1acfad7)
**Goal**: Sub-millisecond queries, microsecond writes at 150M scale.

Build: ✓
- ✓ Cardinality-based query planning (sort filter clauses by estimated size) — planner.rs
- ✓ Trie cache with LRU eviction — cache.rs
- ✓ Trie prefix matching for partial cache hits
- ✓ Cache invalidation via generation counters on filter fields
- ✓ Automatic promotion/demotion with exponential decay hit stats
- ✓ Concurrent engine with RwLock-based read/write separation — concurrent_engine.rs
- ✓ Write coalescing via crossbeam channels with batched flush loop — write_coalescer.rs
- ✓ Arc<str> field name interning for zero-copy mutation ops
- ✓ Benchmark harness with 20 query types, memory reporting, multi-stage benchmarking

Test: ✓
- ✓ Benchmarked at 5M, 50M, 100M, and 104.6M (full Civitai dataset)
- ✓ Filter-only queries: 0.034ms (userId) to 5ms (dense bitmaps) at 104.6M
- ✓ Filter+sort queries: 7-21ms at 104.6M (sort layer traversal dominates at scale)
- ✓ Cache prefix matching verified: trie cache grows from 0 to 111 MB for 10 entries
- ✓ Memory scaling verified linear: ~62 bytes/record bitmap memory across all scales
- ✓ Full results: docs/benchmark-report.md

### Phase 4: Operations
**Goal**: Production-ready observability and resilience.

Build:
- Prometheus metrics endpoint (all metrics listed above)
- Autovac process (dead slot cleanup, clean bitmap production)
- Admin API (config reload, cache stats, force snapshot, force autovac)
- Graceful shutdown (flush WAL, snapshot)
- Health check endpoint

Test:
- Verify all Prometheus metrics are emitted and accurate
- Autovac: delete 10k documents, run autovac, verify bitmaps are cleaned and slots are recycled
- Verify new inserts reuse cleaned slots before appending
- Verify hot config reload works without restart

### Phase 5: Integration
**Goal**: Running in production on Civitai.

Build:
- Postgres WAL consumer sidecar (translates Postgres logical replication to Bitdex mutations)
- Backfill pipeline (batch load from Postgres replica)
- API server integration (translate application queries to Bitdex JSON format)
- Shadow mode: run queries against both Bitdex and Postgres, diff results, log discrepancies

Test:
- Shadow mode differential testing: run identical queries against Bitdex and Meilisearch for 1 week, verify zero result discrepancies
- Backfill correctness: load full dataset, spot-check random documents against Postgres
- WAL consumer lag under production write load
- End-to-end latency: API request → Bitdex query → document fetch → API response
- Failover: kill Bitdex, verify API falls back to Postgres gracefully

---

## Testing Strategy

### Unit Tests
Every bitmap operation gets a unit test. Insert a document, verify the correct bits are set in the correct bitmaps. Update a field, verify old bits cleared and new bits set. Delete, verify alive bit cleared. These are your foundation—if bitmap state is correct, everything else works.

### Property-Based Tests
Use `proptest` or `quickcheck` to generate random documents, random mutations, and random queries. After every operation, verify that a brute-force scan of all bitmaps produces the same result as the query engine. This catches edge cases that hand-written tests miss.

### Fuzz Testing
Fuzz the JSON query parser with arbitrary input. Fuzz the mutation API with random payloads. Nothing should panic or corrupt state.

### Shadow Mode (Critical)
This is your most important test. Run every production query against both Bitdex and Meilisearch. Diff the results. Log every discrepancy. Do NOT trust Bitdex in production until shadow mode has run for at least one week with zero discrepancies. This catches subtle bugs where bitmap state drifts from truth due to edge cases in WAL consumption, race conditions, or encoding issues.

### Benchmarks
Maintain a benchmark suite that runs on every PR:
- Filter query latency at 1M, 10M, 50M, 150M documents
- Filter+sort query latency at same scales
- Write throughput (inserts/sec, patches/sec)
- Mixed read/write throughput
- Memory usage at each scale
- Snapshot save/load time

Track regressions. Any PR that degrades benchmarks by >10% gets flagged.

### Chaos Testing
- Kill the process during writes, restart, verify consistency
- Fill the WAL without snapshotting, verify recovery
- Exhaust memory (load more docs than fit), verify graceful degradation
- Corrupt the snapshot file, verify fallback to WAL-only recovery

---

## Team Structure Recommendation

### Agent 1: Core Engine
Owns: Slot allocation, alive bitmap, filter bitmaps, sort layer bitmaps, mutation API (PUT/PATCH/DELETE/DELETE WHERE), query execution, query planning, JSON query parser.

This is the heart of Bitdex. This agent should focus on correctness first, performance second. Every bitmap operation needs exhaustive unit tests and property-based tests.

### Agent 2: Persistence & Operations
Owns: WAL, snapshot serialization/deserialization, sidecar snapshot builder, startup sequence, autovac, clean bitmap, Prometheus metrics, admin API, config system.

This agent builds the shell around the core. It should be able to work with a mock/simple core engine initially and integrate with the real one once Agent 1 has stable APIs.

### Agent 3: Cache & Concurrency
Owns: Trie cache, LRU eviction, prefix matching, promotion/demotion, generation counters, optimistic concurrency (in-flight write tracking, reader validation).

This can be built independently and layered on top of the core engine. Agent 3 should start after Agent 1 has basic query execution working.

### Agent 4: Integration & Testing
Owns: Postgres WAL consumer, backfill pipeline, shadow mode, end-to-end tests, benchmark suite, chaos tests.

This agent validates that everything works together and works correctly against real data. Should start building the shadow mode infrastructure and benchmark suite early, then integrate as other agents deliver components.

### Audit Practices
- Every PR must include tests for the code it adds
- Agents should review each other's PRs—especially Agent 4 reviewing Agents 1-3
- Weekly: run full benchmark suite, review metrics for regressions
- Before each phase gate: Agent 4 runs the full test suite including property tests and chaos tests
- Before production: minimum 1 week of shadow mode with zero discrepancies

---

## Key Design Decisions — Do Not Deviate

1. **Documents stored on disk only.** An embedded key-value store (redb) stores documents keyed by slot ID for upsert diffing and optional doc serving. No in-memory document storage. Bitmaps are the in-memory index.

2. **No sorted data structures.** No sorted Vecs, no skip lists, no B-trees for maintaining sort order. Sorting is done via bit-layer bitmap traversal. Period.

3. **No in-memory forward maps or reverse indexes.** The on-disk document store replaces the need for these. On upsert, read old doc from disk to determine which bitmaps to update. For DELETE WHERE on high-cardinality fields, scan the bitmaps.

4. **Deletes only clear the alive bit.** No cleanup of other bitmaps on delete. Autovac handles that in the background.

5. **Slot = Postgres ID** for integer ID users. No mapping layer.

6. **Full precision sort layers first.** Do not implement log encoding or reduced bit depths until benchmarks prove it's necessary.

7. **JSON query parser only for V2.** OpenSearch and Meilisearch syntax plugins are future work.

8. **Single process, single node.** No clustering, no replication, no distributed consensus. A Postgres fallback in the API layer handles the (rare) downtime during restarts.

---

## Future Roadmap (Not V2 Scope)

- **LSH vector similarity**: Binary hashing of embeddings stored as bitmaps. Hamming distance via XOR+popcount fused with filter bitmaps in a single pass. Approximate top-N candidates reranked by exact cosine from doc store.
- **Postgres extension**: Port core bitmap engine into a pgrx extension. Custom index access method. No separate service to manage.
- **Log encoding for sort fields**: Configurable encoding to reduce bit layers for power-law distributed fields.
- **OpenSearch/Meilisearch query parser plugins**
- **Visual bitmap explorer**: WebGL canvas rendering bitmaps as pixel rows for data exploration and business intelligence.
- **Multi-index support**: Separate Bitdex instances for images, models, posts, users.