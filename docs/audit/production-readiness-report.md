# Production Readiness Audit — Consolidated Report

**Date:** 2026-03-13
**Agents:** 4 independent auditors (Hot Path, Write Path, Memory, Reliability)
**Total findings:** 55 across all agents, deduplicated and cross-referenced below

---

## Tier 1: Fix Before Ship (Critical — data loss, crashes, or correctness)

These issues can cause data loss, silent corruption, or production crashes. Non-negotiable fixes.

### 1. No Graceful Shutdown Signal Handling
**Found by:** Agent D (F3)
**Severity:** CRITICAL
**Files:** `src/bin/server.rs:52-63`, `src/server.rs:454-456`

No SIGTERM/SIGINT handler. Every deployment/restart silently drops in-flight mutations. The `ConcurrentEngine::Drop` and `shutdown()` methods exist but never get called because the process exits before axum yields control. Combined with `panic = "abort"` (Cargo.toml:92), Drop handlers don't run even on panics.

**Fix:** Add `tokio::signal::ctrl_c()` + SIGTERM handler. Use `axum::serve(...).with_graceful_shutdown(signal)`. On signal: stop accepting requests → drain flush channel → call `engine.shutdown()` → force final snapshot save.

**Effort:** Low (1-2 hours)

---

### 2. Missing fsync in All Atomic Write Paths
**Found by:** Agent B (#3), Agent D (F4) — independently discovered
**Severity:** CRITICAL
**Files:** `src/bitmap_fs.rs:65-79` (`write_bitmap_atomic`), `src/bitmap_fs.rs:94-105` (`write_bytes_atomic`), `src/docstore.rs:434-439` (`write_shard_file`), `src/docstore.rs:166-170` (`save_field_dict`)

Code comments claim "atomic tmp→fsync→rename" but fsync is never called. `std::fs::write` + `std::fs::rename` without fsync means on power loss the renamed file can have empty/corrupted contents. Affects ALL bitmap persistence: filter bitmaps, sort layers, alive bitmap, slot counter, cursors, docstore shards.

**Fix:** After `std::fs::write`, open the file, call `.sync_all()`, then rename. Also fsync the parent directory after rename.

**Effort:** Low (30 min — mechanical change in 2 helper functions)

---

### 3. Flush/Merge Thread Panics Silently Swallowed
**Found by:** Agent D (F5)
**Severity:** CRITICAL
**Files:** `src/concurrent_engine.rs:383` (flush), `src/concurrent_engine.rs:894` (merge)

Background threads spawned with bare `thread::spawn`. If flush thread panics: mutation channel fills up, all writes block, queries see stale data forever, zero logging. With `panic = "abort"` the process dies instantly. Without abort, the server appears healthy but is non-functional — `/api/health` still returns 200.

**Fix:** (1) Install a panic hook for diagnostics. (2) Add flush thread heartbeat (atomic counter bumped each cycle). (3) `/api/health` checks heartbeat is advancing. (4) On flush thread death, set engine to degraded state and log error.

**Effort:** Medium (half day)

---

### 4. Docstore Write Errors Silently Dropped
**Found by:** Agent D (F6)
**Severity:** HIGH
**Files:** `src/concurrent_engine.rs:837-839`, also lines 873-875, 2788-2789, 2795-2796

When flush thread docstore write fails, bitmap mutations have already been applied and published. Result: queries return IDs for docs that don't exist in the docstore. Subsequent upserts can't diff against old doc (treated as fresh insert). Clean deletes can't read the doc to clear filter bitmaps. Only evidence is an `eprintln!` to stderr.

**Fix:** (1) Prometheus counter for docstore write failures. (2) Retry buffer for failed batches. (3) Consider reverting bitmap mutations on docstore failure, or at minimum track affected slot IDs for later reconciliation.

**Effort:** Medium

---

### 5. Delete Missing In-Flight Marker — Race Condition
**Found by:** Agent B (#7), Agent D (implicit)
**Severity:** HIGH
**Files:** `src/concurrent_engine.rs:1243-1287`

`put()` marks slots in-flight (line 1160) but `delete()` does not. Concurrent put+delete on the same slot: delete reads old doc, put sends new bitmap ops, delete sends remove ops that clear the bits just set by put. Result: stale bitmap state.

**Fix:** Add `self.in_flight.mark_in_flight(id)` at start of `delete()` and `clear_in_flight(id)` at end, matching `put()` pattern.

**Effort:** Very low (2 lines)

---

### 6. `expect()` in Loader Thread Kills Server
**Found by:** Agent D (F2)
**Severity:** HIGH
**Files:** `src/loader.rs:145`, `src/loader.rs:190`

`File::open(&path).expect(...)` in `thread::spawn` — API-triggered load with invalid path kills the entire process. With `panic = "abort"`, no cleanup runs.

**Fix:** Replace `expect()` with `?` or `map_err`. Send errors through the channel instead of panicking.

**Effort:** Very low (15 min)

---

### 7. Lazy Load Snapshot Publish Race Condition
**Found by:** Agent C (F7)
**Severity:** HIGH
**Files:** `src/concurrent_engine.rs:1424-1425`

Query threads that trigger lazy per-value loads clone the snapshot, modify it, and publish via `inner.store()`. Between `load_full()` and `store()`, the flush thread may have published a newer snapshot. The query thread's publish clobbers the flush thread's mutations.

**Fix:** Remove query-thread publish path. Send all loaded data exclusively through `lazy_tx` channel. Let only the flush thread publish snapshots.

**Effort:** Medium

---

## Tier 2: Fix Soon After Ship (Performance — measurable impact at scale)

These are optimizations that will measurably improve throughput, latency, or resource usage. Won't cause crashes, but leaving them means leaving performance on the table.

### 8. Unified Cache Mutex Serializes All Query Threads
**Found by:** Agent A (#5)
**Severity:** HIGH for performance
**Files:** `src/concurrent_engine.rs:1683-1696`

Every sorted query acquires a `parking_lot::Mutex` for cache lookup. Bitmap clones happen while holding the lock. At 100+ concurrent readers, this is the primary query throughput bottleneck.

**Fix:** `RwLock` for the cache (lookups are reads). Store entries behind `Arc<UnifiedEntry>` so lookup returns an Arc clone instead of bitmap clone. Use `AtomicU64` for the LRU `last_used` timestamp.

**Effort:** Medium

---

### 9. Multi-Value Write Amplification (10-30x)
**Found by:** Agent B (#1)
**Severity:** HIGH for write throughput
**Files:** `src/mutation.rs:162-181`

When any value in a multi-value field changes (e.g., 1 of 30 tagIds), the diff emits remove-all + insert-all instead of a symmetric difference. A single-tag change generates 60 MutationOps instead of 2. tagIds is 79% of filter bitmap memory — this dominates coalescer and flush thread work under CDC.

**Fix:** Compute symmetric difference: only emit ops for values in old-but-not-new and new-but-not-old. HashSet intersection approach.

**Effort:** Low-medium

---

### 10. Alive Bitmap Full Clone on Empty-Filter Queries
**Found by:** Agent A (#2)
**Severity:** HIGH for query latency
**Files:** `src/executor.rs:293`

"Show all docs sorted by X" queries deep-clone the alive bitmap. At 105M = ~13MB. At 500M = ~60MB. These are the most common infinite-scroll queries.

**Fix:** Return `Arc<RoaringBitmap>` or `Cow<RoaringBitmap>` from `compute_filters`. The alive bitmap is already Arc-wrapped inside VersionedBitmap — this can be zero-copy.

**Effort:** Low

---

### 11. AND Chain Allocates New Bitmap Per Step
**Found by:** Agent A (#8)
**Severity:** MEDIUM
**Files:** `src/executor.rs:296-306`

`existing & &bitmap` (allocating) instead of `existing &= &bitmap` (in-place). Each AND step allocates a new multi-MB bitmap.

**Fix:** One-character change: `&` → `&=`

**Effort:** Very low (literally 1 character)

---

### 12. Alive Bitmap Clone on Every Negation Clause
**Found by:** Agent A (#3)
**Severity:** MEDIUM
**Files:** `src/executor.rs:337-339, 367-370, 377-379`

NotEq/Not/NotIn all clone the full alive bitmap. When there's already an accumulated AND result, `accumulated &! negated` is correct and avoids the alive clone entirely.

**Fix:** Use the accumulated result (if present) instead of alive for negation. Only fall back to alive for the first clause.

**Effort:** Low

---

### 13. Existence Set Clone-Per-Value During Ingestion
**Found by:** Agent B (#5), Agent C (F1) — independently discovered
**Severity:** MEDIUM
**Files:** `src/concurrent_engine.rs:462-470`

Each new distinct value triggers a full clone of the ~31K-entry HashSet. During bulk ingestion with 100 new tags, that's 100 clones of ~500KB each.

**Fix:** Batch all new values from a single flush cycle into one clone+insert+store.

**Effort:** Very low

---

### 14. Docstore Mutex Contention (Read vs Write vs Flush)
**Found by:** Agent B (#4), Agent D (F16)
**Severity:** MEDIUM
**Files:** `src/concurrent_engine.rs:1177, 1245, 837`

Every put/delete blocks on the same Mutex as the flush thread's batch writes. Under sustained CDC pressure, this serializes all write-path disk I/O.

**Fix:** RwLock — reads (get) can happen concurrently; only batch writes need exclusive access.

**Effort:** Low

---

### 15. Cache Key Canonicalization String Allocations
**Found by:** Agent A (#4)
**Severity:** MEDIUM
**Files:** `src/cache.rs:20-114`

Every query allocates multiple Strings for cache key construction: field.clone(), "eq".to_string(), value formatting. At 10K+ QPS, this is significant allocator pressure.

**Fix:** Use `&'static str` for operator names. Pre-intern field names. Consider incremental hashing without materializing intermediate Strings.

**Effort:** Medium

---

## Tier 3: Improve When Convenient (Polish — operational quality)

### 16. Replace All `eprintln!` with `tracing` (Agent D, F10)
183 occurrences across 15 files. `tracing` is already a dependency. No structured logging, no log levels, no timestamps. Severely impairs production debugging.

### 17. Path Traversal in Load Endpoint (Agent D, F7)
`/api/indexes/{name}/load` accepts arbitrary filesystem paths from HTTP request body. Validate path is under an allowed directory.

### 18. Merge Thread Writes ALL Filter Bitmaps on Every Dirty Cycle (Agent B, #12)
Even when only a handful of bitmaps changed, all loaded filters are serialized and written. Add per-field dirty-for-merge tracking.

### 19. Unbounded Lazy Load Channel (Agent C, F2, Agent D, F12)
Could buffer multi-GB payloads at startup under concurrent queries. Use bounded channel or per-field load lock.

### 20. Merge Thread Deep-Clones Snapshot for Persist (Agent C, F3)
`fields_mut()` triggers `Arc::make_mut` deep copies. Serialize directly from Arc refs instead.

### 21. Request Body Size Limits (Agent D, F8)
No `RequestBodyLimitLayer`. Add configurable limits per endpoint type.

### 22. Eviction Stamps DashMap Unbounded Growth (Agent C, F5)
Stamps accumulate for all ever-queried values. Cache `Arc<str>` field names, add periodic cleanup.

### 23. Health Endpoint Should Check Engine Health (Agent D, F14)
Currently returns 200 unconditionally. Should check flush thread heartbeat, disk space, docstore health.

### 24. `unwrap()` Race in Upsert Handler (Agent D, F9)
`guard.as_ref().unwrap()` after re-acquiring lock — index could be deleted between acquisitions.

### 25. MutationOp Vec<u32> Allocations (Agent B, #9)
Every single-slot op heap-allocates a `Vec<u32>` with 1 element. Use `SmallVec<[u32; 1]>`.

### 26. Cursor Name Path Injection (Agent D, F15)
HTTP cursor names used directly in filesystem paths. Validate alphanumeric+hyphens only.

---

## Quick Wins (< 1 hour each, ship-blocking or high-value)

| # | What | Effort | Impact |
|---|------|--------|--------|
| 5 | Delete in-flight marker | 2 lines | Correctness fix |
| 6 | Loader expect → Result | 15 min | Prevents crash |
| 11 | `&` → `&=` in AND chain | 1 character | Eliminates per-clause allocation |
| 13 | Batch existence set updates | 15 min | 100x fewer HashSet clones during ingestion |
| 2 | Add fsync to atomic writes | 30 min | Crash safety |
| 1 | Graceful shutdown handler | 1-2 hr | Prevents data loss on deploy |

---

## Full Finding Cross-Reference

| Finding | Agent A | Agent B | Agent C | Agent D |
|---------|---------|---------|---------|---------|
| Missing fsync | — | #3 | — | F4 |
| Graceful shutdown | — | — | — | F3 |
| Flush thread panic handling | — | — | — | F5 |
| Docstore write errors dropped | — | — | — | F6 |
| Delete in-flight race | — | #7 | — | — |
| Loader expect panics | — | — | — | F2 |
| Lazy load publish race | — | — | F7 | — |
| Cache mutex serialization | #5 | — | — | — |
| Multi-value write amplification | — | #1 | — | — |
| Alive bitmap clone (empty filter) | #2 | — | — | — |
| AND chain allocation | #8 | — | — | — |
| Negation alive clone | #3 | — | — | — |
| Existence set clone-per-value | — | #5 | F1 | — |
| Docstore mutex contention | — | #4 | — | F16 |
| Cache key string allocs | #4 | — | — | — |
| Unbounded lazy channel | — | — | F2 | F12 |
| Merge thread snapshot clone | — | #10 | F3 | — |
| eprintln everywhere | — | — | — | F10 |
| Path traversal in load | — | — | — | F7 |
| Merge writes all bitmaps | — | #12 | — | — |
| Eviction stamps unbounded | — | — | F5 | — |
| Request body limits | — | — | — | F8 |
| Health check shallow | — | — | — | F14 |
| Upsert handler unwrap race | — | — | — | F9 |
| Cursor name injection | — | — | — | F15 |

---

## Individual Agent Reports
- [Agent A — Hot Path & Query Performance](agent-a-hot-path.md) (15 findings)
- [Agent B — Write Path & Mutations](agent-b-write-path.md) (12 findings)
- [Agent C — Memory & Resource Management](agent-c-memory.md) (12 findings)
- [Agent D — Reliability & Error Handling](agent-d-reliability.md) (16 findings)
