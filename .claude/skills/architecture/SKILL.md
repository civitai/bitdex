---
name: architecture
description: Architecture reference and design doc guide. Use when changing system architecture, proposing new subsystems, or needing to understand why existing systems work the way they do. Points to design docs and explains the responsibility to keep them updated.
disable-model-invocation: false
user-invocable: true
---

# Architecture & Design Reference

Guide for understanding, modifying, and documenting BitDex's architecture.

## Before Changing Architecture

**Read the relevant design doc first.** These capture the rationale behind decisions and prevent re-inventing approaches that were already evaluated.

| System | Design Doc | Covers |
|--------|-----------|--------|
| Concurrency | `docs/design/design-concurrency.md` | ArcSwap snapshots, Arc-per-bitmap CoW, VersionedBitmap diffs, flush/merge threads, loading mode, lazy loading |
| Storage | `docs/design/design-storage.md` | BitmapFs (hex-bucketed bitmap persistence), DocStore (sharded zstd-compressed msgpack), persistence lifecycle |
| Unified Cache | `docs/design/design-unified-cache-final.md` | Cache architecture consolidating filter+sort+time bucket caching |
| Cache Persistence | `docs/design/design-unified-cache-persistence.md` | BoundStore for warm cache restarts (APPROVED, not yet built) |
| Idle Eviction | `docs/design/design-idle-eviction.md` | Per-value bitmap eviction for multi_value fields |
| Radix Sort | `docs/design/design-radix-sort-trie.md` | 8-bit radix bucketing for large cache entries |
| Rolling Restart | `docs/design/design-rolling-restart-cursors.md` | Named cursors for zero-downtime restarts |

## Learnings (What We Tried That Didn't Work)

Check these before proposing alternatives — we may have already tried and rejected the approach:

- `docs/learnings/write-pipeline.md` — Loading mode vs adaptive pressure, persist thread, bulk accumulator
- `docs/learnings/storage.md` — Lazy loading vs tiered caching, redb vs custom filesystem
- `docs/learnings/ingestion.md` — Parsing bottlenecks, simd-json/rkyv evaluation

## Design Conversations (Understand WHY)

- `docs/_in/architecture-conversations.md` — Merged design conversations covering evolution from OpenSearch to bitmaps, slot model, sort layers, meta-index, bound cache, time buckets, bulk loading. Has a navigable summary with line references.
- `docs/_in/prepared-prompt.md` — Authoritative specification with complete architecture, API specs, config schemas, testing strategy, and development phases.
- `docs/_in/storage-overhaul.md` — Requirements for the redb-to-filesystem pivot

## Known Gaps

- `docs/to-resolve/cache-maintenance-scaling.md` — Clause narrowing limits, budget cap tradeoffs, remaining O(N) scans

## After Changing Architecture

When you modify a system's architecture, you MUST update the corresponding documentation:

1. **Update the design doc** (`docs/design/`) if you changed how a subsystem works. Add a section describing what changed and why.
2. **Update `docs/guide/api.md`** if you added, changed, or removed HTTP endpoints.
3. **Update `docs/guide/config-schema.md`** if you added or changed config fields.
4. **Add to learnings** (`docs/learnings/`) if you tried an approach and rejected it. Future agents need to know what didn't work and why.
5. **Create a new design doc** if you're building a new subsystem. Follow the existing format in `docs/design/`.
6. **Update `docs/README.md`** if you created new doc files.

## External References

- **V1 Codebase**: `C:\Dev\Repos\open-source\bitdex\` — Reference for reusable code (filter bitmaps, WAL consumer, server scaffolding). DO NOT bring over Vecs, skip lists, sorted arrays, forward maps, or reverse indexes.
- **Full docs folder structure**: `docs/README.md`
