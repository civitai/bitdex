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

### Core Architecture

| System | Design Doc | Covers |
|--------|-----------|--------|
| Storage | `docs/design/storage.md` | ShardStore (bitmap persistence), DocStore V2 (append-only tuples), BitmapFs (legacy), BoundStore (cache persistence) |
| Concurrency | `docs/design/concurrency.md` | ArcSwap snapshots, Arc-per-bitmap CoW, flush/merge threads, loading mode, lazy loading |
| Unified Cache | `docs/design/unified-cache.md` | Cache + persistence (BoundStore), radix bucketing, live maintenance, LRU eviction |
| Idle Eviction | `docs/design/idle-eviction.md` | Per-value bitmap eviction for multi_value fields |
| Radix Sort | `docs/design/radix-sort-trie.md` | 8-bit radix bucketing for large cache entries |
| Rolling Restart | `docs/design/rolling-restart-cursors.md` | Named cursors for zero-downtime restarts |

### Sync V2 Pipeline

| System | Design Doc | Covers |
|--------|-----------|--------|
| Sync V2 Design | `docs/design/pg-sync-v2-final.md` | Config-driven dump + WAL-based steady-state |
| Implementation Plan | `docs/design/sync-v2-final-implementation-plan.md` | Task tracker with checkboxes |
| Trigger Deployment | `docs/design/trigger-deployment-process.md` | PG trigger generation, review, deploy, cleanup |
| Field Requirements | `docs/design/civitai-field-requirements.md` | Ground truth: 22 filters, 5 sorts, 30 doc fields |

### Data Silos (Proposed)

| System | Design Doc | Covers |
|--------|-----------|--------|
| Architecture | `docs/design/data-silo-architecture.md` | Per-thread silos replacing DocStore V2 (4.8M/s, 11x faster) |
| Implementation Plan | `docs/design/data-silo-implementation-plan.md` | 6-phase task tracker |
| Benchmarks | `docs/benchmarks/data-silo/benchmark-plan.md` | 5 experiments with goal thresholds |

### Operations

| System | Design Doc | Covers |
|--------|-----------|--------|
| Production Readiness | `docs/design/production-readiness-checklist.md` | Gate tracker for sync-v2 deploy |
| Runtime Config | `docs/design/runtime-config-reference.md` | All 30+ configurable settings |
| Bitmap Memory Scanner | `docs/design/amortized-bitmap-memory-scanner.md` | Fix for 52s /metrics stall |

### Archived Docs

Historical V1 designs and superseded proposals are in `docs/design/archive/`. Check there before proposing something that may have been tried before.

## Learnings (What We Tried That Didn't Work)

- `docs/learnings/write-pipeline.md` — Loading mode vs adaptive pressure, persist thread, bulk accumulator
- `docs/learnings/storage.md` — Lazy loading vs tiered caching, redb vs custom filesystem
- `docs/learnings/ingestion.md` — Parsing bottlenecks, simd-json/rkyv evaluation
- `docs/reviews/liz-dump-perf-session.md` — 10-hour perf session: 6 docstore approaches tried, only filter_only worked
- `docs/reviews/josh-dump-processor-session.md` — Dump processor iterations, what failed and why

## Session Reviews (Understand WHY)

When you need context on why something was built a certain way, check `docs/reviews/` for session extraction docs that capture decisions, gotchas, and regression risks from agent conversations.

## After Changing Architecture

When you modify a system's architecture, you MUST:

1. **Update the design doc** if you changed how a subsystem works
2. **Update `docs/guide/api.md`** if you changed HTTP endpoints
3. **Update `docs/guide/config-schema.md`** if you changed config fields
4. **Add to learnings** if you tried an approach and rejected it
5. **Notify Dakota (Doc Keeper)** via mailbox — send findings, benchmark data, and session ID for review
6. **Follow the design process** in `docs/guide/team-standards.md` for new proposals

## Team Standards

Read `docs/guide/team-standards.md` for the full design process: Capture → Document → Review → Benchmark → Plan Review → Implement → Validate at Scale.
