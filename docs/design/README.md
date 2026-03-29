# Design Docs Index

Active design documents for BitDex V2. Grouped by subsystem, with status tags from frontmatter.

For archived/historical docs (V1 designs, superseded proposals), see [`archive/`](archive/).

## Engine Core

| Doc | Status | Description |
|-----|--------|-------------|
| [storage.md](storage.md) | IMPLEMENTED | ShardStore + DocStore V2: persistence, generation model, compaction |
| [concurrency.md](concurrency.md) | IMPLEMENTED | ArcSwap snapshots, flush thread, merge thread, in-flight tracking, loading mode |
| [filter-only-fields.md](filter-only-fields.md) | ACTIVE | Filter-only field concept: indexed for filtering but not stored in docstore |
| [deferred-alive-scheduled-posts.md](deferred-alive-scheduled-posts.md) | ACTIVE | Scheduled post visibility via deferred alive bitmap |
| [amortized-bitmap-memory-scanner.md](amortized-bitmap-memory-scanner.md) | APPROVED | Amortized /metrics scanner replacing 52s full-bitmap serialization |
| [system-map.md](system-map.md) | ACTIVE | System component map (WARNING: incomplete, see CLAUDE.md for full picture) |

## Caching & Eviction

| Doc | Status | Description |
|-----|--------|-------------|
| [unified-cache.md](unified-cache.md) | IMPLEMENTED | Unified query cache: flat HashMap, dynamic capacity, live maintenance, LRU eviction, BoundStore persistence |
| [radix-sort-trie.md](radix-sort-trie.md) | IMPLEMENTED | 8-bit radix bucketing for sort cache, Phase 1 complete |
| [idle-eviction.md](idle-eviction.md) | IMPLEMENTED | Per-value idle eviction for multi-value filter fields (idle_seconds config) |

## Sync V2

| Doc | Status | Description |
|-----|--------|-------------|
| [pg-sync-v2-final.md](pg-sync-v2-final.md) | ACTIVE | Design spec: config-driven dump pipeline + ops-based steady-state sync |
| [sync-v2-final-implementation-plan.md](sync-v2-final-implementation-plan.md) | ACTIVE | Task tracker for sync V2 implementation |
| [trigger-deployment-process.md](trigger-deployment-process.md) | ACTIVE | PG trigger generation, deployment, testing, and cleanup process |
| [rolling-restart-cursors.md](rolling-restart-cursors.md) | IMPLEMENTED | Named cursor lifecycle, MetaStore persistence, PG cleanup triggers |
| [e2e-validation-checklist-sync-v2.md](e2e-validation-checklist-sync-v2.md) | ACTIVE | 158-item QA validation checklist with test procedures |
| [production-readiness-checklist.md](production-readiness-checklist.md) | ACTIVE | Deployment gate tracker: status, owners, blockers |

## Data Silos

| Doc | Status | Description |
|-----|--------|-------------|
| [data-silo-architecture.md](data-silo-architecture.md) | PROPOSED | Per-thread data silos replacing DocStore V2 |
| [data-silo-implementation-plan.md](data-silo-implementation-plan.md) | ACTIVE | Phased task list for data silo implementation |

## Field Requirements

| Doc | Status | Description |
|-----|--------|-------------|
| [civitai-field-requirements.md](civitai-field-requirements.md) | APPROVED | Ground truth for Civitai fields: types, sources, filter/sort config |

## Operations & Performance

| Doc | Status | Description |
|-----|--------|-------------|
| [runtime-config-reference.md](runtime-config-reference.md) | ACTIVE | All configurable settings: startup, runtime-patchable, env vars, CLI args |
| [performance-tuning-roadmap.md](performance-tuning-roadmap.md) | ACTIVE | Remaining optimization opportunities, ordered by impact (5 done, 4 remaining) |
