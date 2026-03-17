# BitDex Project Handoff

> **For humans:** Have your AI agent read this document and the linked references before starting work. The agent should also run `/dev-guide` and `/architecture` to load the full project context.

---

## Quick Start for Agents

1. Read `CLAUDE.md` — design principles, architecture overview, coding standards
2. Run `/dev-guide` — phase status, what's built vs not, workflow expectations
3. Run `/architecture` — design docs, known gaps, update expectations
4. Read this document — operational context, common pitfalls, who to ask

---

## What BitDex Does

BitDex is a bitmap index engine for Civitai. It takes filter predicates + sort parameters and returns ordered IDs. Everything is roaring bitmaps — filtering, sorting, caching.

**Production scale:** 107M+ images, 2 replicas, 8 CPU each, serving live traffic via shadow mode comparison with Meilisearch.

**The pipeline:**
```
PG tables → CSV dump → bulk loader → bitmaps + docstore on disk
                                          ↓
                              server loads from disk on startup
                                          ↓
                          pg-sync sidecar keeps data current via outbox polling
```

---

## Key Architectural Decisions (What Agents Get Wrong)

### DocStore V2 — NOT compressed msgpack
`CLAUDE.md` says "zstd-compressed msgpack." That's the V1 format. **Production uses V2:** append-only tuple logs, no compression, LIFO scan for reads. V2 is 215x faster at p50. The V1 format is still referenced in code comments but V2 is what runs. See `src/docstore.rs`.

### Three Docstore Write Paths — Must Stay In Sync
These all write to the docstore and must produce identical field names/types:
1. **`src/pg_sync/single_pass.rs`** — bulk loader (CSV → bitmaps + docstore). Production path.
2. **`src/pg_sync/bulk_loader.rs`** — older loader, not used in production
3. **`src/pg_sync/row_assembler.rs`** — outbox poller (PG → PATCH/PUT)

If you change field names, conversions (ms_to_seconds), or types (exists_boolean) in one path, you MUST update all three.

### Source vs Target Field Names
Schema mappings have `source` (PG column name, e.g. `publishedAtUnix`) and `target` (BitDex field name, e.g. `publishedAt`). The bulk loader stores under target names with conversions applied (as of v1.0.55+). The outbox poller stores under target names via `json_to_document_with_dicts`. The doc serving path (`format_document` in `server.rs`) looks up by target name first, then source name, and only applies ms_to_seconds when found by source name.

### Cursor Lifecycle — The Hardest Part
The pg-sync outbox poller tracks its position via a cursor stored in `bitmaps/cursors/pg-sync-bitdex-{0,1}`. This cursor is:
- Set in memory by the bulk loader via `engine.set_cursor()`
- Checkpointed to disk by the merge thread periodically
- Read from disk by the engine on startup (`BitmapFs::load_all_cursors`)
- Read by pg-sync from the engine via HTTP GET `/cursors/{name}`
- Advanced by pg-sync on every batch via HTTP PATCH/PUT with cursor parameter
- Also written to PG `bitdex_cursors` table for outbox cleanup

**Critical:** The bulk loader seeds the cursor at the current outbox head, not at CSV dump time. After a reload, you must manually reset the cursor to the CSV dump time. See the reload docs.

### Flux Manages K8s State
The BitDex StatefulSet is managed by Flux CD from the `talos-infra` repo. Any `kubectl` changes (scale, image, memory limits) get overwritten by Flux within ~30s-5min. To make persistent changes, either:
- Push to `talos-infra` (Arabella manages this repo)
- Suspend Flux via git push (not kubectl patch — it gets overwritten)

---

## Production Operations

### Deployed Versions
- **Server + pg-sync:** `ghcr.io/civitai/bitdex:1.0.57` (as of 2026-03-17)
- **K8s namespace:** `bitdex`
- **StatefulSet:** 2 replicas, each with `bitdex` (server) + `pg-sync` (sidecar) containers
- **Node:** `talos-fq9-f3k`
- **PG replica:** `cnpg-cluster-nvme0-1` in `cnpg-database` namespace

### Key Endpoints
- Health: `GET /api/health`
- Query: `POST /api/indexes/{name}/query?format=compact`
- Stats: `GET /api/indexes/{name}/stats`
- Traces: `GET /api/indexes/{name}/traces`
- Cursors: `GET /api/indexes/{name}/cursors/{cursor_name}`
- Set cursor: `PUT /api/indexes/{name}/cursors/{cursor_name}` (admin)

### Monitoring
- **pg-sync health:** `node .claude/skills/deploy/cli.mjs pg-sync-health`
- **Pod status:** `node .claude/skills/deploy/cli.mjs status`
- **Logs:** `node .claude/skills/deploy/cli.mjs pg-sync-logs [pod] [lines]`

### Bulk Reload
See `docs/bulk-load-handoff.md` for the full procedure. Key points:
- Suspend Flux via git push FIRST (not kubectl)
- Dump CSVs directly on the PG pod (not via COPY TO STDOUT over network)
- The bulk loader seeds the wrong cursor — must be manually reset after load
- A `safety-hold` cursor in PG prevents outbox cleanup during reload
- The deploy skill at `.claude/skills/deploy/` has CLI commands for common operations

### Release Pipeline
```bash
node .claude/skills/deploy/cli.mjs release     # bump, tag, push, trigger Docker build
node .claude/skills/deploy/cli.mjs watch-build  # wait for Docker build
node .claude/skills/deploy/cli.mjs rollout X.Y.Z  # update K8s image + rolling restart
```

---

## Key Files

| File | Purpose |
|------|---------|
| `CLAUDE.md` | Design principles, architecture, coding standards |
| `src/engine.rs` | Core bitmap engine |
| `src/concurrent_engine.rs` | ArcSwap snapshot reads, flush/merge threads, cursor management |
| `src/docstore.rs` | V2 append-only tuple store (LIFO scan, field dictionaries) |
| `src/server.rs` | HTTP server (axum), query handling, doc serving, admin endpoints |
| `src/pg_sync/single_pass.rs` | Production bulk loader (CSV → bitmaps + docstore) |
| `src/pg_sync/outbox_poller.rs` | Steady-state sync from PG outbox |
| `src/pg_sync/row_assembler.rs` | Assembles documents from PG rows for outbox path |
| `src/pg_sync/copy_queries.rs` | COPY TO STDOUT queries for CSV download |
| `src/config.rs` | Schema mapping (source→target, ms_to_seconds, exists_boolean) |
| `src/loader.rs` | JSON→Document conversion with null handling |
| `src/mutation.rs` | Diff computation (diff_document, diff_document_partial) |
| `src/bitmap_fs.rs` | Filesystem bitmap persistence (fpack files, cursor files) |
| `src/cache.rs` | Unified cache with live maintenance |
| `src/bound_store.rs` | Persistent cache (meta.bin + ucpack shards) |
| `.claude/skills/deploy/` | Deploy skill (CLI + reload script) |
| `docs/bulk-load-handoff.md` | Bulk reload procedure and pitfalls |

---

## Important Runtime Settings

These are configurable via `PATCH /api/indexes/{name}/config` (hot config) or in the index definition. Know what they do before changing them.

### Cache Persistence (BoundStore)
- **What:** Persists cached query results to disk so they survive restarts. Saves ~2-13x on sort queries.
- **Config:** `storage.bound_store_path` in config. Purge via `DELETE /cache/persistent`.
- **When to disable:** If you see stale results after data changes, or if live maintenance is causing issues. Disabling means cold start after every restart — first queries will be slow.
- **To disable:** Remove the `bound_store_path` from config, or purge and don't save.

### Live Cache Maintenance
- **What:** The flush thread updates cached results on every mutation (adds/removes slots from cached bitmaps).
- **Config:** `cache.max_maintenance_work` (default 500,000). Set to 0 to disable live maintenance entirely.
- **When to reduce:** If flush cycles are slow or writes are bottlenecked. Lower values = less work per flush but more stale cache entries.
- **Hot patch:** `PATCH /config` with `{"cache":{"max_maintenance_work": 500000}}`

### Eager vs Lazy Field Loading
- **What:** `eager_load: true` on a filter/sort field means it loads at startup before the server accepts traffic. Lazy fields load on first query.
- **When to change:** If startup is too slow (too many eager fields) or first-query latency is unacceptable (field should be eager).
- **Production config:** `nsfwLevel`, `isPublished`, `postId` are eager. `tagIds` (79% of memory, 6.6s load) is lazy.

### Deferred Alive
- **What:** Documents with future publishedAt timestamps are held invisible until activation time.
- **Config:** `deferred_alive` in the index config with `source_field`, `ms_to_seconds`.
- **Known issue:** Activation reads stored doc and replays mutations but does NOT update docstore fields. The stored publishedAt value stays as originally written.

### Idle Eviction
- **What:** Per-value eviction for multi_value fields (tagIds, modelVersionIds). Values not queried within `idle_seconds` are evicted from memory and reloaded on demand.
- **Config:** `eviction: { idle_seconds: N }` on FilterFieldConfig.
- **Metrics:** `bitdex_eviction_total`, `bitdex_eviction_resident_values`

### Query Backpressure
- **What:** Limits concurrent query execution to prevent OOM under load.
- **Config:** `max_concurrent_queries` (default: num_cpus * 2). Excess queries get 503.

---

## Common Pitfalls

1. **Editing schema mappings without updating all three write paths.** The bulk loader, outbox assembler, and doc serving path must agree on field names and conversions.

2. **Assuming kubectl changes persist.** Flux reverts everything. Push to talos-infra.

3. **Not resetting cursors after bulk reload.** The loader seeds the current outbox head, not the CSV dump time. Manual reset required.

4. **Running server pods during bulk load.** Same-node PVC contention means both can mount ReadWriteOnce PVCs simultaneously. Data corruption risk.

5. **Adding new FilterFieldConfig fields.** Breaks ALL struct literals across ~15 files. Use `replace_all` in filter.rs helpers + manual sweep.

6. **Referencing V1 docstore (compressed msgpack).** Production uses V2 (append-only tuples, no compression).

7. **Mmap in pg-sync sidecar.** The sidecar has a 1Gi memory limit. Large CSV mmaps (collection_items at 1.2GB) cause OOM. Backfill was moved to the bulk loader to avoid this.

---

## Team & Contacts

- **Justin** — Project lead, architecture decisions
- **Donovan** — model-share (shadow mode integration), K8s config
- **Charlie** — pg-sync, bulk loader, collectionIds, backfill
- **Adam** — doc serving, format_document, schema audit
- **Arabella** — talos-infra (Flux, K8s manifests, Grafana)
- **Aidan** — long-term context holder, deploy skill, production ops

Reach agents via mailbox: `node ~/.claude/skills/mailbox/query.mjs send <name> "message"`

---

## What's In Progress / Known Gaps

- **Shadow mode comparison** — BitDex runs alongside Meilisearch, results compared. Donovan manages the model-share side.
- **Autovac** — Not started. Slot recycling for deleted documents.
- **Admin dashboard** — Not started.
- **Deferred alive activation doesn't update docstore** — Known behavior. Activation reads stored doc and replays mutations but doesn't update docstore publishedAt. Not a production issue as long as the bulk loader sets correct values.
- **Cursor override for bulk loader** — No `--cursor` CLI flag yet. Manual file write required after load.
