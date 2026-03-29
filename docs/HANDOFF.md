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

**The V1 pipeline (production today):**
```
PG tables → CSV dump → single_pass bulk loader → bitmaps + docstore on disk
                                          ↓
                              server loads from disk on startup
                                          ↓
                          pg-sync sidecar keeps data current via outbox polling
```

**The V2 pipeline (feat/sync-v2, replacing V1):**
```
PG tables → COPY → CSV files → dump_processor.rs (config-driven, per-phase)
                                          ↓
                        ShardStore bitmaps + DocStore V2 tuples on disk
                                          ↓
                              server loads from disk on startup
                                          ↓
          PG triggers → BitdexOps table → bitdex-sync ops_poller → POST /ops
                                          ↓
                    ops_wal.rs (WAL append + fsync) → WAL reader thread
                                          ↓
              ops_processor.rs (BitmapSink + DocSink) → flush → ArcSwap publish
```

---

## Key Architectural Decisions (What Agents Get Wrong)

### DocStore V2 — NOT compressed msgpack
`CLAUDE.md` says "zstd-compressed msgpack." That's the V1 format. **Production uses V2:** append-only tuple logs, no compression, LIFO scan for reads. V2 is 215x faster at p50. The V1 format is still referenced in code comments but V2 is what runs. See `src/docstore.rs`.

### Docstore Write Paths (V2)
In sync-v2, there are two write paths that must stay in sync:
1. **`src/dump_processor.rs`** — V2 bulk loader (CSV → bitmaps + docstore). Config-driven via dump request body.
2. **`src/ops_processor.rs`** — V2 steady-state (ops → WAL → BitmapSink + DocSink).

Both paths produce docstore tuples. Field names and conversions come from the sync config, not hardcoded.

**V1 paths (removed on feat/sync-v2):** `single_pass.rs`, `outbox_poller.rs`, `row_assembler.rs` were deleted as part of Phase 3 V1 code cleanup.

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
- **Server + pg-sync:** `ghcr.io/civitai/bitdex:1.0.97` (as of 2026-03-26)
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

### CSV Dump Source (Justin Directive, 2026-03-28)
CSV dumps should use `bitdex-sync` bulk_loader which reads COPY queries from the sync config YAML — NOT manual agent-issued COPY commands. This ensures field names, column ordering, and table schemas match the sync config. Manual COPY queries risk divergence from the config-driven pipeline.

### File Downloads Endpoint (Implemented, 2026-03-28)
The `/downloads/` endpoint on BitDex serves files via K8s ingress at `https://bitdex.civitai.com/downloads/`. This avoids `kubectl cp` corruption issues when transferring files between pods.

**Download method priority** (deploy skill `csv-download`):
1. Ingress (`https://bitdex.civitai.com/downloads/`) with Bearer token (`BITDEX_DL_TOKEN` env or `--token` flag)
2. Falls back to `kubectl port-forward` if ingress unavailable

**Parallel chunked download:** Files >1GB are auto-split into 8 parallel HTTP Range request chunks. Override with `--chunks N`. Implementation: `lib/multipart-download.mjs` in the deploy skill.

Connection strings stored in `data/.env.bitdex`.

### Bulk Reload
See `docs/archive/bulk-load-handoff.md` for the V1 procedure (archived). Key points:
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

### Deploy CLI Quick Reference

The deploy skill (`.claude/skills/deploy/`) has CLI tools agents should use instead of manual kubectl/psql. Run `/deploy` to see the full skill docs.

**Tunnels** (use these, don't set up port-forwards manually):
```bash
node .claude/skills/deploy/cli.mjs tunnel pg       # PG tunnel on localhost:5432 (auto password + .env)
node .claude/skills/deploy/cli.mjs tunnel bitdex   # BitDex server tunnel on localhost:3099
```

**CSV operations:**
```bash
node .claude/skills/deploy/cli.mjs csv-dump-tables  # list available tables with sizes
node .claude/skills/deploy/cli.mjs csv-download      # download CSVs (ingress + parallel chunks)
node .claude/skills/deploy/cli.mjs csv-full-pipeline  # end-to-end: dump → serve → download → verify
```

**Common mistake:** Do NOT port-forward to `bitdex-0:5432` — that's the BitDex server, not PG. The PG replica is `cnpg-cluster-nvme0-1` in `cnpg-database` namespace. The CLI handles the correct target automatically.

---

## Key Files

| File | Purpose |
|------|---------|
| `CLAUDE.md` | Design principles, architecture, coding standards |
| `src/engine.rs` | Core bitmap engine |
| `src/concurrent_engine.rs` | ArcSwap snapshot reads, flush/merge threads, cursor management |
| `src/docstore.rs` | V2 append-only tuple store (LIFO scan, field dictionaries) |
| `src/server.rs` | HTTP server (axum), query handling, doc serving, admin endpoints |
| `src/dump_processor.rs` | V2 dump pipeline — config-driven CSV → bitmaps + docstore |
| `src/dump_expression.rs` | Expression evaluator for dump filters and computed fields |
| `src/dump_enrichment.rs` | HashMap-based enrichment with nested lookups for dump |
| `src/ops_wal.rs` | Write-ahead log for ops (append, fsync, LIFO dedup) |
| `src/ops_processor.rs` | Applies ops batches to BitmapSink + DocSink |
| `src/write_coalescer.rs` | Coalesces bitmap writes for flush efficiency |
| `src/pg_sync/ops_poller.rs` | V2 steady-state: polls BitdexOps table → POST /ops |
| `src/pg_sync/trigger_gen.rs` | Generates PG trigger SQL from sync config |
| `src/pg_sync/sync_config.rs` | Sync config parsing (YAML-based) |
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

1. **Editing schema mappings without updating both write paths.** The dump processor (`dump_processor.rs`) and ops processor (`ops_processor.rs`) must agree on field names and conversions. In V2, field names come from the sync config, not hardcoded.

2. **Assuming kubectl changes persist.** Flux reverts everything. Push to talos-infra.

3. **Not resetting cursors after bulk reload.** The loader seeds the current outbox head, not the CSV dump time. Manual reset required.

4. **Running server pods during bulk load.** Same-node PVC contention means both can mount ReadWriteOnce PVCs simultaneously. Data corruption risk.

5. **Adding new FilterFieldConfig fields.** Breaks ALL struct literals across ~15 files. Use `replace_all` in filter.rs helpers + manual sweep.

6. **Referencing V1 docstore (compressed msgpack).** Production uses V2 (append-only tuples, no compression).

8. **Referencing deleted V1 sync files.** `single_pass.rs`, `outbox_poller.rs`, `row_assembler.rs` were removed in sync-v2 Phase 3. The V2 equivalents are `dump_processor.rs` (bulk), `ops_processor.rs` (steady-state), and `ops_poller.rs` (PG polling).

7. **Mmap in pg-sync sidecar.** The sidecar has a 1Gi memory limit. Large CSV mmaps (collection_items at 1.2GB) cause OOM. Backfill was moved to the bulk loader to avoid this.

---

## Team & Contacts

- **Justin** — Project lead, architecture decisions, final approval on sync-v2 merges
- **Tom** — CTO oversight for sync-v2 production push
- **Scarlet** — Team lead for sync-v2 implementation (manages Josh, Nate, Lucy)
- **Josh** — Phase 1: dump processor implementation
- **Nate** — Enrichment engine, Phase 3 partial (3.1-3.3, 3.7 V1 removal)
- **Lucy** — Phase 2: WAL reader, steady-state pipeline
- **Adam** — Design architect, code reviewer, QA oversight
- **Aidan** — Infra/monitoring, Grafana/Prometheus, K8s deploys, PG access, deploy skill
- **Donovan** — model-share (shadow mode integration) — currently offline
- **Arabella** — talos-infra (Flux, K8s manifests, Grafana)
- **Dakota** — Doc Keeper (documentation, CLAUDE.md, memory curation)

Reach agents via mailbox: `node ~/.claude/skills/mailbox/query.mjs send <name> "message"`

---

## What's In Progress / Known Gaps

### Sync V2 (Active — feat/sync-v2 branch)
- **Phase 1 (Dump Pipeline):** Code complete, Gate 1 CLEAR (107M validation passed)
- **Phase 2 (WAL Reader):** Code complete, Gate 2 CLEAR (17 tests passing, 2 skipped)
- **Phase 2.5 (Trigger Validation):** PARTIAL — crafted tests pass but real PG triggers NOT deployed or tested
- **Phase 3 (Activation):** 60% built — 3.1-3.3 done (rename, subcommands, CH polling), 3.7 done (V1 removal). 3.4 (trigger reconciliation), 3.5 (boot sequence), 3.6 (config hash) NOT YET IMPLEMENTED
- **Gate 5 (Local Integration):** PARTIAL — crafted data tests pass, real PG integration NOT done
- **Production readiness:** See `docs/design/production-readiness-checklist.md`

### Other
- **Shadow mode comparison** — BitDex runs alongside Meilisearch, results compared. Donovan manages the model-share side (currently offline).
- **Autovac** — Not started. Slot recycling for deleted documents.
- **Admin dashboard** — Not started.
- **Deferred alive activation doesn't update docstore** — Known behavior. Not a production issue as long as the bulk loader sets correct values.
