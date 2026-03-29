---
status: ACTIVE
updated: 2026-03-28
---

# Sync V2 — Final Implementation Plan

> Consolidated from the working design review (2026-03-26). All architecture
> decisions finalized with Justin. References:
> - Design spec: `docs/design/pg-sync-v2-final.md`
> - Sync config: `docs/design/sync-config-civitai.yaml`
> - Working notes: `docs/design/sync-v2-implementation-plan.md`

---

## Architecture Decisions (Finalized)

| Decision | Resolution |
|---|---|
| Dump pipeline | New `dump_processor.rs` based on single_pass.rs. Config-driven from the start via dump request body. Write to BitmapFs per phase, save+drop. No engine staging. |
| Steady-state pipeline | One path: all writes → ops → WAL → WAL reader → BitmapSink + DocSink. PUT/PATCH endpoints decompose to ops. |
| Persistence | BitmapFs for now. ShardStore migration is a follow-up. Server restores from BitmapFs on startup via lazy loading — dump processor MUST write to BitmapFs. |
| Config | Sync config read by bitdex-sync only. BitDex receives dump instructions via PUT /dumps request body (see D3). BitDex is blind to PG schema. |
| Enrichment (dump) | HashMap lookups with nested enrichment config. See D1. |
| Enrichment (steady-state) | queryOpSet resolves affected slots via bitmap query. |
| Sidecar | Rename to `bitdex-sync` with subcommands: `pg`, `ch`, `all`. |
| V1 code | Remove after V2 validated (Phase 3). |
| Collections | Dropped from BitDex. Intentional deviation from design spec which included CollectionItem. |
| Tag disabled filter | Optional in config. COPY includes attributes, dump processor filters `(attributes >> 10) & 1 = 0`. Absent column = include all (for local testing). |
| baseModel | Only set for Checkpoint model types (filter in nested enrichment config). |
| Deferred alive | Skip ALL bitmaps (alive + filter + sort). Write docstore only. Collect `BTreeMap<u64, Vec<u32>>`, save via `write_deferred_alive()`. |
| modelVersionIdsManual | Keep. Real field, `detected=false` resources. |
| No `full` op type | INSERTs emit individual `set` ops per field. One format for everything. |
| Dump ordering | Tags first (63GB, free memory early), then images, resources, tools, techniques, metrics. Sequential — no parallelism in V2. Differs from design spec (which puts Image first) for memory reasons. |
| Cursors | Two separate cursors: WAL byte-offset cursor (MetaStore, used by WAL reader) and BitdexOps row-ID cursor (`bitdex_cursors` PG table, used by bitdex-sync ops poller). |

---

## Key Design Details

### D1: Enrichment System (Dump Path)

For each dump phase that has `enrichment` in the config, the dump processor:
1. **Load:** Read the lookup CSV into `HashMap<i64, ParsedRow>` keyed by the `key` field
2. **Join:** While iterating the main CSV, look up each row's `join_on` field value in the HashMap
3. **Nested:** If the lookup itself has an `enrichment` child, load that CSV too. The child lookup uses a field from the parent lookup row as its key (e.g., MV.modelId → Model.id)
4. **Filter:** If a lookup has `filter`, only propagate fields when the filter expression evaluates true (e.g., `type = 'Checkpoint'`)
5. **Memory:** Load lookup HashMap before its dependent phase, `drop()` explicitly after phase completes

Example chain for Resources: iterate resources.csv → look up MV by modelVersionId → from MV row, look up Model by modelId → if Model.type == "Checkpoint", set baseModel from MV.baseModel; set poi from Model.poi.

**Note:** `postedToId` and `isPublished` are enrichment-derived computed fields. `postedToId` uses `lookup_key` (the Post.id itself). `isPublished` uses null-check on `publishedAtSecs`. `availability` comes from Post enrichment, not images.csv directly.

### D2: WAL Format and Lifecycle

**Record format:** `[4-byte payload_len (u32 LE)][8-byte entity_id (i64 LE)][1-byte flags][payload_len bytes: ops JSONB][4-byte CRC32]`
- Flag bit 0 = creates_slot

**Files:** Single `ops.wal` file for now. Size-based rotation (100MB) with generation naming (`ops_000001.wal`) is a future optimization — not required for V2 launch.

**WAL cursor:** Byte offset into the WAL file. Persisted to MetaStore after each batch. On restart, WAL reader seeks to persisted cursor position. This is separate from the BitdexOps cursor in PG.

**Crash recovery:** POST /ops returns 200 only after fsync. If crash before response, pg-sync resends. LIFO dedup on WAL reader handles re-delivered ops. Partial records at EOF are detected by CRC check and skipped.

### D3: Dump Request Body Schema

What bitdex-sync sends to BitDex with `PUT /api/indexes/{name}/dumps`:

```json
{
  "name": "tags-a1b2c3d4",
  "csv_path": "/data/load_stage/tags.csv",
  "format": "csv",
  "slot_field": "imageId",
  "sets_alive": false,
  "fields": [
    { "column": "tagId", "target": "tagIds" }
  ],
  "filter": "(attributes >> 10) & 1 = 0",
  "computed_fields": [
    { "target": "hasMeta", "expression": "(flags >> 13) & 1 == 1 && (flags >> 2) & 1 == 0" }
  ],
  "enrichment": [
    {
      "csv_path": "/data/load_stage/posts.csv",
      "key": "id",
      "join_on": "postId",
      "fields": [{ "column": "publishedAtSecs", "target": "publishedAt" }],
      "computed_fields": [
        { "target": "isPublished", "expression": "publishedAtSecs != null" },
        { "target": "postedToId", "expression": "lookup_key" }
      ],
      "enrichment": []
    }
  ]
}
```

No separate `/loaded` signal. PUT /dumps is the single registration+trigger call. Returns a `task_id` for async status polling via `GET /api/tasks/{task_id}`.

**ClickHouse metrics:** Uses `format: "tsv"` and `source: "clickhouse"` in the dump request. bitdex-sync fetches from CH (not PG COPY) and writes a local TSV file, then registers the dump with the same PUT /dumps endpoint. The dump processor handles TSV parsing the same as CSV (tab delimiter instead of comma).

### D4: Write Path Architecture (Steady-State)

```
HTTP PUT /documents → decompose to ops → append to WAL
HTTP PATCH /documents → decompose to ops → append to WAL
POST /ops (from bitdex-sync) → append to WAL
                                      ↓
                              WAL reader thread
                                      ↓
                              dedup_ops() (LIFO)
                                      ↓
                    apply_ops_batch(BitmapSink + DocSink)
                                      ↓
              ┌─────────────┐    ┌──────────┐
              │ BitmapSink  │    │ DocSink  │
              │ (CoalescerSink)  │ (tuple append)
              └──────┬──────┘    └────┬─────┘
                     ↓                ↓
              Flush thread       Docstore V2
              (staging → ArcSwap)
```

`engine.put()` and `engine.patch_document()` become thin wrappers: decompose document into `Vec<Op>`, write to WAL. They no longer directly mutate staging. ONE write path, not two.

**queryOpSet exception:** Fan-out ops resolve to potentially millions of slots. BitmapSink handles bulk bitmap OR/ANDNOT. DocSink writes are batched (not per-slot) for queryOpSet.

**POST /ops request body** (from design spec):
```json
{
  "ops": [
    { "entity_id": 12345, "ops": [{"op":"set","field":"nsfwLevel","value":16}], "creates_slot": true }
  ],
  "meta": { "source": "pg-ops", "cursor": 98765, "max_id": 99000, "lag_rows": 235 }
}
```

### D5: Op Dedup

Two-layer dedup:
1. **bitdex-sync side:** Before POST /ops, dedup by entity_id (LIFO — keep latest ops per entity)
2. **WAL reader side:** After reading batch, `dedup_ops()` — same LIFO dedup. Handles re-delivered ops from crash recovery.

queryOpSet ops dedup by `(entity_id, query)` not `(entity_id, field)`.

### D6: Behavioral Rules

- **No `full` op type.** INSERTs emit individual `set` ops per field. One format for everything.
- **Ops on non-alive slots are silently dropped** (except creates_slot=true which creates the slot).
- **Delete requires docstore read** — delete ops carry no old values, so BitDex reads the stored doc to know which bitmaps to clear (clean delete principle).
- **queryOpSet eventual consistency** — snapshot-level isolation is acceptable. The next steady-state trigger on a missed image corrects the state. Consistency window bounded by poll interval (~2s).
- **BitmapFs restore on startup** — the server restores bitmaps from BitmapFs via lazy loading. The dump processor MUST write to BitmapFs.

### D7: Expression Types

Two evaluation contexts:

**Dump expressions** (evaluated by BitDex dump processor against CSV column values):
- Bitfield extraction: `(flags >> 13) & 1 == 1`
- Equality: `type = 'Checkpoint'`
- Null check: `publishedAtSecs != null`
- Max: `max(scannedAtSecs, createdAtSecs)`
- Identity: `id` (pass-through)
- Lookup key: `lookup_key` (the enrichment join key value itself)
- Boolean inversion: `detected == false`

**Trigger expressions** (compiled into PG trigger SQL by bitdex-sync):
- Template: `{publishedAt}` resolves to `OLD."publishedAt"` for remove ops, `NEW."publishedAt"` for set ops
- SQL cast: `{availability}::text`
- SQL function: `extract(epoch from {publishedAt})::bigint`
- Null handling: when OLD is not null and NEW is null → remove op. When NEW is not null → set op.

---

## Phase 1: Dump Pipeline Rewrite (Config-Driven)

New file `src/dump_processor.rs` based on single_pass.rs patterns. Config-driven from the start — receives dump request body (D3) and processes CSV generically.

### Agent Assignments

- **Josh (Agent A)** — Core dump processor. File: `src/dump_processor.rs`. Owns the skeleton, CSV parsing, per-phase bitmap loop, BitmapFs persistence, docstore, crash recovery, server wiring.
- **Nate (Agent B)** — Enrichment + expression engine. Files: `src/dump_expression.rs`, `src/dump_enrichment.rs`. Owns filter/computed expression evaluation, HashMap enrichment with nesting, LCS dictionary handling.
- Josh calls Nate's library for enrichment/expression evaluation. Josh builds with placeholder calls initially; integrate when both are ready.

### Task List

- [x] **1.1** `[Josh]` Create `src/dump_processor.rs` — skeleton with dump request body deserialization (D3 schema) ✅ *QA verified: 4 deser tests*
- [x] **1.2** `[Josh]` Generic CSV column parser — parse CSV/TSV using named columns from dump request (not hardcoded parsers). Support both comma (CSV) and tab (TSV) delimiters via `format` field. ✅ *QA verified: 3 tests (CSV, quoted, TSV)*
- [x] **1.3** `[Nate]` Filter expression evaluator — evaluate config `filter` expressions per row. Support expression types from D7 (bitfield, equality, null check, boolean inversion). Filter is optional — absent means include all rows. **File: `src/dump_expression.rs`** ✅ *QA verified: tokenizer + recursive descent, all D7 types, 34 tests*
- [x] **1.4** `[Nate]` Computed field evaluator — evaluate `computed_fields` expressions per row (bitfield extraction, max, identity, lookup_key, null check). Results feed into bitmap writes. **File: `src/dump_expression.rs`** ✅ *QA verified: max(), identity, lookup_key, conditional multi-value*
- [x] **1.5** `[Nate]` Enrichment system — HashMap lookups with nested enrichment from config (see D1). **File: `src/dump_enrichment.rs`** ✅ *QA verified: 15 tests*
  - [x] 1.5a `[Nate]` Single-level lookup (Post → Image enrichment) ✅
  - [x] 1.5b `[Nate]` Nested lookup (MV → Model within Resources) ✅
  - [x] 1.5c `[Nate]` Lazy loading per-phase (load before dependent CSV, drop after) ✅
- [x] **1.6** `[Josh]` Per-phase bitmap processing: build bitmaps in HashMap, save to BitmapFs, drop. Josh owns the phase loop; calls Nate's enrichment/expression APIs within each phase. ✅ *QA verified*
  - [x] 1.6a `[Josh]` Tags — Vec indexing optimization (MAX_TAG_ID=300K preallocated Vec, convert to HashMap for save). If tag count exceeds MAX_TAG_ID, fall back to HashMap. ✅
  - [x] 1.6b `[Josh]` Images — direct field writes + calls Nate's enrichment + computed fields ✅
  - [x] 1.6c `[Josh]` Resources — calls Nate's nested enrichment (MV → Model chain), baseModel Checkpoint filter ✅
  - [x] 1.6d `[Josh]` Tools, Techniques — simple multi-value (concise `fields: [toolIds]` shorthand) ✅
  - [x] 1.6e `[Josh]` Metrics — TSV format from ClickHouse, sort fields only ✅
- [x] **1.7** `[Josh]` Port mmap + `split_mmap_ranges` from single_pass.rs for large CSVs (>1GB). Keep BufReader for small enrichment CSVs (<100MB). ✅ *QA verified: memmap2, newline-aligned splits*
- [x] **1.8** `[Josh]` Deferred alive — skip ALL bitmaps (alive + filter + sort) for future publishedAt, write docstore tuples only, collect `BTreeMap<u64, Vec<u32>>` (activate_at → slots), save via `write_deferred_alive()`. Slot counter = `max(max_alive_slot, max_deferred_slot) + 1`. ✅ *QA verified*
- [x] **1.9** `[Josh]` Docstore writes — BulkWriter integration, append tuples per row (including deferred slots — required for `activate_due()` to rebuild bitmaps later) ✅ *QA verified: BulkWriter, PackedValue*
- [x] **1.10** `[Nate]` LCS dictionary handling — resolve string fields via FieldDictionary (type, availability, blockedFor, baseModel), persist to `dictionaries/{name}.dict` after all phases. **Nate provides the API; Josh calls it from the phase loop.** ✅ *QA verified: DictionarySet with resolve + persist_all. ⚠️ Josh not yet using Nate's DictionarySet (uses engine.dictionaries_arc() directly) — fix pending*
- [x] **1.11** `[Josh]` Crash recovery — `field_already_loaded()` checks BitmapFs for existing data files per field name; skip phase if present (ref: single_pass.rs:44-52) ✅ *QA verified*
- [x] **1.12** `[Josh]` Computed sort fields — existedAt = GREATEST(scannedAt, createdAt), id = slot. sortAt = GREATEST(existedAt, publishedAt) resolved by BitDex from index config's `computed` property. **Prerequisite:** verify computed sort field feature exists (Ryan's PR #82). ✅ PR #82 merged. ✅ *QA verified*
- [x] **1.13** `[Josh]` Wire dump_processor into server — PUT /dumps endpoint triggers async processing via existing task system (`GET /api/tasks/{task_id}` for status polling) ✅ *QA verified: TaskType::Dump, tokio::spawn_blocking*
- [x] **1.14** `[Josh]` Config validation — malformed dump request → clear error (not crash), unknown target field → warning ✅ *QA verified: 6 checks, 4 tests*
- [ ] **1.15** `[Josh]` Address ALL Ollie findings: ⚠️ *5/8 done, 3 pending fixes from Adam's review*
  - [x] #1 Direct bitmap writes (done by design — no Op abstraction in dump) ✅
  - [x] #2 Mmap for large CSVs (1.7) ✅
  - [ ] #3 Arc<str> field names instead of String clones per rayon task — ⚠️ *MISSING: still using String*
  - [x] #4 No double conversion (done by design — CSV value → bitmap key directly) ✅
  - [ ] #5 Vec for sort bit layers instead of HashMap (preallocate `Vec<RoaringBitmap>` of size `num_bits`) — ⚠️ *MISSING: still using HashMap<usize, RoaringBitmap>*
  - [x] #6 No apply_accum (write to BitmapFs directly, no engine staging) ✅
  - [x] #7 `[Nate]` Dict refs — pass individual `&FieldDictionary` refs, not full `HashMap<String, FieldDictionary>` (part of 1.10 API design) ✅ *Nate's API done; ⚠️ Josh not yet wired to use it*
  - [x] #8 Deferred alive skips all bitmaps (1.8) ✅

### Review Findings (Adam + QA sub-agents, 2026-03-26)

- **P0 BUG:** LCS encoding broken for enrichment-derived filter fields (availability, baseModel). String values fail i64 parse → bitmap never written. Josh fixing.
- **P1:** Ollie #3 (Arc\<str\>), #5 (Vec sort layers), #7 (DictionarySet wiring) — Josh fixing.
- **P2:** Dead placeholder code cleanup, multi-value docstore writes check — Josh fixing.
- **Nate's code:** All 7 tasks verified DONE by QA. 49 tests. Two non-blocking nits (null=null semantics, estimated_memory divisor).

### Phase 1 Validation

- [ ] **V1.1** Load all CSVs at 107M — completes under 15GB RSS, under 10 min
- [ ] **V1.2** Bitmap spot checks — run query suite, compare counts against CSV-derived expected values
  - `nsfwLevel eq 1` count matches grep of images.csv
  - `tagIds eq {known_tag}` count matches lines in tags.csv (excluding disabled if attributes column present)
  - `type eq "image"` uses LCS dictionary correctly
  - `baseModel eq "SDXL"` only from Checkpoint model types
- [ ] **V1.3** Sort correctness — `sort=reactionCount desc limit 10` matches metrics TSV order
- [ ] **V1.4** Deferred alive — images with future publishedAt not in query results
- [ ] **V1.5** Docstore — GET /documents/{slot} returns all fields including doc-only (url, hash, width, height)
- [ ] **V1.6** Dictionary persistence — restart server, LCS queries still work
- [ ] **V1.7** Crash recovery — kill during resources phase, restart resumes from resources (tags + images already saved)
- [ ] **V1.8** Per-phase memory — RSS drops after each phase's save+drop
- [ ] **V1.9** Config-driven test — add a new field to dump request body → dump picks it up without code changes

---

## Phase 2: WAL Reader Thread (Steady-State)

Single pipeline: ops → WAL → WAL reader → apply_ops_batch with BitmapSink + DocSink. See D4.

### Task List

- [x] **2.1** `[Lucy]` WAL reader background thread ✅ *pre-existing, verified*
  - Read batch (10K), dedup (D5), apply via BitmapSink + DocSink, save WAL cursor to MetaStore
  - Sleep 50ms when empty
- [x] **2.2** `[Lucy]` Wire DocSink into apply_ops_batch ✅ *new code: DocWriter struct*
- [x] **2.3** `[Lucy]` Computed sort field recomputation ✅ *new code: old bit clearing + new bit setting*
- [x] **2.4** `[Lucy]` Deferred alive in ops path ✅ *new code: skip ALL bitmaps*
- [x] **2.5** `[Lucy]` Fix diff_document_partial bypass ✅ *new code: 53 lines in mutation.rs*
- [x] **2.6** `[Lucy]` WAL cursor persistence ✅ *pre-existing, verified*
- [x] **2.7** `[Lucy]` Refactor PUT/PATCH HTTP endpoints ✅ *document_to_ops with is_patch parameter*
- [x] **2.8** `[Lucy]` Op dedup in WAL reader ✅ *pre-existing, verified*
- [x] **2.9** `[Lucy]` POST /ops endpoint ✅ *pre-existing, verified*
- [x] **2.10** `[Lucy]` Ops on non-alive slots ✅ *new code: is_slot_alive check*
- [x] **2.11** `[Lucy]` Delete docstore read ✅ *pre-existing, verified*
- [x] **2.12** `[Lucy]` Prometheus metrics ✅ *2 new (cycle_duration, wal_pending), 2 pre-existing*

**Dakota (Doc Keeper) independent verification (2026-03-28):** All 12 tasks confirmed in code via Explorer agents:
- 2.1: server.rs:1130-1214 (WAL reader thread spawn, batch read, dedup, CoalescerSink + DocWriter)
- 2.2: ops_processor.rs:44-122 (DocWriter struct, write_set/add/remove per op, flush)
- 2.3: ops_processor.rs:823-883 (FieldMeta.computed_deps, GREATEST/LEAST recomputation)
- 2.4: ops_processor.rs:728-754 (check_deferred_alive, skips ALL bitmaps for future publishedAt)
- 2.5: mutation.rs:302-350 (deferred check in diff_document_partial, clears old bitmaps)
- 2.6: ops_processor.rs:1224-1234 (save_cursor/load_cursor, byte-offset persistence)
- 2.7: concurrent_engine.rs:2816-2891 (put_via_wal, patch_document_via_wal, document_to_ops)
- 2.8: pg_sync/op_dedup.rs:22-52 (dedup_ops, two-layer: ops_processor.rs:688 + ops_poller.rs:126)
- 2.9: server.rs:4410-4494 (handle_ops → spawn_blocking → append_batch → sync_all → OK)
- 2.10: ops_processor.rs:714-726 (!creates_slot && !has_query_op_set → is_slot_alive → skip)
- 2.11: ops_processor.rs:995-1016 → concurrent_engine.rs:3039-3080 (docstore.get reads old doc)
- 2.12: metrics.rs:584-611 (all 4 metrics), server.rs:1267+4790-4795 (sync-lag endpoint)

### Phase 2 Validation

- [x] **V2.1** Single op roundtrip — POST /ops set op → query shows change → docstore updated ✅
- [x] **V2.2** Multi-value add/remove — add tagIds, query matches, remove, query no longer matches ✅
- [x] **V2.3** Delete — clean delete clears all filter+sort bits, reads stored doc ✅
- [x] **V2.4** queryOpSet — fan-out to 1000+ slots, verify bitmap bulk update + batched docstore ✅
- [x] **V2.5** Deferred alive via ops — future publishedAt creates_slot → not queryable until timestamp ✅
- [x] **V2.6** WAL cursor restart — kill server, restart, no duplicate processing ✅
- [x] **V2.7** PUT/PATCH → WAL — verify PUT endpoint generates ops in WAL (not direct staging write) ✅
- [x] **V2.8** Op dedup — verify duplicate ops in same batch are deduped (LIFO, last wins) ✅
- [x] **V2.9** Non-alive slot ops — verify set/add ops on non-alive slots are silently dropped ✅
- [x] **V2.10** Delete docstore read — verify delete reads stored doc and clears correct bitmaps ✅
- [x] **V2.11** Prometheus metrics — /api/internal/sync-lag returns cursor/lag data ✅

---

## Phase 2.5: Trigger Validation (via PG Tunnel)

Test the trigger → BitdexOps → ops processing chain before full activation.

### Task List

- [ ] **2.5.1** Get PG access from Aidan (tunnel to replica DP-6228)
- [ ] **2.5.2** Create BitdexOps table on PG replica
- [ ] **2.5.3** Generate trigger SQL from sync config (trigger_gen.rs)
- [ ] **2.5.4** Deploy triggers to PG replica
- [ ] **2.5.5** Wait for organic traffic / make test changes
- [ ] **2.5.6** Read BitdexOps rows — verify ops structure:
  - Image UPDATE → remove + set ops with old/new values
  - Tag INSERT → add op with tagId (disabled tags filtered by trigger)
  - Post UPDATE → queryOpSet with "postId eq {id}", publishedAt null handling correct
  - ModelVersion UPDATE → queryOpSet with Checkpoint filter (via Model JOIN)
  - Model UPDATE → expression resolves MV ids, queryOpSet with "modelVersionIds in [...]"
- [ ] **2.5.7** POST those ops to local BitDex → verify bitmap changes match expectations
- [ ] **2.5.8** Verify null handling — publishedAt null→value and value→null transitions produce correct remove/set ops

### Phase 2.5 Validation

- [ ] **V2.5.1** At least 100 ops from each trigger type (Image, Tag, Post, MV, Model) verified
- [ ] **V2.5.2** Fan-out ops (queryOpSet) resolve correct slot counts
- [ ] **V2.5.3** Null transitions produce remove ops (not set with null)

---

## Phase 3: Activation Infrastructure

### Task List

- [x] **3.1** `[Nate]` Rename binary: `bitdex-pg-sync` → `bitdex-sync` ✅
- [x] **3.2** `[Nate]` New subcommands: `pg` (dump + ops poll), `ch` (ClickHouse poll), `all` (both, default) ✅ *Also added: setup, validate*
- [x] **3.3** `[Nate]` Implement `ch` subcommand — ClickHouse metrics polling. Each CH row becomes three set ops (reactionCount, commentCount, collectedCount) posted to /ops. Poll interval configurable (default 60s). Goes through the WAL like everything else. ✅
- [x] **3.4** `[Nate]` Trigger reconciliation — read sync config, generate SQL via trigger_gen, CREATE OR REPLACE on boot, DROP stale triggers (hash mismatch) ✅ *QA verified: 28 tests*
- [x] **3.5** `[Nate]` Boot sequence implementation (10-step autonomous, QA verified): ✅
  - [ ] 3.5a Wait for BitDex health check
  - [ ] 3.5b Capture/create pre_dump_cursor from BitdexOps (if table empty, seed at 0). Stored in `bitdex_cursors` PG table.
  - [ ] 3.5c Check dump history (GET /dumps)
  - [ ] 3.5d For each sync_source not yet dumped: COPY → CSV, PUT /dumps with dump request body (D3). No separate /loaded signal.
  - [ ] 3.5e Poll task status until complete (`GET /api/tasks/{task_id}`)
  - [ ] 3.5f Seed BitdexOps cursor at pre_dump_cursor (catches dump-window ops)
  - [ ] 3.5g Transition to steady-state ops polling (BitdexOps → POST /ops)
  - [ ] 3.5h K8s readiness probe → 200
- [x] **3.6** `[Nate]` Config hash change detection — dump names include `{table}-{hash8}` where hash is of the table's sync config YAML block. Mismatch triggers re-dump. ✅ *QA verified: 2 tests*
- [x] **3.7** `[Nate]` V1 code removal (-5,274 lines): ✅
  - [ ] 3.7a Delete copy_streams.rs (830 lines, unused)
  - [ ] 3.7b Delete table_streams.rs (551 lines, unused)
  - [ ] 3.7c Delete outbox_poller.rs (219 lines, replaced by ops_poller)
  - [ ] 3.7d Delete row_assembler.rs (205 lines, hardcoded enrichment)
  - [ ] 3.7e Remove V1 functions from queries.rs (SETUP_SQL, poll_outbox, fetch_enrichment)
  - [ ] 3.7f Remove run_bulk_load/run_bulk_load_copy from bulk_loader.rs
  - [ ] 3.7g Remove old Load/Sync/Setup subcommands from pg_sync.rs
  - [ ] 3.7h Delete single_pass.rs (replaced by dump_processor.rs)
  - [ ] 3.7i Remove load_ndjson from loader.rs (verify no active callers in benchmark harness first)
  - [ ] 3.7j Remove dump code from ops_processor.rs (process_csv_dump_direct, process_multi_value_csv, apply_accum_to_staging)

### Phase 3 Validation

- [ ] **V3.1** Fresh boot — empty data dir, bitdex-sync dumps all tables, ops polling starts, readiness 200
- [ ] **V3.2** Config change — modify sync config field, hash changes, affected table re-dumps
- [ ] **V3.3** Dump-window ops — changes during dump captured by pre_dump_cursor, no data loss
- [ ] **V3.4** V1 code removed — `cargo build` succeeds with no V1 references
- [ ] **V3.5** CH polling — ClickHouse metrics arrive via ops, sort fields update correctly

---

## Test Environment Setup

| Test Type | Environment | What's Needed |
|---|---|---|
| Dump pipeline (Phase 1) | Local | CSVs in data/load_stage/ (already have 107M) |
| Ops processing (Phase 2) | Local | POST crafted ops to local BitDex server |
| Trigger validation (Phase 2.5) | PG tunnel | Aidan provides tunnel to DP-6228 replica |
| Boot sequence (Phase 3) | K8s staging | Aidan deploys to staging pod |

**Ground truth for bitmap correctness:** Derive expected counts from CSVs (grep/count). For trigger validation, tunnel into PG and read BitdexOps directly.

---

## Reference Documents

- `docs/design/pg-sync-v2-final.md` — original design spec (note: dump processing via AccumSink is superseded by BitmapFs-direct approach)
- `docs/design/sync-config-civitai.yaml` — complete sync config with all tables + triggers
- `docs/design/sync-v2-implementation-plan.md` — working notes with inline discussion
- `src/pg_sync/single_pass.rs` — reference implementation for dump patterns
- Ollie's deferred alive audit — captured in working notes (5 questions, all answered)
- V1 code inventory — captured in working notes (7 files identified for removal)
