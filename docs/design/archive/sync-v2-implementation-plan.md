# Sync V2 — Complete Implementation Plan

> Full gap analysis + implementation plan. Produced 2026-03-26 after audit of:
> - `docs/design/pg-sync-v2-final.md` (design spec)
> - `src/loader.rs` (NDJSON loader — reference for memory patterns)
> - `src/pg_sync/single_pass.rs` (CSV loader — reference for per-table processing)
> - `src/pg_sync/bulk_loader.rs` (PG COPY → CSV loader)
> - Ollie's 8 performance findings
> - Current `src/ops_processor.rs` (V2 dump pipeline — what exists today)

---

## Current State (Honest Assessment)

**What works:** Test harness processes CSV files into correct bitmaps at 50M scale.

**What's broken or missing:**
1. OOM at 107M — `apply_accum()` deep-clones InnerEngine per 2M chunk (fixed in uncommitted code, needs validation)
2. CSV processing is hardcoded, not config-driven
3. No WAL reader thread (ops written to WAL but never consumed)
4. No docstore writes from ops path
5. No inter-phase save+unload (single_pass.rs does this)
6. No mmap for large CSVs (single_pass.rs uses memmap2)
7. No sidecar V2 subcommand
8. No PG trigger reconciliation
9. No boot sequence automation

---

## Phase 1: Fix the Dump Pipeline (match single_pass.rs)

Priority: **CRITICAL** — this is the bulk loading path that must work at 107M.

### ~~1.1 Clone-Once Staging Pattern~~ — SUPERSEDED by 1.2

~~**Fix:** Clone staging once, mutate in place, publish once.~~

**Review verdict:** This contradicts 1.2. single_pass.rs does NOT use engine staging at all (line 102-103: "No enter_loading_mode — we write directly to BitmapFs"). The clone-once pattern from load_ndjson is the wrong reference — load_ndjson processes a single NDJSON file, not multi-phase CSV dumps. **Follow 1.2 instead.**

### 1.2 Per-Phase Save to BitmapFs + Memory Release

**Gap:** All phases accumulate in one staging = ~12GB peak. single_pass.rs saves each phase to disk and drops the HashMap.
**Fix:** After each CSV phase, call `save_filter_field_to_disk()` / `save_sort_field_to_disk()` and drop the result. Do NOT use engine staging at all — write directly to BitmapFs like single_pass.rs does.

```
Reference: src/pg_sync/single_pass.rs:138   — save_filter_field_to_disk(&bitmap_fs, "tagIds", &tag_map)
Reference: src/pg_sync/single_pass.rs:197   — save filter bitmaps after images
Reference: src/pg_sync/single_pass.rs:215   — bitmap_fs.write_alive(&img_result.alive)
Reference: src/pg_sync/single_pass.rs:1418  — save_filter_field_to_disk() implementation
Reference: src/pg_sync/single_pass.rs:1443  — save_sort_field_to_disk() implementation
```

**Architecture change:** The dump pipeline should NOT use engine staging/ArcSwap at all. It should:
1. Build bitmaps in HashMap (per-phase)
2. Save directly to BitmapFs files
3. Drop the HashMap (free memory)
4. After all phases: engine restores from BitmapFs via lazy loading

This is exactly what single_pass.rs does. No clone_staging, no publish_staging, no loading mode.

### 1.3 Mmap for Large CSVs

**Gap:** Using `BufReader::read_until` + `Vec<Vec<u8>>` per chunk. Allocates per-line.
**Fix:** Use `memmap2::Mmap` + `split_mmap_ranges()` for CSVs > 1GB (tags 63GB, images 14GB, resources 820MB, metrics 1.4GB).

```
Reference: src/pg_sync/single_pass.rs:516  — let mmap = memmap2::Mmap::map(&file)?
Reference: src/pg_sync/single_pass.rs:523  — split_mmap_ranges() divides by byte ranges
Reference: src/pg_sync/single_pass.rs:550  — par_iter over byte ranges
```

**Keep BufReader for:** Small enrichment tables (posts 639MB, model_versions 25MB, models 12MB, tools 50MB, techniques 71MB).

### 1.4 Direct Vec Indexing for Tags

**Gap:** Using `HashMap<u64, RoaringBitmap>` for tagIds. HashMap overhead at 31K entries.
**Fix:** Preallocate `Vec<RoaringBitmap>` of size MAX_TAG_ID (300K) and index directly.

```
Reference: src/pg_sync/single_pass.rs:512  — MAX_TAG_ID = 300_000
Reference: src/pg_sync/single_pass.rs:554  — let bitmaps: Vec<RoaringBitmap> = (0..MAX_TAG_ID).map(...)
```

After merge, convert to HashMap for save_filter_field_to_disk.

### 1.5 Direct Bitmap Writes (No Op Abstraction in Dump)

**Gap:** Was using `image_row_to_ops_pub()` → `Vec<Op>` → `json_to_qvalue()` → `value_to_bitmap_key()`.
**Fix:** Already done — macros `filter_int!`, `filter_bool!`, `filter_str!` write directly from CopyImageRow fields.
**Status:** Committed (834e806).

### 1.6 Deferred Alive in Dump Path

**Gap:** Was unconditionally setting alive for all images.
**Fix:** Compare publishedAt against now_unix. Future-dated: skip alive AND filter AND sort bitmaps, but still write docstore tuples. Add slot to `BTreeMap<u64, Vec<u32>>`, save via `bitmap_fs.write_deferred_alive()`.

```
Reference: src/pg_sync/single_pass.rs:816-828 — deferred alive check
Reference: src/pg_sync/single_pass.rs:821     — docstore tuples STILL written for deferred slots
Reference: src/pg_sync/single_pass.rs:234-240 — bitmap_fs.write_deferred_alive()
```

**CORRECTNESS BUG in current code:** Current V2 ops_processor sets filter/sort bitmaps for deferred slots but skips alive. This is wrong — per CLAUDE.md principle 5, filter bitmaps are always clean (alive is NOT ANDed into queries). Setting filter bits on non-alive slots means deferred images appear in query results. single_pass.rs correctly skips ALL bitmaps (alive + filter + sort) for deferred slots. Docstore tuples are still written so `activate_due()` can read them back when the timestamp arrives.

### 1.7 Docstore Writes During Dump

**Gap:** No docstore writes at all in V2 dump path.
**Fix:** Use BulkWriter pattern — prepare once, append tuples per row.

```
Reference: src/pg_sync/single_pass.rs:88-100  — prepare BulkWriter
Reference: src/pg_sync/single_pass.rs:821     — append_image_docstore_tuples()
Reference: src/pg_sync/single_pass.rs:1039    — macro append_int!/append_str!/append_bool!
```

### 1.8 Dictionary Persistence

**Gap:** Dictionaries created per-dump but never saved to disk.
**Fix:** After all CSVs complete, persist all LCS dictionaries.

```
Reference: src/pg_sync/single_pass.rs:440-462 — save each dict to dictionaries/{name}.dict
```

### 1.9 Processing Order

Match single_pass.rs order. Load enrichment lazily per-phase (not all upfront). Explicit `drop()` after each phase to free memory before next.

```
Phase 1: Tags (63GB) — mmap, Vec indexing, save_filter_field_to_disk("tagIds"), drop tag_map
Phase 2: Images (14GB)
    Load post_map (BufReader, ~1.5GB)
    mmap images.csv, rayon parallel
    Post enrichment (publishedAt, availability, postedToId, isPublished)
    Deferred alive: skip ALL bitmaps for future-dated, write docstore only
    Docstore tuples via BulkWriter for ALL images (including deferred)
    save filter bitmaps, sort bitmaps, alive bitmap
    write slot_counter = max(alive, deferred) + 1
    write deferred_alive BTreeMap
    drop(post_map), drop(img_result)
Phase 3: Resources (820MB)
    Load mv_map (BufReader, 25MB)
    Load model_map (BufReader, 12MB)
    mmap resources.csv, rayon parallel
    modelVersionIds for all, baseModel ONLY for Checkpoint model types
    save_filter_field_to_disk for each field
    drop(mv_map), drop(model_map), drop(res_result)
Phase 4: Tools (50MB) — mmap, save_filter_field_to_disk("toolIds"), drop
Phase 5: Techniques (71MB) — same as tools
Phase 6: Metrics (1.4GB) — BufReader (TSV), save_sort_field_to_disk for each, drop
Phase 7: CollectionItems (if exists) — save_filter_field_to_disk("collectionIds"), drop
Final: Save LCS dictionaries to dictionaries/{name}.dict
```

**Crash recovery:** Each phase checks `field_already_loaded()` before processing. If BitmapFs already has data for a field (from a prior interrupted run), skip that phase.

```
Reference: src/pg_sync/single_pass.rs:44-52 — field_already_loaded()
Reference: src/pg_sync/single_pass.rs:123   — skip tagIds if already loaded
```

@justin: This process should become config driven now... Syncing/dumping will just follow the sync config. Please tell me why that doesn't work or will cause issues.

@adam: Agreed — no reason it can't be config-driven. The processing order and per-phase logic (mmap vs BufReader, enrichment loading, field_already_loaded skip) are all derivable from the sync config. The sync config defines table ordering + dependencies, and each table's config tells us slot_field, sets_alive, track_fields. The dump processor iterates sync_sources in order, builds bitmaps per the field config, saves to ShardStore, drops. The only thing that's currently hardcoded and needs to stay flexible is: (a) which tables need enrichment HashMaps loaded first, and (b) the tag Vec-indexing optimization (needs a `max_cardinality` hint in config). Both can be config properties.

@justin: Great. If we haven't already, we probably need to prepare essentially a spec for the config, or at least a solid example so that it's clear how things should be configured, and making sure that we can essentially utilize that fully as we implement this thing. It can be essentially our validation.

@adam: Agreed. I'll draft a complete example sync config as a separate file (`docs/design/sync-config-spec.md`) that covers all our tables with real Civitai field names. That becomes the validation target — if the dump processor can consume that config and produce correct bitmaps at 107M, we're good. I'll include it in the next iteration of this plan.

---

## Phase 2: WAL Reader Thread (Steady-State Ops Processing)

Priority: **HIGH** — without this, real-time updates don't work.

### 2.1 Background WAL Reader Thread

**Gap:** `/ops` endpoint writes to WAL file but nothing reads it.
**Fix:** Spawn a thread on server startup that tails the WAL file.

```
Loop:
  reader.read_batch(10_000)
  if empty: sleep(poll_interval) // 50ms default
  dedup_ops(&mut batch)
  apply_ops_batch(&mut coalescer_sink, &meta, &mut batch, Some(&engine))
  save_cursor(cursor_path, reader.cursor())
```

### 2.2 Ops → CoalescerSink → Flush Thread

**Gap:** `apply_ops_batch` exists but WAL reader doesn't call it.
**Fix:** Wire WAL reader → CoalescerSink → existing flush thread.

```
Reference: src/ops_processor.rs — apply_ops_batch<S: BitmapSink>(sink, meta, batch, engine)
Reference: src/ingester.rs:53   — CoalescerSink::new(engine.mutation_sender())
```

### 2.3 Docstore Writes from Steady-State Ops

**Gap:** Ops only update bitmaps, not docstore.
**Fix:** Two paths depending on op type:

**Single-slot ops (set/remove/add/delete):** Route through `engine.put()` or `engine.patch_document()` which handle docstore, computed fields, cache invalidation, and deferred alive automatically. Per Ryan's recommendation — avoids reimplementing all that logic.
@justin: No! Why would we do that? That creates a lot of overhead... If anything those pipelines should be using the new ops ingestion path or better yet, just decompose and write ops to the WAL for processing... This simplifies from having multiple ways of doing things to just one. Please adjust this item accordingly

@adam: You're right — one path, not two. Revised approach: ALL steady-state ops (including docstore writes) go through the same pipeline: ops → WAL → WAL reader → apply_ops_batch (with BitmapSink for bitmaps + DocSink for docstore). The apply_ops_batch function already handles all op types. We just need to wire DocSink alongside BitmapSink in that same function. No engine.put/patch, no separate paths. One pipeline handles everything: bitmap updates, docstore writes, computed fields, deferred alive.

@justin: Cool. I think you're right. Essentially, put and patch would go away on the engine. We'd probably still have the endpoints in tokio so that it's easier to use, and then those basically break things down into ops and write them to the wall.

@adam: Exactly. The HTTP PUT/PATCH endpoints stay as a convenience API (for manual upserts, testing, etc.) but internally they decompose the document into ops and write to WAL. Then the WAL reader processes them through the same pipeline as everything else. One path for all writes: HTTP PUT → ops → WAL → WAL reader → BitmapSink + DocSink. This means engine.put()/engine.patch_document() become thin wrappers that generate ops + write to WAL, rather than directly mutating staging.
@justin: Perfect.

**Fan-out ops (queryOpSet):** Keep BitmapSink path. A queryOpSet can resolve to millions of slots (e.g., "modelVersionIds eq 456" → 15M images). Routing each through `engine.put()` would be too slow. queryOpSet needs the bulk bitmap OR/ANDNOT pattern via BitmapSink + separate docstore batch update.

**Design doc deviation:** The design doc (M5) specifies per-op DocSink appends for everything. The put/patch approach is higher-level but avoids duplicating computed field logic. The queryOpSet still uses BitmapSink as the design intended.

---

## Phase 3: Activation Infrastructure

@justin: I think we should remove all V1 stuff as well.

@adam: Added to Phase 3 scope. V1 removal includes: BitdexOutbox table references, V1 outbox_poller, old Load/Sync/Setup subcommands, and the NDJSON loader path (load_ndjson in loader.rs). I'll inventory all V1 code paths and list them for removal.
@justin: Great. Can you go ahead and have an explorer go fetch all of those for you, I guess? This would probably include any of the hard-coded references since we're moving everything to config. Like those things in row_assembler.rs

@adam: Will do — I'll run a thorough inventory of all V1 code paths and hardcoded references in a follow-up. Quick high-level list from memory: `src/pg_sync/outbox_poller.rs` (V1 BitdexOutbox poller), `src/pg_sync/row_assembler.rs` (hardcoded enrichment queries), `src/pg_sync/queries.rs` (BitdexOutbox table setup), `src/pg_sync/bulk_loader.rs` (V1 COPY+assemble loader), `src/pg_sync/single_pass.rs` (V1 CSV loader — replaced by dump_processor.rs), `src/loader.rs:load_ndjson` (NDJSON path), old Load/Sync/Setup subcommands in `src/bin/pg_sync.rs`. I'll have an explorer do the full sweep and produce a concrete file list with line ranges.
@justin: Great. We'll definitely want to conclude that in the final implementation plan.

@adam: Will include the full V1 code inventory in the final plan. Running the explorer now as an action item.

Priority: **HIGH** — needed before V2 can run in production.

### 3.1 BitdexOps Table + PG Triggers

**Status:** SQL generation exists (`trigger_gen.rs`). Need:
- YAML config loading for SyncSource definitions
- Trigger reconciliation on sidecar boot (CREATE OR REPLACE, DROP stale)
- `bitdex-pg-sync setup-v2` subcommand to apply triggers

### 3.2 Sidecar Rename + Subcommands

**Gap:** Binary is `bitdex-pg-sync` with V1 subcommands (Load/Sync/Setup).
**Fix per design doc:** Rename to `bitdex-sync` with subcommands:
- `bitdex-sync pg` — PG ops polling (dump on boot + steady-state BitdexOps polling)
- `bitdex-sync ch` — ClickHouse metrics polling
- `bitdex-sync all` — both (default for production)

Dump vs steady-state is a lifecycle phase within the `pg` subcommand (boot → dump → transition → poll), not separate subcommands.

### 3.3 Boot Sequence

```
1. bitdex-sync waits for BitDex health check
2. Capture or create max(BitdexOps.id) as pre_dump_cursor @justin: Capture or Create, right?
   @adam: Yes — if BitdexOps table is empty (fresh deploy), create cursor at 0. If populated, capture current max(id). Either way, we have a cursor position to seed after dumps complete.
   @justin: Great
3. GET /api/indexes/{name}/dumps — check dump history
4. For each sync_source not yet dumped:
   a. PUT /api/indexes/{name}/dumps — register dump (returns task ID)
   b. COPY table from PG → write CSV to shared volume
   c. POST /api/indexes/{name}/dumps/{name}/loaded — signal CSV ready
   d. BitDex reads CSV, processes via Phase 1 pipeline
   e. BitDex saves bitmaps to BitmapFs, writes slot_counter
5. Seed cursor at pre_dump_cursor (catches dump-window ops)
6. Transition to steady-state ops polling (BitdexOps → POST /ops)
7. K8s readiness probe returns 200, traffic starts routing
```

@justin: The register/load system was necessary when we were writing to the WAL at the same time as we expected the WAL reader to be processing it. Since we'll be loading the full CSV and then processing it, I think we only need the register, no need to signal load. Just register once the CSV is ready.

@adam: Agreed — simplified to single registration. bitdex-sync writes CSV, then registers it with BitDex (`PUT /dumps` with csv_path). BitDex processes it synchronously (or async with status polling). No separate "loaded" signal needed. Updated boot sequence step 4 accordingly:
```
4. For each sync_source not yet dumped:
   a. COPY table from PG → write CSV to shared volume
   b. PUT /api/indexes/{name}/dumps — register dump with csv_path (BitDex processes it)
   c. Poll GET /api/indexes/{name}/dumps until complete (or sync wait)
```
@justin: that BitDex should process it asynchronously with a task that it returns. We already have a task system, so we can just utilize that same thing. You'll be able to find the APIs for it, and I think we even mentioned it in the design doc for the v2 final.

@adam: Right — the design doc mentions `GET /api/tasks/{task_id}` for polling dump processing status (line 344). So the flow is: PUT /dumps returns a task_id, bitdex-sync polls GET /tasks/{task_id} until status=complete. BitDex processes the CSV asynchronously on a background thread. This also means the dump processor needs to integrate with the existing task system. I'll check what task infrastructure already exists in server.rs.
@justin: Okay, probably worth noting that action that you're going to need to take, I guess, somewhere, just so that we don't lose it.

@adam: Noted. Action item: audit existing task system in server.rs, confirm API shape (POST returns task_id, GET /tasks/{id} returns status), and document what dump_processor.rs needs to integrate with it. Will include in the final plan's Phase 1 task list.

### 3.4 Config Hash Change Detection

Dump names: `{table}-{config_hash8}`. If config changes, hash changes, name doesn't match → auto-re-dump.

---

## Phase 4: Config-Driven CSV Processing

Priority: **MEDIUM** — hardcoded version works but isn't maintainable.

### 4.1 Sync YAML Config

Replace hardcoded CSV parsers with a config that maps:
```yaml
sync_sources:
  - table: Image
    slot_field: id
    sets_alive: true
    track_fields:
      - { source: nsfwLevel, target: nsfwLevel, type: integer }
      - { source: type, target: type, type: low_cardinality_string }
      - { source: userId, target: userId, type: integer }
    copy_columns: "id, url, nsfwLevel, hash, flags, type::text, userId, ..."

  - table: TagsOnImageDetails
    slot_field: imageId
    sets_alive: false
    track_fields:
      - { source: tagId, target: tagIds, type: multi_value_integer }
```

@justin: This is starting to overlap with the general index config... There are probably some things we can drop from here since we have them defined there... We should revise this. It also doesn't really align with the design doc.

@adam: Agreed. Revised approach per your Q1 answer (Option A): sync config only defines PG-specific stuff. Field types/names already live in index config.json. Sync config becomes:
```yaml
sync_sources:
  - table: Image
    slot_field: id
    sets_alive: true
    copy_columns: "id, url, nsfwLevel, hash, ..."
    # Field names + types come from index config.json (filter_fields, sort_fields, data_schema)

  - table: TagsOnImageNew
    slot_field: imageId
    sets_alive: false
    copy_columns: "imageId, tagId, attributes"
    target_field: tagIds  # maps to filter_fields entry in index config
```
And per your Q1 answer about dump registration: bitdex-sync sends the CSV with a column→field mapping as part of the PUT /dumps request, so BitDex knows how to process it. The mapping is derived from sync config + index config together.

@justin: Okay, the config there for sync looks good. We'll want to put together a full config, like I mentioned above, probably in a separate file that we just point to, or it can be in here. I think the original design doc had a pretty solid one. and then I also want to see what data we're going to be sending with the dump request so we can make sure that we fully cover the information that we're going to need to send for the dump to be processed correctly.

@adam: Will produce two things: (1) Full sync config spec file with all Civitai tables, and (2) The dump request body schema showing exactly what bitdex-sync sends to BitDex. Both in the next iteration. The design doc's YAML config (lines 129-188 in pg-sync-v2-final.md) is a good starting point — I'll extend it with the enrichment and column_map additions.
@justin: Ok, great. for this one. Can you go ahead and follow the pattern that we landed on the bottom of this file for the config? And then let's look at that config together and I'll let you know if I have any feedback.

@adam: Action item taken — drafting the full sync config now using the dump_phases pattern from Q2 (with nested enrichment lookups). Will produce `docs/design/sync-config-civitai.yaml` as a concrete, complete example covering all tables. See below in this response.

### 4.2 Generic CSV Processor

Read sync config → for each source, generate:
- CSV column parser (position-based from copy_columns)
- Field→bitmap mapping (from track_fields)
- Enrichment joins (from depends_on)

@justin: Can you explain the joins? Why do we need to do this? Can't we just populate directly from the table as we speced in the schema in the design doc?

@adam: You're right — for most tables we can populate directly. The "joins" I mentioned are only for the enrichment chain where one table's data populates fields on another table's slots. Specifically:
- **Posts** → enriches Image slots with publishedAt, availability, postedToId, isPublished
- **ModelVersions** → enriches Resource slots with baseModel
- **Models** → enriches Resource slots with poi (through MV → Model chain)

For dump, we load these as HashMaps for fast lookup (per Q2 answer). For steady-state, these come through as queryOpSet ops that resolve affected slots via bitmap query.

The sync config needs to express this dependency: "when processing Images, load Posts enrichment first; column mapping: post.published_at_secs → image.publishedAt". Proposed config addition:
```yaml
  - table: Image
    enrichment:
      - source_table: Post
        join_field: postId           # Image.postId → Post.id
        fields:
          - { source: published_at_secs, target: publishedAt }
          - { source: availability, target: availability }
```
For tables that populate directly (Tags, Tools, Techniques, Metrics, CollectionItems), no enrichment block needed — they map 1:1 from CSV columns to bitmap fields.

@justin:* Cool. So, do we have the Bitdex server read this sync doc for understanding how to parse the CSV then? Or... Yeah, I think that the how to parse the CSV still needs to come in as part of the endpoint, right? This is just more about... Yeah, I don't know. I guess providing the sync config the information that it's going to need to be able to use, that you need to send to the Bitdex server as part of processing this CSV or whatever, right? And then the sync server will know that it needs to prepare the post dataset before, like, download it and all of that before it can send the image CSV to the dump endpoint.

---

## Ollie's Findings — Status

| # | Finding | Impact | Status |
|---|---|---|---|
| 1 | Op abstraction in dump (String+JsonValue allocs) | HIGH | DONE (834e806) — direct bitmap writes |
| 2 | BufReader line-by-line vs mmap | MEDIUM | TODO (Phase 1.3) |
| 3 | BitmapAccum clones field name Strings per rayon task | LOW | TODO |
| 4 | json_to_qvalue + value_to_bitmap_key double conversion | HIGH | DONE (834e806) — eliminated with direct writes |
| 5 | HashMap for sort bit layers → use Vec | LOW | TODO |
| 6 | apply_accum serial bottleneck (clone per chunk) | HIGH | DONE (uncommitted) — clone-once pattern |
| 7 | Dict HashMap keyed by String | LOW | TODO |
| 8 | No deferred alive in dump path | CRITICAL | PARTIAL — alive skip done, but filter/sort still set (CORRECTNESS BUG, see 1.6) |

@justin: I want all of these to be done, even if they're low priority.

@adam: Understood. All 8 will be addressed in the dump pipeline rewrite. Items 1, 4, 5, 6, 8 are resolved by moving to the single_pass.rs-based architecture (direct writes, Vec for sort layers, no apply_accum). Items 2 (mmap) and 3 (field name Strings) get fixed as part of matching single_pass.rs patterns. Item 7 (dict HashMap keyed by String) can be fixed by interning dict keys with Arc<str>.

---

## Testing Strategy

### Phase 1 Tests: Dump Pipeline

**T1.1 — Bitmap correctness (per-field spot checks)**
After dump completes, start the BitDex server on the dumped data and run known queries:
- `nsfwLevel eq 1` → count should match `SELECT count(*) FROM "Image" WHERE "nsfwLevel" = 1`
- `tagIds eq 42` → count should match tag association count in PG
- `type eq "image"` → verify LCS dictionary resolves correctly
- `baseModel eq "SDXL"` → only Checkpoint model types contribute
- `userId eq {known_user}` → spot check a specific user
Run against both V2 dump output and current single_pass.rs output — counts must match.

**T1.2 — Sort field correctness**
- Query `sort=reactionCount desc limit 10` → verify order matches PG `ORDER BY "reactionCount" DESC LIMIT 10`
- Query `sort=sortAt desc limit 10` → verify GREATEST(existedAt, publishedAt) computation
- Query `sort=id desc limit 10` → verify slot-as-sort works

**T1.3 — Deferred alive**
- Identify images with `publishedAt > now()` in the test dataset
- After dump: query all-time sort → these images must NOT appear
- Manually advance the clock or call activate_due → they should appear

**T1.4 — Docstore completeness**
- After dump, GET `/documents/{slot_id}` for a known image
- Verify all fields present: url, hash, width, height, nsfwLevel, userId, etc.
- Verify doc-only fields (url, hash, width, height) that don't have bitmaps

**T1.5 — Memory + performance**
- 107M dump completes under 15GB RSS (monitor via `/proc/self/statm` or equivalent)
- Total time < 10 min (matching single_pass.rs baseline)
- Per-phase RSS should drop after save+drop (verify memory is freed between phases)

**T1.6 — Dictionary persistence**
- After dump, check dictionaries/{name}.dict files exist for type, availability, blockedFor, baseModel
- Restart server → dictionaries load from disk → LCS queries still work

**T1.7 — Crash recovery**
- Kill dump midway through Phase 3 (resources)
- Restart → `field_already_loaded()` detects tags + images already done
- Resume from resources → completes without re-processing earlier phases

### Phase 2 Tests: Steady-State Ops

**T2.1 — Single op roundtrip**
- POST `/ops` with a single `set` op: `[{"op":"set","field":"nsfwLevel","value":32}]` on a known slot
- Query `nsfwLevel eq 32` → slot appears
- GET `/documents/{slot}` → docstore updated

**T2.2 — Multi-value add/remove**
- POST `/ops` with `add` tagIds=99999 on a slot
- Query `tagIds eq 99999` → slot appears
- POST `/ops` with `remove` tagIds=99999
- Query `tagIds eq 99999` → slot gone

**T2.3 — Delete**
- POST `/ops` with `delete` on a known alive slot
- Alive count decreases by 1
- All filter bitmaps for that slot are cleared (clean delete)

**T2.4 — queryOpSet fan-out**
- POST `/ops` with `queryOpSet "modelVersionIds eq 456"` + `set baseModel "SDXL"`
- All images with modelVersionId=456 now have baseModel=SDXL in queries

**T2.5 — Deferred alive via ops**
- POST `/ops` with creates_slot=true and publishedAt in the future
- Slot should NOT be alive/queryable
- After timestamp passes → activate_due fires → slot becomes alive

**T2.6 — WAL cursor persistence**
- POST several ops batches
- Kill server, restart
- WAL reader resumes from cursor → no duplicate processing

### Phase 3 Tests: Boot Sequence

**T3.1 — Fresh pod boot**
- Start with empty data dir
- bitdex-sync runs dumps → all CSVs processed → ops polling starts
- Readiness probe returns 200
- Queries return correct results

**T3.2 — Config change re-dump**
- Change a field in sync config (add a new track_field)
- Config hash changes → dump name doesn't match
- bitdex-sync detects mismatch → re-dumps affected table

**T3.3 — Dump-window ops**
- During dump, simulate PG writes (ops accumulate in BitdexOps)
- After dump completes, cursor seeded at pre_dump_cursor
- Ops polling catches up → no data loss

### Phase 4 Tests: Config-Driven Processing

**T4.1 — Config parity**
- Process all CSVs with new config-driven dump_processor.rs
- Compare bitmap output (alive count, filter counts, sort layer checksums) against single_pass.rs output
- Must be identical

**T4.2 — Config validation**
- Malformed config → clear error message (not a crash)
- Missing required field → error
- Unknown field name (not in index config) → warning

---

## Verification Checklist

Before declaring V2 done, each item must pass:

- [ ] T1.1: Bitmap spot checks match PG counts for 5+ fields
- [ ] T1.2: Sort queries return correct order vs PG
- [ ] T1.3: Deferred alive images invisible before timestamp
- [ ] T1.4: Docstore has all fields including doc-only
- [ ] T1.5: 107M dump < 15GB RSS, < 10 min
- [ ] T1.6: Dictionaries persist and restore
- [ ] T1.7: Crash recovery resumes from last completed phase
- [ ] T2.1-T2.4: All op types roundtrip correctly
- [ ] T2.5: Deferred alive via ops path works
- [ ] T2.6: WAL cursor survives restart
- [ ] T3.1: Fresh pod boots to serving with no manual steps
- [ ] T3.2: Config change triggers re-dump
- [ ] T3.3: No data loss during dump window
- [ ] T4.1: Config-driven output matches single_pass.rs output

---

## Recommended Execution Order

1. **Phase 1 (dump pipeline rewrite)** — 1-2 sessions
   - New dump_processor.rs based on single_pass.rs (BitmapFs direct, per-phase save+drop, mmap)
   - Tests: T1.1-T1.7
   - Gate: 107M under 15GB, bitmap counts match single_pass.rs

2. **Phase 2 (WAL reader thread)** — 1 session
   - Wire background thread, BitmapSink + DocSink, cursor persistence
   - Tests: T2.1-T2.6
   - Gate: all op types roundtrip, WAL cursor survives restart

3. **Phase 3 (activation)** — 1-2 sessions
   - Sidecar rename, subcommands, trigger reconciliation, boot sequence, V1 removal
   - Tests: T3.1-T3.3
   - Gate: fresh pod boots to serving, config change re-dumps

4. **Phase 4 (config-driven)** — 1 session
   - Replace hardcoded parsers with config-driven processing
   - Tests: T4.1-T4.2
   - Gate: output identical to single_pass.rs

---

## Correctness Bugs Found During Review

These exist in the current merged code (PR #86) and must be fixed:

1. **baseModel set for ALL model types** — single_pass.rs only sets baseModel when model type is "Checkpoint" (line 1147). Current ops_processor sets it for every ModelVersion. This means non-Checkpoint model versions incorrectly populate the baseModel filter.
@justin: I guess we'll probably need to put something in place to make it so that as part of the steady state sync, we also have the trigger-only trigger on checkpoints.

@adam: Right. For dump: we filter in the dump processor (only set baseModel when model_type == "Checkpoint"). For steady-state: the PG trigger on ModelVersion should include a `filter` clause like `WHERE "Model"."type" = 'Checkpoint'` in its queryOpSet, so non-Checkpoint ModelVersion changes don't emit baseModel ops at all. This goes into the sync config as a filter on the ModelVersion sync_source.

@justin: because we don't want to hard code anything, that filter in the dump processor... I guess that's going to have to be part of the config that gets passed in to the endpoint for the dump. Or maybe as part of the sync settings. I guess we need to decide if the sync settings get read by the Bitdex server at all, and I think the answer is probably no. I think the Bitdex server's completely blind to all of that. It just has config options that are accepted as part of the dump request, and the and the licensing server populates those based on its config.

@adam: Agreed — BitDex server stays blind to sync config. The dump request body carries everything BitDex needs: column_map, filters, enrichment lookups. bitdex-sync reads its sync config, translates it into the dump request payload, sends it with PUT /dumps. BitDex just executes what it receives. This keeps BitDex generic — it could process dumps from any source, not just PG. I'll include the dump request body schema in the sync config draft.

2. **Deferred alive sets filter/sort bitmaps** — Current code sets filter+sort bitmaps but skips alive for deferred slots. Per CLAUDE.md principle 5 (filter bitmaps are always clean, alive NOT ANDed into queries), this makes scheduled posts appear in query results before their publishedAt time.
@justin: Correct. We should double-check with Ollie about the correct way to handle deferred alive. He worked on this and identified that the only place that this isn't being handled is in the loading, so you might have him do a deeper dive to make sure that that's true and that things will get into the alive state eventually, that we're just putting stuff into the doc store for later pull

@adam: Will ask Ollie to do a full deferred alive audit. The questions to confirm: (1) dump path correctly skips ALL bitmaps for deferred slots but writes docstore, (2) `activate_due()` in the timer thread correctly reads docstore to build filter+sort bitmaps when the timestamp arrives, (3) steady-state ops path handles deferred alive for new creates with future publishedAt, (4) no other code paths bypass this check.

@justin: So, ask him about this now just to make sure. And then confirm in your next comment here that you've done that and that you've got all the details captured somewhere. So we can include that in the final implementation plan.

@adam: Ollie completed the audit. Full details below:

**Deferred Alive Audit (from Ollie):**

1. **Dump path (single_pass.rs):** CORRECT after PRs #77/#79. Future-dated slots: skip alive/filter/sort, write docstore only, collect BTreeMap, save via write_deferred_alive(). Slot counter includes deferred slots.

2. **activate_due() code path:** concurrent_engine.rs:1545-1600. Flush thread calls `staging.slots.activate_due(now_unix)` every cycle. For each due slot: reads stored doc from docstore → calls `diff_document(slot, None, &doc)` as fresh insert → builds filter/sort/alive bitmaps → persists updated deferred map.

3. **V2 steady-state bug:** The PATCH path (`diff_document_partial` at mutation.rs:295) skips deferred alive check. Fresh inserts via put_inner → diff_document are correct. Bug is specifically: PATCH on an existing alive slot with future publishedAt won't re-defer it. **Fix: add deferred check to diff_document_partial().**

4. **All bypass points:**
   - ~~bulk_loader.rs~~ — FIXED (PR #77)
   - ~~single_pass.rs~~ — FIXED (PR #79)
   - **diff_document_partial** — UNFIXED (mutation.rs:295)
   - **put_bulk/put_bulk_loading** — doesn't check deferred. Only used by old NDJSON loader + benchmark. Low priority.
   - **apply_bitmap_maps** — sets alive unconditionally. Used by old bulk_loader (handled upstream). ops_processor handles at accum level.

5. **V2 dump_processor.rs correct behavior:** Skip ALL bitmaps for future-dated, write docstore only, collect BTreeMap<u64, Vec<u32>>, save via bitmap_fs.write_deferred_alive(). Docstore entries MUST exist for activate_due() to work. Slot counter must include deferred slots.

3. **modelVersionIdsManual** — Current ops_processor populates this for `!detected` resources. single_pass.rs doesn't have this field. Need to confirm whether this field is actually needed or was added by mistake.
@justin: This is more correct than the old version.

@adam: Good — keeping modelVersionIdsManual. Will add it to single_pass.rs-based dump processor and ensure the sync config includes it for the Resources sync_source.

---

## Questions for Justin

### Q1: Sync config overlap with index config

You noted that `sync_sources.track_fields` overlaps with `config.json`'s `data_schema.fields` and `filter_fields`/`sort_fields`. Both define field names, types, and mappings.

**Options:**
- A) Sync config only defines PG-specific stuff (table name, COPY columns, enrichment joins). Field types come from index config.
- B) Sync config is the single source of truth for field definitions, index config derives from it.
- C) Keep both but validate they don't contradict.

Which direction?
@justin: I was imagining option A, but I'm curious what the config would look like if we just included everything in the config. But no, I think option A is probably the best because it is tied specifically to the sync thing. And we were planning on using it specifically for handling dumps, I guess, as well. But do we really need it for dumps? I don't think we even need it for dumps for processing those CSVs. I think the config that we have is adequate. that's not the same. So, I guess maybe we need to make it so that as part of sending in a dump, you can specify how the CSV props map or something like that, and then pg-sync as part of sending its CSV, which we're just calling bit-dex-sync now, will handle sending in the details that the Bitdex server needs to process the CSV correctly. Does that make sense?

@adam: Makes sense. So the flow is: bitdex-sync owns the column→field mapping knowledge (from its sync config), and when it registers a dump with BitDex, it sends that mapping as part of the request body:
```json
PUT /api/indexes/civitai/dumps
{
  "name": "tags-a1b2c3d4",
  "csv_path": "/data/load_stage/tags.csv",
  "slot_field": "imageId",
  "sets_alive": false,
  "column_map": [
    { "column": 0, "field": "tagIds", "type": "multi_value_integer" },
    { "column": 2, "field": null, "type": "attributes_bitfield" }
  ],
  "enrichment": null
}
```
BitDex receives this, parses the CSV using the column_map, and maps to its own index config's filter_fields/sort_fields. BitDex doesn't need sync config — it just needs the column mapping per dump. This keeps the coupling clean: bitdex-sync knows PG schema, BitDex knows bitmap schema, the column_map bridges them.

@justin: is this a legit example for tags? Because I don't know that I fully follow the column map for like the attributes bitfield. Are you suggesting that that's going to have to be hard encoded in Bitdex how to handle those bits? Because obviously we don't want to do that. Nothing in the Bitdex server should be hard coded to our config.

@adam: No — that `attributes_bitfield` type was a bad example. The attributes filtering should be handled by bitdex-sync before the CSV reaches BitDex, not by BitDex itself. Two options:

Option A (preferred): The COPY query filters server-side: `COPY (SELECT "tagId", "imageId" FROM "TagsOnImageNew" WHERE ("attributes" >> 10) & 1 = 0) TO STDOUT`. BitDex never sees disabled tags. Clean separation.

Option B: If we want to COPY all rows and filter client-side, bitdex-sync filters before writing the CSV (or the dump config has a `filter` expression that the dump processor evaluates per row). BitDex still doesn't know about attributes.

Either way, the column_map for tags becomes simply: `[{ column: 0, field: "tagIds" }]` with `slot_field: column 1`. No bitfield handling in BitDex. The filtering happens before BitDex ever sees the data.
@justin: I think what we landed on below, kind of the bottom where we had the final version of the sync config is having a filter on that field.

@adam: Right — the sync config for tags will have the filter expression. BitDex evaluates it per-row during dump processing. Included in the sync config draft.

### Q2: queryOpSet vs enrichment-join for dump processing

The design doc says Post and ModelVersion should use `queryOpSet` during dump (e.g., dump Posts, then for each post emit `queryOpSet "postId eq {id}"` to resolve affected images). single_pass.rs uses enrichment-join instead (load posts into HashMap, look up during image processing).

The enrichment-join is much faster for dumps (HashMap lookup vs bitmap query per post). But it means the dump path and steady-state path use completely different code.

Should the dump path use enrichment-joins (matching single_pass.rs) or should we make queryOpSet fast enough for dumps too?

@justin: Let's see if we can make the query offset fast enough for dumps too. I mean, the reality is that a lookup takes microseconds, I would think. I mean, I guess we can see how that query takes against our real BetDex server. And so I'm hoping that even if we run this, it won't be too bad. You're right, it probably can never be as fast as a HashMap lookup. Probably for dump, we would want it to be a bit faster. to be a hash map in Rich, and then for the steady state, it obviously gets handled this other way. We'll need to make sure that both are wired up, obviously. And then... We'll need to figure out how to define inside of the config schema, and maybe you can throw out an idea below, how... that, you know, this CSV needs to get loaded as enrichment for this one, and this is how it's attached and what it should populate.

@adam: Two paths, both wired:
- **Dump:** HashMap enrichment (fast). Sync config defines the dependency:
```yaml
  - table: Image
    enrichment:
      - source_table: Post
        join_field: postId        # Image.postId → Post.id
        fields:
          - { source: published_at_secs, target: publishedAt, type: sort }
          - { source: availability, target: availability, type: filter }
      - source_table: ModelVersion
        join_field: null           # Not direct — goes through Resources table
        chain_through: Resources   # Resource.modelVersionId → MV.id
        fields:
          - { source: base_model, target: baseModel, type: filter, filter: "model_type == 'Checkpoint'" }
```
- **Steady-state:** queryOpSet. When a Post is updated, the PG trigger emits `queryOpSet "postId eq {post_id}"` with the changed fields. BitDex resolves the query against its bitmaps to find affected image slots.

Both paths produce the same bitmap result — the enrichment config drives both. For dump it generates HashMap lookups, for steady-state it generates queryOpSet ops.

@justin: Okay, I've got a few questions here. So first, why do we need type here? Isn't that configuration that the BitDex server cares about? It already knows what these fields are. It knows what the target config is. So I don't know why we need to pass that, so you'd need to explain that to me. And then for the way that things are connected, I'm not sure that I like that. We had a lot of config attached to post and model version and things like that, which I feel like is getting missed here. I guess, does it still get defined as a table and then it gets connected here like this? That would make sense. The join chain through thing. I wonder if we can maybe do that a bit differently, because it's not clear at all how that would get chained. Seems like maybe something like this would be better:

@adam: You're right — the `type: sort/filter` is redundant. BitDex already knows from its index config whether `publishedAt` is a sort field or `availability` is a filter field. The sync config only needs to map source→target. Removed it. Your `on:` syntax is much clearer for expressing the join path. I like it.
```yaml
- table: Image
  dump:
    enrichment:
      - source_table: Post
        on: Image.postId = Post.id
        fields:
          - { source: published_at_secs, target: publishedAt }
          - { source: availability, target: availability }
      - source_table: ModelVersion
        on: ModelVersion.id = Resources.modelVersionId = Image.id
        fields:
          - { source: base_model, target: baseModel, where: "model_type == 'Checkpoint'" } # @justin: Does the model version table have model underscore type? Or is that going to have to be joined from the model table? If so, maybe something like this: `where: "Model.type = 'Checkpoint'"
          # @adam: ModelVersion does NOT have model_type directly — it has modelId which references Model.id, and Model has type. So the filter needs to go through the Model table. Your syntax `where: "Model.type = 'Checkpoint'"` is correct. For the dump processor, this means: when processing resources, load Model HashMap too, look up model_type via MV.modelId → Model.id, only set baseModel if model_type == "Checkpoint". We already do this chain today (just without the filter). For steady-state, the PG trigger on ModelVersion would need to JOIN Model to check type before emitting the queryOpSet.
          # @justin: Yeah, I think the way that we handled this below is probably better, and then it keeps it separate enough that I think we can handle essentially doing the actual SQL query from this to get the correct joins in, right?
          # @adam: Yes — the nested enrichment pattern from Q2 below handles this cleanly. The model_type filter goes on the nested models.csv lookup with `filter: "type = 'Checkpoint'"`. For the PG trigger SQL generation, the trigger_gen can produce the correct JOIN from this config: ModelVersion trigger joins Model table to check type before emitting the queryOpSet.
```
@justin: How difficult is this going to be? This is starting to get pretty complex here, but seems necessary. Is there maybe another way that we could say all of this that's going to be easier to process, given this config?

@adam: Honestly, parsing and executing the enrichment config is the hardest part of the whole thing. The `on:` chain syntax needs a mini-interpreter to resolve multi-hop joins. A simpler alternative: instead of a generic join engine, we define enrichment as **ordered dump phases** where each phase can reference data from prior phases. Like this:

```yaml
dump_phases:
  - name: tags
    csv: tags.csv
    slot_field: imageId
    fields: [{ column: tagId, target: tagIds }]
    filter: "(attributes >> 10) & 1 = 0"

  - name: images
    csv: images.csv
    slot_field: id
    sets_alive: true
    fields: [{ column: nsfwLevel, target: nsfwLevel }, ...]
    enrichment:
      - lookup: posts.csv           # load as HashMap<id, row>
        key: id                     # key in the lookup CSV
        join_on: postId             # field in images.csv that references the key
        fields: [{ source: published_at_secs, target: publishedAt }, ...]

  - name: resources
    csv: resources.csv
    slot_field: imageId
    fields: [{ column: modelVersionId, target: modelVersionIds }]
    enrichment:
      - lookup: model_versions.csv
        key: id
        join_on: modelVersionId     # field in resources.csv
        fields: [{ source: base_model, target: baseModel }]
        chain_lookup: models.csv    # second-level lookup through modelId
        chain_key: id
        chain_on: modelId           # field in model_versions.csv
        chain_filter: "type = 'Checkpoint'"
        chain_fields: [{ source: poi, target: poi }]
```

This is more explicit but avoids needing a generic SQL-like join parser. Each `lookup` is just "load CSV as HashMap, look up by key". The `chain_lookup` is "from the first lookup result, do a second lookup". Two levels of lookup covers all our cases (Post→Image, MV→Model→Resource). We don't need a generic N-hop join engine.

The implementation is straightforward: load lookup CSVs as `HashMap<i64, Vec<u8>>` (key → raw row bytes), iterate main CSV, look up enrichment per row. This is exactly what single_pass.rs does today, just config-driven instead of hardcoded.

@justin: Okay, if that's gonna make things easier, that makes sense. I do wonder though, rather than having a chain underscore series of commands, if we just make it so that a lookup can also have a lookup, like this.

@adam: Much better — recursive `enrichment` nesting is cleaner than the chain_lookup flat keys. This is the pattern I'll use in the full config draft. Each lookup can optionally have its own `enrichment` child lookups, and `filter` at any level. Implementation-wise this maps to nested HashMap lookups which is straightforward.
```yaml
dump_phases:
  - name: tags
    csv: tags.csv
    slot_field: imageId
    fields: [{ column: tagId, target: tagIds }]
    filter: "(attributes >> 10) & 1 = 0"

  - name: images
    csv: images.csv
    slot_field: id
    sets_alive: true
    fields: [{ column: nsfwLevel, target: nsfwLevel }, ...]
    enrichment:
      - lookup: posts.csv           # load as HashMap<id, row>
        key: id                     # key in the lookup CSV
        join_on: postId             # field in images.csv that references the key
        fields: [{ source: published_at_secs, target: publishedAt }, ...]

  - name: resources
    csv: resources.csv
    slot_field: imageId
    fields: [{ column: modelVersionId, target: modelVersionIds }]
    enrichment:
      - lookup: model_versions.csv
        key: id
        join_on: modelVersionId     # field in resources.csv
        fields: [{ source: base_model, target: baseModel }]
        enrichment:
          - lookup: models.csv    # second-level lookup through modelId
            key: id
            join_on: modelId           # field in model_versions.csv
            fields: [{ source: poi, target: poi }]
            filter: "type = 'Checkpoint'"
```

### Q3: BitmapFs vs ShardStore for dump persistence

single_pass.rs writes to BitmapFs (`save_filter_field_to_disk`, `write_alive`, etc.). The design doc mentions ShardStore. CLAUDE.md says BitmapFs is "legacy" and ShardStore is current.

Should the V2 dump pipeline write to BitmapFs (matching single_pass.rs, known to work) or ShardStore (matching the stated architecture direction)?

@justin: V2 should be using ShardStore. I don't know why we were still using BitmapFS. We should basically be completely removing BitmapFS. If there's references to it, you should tell me what they are so that we can know if they can just be retired. That probably needs to be its own thing. I thought that during implementation of BitmapFS, it was designed as essentially just a wrapper for ShardStore, so we'll need to check on that. That could be the easy migration path for now.

@adam: BitmapFs is NOT a wrapper for ShardStore — they're separate implementations. BitmapFs (`src/bitmap_fs.rs`) writes `.fpack` files for filters and `.sort` files for sort layers, using its own hex-bucketed directory structure. ShardStore (`src/shard_store.rs`) uses a different format with 28-byte headers, CRC32 ops logs, and generation model.

BitmapFs references in the codebase:
- `src/bitmap_fs.rs` — the implementation (~800 lines)
- `src/pg_sync/single_pass.rs` — all dump save calls use BitmapFs (save_filter_field_to_disk, write_alive, etc.)
- `src/pg_sync/bulk_loader.rs` — same
- `src/bin/server.rs` — bitmap restore on startup uses BitmapFs
- `src/loader.rs` — load_from_bitmapfs restore path
- `src/concurrent_engine.rs` — save_snapshot references

For the V2 dump processor, I'll write save functions that target ShardStore instead. The single_pass.rs save_filter_field_to_disk/save_sort_field_to_disk functions need ShardStore equivalents. This should be a separate preparatory PR before the dump rewrite. The engine's lazy loading already reads from ShardStore, so once we write to ShardStore, restore works automatically.

@justin: Ugh! I'm really bummed to hear this. BitmapFS was supposed to be retired at the same time that we added ShardStore. I don't want to have to tackle that project right now. I guess we just use bitmapFS and we can tackle that replacement later. What do you think?

@adam: Yeah, let's use BitmapFs for now and retire it later. The dump processor will use the same save_filter_field_to_disk/save_sort_field_to_disk functions that single_pass.rs already uses. They work at 107M, they're proven. ShardStore migration for the dump path can be a follow-up PR — it's a drop-in replacement for the save functions, not a structural change. I'll note it as a post-V2 task.

@justin: Ok, good!

### Q4: Should Phase 1 rewrite replace or extend single_pass.rs?

single_pass.rs already does everything Phase 1 describes — mmap, per-phase save, deferred alive, docstore writes, crash recovery. The V2 dump pipeline in ops_processor.rs is a worse version of the same thing.

**Options:**
- A) Delete ops_processor.rs dump code, extend single_pass.rs to be the V2 dump processor (add string dict, enrichment chain, computed sortAt).
- B) Rewrite ops_processor.rs to match single_pass.rs patterns (duplicate the code but in the V2 context).
- C) Extract shared infrastructure from single_pass.rs into reusable components, use from both.

Option A seems simplest — single_pass.rs already works at 107M under 15GB. Why rebuild it?
@justin: I agree. I think it makes sense to make a new file based on single-pass RS. I think ops process probably is still something that needs to exist because it gets used for the steady state, but we can put everything in there tied to dump code, like you said. And then we'd make a new dump processor that has pretty much everything from single-pass, obviously, except all of the hard-coded and probably something to handle config and handle CSV-specific stuff. So, yeah. What do you think?

@adam: Agreed. The plan:
- **New file: `src/dump_processor.rs`** — based on single_pass.rs, config-driven, writes to ShardStore. Handles all dump-time CSV processing. Receives column_map from the dump registration request.
- **Keep `src/ops_processor.rs`** — steady-state only. WAL reader → apply_ops_batch with BitmapSink + DocSink. No dump code.
- **Delete from ops_processor.rs:** `process_csv_dump_direct()`, `process_multi_value_csv()`, `apply_accum_to_staging()`, all the CSV-related imports. Keep: `FieldMeta`, `apply_ops_batch`, `process_set_op`, `process_remove_op`, etc.
- **Delete `src/pg_sync/single_pass.rs`** once dump_processor.rs replaces it.
@justin: Yeah, the single pass stuff can probably be deleted as part of the V1 cleanup, I would think. Unless you think the V1 clean-up should happen first

@adam: I'd do V1 cleanup last (Phase 3), after the new dump_processor.rs is working and validated at 107M. That way we have a clean cutover: new code works → delete old code. If we delete V1 first, we lose the ability to fall back. Order: build dump_processor.rs → validate → then sweep V1 code.
@justin: Ok, good.

### Q5: modelVersionIdsManual — keep or drop?

The current ops_processor populates `modelVersionIdsManual` for resources where `detected=false`. single_pass.rs doesn't have this field. Is this a real field that queries need, or was it added by mistake?

@justin: Yes, this is a real field that queries need.

@adam: Confirmed — modelVersionIdsManual stays. Will be included in dump_processor.rs and in the Resources sync_source config.


@justin: So, another thing that we'll need to think about is that there is flag attributes on the tags on images new table. I'm not sure if we're pulling in that column or not. We should check on that. Based on that determines whether or not we include that tag. So, I think probably what needs to happen is we don't necessarily need to pull that property as part of the dump. I mean, I don't know. We'll have to figure out how we want to do that. I know that we're handling flags elsewhere for images, so we'll just want to make sure that we do the same thing here for tags, and that both the steady state and dump state handle it. You can look at this file to understand how we filter tags: C:\Dev\Repos\work\model-share\src\server\redis\caches.ts:39

@adam: Good catch. The `TagsOnImageNew` table has an `attributes` integer with bit-packed flags. The view `TagsOnImageDetails` decodes them. The key field for us is `disabled` — bit 10: `(attributes >> 10) & 1`. If `disabled=true`, the tag should NOT be included in tagIds.

**Current state:** Our COPY query for tags pulls from `TagsOnImageNew` which has raw `(tagId, imageId, attributes)`. The current tags.csv parser (`parse_tag_row`) only reads tagId and imageId — it ignores `attributes` entirely. So we're including disabled tags.

**Fix for dump:** The COPY query should either:
- A) Pull from the `TagsOnImageDetails` view and filter `WHERE NOT disabled`, or
- B) Pull raw `attributes` column and filter in the dump processor: `if (attributes >> 10) & 1 == 0` (not disabled)

Option B is better for performance (no view overhead on 1.6B rows) and lets us handle it in BitDex.

**Fix for steady-state:** The PG trigger on `TagsOnImageNew` should check the disabled bit and either:
- Not emit an op if disabled=true (tag not indexed)
- Emit a Remove op if disabled transitions from false→true

This needs to be in the sync config for the Tags sync_source: a filter condition on the attributes column.

**Action items:**
1. Check if tags.csv currently includes the `attributes` column (check copy_queries.rs COPY command)
2. Update `parse_tag_row` to read attributes and skip disabled tags
3. Add to sync config for Tags: `filter: "(attributes >> 10) & 1 = 0"`
4. Ensure steady-state trigger handles disabled transitions
@justin: I agree. Option B is the way to go. And then for the steady state, we just need the config to drive, you know, filtering, like we already do in other places.

@adam: Got it. Option B for tags: COPY includes attributes column, dump processor filters `(attributes >> 10) & 1 == 0` per row. Steady-state trigger on TagsOnImageNew includes the same filter condition in its config, so disabled tag changes don't emit ops (or emit Remove ops on disable transitions). I'll check whether copy_queries.rs currently pulls the attributes column — if not, we need to add it to the COPY query.

@justin: I think the current CSV that we're running against doesn't have this column, so that's probably going to be an issue since it'll be different than what we have in production. And it's a massive file, so I don't think it makes sense for us to try and add that column. Maybe there's some way for this sync, the local sync, we can just have that source value that would just be, I don't know, I don't know. Maybe we just don't filter on that. We just put everything in locally for now. What do you think?

@adam: Makes sense — for local testing with the existing 63GB tags.csv (which only has tagId, imageId), we skip the disabled filter. The filter is optional in the config: if the `filter` field isn't present or the column isn't in the CSV, we include all rows. In production the COPY query will include the attributes column and the filter will apply. For local dev, we just don't set the filter in the dump request. The dump processor handles both cases — filter present = evaluate, filter absent = include all.

```
CREATE OR REPLACE VIEW "TagsOnImageDetails" AS
SELECT
  "imageId",
  "tagId",
	CASE
    WHEN (("attributes" >> 12) & 15) = 1 THEN 'User'::"TagSource"
    WHEN (("attributes" >> 12) & 15) = 2 THEN 'Rekognition'::"TagSource"
    WHEN (("attributes" >> 12) & 15) = 3 THEN 'WD14'::"TagSource"
    WHEN (("attributes" >> 12) & 15) = 4 THEN 'Computed'::"TagSource"
    WHEN (("attributes" >> 12) & 15) = 5 THEN 'ImageHash'::"TagSource"
    WHEN (("attributes" >> 12) & 15) = 6 THEN 'MinorDetection'::"TagSource"
    ELSE 'User'::"TagSource"  -- Default case in case source_id is outside expected range
  END AS source,
  CASE WHEN ("attributes" >> 11) & 1 = 1 THEN TRUE ELSE FALSE END AS automated,
  CASE WHEN ("attributes" >> 10) & 1 = 1 THEN TRUE ELSE FALSE END AS disabled,
  CASE WHEN ("attributes" >> 9) & 1 = 1 THEN TRUE ELSE FALSE END AS "needsReview",
  CASE WHEN ("attributes" >> 8) & 1 = 1 THEN TRUE ELSE FALSE END AS reserved_1,
  CASE WHEN ("attributes" >> 7) & 1 = 1 THEN TRUE ELSE FALSE END AS reserved_2,
  ("attributes" & 127) AS confidence
FROM "TagsOnImageNew";
```
