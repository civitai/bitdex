# PG-Sync V2 — Ops-Based Outbox Redesign

> Design doc for replacing the enrichment-heavy outbox poller with an ops-based approach. Collaborative doc — Justin and Adam.

## Status Inventory

Before diving into the redesign, here's where Justin's original ten items stand. Most of the foundation is already built:

| # | Item | Status | Notes |
|---|------|--------|-------|
| 1 | Generation-based storage | **Done** | ShardStore: snapshot + ops log + generation pinning + compaction |
| 2 | Doc store as ops log | **Done** | DocStore V2: append-only tuple logs, LIFO scan, field dictionary encoding |
| 3 | Bitmap storage with op log append | **Done** | ShardStore bitmap stores: 28-byte header, CRC32 ops log, per-shard compaction |
| 4 | In-memory caching layer | **Done** | DocCache (generational eviction), lazy bitmap loading, idle eviction |
| 5 | Read/write worker architecture | **Done** | Flush thread (write), ArcSwap readers (lock-free), merge thread, lazy load thread |
| 6 | Cache invalidation via hash maps | **Done** | MetaIndex (bitmaps indexing bitmaps) + shard_to_keys reverse index |
| 7 | PG-Sync redesign | **This doc** | Current system can't keep up; needs ops-based approach |
| 8 | Multi-instance leader election | **Skip** | Cursor-per-instance is sufficient for now |
| 9 | Snapshot/benchmark system | **Done** | Capture/replay, generation pinning, trace + metrics in snapshots |
| 10 | WAL pattern | **Done** | ShardStore IS the WAL — append ops, compact periodically |

---

## Problem Statement

The current outbox poller is **80M rows behind** and will never catch up. Here's why:

### Current Flow (V1)

```
PG trigger fires → INSERT into BitdexOutbox (entity_id, event_type)
                         ↓
Poller reads 5,000 rows from BitdexOutbox WHERE id > cursor
                         ↓
Deduplicate by entity_id (highest outbox ID wins)
                         ↓
For UPSERTs: fetch full Image row from PG (JOIN Post)
             fetch tags, tools, techniques, resources (4 enrichment queries)
             assemble full JSON document
             PATCH to BitDex server
                         ↓
For DELETEs: send DELETE to BitDex server
                         ↓
Advance cursor in PG + BitDex
```

### Why It's Slow

1. **Enrichment JOINs dominate**: Each batch of 5,000 outbox rows requires 5 PG queries (images + 4 enrichment tables). At ~1-2s per cycle, that's ~2,500 effective changes/second — optimistic, since deduplication often reduces 5,000 outbox rows to far fewer actual changes.

2. **Amplification**: A single Model update fires the `bitdex_model_trg` which JOINs `ImageResourceNew` → `ModelVersion` and inserts one outbox row per affected image. A popular model touching 10K images = 10K outbox rows, each requiring the same enrichment queries to process.

3. **Full-document assembly**: Even if only a tag was added, V1 re-fetches the entire image + all enrichment data. The `assemble_document()` function builds a complete JSON blob every time.

4. **Fixed batch size**: `outbox_batch_limit` is set in `sync.toml` at startup. No runtime tuning without restart.

---

## V2 Design: Ops-Based Outbox

### Core Idea

Instead of the outbox saying "image X changed, go figure out what changed," the triggers encode the **operation itself** — the delta. BitDex reads ops and applies them directly, no enrichment queries needed.

### New Table: `BitdexOps`

> @adam: Separate table from `BitdexOutbox` so we don't break existing V1 during migration. V1 poller keeps running until V2 is validated, then we drop the old triggers and table.

```sql
CREATE TABLE IF NOT EXISTS "BitdexOps" (
    id          BIGSERIAL PRIMARY KEY,
    entity_id   BIGINT NOT NULL,
    op_type     TEXT NOT NULL,       -- 'set', 'add', 'remove', 'delete'
    field       TEXT,                -- null for 'delete' (whole-document)
    value       JSONB,               -- the delta payload
    created_at  TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_bitdex_ops_id ON "BitdexOps" (id);
```

**Op types:**

| op_type | field | value | Meaning |
|---------|-------|-------|---------|
| `set` | `nsfwLevel` | `16` | Set scalar field to value |
| `set` | `sortAt` | `1711234567` | Set sort field |
| `add` | `tagIds` | `[42, 99]` | Add values to multi-value field |
| `remove` | `tagIds` | `[42]` | Remove values from multi-value field |
| `set` | `_full` | `{...full doc...}` | Full document upsert (for new images) |
| `delete` | null | null | Delete document |

> @adam: The `_full` op handles new image inserts where we need all fields. For existing images, individual `set`/`add`/`remove` ops avoid enrichment entirely. The triggers know which table fired, so they know exactly what changed.
>
> **Open question for Justin: ** Should we combine multiple field changes from a single DB transaction into one op row with a compound value? E.g., if an Image UPDATE touches both `nsfwLevel` and `type`, should that be one op `{field: null, value: {nsfwLevel: 16, type: "video"}}` or two separate op rows? Single row = fewer outbox writes + natural batching, but compound ops are slightly more complex to parse. My lean: **compound ops for same-row UPDATEs**, individual ops for join-table changes (tags, tools, etc.). The triggers already know which columns changed via `OLD` vs `NEW`.

@justin: Yeah, I think this makes sense. I think we'd want to make sure that it's still a patch when you do this, right? So it's set, but you're setting just those properties. It's not like you're replacing things. And then what we can do when we're processing this op is still break it down into the individual ops or whatever, so that the actual processing pipeline is, you know, in the form. So for your example here, yeah, in the database it would be, as we prep it for processing, it would have, it would break that down into a set NSFWLevel16 and a set type video. Does that make sense? Do you think that's worth doing?
@justin: It may also be worth looking into whether or not we should be passing old values. Otherwise, we're having to go look up old values in the docs store to update the appropriate bitmaps by removing them from those places. Which isn't the worst thing, especially with our docs stores caching or whatever. But we need to essentially write removes from the current locations. Whereas now, with the triggers, theoretically, we can even say what should be removed. And so, in a way, I almost wonder if the ops thing should be structured slightly differently. Rather than having an actual column for op-type and field, maybe the whole thing is always JSONB. and it's an array of ops to apply. And it includes the remove ops and the set ops. So you're removing from the old bitmap and you're adding to the new one. And I guess when we write to the doc store we flatten, but we include both so that we can handle writing to the bitmap shard store correctly. What do you think?

> @adam: Love this. Including old values is a huge win — it eliminates the docstore read on the write path entirely. Right now, `put()` does `read_doc_from_store() → diff old vs new → update changed bitmaps`. If the op already carries the old value, we can skip the read and go straight to bitmap mutations.
>
> **Revised table structure** — single JSONB column with an ops array:
>
> ```sql
> CREATE TABLE IF NOT EXISTS "BitdexOps" (
>     id          BIGSERIAL PRIMARY KEY,
>     entity_id   BIGINT NOT NULL,
>     ops         JSONB NOT NULL,      -- array of ops to apply
>     created_at  TIMESTAMPTZ DEFAULT now()
> );
> ```
>
> Each `ops` entry is an array like:
> ```json
> [
>   {"op": "remove", "field": "nsfwLevel", "value": 8},
>   {"op": "set", "field": "nsfwLevel", "value": 16},
>   {"op": "remove", "field": "type", "value": "image"},
>   {"op": "set", "field": "type", "value": "video"}
> ]
> ```
>
> The trigger has `OLD` and `NEW` — it knows both values. For bitmaps: remove the slot from the old value's bitmap, add it to the new value's bitmap. For the docstore: flatten to just the final `set` values and write.
>
> For multi-value fields (tags), the trigger already knows add vs remove:
> ```json
> [{"op": "add", "field": "tagIds", "value": 42}]    -- tag INSERT
> [{"op": "remove", "field": "tagIds", "value": 42}]  -- tag DELETE
> ```
>
> This also means compound ops from a single UPDATE are naturally a single row — the `ops` array just has multiple entries. One outbox row, multiple field changes. Exactly what we wanted.
>
> For `_full` (new image INSERT), it's still one op:
> ```json
> [{"op": "full", "doc": {...}}]
> ```
>
> **On the processing side:** the reader iterates the ops array. `remove` = clear bit from old bitmap. `set` = set bit in new bitmap. `add`/`remove` on multi-value = direct bitmap ops. `full` = existing `put()` path. The docstore write just takes the final set values and writes them (same as today's `assemble_document` but from the op data, not from a PG re-fetch).
>
> This is strictly better than the separate-columns design. Simpler table, more expressive ops, no docstore reads on the write path. Let's go with this.
@justin: Great!

### New Triggers

The triggers replace the current "entity_id + event" pattern with field-level deltas:

```sql
-- Image table: detect which columns actually changed
CREATE OR REPLACE FUNCTION bitdex_image_ops() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        -- New image: full document op (caller enriches once during insert)
        INSERT INTO "BitdexOps" (entity_id, op_type, field, value)
        VALUES (NEW.id, 'set', '_full', to_jsonb(NEW));
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO "BitdexOps" (entity_id, op_type, field, value)
        VALUES (OLD.id, 'delete', NULL, NULL);
        RETURN OLD;
    ELSE -- UPDATE
        -- Only emit ops for columns that actually changed
        IF OLD."nsfwLevel" IS DISTINCT FROM NEW."nsfwLevel" THEN
            INSERT INTO "BitdexOps" (entity_id, op_type, field, value)
            VALUES (NEW.id, 'set', 'nsfwLevel', to_jsonb(NEW."nsfwLevel"));
        END IF;
        IF OLD.type IS DISTINCT FROM NEW.type THEN
            INSERT INTO "BitdexOps" (entity_id, op_type, field, value)
            VALUES (NEW.id, 'set', 'type', to_jsonb(NEW.type));
        END IF;
        -- ... similar for each indexed column
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Tag join table: add/remove ops
CREATE OR REPLACE FUNCTION bitdex_tag_ops() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO "BitdexOps" (entity_id, op_type, field, value)
        VALUES (NEW."imageId", 'add', 'tagIds', to_jsonb(ARRAY[NEW."tagId"]));
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO "BitdexOps" (entity_id, op_type, field, value)
        VALUES (OLD."imageId", 'remove', 'tagIds', to_jsonb(ARRAY[OLD."tagId"]));
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;
```

> @adam: The `IS DISTINCT FROM` check is key — it means we only emit ops when a value actually changes, not on every UPDATE. This dramatically reduces outbox volume compared to V1, which inserts an outbox row for every UPDATE regardless of whether anything BitDex cares about changed.
>
> The tricky ones are the **indirect triggers** — `Post` updates, `ModelVersion` updates, `Model` updates. In V1, these do fan-out JOINs inside the trigger to find affected image IDs. In V2, we have two choices:
>
> 1. **Keep the fan-out in the trigger** — same JOINs, but emit targeted ops instead of generic "image changed" rows. Still O(affected_images) outbox rows, but no enrichment needed on the reader side.
>
> 2. **Move fan-out to the reader** — trigger just says "Model X changed", reader does the JOIN to find affected images. Simpler triggers, but the reader needs PG access for these specific cases.
>
> **My recommendation: option 1.** The fan-out is the same cost either way — it's just a question of where it runs. Doing it in the trigger keeps the reader pure (no PG queries at all during steady-state), which is the whole point of V2.
>
> **However**, there's a subtlety with Model/ModelVersion: when a Model's `poi` field changes, every image using that model needs `poi` recalculated. The trigger can't easily compute `image.poi OR resource_poi` because it doesn't have the image's `poi` field. Options:
> - Emit a `set` op with `poi: true/false` based on the model change, and let BitDex figure out the OR logic
> - Emit a special `recalculate` op type that tells BitDex to re-derive the field
> - **Simplest: just emit `set _full`** for these rare model-level changes and accept the one-time enrichment cost
>
> Justin — what's your preference here?
@justin: Agreed. We should go with option one. I think that the actual cost of the fan-out is actually probably pretty limited, and if we're doing it inside of Postgres, it should be fine. Obviously, there's a little bit of write cost there, but I think that it will be okay. for the POI field. You should ask Donovan in the model share repo if we're already setting the image POI based on the model's POI being set. And I guess when that happens, when it gets changed, it should change the POI field on all of the images already, so that the image data is always correct. So then we don't have to monitor for the model's POI field changing. I guess the other place where this will happen is maybe the model version base model changing and now the base model on all of the images needs to change That could be a pretty massive fan like you saying It has me thinking that maybe we need to be actually storing those differently I don know that kind of sucks Because you right there are model versions that have tens of thousands of images. That would be a lot of ops all of a sudden if somebody did that. And I guess the alternative is something that wouldn't be bitmaps, so it would cause issues in the entire pipeline. So I guess we have to do this fan out. But I guess at least we'd be paying that cost once rather than doing it the way that we're doing right now. Either way, it's probably a big lift in performance there. I guess we just need to worry about the write impact of that. And there's no way to do triggers asynchronously. I guess we could do the trigger after, right, if we aren't already. Triggers should be after the write so that it's not blocking or whatever.

> @adam: Solid. Three action items from this:
>
> 1. **POI** — I'll ask Donovan if model-share already propagates `model.poi` changes down to the Image table. If it does, our Image trigger catches it and we skip the Model→Image fan-out for POI entirely.
>
> 2. **baseModel fan-out** — Unavoidable, but the cost model is much better than V1. A ModelVersion baseModel change with 50K affected images produces 50K ops rows (each ~100 bytes), but each op is just `[{"op":"remove","field":"baseModel","value":"SD 1.5"},{"op":"set","field":"baseModel","value":"SDXL"}]`. Two bitmap mutations per image, no enrichment. At 50K ops/s processing, the whole cascade resolves in ~1s. In V1, those same 50K images would each need 5 enrichment queries = 250K queries. Night and day.
>
> 3. **AFTER triggers** — Confirmed. All existing triggers are `AFTER INSERT OR UPDATE OR DELETE` (see queries.rs lines 51-117). The `ENABLE ALWAYS` flag is about CDC replication, not timing. AFTER means the trigger fires after the main row write is committed, so the INSERT into BitdexOps doesn't block the original transaction beyond the time it takes to do the insert itself. The fan-out JOINs in Model/MV triggers are the main concern — those run inside the trigger transaction. But they're the same JOINs V1 does today, just with a lighter payload.
@justin: I am worried about the cost of that fan-out. If there are 200,000 images attached to a model version or something like that, obviously this could be a huge problem. So I wonder if for some things we have an ops type where where the PG-sync utility or the utility that we're creating here does its own kind of fan out from there so that it can pull in batches, because I have a feeling that trying to get all of those at once could be really problematic. And if it could instead somehow batch those queries, maybe that's better. You can ask Donovan to use the PG, the Postgres skill he has to get some metrics for you. You'll need to make sure that he's aware that he can set timeouts on those, because these could be really slow queries, and so he's going to have to be cautious about how he does that. Probably look at indexes and things like that first to make sure that he queries with indexes, and if there's not indexes to let you know that he can set timeouts on those. that you can't get good numbers right now without there being problems. So if you could follow up with him on that, that'd be great.

> @adam: Good instinct on the fan-out risk. Two approaches:
>
> 1. **Move large fan-outs out of the trigger entirely.** The trigger emits a lightweight "cascade" op: `[{"op": "cascade", "source_table": "ModelVersion", "source_id": 456, "field": "baseModel", "old": "SD 1.5", "new": "SDXL"}]`. pg-sync sees the cascade op, does the fan-out JOIN itself in batches (`SELECT imageId FROM ImageResourceNew WHERE modelVersionId = 456 LIMIT 10000 OFFSET ...`), and emits individual `set baseModel` ops per image. This keeps the trigger fast and moves the heavy lifting to pg-sync where we control batch sizes and can pace the work.
>
> 2. **Keep the trigger simple**, just emit "ModelVersion 456 changed baseModel", and pg-sync handles the rest. Same effect, less trigger complexity.
>
> I'll message Donovan to get metrics on the fan-out query sizes — how many images per ModelVersion, distribution of the top ModelVersions, and whether `ImageResourceNew.modelVersionId` is indexed. Will ask him to use `SET statement_timeout` to be safe.
>
> **Update on POI:** Donovan confirmed Image.poi and Model.poi are independent — no propagation. So we DO need the Model→Image fan-out for POI. Same cascade pattern applies — keep the trigger lightweight, let pg-sync batch the fan-out.

### Processing: BitDex Side

The ops map directly to BitDex mutations — no assembly needed:

| Op | BitDex Action |
|----|---------------|
| `set` on a filter field | `put()` with that field value (bitmap update) |
| `set` on a sort field | `put()` with new sort value (sort layer update) |
| `add` on a multi-value | `filter_sync` or direct bitmap set for each value |
| `remove` on a multi-value | Direct bitmap clear for each value |
| `set _full` | Full `put()` with all fields (same as today's PATCH) |
| `delete` | `delete()` |

> @adam: This is where it gets interesting. Most ops translate to **partial updates** — we don't need to rebuild the whole document. For a tag add, we literally just need to set one bit in one bitmap. That's microseconds, not milliseconds.
>
> The `_full` op is the escape hatch for cases where we don't have fine-grained ops (new images, model-level cascades). It falls back to the V1 path — assemble full doc, PATCH to BitDex. But these should be rare compared to the steady-state tag/metric/field updates.

### New Endpoint: `POST /api/indexes/{name}/ops`

> @adam: Rather than mapping each op to an existing endpoint (PATCH, filter-sync, etc.), I think a dedicated ops endpoint makes more sense. It accepts a batch of ops in the same format as the outbox, and the server applies them internally. This avoids HTTP overhead for individual operations and lets the server batch internally.
>
> **Alternative Justin mentioned:** WAL watcher pattern — pg-sync writes to an append-only file, BitDex server watches it. This avoids HTTP entirely. But it couples pg-sync and BitDex to the same filesystem, which breaks the sidecar model in K8s. I'd stick with HTTP for now and optimize later if it becomes a bottleneck.
@justin: Maybe we can make this the best of both worlds. Maybe posting to the ops endpoint just writes to the wall file, and we only perform ops off of the wall file. So essentially it's just a way to be dumping stuff into the wall, and that way we can be reading as efficiently as possible from the wall file. and I guess we'll want to be compacting the wall, but that makes it so that we won't have a loose state. So as soon as we accept the request from the pg-sync utility, it means that eventually that operation is going to be processed. And I guess as the wall reader inside of the server processes lines, it needs to do something. or something with those lines. We'll want the wall to be append-only, obviously, but we'll need to be pruning that wall, probably starting doing generations. Periodically, we just start a new wall, and it essentially has to work through all of the generations or something, and it has a cursor or whatever. I don't know. We're going to have to figure that part out. It's probably a design discussion portion that we'll need to have here.

> @adam: I like this a lot — HTTP as ingestion, WAL as the durable processing queue. Here's how I'd structure it:
>
> **Ingestion:** `POST /ops` receives ops from pg-sync, immediately appends them to the WAL file on disk, returns 200. The endpoint does zero processing — just fsync and acknowledge. This makes the HTTP path trivially fast and guarantees durability.
>
> **Processing:** A dedicated WAL reader thread (similar to our existing flush thread) tails the WAL file, reads batches, deduplicates, and submits mutations to the coalescer. It maintains a cursor (byte offset or line number) of the last processed position. On restart, it resumes from the cursor.
>
> **WAL lifecycle:**
> - Append-only files, one per generation: `ops_000001.wal`, `ops_000002.wal`, ...
> - The reader processes the current file sequentially. When it reaches the end, it sleeps briefly and retries (or uses inotify/kqueue for instant wake-up).
> - When a file hits a size threshold (e.g., 100MB), rotate: close current, open next generation. The reader finishes the old file, then moves to the new one.
> - Old generations are deleted once fully processed (cursor past EOF).
> - On crash recovery: reader resumes from persisted cursor position in the current generation file.
>
> **Format:** Each WAL entry is a length-prefixed binary record: `[4-byte len][entity_id: i64][ops: JSONB bytes][CRC32]`. This matches our ShardStore pattern — same CRC32 integrity, same append-only model. We could even reuse `BucketDiffLog`'s append/read primitives.
>
> This gives us exactly what you described: accept fast, process reliably, never lose ops. The WAL is the source of truth between ingestion and processing.
@justin: Great!

```
POST /api/indexes/civitai/ops
{
  "ops": [
    {"entity_id": 123, "op_type": "add", "field": "tagIds", "value": [42]},
    {"entity_id": 456, "op_type": "set", "field": "nsfwLevel", "value": 16},
    {"entity_id": 789, "op_type": "delete"}
  ],
  "cursor": {"name": "pg-sync-default", "value": "8500000"}
}
```
@justin: Interestingly enough, this removes the need to include the cursor at all in these requests, right? Because PG-Sync doesn't need to say what the cursor value is. PG-Sync can keep track of that for itself. I mean, it doesn't even need to, right? All it needs to know is its pod name or whatever, whatever the name of its cursor should be, and then it can just be getting that cursor from Postgres as it where the client operates, right? So I think that simplifies things.

> @adam: Agreed — the cursor stays entirely in PG. pg-sync reads `BitdexOps WHERE id > cursor`, processes the batch, advances its cursor in the `bitdex_cursors` table. BitDex server never needs to know about the cursor. The ops endpoint is stateless — just "here are ops, append them to the WAL." Clean separation.
>
> This also means we can drop the cursor from all existing endpoints too (upsert, patch, delete). Those were carrying cursor info as a side-channel because pg-sync couldn't persist cursor state itself. Now pg-sync owns its own cursor in PG, and the ops endpoint is fire-and-forget.
@justin: Great.

The server handles each op type:

- **`set` with scalar field**: Update the field on the document, compute bitmap diff, submit to coalescer
- **`add`/`remove` on multi-value**: Direct bitmap set/clear without touching the document at all (filter-sync path)
- **`set _full`**: Full upsert path (existing `put()`)
- **`delete`**: Existing `delete()` path

### Op Deduplication and Compression

Justin mentioned that the ShardStore already handles some of this. Here's how we apply it:

1. **LIFO deduplication** (same as V1): When reading a batch from `BitdexOps`, if the same `entity_id + field` appears multiple times, the latest op wins. A `set nsfwLevel=16` followed by `set nsfwLevel=32` collapses to just the latter.

2. **Add/remove cancellation**: If entity 123 gets `add tagIds [42]` then `remove tagIds [42]` in the same batch, they cancel out. Net-zero ops are dropped.

3. **Full-document absorption**: If entity 123 has `add tagIds [42]` followed by `set _full {...}`, the `_full` supersedes all prior ops for that entity. The full doc already includes the current tag state.

> @adam: This dedup logic can run in the poller before sending to BitDex, or in the ops endpoint on the server side. I'd do it in the poller — it reduces network traffic and the poller already has the batch in memory. The ShardStore's op compression (set-then-unset cancellation) is a different layer — that happens on the bitmap side after the mutation is applied.

@justin: So, I think with full doc absorption, the answer is not quite. I mean, because typically when I think about the full doc rights, they're not even going to be touching those fields. Like, the tag ID ones are going to be coming specifically from the tags on image new table or whatever, right? So, it would be bad if updates to the image table overwrote all of the stuff with tags. So instead what I'm imagining is that the Polar, the thing that the PG-Sync utility basically decomposes those full into individual ops still. So ops are just getting pumped into, so the PG-Sync thing will essentially dedupe LIFO style after decomposing the fulls. And then it will that will send in its deduped stuff to the ops endpoint, and then that all writes to the log, and then workers as they're working through the log, as they pull batches from the log, they dedupe as well. Because there's a chance that between polls or whatever, the same things were done. And so it allows us to ensure kind of double consistency or double deduplication in a sense. as for the reason why I mentioned the shard store's ops compression or whatever, I mean, it's me looking at the fact that for the operations and us deduping them, it's probably going to be essentially the same logic over and over again for handling that dedupe, so it probably makes sense to have some sort of helper that does that, since we'll be using it in several different places.

> @adam: You're right — `full` ops should NOT absorb tag/tool/technique ops because they come from different tables. A `full` from the Image INSERT trigger only has Image-table columns, not tags. The tag ops come separately from `TagsOnImageNew` triggers. So the dedup is per-field, not per-entity.
>
> The two-layer dedup is smart:
> 1. **pg-sync side**: Decompose `full` into individual field ops, LIFO dedup per `(entity_id, field)`, send deduped batch to ops endpoint
> 2. **WAL reader side**: Same dedup on the WAL batch (catches cross-poll duplicates)
>
> For the shared dedup helper — agreed. Something like `dedup_ops(ops: &mut Vec<Op>)` that does LIFO dedup by `(entity_id, field)` and add/remove cancellation. Used in pg-sync before sending, and in the WAL reader before applying. Same logic, one function, two call sites. We can put it in a shared `op_dedup` module.
@justin: I wonder if rather than doing the full at all, now that we're having Ops be an array of operations, if we have it just still be an array of operations. I mean, I guess we could treat full as essentially saying, hey, here's the state of these fields, there is no prior state. That probably works fine. It just means that we'd only want to do that for inserts. wouldn't want to do it for updates. And so I kind of feel like because it's only for inserts, maybe it doesn't make sense. And to be consistent, we just want to have one way of doing it. It's always just ops and for inserts it's just all additive...

> @adam: Yeah, I think you're right — drop `full` entirely. For an INSERT, the trigger has `NEW` with all column values. It emits them as individual `set` ops (no `remove` since there's no prior state):
> ```json
> [
>   {"op": "set", "field": "nsfwLevel", "value": 1},
>   {"op": "set", "field": "type", "value": "image"},
>   {"op": "set", "field": "userId", "value": 12345},
>   {"op": "set", "field": "sortAt", "value": 1711234567}
> ]
> ```
> One format for everything. The reader doesn't need to know whether it's an INSERT or UPDATE — it just processes ops. For INSERTs, there are no `remove` ops because there's nothing to remove. For UPDATEs, there are paired `remove`/`set` ops. For DELETEs, a single `{"op": "delete"}`.
>
> This simplifies the dedup helper too — no special `full` absorption logic. Just LIFO per `(entity_id, field)` and add/remove cancellation. One code path.

---

## Immediate Relief: Runtime Batch Size

Sky's ask: configurable batch size without restarts.

### Endpoint: `POST /api/internal/pgsync-config`

```
POST /api/internal/pgsync-config
{
  "outbox_batch_limit": 50000,
  "poll_interval_secs": 1
}

GET /api/internal/pgsync-config
→ { "outbox_batch_limit": 50000, "poll_interval_secs": 1 }
```

> @adam: This is straightforward — pg-sync already reports metrics to the BitDex server via `POST /api/internal/pgsync-metrics`. We add a config endpoint that pg-sync checks at the start of each poll cycle. The server stores the override in memory (no persistence needed — on restart it falls back to sync.toml values).
>
> For the V1 poller (while we're migrating), the batch_limit can safely go higher. The bottleneck is the enrichment queries, not the outbox SELECT. At 50K batch_limit, we'd process more per cycle but each cycle takes proportionally longer. Worth measuring where the sweet spot is.
>
> **Alternative:** AtomicI64 in pg-sync itself, updated via a signal or small HTTP endpoint on pg-sync's progress port (9091). Avoids the round-trip to BitDex server. But the BitDex server route is simpler since the endpoint infrastructure already exists.
@justin: Yeah, I like your plan to have it essentially pulling the config from Bitdex. That makes sense. It should follow essentially the server's lead on the configuration. It's a little bit weird to have logic associated with a service that's kind of unique to Civitai set up there unless we can find a way to make this more like agnostic to a polling system. a pull system. I mean, PG-Sync really should be configured to be something that, again, is config-driven, and it's, you know, you just provide a config of the triggers that you want to have and stuff like that. I'm not sure if you cover that later, but it's going to be important. And then it doesn't seem so weird for us to have that config logic in Bitdex and for it to pull that essentially on boot in the same way that it checks for health from Bitdex, like it's already pulling, like you said, it can to get those settings there and use those as its polling interval and all of that. And then, like you said, on every poll it can check for updates to that. But I do think that it probably should be... Well, I don't think that it needs to be persisted if that's the case, like you said. Because if it checks that before it ever even starts the poll loop, then it'll always be accurate.

> @adam: Right — the config-driven direction ties into your later comment about the YAML trigger schema. pg-sync becomes a generic utility: give it a config (trigger definitions + poll settings), it sets up PG triggers and starts pumping ops. BitDex is the authority on what fields it cares about, pg-sync is just the plumbing. No Civitai-specific logic in either place.
@justin: Right. I'm trying to make sure that BitDex is something that can be used in more places. What we've built is really powerful and so I want it to be reusable!

### Implementation

In `outbox_poller.rs`:

```rust
// At the start of each poll cycle, check for config override
let effective_batch_limit = match client.get_pgsync_config().await {
    Ok(Some(config)) => config.outbox_batch_limit.unwrap_or(batch_limit),
    _ => batch_limit,
};
```

---

## Outbox Lag Metrics

### Endpoint: `GET /api/internal/pgsync-lag`

```json
{
  "replica_id": "pg-sync-default",
  "cursor_position": 420000000,
  "max_outbox_id": 500000000,
  "lag_rows": 80000000,
  "lag_pct": 16.0,
  "last_cycle_at": "2026-03-25T12:00:00Z",
  "last_cycle_rows": 4850,
  "last_cycle_secs": 1.23
}
```

> @adam: pg-sync already reports `cursor_position` and `rows_fetched` via `report_pgsync_metrics()`. We just need the BitDex server to:
> 1. Store the latest metrics per replica
> 2. On the lag endpoint, also query `max_outbox_id` from PG (or have pg-sync report it)
>
> Actually, we should have pg-sync report `max_outbox_id` too — that way the BitDex server doesn't need its own PG connection. Add it to the existing metrics payload:
>
> ```rust
> let max_id = queries::get_max_outbox_id(&pool).await.unwrap_or(0);
> client.report_pgsync_metrics(rid, cycle_secs, processed as u64, cursor, max_id).await;
> ```
@justin: Maybe this data can be passed in by PG-sync as part of the ops endpoint so that every time it's sending in data, it's saying, hey, here's all the operations. This is at cursor position of this of this, and we're behind by this percent. Basically, all the data that you've got here, it can be reporting in with each ops set. and then Bitdex will basically, the endpoint to check its lag or whatever on a sync source. Basically just reports the data that's coming in from that sync source.

> @adam: Makes sense — bundle the metrics with the ops payload. The ops endpoint already receives data from pg-sync, so adding a `meta` field with cursor/lag info is free:
> ```json
> {
>   "ops": [...],
>   "meta": {
>     "source": "pg-sync-default",
>     "cursor": 420000000,
>     "max_id": 500000000,
>     "lag_rows": 80000000
>   }
> }
> ```
> BitDex stores the latest `meta` per source and exposes it via the lag endpoint and Prometheus. No separate metrics reporting call needed.
@justin: Great.

### Prometheus Metrics

```
bitdex_pgsync_cursor_position{replica="default"} 420000000
bitdex_pgsync_max_outbox_id{replica="default"} 500000000
bitdex_pgsync_lag_rows{replica="default"} 80000000
bitdex_pgsync_cycle_duration_seconds{replica="default"} 1.23
bitdex_pgsync_cycle_rows{replica="default"} 4850
```
@justin: So I think rather than calling this pg-sync, it would probably just be sync, and then we'd have labels for the different sync sources. So there's something forming here about the concept of a sync source, and then pg-sync is just a sync source type, and then you have configuration for the pg-sync server, or utility, or whatever you want to call that, that drives how it does its syncing and the triggers that it sets up and all of that stuff.

> @adam: Good abstraction. Rename to `bitdex_sync_*` with a `source` label:
> ```
> bitdex_sync_cursor_position{source="pg-sync-default"} 420000000
> bitdex_sync_lag_rows{source="pg-sync-default"} 80000000
> bitdex_sync_cycle_duration_seconds{source="clickhouse"} 0.45
> ```
> This way ClickHouse metrics, PG-sync, and any future sync sources all report through the same Prometheus namespace. The `source` label is whatever the sync utility identifies itself as in the `meta` field of the ops payload.
@justin: Yes! Perfect to see this design will already be useful.

---

## Migration Plan

### Phase 1: Immediate (V1 improvements)

1. **Runtime batch size endpoint** — add to BitDex server + pg-sync config check
2. **Outbox lag metrics** — extend `report_pgsync_metrics` + add Prometheus gauges
3. **Increase default batch_limit** — from 5,000 to 20,000+ (measure enrichment query scaling first)

@justin: I want to flatten this migration plan. We don't need to try and fix the current pg-sync. It's just going to stay broken. We're going to be able to tackle all of this relatively quickly, so I think it makes sense to skip pretty much all of this. The thought is that we'll just do a full reload and then start using this new system. So it allows us to basically just do a full cutover all at once. The stuff's not really being used in production right now anyway, so there's no worry about us interrupting anything.

> @adam: Agreed — skip the V1 improvement phase entirely. Flat plan:
> 1. Build V2 triggers + BitdexOps table + ops poller + WAL endpoint
> 2. Full reload from CSVs (we already have the single-pass loader for this)
> 3. Deploy V2 triggers, start ops poller
> 4. Done. V1 code stays in the repo but unused until we clean it up later.
>
> Much simpler. No shadow mode, no parallel pollers, no incremental migration. Just build it, load fresh, cut over.
@justin: Yes, agreed.

### Phase 2: V2 Triggers + Table

1. Create `BitdexOps` table alongside existing `BitdexOutbox`
2. Deploy new triggers that write to `BitdexOps` (V1 triggers remain)
3. Validate: compare ops volume vs outbox volume, verify correctness

### Phase 3: V2 Poller

1. New `ops_poller.rs` that reads from `BitdexOps` instead of `BitdexOutbox`
2. Deduplication + compression logic
3. `POST /ops` endpoint on BitDex server
4. Run V1 and V2 pollers in parallel, compare results (shadow mode)

### Phase 4: Cutover

1. Disable V1 triggers
2. Drop `BitdexOutbox` table
3. Remove V1 poller code
4. V2 is now the only sync path

> @adam: Each phase is independently deployable and reversible. We never cut over without validation. The shadow mode in Phase 3 is critical — we compare V2 ops against V1 full-doc patches to verify they produce identical bitmap state.
@justin: No need to try and verify or prove that it's better than v1. v1 is completely broken because it just takes way too long to prepare ops, because those additional enriching queries are just too slow.

> @adam: Yep — no need for shadow mode comparison. V1 is fundamentally broken at this scale. We build V2, validate it works (E2E tests against a test dataset), and ship.
@justin: Yes!

---

## Config-Driven Trigger Management

Justin asked for an API to enable/disable triggers and adjust what goes into the outbox.

> @adam: I want to be honest about the tradeoff here. Fully config-driven triggers means:
>
> 1. The trigger functions need to be generic (read config from a table, decide what to emit)
> 2. Or we generate trigger SQL from a config and `CREATE OR REPLACE` on the fly
>
> Option 1 adds a table lookup to every trigger fire — that's per-row overhead on every INSERT/UPDATE/DELETE across all watched tables. At Civitai's write volume, that's not free.
>
> Option 2 is safer — generate the SQL once, execute it. An API endpoint like `POST /api/internal/pgsync-triggers/reload` regenerates trigger functions from config and runs `CREATE OR REPLACE`. No per-row overhead, but requires PG superuser access from the API.
>
> **My recommendation: start without this.** The triggers are stable — they match the schema, and the schema doesn't change often. If we need to add a field, we update the trigger SQL in code and deploy. Config-driven triggers add complexity for a problem we don't actually have yet.
>
> That said, a few things ARE worth making runtime-configurable:
> - **Batch size** (covered above)
> - **Poll interval** (same endpoint)
> - **Enable/disable the V2 poller** (feature flag, not trigger management)
> - **Field allowlist**: Which fields the poller forwards to BitDex (filter in the poller, not the trigger)
>
> Justin — does this cover your intent, or do you specifically want trigger DDL management via API?

@justin: Yeah, I agree. Probably doesn't need to be an endpoint, but being able to have this config-driven means that it can start to... I really wanted Bitdex to not be something dedicated to Civitai, and so trying to have it be a more generic thing where now you configure how it works... I don't know. It's tough. Can you think of any simpler way for us to be able to essentially define these things? I mean, I guess it could even straight up be SQL for now, for defining the triggers that create ops or whatever. Unless there's some sort of schema we can come up with that can then be read by code to handle creating ops off of defined tables and things like that. Like:
```
TagsOnImageNew:
  sourceSlotField: imageId // pg field that maps to slot
  onInsert:
    - op: add
      field: tagIds
      sourceValueField: tagId
  onDelete:
    - op: remove
      field: tagIds
      sourceValueField: tagId

Image:
  sourceSlotField: imageId
  onUpsert: // Syntactic sugar for update and insert
    - op: set
      field: nsfwLevel
      sourceValueField: nsfwLevel
  onDelete:
    - op: deleteSlot
```
@justin: This seems like it could probably be even more concise. There's a lot of kind of repetition here. But I mean, ultimately, this kind of does boil down to exactly what would be created by each of the triggers. And also makes it really clear what ops and stuff we can expect here. I guess it'd probably be good to look at the current pg-sync stuff and determine if there are things that are going to be more complex than what we can demonstrate here with config. But I like what we have here for kind of starting to be a way where the pg-sync utility is kind of just, like I said, a utility and you provide it essentially direction. and it sets itself up and runs. And all it does is set up the triggers that create ops, and then it processes and passes those ops onto Bitdex. And then Bitdex can essentially make sense of what ops to process based on its configuration. So if it sees an op coming in that doesn't align with its configuration, then it should just skip that op. So probably a new point there for you.

> @adam: I like the YAML trigger config direction. Let me tighten it up — here's a more concise version that covers all our current triggers:
>
> ```yaml
> sync_sources:
>   - table: Image
>     slot_field: id
>     track_fields: [nsfwLevel, type, userId, postId, minor, poi, hideMeta, meta, blockedFor]
>     on_delete: delete_slot
>
>   - table: TagsOnImageNew
>     slot_field: imageId
>     field: tagIds
>     value_field: tagId
>     # INSERT = add, DELETE = remove (inferred from table type)
>
>   - table: ImageTool
>     slot_field: imageId
>     field: toolIds
>     value_field: toolId
>
>   - table: ImageTechnique
>     slot_field: imageId
>     field: techniqueIds
>     value_field: techniqueId
>
>   - table: CollectionItem
>     slot_field: imageId
>     field: collectionIds
>     value_field: collectionId
>     filter: "status = 'ACCEPTED' AND imageId IS NOT NULL"
>
>   - table: Post
>     fan_out:
>       join: "Image WHERE postId = NEW.id"
>       slot_field: id
>     track_fields: [publishedAt, availability]
>
>   - table: ModelVersion
>     fan_out:
>       join: "ImageResourceNew WHERE modelVersionId = NEW.id"
>       slot_field: imageId
>     track_fields: [baseModel]
> ```
>
> pg-sync reads this config, generates the trigger SQL (one function per table), and deploys via `CREATE OR REPLACE`. The config also tells it what ops to expect, so it can validate incoming ops before forwarding.
>
> On the BitDex side: good call on skipping unknown ops. If BitDex's index config doesn't have a `toolIds` filter field, it ignores `add toolIds` ops. The ops are self-describing, so BitDex just checks its own config and drops what it doesn't recognize. Zero coupling between trigger config and index config — they just need to agree on field names.
>
> The complex cases (fan-out JOINs, conditional filters like CollectionItem) will need more thought in the trigger generator, but the schema handles them. We can start with the simple cases (Image, tags, tools) and add fan-out support later.
@justin: Okay, so for the fanouts, I think, like I mentioned above, we probably need to have something that handles those a little bit differently. Yeah, like a config where it's like you can give it a fanout function. I mean, we're already kind of starting to put together some places where there's SQL. You know, you have your join there. Huh. I guess all I'm suggesting is maybe that part's not in the trigger. Instead, it's something that gets handled when the op comes through, and it schedules essentially a background thread to process those so that it can keep moving through the cursors. But then I guess that's a problem because we need to wait for the fanout to be done before it can call that op done. But, like I said, maybe there's a way where we can essentially count and then batch those. I don't know. We'll need to check on that. because that could be dangerous if we have the database do it.
@justin: Another gotcha is probably removing things. So if you change your config and it drops a thing that you're tracking or whatever, the pg-sync needs to know, hey, does this trigger need to change? Or does this trigger need to be removed? We probably need to essentially have part of the name of the triggers be something so they can go and look for all the triggers that belong to the sync utility, and then have the hash of the function that's a function in there as part of the name as well so that it can compare that to its own function that it's about to create or something like that. So that it knows, hey, I need to clean this up and replace it or whatever. I'd also, you know, suggest maybe having the hash as part of the ops item, but I don't think that that's necessary because since Bitdex kind of handles things downstream anyways, filtering out bad props and things like that, then it kind of doesn't matter. What do you think?

> @adam: **Fan-out metrics from Donovan (2026-03-25):** The numbers are much larger than expected. `ImageResourceNew` has ~375M rows. The top ModelVersion (290640) has ~15.1M images. Top Model has ~15M. But the distribution is extremely heavy-tailed: p50=1 image, p90=5, p95=10, p99=53. So 99% of fan-outs are trivial. The top ~1% are catastrophic — a trigger trying to INSERT 15M rows into BitdexOps in one transaction would lock PG for minutes.
>
> This confirms we **cannot do fan-outs in triggers** for Model/ModelVersion. The cascade op approach is mandatory: trigger emits a lightweight `{"op": "cascade", "source_table": "ModelVersion", "source_id": 456, "field": "baseModel", "old": "SD 1.5", "new": "SDXL"}`, and pg-sync handles the fan-out with batched keyset-paginated queries against the indexed `ImageResourceNew_modelVersionId_idx`. For hot keys (>100K images), rate-limit the batch emission to avoid overwhelming the WAL.
>
> Good news: these massive fan-outs are extremely rare events (nobody changes baseModel on SDXL often). Steady-state ops are all p50=1 territory.

> @adam: Two solid points. Here's my thinking on both:
>
> **Fan-out outside the trigger:** Agreed — the trigger just emits a `cascade` op, pg-sync handles the fan-out in a background task with batched queries. For cursor advancement: the cascade op itself gets marked as "processed" immediately, but pg-sync tracks the fan-out expansion separately. It can maintain a small in-memory queue of pending fan-outs that run in parallel with the main cursor loop. The main cursor keeps advancing (processing non-cascade ops), while a separate tokio task handles the fan-out batches. If pg-sync restarts mid-fan-out, it re-processes the cascade op and re-does the fan-out (idempotent since each resulting op is just set/remove on a bitmap).
>
> **Trigger cleanup/hashing:** Great idea. Naming convention:
> - Trigger name: `bitdex_{table}_{hash8}` where `hash8` is the first 8 chars of SHA256 of the trigger function body
> - Function name: `bitdex_{table}_ops_{hash8}()`
>
> On startup, pg-sync:
> 1. Generates trigger SQL from config
> 2. Computes hash of each function body
> 3. Queries `pg_trigger WHERE tgname LIKE 'bitdex_%'` to find existing triggers
> 4. For each table: if hash matches → skip (already deployed). If hash differs → `CREATE OR REPLACE` + rename trigger. If trigger exists but table not in config → `DROP TRIGGER`.
>
> This gives us fully declarative trigger management — the config is the source of truth, pg-sync reconciles PG state to match. No manual DDL.
>
> And agreed on not needing the hash in ops — BitDex filters unknown fields downstream, so stale triggers that emit ops for removed fields are harmless. They just get ignored.

@justin: Regarding fan-outs... After seeing this: `{"op": "cascade", "source_table": "ModelVersion", "source_id": 456, "field": "baseModel", "old": "SD 1.5", "new": "SDXL"}` I realized something... We already know all of the images that have that model version as its source. We don't need to go query Postgres. We can use Bitdex. Essentially, we would just do a read query against Bitdex to get all the image IDs and could probably even do them in pages or whatever, or do some sort of back pressure system to handle that. And so we would just give this as an opt to Bitdex, and it would handle updating all of the corresponding images based model values. values to be this. It knows exactly it'll essentially get all the image IDs, all the slots that it needs to change, and then update the SD 1.5 bitmap to remove all of those slots, and then update the SD Excel bitmap to add all of those slots. Much simpler operation, doesn't even have to make a bunch of inserts into Postgres, doesn't have to do a bunch of queries or anything like that. What do you think? the op structure probably needs to be slightly different, but I think it's pointed to some sort of like query-based op or something.

> @adam: This is a massive simplification. BitDex already HAS the `modelVersionIds` filter bitmap — it literally has a roaring bitmap for every modelVersionId value, containing all the slot IDs of images that use it. So a `baseModel` cascade doesn't need to query PG at all:
>
> 1. Look up the `modelVersionIds` bitmap for MV 456 → get the set of all affected slots
> 2. For each slot: remove from bitmap `baseModel="SD 1.5"`, add to bitmap `baseModel="SDXL"`
> 3. Done. Two bitmap operations per slot, all in-memory, no PG round-trip.
>
> And since roaring bitmaps support bulk operations, we can do this even faster: `bitmap_old_basemodel.andnot_inplace(&mv_bitmap)` and `bitmap_new_basemodel.or_inplace(&mv_bitmap)`. That's **two bitmap ops total** regardless of how many images are affected. A 15M-image fan-out becomes two bitmap OR/ANDNOT operations that complete in microseconds.
>
> The op format would be something like:
> ```json
> {"op": "cascade", "via_field": "modelVersionIds", "via_value": 456, "target_field": "baseModel", "old": "SD 1.5", "new": "SDXL"}
> ```
>
> BitDex reads this as: "find all slots where `modelVersionIds` contains 456, then for those slots, remove `baseModel=SD 1.5` and set `baseModel=SDXL`."
>
> This completely eliminates the fan-out problem. No 15M individual ops, no PG queries, no batching needed. Just bitmap math. This is exactly what the bitmap engine is built for.
>
> For the docstore: we'd still need to update each affected doc's `baseModel` field, but that can happen lazily (on next read) or in a background sweep. The bitmap state is correct immediately.
>
> For POI cascades (Model.poi changes): same pattern. `{"op": "cascade", "via_field": "modelVersionIds", "via_value": [all MVs of the model], "target_field": "poi", "old": false, "new": true}`. BitDex ORs the MV bitmaps together to get all affected slots, then does the bitmap swap.

@justin: I think we can make it even more slick:
```
{"op": "queryOpSet", query: "modelVersionIds eq 456", ops: [
  {"op": "remove", "field": "baseModel", "value": "SD 1.5" },
  {"op": "add", "field": "baseModel", "value": "SDXL" },
]}
```
This get's the affected slots using the query, and then applies the ops to the appripriate bitmaps for all query results. And this op is simple enough that it's probably something that can still be constructed by the actual trigger as well. We would just need to think about how to set up the config in the schema.

> @adam: Yes — this is clean. `queryOpSet` uses our existing query parser to resolve the slot set, then applies the nested ops to all those slots. The query string is the same format we already support (`modelVersionIds eq 456`), so it goes straight through the existing `BitdexQuery` pipeline. No new query language, no special-case code for "cascade" ops.
>
> And you're right that the trigger can construct this directly:
> ```sql
> -- ModelVersion trigger: baseModel changed
> IF OLD."baseModel" IS DISTINCT FROM NEW."baseModel" THEN
>     INSERT INTO "BitdexOps" (entity_id, ops) VALUES (
>         0,  -- entity_id irrelevant for queryOpSet
>         jsonb_build_array(jsonb_build_object(
>             'op', 'queryOpSet',
>             'query', 'modelVersionIds eq ' || NEW.id,
>             'ops', jsonb_build_array(
>                 jsonb_build_object('op', 'remove', 'field', 'baseModel', 'value', OLD."baseModel"),
>                 jsonb_build_object('op', 'set', 'field', 'baseModel', 'value', NEW."baseModel")
>             )
>         ))
>     );
> END IF;
> ```
>
> One outbox row, one query, bulk bitmap ops. The trigger stays lightweight (single INSERT), and BitDex does what it's best at — bitmap math.
>
> For the YAML config, the fan-out tables would look like:
> ```yaml
> - table: ModelVersion
>   slot_field: id  # not used as slot, just as identifier
>   fan_out:
>     query_field: modelVersionIds  # BitDex field to query
>     query_op: eq                  # uses "field eq NEW.id"
>   track_fields: [baseModel]
> ```
>
> pg-sync's trigger generator sees `fan_out` → generates a `queryOpSet` op instead of a direct `set` op. No special handling needed in the ops processing pipeline — `queryOpSet` is just another op type that the WAL reader knows how to handle.
@justin: Okay, regarding the schema, it feels a little bit weird to have the query op be equal because I can't think of a query op that we would ever do differently. And I partially wonder if it makes more sense to have the thing just be query. We already have kind of like SQL-like stuff inside of filter for collection items. Maybe this could just be a place where you put the bitdex query and then you can where you can have values that get injected by the trigger with a replace. So something like `query: "modelVersionIds eq {id}"` That way you can be really expressive with your queries and it doesn't have to be so structured or whatever. And you can basically just pull in values from the Postgres table that you're reading from. I also don't really understand the purpose of slot field there. It feels kind of confusing because we're using it differently than anywhere else. Essentially what we're saying here in this one is that the slots are going to be determined by the query. Wanna take another stab at the schema here? Maybe outline how it'd work for the other fan out, post...

> @adam: Good call — template queries with `{column}` placeholders are much cleaner. And you're right, `slot_field` on fan-out tables is confusing since the slots come from the query result, not a PG column. Here's a revised schema with all the fan-out cases:
>
> ```yaml
> sync_sources:
>   # ── Direct tables (slot = PG row ID) ──
>   - table: Image
>     slot_field: id
>     track_fields: [nsfwLevel, type, userId, postId, minor, poi, hideMeta, meta, blockedFor]
>     on_delete: delete_slot
>
>   - table: TagsOnImageNew
>     slot_field: imageId
>     field: tagIds
>     value_field: tagId
>
>   - table: ImageTool
>     slot_field: imageId
>     field: toolIds
>     value_field: toolId
>
>   - table: ImageTechnique
>     slot_field: imageId
>     field: techniqueIds
>     value_field: techniqueId
>
>   - table: CollectionItem
>     slot_field: imageId
>     field: collectionIds
>     value_field: collectionId
>     filter: "status = 'ACCEPTED' AND \"imageId\" IS NOT NULL"
>
>   # ── Fan-out tables (slots determined by BitDex query) ──
>   - table: ModelVersion
>     query: "modelVersionIds eq {id}"
>     track_fields: [baseModel]
>
>   - table: Model
>     query: "modelVersionIds eq {modelVersionId}"
>     # Model doesn't have MV id directly — need a PG subquery
>     # to get MV ids first, then BitDex query per MV
>     query_source: "SELECT id as \"modelVersionId\" FROM \"ModelVersion\" WHERE \"modelId\" = {id}" # @justin: added cast here since the query references a value that wouldn't be in the base table.
>     track_fields: [poi]
>
>   - table: Post
>     query: "postId eq {id}"
>     track_fields: [publishedAt, availability]
> ```
>
> **How each fan-out works:**
>
> **ModelVersion** (baseModel changes): Trigger detects `OLD.baseModel IS DISTINCT FROM NEW.baseModel`, emits:
> ```json
> [{"op": "queryOpSet", "query": "modelVersionIds eq 456", "ops": [
>   {"op": "remove", "field": "baseModel", "value": "SD 1.5"},
>   {"op": "set", "field": "baseModel", "value": "SDXL"}
> ]}]
> ```
> BitDex queries its `modelVersionIds` bitmap for value 456, gets all affected slots, applies the bitmap swap. Done.
>
> **Post** (publishedAt changes): Same pattern. `postId` is a filter field in BitDex, so `"postId eq 789"` resolves the slot set directly:
> ```json
> [{"op": "queryOpSet", "query": "postId eq 789", "ops": [
>   {"op": "remove", "field": "publishedAt", "value": 1711000000},
>   {"op": "set", "field": "publishedAt", "value": 1711234567}
> ]}]
> ```
>
> **Model** (poi changes): This one's trickier — BitDex doesn't have a `modelId` field, only `modelVersionIds`. The trigger needs the MV ids for this model. Two options:
> 1. The trigger does `SELECT id FROM ModelVersion WHERE modelId = NEW.id` and emits one `queryOpSet` per MV id. This is a small PG query (most models have <10 MVs).
> 2. Or we add `modelId` as a filter field in BitDex. Then it's just `"modelId eq 123"`.
>
> I'd go with option 1 for now — the MV lookup is tiny (indexed, usually <10 rows), and it avoids adding a new filter field. The `query_source` field in the config tells the trigger generator to do the PG subquery first.
>
> **The key insight:** fan-out tables don't have `slot_field` at all. They have `query` (a BitDex query template with `{column}` placeholders). The trigger injects PG column values into the template, and BitDex resolves slots from its own bitmaps. Clean separation.
@justin: Yeah, I'd go with option one. Another thing that I'm thinking about as I see this is that there could potentially be multiple model version IDs that come back from that query, and so it would need to be able to support an in statement, which I believe we have inside of Bitdex. You might send an explorer just to confirm. But then the query would be something like: `query: "modelVersionIds in {modelVersionIds}` and the source query would have to return a JSON array or something like that.

> @adam: Yes, we support `in` — it's `InList` in our query parser. So the Model trigger can collect all MV ids from the subquery into an array, then emit one `queryOpSet` with `"modelVersionIds in [101, 102, 103]"`. BitDex ORs those MV bitmaps together to get the full slot set, then applies the ops. One op row regardless of how many MVs the model has.
>
> The `query_source` returning a JSON array works well. In the trigger:
> ```sql
> SELECT jsonb_agg(id) INTO mv_ids FROM "ModelVersion" WHERE "modelId" = NEW.id;
> -- Produces: [101, 102, 103]
> -- Template becomes: "modelVersionIds in [101, 102, 103]"
> ```
>
> And the updated config for Model becomes simply:
> ```yaml
> - table: Model
>   query: "modelVersionIds in {modelVersionIds}"
>   query_source: "SELECT jsonb_agg(id) as \"modelVersionIds\" FROM \"ModelVersion\" WHERE \"modelId\" = {id}"
>   track_fields: [poi]
> ```
>
> Good catch on the `as "modelVersionId"` cast in the source query too — the placeholder name in `query` needs to match a column alias from `query_source`. That's the contract: source query returns named columns, template substitutes them by name.
@justin: Right, and if there isn't a source query provided, then the assumption is that it's just the values off of new, right? EX `NEW.id` when running the query op set off post...

> @adam: Exactly. No `query_source` = placeholders resolve directly from `NEW` columns. So for Post:
> - Config: `query: "postId eq {id}"`, no `query_source`
> - Trigger substitutes `{id}` → `NEW.id` → `"postId eq 789"`
>
> `query_source` is only needed when the query references a value that isn't a column on the triggering table (like Model needing MV ids). Simple and predictable.

---

## Throughput Estimate

### V1 (current)

- 5,000 outbox rows/cycle, ~2s/cycle = **~2,500 rows/s**
- After dedup: maybe 2,000-3,000 effective changes/s
- 80M row gap at 2,500/s = **~9 hours** to catch up (if no new writes)

### V2 (projected)

- No enrichment queries: each op is self-contained
- Batch dedup + compression reduces ops further
- BitDex `put()` throughput: ~50,000 partial updates/s (measured for filter-sync)
- Bottleneck shifts to PG outbox SELECT + network: **~50,000-100,000 ops/s** with 50K batches
- 80M row gap at 50K/s = **~27 minutes** to catch up

> @adam: The 50K/s estimate assumes the ops endpoint can batch internally. If we're doing individual HTTP calls per op, it'll be much slower. The batch ops endpoint is essential.
>
> The real win is steady-state: V1 processes ~2,500 changes/s but Civitai generates changes faster than that during peak. V2 at 50K/s has massive headroom. We'd only fall behind during truly exceptional traffic spikes.
@justin: Yeah, with PG-Sync being able to essentially just pipe things into Bitdex, it's really just a matter of how quickly we can get data out of Postgres. And then, I guess, Bitdex itself could be behind on its wall or whatever, but knowing the speed with which we process documents from CSVs, I don't think that that's going to be an issue. We were processing 5 million tag ops per second. 5.5 million per second. So, I don't think that we'll ever be behind if we can get this built right... Additionally, once we get this in, we probably could even make it so that the pg-sync utility pulls even faster than two seconds. But, we'll see.

> @adam: Right — at 5.5M tag ops/s from CSV processing, the WAL reader will never be the bottleneck. The bottleneck is how fast we can read from PG's `BitdexOps` table. With indexed sequential reads (`WHERE id > cursor ORDER BY id LIMIT N`), PG should handle 100K+ rows/s easily. And with the poll interval potentially dropping below 2s, we'd have near-real-time sync. The 80M gap becomes a non-issue — it's a V1 problem that V2 doesn't inherit since we're doing a full reload + fresh start.
@justin: Yes!

---

## Open Questions for Justin

1. **Compound vs individual ops** for same-row UPDATEs — one row or many?
2. **Model/ModelVersion cascade** — fan-out in trigger (option 1) or reader (option 2)?
3. **Config-driven triggers** — necessary now or defer?
4. **WAL watcher vs HTTP** for ops delivery — HTTP for now, optimize later?
5. **`_full` op for new images** — the INSERT trigger can emit `to_jsonb(NEW)` but that's the raw Image row without enrichment (no tags, tools, etc.). Should the poller detect `_full` ops and do a one-time enrichment fetch for those? Or should the trigger somehow include enrichment? (I lean toward poller-side enrichment for `_full` only — it's the same V1 path but only for genuinely new images, not every update.)
6. **ClickHouse metrics** — keep separate `metrics_poller` or fold into ops? Metrics come from ClickHouse, not PG, so they can't be in the PG outbox. I'd keep the metrics poller separate.

@justin: for ClickHouse. I think we need a separate thing. We should just have a separate service, because this one is called pg-sync, so I think it makes sense for the ClickHouse one to be something different. And then I guess we would update our k8s config to have the pg-sync thing and to have a ClickHouse sync thing. And the ClickHouse sync thing would, I guess, have its own configuration. I don't know. Now you're making me question the configuration thing because, yeah, we could do that, but then we're creating extra work for ourselves. Maybe for now the ClickHouse one could be separate, and it's just a simple thing that does what we're already doing for ClickHouse. So the pg-sync one is something that we share, and the click-house one is something that we wouldn't share, I guess. What do you think about that? Or do you really think we should just skip the config and have everything still be kind of a standalone thing and we don't call it pg-sync, instead we just call it ops-sync or something like that for now.

> @adam: I think the pragmatic answer is: **keep ClickHouse as a separate simple service**, and make pg-sync the config-driven generic utility. Here's why:
>
> - ClickHouse metrics are fundamentally different — they poll for aggregate counts (reactionCount, commentCount), not row-level changes. There's no trigger/outbox pattern. It's a simple "query CH every 60s, push updates to BitDex." Making this config-driven doesn't buy much because the query itself is CH-specific.
>
> - pg-sync is the one that benefits from being generic — the trigger setup, ops format, and polling loop are all table-agnostic. The YAML config makes it reusable across any PG schema.
>
> So: rename the pg-sync binary to something like `bitdex-sync` with subcommands (`bitdex-sync pg` and `bitdex-sync clickhouse`), or keep them as separate binaries (`bitdex-pg-sync` and `bitdex-ch-sync`). Both use the same ops endpoint on BitDex, both report via the same `meta` field with their `source` label, but their internals are different. The Prometheus metrics unify them under `bitdex_sync_*{source="..."}`.
>
> For now: keep ClickHouse as-is (it works fine at current scale), focus all the V2 effort on the PG side where the 80M gap lives.
@justin: Agreed. I want to keep the K8's deployment simple. Do you have each one as a separate sidecar, or is it a single sidecar and both processes run in there or something? But yeah, I think it makes sense to break out the ClickHouse thing since it's so much smaller and kind of its own standalone thing. Later, we can create some sort of plug-in system for this sync utility and have there be a Postgres one and a ClickHouse one, and you can configure them to differently, all of that stuff. But that can be something for the future. Unless you think that our config stuff could branch into how we're handling ClickHouse without too much extra lift.

> @adam: Currently in K8s it's a single sidecar container running the `bitdex-pg-sync sync` binary — it runs both the outbox poller and metrics poller as concurrent tokio tasks in one process. I'd keep that model: **one sidecar, two async tasks** (pg ops poller + CH metrics poller). Simpler than two sidecars, and they share the HTTP client to BitDex.
>
> For the binary naming: I'd rename from `bitdex-pg-sync` to just `bitdex-sync` with subcommands:
> - `bitdex-sync pg --config sync.toml` — runs the ops poller from PG
> - `bitdex-sync ch --config sync.toml` — runs the ClickHouse metrics poller
> - `bitdex-sync all --config sync.toml` — runs both (default for sidecar deployment)
>
> The ClickHouse side can stay hardcoded for now. If we later want to make it config-driven, the YAML config could have a `clickhouse_sources` section alongside `sync_sources`, but that's future work. The current CH poller is ~200 lines and works fine — not worth abstracting yet.

---

## Unified Load + Sync Pipeline

@justin: The current initial load process is painful — tear down pods, set replica to 0, run the bulk loader, wait, turn things back on. Can we unify initial load and steady-state sync through the same ops pipeline? The YAML config already describes the PG-to-BitDex field mapping. The same schema that drives triggers could drive initial load. If pg-sync detects BitDex doesn't have data for a configured field, it could COPY the table data, decompose it into ops following the same schema, and pipe them into the ops endpoint or write directly to the WAL.

> @adam: I think there's a clean design here. The key insight: **the YAML sync config already describes how to transform PG rows into ops**. A `TagsOnImageNew` entry with `slot_field: imageId, field: tagIds, value_field: tagId` tells you everything you need to bulk-load tags — it's the same mapping whether the data comes from a trigger firing or a COPY stream.
>
> ### How It Works
>
> **Detection:** On startup, pg-sync checks BitDex's `/stats` endpoint. If the alive count is 0 (or below some threshold), it knows a load is needed. Alternatively, BitDex could expose a `/sync-status` endpoint per sync source that tracks whether initial load has completed.
>
> **Loading:** pg-sync iterates the `sync_sources` config:
>
> 1. **Direct tables with `slot_field`** (Image, tags, tools, etc.): Stream rows via `COPY ... TO STDOUT` (same as the current CSV pipeline, but without writing CSVs to disk). For each row, produce ops following the config:
>    - Image row → `[{"op":"set","field":"nsfwLevel","value":1}, {"op":"set","field":"type","value":"image"}, ...]`
>    - Tag row → `[{"op":"add","field":"tagIds","value":42}]`
>
> 2. **Fan-out tables** (ModelVersion, Post, Model): Skip during initial load. Their fields (baseModel, publishedAt, availability) are derived — they'll be correct once the direct tables are loaded and triggers start firing. Or, more practically: include them as direct tables during load (e.g., JOIN Image + Post to get publishedAt) since the initial load doesn't need the trigger indirection.
>
> 3. **Pipe into BitDex:** Two options for throughput:
>    - **Option A: HTTP ops endpoint.** Batch ops into large payloads (50K ops per POST). The WAL-backed endpoint is fast (just append + fsync), and the WAL reader processes at 5.5M ops/s. HTTP overhead is the bottleneck — maybe 100K-200K ops/s. At 105M records with ~5 fields each = ~525M ops, that's ~45-90 minutes.
>    - **Option B: Direct WAL write.** pg-sync writes directly to the WAL file (shared filesystem in the same pod). Eliminates HTTP entirely. The WAL reader processes as fast as disk I/O allows. This is essentially what the current CSV loader does, but through the WAL format instead of a custom binary loader. Could hit 1M+ ops/s.
>    - **Option C: Loading mode bypass.** For initial load, skip the WAL entirely and use the engine's `enter_loading_mode()` to write directly to bitmaps in memory, then save snapshots. This is what the current single-pass loader does. Fastest possible path (~345K records/s sustained), but requires a dedicated code path.
>
> ### My Recommendation
>
> **Start with Option A** (HTTP ops endpoint) because it requires zero new code — the ops endpoint and WAL reader already exist. The load takes longer (~60-90 min at 105M) but:
> - No special loading mode, no dedicated code path
> - Same ops format, same dedup, same processing pipeline
> - pg-sync can run the load and then seamlessly transition to steady-state polling
> - If BitDex crashes mid-load, pg-sync just resumes from its cursor (WAL is durable)
>
> **The boot sequence becomes:**
> 1. K8s starts BitDex pod (server + pg-sync sidecar)
> 2. pg-sync waits for BitDex health check
> 3. pg-sync checks BitDex alive count — if 0, starts bulk load via ops endpoint
> 4. pg-sync streams PG tables through the config mapping → ops → POST /ops
> 5. Load completes, pg-sync seeds its cursor at current `max(BitdexOps.id)`
> 6. pg-sync transitions to steady-state polling — same process, no restart
>
> No manual intervention. No replica set juggling. No separate loader binary. Just boot and it works.
>
> **If 60-90 min is too slow**, we optimize later with Option B (direct WAL write) or Option C (loading mode). But let's prove the unified pipeline works first — the simplicity of one code path for both load and sync is worth a lot.
>
> ### What Changes in the Config
>
> The YAML config could add optional `load_query` fields for tables that need a different query during bulk load vs trigger-time:
>
> ```yaml
> - table: Image
>   slot_field: id
>   track_fields: [nsfwLevel, type, userId, postId, minor, poi, hideMeta, meta, blockedFor]
>   on_delete: delete_slot
>   load_query: |
>     SELECT i.id, i."nsfwLevel", i.type, i."userId"::int8, i."postId"::int8,
>            i.minor, i.poi, i."hideMeta", i.meta, i."blockedFor",
>            GREATEST(p."publishedAt", i."scannedAt", i."createdAt") as "sortAt",
>            p."publishedAt", p.availability
>     FROM "Image" i
>     JOIN "Post" p ON p.id = i."postId"
> ```
>
> This lets the Image load query JOIN Post to grab publishedAt/availability/sortAt in one pass, rather than needing a separate Post load. The trigger handles them separately (Post trigger fires `queryOpSet` for publishedAt changes), but for bulk load we can be smarter.
>
> Tables without `load_query` default to `SELECT {slot_field}, {value_field} FROM "{table}"` (or `SELECT * FROM "{table}"` for tracked fields).
>
> ### Detecting Load State
>
> Simplest approach: pg-sync persists a `load_complete` flag in BitDex (via a cursor or metadata endpoint). On boot:
> - Flag absent or false → run bulk load
> - Flag present and true → skip to steady-state polling
>
> If BitDex gets wiped (fresh data dir), the flag is gone, so pg-sync auto-loads. If pg-sync restarts mid-load, it checks the flag, sees it's incomplete, and resumes (cursor-based, same as steady-state recovery).

@justin: The reason that as part of the loader we grabbed post CSV and model CSV and model version CSV as separate things is because doing those joins in Postgres is actually really, really slow. We found that it was actually much faster to do a full dump on the table and then kind of do the joins ourselves. I wonder if there's a way that we can do that here still. as it is, it's not like we actually have to join those documents to have them get written as ops, right? I mean, for example, post is like this:
```
COPY SELECT id, "publishedAt", availability FROM "Post"
```
and then we can essentially have something that says, you know, query on this. Like:
```yaml
dependsOn: image # image table needs to be processed first
query: "postId = {id}" # Should match what we already have...
```
Then it means that we can still grab this table separately, and we just can use the post IDs in the bitdex to know the slots that need to have these values set. This will be an interesting one to measure, since it will mean that we need to be doing queries against the bitdex while we're loading it as well. But I think that'll be okay. It means that the bitdex needs to be able to operate while we're loading it.
@justin: As for how we get the data in as quickly as we can, I think HTTP is going to be a lot of overhead, and that writing directly to files is going to make more sense. If we can essentially be dumping tables at a time into a wall file, and then do a single HTTP call that says, hey, we're piping data into this wall, we'll tell you when we're done, then it allows the bit-dex process to essentially just process that file as quickly as it can. for the PG-Sync service to also just be piping data into that file as quickly as it can. I guess, would you have a file watcher that basically watches for new lines, or would it just pull and then batch everything up to its last known position. And then eventually the pg-sync thing will say, hey, I'm done putting data into this thing. And so then it knows once it reaches the end of file, it's done and can clean/remove the file.
@justin: So essentially, for knowing if something exists or not, I think we need to have... We know that measuring the size of a bitmap index is expensive. And alive probably isn't enough to know whether or not the table has been processed. So maybe we retain some sort of history. of the sinks, the dumps that have happened. So if we're saying that this wall processing thing is a dump, we can keep a history of the dumps and the pg-sync can look and see, hey, was this dump done? And if it was, then it knows that it doesn't need to do it. What do you think about that? That way, theoretically, even if we're able to dump a few tables and then it dies, we can always continue with the tables that we haven't dumped. Or if we decide to add additional data or tables in the future, we can easily do that as well. conceptually there's starting to be a few different things. There's like the real-time sync with like it's the metadata and cursor and all of that that handles going through the ops channel, and then there's like a separate dump or sync or something. I don't know. Maybe it could still go through... No, it probably doesn't make sense to have that go through the ops channel. I mean, I guess it could, because all that the ops channel does is put things into the wall, and so maybe you send an op that says watch this wall or something. Probably not. Can you help me think through the design for this one? Kind of being aware of the existing endpoints that we have?

> @adam: Okay, let me pull all three threads together into a concrete design.
>
> ### Concept: Table Dumps as WAL Files
>
> The bulk load pipeline has two channels, both feeding the same WAL reader:
>
> 1. **Dump channel** — pg-sync writes table dumps directly to WAL files on the shared filesystem. One WAL file per table. BitDex's WAL reader processes them.
> 2. **Ops channel** — steady-state ops arrive via `POST /ops`, get appended to the live WAL file. Same reader, same processing.
>
> Both produce the same format. The WAL reader doesn't care where the file came from.
>
> ### Flow
>
> ```
> pg-sync boot
>   → GET /api/indexes/{name}/dumps (check dump history)
>   → For each sync_source not yet dumped:
>       1. COPY table FROM PG via streaming query
>       2. Transform rows → ops using config schema
>       3. Write ops directly to WAL file: dumps/{table}_{timestamp}.wal
>       4. POST /api/indexes/{name}/dumps/register
>          { "table": "TagsOnImageNew", "wal_file": "dumps/tags_20260325.wal", "row_count": 375000000 }
>   → BitDex WAL reader picks up dump WAL files, processes them
>   → Once all tables dumped + processed:
>       - pg-sync seeds cursor at current max(BitdexOps.id)
>       - Transitions to steady-state ops polling
> ```
>
> ### How pg-sync Writes Dump WAL Files
>
> pg-sync and BitDex run in the same pod (sidecar model), so they share a filesystem. pg-sync writes directly to BitDex's WAL directory — no HTTP overhead for the bulk data.
>
> The WAL reader uses a **poll-based tail**: check file size periodically, read new bytes since last position, process batch. No inotify needed — just a tight loop with a small sleep (10ms). At 1M+ ops/s write rate, the reader keeps up easily since bitmap ops are faster than disk writes.
>
> When pg-sync finishes writing a dump file, it calls the register endpoint. This tells BitDex "there's a complete dump WAL at this path, process it to completion." The WAL reader processes it sequentially, and when it reaches EOF on a registered-complete file, it marks the dump as done.
@justin: I really wanted things to be able to start getting processed as soon as pg-sync essentially starts writing to that file, rather than having to wait until it's done dumping to that file. Are we on the same page there regarding how that would work? Essentially, PG-Sync is going to get a dump ID back, and then it'll say when it's done, and the bitdex wall reader will be reading from that dump file, polling for changes, handling that, and will continue to poll until it's told that it's done or whatever. I think that's what you're describing, but I'm not completely sure.

> ### Table Ordering with `dependsOn`
>
> Good call on the Post table needing Image loaded first (so `postId eq {id}` queries work). The config handles this:
>
> ```yaml
> # Phase 1: Direct tables (no dependencies)
> - table: Image
>   slot_field: id
>   track_fields: [nsfwLevel, type, userId, postId, minor, poi, hideMeta, meta, blockedFor]
>   load_query: |
>     COPY (SELECT id, "nsfwLevel", type::text, "userId"::int8, "postId"::int8,
>           minor, poi, "hideMeta", meta, "blockedFor",
>           EXTRACT(EPOCH FROM GREATEST(p."publishedAt", i."scannedAt", i."createdAt"))::int8 as "sortAt"
>     FROM "Image" i JOIN "Post" p ON p.id = i."postId") TO STDOUT
@justin: I don't know if you caught it, but I tried to say above that joining posts like that is a real problem here. We can't do this. And that's why we do the thing below instead. So instead, this should just be a simple copy straight from the image table. No joins, no orders, anything like that. As for how we handle sort at, I think we probably need to adjust the config for the Civitai index to have a computed field or something for sort at that's driven by published at, scanned at, and created at. and maybe we don't even track scanned at and created at together. Maybe we just do greater greatest of scanned at and created at since those both come from the image table and we can call it, uh, yeah, we can just call it created at even though it's technically not the created at and that's fine. Um, and then we'd have publish at come from the posts table. and then the computed field sort at would be, yeah, the greatest of the two. It's probably something we would need to add support for. And I guess looking at the config, we might need to see how that gets handled. Just because having to do this join right here is a real problem. It killed our ability to pull this data. I guess that technically this might actually be better anyways if we can do it this way because otherwise, sort out, where is that going to come from when we continue in the steady state? Are we going to join posts every time image changes to look and see what the sort out is? Or do we want to just continue to be able to send in, hey, created at or scanned at changed or whatever we call that joint prop that has changed on image, and whenever post changes, we just send in, hey, the published at has changed, and then the computed field handles, you know, doing what it's got to do.

> @adam: You're absolutely right — I missed the JOIN problem. At 107M images, even an indexed JOIN with Post is brutal for COPY. No JOINs in load queries, period.
>
> **Computed fields** solve this cleanly. Here's how it would work:
>
> 1. **Image dump** is a flat COPY — no JOINs:
>    ```sql
>    COPY (SELECT id, "nsfwLevel", type::text, "userId"::int8, "postId"::int8,
>          minor, poi, "hideMeta", meta, "blockedFor",
>          EXTRACT(EPOCH FROM GREATEST("scannedAt", "createdAt"))::int8 as "existedAt"
>    FROM "Image") TO STDOUT
>    ```
>    This gives us `existedAt` = `GREATEST(scannedAt, createdAt)` from Image alone.
>
> 2. **Post dump** is separate, also flat:
>    ```sql
>    COPY (SELECT id, EXTRACT(EPOCH FROM "publishedAt")::int8 as "publishedAt",
>          availability FROM "Post") TO STDOUT
>    ```
>    Post ops use `queryOpSet` with `"postId eq {id}"` to set `publishedAt` and `availability` on the right image slots.
>
> 3. **Computed field `sortAt`** defined in the BitDex index config:
>    ```yaml
>    sort_fields:
>      sortAt:
>        type: u32
>        computed: "GREATEST(existedAt, publishedAt)"
>    ```
>    Whenever `existedAt` or `publishedAt` changes (from any source — Image trigger, Post trigger, or dump), BitDex recomputes `sortAt` as the max of the two.
>
> **Steady-state is clean too:**
> - Image `scannedAt` changes → trigger emits `set existedAt` → BitDex updates `existedAt` sort layer + recomputes `sortAt`
> - Post `publishedAt` changes → trigger emits `queryOpSet "postId eq {id}"` with `set publishedAt` → BitDex updates `publishedAt` + recomputes `sortAt`
> - No JOINs anywhere. Each trigger only touches its own table's columns.
>
> This is a new feature we'd need to add — computed sort fields. But the implementation is straightforward: on any mutation that touches a source field of a computed field, recalculate the computed value and update the sort layers. It's just a `max()` of two u32 values per affected slot.
>
> **Updated Image config:**
> ```yaml
> - table: Image
>   slot_field: id
>   track_fields: [nsfwLevel, type, userId, postId, minor, poi, hideMeta, meta, blockedFor]
>   track_sort_fields:
>     existedAt: "GREATEST(\"scannedAt\", \"createdAt\")"  # computed at PG level
>   on_delete: delete_slot
>   load_query: "COPY (SELECT id, ... FROM \"Image\") TO STDOUT"
> ```
@justin: I don't necessarily like the track sort fields because we would want to be able to use that constructed query thing that we have there for existed at for all types of fields. So it has me thinking maybe something more like this:
```
- table: Image
  slot_field: id
  track_fields: [nsfwLevel, type, userId, postId, minor, poi, hideMeta, meta, blockedFor, "GREATEST({scannedAt}, {createdAt}) as existedAt"] # @justin: the strings inside of tracked fields can include interpolation of field names as part of the query. What do you think?
  on_delete: delete_slot
  load_query: "COPY (SELECT id, ... FROM \"Image\") TO STDOUT"
```

> @adam: Yeah, this is much cleaner — one flat list, and expressions are just strings with `{column}` interpolation. No separate `track_sort_fields` concept. The trigger generator sees a plain string like `nsfwLevel` → direct column reference. Sees `"GREATEST({scannedAt}, {createdAt}) as existedAt"` → expression with column refs, aliased as `existedAt`.
>
> In the trigger, this becomes:
> ```sql
> -- For plain field: nsfwLevel
> IF OLD."nsfwLevel" IS DISTINCT FROM NEW."nsfwLevel" THEN ...
>
> -- For expression: GREATEST({scannedAt}, {createdAt}) as existedAt
> IF GREATEST(OLD."scannedAt", OLD."createdAt") IS DISTINCT FROM
>    GREATEST(NEW."scannedAt", NEW."createdAt") THEN
>     -- emit remove with old GREATEST value, set with new GREATEST value, field = "existedAt"
> ```
>
> The trigger generator parses the expression, substitutes `{col}` with `OLD."col"` for the remove and `NEW."col"` for the set. The alias (`as existedAt`) becomes the BitDex field name in the op.
>
> For the load query, the COPY SELECT includes `GREATEST("scannedAt", "createdAt") as "existedAt"` as a column. Same expression, different context.
>
> This also means computed fields aren't a BitDex-engine feature — they're a pg-sync trigger feature. BitDex just sees `existedAt` as a normal sort field. The computation happens in PG at trigger time and at COPY time. Simpler.

> - table: TagsOnImageNew
>   slot_field: imageId
>   field: tagIds
>   value_field: tagId
>   # No dependsOn — can load in parallel with Image since it just adds to tagIds bitmaps
>
> # Phase 2: Fan-out tables (need Image loaded first for query resolution)
> - table: Post
>   dependsOn: [Image]
>   query: "postId eq {id}"
>   track_fields: [publishedAt, availability]
>   load_query: "COPY (SELECT id, \"publishedAt\", availability FROM \"Post\") TO STDOUT"
> ```
>
> But actually — for initial load, we can be smarter. The Image `load_query` already JOINs Post to grab `sortAt`, `publishedAt`, and `availability`. So we can fold Post's fields into the Image load query and skip the Post dump entirely during initial load. The Post trigger only matters for steady-state (when a Post's publishedAt changes independently of the Image).
>
> Same for ModelVersion/Model — during initial load, the CSV loader already grabs baseModel and poi via JOINs. The fan-out triggers only matter for steady-state changes.
>
> So the dump order is really:
> 1. **Image** (with Post JOIN for sortAt/publishedAt/availability)
> 2. **TagsOnImageNew, ImageTool, ImageTechnique, CollectionItem, ImageResourceNew** (can run in parallel — each is an independent multi-value field)
> 3. **ClickHouse metrics** (separate, runs after)
>
> No fan-out table dumps needed. Fan-out tables only produce ops during steady-state.
@justin: So as I mentioned above, I don't think that this is a good idea to have joins in the queries, so we actually will need to dump those tables individually or something. If you might look at what we currently have in the CSV thing, we might be doing a join on the model version and models table, and I think that's okay because there's so many less. I think there's less than a million, so it's just not an issue. But with images where there's 107 million, and then posts where there's, you know, 18 million or something like that, it starts to be really problematic, even with Postgres indexes and everything. It makes it so it's way slower to do that copy...

> @adam: Understood — no JOINs for large tables. Revised dump order:
>
> 1. **Image** — flat COPY, no JOINs. Produces `existedAt` (= GREATEST of scannedAt, createdAt) locally.
> 2. **TagsOnImageNew, ImageTool, ImageTechnique, CollectionItem, ImageResourceNew** — flat COPYs, parallel.
> 3. **Post** — flat COPY (id, publishedAt, availability). Uses `queryOpSet "postId eq {id}"` to set these on the right image slots. Depends on Image being loaded first so the postId bitmaps exist.
> 4. **ModelVersion** — flat COPY (small table, <1M rows). JOINs with Model are fine at this scale. Sets baseModel via `queryOpSet`.
> 5. **ClickHouse metrics** — separate dump.
>
> The computed `sortAt` field in BitDex handles the GREATEST(existedAt, publishedAt) calculation. No JOINs needed anywhere in the dump pipeline.

>
> ### Dump History
>
> BitDex stores dump history in MetaStore (simple atomic JSON file, same as cursors/slot_counter):
>
> ```json
> {
>   "dumps": {
>     "Image": {"completed_at": "2026-03-25T20:00:00Z", "rows": 107500000, "config_hash": "a1b2c3d4"},
>     "TagsOnImageNew": {"completed_at": "2026-03-25T20:15:00Z", "rows": 375000000, "config_hash": "e5f6g7h8"}
>   }
> }
> ```
>
> On boot, pg-sync fetches this via `GET /api/indexes/{name}/dumps`. For each sync_source:
> - Dump exists with matching `config_hash` → skip
> - Dump exists but `config_hash` differs (config changed) → re-dump
> - No dump → run dump
>
> `config_hash` is the hash of that table's YAML config entry. If you add a field to `track_fields`, the hash changes, triggering a re-dump. This handles the "add new data/tables in the future" case automatically.
>
> If pg-sync dies mid-dump, the WAL file is incomplete. On restart, pg-sync sees no `completed` entry for that table, deletes the partial WAL file, and re-dumps. Simple and safe.
@justin: rather than having config hash as a separate thing, I think it should all just be part of the name. The name is something that the dump request includes anyways, and so the pg sync can just include the hash as part of the name. `Image-a1b2c3d4`. This keeps this dumps thing more denormalized anyways. ConfigHash would probably be something unique to this sync mechanism that we're creating, but could, you know, be unnecessary for a lot of other things. Something else I'm realizing this doesn't handle yet is the ClickHouse data, but I'm assuming we would essentially have a similar mechanism for on boot that doing a similar and other Dump and all of that. This points to probably some shared code between those...

> @adam: Clean — hash in the name is simpler. BitDex just stores dump names as opaque strings. pg-sync constructs them as `{Table}-{hash}` and checks if that exact name exists. If the config changes, the hash changes, name doesn't match, re-dump. BitDex doesn't need to know what the hash means.
>
> For ClickHouse: yes, same dump mechanism. `ch-sync` registers a dump like `Metrics-{hash}`, writes the CH metrics dump to a WAL file, signals loaded. The shared code is the WAL file format + the dumps API client. Both pg-sync and ch-sync use the same `DumpClient` struct to register/signal/check dumps. The WAL writer is the same too — just the data source differs (PG COPY stream vs CH HTTP query).

>
> ### Endpoints
>
> | Endpoint | Purpose |
> |----------|---------|
> | `GET /api/indexes/{name}/dumps` | Return dump history |
> | `POST /api/indexes/{name}/dumps/register` | Register a completed dump WAL file |
> | `POST /api/indexes/{name}/dumps/clear` | Clear dump history (force re-dump on next boot) |
> | `POST /api/indexes/{name}/ops` | Steady-state ops (existing) |
@justin: So, I think what I'm suggesting is that register is just, hey, we're starting a dump. And then there needs to be some way to mark it complete. And maybe rather than clear, it makes sense to just use the actual rest stuff for that, like this.
```
GET /api/indexes/{name}/dumps
PUT /api/indexes/{name}/dumps - Add a new dump -> returns TaskID that can be used to watch for completion. WAL reader immediately starts watching for ops and polls for new lines on EOF
POST /api/indexes/{name}/dumps/{dumpName}/loaded - Signal that the dump has been fully written to the WAL -> WAL reader stops polling for additional ops on EOF
DELETE /api/indexes/{name}/dumps - clear dump history
DELETE /api/indexes/{name}/dumps/{dumpName} - remove dump from history
POST /api/indexes/{name}/ops
```

> @adam: Love the RESTful endpoint design. The lifecycle is clear:
>
> 1. `PUT /dumps` with dump name → BitDex creates the dump entry, starts WAL reader polling for that file, returns a task ID
> 2. pg-sync writes ops to the WAL file at its own pace
> 3. `POST /dumps/{name}/loaded` → BitDex's WAL reader knows to stop polling at EOF (file is complete)
> 4. WAL reader finishes processing → dump status becomes "complete"
> 5. pg-sync can poll the task ID or `GET /dumps` to check completion
>
> The task ID return from PUT is a nice touch — pg-sync can poll `GET /dumps/{name}` to check status without blocking. Or even `GET /dumps/{name}/status` returning `{"state": "processing", "ops_processed": 42000000, "ops_total": null}` (total unknown until `/loaded` is called).
>
> `DELETE /dumps/{name}` for removing individual entries is cleaner than a bulk clear. If you want to force re-dump of just tags, delete that one entry. If you want a full reset, delete all.
@justin: We actually already have an endpoint for checking the status of tasks, I believe. You might want to double check on that.

> @adam: Right — we have `GET /api/indexes/{name}/load-status` that returns the load state (Loading, Saving, Complete). We could reuse or extend it. But for dumps, `GET /dumps` gives per-table granularity (Image done, Tags in progress, Post pending). The task ID from `PUT /dumps` could just be the dump name — pg-sync polls `GET /dumps/{name}` to check completion.
@justin: No, send an explorer, I thought we had something like `/api/...tasks`...

> @adam: You're right — we do have task endpoints:
> - `GET /api/indexes/{name}/tasks` — list tasks
> - `GET /api/tasks/{task_id}` — get task status
>
> (server.rs lines 1062-1063). The `TaskTracker` supports task types, progress counters, and status (active/saving/complete/error). So `PUT /dumps` can return a task ID, and pg-sync polls `GET /api/tasks/{task_id}` to track dump processing progress. No new status endpoint needed — we just wire dumps into the existing task system.

>
> ### BitDex Loading Mode
>
> When BitDex detects it's processing dump WALs (registered via the dumps endpoint), it can enter a lightweight loading mode:
> - Skip cache invalidation (no queries happening yet)
> - Skip bound cache maintenance
> - Batch snapshot publishing (less frequent ArcSwap stores)
> - But keep the server responsive for health checks and the dumps API
>
> This isn't the full `enter_loading_mode()` we have today — it's more like "reduced maintenance mode" that still allows queries (for `queryOpSet` resolution during Post/MV loads if needed).
@justin: I think for processing dumps, we would want it to be as efficient as it can be similar to what we have for the single pass loader, just so that it can ensure maximum throughput of those wall files. I think you're right that it essentially skips all of those things. I know we have an ingester now. It might make sense to have an explorer go look at what's there. Maybe we can use that here with just different traits so that it skips some of these things. I don't know, I'm worried that maybe things haven't been wired up quite right, but I was hoping that it would be easy for us to essentially skip all of this maintenance stuff and have it basically be the same as what we're doing in the single pass loader. And then probably when a dump completes, it writes the bitmap to disk. and unloads it. And if a query needs it, then it will go load it. But because of the way that we're doing the pg-sync here, it's unlikely that any queries are going to come in for something that's being loaded from a dump during that time. And we would probably want to that we would need to do something to have Bitdex server be healthy but to not be taking incoming requests from external sources or whatever during this time Maybe we don need to worry about that part I can just have traffic not coming in for now But I thinking about you know later when we potentially adding stuff while the text is loading I don think it be an issue because you be loading things that aren referenced by any queries downstream yet so it shouldn be an issue and if you do need to load things up, then you would just be doing that against an instance that's not taking queries yet. And then for ones that are requiring queries, then it'll load them up just like normal through the standard query path, and it'll already have all the bitmaps ready for it to be loaded. So essentially, processing a dump skips all of the normal write path stuff and is instead all about preparing the in a bitmap as quickly as possible, or preparing bitmaps as quickly as possible. And since everything in the wall will be tied to a single bitmap, it should be okay. Or very few bitmaps should be okay. And then saving those as soon as they can, as soon as you're done processing the wall. And then the doc store, or shard store for docs, there shouldn't be anything that we have to carry in memory since everything's just append all, or append to the end of the in the shard file. And so really what we could do is, before we call the dump complete, is we do a compact on that, and that creates the actual snapshot portion of those shards. But maybe we don't need to do that compact. Maybe that compact can happen on demand. Do we have an endpoint for doing that? Then essentially what we would do is we'd, you know, do our bulk load with pg-sync, and then when we're happy with how everything is loaded, then we can, you know, do a compact on demand. And it'll handle going through all the shard files and compressing things. Because if we compact after each dump, then we'll be doing it over and over a lot, which kind of, like, is a that doesn't make sense. Yeah, I don't know. What do you think about that? Maybe we don't ever compact, because we don't necessarily need to. We could just wait for that to happen naturally over time...

> @adam: Good thinking on all of this. Let me address each piece:
>
> **Dump processing mode:** You're right — dump WAL processing should bypass the normal write path (coalescer → flush thread → snapshot publish → cache invalidation). Instead, the dump processor builds bitmaps directly, same as the single-pass loader. The Ingester trait system (`src/ingester.rs`) was designed for exactly this — different sinks for different paths. We have `CoalescerSink` (normal writes), `AccumSink` (batch accumulation), and `DocSink` (docstore writes). For dumps, we'd use something like a `DumpSink` that:
> - Accumulates bitmap mutations in memory (like `AccumSink`)
> - Writes docstore entries as append-only (no read-back needed since everything is new)
> - Skips snapshot publishing, cache invalidation, bound maintenance
> - On dump completion: saves bitmaps to disk via ShardStore, then unloads from memory
>
> The existing single-pass loader (`single_pass.rs`) writes directly to BitmapFs — we can reuse that pattern. The key difference is it reads from CSVs while the dump processor reads from WAL files. Same bitmap-building logic, different input source.
>
> **Compaction:** Don't compact after each table dump. The ShardStore already has threshold-based compaction (default 1,000 ops before compacting a shard). During dump processing, the ops count will exceed that threshold naturally, but we should **skip compaction during dumps** and let it happen after all dumps complete. Or just let the normal compaction thread handle it in steady-state — it runs periodically and compacts shards that exceed the threshold. No manual trigger needed.
>
> We do have `POST /api/indexes/{name}/compact` for on-demand compaction if you want it, but honestly the automatic compaction handles this fine. After a full dump, the first few minutes of steady-state will trigger compaction on the hot shards, and everything settles.
>
> **Save + unload pattern:** This is elegant. After a table dump completes:
> 1. Save the bitmap to disk (ShardStore snapshot)
> 2. Unload from memory (drop the in-memory bitmap)
> 3. If a query later needs it → lazy load from disk (existing `ensure_fields_loaded()` path)
>
> This keeps peak memory low during multi-table dumps. Load Image bitmaps (biggest), save to disk, unload, then load TagsOnImageNew, save, unload, etc. At any point, only one table's bitmaps are in memory. Queries during loading just trigger lazy loads for the fields they need.
>
> **Server health during dumps:** The server stays healthy (health endpoint responds) but isn't taking external query traffic yet. This is fine — in K8s, the readiness probe can check a `/ready` endpoint that returns 503 until all dumps are complete. Health probe says "alive" (don't kill me), readiness probe says "not ready" (don't send traffic). Once dumps complete and pg-sync transitions to steady-state, readiness flips to 200 and K8s starts routing traffic.
@justin: Great! regarding the doc sync. You mentioned that there's readback as part of the doc store. I thought that we always just had doc store entries as append only. I don't know why we read back. Can you help me understand that? Because we designed doc store on top of shard store or whatever, right? So it should be essentially snapshot plus ops. So theoretically, we could just be always doing whatever we're about to do here. You just append ops. So it kind of sounds like all we've got here is a QM sync and doc sync and then some command that we call at the end of the dump to... Oh, basically when when pg-sync has said, hey, I'm done, and the wall reader finishes the file, then we unload that bitmap, and it will save and unload or whatever. So I guess that is a difference. But I feel like we've got all of the traits that we actually need for the of actual processing, hopefully.

> @adam: Good question. The docstore IS append-only — writes are always appends. The readback I mentioned happens on the **bitmap write path**, not the docstore path. Let me clarify:
>
> **Current `put()` flow for an existing document:**
> 1. Read old doc from docstore (or doc cache) to get current field values
> 2. Diff old vs new to determine which bitmaps changed
> 3. Update only the changed bitmaps
> 4. Append new doc to docstore
>
> Step 1 is the "readback" — we read the old doc to know which bitmaps to remove the slot from. With V2 ops carrying old values (`{"op":"remove","field":"nsfwLevel","value":8}`), we skip step 1 entirely. The op tells us what to remove. So the write path becomes: apply bitmap ops from the op array → append doc to docstore. No reads.
>
> For **dumps** specifically, there's no old doc at all (everything is new), so there's never a readback. It's purely: set bits in bitmaps + append to docstore. The existing traits handle this — you're right that we have what we need. The dump processor just:
> 1. Iterates ops from the WAL
> 2. For bitmap ops: set/clear bits in in-memory bitmaps
> 3. For docstore: append field values to shard files
> 4. On dump complete: save bitmaps to disk, unload from memory
>
> So yes — bitmap sync + doc sync + save/unload on completion. The traits (`AccumSink` for bitmaps, `DocSink` for docstore) cover the processing. The new part is just the WAL reader feeding ops into those sinks instead of CSV rows.

>
> ### Summary
>
> Two channels, one WAL reader, one processing pipeline:
> - **Dumps:** pg-sync → direct WAL file writes → WAL reader → bitmaps
> - **Ops:** pg-sync → POST /ops → WAL append → WAL reader → bitmaps
> - **History:** Per-table dump tracking with config hashing for change detection
> - **Boot:** Auto-detect what's missing, dump only what's needed, transition to steady-state

---

## Files That Change

| File | Change |
|------|--------|
| `src/pg_sync/queries.rs` | Add `BitdexOps` table SQL, `poll_ops_from_cursor()` |
| `src/pg_sync/ops_poller.rs` | **New** — V2 poller with dedup/compression |
| `src/pg_sync/config.rs` | Add V2 config fields (ops table name, feature flags) |
| `src/bin/pg_sync.rs` | Add `SyncV2` subcommand, wire ops_poller |
| `src/server.rs` | Add `POST /ops`, `GET/POST /pgsync-config`, `GET /pgsync-lag` |
| `src/pg_sync/bitdex_client.rs` | Add `post_ops()`, `get_pgsync_config()` |
| `src/pg_sync/outbox_poller.rs` | Add config-check for runtime batch_limit |
| `src/metrics.rs` | Add pgsync lag Prometheus gauges |
