# BitDex Prod Sync Config + Trigger Inventory (2026-07-15)

Scout report for the scheduled-publish / date-handling redesign.

## Source-of-truth locations

| Artifact | Path |
|---|---|
| **PROD sync config** (deployed) | `talos-infra/clusters/production/apps/bitdex/v2-sync-config.yaml` (ConfigMap `bitdex-v2-sync-config`) |
| **PROD index config** (deployed) | `talos-infra/clusters/production/apps/bitdex/deployment.yaml` lines 78–265 (inline ConfigMap) |
| Repo copies (`deploy/configs/`) | `sync-config-civitai.yaml` (matches prod, whitespace-only diff), `prod-sync-config-civitai.yaml` (**STALE**), `civitai-index.yaml` (matches prod index minus prod eviction knobs) |
| Trigger generator | `src/pg_sync/trigger_gen.rs` |
| DeferredAliveConfig struct | `src/config.rs:174–198` |

---

## 1. PROD sync config — entities, fields, trigger events

Two sections: **`dump_phases`** (bulk boot load) and **`triggers`** (steady-state PG triggers → `BitdexOps`). The BitDex **index config** (in deployment.yaml) is a *separate* file defining what the engine indexes and how `sortAt`/`isPublished` are derived.

### Image dump phase (v2-sync-config.yaml:44–103) — full quote
```yaml
- name: images
  table: Image
  copy_query: >
    COPY (SELECT id, url, "nsfwLevel", hash, flags, type::text,
                 "userId", "blockedFor",
                 extract(epoch from "scannedAt")::bigint as "scannedAtSecs",
                 extract(epoch from "createdAt")::bigint as "createdAtSecs",
                 "postId", width, height
          FROM "Image")
    TO STDOUT WITH (FORMAT csv)
  columns: [id, url, nsfwLevel, hash, flags, type, userId, blockedFor, scannedAtSecs, createdAtSecs, postId, width, height]
  slot_field: id
  sets_alive: true
  fields:
    - nsfwLevel
    - { column: type, target: type }
    - userId
    - postId
    - blockedFor
    - { column: url, target: url }          # doc-only
    - { column: hash, target: hash }        # doc-only
    - width                                  # doc-only
    - height                                 # doc-only
  computed_fields:
    - target: hasMeta
      expression: "(flags >> 13) & 1 == 1 && (flags >> 2) & 1 == 0"
    - target: onSite
      expression: "(flags >> 14) & 1 == 1"
    - target: minor
      expression: "(flags >> 3) & 1 == 1"
    - target: poi
      expression: "(flags >> 4) & 1 == 1"
    - target: existedAt
      expression: "max(scannedAtSecs, createdAtSecs)"
    - target: id
      expression: "id"  # slot ID as sort value
  enrichment:
    - lookup: posts.csv
      table: Post
      copy_query: >
        COPY (SELECT id,
                     extract(epoch from "publishedAt")::bigint as "publishedAtSecs",
                     availability::text,
                     "modelVersionId"
              FROM "Post")
        TO STDOUT WITH (FORMAT csv)
      columns: [id, publishedAtSecs, availability, modelVersionId]
      key: id
      join_on: postId
      fields:
        - { column: publishedAtSecs, target: publishedAt }
        - { column: availability, target: availability }
        - { column: modelVersionId, target: postedToId }
      computed_fields:
        - target: isPublished
          expression: "publishedAtSecs != null"
  # sortAt = GREATEST(existedAt, publishedAt) is defined in index config
  # as a computed sort field — BitDex resolves it automatically.
```

`publishedAt`/`availability`/`postedToId`/`isPublished` all come from the **Post** table via CSV enrichment join on `postId` — they are NOT Image columns.

### Other dump phases (brief)
- **tags** (`TagsOnImageNew`, slot `imageId`): `tagId → tagIds`, `filter: (attributes >> 10) & 1 = 0`.
- **resources** (`ImageResourceNew`): `modelVersionId → modelVersionIds`; computed `modelVersionIdsManual` when `detected == false`; nested ModelVersion→Model enrichment producing `baseModel` and `poi` (filter `type = 'Checkpoint'`).
- **tools** (`ImageTool`): `toolId → toolIds`. **techniques** (`ImageTechnique`): `techniqueId → techniqueIds`.
- **metrics** (ClickHouse `entityMetricDailyAgg_v2`): `reactionCount`, `commentCount`, `collectedCount`.

### Image steady-state trigger (v2-sync-config.yaml:213–237) — full quote
```yaml
- table: Image
  slot_field: id
  sets_alive: true
  on_delete: true
  track_fields:
    - nsfwLevel
    - { column: type, expression: "{type}::text" }
    - userId
    - postId
    - blockedFor
    - url
    - hash
    - width
    - height
    # existedAt = GREATEST(scannedAt, createdAt) — required for sortAt computation.
    - { column: existedAt, expression: "GREATEST(extract(epoch from {scannedAt})::bigint, extract(epoch from {createdAt})::bigint)" }
  computed_fields:
    - target: hasMeta
      expression: "({flags} >> 13) & 1 = 1 AND ({flags} >> 2) & 1 = 0"
    - target: onSite
      expression: "({flags} >> 14) & 1 = 1"
    - target: minor
      expression: "({flags} >> 3) & 1 = 1"
    - target: poi
      expression: "({flags} >> 4) & 1 = 1"
```
**Events** (`trigger_gen.rs:185–193`): Image has `on_delete: true` and no `field:`, so it fires **`AFTER INSERT OR UPDATE OR DELETE`**. UPDATE emits remove/set pairs only where `OLD IS DISTINCT FROM NEW`. **The Image trigger does NOT track `publishedAt`, `availability`, `postedToId`, or `isPublished` — those arrive only via the Post fan-out.**

### Post fan-out trigger (v2-sync-config.yaml:264–274) — full quote
```yaml
# Fan-out triggers (queryOpSet):
- table: Post
  type: fan_out
  query: "postId eq {id}"
  track_fields:
    - { column: publishedAt, target: publishedAt, expression: "extract(epoch from {publishedAt})::bigint" }
    # Null handling: if {publishedAt} is NULL, trigger emits remove op (clears the field).
    - { column: availability, target: availability, expression: "{availability}::text" }
    - { column: modelVersionId, target: postedToId }
```
**Events** (fan-out path, `trigger_gen.rs:476,497`): fires **INSERT and UPDATE only** (no DELETE branch for fan-out). Emits a single `queryOpSet` op keyed on `NEW.id`; `query` resolves to `postId eq <id>`, `ops` are the changed set/remove pairs.

Other fan-outs: **ModelVersion** (`modelVersionIds eq {id}` → `baseModel`, Model join for Checkpoint filter) and **Model** (json_agg subquery → `modelVersionIds in [{ids}]` → `poi`).

---

## 2. Repo vs talos-infra differences (`deploy/configs/`)

- **`sync-config-civitai.yaml`** — content-identical to deployed talos config (diff is whitespace only: talos wraps it in the ConfigMap `sync-config-civitai.yaml: |` with extra indent). Current.
- **`prod-sync-config-civitai.yaml`** — **STALE**, two substantive drifts vs deployed:
  1. **Metrics query old**: `SELECT ... FROM image_metrics` instead of deployed `sumIf(...) FROM entityMetricDailyAgg_v2 ... WHERE entityType='Image' AND entityId >= {id_lo} AND entityId < {id_hi} GROUP BY entityId`.
  2. **Missing the Image `existedAt` track_field** (v2-sync-config.yaml:228) that the deployed trigger config has.
- **`civitai-index.yaml`** matches deployed index config **except** it omits prod eviction knobs (`tagIds eviction idle_seconds:300`, `modelVersionIds/Manual per_value_lazy + eviction:7200`, `eviction_sweep_interval`, `doc_compact_threshold`, cache `max_entries/max_bytes/bucket_entry_ttl_secs`). Field/sort/deferred definitions identical.

---

## 3. How `queryOpSet` is generated (trigger_gen.rs)

- Produced **iff `type: fan_out`** (i.e. `query:` set and `field:` unset). Dispatch `trigger_gen.rs:214–225` → `generate_fan_out_body` (line 424).
- Body builds `_query` from the `query:` template (`build_query_concatenation`, line 618, e.g. `'postId eq ' || NEW."id"::text`) and inserts one op: `jsonb_build_object('op', 'queryOpSet', 'query', _query, 'ops', _ops)` (lines 490–492, 520–523).
- The op ships a **query**, not per-slot writes. BitDex resolves the query against its own index at apply time (mechanism behind the publish fan-out drift, FD #69397).
- **Fields using queryOpSet fan-out in PROD:**
  - **Post** → `publishedAt`, `availability`, `postedToId` (query `postId eq {id}`)
  - **ModelVersion** → `baseModel` (query `modelVersionIds eq {id}`)
  - **Model** → `poi` (query `modelVersionIds in [{ids}]`)
  - So **yes** to publishedAt, availability, postedToId — all three ride the same Post fan-out.

---

## 4. Fields BitDex indexes for images (deployment.yaml:81–121)

**Filter fields (20):** `nsfwLevel`, `userId`, `type`, `baseModel`, `availability` (single_value, eager); `postId`, `postedToId` (single_value, per_value_lazy); `remixOfId` (single_value); `hasMeta`, `onSite` (boolean eager); `poi`, `minor` (boolean); `isPublished` (boolean eager); `isRemix` (boolean); `blockedFor` (single_value eager); `tagIds`, `modelVersionIds`, `modelVersionIdsManual`, `toolIds`, `techniqueIds` (multi_value).

**Sort fields (7):** `reactionCount` (32b eager), **`sortAt`** (32b eager, computed), `commentCount` (32b eager), `collectedCount` (32b eager), `existedAt` (32b), `publishedAt` (32b), `id` (32b).

---

## 5. sortAt / publishedAt / existedAt / isPublished — and the PG `sortAt` question

| Field | How computed today | Sourced from PG `Image."sortAt"`? |
|---|---|---|
| **existedAt** | `max(scannedAt, createdAt)`. Dump: `max(scannedAtSecs, createdAtSecs)`. Trigger: `GREATEST(extract(epoch from {scannedAt}), extract(epoch from {createdAt}))`. | No |
| **publishedAt** | From **Post** only. Dump enrichment `publishedAtSecs → publishedAt`; Post fan-out `extract(epoch from {publishedAt})`. NULL publishedAt → remove op (clears field). | No |
| **isPublished** | `exists_boolean` shadow of `publishedAtUnix` (data_schema: `source: publishedAtUnix, target: isPublished, value_type: exists_boolean`). Dump also sets via `publishedAtSecs != null`. True iff publishedAt present. | No |
| **sortAt** | **Engine-computed sort field**, never ingested. Index config (deployment.yaml:111–116): `computed: { op: greatest, source_fields: [existedAt, publishedAt] }`. BitDex resolves `sortAt = GREATEST(existedAt, publishedAt)` whenever existedAt/publishedAt change. | **No** |

**Key finding:** PG's `Image."sortAt"` column is **referenced nowhere** — not in any COPY query, dump phase, trigger, or index mapping. The Image copy_query selects `scannedAt, createdAt, postId` but never `sortAt`.

There IS a dormant mapping `{ source: sortAtUnix, target: sortAt, value_type: integer, fallback: sortAt, ms_to_seconds: true }` (deployment.yaml:261) and `time_buckets.filter_field: sortAtUnix` — but **no dump column or op ever emits a `sortAtUnix` field**, so this ingestion path is currently unused. Today `sortAt` is populated exclusively by the computed sort field. **This is the seam the redesign targets:** writing `Image.sortAt` at schedule time and ingesting it directly would flow through this already-wired-but-dormant `sortAtUnix → sortAt` mapping.

---

## 6. DeferredAliveConfig (src/config.rs:174–198)

- `source_field: String` — doc field holding activation timestamp (unix seconds). Future value ⇒ filter/sort bitmaps set immediately, alive bit deferred until timestamp arrives; flush thread activates due slots each cycle.
- `ms_to_seconds: bool` (default false) — divide source by 1000 if ms.
- `sweep_interval_secs: u64` (default 0 = disabled) — overdue-deferred safety-net sweep on WAL reader thread: queries alive slots whose `exists_boolean` shadow of `source_field` is still false, doc-checks timestamp is past, re-emits activation.
- `sweep_limit: usize` (default 20,000) — max candidate slots per sweep pass.

**Deployed prod value** (deployment.yaml:188–189):
```yaml
deferred_alive:
  source_field: publishedAt
```
Only `source_field: publishedAt` — so `ms_to_seconds=false`, **`sweep_interval_secs=0` (sweep DISABLED in prod)**, `sweep_limit=20000` default. Companion `activation_verify: { membership_field: postId }` is set. So today publish is a deferred-alive activation event keyed on Post's `publishedAt` arriving via the Post fan-out queryOpSet — exactly the orphan/verifier surface the sortAt-at-schedule-time redesign proposes to delete.
