---
status: ACTIVE
updated: 2026-03-28
---

# BitDex System Map

> **WARNING:** This system map is incomplete as of 2026-03-28. Missing: caching (unified_cache, doc_cache, bound_store), storage (shard_store, docstore, ops_wal), query execution (planner, executor, query_metrics), and dump processing modules. See CLAUDE.md Architecture Overview for the current complete picture.

Current system paths, showing where logic is duplicated.

```
<bitdex-server>
  <endpoints>
    <POST /documents/upsert>
      json_to_document_with_dicts()          ← FIELD MAPPING #1
        for each schema.field:
          resolve_raw (source → fallback)
          convert_field_with_dict             ← TYPE CONVERSION #1
            Integer, Boolean, MappedString, LCS, IntegerArray, ExistsBoolean
            ms_to_seconds applied here
          store under mapping.TARGET name
      engine.put(slot, doc)
        read old doc from docstore
        diff_document(old, new)              ← DIFF LOGIC
          for each filter field: compare old vs new → FilterInsert/FilterRemove
          for each sort field: compare old vs new → SortSet/SortClear
          AliveInsert
        send MutationOps to coalescer channel
        write doc to docstore channel
      flush thread
        drain channel
        apply to staging bitmaps
        cache maintenance
        ArcSwap publish

    <PATCH /documents/patch>
      json_to_document_with_dicts()          ← FIELD MAPPING #1 (same)
        (same conversion as upsert)
      engine.patch_document(slot, doc)
        if not alive → fall through to put_inner()
        read old doc from docstore
        diff_document_partial(old, new)      ← DIFF LOGIC (partial variant)
        send MutationOps to coalescer
        merge into stored doc

    <POST /documents/filter-sync>
      engine.sync_filter_values(slot, field, values)
        scan loaded bitmaps for old values
        diff old vs new → FilterInsert/FilterRemove
        send to coalescer

    <POST /load (NDJSON — existing)>
      for each line:
        json_to_document_with_dicts()        ← FIELD MAPPING #1 (same)
        engine.put(slot, doc)                ← same as upsert

    <GET /query?include_docs=true>
      format_document()                      ← FIELD MAPPING #2 (REVERSE)
        for each schema.field:
          lookup by mapping.target            ← tries target name first
          OR lookup by mapping.source         ← fallback for bulk-loaded data
          if from_source && ms_to_seconds:    ← CONDITIONAL conversion
            divide by 1000
          reverse MappedString int→string
        return JSON

  <engine internals>
    <flush thread>
      drain coalescer channel
      apply bitmap mutations to staging
      cache maintenance (time-based budget)
      ArcSwap::store(staging.clone())
      write docs from doc channel to DocStore

    <lazy loading>
      ensure_fields_loaded()
        check pending_filter_loads / pending_sort_loads
        load from BitmapFs on first query
        existence set for per-value loading

    <BitmapFs>
      filter/{field}/{bucket}.fpack
      sort/{field}.sort
      alive.roar
      slot_counter


<pg-sync load — STANDALONE BINARY>                    ← SEPARATE PROCESS
  download_all_tables()                                ← CSV download from PG
    COPY tags, images, tools, techniques, resources,
         posts, model_versions, models, collection_items

  run_single_pass_v2()
    <Step 1: Tags>
      process_tags_csv()                               ← CSV ADAPTER (tags-specific)
        mmap + rayon
        parse_tag_line                                 ← LINE PARSER #1
        insert into Vec<RoaringBitmap>[tag_id]
        append docstore tuple (tagId)
      save_filter_field_to_disk("tagIds")

    <Step 2: Images>
      load_post_map()                                  ← ENRICHMENT (manual HashMap)
      process_images_csv()                             ← CSV ADAPTER (images-specific)
        mmap + rayon
        per-line:
          parse 11 CSV columns                         ← LINE PARSER #2
          join with post_map                           ← ENRICHMENT JOIN
          HARDCODED FIELD MAPPING:                     ← FIELD MAPPING #3 ⚠️
            append_int!("nsfwLevel", nsfw_level)
            append_int!("userId", user_id)
            append_int!("publishedAt", pub_secs)       ← was "publishedAtUnix" in ms
            append_bool!("isPublished", pub_secs > 0)  ← was raw integer
            append_int!("sortAt", sort_at_secs)        ← was "sortAtUnix" redundant
            ... 15+ more hardcoded fields
          insert into filter HashMaps
          insert into sort HashMaps
          set alive bit
      save all filter fields to BitmapFs
      save all sort fields to BitmapFs
      save alive bitmap

    <Step 3: Resources>
      load_mv_map, load_model_map                      ← ENRICHMENT (manual)
      process_resources_csv()                          ← CSV ADAPTER (resources-specific)
        mmap + rayon
        HARDCODED: baseModel, modelVersionIds, poi     ← FIELD MAPPING #3 (cont.)
      save filter fields

    <Step 4: Tools>
      process_multi_value_csv("tools.csv", parse_tool_row, "toolIds")
        mmap + rayon                                   ← CSV ADAPTER (generic 2-col)
        parse_fn produces (value_id, slot_id)          ← LINE PARSER #3
        insert into HashMap + append docstore tuple
      save_filter_field_to_disk("toolIds")

    <Step 5: Techniques>
      process_multi_value_csv("techniques.csv", ...)   ← same generic adapter
      save_filter_field_to_disk("techniqueIds")

    <Step 6: Metrics>
      process_metrics_csv()                            ← CSV ADAPTER (metrics-specific)
        mmap + rayon
        HARDCODED sort field writes                    ← FIELD MAPPING #3 (cont.)
      save sort fields

    <Step 7: CollectionItems>
      process_collection_items_csv()                   ← CSV ADAPTER (collections)
        mmap + rayon
        parse_collection_line → (coll_id, image_id)    ← LINE PARSER #4
        insert into HashMap<u64, RoaringBitmap>
      save_filter_field_to_disk("collectionIds")

    save dictionaries
    save slot counter


<pg-sync sync — SIDECAR PROCESS>                      ← SEPARATE PROCESS
  <outbox poller>
    poll BitdexOutbox from PG
    deduplicate by entity_id
    fetch_and_push_upserts()
      fetch_images_by_ids()                            ← PG QUERY
      fetch_tags, tools, techniques, resources         ← PG ENRICHMENT QUERIES
      fetch_collections                                ← PG ENRICHMENT QUERY
      assemble_document()                              ← FIELD MAPPING #4 ⚠️
        HARDCODED:
          json!({ "nsfwLevel": image.nsfw_level,       ← manual field assembly
                   "sortAt": sort_at_secs,
                   "publishedAtUnix": pub_ms,          ← SOURCE name, not target
                   ... })
        manual hasMeta, onSite, poi computation
      PATCH to BitDex server
        → goes through PATCH endpoint → FIELD MAPPING #1
      filter_sync for collectionIds
        → goes through filter-sync endpoint

  <metrics poller>
    fetch from ClickHouse
    PATCH sort field values to BitDex server
```

## Where We Repeat Ourselves

```
FIELD MAPPING (4 independent implementations):
  #1  json_to_document_with_dicts     — schema-driven, stores target names     ✅ correct
  #2  format_document                 — reverse mapping with source fallback   ⚠️ patched
  #3  single_pass hardcoded macros    — manual per-field, stored source names  ⚠️ bug source
  #4  row_assembler assemble_document — manual JSON construction               ⚠️ source names

TYPE CONVERSION (3 implementations):
  convert_field_with_dict()           — ms_to_seconds, MappedString, LCS       ✅
  single_pass append_int!/append_bool!— manual per-field                       ⚠️ was wrong
  format_document from_source check   — conditional reverse conversion          ⚠️ patched

CSV ADAPTERS (5 specific + 1 generic):
  process_tags_csv                    — Vec[300K] direct index, tags-specific
  process_images_csv                  — 11-col entity with enrichment joins
  process_resources_csv               — 3-col with model enrichment
  process_metrics_csv                 — ClickHouse format, sort-only
  process_collection_items_csv        — 2-col, HashMap (in backfill module)
  process_multi_value_csv             — generic 2-col (tools, techniques)

LINE PARSERS (4 implementations):
  parse_tag_line                      — comma-split, 2 ints
  parse_tool_row / parse_technique_row— imported from copy_queries
  parse_collection_line               — comma-split, 2 ints, with validation
  parse_image_row (11 cols)           — complex, handles quoted fields
```

## What the Shared Mapper Collapses

```
BEFORE: 4 field mapping paths, 3 type conversion paths

AFTER:
  map_raw_to_target()                 — ONE function, schema-driven
    called by: CSV workers, JSON upsert, PATCH, row_assembler
    handles: source→target, ms_to_seconds, ExistsBoolean, MappedString, LCS
    always stores under TARGET name with conversions applied

  format_document()                   — simplified
    just reads target names, no fallback, no conditional conversion
```

---

## Refined System

```
<bitdex-server>

  <shared core>
    <field_mapper>                                     ← ONE implementation
      map_raw_to_target(source_name, raw_value, schema)
        resolve source → target name
        apply type conversion (ms_to_seconds, ExistsBoolean, MappedString, LCS)
        return (target_name, FieldValue, filter_only, doc_only)

    <ingester — generic over BitmapSink>               ← ALREADY EXISTS, now used everywhere
      filter_insert(field, value, slot)
      sort_set(field, bit_layer, slot)
      alive_insert(slot)
      doc_append(slot, field_idx, value)
      flush()

    <sinks>
      AccumSink                                        ← bulk: collect into HashMap, no diff
      CoalescerSink                                    ← online: send ops to flush thread

    <BitmapFs>                                         ← unchanged
      save_filter_field_to_disk
      save_sort_field_to_disk
      reload_existence_set

  <endpoints>

    <POST /load>                                       ← NEW (replaces pg-sync load binary)
      parse LoadRequest (format, sources, columns)
      spawn blocking task
      for each source:
        <source adapter>
          csv_two_column(path, field)                   ← tags, tools, techniques, collections
          csv_entity(path, enrichment_paths)             ← images + post/resource/model enrichment
          csv_metrics(path)                              ← ClickHouse sort fields
        <rayon workers>
          mmap file, split into chunks
          each worker:
            parse line → raw fields                     ← source adapter does this
            map_raw_to_target(raw, schema)              ← SHARED MAPPER
            ingester.filter_insert / sort_set / alive   ← SHARED INGESTER w/ AccumSink
            ingester.doc_append (unless filter_only)
        <after rayon join>
          save_filter_field_to_disk(AccumSink.maps)
          save_sort_field_to_disk(AccumSink.sorts)
          save alive bitmap
          reload_existence_set
      mark task complete

    <POST /documents/upsert>                           ← simplified
      parse JSON body
      for each field in JSON:
        map_raw_to_target(field, value, schema)         ← SHARED MAPPER
      build Document (all target names, converted)
      engine.put(slot, doc)
        read old doc → diff → MutationOps              ← diff uses target names (match)
        ingester w/ CoalescerSink                       ← SHARED INGESTER
        write doc to DocStore

    <PATCH /documents/patch>                           ← simplified
      parse partial JSON body
      for each provided field:
        map_raw_to_target(field, value, schema)         ← SHARED MAPPER
      engine.patch_document(slot, doc)
        if not alive → put_inner (same as upsert)
        read old doc → partial diff
        ingester w/ CoalescerSink                       ← SHARED INGESTER
        merge into stored doc

    <POST /documents/filter-sync>                      ← unchanged
      engine.sync_filter_values(slot, field, values)

    <DELETE /documents>                                ← unchanged
      engine.delete(slot)

    <GET /query?include_docs=true>
      format_document()                                ← SIMPLIFIED
        for each schema.field:
          doc.fields.get(mapping.target)                ← just target name, no fallback
          field_value_to_json(fv)                       ← no ms_to_seconds, no from_source
          or default_json_for_field(mapping)

  <engine internals>                                   ← unchanged
    <flush thread>
      drain coalescer channel
      apply mutations to staging
      cache maintenance
      ArcSwap publish
    <lazy loading>
      ensure_fields_loaded from BitmapFs
    <DocStore V2>
      append-only tuple logs


<pg-sync sync — SIDECAR>                               ← simplified, sync-only
  <outbox poller>
    poll BitdexOutbox
    deduplicate
    fetch full doc from PG
    assemble_document()                                ← USES SHARED MAPPER
      for each PG row field:
        map_raw_to_target(source, value, schema)        ← same mapper as everything else
      build JSON with target names
    PATCH to BitDex server                             ← flows through PATCH endpoint above
    filter_sync for filter_only fields

  <metrics poller>                                     ← unchanged
    fetch from ClickHouse
    PATCH sort values to server


NO LONGER EXISTS:
  ✗ pg-sync load binary
  ✗ single_pass standalone entry point
  ✗ Standalone K8s loader Job
  ✗ Auto-backfill in pg-sync
  ✗ Hardcoded field mapping in single_pass
  ✗ Manual JSON assembly in row_assembler
  ✗ format_document source-name fallback
  ✗ format_document from_source conditional conversion
```

## Side-by-Side: Duplication Eliminated

```
                        BEFORE                          AFTER

Field mapping:          4 implementations               1 (map_raw_to_target)
Type conversion:        3 implementations               1 (inside map_raw_to_target)
Bitmap insertion:       2 paths (direct + coalescer)    1 trait (Ingester<BitmapSink>)
Doc serving:            complex (fallback + conditional) simple (read target, return)
Loader entry points:    2 (CLI binary + HTTP NDJSON)    1 (POST /load)
Processes that load:    2 (pg-sync load + server)       1 (server only)
Field name source:      mixed (source + target)         target only (everywhere)
```
