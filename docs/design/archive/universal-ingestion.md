# Universal Ingestion: Pluggable Source Adapters

> Every data source — CSV, NDJSON, PG COPY, HTTP, webhook, stream — reduces to the same thing: `(slot, field, value)` BitTuples routed through an `Ingester<B>`. The format parsing and the ingestion mode are orthogonal. You pick a source adapter and an ingestion mode independently.

## Status: DESIGN SKETCH — not yet implemented

## The Architecture

```
Source Adapters              Ingester Core              Bitmap Engine
(pluggable front-ends)       (universal)                (always the same)
──────────────────           ──────────                 ──────────────

CSV files ──→ CsvAdapter ─┐
NDJSON    ──→ NdjsonAdapter┤
PG COPY   ──→ PgCopyAdapter┤→ (slot, field, value) ──→ Ingester<B>
HTTP PUT  ──→ HttpAdapter ─┤   BitTuples                ├→ BitmapSink
Webhook   ──→ WebhookAdapt─┤                            └→ DocSink
Stream    ──→ StreamAdapter┘

                             Two ingestion modes:
                             ├─ AccumSink (bulk)
                             └─ CoalescerSink (live)
```

## Two Decisions Per Source

Every adapter makes exactly two choices:

1. **Which `BitmapSink`?**
   - `AccumSink` — bulk mode: loading mode, no readers, 300K+ docs/sec
   - `CoalescerSink` — live mode: readers stay active, low-latency upserts

2. **Needs diff?**
   - Fresh inserts (bulk load): no diff, just set bits
   - Upserts (live): read old doc, diff, clear old bits + set new bits

Everything else — docstore appends, LIFO dedup, compaction — is handled by the core.

## Config-Driven Source Definition

Instead of hardcoding CSV column mappings in Rust, define them in the schema config:

```yaml
sources:
  # Primary source: defines the slots (one row = one document)
  - type: csv
    file: images.csv
    role: primary
    id_column: id
    columns:
      nsfwLevel: { target: nsfwLevel }
      userId: { target: userId }
      sortAt: { target: sortAt, transform: epoch_seconds }
      type: { target: type, transform: mapped_string }
      url: { target: url, doc_only: true }
      hash: { target: hash, doc_only: true }

  # Join source: foreign key maps rows to slots from the primary
  - type: csv
    file: tags.csv
    role: join
    join_column: imageId
    columns:
      tagId: { target: tagIds, multi_value: true }

  - type: csv
    file: resources.csv
    role: join
    join_column: imageId
    columns:
      modelVersionId: { target: modelVersionIds, multi_value: true }
      baseModel: { target: baseModel, transform: mapped_string }

  # NDJSON source: each line is a full document
  - type: ndjson
    file: images.ndjson
    role: primary
    id_field: id
    # columns inferred from DataSchema.fields mapping
```

### Source Roles

- **primary** — this source defines the slot IDs. One row/line = one document. The `id_column`/`id_field` value IS the slot.
- **join** — this source adds fields to existing documents. The `join_column` is a foreign key that maps to a slot from the primary source. Many rows can map to the same slot (e.g., tags).
- **enrichment** — lookup tables that don't directly create BitTuples but provide values needed by other sources (e.g., post → availability mapping).

## What Exists Today vs What Changes

### Already built (just needs generalization)

| Component | Current (Civitai-specific) | Generic version |
|-----------|--------------------------|-----------------|
| CSV parsing | `parse_image_row`, `parse_tag_row`, etc. | Config-driven: column index → field target |
| NDJSON parsing | `load_ndjson` in loader.rs | Config-driven: JSON path → field target |
| Bitmap building | `BitmapAccum` with manual HashMap inserts | `Ingester<AccumSink>` |
| Doc writing | `append_tuple_raw` | `DocSink.append()` |
| Field schema | `DataSchema.fields` with FieldMapping | Already generic — just needs source config on top |

### New pieces needed

1. **Source config schema** — the YAML above, parsed alongside DataSchema
2. **Generic CSV adapter** — reads any CSV, maps columns based on config
3. **Generic NDJSON adapter** — reads any NDJSON, maps fields based on config
4. **CLI integration** — `bitdex load --source images.csv --mode bulk`

## Ingestion Modes

The format adapter and the ingestion mode are orthogonal:

```
bitdex load --source images.csv --mode bulk      # AccumSink, loading mode
bitdex load --source images.csv --mode live      # CoalescerSink, readers active
bitdex load --source data.ndjson --mode bulk     # same engine, different parser
```

### Bulk mode (`AccumSink`)
- Calls `enter_loading_mode()` — no snapshots published, no cache maintenance
- Thread-local bitmap accumulators via rayon
- After all sources processed: merge, apply to staging, `exit_loading_mode()`
- Best for initial loads, full rebuilds, large imports

### Live mode (`CoalescerSink`)
- Readers stay active, snapshots keep publishing
- Each document goes through `diff_document()` → `MutationOp`s → coalescer channel
- Best for incremental updates, streaming, real-time sync

### Server mode (HTTP)
- Already exists: PUT /documents/upsert uses CoalescerSink pattern
- Could accept `Content-Type: text/csv` or `application/x-ndjson` for bulk HTTP ingestion
- Request header or query param selects mode: `?mode=bulk` vs `?mode=live`

## Join Resolution

The trickiest part of multi-CSV loading is join resolution: when `tags.csv` says `imageId=12345, tagId=67`, how do we know slot 12345 exists?

**Current approach:** The primary CSV (images) must be loaded first. It registers all slot IDs. Join CSVs are loaded after, and rows with unknown join keys are skipped.

**Generic approach:** Same, but config-driven:
1. Process sources in dependency order: primary first, then joins
2. The source config declares the order (or we infer: primary before joins)
3. The slot registry (alive bitmap) is the source of truth — if the slot isn't alive, skip the join row

## What This Replaces

Today's `single_pass.rs` has:
- Hardcoded CSV file names ("tags.csv", "images.csv")
- Hardcoded row parsers (`parse_image_row`, `parse_tag_row`)
- Hardcoded processing order
- Hardcoded field-to-bitmap mappings

All of this becomes config. The Rust code becomes a generic "read rows, map columns, emit BitTuples" loop that works for any dataset.

## Relationship to Existing Design Docs

- **ingester.md** — defines the `BitmapSink`/`DocSink`/`Ingester<B>` traits (the plumbing). This doc defines how those traits get called (the adapters).
- **pipeline-architecture.md** — describes the connected-pools vision. Source adapters feed the pipeline's Parse stage.
- **janitor.md** — compaction happens automatically regardless of source. Reader-triggered, no adapter changes needed.
