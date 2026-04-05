# BitDex Auxiliary Indexes — Cascading One-to-Many Sync Design

**Status:** Draft — for Justin's review
**Date:** 2026-04-02
**Author:** Josh (with GPT-5.4 + Gemini 3.1 Pro brainstorming)

---

## Problem Statement

BitDex indexes ~108M images. Some image fields are **derived from related entities** through join tables:

- `Image.baseModel` from `ModelVersion.baseModel` (only Checkpoint type), linked via `ImageResourceNew`
- `Image.poi` boolean OR of `Model.poi` for all linked models, linked via `ImageResourceNew -> ModelVersion -> Model`

**Today's gap:** When `ImageResourceNew` links/unlinks an image to a model version, the `modelVersionIds` bitmap is updated, but derived fields (`baseModel`, `poi`) are NOT cascaded. The fan-out triggers only fire when the **source entity** (Model, ModelVersion) changes -- not when the **linkage** changes.

**DELETE is the hard case:** When an image is unlinked from a model version, we need to know the *remaining* state to re-evaluate baseModel and poi. This requires either a DB re-query or engine-side state.

---

## Recommended Architecture: Auxiliary Indexes

### Core Idea

Promote `Model` and `ModelVersion` from "transient fan-out targets" to **first-class auxiliary entities** with their own lightweight state inside BitDex. The image index references them during write processing.

### What changes

| Component | Today | Proposed |
|-----------|-------|----------|
| Model/MV data in BitDex | None (fan-out ops carry values) | Auxiliary index: `HashMap<Id, SmallDoc>` |
| Fan-out triggers | `queryOpSet` with query string | Update auxiliary index, engine resolves affected images internally |
| ImageResourceNew trigger | Adds `modelVersionIds` only | Also triggers derived field recomputation via cross-index lookup |
| poi semantics | Single field, last-write-wins | `poiSelf` (Image flags) OR `poiModelDerived` (linked models) |
| baseModel on DELETE | Undefined (stale value remains) | Recompute from remaining linked MVs |

### Memory overhead

- ~4M ModelVersions x ~20 bytes = **~80 MB**
- ~1M Models x ~8 bytes = **~8 MB**
- Reverse index (MV to images): already exists as `modelVersionIds` bitmaps
- Reverse index (Model to MVs): `HashMap<ModelId, Vec<MvId>>` ~16 MB

**Total: ~100 MB** -- negligible vs 6.5 GB bitmap memory

---

## Config Schema

### Auxiliary entities (new section)

```yaml
auxiliary_entities:
  - name: model_version
    id_field: id
    fields:
      - { name: baseModel, type: string }
      - { name: modelId, type: integer }
      - { name: type, type: string }

  - name: model
    id_field: id
    fields:
      - { name: poi, type: boolean }
```

### Derived fields (new section)

```yaml
derived_fields:
  - target: baseModel
    on_entity: image
    source_relation:
      join_field: modelVersionIds
      auxiliary: model_version
    filter:
      field: type
      equals: "Checkpoint"
    project: baseModel
    aggregation:
      kind: pick_one
      strategy: max_id
      on_empty: clear

  - target: poiModelDerived
    on_entity: image
    source_relation:
      join_field: modelVersionIds
      auxiliary: model_version
      via:
        field: modelId
        auxiliary: model
    project: poi
    aggregation:
      kind: any_true
      on_empty: false

composed_fields:
  - name: poi
    type: boolean
    compose: or
    sources:
      - field: poiSelf
      - field: poiModelDerived
```

### Trigger changes

```yaml
triggers:
  - table: ImageResourceNew
    slot_field: imageId
    field: modelVersionIds
    value_field: modelVersionId
    # Engine auto-triggers derived field recomputation

  - table: Model
    type: auxiliary_update
    auxiliary: model
    track_fields: [poi]

  - table: ModelVersion
    type: auxiliary_update
    auxiliary: model_version
    track_fields: [baseModel, type]
```

---

## Processing Flows

### 1. ImageResourceNew INSERT (link added)

```
PG trigger: {"op":"add", "field":"modelVersionIds", "value":98765, "slot":imageId}
    |
Ops processor: add 98765 to image's modelVersionIds bitmap
    |
Derived field engine: modelVersionIds changed for this image
  -> Look up MV 98765 in auxiliary -> {baseModel:"SDXL", type:"Checkpoint", modelId:42}
  -> Look up Model 42 in auxiliary -> {poi:true}
  -> Recompute baseModel: pick highest Checkpoint MV -> set baseModel="SDXL"
  -> Recompute poiModelDerived: any linked model poi=true -> true
  -> Compose: poi = poiSelf OR poiModelDerived
```

### 2. ImageResourceNew DELETE (link removed)

```
PG trigger: {"op":"remove", "field":"modelVersionIds", "value":98765, "slot":imageId}
    |
Ops processor: remove 98765 from image's modelVersionIds bitmap
    |
Derived field engine: modelVersionIds changed for this image
  -> Get remaining MVs for this image (from docstore)
  -> For each remaining MV, look up auxiliary index
  -> Recompute baseModel from remaining Checkpoint MVs
  -> Recompute poiModelDerived from remaining models
  -> If no remaining Checkpoint MVs -> clear baseModel
  -> If no remaining poi=true models -> clear poiModelDerived
```

### 3. Model.poi changes (auxiliary update)

```
PG trigger: {"op":"auxiliary_update", "entity":"model", "id":42, "field":"poi", "value":true}
    |
Update auxiliary index: model[42].poi = true
    |
Reverse resolution:
  -> Find MVs for model 42: model_versions_by_model[42] -> [98765, 98766, ...]
  -> Find images for each MV: modelVersionIds bitmap -> [img1, img2, ...]
  -> For each affected image, recompute poiModelDerived
```

### 4. ModelVersion.baseModel changes (auxiliary update)

```
PG trigger: {"op":"auxiliary_update", "entity":"model_version", "id":98765, ...}
    |
Update auxiliary index -> find images for MV 98765 -> recompute baseModel
```

---

## Bulk Dump Changes

```
1. images (sets alive, direct fields, poiSelf from flags)
2. models.csv -> populate model auxiliary index
3. model_versions.csv -> populate model_version auxiliary index
4. resources (ImageResourceNew) -> add modelVersionIds + derived recomputation
5. tags, tools, techniques (unchanged)
6. metrics (unchanged)
7. Final pass: recompute all derived fields (safety net)
```

Steps 2-3 are already loaded as enrichment lookups -- they just persist into the auxiliary index instead of being dropped.

---

## What This Eliminates

- **queryOpSet entirely** -- no more query string parsing for fan-out
- **Null query bug** -- no query strings to be null
- **Model fan-out trigger's json_agg** -- Model trigger just emits field changes
- **Stale baseModel on unlink** -- deterministic recomputation
- **poi clobbering** -- separated into poiSelf + poiModelDerived

## What This Adds

- ~100 MB RAM for auxiliary indexes
- New `auxiliary_update` op type
- Derived field recomputation engine in ops_processor
- Reverse index maintenance (Model to MVs)
- Config complexity: `auxiliary_entities`, `derived_fields`, `composed_fields`

---

## Migration Path

1. **Phase 1** (PR #122): Null query fix unblocks ops now
2. **Phase 2**: Add auxiliary index infrastructure + reverse indexes
3. **Phase 3**: Implement derived field engine with `any_true` and `pick_one`
4. **Phase 4**: Migrate Model/MV triggers from `queryOpSet` to `auxiliary_update`
5. **Phase 5**: Add `composed_fields` for `poi = poiSelf OR poiModelDerived`
6. **Phase 6**: Remove queryOpSet code path (or keep as fallback)

---

## Open Questions

1. **Recomputation scope on auxiliary update**: When Model.poi changes, potentially millions of images are affected. Batch? Rate-limit? Async?
2. **Deterministic baseModel**: Is `max(modelVersionId)` the right pick-one strategy?
3. **Image-level poi vs model-level poi**: Does the app update Image.flags bit 4 when Model.poi changes? If so, poiModelDerived might be redundant.
4. **Forward index for DELETE**: Docstore stores modelVersionIds per doc -- sufficient or need dedicated in-memory forward index?
5. **Other future cascades**: Collection membership, user-level flags?
