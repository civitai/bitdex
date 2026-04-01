# Filter Field Type Refactor

**Status:** Proposed
**Date:** 2026-04-01

## Problem

`FilterFieldConfig` currently has a `field_type` property (`single_value`, `multi_value`, `boolean`) that is redundant with `data_schema`'s `value_type`:

| data_schema value_type | Implied filter field_type |
|----------------------|--------------------------|
| `integer` | `single_value` |
| `low_cardinality_string` | `single_value` |
| `boolean` | `boolean` |
| `exists_boolean` | `boolean` |
| `integer_array` | `multi_value` |
| `mapped_string` | `single_value` |

The data_schema is the source of truth for field semantics. Filter field config should only contain indexing behavior: `eager_load`, `per_value_lazy`, `eviction`.

## Current State

```yaml
# filter_fields has redundant field_type
filter_fields:
  - { name: nsfwLevel, field_type: single_value, eager_load: true }
  - { name: tagIds, field_type: multi_value }

# data_schema already has value_type
data_schema:
  fields:
    - { source: nsfwLevel, target: nsfwLevel, value_type: integer }
    - { source: tagIds, target: tagIds, value_type: integer_array }
```

## Proposed State

```yaml
# filter_fields: indexing behavior only
filter_fields:
  - { name: nsfwLevel, eager_load: true }
  - { name: tagIds }

# data_schema: source of truth for types, nullability, defaults
data_schema:
  fields:
    - { source: nsfwLevel, target: nsfwLevel, value_type: integer }
    - { source: tagIds, target: tagIds, value_type: integer_array }
```

## Migration Plan

1. Add `Config::derive_filter_type(field_name) -> FilterFieldType` that looks up data_schema
2. Make `field_type` optional on `FilterFieldConfig` with `#[serde(default)]`
3. When `field_type` is omitted, derive from data_schema
4. When `field_type` is present, validate it matches data_schema (warn on mismatch)
5. After all configs updated, remove `field_type` from `FilterFieldConfig`

## Impact

- ~15 files with `FilterFieldConfig` struct literals need updating
- All YAML configs need `field_type` removed from filter_fields
- Tests need updating
- Config parser needs the derivation logic

## Why Not Now

This is a config cleanup, not a correctness fix. The current redundancy works — it's just unnecessary duplication that could drift. Prioritize after the nullable/null-bitmap work is stable.
