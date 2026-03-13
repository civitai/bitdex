# Query Formats

BitDex supports three query formats. All produce identical results — they differ only in syntax. Select per-request with `?format=`, or set a server-wide default.

## Format Selection

| Method | Scope | Example |
|--------|-------|---------|
| `?format=` query param | Per-request (highest priority) | `POST /api/indexes/my-index/query?format=compact` |
| `--default-format` CLI flag | Server boot | `bitdex-server --default-format compact` |
| `default_query_format` in TOML | Config file | `default_query_format = "compact"` |

Priority: query param > CLI flag > TOML > built-in default ("bitdex").

List available formats: `GET /api/formats`

```json
{ "formats": ["bitdex", "compact", "meilisearch"], "default": "bitdex" }
```

---

## 1. BitDex (default)

The original format. Explicit typed values, serde-based deserialization.

```json
{
  "filters": [
    { "And": [
      { "Eq": ["nsfwLevel", { "Integer": 1 }] },
      { "In": ["tagIds", [{ "Integer": 12 }, { "Integer": 45 }]] },
      { "Eq": ["type", { "String": "image" }] },
      { "Gte": ["publishedAtUnix", { "Integer": 1700000000 }] }
    ]}
  ],
  "sort": { "field": "reactionCount", "direction": "Desc" },
  "limit": 50,
  "cursor": { "sort_value": 342, "slot_id": 7042 },
  "offset": null,
  "include_docs": false
}
```

### Filter operators

| Operator | Format |
|----------|--------|
| `Eq` | `{ "Eq": ["field", value] }` |
| `NotEq` | `{ "NotEq": ["field", value] }` |
| `In` | `{ "In": ["field", [value, ...]] }` |
| `NotIn` | `{ "NotIn": ["field", [value, ...]] }` |
| `Gt`, `Gte`, `Lt`, `Lte` | `{ "Gt": ["field", value] }` |
| `And` | `{ "And": [clause, ...] }` |
| `Or` | `{ "Or": [clause, ...] }` |
| `Not` | `{ "Not": clause }` |

Values are typed: `{ "Integer": 42 }`, `{ "Float": 3.14 }`, `{ "Bool": true }`, `{ "String": "foo" }`.

### Pagination

- **Cursor**: `"cursor": { "sort_value": 342, "slot_id": 7042 }`
- **Offset**: `"offset": 40`

When both are set, cursor takes precedence.

### When to use

Rust/TypeScript clients with generated types, or when exact type control matters.

---

## 2. Compact (MongoDB-style)

Field names as keys, operators inferred from value shape. ~45% fewer tokens than BitDex format.

```json
{
  "filter": {
    "nsfwLevel": 1,
    "tagIds": [12, 45],
    "type": "image",
    "publishedAtUnix": { "$gte": 1700000000 }
  },
  "sort": "-reactionCount",
  "limit": 50,
  "cursor": "342:7042",
  "offset": null,
  "include_docs": false
}
```

### Filter rules

| Value shape | Meaning |
|-------------|---------|
| Scalar (`1`, `true`, `"image"`) | Equality |
| Array (`[12, 45]`) | In (multi-value) |
| `{ "$gt": v }` | Greater than |
| `{ "$gte": v }` | Greater than or equal |
| `{ "$lt": v }` | Less than |
| `{ "$lte": v }` | Less than or equal |
| `{ "$ne": v }` | Not equal |
| `{ "$in": [v, ...] }` | Explicit In |
| `{ "$nin": [v, ...] }` | Not in |
| `{ "$gte": a, "$lt": b }` | Combined range (AND) |

Multiple top-level keys are implicit AND.

### Boolean logic

```json
{
  "filter": {
    "$or": [{ "nsfwLevel": 1 }, { "nsfwLevel": 2 }],
    "type": { "$ne": "video" }
  }
}
```

`$or`, `$and`, `$not` can appear at any nesting level alongside field filters.

### Sort

`"-reactionCount"` = descending, `"reactionCount"` = ascending.

### Pagination

- **Cursor**: `"cursor": "342:7042"` (sort_value:slot_id as string)
- **Offset**: `"offset": 40`

### When to use

AI agents, scripts, quick manual queries. The common case (equality + sort) is minimal syntax.

---

## 3. Meilisearch

String-based filter DSL matching [Meilisearch's filter syntax](https://www.meilisearch.com/docs/learn/filtering_and_sorting/filter_expression_reference). Useful for teams migrating from Meilisearch.

```json
{
  "filter": "nsfwLevel = 1 AND tagIds IN [12, 45] AND type = 'image' AND publishedAtUnix >= 1700000000",
  "sort": ["reactionCount:desc"],
  "limit": 50,
  "offset": 0
}
```

### Filter syntax

```
field = value              Equality
field != value             Not equal
field > value              Greater than
field >= value             Greater than or equal
field < value              Less than
field <= value             Less than or equal
field IN [v1, v2]          Multi-value
field NOT IN [v1, v2]      Not in set
field min TO max           Inclusive range
NOT expr                   Negation
expr AND expr              Intersection
expr OR expr               Union
(expr)                     Grouping
```

**Precedence**: NOT > AND > OR. Parentheses override.

**Values**: unquoted words (`image`), single-quoted strings (`'Tim Burton'`), double-quoted strings (`"foo"`), numbers (`42`, `3.14`, `-5`), booleans (`true`, `false`).

### Array filter format (alternative)

Outer array = AND, inner arrays = OR:

```json
{
  "filter": [["type = image", "type = video"], "nsfwLevel = 1"]
}
```

Equivalent to: `(type = image OR type = video) AND nsfwLevel = 1`

### Sort

Array of `field:direction` strings. Only the first element is used (BitDex supports single-field sort).

```json
{ "sort": ["reactionCount:desc"] }
```

### Pagination

- **Offset/limit**: `"offset": 40, "limit": 20`
- **Page/hitsPerPage**: `"page": 3, "hitsPerPage": 20` → converted to offset 40, limit 20

These are mutually exclusive. No cursor pagination (Meilisearch uses offset).

### Not supported

These Meilisearch features have no BitDex equivalent:

- `q` (full-text search)
- `EXISTS` / `IS NULL` / `IS EMPTY`
- `_geoRadius()` / `_geoBoundingBox()`
- Nested field dot notation
- `facets` / `distinct`

### When to use

Teams migrating from Meilisearch, or anyone who prefers a readable string DSL.

---

## Side-by-side Comparison

The same query in all three formats:

**"SFW images with tag 12 or 45, sorted by reactions descending, page size 50"**

### BitDex

```json
{
  "filters": [{ "And": [
    { "Eq": ["nsfwLevel", { "Integer": 1 }] },
    { "In": ["tagIds", [{ "Integer": 12 }, { "Integer": 45 }]] },
    { "Eq": ["type", { "Integer": 1 }] }
  ]}],
  "sort": { "field": "reactionCount", "direction": "Desc" },
  "limit": 50
}
```

### Compact

```json
{
  "filter": { "nsfwLevel": 1, "tagIds": [12, 45], "type": 1 },
  "sort": "-reactionCount",
  "limit": 50
}
```

### Meilisearch

```json
{
  "filter": "nsfwLevel = 1 AND tagIds IN [12, 45] AND type = 1",
  "sort": ["reactionCount:desc"],
  "limit": 50
}
```

---

## Response Format

All formats return the same response:

```json
{
  "ids": [948271, 831044, 720193],
  "cursor": { "sort_value": 15842, "slot_id": 720193 },
  "total_matched": 3847291,
  "elapsed_us": 1423
}
```

The `include_docs` field works the same across all formats. For non-bitdex formats, include it as a top-level field in the request JSON.
