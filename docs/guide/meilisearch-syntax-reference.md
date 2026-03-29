# Meilisearch Filter/Sort Syntax Reference

Reference for the Meilisearch query parser plugin. This documents the subset of Meilisearch syntax that maps to BitDex capabilities.

## Filter Syntax

Meilisearch filters are string expressions (or array-of-strings with implicit AND/OR).

### Operators

| Expression | Meaning | BitDex mapping |
|---|---|---|
| `field = value` | Equality | `Eq(field, value)` |
| `field != value` | Not equal | `NotEq(field, value)` |
| `field > value` | Greater than | `Gt(field, value)` |
| `field >= value` | Greater than or equal | `Gte(field, value)` |
| `field < value` | Less than | `Lt(field, value)` |
| `field <= value` | Less than or equal | `Lte(field, value)` |
| `field min TO max` | Range (inclusive) | `And([Gte(field, min), Lte(field, max)])` |
| `field IN [v1, v2]` | Multi-value | `In(field, [v1, v2])` |
| `field NOT IN [v1, v2]` | Not in set | `NotIn(field, [v1, v2])` |
| `NOT expr` | Negation | `Not(expr)` |
| `expr AND expr` | Intersection | `And([expr, expr])` |
| `expr OR expr` | Union | `Or([expr, expr])` |
| `(expr)` | Grouping | Precedence override |

### Precedence

1. `NOT` (highest)
2. `AND`
3. `OR` (lowest)

Parentheses override precedence.

### Value Types

- **Numbers**: `42`, `3.14`, `-1` — parsed as Integer or Float
- **Strings**: unquoted single-word (`horror`) or single-quoted (`'Tim Burton'`)
- **Booleans**: `true`, `false`

### Array Format (alternative)

```json
{
  "filter": [["genres = horror", "genres = comedy"], "director = 'Jordan Peele'"]
}
```

Outer array = AND, inner arrays = OR. Max nesting depth: 2.

### Not Mapped (out of scope for BitDex)

- `EXISTS` / `NOT EXISTS` — BitDex tracks alive slots, not field presence
- `IS NULL` / `IS NOT NULL` — no null concept in bitmap index
- `IS EMPTY` / `IS NOT EMPTY` — no empty concept
- `_geoRadius()` / `_geoBoundingBox()` — no geo support
- Nested field dot notation — BitDex fields are flat

## Sort Syntax

Array of `field:direction` strings:

```json
{
  "sort": ["reactionCount:desc", "publishedAtUnix:asc"]
}
```

BitDex only supports single-field sort, so only the first element is used.

## Search Parameters → BitDex Mapping

| Meilisearch | BitDex | Notes |
|---|---|---|
| `filter` | `filters` | String parsed → FilterClause tree |
| `sort` | `sort` | First element only |
| `limit` | `limit` | Default 20, max 10000 |
| `offset` | `offset` | Offset pagination |
| `q` | — | Full-text search not in BitDex scope |
| `facets` | — | Future work |
| `page` / `hitsPerPage` | Converted to offset/limit | `offset = (page - 1) * hitsPerPage` |

## Full Example

Meilisearch request:
```json
{
  "filter": "nsfwLevel = 1 AND tagIds IN [12, 45, 678] AND type = 'image' AND publishedAtUnix >= 1700000000",
  "sort": ["reactionCount:desc"],
  "limit": 50,
  "offset": 0
}
```

Parsed to BitDex:
```rust
BitdexQuery {
    filters: vec![
        And(vec![
            Eq("nsfwLevel", Integer(1)),
            In("tagIds", [Integer(12), Integer(45), Integer(678)]),
            Eq("type", String("image")),
            Gte("publishedAtUnix", Integer(1700000000)),
        ])
    ],
    sort: Some(SortClause { field: "reactionCount", direction: Desc }),
    limit: 50,
    offset: Some(0),
    cursor: None,
}
```
