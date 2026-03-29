# Query Language Proposals

Designs for concise, AI-agent-friendly query syntaxes. Evaluated against:
conciseness (token count), AI reliability (LLMs generate correctly), human readability, and unambiguity.

## Current Syntax (baseline, ~85 tokens)

```json
{
  "filters": [
    {"field": "nsfwLevel", "op": "Eq", "value": 1},
    {"field": "tagIds", "op": "In", "value": [12, 45, 678]},
    {"field": "type", "op": "Eq", "value": "image"},
    {"field": "publishedAtUnix", "op": "Gte", "value": 1700000000}
  ],
  "sort": {"field": "reactionCount", "direction": "Desc"},
  "limit": 50,
  "offset": 0
}
```

---

## Winner: Field-Keyed JSON (MongoDB-style, ~45 tokens)

```json
{
  "filter": {
    "nsfwLevel": 1,
    "tagIds": [12, 45, 678],
    "type": "image",
    "publishedAtUnix": {"$gte": 1700000000}
  },
  "sort": "-reactionCount",
  "limit": 50,
  "offset": 0
}
```

### Rules

| Value shape | Meaning |
|---|---|
| `scalar` | Equality (`Eq`) |
| `[a, b, c]` | Multi-value (`In`) |
| `{"$gt": v}` | Greater than |
| `{"$gte": v}` | Greater than or equal |
| `{"$lt": v}` | Less than |
| `{"$lte": v}` | Less than or equal |
| `{"$ne": v}` | Not equal |
| `{"$nin": [a,b]}` | Not in |
| `{"$gt": a, "$lt": b}` | Combined range (AND) |
| `true` / `false` | Boolean equality |

**Sort**: `-field` = descending, `field` = ascending.

**Boolean logic**: top-level `filter` object is implicit AND. Use `$or` / `$and` for explicit logic:

```json
{
  "filter": {
    "$or": [
      {"nsfwLevel": 1},
      {"tagIds": [99]}
    ],
    "type": {"$ne": "video"},
    "publishedAtUnix": {"$gte": 1704067200, "$lt": 1735689600}
  },
  "sort": "-publishedAtUnix",
  "limit": 20
}
```

**Cursor pagination**: `"cursor": "sort_value:slot_id"` string.

### Why this wins

1. **AI reliability** — MongoDB `$`-prefix operators are ubiquitous in LLM training data. Models generate this correctly on first attempt.
2. **80/20 ergonomics** — equality (the common case) is just `"field": value`. No operator needed.
3. **40-50% fewer tokens** than current syntax.
4. **Stays JSON** — respects CLAUDE.md principle 8.
5. **Extensible** — new operators are new `$`-prefixed keys.

---

## Runner-up: Tuple-Array (~55 tokens)

```json
{
  "filter": [
    ["nsfwLevel", "=", 1],
    ["tagIds", "in", [12, 45, 678]],
    ["type", "=", "image"],
    ["publishedAtUnix", ">=", 1700000000]
  ],
  "sort": ["reactionCount", "desc"],
  "limit": 50
}
```

Operators: `=`, `!=`, `>`, `>=`, `<`, `<=`, `in`, `!in`. AND/OR via `{"or": [...]}` wrappers.

**Pros**: explicit operators, no inference needed. **Cons**: positional args — LLMs occasionally swap operator and value.

---

## Also considered: Predicate-Per-Key (Django-style)

```json
{
  "nsfwLevel": 1,
  "tagIds__in": [12, 45, 678],
  "type": "image",
  "publishedAtUnix__gte": 1700000000,
  "sort": "-reactionCount",
  "limit": 50
}
```

**Pros**: maximally flat, URL-query-string compatible. **Cons**: `sort`/`limit` are reserved keys (namespace collision), OR/AND nesting is awkward.

---

## Also considered: SQL-Fragment DSL

```
nsfwLevel = 1 AND tagIds IN (12, 45, 678) AND type = "image" AND publishedAtUnix >= 1700000000
```

**Pros**: most readable for humans. **Cons**: requires custom parser, quoting errors from LLMs, conflicts with JSON-only principle.
