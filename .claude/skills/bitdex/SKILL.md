---
name: bitdex
description: Interact with the BitDex bitmap index server — query, manage indexes, load data, check status, upsert/delete documents. Use when working with BitDex, testing queries, or managing the server.
argument-hint: "[command] [options]"
allowed-tools: Bash, Read, Write, Edit, Grep, Glob
---

# BitDex Server Skill

CLI: `node ~/.claude/skills/bitdex/cli.mjs <command> [options]`

BitDex runs on **port 3001** by default (set via `BITDEX_URL` env or `--port` flag).

## Quick Start

```bash
SKILL="node ~/.claude/skills/bitdex/cli.mjs"

# Check if server is running
$SKILL health

# Full status (health + index info + tasks)
$SKILL status

# Query with filters + sort
$SKILL query --filter '[{"Eq":["nsfwLevel",{"Integer":1}]}]' --sort reactionCount --dir Desc --limit 10

# Query with docs included
$SKILL query --filter '[{"Eq":["nsfwLevel",{"Integer":1}]}]' --sort reactionCount --limit 5 --include-docs

# Get a single document
$SKILL doc-get --id 948271

# Get index stats
$SKILL stats
```

## Starting the Server

If the server isn't running:
```bash
# Check first
$SKILL health

# Start command (run in a separate terminal)
cd C:\Dev\Repos\open-source\bitdex-v2
cargo run --release --features server --bin bitdex-server -- --port 3001 --data-dir ./data
```

With `--rebuild` to rebuild bitmaps from docstore:
```bash
cargo run --release --features server --bin bitdex-server -- --port 3001 --data-dir ./data --rebuild
```

## Commands

### Server
| Command | Description |
|---------|-------------|
| `health` | Check if server is reachable |
| `status` | Full status: health + all indexes + active tasks |
| `start` | Print the command to start the server |
| `metrics` | Prometheus metrics (raw text) |

### Index Management
| Command | Description |
|---------|-------------|
| `index-list` | List all indexes |
| `index-get [--index NAME]` | Get index config, schema, and stats |
| `index-create --name NAME --config-file PATH` | Create index from JSON config file |
| `index-delete --index NAME` | Delete an index |

### Querying
| Command | Description |
|---------|-------------|
| `query [opts]` | Filter + sort query (see below) |
| `query-raw --body JSON_OR_FILE` | Raw JSON query body |

### Documents
| Command | Description |
|---------|-------------|
| `doc-get --id SLOT_ID` | Get single document |
| `doc-batch --ids 1,2,3` | Get multiple documents |
| `upsert --docs JSON_ARRAY_OR_FILE` | Insert/update documents |
| `delete --ids 1,2,3` | Delete documents by ID |

### Operations
| Command | Description |
|---------|-------------|
| `stats` | Index stats (counts, cache, memory) |
| `traces [--last N]` | Recent query traces (clause timing, cache hits) |
| `cache-clear` | Clear the unified cache |
| `warm --body JSON_OR_FILE` | Pre-populate cache with specified queries |
| `snapshot` | Save bitmap snapshot to disk (blocks) |
| `rebuild [--sort-fields f1,f2] [--filter-fields f1,f2]` | Rebuild bitmaps from docstore |
| `fields-add --body JSON` | Hot-add new fields |
| `fields-remove --body JSON` | Remove fields |
| `load --path /path/to/file.ndjson [--follow]` | Load NDJSON data |

### Tasks
| Command | Description |
|---------|-------------|
| `task TASK_ID [--follow]` | Get task status (or poll until done) |
| `tasks` | List active + recent tasks |

### Cursors
| Command | Description |
|---------|-------------|
| `cursor-list` | List named cursors |
| `cursor-get NAME` | Get cursor value |

## Query Syntax

### Filter Operators

Filters use serde externally-tagged enum format (objects, NOT arrays):

```
Eq:    {"Eq": ["field", {"Integer": 1}]}
NotEq: {"NotEq": ["field", {"Integer": 1}]}
In:    {"In": ["field", [{"Integer": 42}, {"Integer": 99}]]}
NotIn: {"NotIn": ["field", [{"Integer": 42}]]}
Gt:    {"Gt": ["field", {"Integer": 100}]}
Lt:    {"Lt": ["field", {"Integer": 100}]}
Gte:   {"Gte": ["field", {"Integer": 100}]}
Lte:   {"Lte": ["field", {"Integer": 100}]}
And:   {"And": [clause, clause, ...]}
Or:    {"Or": [clause, clause, ...]}
Not:   {"Not": clause}
```

### Values
```
{"Integer": 42}
{"Float": 3.14}
{"Bool": true}
{"String": "tos"}
```

### Query Flags
| Flag | Description |
|------|-------------|
| `--filter JSON` | Filter clauses array |
| `--sort FIELD` | Sort field name |
| `--dir Asc\|Desc` | Sort direction (default: Desc) |
| `--limit N` | Max results (default: 20) |
| `--offset N` | Skip first N results |
| `--cursor JSON` | Keyset pagination cursor |
| `--include-docs` | Include documents in response |
| `--fields f1,f2` | Select specific doc fields (with --include-docs) |
| `--index NAME` | Index name (auto-detected if only one) |

### Pagination

**Cursor (keyset)** — fast, use for infinite scroll:
```bash
# First page
$SKILL query --sort reactionCount --limit 20
# Next page (use cursor from response)
$SKILL query --sort reactionCount --limit 20 --cursor '{"sort_value": 15842, "slot_id": 720193}'
```

**Offset** — compatible with traditional pagination:
```bash
$SKILL query --sort reactionCount --limit 20 --offset 40
```

## Common Workflows

### Test a filter combination
```bash
$SKILL query --filter '[{"Eq":["nsfwLevel",{"Integer":1}]},{"In":["tagIds",[{"Integer":42}]]}]' --sort reactionCount --limit 5 --include-docs
```

### Check server performance
```bash
$SKILL stats
$SKILL metrics
```

### Load data from NDJSON
```bash
$SKILL load --path "C:\\Dev\\Repos\\open-source\\bitdex\\data\\images-full-v2.ndjson" --follow
```

### Upsert a document
```bash
$SKILL upsert --docs '[{"id": 12345, "nsfwLevel": 1, "stats": {"reactionCountAllTime": 500}, "tags": [42, 99]}]'
```

### Rebuild specific fields
```bash
$SKILL rebuild --filter-fields nsfwLevel,tagIds --follow
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BITDEX_URL` | `http://localhost:3001` | Server URL |
| `BITDEX_INDEX` | (auto-detect) | Default index name |
| `BITDEX_DIR` | `C:\Dev\Repos\open-source\bitdex-v2` | Repo path (for start) |
| `BITDEX_DATA_DIR` | `./data` | Data directory (for start) |

## Civitai Fields Reference

The standard Civitai index has these fields:

**Filter fields:** nsfwLevel, type, tagIds, modelVersionIds, userId, toolIds, techniqueIds, baseModel, blockedFor, availability, hasMeta, onSite, postedToModelVersion, hasPositiveReactions
**Sort fields:** reactionCount, commentCount, collectedCount, tippedAmountCount, sortAt, publishedAtUnix
**Doc-only fields:** url, hash

## When to Use

- Testing BitDex queries during development
- Checking server health and status
- Managing indexes (create, delete, rebuild)
- Loading data
- Upserting or deleting documents
- Inspecting cache and memory usage
- Any BitDex interaction — use this CLI instead of raw curl
