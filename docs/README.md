# Bitdex V2 Documentation

## Organization Strategy

The docs/ folder follows this hierarchy:

| Location | Purpose | What belongs here |
|----------|---------|-------------------|
| `docs/HANDOFF.md` | Operational onboarding | Single file, always current |
| `docs/guide/` | Daily-use references | API docs, config refs, query formats, testing guides |
| `docs/design/` | Active design docs | Architecture decisions, implementation plans, checklists |
| `docs/design/archive/` | Superseded designs | V1 designs, replaced proposals, completed plans |
| `docs/reviews/` | Session knowledge | Extracted findings from agent sessions, architecture audits |
| `docs/benchmarks/` | Performance data | Benchmark plans, results, baselines |
| `docs/learnings/` | Rejected approaches | What we tried that didn't work (read before proposing) |
| `docs/_in/` | Input documents | Specs and requirements from Justin |
| `docs/archive/` | Historical docs | Everything else: past incident reports, stale handoffs |

## Standards

1. Every doc in `docs/` or `docs/design/` must have a clear owner or be in a README index
2. Date-stamped docs go to archive immediately — they're event records, not references
3. Working docs (task trackers, session plans) don't belong in committed docs
4. If a doc's content is fully covered by CLAUDE.md or another active doc, archive it
5. `docs/plans/` and `docs/to-resolve/` are retired — plans go in design docs, issues go in code

## Key Entry Points

- **New to the project?** Start with `HANDOFF.md`
- **Looking for design docs?** See `design/README.md` for the categorized index
- **API reference?** `guide/api.md`
- **Query syntax?** `guide/query-formats.md`
- **Config options?** `guide/config-schema.md` + `design/runtime-config-reference.md`
