# Doc Keeper Operations

Quick reference for doc-keeping workflows. Use `/doc-keeper` when you need to audit docs, review sessions, onboard agents, or check freshness.

## Common Operations

### Audit a Design Doc
Check if a design doc matches the current code:
1. Read the doc, note every claim (file paths, module names, behaviors, numbers)
2. Send an Explorer agent to verify each claim against actual code
3. Flag discrepancies in your doc-keeper-plan.md stale items log
4. If code is correct and doc is wrong: update the doc (code is truth)
5. If doc describes intended behavior that code doesn't match: flag to Tom/Justin

### Review an Agent's Session
Extract institutional knowledge from a completed agent's session:
```bash
# Find the agent and session ID
node ~/.claude/skills/agent-monitor/query.mjs agents

# Read their prompts (Justin's decisions)
node ~/.claude/skills/agent-monitor/query.mjs tail <name> --type prompts --lines 200

# Read their analysis
node ~/.claude/skills/agent-monitor/query.mjs tail <name> --type text --lines 200
```
Then spawn a Conversation Reviewer agent with the session context. Output goes to `docs/reviews/`.

### Check Doc Freshness
Scan for stale docs by checking `updated` frontmatter dates and comparing to recent git activity:
```bash
# Find design docs older than 2 weeks
grep -r "^updated:" docs/design/*.md | sort

# Check git log for recent changes to the areas those docs cover
git log --oneline --since="2 weeks ago" -- src/
```
Any design doc whose covered code area has changed since the doc's `updated` date is potentially stale.

### Onboard a New Agent
When a new team lead or engineer arrives:
1. Identify which project they're on and what docs are relevant
2. Send a mailbox message with:
   - Required reading list (design doc, benchmark plan, session reviews)
   - Key decisions already made (don't relitigate)
   - Immediate next step
   - Who to contact for what
3. Point them to `docs/guide/team-standards.md`
4. Follow up to verify they've read the material

### Create a Clarification Request
When you find ambiguity that needs Justin's decision:
1. Check `docs/clarifications/` for the next sequential number
2. Create `docs/clarifications/NNN-topic.md` with: issue, context (code blocks + doc excerpts), specific questions, options, impact
3. Add entry to `docs/review-queue.md` pending table
4. Or spawn a Clarifier agent: `Agent(subagent_type="Clarifier", prompt="...")`

### Coordinate a Design Process
Full lifecycle for a new architecture proposal (per `docs/guide/team-standards.md`):
1. **Capture** — voice memo / conversation → design doc in `docs/design/`
2. **Document** — problem statement with numbers, link source material
3. **Review** — broadcast to team, collect concerns, add to design doc
4. **Benchmark** — work with engineer to create benchmark plan in `docs/benchmarks/{feature}/`
5. **Implement** — engineer reads design doc first, code must match
6. **Validate** — session review to extract knowledge after completion

### Verify Checklist Items
For any checklist (implementation plan, production readiness, benchmark plan):
- Send Explorer agents to find the specific code for each item
- Each verified item gets: `[x]` + agent name + file:line evidence
- Unverified items stay `[ ]` — no rounding up
- Items that can't be done get explicit `DEFERRED: reason`

## Key Paths

| What | Where |
|------|-------|
| Design docs | `docs/design/*.md` |
| Guide docs | `docs/guide/*.md` |
| Session reviews | `docs/reviews/*.md` |
| Clarifications | `docs/clarifications/*.md` |
| Review queue | `docs/review-queue.md` |
| Benchmark plans | `docs/benchmarks/{feature}/benchmark-plan.md` |
| Agent definitions | `.claude/agents/*.md` |
| Memory files | `C:\Users\Zipp4\.claude\projects\C--Dev-Repos-open-source-bitdex-v2\memory\` |
| Master plan | `docs/doc-keeper-plan.md` |

## Sub-Agent Definitions

| Agent | File | Purpose |
|-------|------|---------|
| Conversation Reviewer | `.claude/agents/conversation-reviewer.md` | Mine sessions for undocumented knowledge |
| Clarifier | `.claude/agents/clarifier.md` | Generate structured decision requests |
