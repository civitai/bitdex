---
name: compaction-prep
description: Prepare for context compaction — save structured session state to memory so you can resume effectively after compaction drops your conversation history.
disable-model-invocation: false
user-invocable: true
---

# Compaction Preparation

When Justin or the system is about to compact your session (drop older conversation history to free context), run this skill to preserve your critical state. After compaction, you'll read this state file to resume work seamlessly.

## When to Use

- When told "compaction incoming" or "prepare for compaction"
- When your context is approaching limits and you need to preserve state proactively
- Before any major context reset

## Step 1: Capture Session State

Spawn a Conversation Reviewer sub-agent on YOUR OWN session to extract critical context, or do it yourself by reviewing your recent conversation:

```bash
# Find your own session ID
node ~/.claude/skills/agent-monitor/query.mjs agents | grep <your-name>

# Review your recent activity
node ~/.claude/skills/agent-monitor/query.mjs tail <your-name> --type text --lines 50
```

## Step 2: Write the State File

Create a memory file at:
```
C:\Users\Zipp4\.claude\projects\C--Dev-Repos-open-source-bitdex-v2\memory\project_{your-name}_session_{date}.md
```

Use this template:

```markdown
---
name: {Your name} session state {date}
description: Pre-compaction session state — critical context for resuming after compaction
type: project
---

## CRITICAL CONTEXT (must not be lost)
- What project am I working on?
- What is my role? (link to agent definition)
- Who do I report to?

## What Is Happening RIGHT NOW
- Active tasks in progress (with status)
- In-flight operations (downloads, builds, deploys running)
- Pending replies awaited (from whom, about what)

## Team Roster
| Agent | Role | Current Focus | Context Level |
|-------|------|--------------|---------------|
| ... | ... | ... | Full/Partial/Stale |

## Key Decisions Made This Session
- Decision 1: {what was decided and why}
- Decision 2: ...

## Pending Actions (Post-Compaction Priority)
1. {highest priority action}
2. {second priority}
3. ...

## Infrastructure Notes
- Server port in use: ...
- Active worktrees: ...
- Files being edited: ...

## Key Artifacts Created This Session
- {file path}: {what it is}
- ...

## Feedback Rules Established
- {any new rules from Justin this session}
```

## Step 3: Update MEMORY.md Index

Add a pointer to your state file in MEMORY.md so it's loaded into your context on the next turn.

## Step 4: Confirm Ready

Send a mailbox message to whoever requested the compaction:
```bash
node ~/.claude/skills/mailbox/query.mjs send <requester> "Compaction prep complete. Session state saved to memory/project_{name}_session_{date}.md. Ready for compaction."
```

## Post-Compaction Recovery Checklist

After compaction, your older conversation history will be gone. Do these in order:

1. **Read your session state file** — `memory/project_{name}_session_{date}.md`
2. **Read MEMORY.md** — for the full project context index
3. **Read your agent definition** — `.claude/agents/{your-role}.md`
4. **Open your mailbox** — check for messages received during compaction
5. **Check agent dashboard** — `node ~/.claude/skills/agent-monitor/query.mjs agents`
6. **Resume pending actions** — from the "Pending Actions" section of your state file
7. **Set your status** — update goal/task via agent-toolkit so Justin knows you're back

## Tips

- **Save early, save often** — don't wait for the compaction notice. If your session is getting long, proactively save state.
- **Be specific about in-flight work** — "downloading CSV" is useless after compaction. "Downloading tags.csv (63GB) from PG pod cnpg-cluster-nvme0-1, started 5 minutes ago, expect 10 more minutes" is recoverable.
- **Include file paths** — after compaction you won't remember which files you were editing. List them.
- **Link to artifacts** — if you created docs, reviews, or plans this session, list them. They survive compaction even if your memory of creating them doesn't.
