# Review Queue for Justin

> Items that need your review or decision. Each links to a detailed clarification doc with context.
> Dakota (Doc Keeper) maintains this. Sub-agents create the clarification docs.
>
> **How to use:** Scan the table below. Click the link to read the full question with context. Answer inline in the doc or tell Dakota via mailbox/voice. Dakota updates the docs based on your answers.

---

## Pending Review

| # | Topic | Priority | Question Summary | Detail |
|---|-------|----------|------------------|--------|
| 1 | Implementation plan ownership | MEDIUM | Who owns keeping the impl plan checkboxes current? | [View](clarifications/001-impl-plan-ownership.md) |
| 2 | BitmapFs in impl plan vs code | LOW | Plan says "Write to BitmapFs per phase" but code uses ShardStore | [View](clarifications/002-bitmapfs-plan-vs-code.md) |

## Resolved

| # | Topic | Resolution | Date |
|---|-------|------------|------|
| *(none yet)* | | | |

---

## How Clarifications Get Created

1. Dakota (or a Clarifier sub-agent) identifies an ambiguity or drift
2. A clarification doc is created in `docs/clarifications/` with:
   - Context snippets from relevant docs/code
   - The specific question(s)
   - Options if applicable
   - Impact of getting it wrong
3. An entry is added to this table
4. Justin reviews and answers
5. Dakota updates the relevant docs and moves the entry to Resolved
