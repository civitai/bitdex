# Clarification #002: BitmapFs in Implementation Plan vs Code

**Status:** PENDING (low priority — informational)
**Created:** 2026-03-28 by Dakota (Doc Keeper)
**Priority:** LOW
**Affects:** docs/design/sync-v2-final-implementation-plan.md, D6 behavioral rules

---

## The Issue

The implementation plan's D6 behavioral rules say:

```markdown
**BitmapFs restore on startup** — the server restores bitmaps from BitmapFs via lazy loading.
The dump processor MUST write to BitmapFs.
```

But the actual dump processor (`src/dump_processor.rs`) writes exclusively to ShardStore:

```rust
// Line 1920: Save a PhaseResult's bitmaps to ShardStore. Drains filter/sort HashMaps
fn save_phase_to_store(
    alive_store: &crate::shard_store_bitmap::AliveBitmapStore,
    filter_store: &crate::shard_store_bitmap::FilterBitmapStore,
    sort_store: &crate::shard_store_bitmap::SortBitmapStore,
    ...
```

The migration happened in commit `4366b1a` (2026-03-27): "chore: remove BitmapFs from dump processor."

## The Question

1. **Is the implementation plan's D6 rule now stale?** Should it be updated to say "ShardStore" instead of "BitmapFs"?

2. **Does the server's lazy loading still work with ShardStore-written shards?** (I believe yes — ConcurrentEngine reads from ShardStore on startup — but want to confirm this is the intended flow.)

## Impact

Low — the code is correct and working. This is just about keeping the plan accurate so future agents don't get confused by the "MUST write to BitmapFs" directive.

---

**Justin's answer:** *(pending)*
