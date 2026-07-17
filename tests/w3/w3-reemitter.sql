-- W3 re-emitter (publish-reemitter.md §1.2) — heals the trailing lookback window.
-- Re-asserts per-image publish values from PG's authoritative state; no-op when BitDex is correct.
-- Usage (heal-path E2E): run after a missed publish op; the poller applies the emitted rows.
INSERT INTO "BitdexOps" (entity_id, ops)
SELECT i.id, bitdex_post_fanout_ops(p) || bitdex_image_sortat_ops(i)   -- jsonb concat
FROM "Image" i JOIN "Post" p ON p.id = i."postId"
WHERE p."publishedAt" >= now() - interval '15 min'
  AND p."publishedAt" <= now();
