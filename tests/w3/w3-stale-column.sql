-- W3 gate-2 addition (post-#328): prove the widened BEFORE trigger self-heals a stale sortAt column.
-- The steady-state emission trusts Image.sortAt (COALESCE-shaped) — safe ONLY because ms_image_sortat
-- (BEFORE INSERT OR UPDATE, all columns) recomputes NEW.sortAt on every write. This asserts that an
-- nsfwLevel-ONLY update, after a deliberately-stale column, emits the CORRECT recomputed sortAtUnix.
-- Run against the local rig (w3-pg + poller + steady BitDex). Inspect BitdexOps + the BitDex doc.
\set img 970001
\set post 3001
INSERT INTO "Post"(id,"publishedAt",availability,"modelVersionId") VALUES (:post, now()-interval '1h','Public',999) ON CONFLICT (id) DO NOTHING;
INSERT INTO "Image"(id,url,"nsfwLevel",hash,flags,"type","userId","scannedAt","createdAt","postId",width,height)
  VALUES (:img,'u',1,'h',0,'image',5, now()-interval '2h', now()-interval '2h', :post, 512,768) ON CONFLICT (id) DO NOTHING;
-- correct value:
SELECT 'correct_greatest' AS label, extract(epoch from GREATEST((SELECT "publishedAt" FROM "Post" WHERE id=:post), (SELECT "scannedAt" FROM "Image" WHERE id=:img), (SELECT "createdAt" FROM "Image" WHERE id=:img)))::bigint AS v;
-- inject stale column (BEFORE trigger off so it isn't clobbered):
ALTER TABLE "Image" DISABLE TRIGGER ms_image_sortat_trg;
UPDATE "Image" SET "sortAt" = to_timestamp(1577836800) WHERE id=:img;   -- 2020-01-01, wrong
ALTER TABLE "Image" ENABLE TRIGGER ms_image_sortat_trg;
-- nsfwLevel-only update through the widened trigger -> recomputes sortAt, emits corrected sortAtUnix:
DELETE FROM "BitdexOps" WHERE entity_id=:img;
UPDATE "Image" SET "nsfwLevel"=8 WHERE id=:img;
-- ASSERT: the emitted `set sortAtUnix` is the correct recomputed value (== correct_greatest * 1000),
-- and Image.sortAt is now correct (not 1577836800). BitDex heals on poll.
SELECT 'emitted_ops' AS label, jsonb_path_query_array(ops, '$[*] ? (@.field == "sortAtUnix")') AS v FROM "BitdexOps" WHERE entity_id=:img;
SELECT 'pg_column_after' AS label, extract(epoch from "sortAt")::bigint AS v FROM "Image" WHERE id=:img;
