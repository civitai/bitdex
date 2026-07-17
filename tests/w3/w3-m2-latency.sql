SET client_min_messages=warning;
DROP TABLE IF EXISTS _l2;
CREATE TEMP TABLE _l2(kind text, us double precision);
INSERT INTO "Post"(id,"publishedAt",availability,"modelVersionId") VALUES (3100, now()-interval '1h','Public',1) ON CONFLICT (id) DO NOTHING;
INSERT INTO "Image"(id,url,"nsfwLevel",hash,flags,"type","userId","scannedAt","createdAt","postId",width,height)
  VALUES (971000,'u',1,'h',0,'image',5, now()-interval '2h', now()-interval '2h', 3100, 512,768) ON CONFLICT (id) DO NOTHING;
DO $$ DECLARE i int; t0 timestamptz; BEGIN
  FOR i IN 1..200 LOOP
    t0:=clock_timestamp(); UPDATE "Image" SET "nsfwLevel"=(i%31)+1 WHERE id=971000;
    INSERT INTO _l2 VALUES ('nsfwLevel_only_WITH_widened', extract(epoch from clock_timestamp()-t0)*1e6);
  END LOOP; END $$;
ALTER TABLE "Image" DISABLE TRIGGER ms_image_sortat_trg;
ALTER TABLE "Image" DISABLE TRIGGER bitdex_image_ee936694;
DO $$ DECLARE i int; t0 timestamptz; BEGIN
  FOR i IN 1..200 LOOP
    t0:=clock_timestamp(); UPDATE "Image" SET "nsfwLevel"=(i%31)+1 WHERE id=971000;
    INSERT INTO _l2 VALUES ('nsfwLevel_only_NO_triggers', extract(epoch from clock_timestamp()-t0)*1e6);
  END LOOP; END $$;
ALTER TABLE "Image" ENABLE TRIGGER ms_image_sortat_trg;
ALTER TABLE "Image" ENABLE TRIGGER bitdex_image_ee936694;
SELECT kind, count(*) n,
  round(percentile_cont(0.50) WITHIN GROUP (ORDER BY us)::numeric,1) p50_us,
  round(percentile_cont(0.95) WITHIN GROUP (ORDER BY us)::numeric,1) p95_us,
  round(percentile_cont(0.99) WITHIN GROUP (ORDER BY us)::numeric,1) p99_us
FROM _l2 GROUP BY kind ORDER BY kind;
