-- W3 gate 4 latency — measured inside PG (no docker/network noise).
-- [AR-5] publish-txn latency at realistic fan-out; [PR-M2] per-image-update overhead.
\timing off
SET client_min_messages = warning;

-- clean prior test rows in the measurement id range
DELETE FROM "BitdexOps" WHERE entity_id BETWEEN 800000 AND 899999;
DELETE FROM "Image" WHERE id BETWEEN 800000 AND 899999;
DELETE FROM "Post"  WHERE id BETWEEN 800000 AND 809999;

DROP TABLE IF EXISTS _lat;
CREATE TEMP TABLE _lat(kind text, us double precision);

-- Helper: seed a post with N images
CREATE OR REPLACE FUNCTION _seed_post(pid bigint, nimg int) RETURNS void AS $$
BEGIN
  INSERT INTO "Post"(id,"publishedAt",availability,"modelVersionId") VALUES (pid, NULL,'Public', 500+pid);
  INSERT INTO "Image"(id,url,"nsfwLevel",hash,flags,"type","userId","scannedAt","createdAt","postId",width,height)
  SELECT 800000+pid*1000+g, 'u', 1, 'h', 0, 'image', 42, now()-interval '1 day', now()-interval '1 day', pid, 512, 768
  FROM generate_series(0, nimg-1) g;
END; $$ LANGUAGE plpgsql;

-- ============ [AR-5] publish-txn latency @ ~20 images/post (P99 images-per-post) ============
DO $$
DECLARE i int; t0 timestamptz; fut boolean := true;
BEGIN
  PERFORM _seed_post(101, 20);
  FOR i IN 1..40 LOOP
    t0 := clock_timestamp();
    UPDATE "Post" SET "publishedAt" = CASE WHEN fut THEN now()+interval '1h' ELSE now()-interval '1h' END WHERE id=101;
    INSERT INTO _lat VALUES ('publish_20img', extract(epoch from clock_timestamp()-t0)*1e6);
    fut := NOT fut;
  END LOOP;
END $$;

-- ============ [AR-5] larger fan-out @ 200 images/post ============
DO $$
DECLARE i int; t0 timestamptz; fut boolean := true;
BEGIN
  PERFORM _seed_post(102, 200);
  FOR i IN 1..40 LOOP
    t0 := clock_timestamp();
    UPDATE "Post" SET "publishedAt" = CASE WHEN fut THEN now()+interval '1h' ELSE now()-interval '1h' END WHERE id=102;
    INSERT INTO _lat VALUES ('publish_200img', extract(epoch from clock_timestamp()-t0)*1e6);
    fut := NOT fut;
  END LOOP;
END $$;

-- ============ [AR-5] model-version worst case: many posts (50) x 20 images, one publish cascade ============
DO $$
DECLARE i int; t0 timestamptz; fut boolean := true; p bigint;
BEGIN
  FOR p IN 200..249 LOOP PERFORM _seed_post(p, 20); END LOOP;
  -- simulate MV publish touching all 50 posts in one txn (BEGIN..COMMIT is implicit in DO)
  FOR i IN 1..20 LOOP
    t0 := clock_timestamp();
    UPDATE "Post" SET "publishedAt" = CASE WHEN fut THEN now()+interval '1h' ELSE now()-interval '1h' END WHERE id BETWEEN 200 AND 249;
    INSERT INTO _lat VALUES ('mv_publish_50post_20img', extract(epoch from clock_timestamp()-t0)*1e6);
    fut := NOT fut;
  END LOOP;
END $$;

-- ============ [PR-M2] per-image scannedAt bump — WITH triggers (ms BEFORE + bitdex AFTER) ============
DO $$
DECLARE i int; t0 timestamptz;
BEGIN
  PERFORM _seed_post(300, 1);
  FOR i IN 1..200 LOOP
    t0 := clock_timestamp();
    UPDATE "Image" SET "scannedAt" = now()-(i||' sec')::interval WHERE id = 800000+300*1000;
    INSERT INTO _lat VALUES ('img_update_WITH_triggers', extract(epoch from clock_timestamp()-t0)*1e6);
  END LOOP;
END $$;

-- ============ [PR-M2] per-image scannedAt bump — WITHOUT triggers (baseline) ============
ALTER TABLE "Image" DISABLE TRIGGER ms_image_sortat_trg;
ALTER TABLE "Image" DISABLE TRIGGER bitdex_image_ee936694;
DO $$
DECLARE i int; t0 timestamptz;
BEGIN
  FOR i IN 1..200 LOOP
    t0 := clock_timestamp();
    UPDATE "Image" SET "scannedAt" = now()-(i||' sec')::interval WHERE id = 800000+300*1000;
    INSERT INTO _lat VALUES ('img_update_NO_triggers', extract(epoch from clock_timestamp()-t0)*1e6);
  END LOOP;
END $$;
ALTER TABLE "Image" ENABLE TRIGGER ms_image_sortat_trg;
ALTER TABLE "Image" ENABLE TRIGGER bitdex_image_ee936694;

-- ============ report percentiles ============
SELECT kind,
       count(*) AS n,
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY us)::numeric,1) AS p50_us,
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY us)::numeric,1) AS p95_us,
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY us)::numeric,1) AS p99_us,
       round(max(us)::numeric,1) AS max_us
FROM _lat GROUP BY kind ORDER BY kind;
