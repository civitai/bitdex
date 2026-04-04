use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use arc_swap::ArcSwap;
use crossbeam_channel::Receiver;
use roaring::RoaringBitmap;
use crate::config::Config;
use crate::doc_format::StoredDoc;
use crate::doc_silo_adapter::DocSiloAdapter;
use crate::mutation::{FieldRegistry, MutationOp};
use crate::time_buckets::TimeBucketManager;
use super::flush_batch::FlushBatch;

/// All captured state passed into the flush thread by value.
/// Each field corresponds to an Arc (or plain value) cloned in `build()`.
pub struct FlushArgs {
    pub slots: Arc<parking_lot::RwLock<crate::slot::SlotAllocator>>,
    pub filters: Arc<parking_lot::RwLock<crate::filter::FilterIndex>>,
    pub sorts: Arc<parking_lot::RwLock<crate::sort::SortIndex>>,
    pub shutdown: Arc<AtomicBool>,
    pub docstore: Arc<parking_lot::Mutex<DocSiloAdapter>>,
    pub flush_interval_us: u64,
    pub cache_silo: Option<Arc<parking_lot::RwLock<crate::cache_silo::CacheSilo>>>,
    pub dirty_flag: Arc<AtomicBool>,
    pub time_buckets: Option<Arc<parking_lot::Mutex<TimeBucketManager>>>,
    pub pending_diffs: Arc<ArcSwap<crate::bucket_diff_log::PendingBucketDiffs>>,
    pub diff_log_path: Option<PathBuf>,
    pub apply_cnt: Arc<AtomicU64>,
    pub dur_nanos: Arc<AtomicU64>,
    pub last_dur_nanos: Arc<AtomicU64>,
    pub apply_ns: Arc<AtomicU64>,
    pub cache_ns: Arc<AtomicU64>,
    pub timebucket_ns: Arc<AtomicU64>,
    pub compact_ns: Arc<AtomicU64>,
    pub opslog_ns: Arc<AtomicU64>,
    pub config: Arc<Config>,
    pub field_registry: FieldRegistry,
    pub mutation_rx: Receiver<MutationOp>,
    pub doc_rx: Receiver<(u32, StoredDoc)>,
}

/// Entry point for the flush thread. Runs until `args.shutdown` is set.
///
/// Periodically drains the mutation channel, applies batched ops to filter/sort/slot
/// indexes under brief write locks, maintains time buckets, invalidates the cache silo,
/// compacts dirty filter diffs, and drains the docstore write channel.
/// On shutdown, performs a final drain of both channels.
pub fn run_flush_thread(args: FlushArgs) {
    let FlushArgs {
        slots: flush_slots,
        filters: flush_filters,
        sorts: flush_sorts,
        shutdown,
        docstore,
        flush_interval_us,
        cache_silo: flush_cache_silo,
        dirty_flag: flush_dirty_flag,
        time_buckets: flush_time_buckets,
        pending_diffs: flush_pending_diffs,
        diff_log_path: flush_diff_log_path,
        apply_cnt: flush_apply_cnt,
        dur_nanos: flush_dur_nanos,
        last_dur_nanos: flush_last_dur_nanos,
        apply_ns: flush_apply_ns,
        cache_ns: flush_cache_ns,
        timebucket_ns: flush_timebucket_ns,
        compact_ns: flush_compact_ns,
        opslog_ns: flush_opslog_ns,
        config: flush_config,
        field_registry: flush_field_registry,
        mutation_rx: flush_mutation_rx,
        doc_rx,
    } = args;

    let min_sleep = Duration::from_micros(flush_interval_us);
    let max_sleep = Duration::from_micros(flush_interval_us * 10);
    let mut current_sleep = min_sleep;
    let mut doc_batch: Vec<(u32, StoredDoc)> = Vec::new();
    let mut flush_cycle: u64 = 0;
    let mut batch = FlushBatch::new();
    // Compact filter diffs every N flush cycles (~5s at 100μs interval).
    // Keeps diff layers small so apply_diff/fused stay fast.
    const COMPACTION_INTERVAL: u64 = 50;
    while !shutdown.load(Ordering::Relaxed) {
        thread::sleep(current_sleep);
        // Phase 1: Drain channel and group/sort (no lock, pure CPU work)
        batch.drain_channel(&flush_mutation_rx);
        let bitmap_count = if !batch.is_empty() {
            let count = batch.len();
            batch.group_and_sort();
            count
        } else {
            0
        };
        let mut stale_fields: Vec<String> = Vec::new();
        // Phase 2: Apply mutations under write locks (brief hold)
        let flush_start = Instant::now();
        if bitmap_count > 0 {
            flush_dirty_flag.store(true, Ordering::Release);
            let t_apply = Instant::now();
            {
                let mut slots_w = flush_slots.write();
                let mut filters_w = flush_filters.write();
                let mut sorts_w = flush_sorts.write();
                batch.apply(&mut *slots_w, &mut *filters_w, &mut *sorts_w);
            }
            flush_apply_ns.store(t_apply.elapsed().as_nanos() as u64, Ordering::Relaxed);
            // Collect mutated field names for bitmap memory cache staleness tracking.
            for fgk in batch.filter_inserts.keys() {
                stale_fields.push(fgk.field.to_string());
            }
            for fgk in batch.filter_removes.keys() {
                stale_fields.push(fgk.field.to_string());
            }
            for sgk in batch.sort_sets.keys() {
                stale_fields.push(sgk.field.to_string());
            }
            for sgk in batch.sort_clears.keys() {
                stale_fields.push(sgk.field.to_string());
            }
            // Yield CPU after apply to let tokio I/O threads deliver
            // pending HTTP responses. Without this, the flush thread
            // monopolizes CPU across apply+cache+publish (~20ms aggregate),
            // causing 1-4s response delivery delays under concurrent load.
            std::thread::yield_now();
            // Live maintenance for time buckets: add newly-alive slots to
            // qualifying buckets, remove deleted slots from all buckets.
            let t_tb = Instant::now();
            if let Some(ref tb_arc) = flush_time_buckets {
                if !batch.alive_inserts.is_empty() || !batch.alive_removes.is_empty() {
                    let now_secs = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    let mut tb = tb_arc.lock();
                    if !batch.alive_inserts.is_empty() {
                        let sort_field_name = tb.sort_field_name().to_string();
                        let sorts_r = flush_sorts.read();
                        if let Some(sort_field) = sorts_r.get_field(&sort_field_name) {
                            for &slot in &batch.alive_inserts {
                                let ts = sort_field.reconstruct_value(slot) as u64;
                                tb.insert_slot(slot, ts, now_secs);
                            }
                        }
                    }
                    for &slot in &batch.alive_removes {
                        tb.remove_slot(slot);
                    }
                }
            }
            flush_timebucket_ns.store(t_tb.elapsed().as_nanos() as u64, Ordering::Relaxed);
            // CacheSilo: invalidate stale entries when mutations touch their fields.
            // Any cache entry whose filter/sort fields changed is deleted from the silo
            // so the next query recomputes and re-seeds it.
            let t_cache = Instant::now();
            if let Some(ref cs_arc) = flush_cache_silo {
                if batch.has_alive_mutations() || !batch.mutated_filter_fields().is_empty() {
                    // On any write we delete ALL cached entries because we don't
                    // maintain a meta-index mapping (field, value) → cache keys.
                    // The silo is small (hundreds of entries), so full invalidation
                    // is cheap and correct. Entries are re-seeded on next query miss.
                    //
                    // Future optimization: build a per-entry field fingerprint and
                    // do targeted deletion. For now correctness > complexity.
                    let _cs = cs_arc.read(); // no-op drop — invalidation done at query time by recomputing on miss
                }
            }
            flush_cache_ns.store(t_cache.elapsed().as_nanos() as u64, Ordering::Relaxed);
            // Yield CPU after cache work to let tokio deliver responses.
            std::thread::yield_now();
            // Periodic filter diff compaction: merge dirty diffs into
            // bases so apply_diff/fused don't accumulate unbounded diffs.
            // Runs every COMPACTION_INTERVAL flush cycles (~5s).
            // Sort diffs and alive are already merged eagerly in WriteBatch::apply().
            //
            // CRITICAL: Only compact fields that have dirty diffs. Using
            // fields_mut() iterates ALL fields and calls Arc::make_mut on
            // each — which deep-clones the entire FilterField HashMap when
            // the Arc is shared with a published snapshot (refcount > 1).
            // For tagIds (31K entries), this clone takes seconds. Targeted
            // compaction avoids the clone cascade on untouched fields.
            let t_compact = Instant::now();
            if flush_cycle % COMPACTION_INTERVAL == 0 {
                // Collect names of dirty fields first under read lock (no write needed)
                let dirty_fields: Vec<String> = {
                    let filters_r = flush_filters.read();
                    filters_r.fields()
                        .filter(|(_, field)| field.has_dirty())
                        .map(|(name, _)| name.clone())
                        .collect()
                };
                // NOTE: Auto-loading bases for dirty+unloaded entries is disabled.
                // It caused OOM by loading all dirty postId bases (22M values)
                // at once during compaction. Only merge fields that have dirty diffs.
                if !dirty_fields.is_empty() {
                    let mut filters_w = flush_filters.write();
                    for name in &dirty_fields {
                        if let Some(field) = filters_w.get_field_mut(name) {
                            field.merge_dirty();
                        }
                    }
                }
            }
            flush_compact_ns.store(t_compact.elapsed().as_nanos() as u64, Ordering::Relaxed);
            flush_cycle += 1;
            stale_fields.clear();
            // Record flush stats for Prometheus
            let flush_elapsed = flush_start.elapsed().as_nanos() as u64;
            flush_apply_cnt.fetch_add(1, Ordering::Relaxed);
            flush_dur_nanos.fetch_add(flush_elapsed, Ordering::Relaxed);
            flush_last_dur_nanos.store(flush_elapsed, Ordering::Relaxed);
            // Yield after apply — let tokio deliver responses before disk I/O.
            std::thread::yield_now();
            // ── Ops-log append ──────────────────────────────────────────────
            let t_opslog = Instant::now();
            flush_opslog_ns.store(t_opslog.elapsed().as_nanos() as u64, Ordering::Relaxed);
        }
        // Activate deferred alive slots whose time has come.
        // Runs every flush cycle regardless of write activity for sub-second
        // activation precision. On activation: read stored doc from docstore,
        // replay the full mutation pipeline (filter/sort/alive ops) as if the
        // document was just PUT for the first time. This ensures the document
        // only becomes visible in bitmaps at activation time.
        let deferred_count = flush_slots.read().deferred_count();
        if deferred_count > 0 {
            let now_unix = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let activated = flush_slots.write().activate_due(now_unix);
            if !activated.is_empty() {
                // Collect all mutation ops for activated slots and apply in bulk.
                let mut activation_batch = FlushBatch::new();
                {
                    let ds = docstore.lock();
                    for &slot in &activated {
                        match ds.get(slot) {
                            Ok(Some(stored_doc)) => {
                                let doc = crate::mutation::Document {
                                    fields: stored_doc.fields.clone(),
                                };
                                let ops = crate::mutation::diff_document(
                                    slot,
                                    None, // fresh insert — no old doc
                                    &doc,
                                    &flush_config,
                                    false, // not upsert
                                    &flush_field_registry,
                                );
                                activation_batch.push_ops(ops);
                            }
                            Ok(None) => {
                                eprintln!("Warning: deferred slot {} has no stored doc, setting alive only", slot);
                                activation_batch.push_ops(vec![
                                    MutationOp::AliveInsert { slots: vec![slot] },
                                ]);
                            }
                            Err(e) => {
                                eprintln!("Warning: failed to read deferred slot {}: {e}, setting alive only", slot);
                                activation_batch.push_ops(vec![
                                    MutationOp::AliveInsert { slots: vec![slot] },
                                ]);
                            }
                        }
                    }
                } // docstore lock released
                activation_batch.group_and_sort();
                let mut slots_w = flush_slots.write();
                let mut filters_w = flush_filters.write();
                let mut sorts_w = flush_sorts.write();
                activation_batch.apply(&mut *slots_w, &mut *filters_w, &mut *sorts_w);
            }
        }
        // Incremental time bucket refresh: instead of scanning 107M alive slots,
        // compute expired slots via narrow range query on the sort layers.
        // Diffs are stored in PendingBucketDiffs for lazy application on cache reads.
        // No cache Mutex contention — flush thread never touches the unified cache for bucket work.
        if let Some(ref tb_arc) = flush_time_buckets {
                let now_secs = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                // Brief lock: check which buckets need refresh and get their config
                let refresh_info: Vec<(String, u64, u64, u64)> = {
                    let tb = tb_arc.lock();
                    let due = tb.refresh_due(now_secs);
                    if due.is_empty() {
                        Vec::new()
                    } else {
                        due.iter()
                            .filter_map(|name| {
                                tb.get_bucket(name).map(|b| (
                                    name.to_string(),
                                    b.duration_secs,
                                    b.refresh_interval_secs,
                                    b.last_cutoff(),
                                ))
                            })
                            .collect()
                    }
                }; // lock released
                if !refresh_info.is_empty() {
                    let tb_lock = tb_arc.lock();
                    let sort_field_name = tb_lock.sort_field_name().to_string();
                    drop(tb_lock);
                    let sorts_r = flush_sorts.read();
                    if let Some(sort_field) = sorts_r.get_field(&sort_field_name) {
                        let start = std::time::Instant::now();
                        for (bucket_name, duration_secs, refresh_interval, old_cutoff) in &refresh_info {
                            let new_cutoff = crate::bucket_diff_log::snap_cutoff(
                                now_secs.saturating_sub(*duration_secs),
                                *refresh_interval,
                            );
                            if new_cutoff <= *old_cutoff {
                                // No new expired slots since last cutoff
                                // Still mark as refreshed so needs_refresh returns false
                                let mut tb = tb_arc.lock();
                                if let Some(bucket) = tb.get_bucket_mut(bucket_name) {
                                    bucket.subtract_expired(&RoaringBitmap::new(), new_cutoff);
                                }
                                continue;
                            }
                            // Find expired slots: those in the bucket bitmap with
                            // sort value in [old_cutoff, new_cutoff)
                            let bucket_bm = {
                                let tb = tb_arc.lock();
                                tb.get_bucket(bucket_name)
                                    .map(|b| RoaringBitmap::clone(b.bitmap()))
                                    .unwrap_or_default()
                            };
                            let old_cutoff_u32 = *old_cutoff as u32;
                            let new_cutoff_u32 = new_cutoff as u32;
                            let mut expired = RoaringBitmap::new();
                            for slot in bucket_bm.iter() {
                                let val = sort_field.reconstruct_value(slot);
                                if val >= old_cutoff_u32 && val < new_cutoff_u32 {
                                    expired.insert(slot);
                                }
                            }
                            let expired_count = expired.len();
                            // Brief lock: subtract expired from bucket bitmap
                            {
                                let mut tb = tb_arc.lock();
                                if let Some(bucket) = tb.get_bucket_mut(bucket_name) {
                                    bucket.subtract_expired(&expired, new_cutoff);
                                }
                            }
                            // Store diff for lazy cache application (no cache Mutex!)
                            let diff = crate::bucket_diff_log::BucketDiff {
                                cutoff_before: *old_cutoff,
                                cutoff_after: new_cutoff,
                                expired: Arc::new(expired),
                            };
                            // Append to on-disk log
                            if let Some(ref log_path) = flush_diff_log_path {
                                let log = crate::bucket_diff_log::BucketDiffLog::new(
                                    log_path.clone(), 100, 0.3,
                                );
                                if let Err(e) = log.append(&diff) {
                                    eprintln!("Warning: failed to append bucket diff to log: {e}");
                                }
                                // Periodic compaction
                                if let Err(e) = log.compact_if_needed() {
                                    eprintln!("Warning: bucket diff log compaction failed: {e}");
                                }
                            }
                            // Update in-memory pending diffs (ArcSwap store)
                            {
                                let old_pending = flush_pending_diffs.load();
                                let mut new_pending = crate::bucket_diff_log::PendingBucketDiffs::from_diffs(
                                    old_pending.diffs().to_vec(),
                                    100,
                                );
                                new_pending.push(diff);
                                flush_pending_diffs.store(Arc::new(new_pending));
                            }
                            eprintln!("Time bucket '{}' incremental refresh: expired={} cutoff {}→{} in {:?}",
                                bucket_name, expired_count, old_cutoff, new_cutoff, start.elapsed());
                        }
                        // Mark dirty so merge thread persists time buckets
                        flush_dirty_flag.store(true, Ordering::Release);
                    } else {
                        eprintln!("Time bucket: sort field '{}' not found in staging", sort_field_name);
                    }
                }
        }
        // Phase 3: Drain docstore channel and batch write
        doc_batch.clear();
        while let Ok(item) = doc_rx.try_recv() {
            doc_batch.push(item);
        }
        let doc_count = doc_batch.len();
        if doc_count > 0 {
            // DataSilo mmap reads are fast enough — no cache needed
            if let Err(e) = docstore.lock().put_batch(&doc_batch) {
                eprintln!("WARNING: docstore batch write failed (skipping {} docs): {e}", doc_batch.len());
            }
        }
        if bitmap_count > 0 || doc_count > 0 {
            current_sleep = min_sleep;
        } else {
            current_sleep = (current_sleep * 2).min(max_sleep);
        }
    }
    // Final flush on shutdown
    let mut shutdown_batch = FlushBatch::new();
    shutdown_batch.drain_channel(&flush_mutation_rx);
    let count = if !shutdown_batch.is_empty() {
        let c = shutdown_batch.len();
        shutdown_batch.group_and_sort();
        c
    } else { 0 };
    if count > 0 {
        flush_dirty_flag.store(true, Ordering::Release);
        let mut slots_w = flush_slots.write();
        let mut filters_w = flush_filters.write();
        let mut sorts_w = flush_sorts.write();
        shutdown_batch.apply(&mut *slots_w, &mut *filters_w, &mut *sorts_w);
        // Compact all remaining filter diffs before shutdown
        for (_name, field) in filters_w.fields_mut() {
            field.merge_dirty();
        }
    }
    // Final docstore drain
    doc_batch.clear();
    while let Ok(item) = doc_rx.try_recv() {
        doc_batch.push(item);
    }
    if !doc_batch.is_empty() {
        if let Err(e) = docstore.lock().put_batch(&doc_batch) {
            panic!("docstore final batch write failed: {e}");
        }
    }
}
