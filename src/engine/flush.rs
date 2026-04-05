use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use arc_swap::ArcSwap;
use crossbeam_channel::Receiver;
use roaring::RoaringBitmap;
use crate::config::Config;
use crate::silos::doc_format::StoredDoc;
use crate::silos::doc_silo_adapter::DocSiloAdapter;
use crate::mutation::{FieldRegistry, MutationOp};
use crate::time_buckets::TimeBucketManager;
use super::flush_batch::FlushBatch;

/// All captured state passed into the flush thread by value.
/// Each field corresponds to an Arc (or plain value) cloned in `build()`.
pub struct FlushArgs {
    pub slots: Arc<parking_lot::RwLock<crate::engine::slot::SlotAllocator>>,
    pub filters: Arc<parking_lot::RwLock<crate::engine::filter::FilterIndex>>,
    pub sorts: Arc<parking_lot::RwLock<crate::engine::sort::SortIndex>>,
    pub shutdown: Arc<AtomicBool>,
    pub docstore: Arc<parking_lot::Mutex<DocSiloAdapter>>,
    pub flush_interval_us: u64,
    pub cache_silo: Option<Arc<parking_lot::RwLock<crate::silos::cache_silo::CacheSilo>>>,
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
    /// When false (no BitmapSilo), apply batched mutations to the in-memory
    /// FilterIndex/SortIndex/SlotAllocator so tests without a silo still work.
    /// In production (BitmapSilo present), mutations go directly to the silo
    /// and this path is skipped.
    pub has_silo: bool,
}

/// Entry point for the flush thread. Runs until `args.shutdown` is set.
///
/// Periodically drains the mutation channel (to prevent backup; V3 mutations go directly
/// to BitmapSilo), maintains time buckets via in-memory sort layers, drains the docstore
/// write channel, and triggers compaction. Bitmap state application has been removed —
/// BitmapSilo owns mutation persistence. On shutdown, compacts dirty filter diffs and
/// performs a final docstore drain.
pub fn run_flush_thread(args: FlushArgs) {
    let FlushArgs {
        slots: flush_slots,
        filters: flush_filters,
        sorts: flush_sorts,
        shutdown,
        docstore,
        flush_interval_us,
        cache_silo: _flush_cache_silo,
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
        opslog_ns: _flush_opslog_ns,
        config: flush_config,
        field_registry: flush_field_registry,
        mutation_rx: flush_mutation_rx,
        doc_rx,
        has_silo,
    } = args;

    let min_sleep = Duration::from_micros(flush_interval_us);
    let max_sleep = Duration::from_micros(flush_interval_us * 10);
    let mut current_sleep = min_sleep;
    let mut doc_batch: Vec<(u32, StoredDoc)> = Vec::new();
    let mut batch = FlushBatch::new();
    while !shutdown.load(Ordering::Relaxed) {
        thread::sleep(current_sleep);
        // Phase 1: Drain channel to prevent backup (mutations go to BitmapSilo in V3;
        // channel is kept alive for test compatibility without a silo).
        batch.drain_channel(&flush_mutation_rx);
        let bitmap_count = if !batch.is_empty() {
            let count = batch.len();
            batch.group_and_sort();
            count
        } else {
            0
        };
        // Phase 2: Apply mutations to in-memory state (no-silo fallback path only).
        // In production, BitmapSilo owns mutation persistence and this block is skipped.
        // In tests without a silo, mutations must still reach FilterIndex/SortIndex/SlotAllocator
        // so queries can see inserted documents.
        let flush_start = Instant::now();
        if bitmap_count > 0 && !has_silo {
            flush_dirty_flag.store(true, Ordering::Release);
            let t_apply = Instant::now();
            {
                let mut slots_w = flush_slots.write();
                let mut filters_w = flush_filters.write();
                let mut sorts_w = flush_sorts.write();
                batch.apply(&mut *slots_w, &mut *filters_w, &mut *sorts_w);
            }
            flush_apply_ns.store(t_apply.elapsed().as_nanos() as u64, Ordering::Relaxed);
            // Yield CPU after apply to let tokio I/O threads deliver pending responses.
            std::thread::yield_now();
        }
        // Phase 3: Live time bucket maintenance — add newly-alive slots to qualifying
        // buckets, remove deleted slots from all buckets. Reads sort layers in-memory
        // to reconstruct sort values for new slots (V2 dependency, removed with #17).
        if bitmap_count > 0 {
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
            flush_cache_ns.store(0, Ordering::Relaxed);
            flush_compact_ns.store(0, Ordering::Relaxed);
            // Record flush stats for Prometheus
            let flush_elapsed = flush_start.elapsed().as_nanos() as u64;
            flush_apply_cnt.fetch_add(1, Ordering::Relaxed);
            flush_dur_nanos.fetch_add(flush_elapsed, Ordering::Relaxed);
            flush_last_dur_nanos.store(flush_elapsed, Ordering::Relaxed);
            // Yield after time bucket work — let tokio deliver responses before disk I/O.
            std::thread::yield_now();
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
        // Phase 4: Drain docstore channel and batch write
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
    // Final flush on shutdown — drain remaining channel entries.
    // In the no-silo path, apply remaining ops to in-memory state before compacting diffs.
    // In the silo path, just drain to empty the channel; mutations are already in the silo.
    let mut shutdown_batch = FlushBatch::new();
    shutdown_batch.drain_channel(&flush_mutation_rx);
    if !shutdown_batch.is_empty() && !has_silo {
        shutdown_batch.group_and_sort();
        flush_dirty_flag.store(true, Ordering::Release);
        let mut slots_w = flush_slots.write();
        let mut filters_w = flush_filters.write();
        let mut sorts_w = flush_sorts.write();
        shutdown_batch.apply(&mut *slots_w, &mut *filters_w, &mut *sorts_w);
    }
    // Compact all remaining filter diffs before shutdown.
    {
        let mut filters_w = flush_filters.write();
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
