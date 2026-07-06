use ahash::AHashMap as HashMap;
use std::sync::Arc;

use roaring::RoaringBitmap;

use crate::config::BucketConfig;

/// A single time range bucket with its pre-computed bitmap.
#[derive(Clone)]
pub struct TimeBucket {
    /// Human-readable name (e.g., "24h", "7d", "30d").
    pub name: String,
    /// Duration of the bucket in seconds.
    pub duration_secs: u64,
    /// How often to rebuild this bucket's bitmap, in seconds.
    pub refresh_interval_secs: u64,
    /// Pre-computed bitmap of slots whose timestamp value falls within [now - duration, now].
    bitmap: Arc<RoaringBitmap>,
    /// Unix timestamp (seconds) when this bucket was last rebuilt.
    last_refreshed: u64,
    /// The snapped cutoff used in the last refresh: snap(now - duration, refresh_interval).
    /// Used for incremental rebuild: expired slots are those in [last_cutoff, new_cutoff).
    last_cutoff: u64,
}

impl TimeBucket {
    pub fn new(name: String, duration_secs: u64, refresh_interval_secs: u64) -> Self {
        Self {
            name,
            duration_secs,
            refresh_interval_secs,
            bitmap: Arc::new(RoaringBitmap::new()),
            last_refreshed: 0,
            last_cutoff: 0,
        }
    }

    /// Returns true if the bucket needs a rebuild based on time elapsed since last refresh.
    pub fn needs_refresh(&self, now: u64) -> bool {
        now.saturating_sub(self.last_refreshed) >= self.refresh_interval_secs
    }

    pub fn bitmap(&self) -> &Arc<RoaringBitmap> {
        &self.bitmap
    }

    pub fn len(&self) -> u64 {
        self.bitmap.len()
    }

    /// Add a slot to this bucket's bitmap (live maintenance on insert).
    pub fn insert_slot(&mut self, slot: u32) {
        Arc::make_mut(&mut self.bitmap).insert(slot);
    }

    /// Remove a slot from this bucket's bitmap (live maintenance on delete).
    pub fn remove_slot(&mut self, slot: u32) {
        Arc::make_mut(&mut self.bitmap).remove(slot);
    }

    /// The snapped cutoff from the last refresh.
    pub fn last_cutoff(&self) -> u64 {
        self.last_cutoff
    }

    /// Set the last cutoff (used when loading persisted state).
    pub fn set_last_cutoff(&mut self, cutoff: u64) {
        self.last_cutoff = cutoff;
    }

    /// Subtract expired slots from the bucket bitmap and update the cutoff.
    /// Used for incremental refresh: only removes slots that aged out since
    /// the last cutoff, instead of rebuilding the entire bitmap.
    pub fn subtract_expired(&mut self, expired: &RoaringBitmap, new_cutoff: u64) {
        if !expired.is_empty() {
            let bm = Arc::make_mut(&mut self.bitmap);
            *bm -= expired;
        }
        self.last_cutoff = new_cutoff;
        self.last_refreshed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
    }
}

/// Manages all time buckets for a single timestamp field.
#[derive(Clone)]
pub struct TimeBucketManager {
    /// The filter field name this manager is associated with (e.g., "sortAtUnix").
    /// Used to match Gte filter clauses for snapping.
    field_name: String,
    /// The sort field name used for value reconstruction (e.g., "sortAt").
    /// Defaults to field_name if not explicitly configured.
    sort_field_name: String,
    /// Buckets keyed by name for fast lookup.
    buckets: HashMap<String, TimeBucket>,
    /// Bucket names sorted by duration (shortest first) for snapping.
    sorted_names: Vec<String>,
}

impl TimeBucketManager {
    pub fn new(field_name: String, bucket_configs: Vec<BucketConfig>) -> Self {
        Self::new_with_sort_field(field_name.clone(), field_name, bucket_configs)
    }

    pub fn new_with_sort_field(
        field_name: String,
        sort_field_name: String,
        bucket_configs: Vec<BucketConfig>,
    ) -> Self {
        let mut buckets = HashMap::new();
        let mut sorted_names: Vec<String> = Vec::new();

        for config in bucket_configs {
            let bucket = TimeBucket::new(
                config.name.clone(),
                config.duration_secs,
                config.refresh_interval_secs,
            );
            buckets.insert(config.name.clone(), bucket);
            sorted_names.push(config.name);
        }

        // Sort by duration ascending (shortest first) for snapping.
        sorted_names.sort_by(|a, b| {
            let da = buckets[a].duration_secs;
            let db = buckets[b].duration_secs;
            da.cmp(&db)
        });

        Self {
            field_name,
            sort_field_name,
            buckets,
            sorted_names,
        }
    }

    /// Load persisted bitmaps into matching buckets. Sets last_refreshed to now
    /// so the periodic refresh timer starts from the restore point.
    /// Sets last_cutoff based on the current time and bucket duration.
    pub fn load_persisted(&mut self, persisted: &[(String, RoaringBitmap)], now: u64) {
        for (name, bitmap) in persisted {
            if let Some(bucket) = self.buckets.get_mut(name.as_str()) {
                let cutoff = now.saturating_sub(bucket.duration_secs);
                let snapped = crate::bucket_diff_log::snap_cutoff(cutoff, bucket.refresh_interval_secs);
                bucket.bitmap = Arc::new(bitmap.clone());
                bucket.last_refreshed = now;
                bucket.last_cutoff = snapped;
            }
        }
    }

    /// Returns an iterator over all (name, bitmap) pairs for persistence.
    pub fn all_buckets(&self) -> impl Iterator<Item = (&str, &RoaringBitmap)> {
        self.sorted_names.iter().map(move |name| {
            let bucket = &self.buckets[name.as_str()];
            (name.as_str(), bucket.bitmap.as_ref())
        })
    }

    /// Returns the names of buckets that need a refresh at the given time.
    pub fn refresh_due(&self, now: u64) -> Vec<&str> {
        self.sorted_names
            .iter()
            .filter(|name| {
                self.buckets
                    .get(name.as_str())
                    .map(|b| b.needs_refresh(now))
                    .unwrap_or(false)
            })
            .map(|s| s.as_str())
            .collect()
    }

    /// Rebuilds a bucket's bitmap from an iterator of (slot, timestamp_value) pairs.
    /// A slot qualifies if its timestamp falls in [now - duration_secs, now].
    pub fn rebuild_bucket(
        &mut self,
        bucket_name: &str,
        value_iter: impl Iterator<Item = (u32, u64)>,
        now: u64,
    ) {
        let Some(bucket) = self.buckets.get_mut(bucket_name) else {
            return;
        };

        let duration = bucket.duration_secs;
        let cutoff = now.saturating_sub(duration);
        let snapped_cutoff = crate::bucket_diff_log::snap_cutoff(cutoff, bucket.refresh_interval_secs);

        let mut new_bitmap = RoaringBitmap::new();
        for (slot, ts) in value_iter {
            if ts >= cutoff && ts <= now {
                new_bitmap.insert(slot);
            }
        }

        bucket.bitmap = Arc::new(new_bitmap);
        bucket.last_refreshed = now;
        bucket.last_cutoff = snapped_cutoff;
    }

    /// Look up a bucket by name.
    pub fn get_bucket(&self, name: &str) -> Option<&TimeBucket> {
        self.buckets.get(name)
    }

    /// Mutable access to a bucket (for incremental subtract_expired).
    pub fn get_bucket_mut(&mut self, name: &str) -> Option<&mut TimeBucket> {
        self.buckets.get_mut(name)
    }

    /// Swap in a pre-built bitmap for a bucket. Used by the lock-free rebuild path
    /// where the bitmap is computed outside the lock, then swapped in briefly.
    pub fn rebuild_bucket_from_bitmap(
        &mut self,
        bucket_name: &str,
        bitmap: RoaringBitmap,
        now: u64,
    ) {
        if let Some(bucket) = self.buckets.get_mut(bucket_name) {
            let cutoff = now.saturating_sub(bucket.duration_secs);
            let snapped = crate::bucket_diff_log::snap_cutoff(cutoff, bucket.refresh_interval_secs);
            bucket.bitmap = Arc::new(bitmap);
            bucket.last_refreshed = now;
            bucket.last_cutoff = snapped;
        }
    }

    /// Given a duration from a range filter (e.g., now - filter_value), find the closest bucket
    /// within `tolerance_pct` (as a fraction, e.g., 0.10 for 10%). Returns the bucket name.
    pub fn snap_duration(&self, duration_secs: u64, tolerance_pct: f64) -> Option<&str> {
        let mut best_name: Option<&str> = None;
        let mut best_delta = u64::MAX;

        for name in &self.sorted_names {
            let bucket = &self.buckets[name];
            let bd = bucket.duration_secs;
            let delta = if duration_secs > bd {
                duration_secs - bd
            } else {
                bd - duration_secs
            };

            // Tolerance is relative to the bucket duration.
            let threshold = (bd as f64 * tolerance_pct).round() as u64;
            if delta <= threshold && delta < best_delta {
                best_delta = delta;
                best_name = Some(name.as_str());
            }
        }

        best_name
    }

    /// Snap to the nearest bucket that covers the requested duration.
    /// Always returns a result (the smallest bucket >= duration, or the largest bucket if
    /// duration exceeds all buckets). Never returns None.
    /// Used when `allow_unsnapped_range_queries` is false (the default).
    pub fn snap_nearest(&self, duration_secs: u64) -> &str {
        // sorted_names is sorted by duration ascending.
        // Find the smallest bucket whose duration >= the requested duration.
        for name in &self.sorted_names {
            let bucket = &self.buckets[name];
            if bucket.duration_secs >= duration_secs {
                return name.as_str();
            }
        }
        // Duration exceeds all buckets — use the largest one.
        self.sorted_names.last().map(|s| s.as_str()).unwrap_or("_none")
    }

    /// Live maintenance: add a slot to all buckets whose time window includes the given timestamp.
    /// Called on insert/upsert when the sort field value is known.
    pub fn insert_slot(&mut self, slot: u32, timestamp: u64, now: u64) {
        for bucket in self.buckets.values_mut() {
            let cutoff = now.saturating_sub(bucket.duration_secs);
            if timestamp >= cutoff && timestamp <= now {
                bucket.insert_slot(slot);
            }
        }
    }

    /// Live maintenance: remove a slot from all buckets.
    /// Called on delete — unconditionally removes from every bucket.
    pub fn remove_slot(&mut self, slot: u32) {
        for bucket in self.buckets.values_mut() {
            bucket.remove_slot(slot);
        }
    }

    /// Returns all bucket names (for forced rebuilds after lazy sort field loading).
    pub fn bucket_names(&self) -> Vec<String> {
        self.sorted_names.clone()
    }

    /// Reset all buckets' last_refreshed to 0, forcing a rebuild on the next periodic check.
    pub fn force_refresh_due(&mut self) {
        for bucket in self.buckets.values_mut() {
            bucket.last_refreshed = 0;
        }
    }

    /// Update the refresh interval for a named bucket. Returns true if the bucket was found.
    pub fn set_refresh_interval(&mut self, bucket_name: &str, interval_secs: u64) -> bool {
        if let Some(bucket) = self.buckets.get_mut(bucket_name) {
            bucket.refresh_interval_secs = interval_secs;
            true
        } else {
            false
        }
    }

    pub fn bucket_count(&self) -> usize {
        self.buckets.len()
    }

    pub fn total_bitmap_bytes(&self) -> usize {
        self.buckets
            .values()
            .map(|b| b.bitmap.serialized_size())
            .sum()
    }

    pub fn field_name(&self) -> &str {
        &self.field_name
    }

    /// The sort field used for value reconstruction during bucket rebuilds.
    pub fn sort_field_name(&self) -> &str {
        &self.sort_field_name
    }

    /// Apply a periodic-rebuild reconcile: re-validate the worker's candidate
    /// sets against CURRENT sort values and mutate the manager in place, pruning
    /// stale members and backfilling missing ones. Mirror-symmetric re-validation
    /// on both boundaries — the backfill counterpart to the prune.
    ///
    /// The worker computes candidates over a minutes-old snapshot; by apply time
    /// a slot may have been deleted or had its sort value move. So each candidate
    /// is re-checked against `sort_field` + `alive` (which MUST come from the
    /// CURRENT staging engine, not the snapshot):
    ///   - a STALE candidate is pruned iff it is still out of window
    ///     (`val < cutoff || val > now`), and
    ///   - a MISSING candidate is backfilled iff it is still alive AND in window
    ///     (`cutoff <= val <= now`) AND not already present in the bucket
    ///     (live maintenance may have added it since the snapshot).
    ///
    /// Cutoff is NOT advanced: `subtract_expired` is called with the bucket's
    /// existing `last_cutoff`, and `insert_slot` doesn't touch it — the
    /// incremental band path owns cutoff advancement. Iterates each bucket once,
    /// computing both deltas before mutating, and zips candidates by bucket NAME
    /// (never assumes the two slices share order).
    ///
    /// Takes the worker's per-bucket `(name, stale, missing)` candidate tuples by
    /// reference (no clone/split). Returns one `(bucket_name, pruned, backfilled)`
    /// per bucket that was actually mutated — the caller sums for logging and
    /// emits per-bucket metrics. `pruned`/`backfilled` are the CONFIRMED counts
    /// (post re-validation), so a bucket appears iff the manager changed.
    pub(crate) fn reconcile_apply(
        &mut self,
        sort_field: &crate::sort::SortField,
        alive: &RoaringBitmap,
        now_secs: u64,
        candidates: &[(String, RoaringBitmap, RoaringBitmap)],
    ) -> Vec<(String, u64, u64)> {
        let mut per_bucket: Vec<(String, u64, u64)> = Vec::new();
        for (name, stale, missing) in candidates {
            // Read-only pass: compute both confirmed deltas for this bucket.
            let (still, add, last_cut) = {
                let Some(bucket) = self.get_bucket(name) else {
                    continue;
                };
                let cutoff = now_secs.saturating_sub(bucket.duration_secs);
                let last_cut = bucket.last_cutoff();
                let current = bucket.bitmap();

                // Re-validate stale. A candidate is `old − fresh` where `fresh`
                // is alive ∩ in-window, so it can be either out-of-window OR
                // no longer alive (clean delete usually clears its sort bits, but
                // don't rely on that at apply time). Prune iff it is still in the
                // bucket AND (dead OR out of window). Skipping slots live
                // maintenance already removed keeps `pruned` = actual removals, so
                // no-op prunes don't inflate metrics or force a needless publish.
                let mut still = RoaringBitmap::new();
                for slot in stale.iter() {
                    if !current.contains(slot) {
                        continue; // already gone — nothing to prune
                    }
                    if !alive.contains(slot) {
                        still.insert(slot); // dead but lingering — prune
                        continue;
                    }
                    let val = sort_field.reconstruct_value(slot) as u64;
                    if val < cutoff || val > now_secs {
                        still.insert(slot); // still out of window — prune
                    }
                    // else: moved back in window since the snapshot — keep.
                }
                // Re-validate missing: backfill only if alive + in window + absent.
                let mut add = RoaringBitmap::new();
                for slot in missing.iter() {
                    if !alive.contains(slot) {
                        continue;
                    }
                    let val = sort_field.reconstruct_value(slot) as u64;
                    if val >= cutoff && val <= now_secs && !current.contains(slot) {
                        add.insert(slot);
                    }
                }
                (still, add, last_cut)
            };

            if still.is_empty() && add.is_empty() {
                continue;
            }
            let pruned = still.len();
            let backfilled = add.len();
            if let Some(bucket) = self.get_bucket_mut(name) {
                if !still.is_empty() {
                    // Prune first (does not advance cutoff — pass existing one),
                    // then backfill. Stale and missing sets are disjoint by
                    // construction (stale = old − fresh, missing = fresh − old),
                    // so the two operations never touch the same slot.
                    bucket.subtract_expired(&still, last_cut);
                }
                for slot in &add {
                    bucket.insert_slot(slot);
                }
                per_bucket.push((name.clone(), pruned, backfilled));
            }
        }
        per_bucket
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_manager(configs: Vec<(&str, u64, u64)>) -> TimeBucketManager {
        let configs = configs
            .into_iter()
            .map(|(name, dur, refresh)| BucketConfig {
                name: name.to_string(),
                duration_secs: dur,
                refresh_interval_secs: refresh,
            })
            .collect();
        TimeBucketManager::new("sortAt".to_string(), configs)
    }

    #[test]
    fn test_basic_lifecycle() {
        let mut mgr = make_manager(vec![
            ("24h", 86400, 300),
            ("7d", 604800, 3600),
        ]);

        let now: u64 = 1_700_000_000;

        // Slots with timestamps within 24h window.
        let data: Vec<(u32, u64)> = vec![
            (1, now - 3600),       // 1h ago — in 24h
            (2, now - 86400 + 1),  // just inside 24h
            (3, now - 86401),      // just outside 24h
            (4, now),              // exactly now — in 24h
            (5, now - 200000),     // way outside 24h
        ];

        mgr.rebuild_bucket("24h", data.iter().copied(), now);

        let bucket = mgr.get_bucket("24h").unwrap();
        assert!(bucket.bitmap().contains(1));
        assert!(bucket.bitmap().contains(2));
        assert!(!bucket.bitmap().contains(3));
        assert!(bucket.bitmap().contains(4));
        assert!(!bucket.bitmap().contains(5));
        assert_eq!(bucket.len(), 3);
    }

    #[test]
    fn test_7d_window() {
        let mut mgr = make_manager(vec![
            ("24h", 86400, 300),
            ("7d", 604800, 3600),
        ]);

        let now: u64 = 1_700_000_000;

        let data: Vec<(u32, u64)> = vec![
            (10, now - 86400),      // 1d ago — in 7d, in 24h boundary
            (11, now - 604800 + 1), // just inside 7d
            (12, now - 604801),     // just outside 7d
            (13, now - 200000),     // 2.3d ago — in 7d
        ];

        mgr.rebuild_bucket("7d", data.iter().copied(), now);

        let bucket = mgr.get_bucket("7d").unwrap();
        assert!(bucket.bitmap().contains(10));
        assert!(bucket.bitmap().contains(11));
        assert!(!bucket.bitmap().contains(12));
        assert!(bucket.bitmap().contains(13));
        assert_eq!(bucket.len(), 3);
    }

    #[test]
    fn test_refresh_timing() {
        let mut mgr = make_manager(vec![
            ("24h", 86400, 300),
        ]);

        let now: u64 = 1_700_000_000;

        // Before any rebuild, last_refreshed = 0, so needs_refresh = true.
        assert!(mgr.get_bucket("24h").unwrap().needs_refresh(now));

        // After rebuild, last_refreshed = now.
        mgr.rebuild_bucket("24h", std::iter::empty(), now);
        let bucket = mgr.get_bucket("24h").unwrap();
        assert!(!bucket.needs_refresh(now));
        assert!(!bucket.needs_refresh(now + 299)); // 299s later, not yet
        assert!(bucket.needs_refresh(now + 300));  // exactly at interval
        assert!(bucket.needs_refresh(now + 500));  // past interval
    }

    #[test]
    fn test_refresh_due() {
        let mut mgr = make_manager(vec![
            ("24h", 86400, 300),
            ("7d", 604800, 3600),
        ]);

        let now: u64 = 1_700_000_000;

        // Before any rebuild, both need refresh.
        let due = mgr.refresh_due(now);
        assert_eq!(due.len(), 2);

        // Rebuild 24h only.
        mgr.rebuild_bucket("24h", std::iter::empty(), now);

        let due = mgr.refresh_due(now);
        assert_eq!(due.len(), 1);
        assert_eq!(due[0], "7d");

        // After refresh interval passes, 24h needs refresh again.
        let due = mgr.refresh_due(now + 301);
        assert_eq!(due.len(), 2);
    }

    #[test]
    fn test_snap_duration_exact() {
        let mgr = make_manager(vec![
            ("24h", 86400, 300),
            ("7d", 604800, 3600),
        ]);

        // Exact match for 24h.
        let snapped = mgr.snap_duration(86400, 0.10);
        assert_eq!(snapped, Some("24h"));

        // Exact match for 7d.
        let snapped = mgr.snap_duration(604800, 0.10);
        assert_eq!(snapped, Some("7d"));
    }

    #[test]
    fn test_snap_duration_within_tolerance() {
        let mgr = make_manager(vec![
            ("24h", 86400, 300),
            ("7d", 604800, 3600),
        ]);

        // 90000s is 86400 + 3600 — delta = 3600, threshold = 86400 * 0.10 = 8640 → within tolerance.
        let snapped = mgr.snap_duration(90000, 0.10);
        assert_eq!(snapped, Some("24h"));

        // 80000s — delta = 6400, threshold = 8640 → within tolerance.
        let snapped = mgr.snap_duration(80000, 0.10);
        assert_eq!(snapped, Some("24h"));
    }

    #[test]
    fn test_snap_duration_outside_tolerance() {
        let mgr = make_manager(vec![
            ("24h", 86400, 300),
            ("7d", 604800, 3600),
        ]);

        // 200000s — far from both.
        // Delta from 24h: 113600, threshold 8640 → miss.
        // Delta from 7d: 404800, threshold 60480 → miss.
        let snapped = mgr.snap_duration(200000, 0.10);
        assert_eq!(snapped, None);
    }

    #[test]
    fn test_empty_rebuild() {
        let mut mgr = make_manager(vec![
            ("24h", 86400, 300),
        ]);

        let now: u64 = 1_700_000_000;
        mgr.rebuild_bucket("24h", std::iter::empty(), now);

        let bucket = mgr.get_bucket("24h").unwrap();
        assert_eq!(bucket.len(), 0);
        assert!(bucket.bitmap().is_empty());
    }

    #[test]
    fn test_sorted_order() {
        // Insert in reverse order (7d first, then 24h).
        let mgr = make_manager(vec![
            ("7d", 604800, 3600),
            ("24h", 86400, 300),
            ("30d", 2592000, 86400),
        ]);

        assert_eq!(mgr.sorted_names[0], "24h");
        assert_eq!(mgr.sorted_names[1], "7d");
        assert_eq!(mgr.sorted_names[2], "30d");
    }

    #[test]
    fn test_bucket_count_and_bytes() {
        let mgr = make_manager(vec![
            ("24h", 86400, 300),
            ("7d", 604800, 3600),
        ]);
        assert_eq!(mgr.bucket_count(), 2);
        // Empty bitmaps — serialized size is minimal but non-negative.
        assert!(mgr.total_bitmap_bytes() < 100);
    }

    #[test]
    fn test_rebuild_unknown_bucket() {
        let mut mgr = make_manager(vec![
            ("24h", 86400, 300),
        ]);
        // Should not panic.
        mgr.rebuild_bucket("nonexistent", std::iter::empty(), 1_700_000_000);
    }

    #[test]
    fn test_set_refresh_interval() {
        let mut mgr = make_manager(vec![
            ("24h", 86400, 300),
            ("7d", 604800, 3600),
        ]);

        // Update existing bucket — returns true
        assert!(mgr.set_refresh_interval("24h", 60));
        assert_eq!(mgr.get_bucket("24h").unwrap().refresh_interval_secs, 60);

        // Other bucket unchanged
        assert_eq!(mgr.get_bucket("7d").unwrap().refresh_interval_secs, 3600);

        // Unknown bucket — returns false
        assert!(!mgr.set_refresh_interval("nonexistent", 120));
    }

    #[test]
    fn test_set_refresh_interval_affects_needs_refresh() {
        let mut mgr = make_manager(vec![
            ("24h", 86400, 300),
        ]);

        let now: u64 = 1_700_000_000;
        mgr.rebuild_bucket("24h", std::iter::empty(), now);

        // At now+200, with interval=300 the bucket does NOT need refresh
        assert!(!mgr.get_bucket("24h").unwrap().needs_refresh(now + 200));

        // Shorten interval to 100 — now it DOES need refresh at now+200
        mgr.set_refresh_interval("24h", 100);
        assert!(mgr.get_bucket("24h").unwrap().needs_refresh(now + 200));
    }

    /// Regression: the flush thread re-evaluates bucket membership when a
    /// slot's tracked sort field value changes. The pattern is `remove_slot`
    /// then `insert_slot(slot, new_ts, now)` — `remove_slot` drops prior
    /// membership across all buckets and `insert_slot` only re-adds when
    /// the new ts is in window. Without this, a slot whose sortAt regressed
    /// from "now" to "two days ago" stayed stuck in the 24h bucket forever
    /// because the periodic `subtract_expired` only catches values in
    /// `[old_cutoff, new_cutoff)`.
    #[test]
    fn test_remove_then_insert_handles_sort_regression() {
        let mut mgr = make_manager(vec![
            ("24h", 86400, 300),
            ("7d", 604800, 3600),
        ]);
        let now: u64 = 1_700_000_000;

        // Initial insert: slot 42 with sortAt = 1h ago. Lands in 24h + 7d.
        mgr.insert_slot(42, now - 3600, now);
        assert!(mgr.get_bucket("24h").unwrap().bitmap().contains(42));
        assert!(mgr.get_bucket("7d").unwrap().bitmap().contains(42));

        // Sort field regresses to 2 days ago (e.g., publishedAt cleared via
        // Post unpublish, sortAt = greatest(existedAt=2d, 0) = 2d). The
        // flush-thread fix calls remove_slot + insert_slot with the new ts.
        mgr.remove_slot(42);
        mgr.insert_slot(42, now - 2 * 86400, now);

        // 24h bucket no longer contains the slot — the bug fix.
        assert!(!mgr.get_bucket("24h").unwrap().bitmap().contains(42),
            "slot must drop from 24h bucket after sortAt regression");
        // 7d bucket still has it (sortAt is 2d ago, within 7d window).
        assert!(mgr.get_bucket("7d").unwrap().bitmap().contains(42),
            "slot must remain in 7d bucket because new ts is still in window");

        // Further regression past every bucket window: slot is fully evicted.
        mgr.remove_slot(42);
        mgr.insert_slot(42, now - 30 * 86400, now);
        assert!(!mgr.get_bucket("24h").unwrap().bitmap().contains(42));
        assert!(!mgr.get_bucket("7d").unwrap().bitmap().contains(42),
            "slot must drop from 7d once new ts is out of window");
    }

    /// Confirms the production leak and that a full rebuild is the cure.
    ///
    /// Steady-state maintenance is incremental: `subtract_expired` only removes
    /// slots whose current sort value lands in the thin `[old_cutoff, new_cutoff)`
    /// band swept by one refresh step. A slot that enters the 24h bucket with a
    /// recent value and then has its value regress *past* that band without a
    /// re-flush is never in the band, so incremental expiry misses it and it
    /// stays stuck — surfacing as "old data in the 24h bucket". A full rebuild
    /// from truth evicts it. This is exactly what the periodic full-rebuild
    /// fallback does.
    #[test]
    fn test_full_rebuild_evicts_slot_missed_by_incremental_band() {
        let mut mgr = make_manager(vec![("24h", 86400, 300)]);
        let now: u64 = 1_700_000_000;

        // Slot 7 enters the 24h bucket with a recent sort value.
        mgr.insert_slot(7, now - 100, now);
        assert!(mgr.get_bucket("24h").unwrap().bitmap().contains(7));

        // One incremental refresh step, 300s later. Its expiry band is
        // [old_cutoff, new_cutoff) = [now-86400, now+300-86400). Slot 7's true
        // value has since regressed to now-200000 — far BELOW old_cutoff, so it
        // is not in the band. Incremental expiry (empty band set) can't touch it.
        let now2 = now + 300;
        let new_cutoff = now2.saturating_sub(86400);
        let empty_band = RoaringBitmap::new();
        mgr.get_bucket_mut("24h")
            .unwrap()
            .subtract_expired(&empty_band, new_cutoff);
        assert!(
            mgr.get_bucket("24h").unwrap().bitmap().contains(7),
            "leak reproduced: incremental band-based expiry misses the regressed slot"
        );

        // Full rebuild from truth (slot 7's real value is old) drops it.
        let truth: Vec<(u32, u64)> = vec![(7, now2 - 200000)];
        mgr.rebuild_bucket("24h", truth.into_iter(), now2);
        assert!(
            !mgr.get_bucket("24h").unwrap().bitmap().contains(7),
            "full rebuild evicts the stale member — the fallback's cure"
        );
    }

    // ---- reconcile_apply (symmetric prune + backfill) ----

    /// Build a `SortField` (sortAt, 32-bit linear) with the given (slot, value)
    /// pairs set. `reconstruct_value` reads base|diff via `fused_contains`, so
    /// slots inserted here are visible without any merge/flush.
    fn make_sort_field(values: &[(u32, u64)]) -> crate::sort::SortField {
        let config = crate::config::SortFieldConfig {
            name: "sortAt".to_string(),
            source_type: "uint32".to_string(),
            encoding: "linear".to_string(),
            bits: 32,
            eager_load: false,
            computed: None,
        };
        let mut field = crate::sort::SortField::new(config);
        for (slot, val) in values {
            field.insert(*slot, *val as u32);
        }
        field
    }

    /// Build a single-bucket candidate tuple `(name, stale, missing)` as the
    /// worker would emit it.
    fn cands(name: &str, stale: &[u32], missing: &[u32]) -> Vec<(String, RoaringBitmap, RoaringBitmap)> {
        let mut s = RoaringBitmap::new();
        for x in stale {
            s.insert(*x);
        }
        let mut m = RoaringBitmap::new();
        for x in missing {
            m.insert(*x);
        }
        vec![(name.to_string(), s, m)]
    }

    fn alive_of(slots: &[u32]) -> RoaringBitmap {
        let mut a = RoaringBitmap::new();
        for x in slots {
            a.insert(*x);
        }
        a
    }

    /// (a) A stale candidate still out of window is pruned.
    /// (b) A stale candidate whose sort value moved back in-window since the
    ///     snapshot is NOT pruned (re-validation).
    #[test]
    fn reconcile_prunes_still_stale_but_keeps_moved_back_in_window() {
        let mut mgr = make_manager(vec![("24h", 86400, 300)]);
        let now: u64 = 1_700_000_000;

        // Both slots currently sit in the 24h bucket (per the snapshot they were
        // flagged stale). Slot 1's CURRENT value is old (still stale); slot 2's
        // CURRENT value moved back in-window since the scan.
        mgr.insert_slot(1, now - 100, now); // membership only; value comes from sort_field
        mgr.insert_slot(2, now - 100, now);
        let sort_field = make_sort_field(&[(1, now - 200_000), (2, now - 50)]);
        let alive = alive_of(&[1, 2]);

        let candidates = cands("24h", &[1, 2], &[]);
        let report = mgr.reconcile_apply(&sort_field, &alive, now, &candidates);

        assert_eq!(report, vec![("24h".to_string(), 1, 0)]);
        assert!(!mgr.get_bucket("24h").unwrap().bitmap().contains(1), "still-stale pruned");
        assert!(mgr.get_bucket("24h").unwrap().bitmap().contains(2), "moved-back-in-window kept");
    }

    /// (b2) A stale candidate that is DEAD (not alive) but whose uncleared sort
    ///      value is still in-window must be pruned unconditionally — a dead slot
    ///      must never linger in a bucket. (Flagged by GPT + Gemini review.)
    #[test]
    fn reconcile_prunes_dead_stale_even_if_value_in_window() {
        let mut mgr = make_manager(vec![("24h", 86400, 300)]);
        let now: u64 = 1_700_000_000;

        mgr.insert_slot(3, now - 100, now); // in the bucket
        // Sort value still reads in-window, but the slot is no longer alive.
        let sort_field = make_sort_field(&[(3, now - 100)]);
        let alive = RoaringBitmap::new(); // slot 3 dead

        let candidates = cands("24h", &[3], &[]);
        let report = mgr.reconcile_apply(&sort_field, &alive, now, &candidates);

        assert_eq!(report, vec![("24h".to_string(), 1, 0)]);
        assert!(!mgr.get_bucket("24h").unwrap().bitmap().contains(3), "dead in-window slot pruned");
    }

    /// A stale candidate live maintenance already removed from the bucket is a
    /// no-op — not counted as pruned, no dirty write. (Flagged by GPT review.)
    #[test]
    fn reconcile_skips_stale_already_absent_from_bucket() {
        let mut mgr = make_manager(vec![("24h", 86400, 300)]);
        let now: u64 = 1_700_000_000;

        // Slot 4 is NOT in the bucket (live maintenance dropped it since the scan),
        // and is out of window per current sort values.
        let sort_field = make_sort_field(&[(4, now - 200_000)]);
        let alive = alive_of(&[4]);

        let candidates = cands("24h", &[4], &[]);
        let report = mgr.reconcile_apply(&sort_field, &alive, now, &candidates);

        assert!(report.is_empty(), "no-op prune not reported");
    }

    /// (c) A missing candidate that is alive + in-window is backfilled.
    #[test]
    fn reconcile_backfills_alive_in_window_missing() {
        let mut mgr = make_manager(vec![("24h", 86400, 300)]);
        let now: u64 = 1_700_000_000;

        // Bucket is empty (slot 5 was dropped by the live path). Its CURRENT
        // value is in-window and it is alive → must be backfilled.
        let sort_field = make_sort_field(&[(5, now - 100)]);
        let alive = alive_of(&[5]);

        let candidates = cands("24h", &[], &[5]);
        let report = mgr.reconcile_apply(&sort_field, &alive, now, &candidates);

        assert_eq!(report, vec![("24h".to_string(), 0, 1)]);
        assert!(mgr.get_bucket("24h").unwrap().bitmap().contains(5), "in-window missing backfilled");
    }

    /// (d) A missing candidate deleted since the snapshot (not in `alive`) is
    ///     NOT inserted.
    #[test]
    fn reconcile_skips_missing_deleted_since_snapshot() {
        let mut mgr = make_manager(vec![("24h", 86400, 300)]);
        let now: u64 = 1_700_000_000;
        let sort_field = make_sort_field(&[(6, now - 100)]);
        let alive = RoaringBitmap::new(); // slot 6 no longer alive

        let candidates = cands("24h", &[], &[6]);
        let report = mgr.reconcile_apply(&sort_field, &alive, now, &candidates);

        assert!(report.is_empty(), "no change");
        assert!(!mgr.get_bucket("24h").unwrap().bitmap().contains(6), "dead candidate not inserted");
    }

    /// (e) A missing candidate whose sort value moved out of window since the
    ///     snapshot is NOT inserted.
    #[test]
    fn reconcile_skips_missing_moved_out_of_window() {
        let mut mgr = make_manager(vec![("24h", 86400, 300)]);
        let now: u64 = 1_700_000_000;
        let sort_field = make_sort_field(&[(8, now - 200_000)]); // out of 24h window now
        let alive = alive_of(&[8]);

        let candidates = cands("24h", &[], &[8]);
        let report = mgr.reconcile_apply(&sort_field, &alive, now, &candidates);

        assert!(report.is_empty(), "no change");
        assert!(!mgr.get_bucket("24h").unwrap().bitmap().contains(8), "out-of-window candidate not inserted");
    }

    /// (f) A missing candidate already present in the bucket is a no-op (no
    ///     double count).
    #[test]
    fn reconcile_skips_missing_already_present() {
        let mut mgr = make_manager(vec![("24h", 86400, 300)]);
        let now: u64 = 1_700_000_000;
        mgr.insert_slot(9, now - 100, now); // already in bucket
        let sort_field = make_sort_field(&[(9, now - 100)]);
        let alive = alive_of(&[9]);

        let candidates = cands("24h", &[], &[9]);
        let report = mgr.reconcile_apply(&sort_field, &alive, now, &candidates);

        assert!(report.is_empty(), "already-present is a no-op");
        assert!(mgr.get_bucket("24h").unwrap().bitmap().contains(9));
    }

    /// (g) A single bucket with both a stale and a missing slot: one apply removes
    ///     one and adds the other, reporting counts (1 pruned, 1 backfilled).
    #[test]
    fn reconcile_prunes_and_backfills_same_bucket_in_one_pass() {
        let mut mgr = make_manager(vec![("24h", 86400, 300)]);
        let now: u64 = 1_700_000_000;

        // Slot 1 is in the bucket but stale now; slot 2 is missing but in-window.
        mgr.insert_slot(1, now - 100, now);
        let sort_field = make_sort_field(&[(1, now - 200_000), (2, now - 100)]);
        let alive = alive_of(&[1, 2]);

        let candidates = cands("24h", &[1], &[2]);
        let report = mgr.reconcile_apply(&sort_field, &alive, now, &candidates);

        assert_eq!(report, vec![("24h".to_string(), 1, 1)]);
        assert!(!mgr.get_bucket("24h").unwrap().bitmap().contains(1), "stale removed");
        assert!(mgr.get_bucket("24h").unwrap().bitmap().contains(2), "missing added");
    }
}
