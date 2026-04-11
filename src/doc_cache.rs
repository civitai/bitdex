//! Generational document cache for DocStore.
//!
//! Replaces the flat DashMap + LRU timestamp scan with generational buckets.
//! Each generation is a time window's worth of cached entries. Reads promote
//! entries to the current (newest) generation. A dedicated eviction thread
//! drops the oldest generation wholesale — no scanning required.
//!
//! ## Design
//!
//! - **Lock-free reads**: `ArcSwap<Vec<Arc<Generation>>>` for the generation list
//! - **Cache-on-read**: First `get()` populates cache, subsequent reads hit memory
//! - **Write-through**: `update_if_cached()` updates existing entries (PR #58 semantics)
//! - **Generational eviction**: Background thread rotates generations and drops oldest
//!   when over budget. O(1) eviction vs O(n log n) LRU scan.
//! - **Promotion on read**: Entries accessed in older generations are moved to current

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use dashmap::DashMap;

use crate::shard_store_doc::StoredDoc;

/// Configuration for the generational document cache.
#[derive(Debug, Clone)]
pub struct DocCacheConfig {
    /// Maximum cache size in bytes. Eviction drops oldest generations when exceeded.
    pub max_bytes: u64,
    /// How often (in seconds) to rotate to a new generation. Default: 60.
    pub generation_interval_secs: u64,
    /// Maximum number of generations before merging the oldest two. Default: 30.
    pub max_generations: usize,
}

impl Default for DocCacheConfig {
    fn default() -> Self {
        DocCacheConfig {
            max_bytes: 1_073_741_824, // 1 GB
            generation_interval_secs: 60,
            max_generations: 30,
        }
    }
}

/// A cached document entry. Generation membership IS the recency signal —
/// no per-entry timestamp needed.
struct CachedEntry {
    doc: StoredDoc,
    /// Approximate size in bytes (fields + overhead).
    size_bytes: u64,
}

/// A single generation (time bucket) of cached entries.
pub struct Generation {
    entries: DashMap<u32, CachedEntry>,
    /// Total bytes in this generation (maintained atomically).
    size_bytes: AtomicU64,
    /// When this generation was created (for merge ordering).
    created_at: Instant,
}

impl Generation {
    fn new() -> Self {
        Generation {
            entries: DashMap::new(),
            size_bytes: AtomicU64::new(0),
            created_at: Instant::now(),
        }
    }

    fn with_created_at(created_at: Instant) -> Self {
        Generation {
            entries: DashMap::new(),
            size_bytes: AtomicU64::new(0),
            created_at,
        }
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn bytes(&self) -> u64 {
        self.size_bytes.load(Ordering::Relaxed)
    }
}

/// Generational document cache with lock-free reads via ArcSwap.
pub struct DocCache {
    /// Generation list: [0] = current (newest), [N] = oldest.
    generations: ArcSwap<Vec<Arc<Generation>>>,
    config: DocCacheConfig,
    /// Runtime override for max_bytes. When non-zero, takes precedence
    /// over `config.max_bytes`. Set via `set_max_bytes()` from the
    /// PATCH /config handler. Default 0 = use config value.
    max_bytes_override: AtomicU64,
    /// Runtime override for max_generations. When non-zero, takes
    /// precedence over `config.max_generations`. Default 0 = use config.
    max_generations_override: AtomicUsize,
    /// Cumulative cache hits.
    hits: AtomicU64,
    /// Cumulative cache misses.
    misses: AtomicU64,
    /// Cumulative evictions (entries dropped via generation eviction).
    evictions: AtomicU64,

    // --- Apr 11 2026: miss-path diagnostic counters (IntGaugeVec-synced) ---
    /// Highest slot_id ever inserted/promoted. Used to classify misses
    /// as "never_seen" (slot_id > max_seen = hot insert region) vs
    /// "below_water" (slot_id <= max_seen = evicted or never queried).
    max_seen_slot: AtomicU32,
    /// Hit count by which generation the entry was found in.
    /// Buckets match `gen_bucket_index`: 0, 1, 2-5, 6-15, 16-29.
    hits_by_gen_bucket: [AtomicU64; 5],
    /// Miss classified as "above_high_water": slot_id > max_seen_slot.
    misses_above_high_water: AtomicU64,
    /// Miss classified as "at_or_below_high_water": slot_id <= max_seen_slot
    /// but absent from every generation. Does NOT prove eviction — may
    /// also be a never-queried slot or persistent null doc.
    misses_at_or_below_high_water: AtomicU64,
    /// Write-through outcomes from the flush thread.
    writethrough_updated: AtomicU64,
    writethrough_skipped: AtomicU64,
}

/// Map a generation index (0 = newest) to its bucket label index.
/// Buckets: 0 -> "0", 1 -> "1", 2-5 -> "2-5", 6-15 -> "6-15", 16+ -> "16+".
/// The last bucket is open-ended so the labels stay valid even when
/// `max_generations` is bumped at runtime (e.g. 30 → 120 experiment).
#[inline]
fn gen_bucket_index(gen_idx: usize) -> usize {
    match gen_idx {
        0 => 0,
        1 => 1,
        2..=5 => 2,
        6..=15 => 3,
        _ => 4,
    }
}

/// Human-readable labels aligned with `gen_bucket_index` output positions.
pub const GEN_BUCKET_LABELS: [&str; 5] = ["0", "1", "2-5", "6-15", "16+"];

/// Outcome of `DocCache::apply_ops_in_place`. Used by the WAL reader
/// to tally per-entity accounting and route entries that need disk
/// reload through the fallback `doc_cache_refresh_slots` path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// All ops applied cleanly; cached doc is now current.
    Applied,
    /// Ops included `Delete`; cache entry removed.
    Deleted,
    /// Slot is not in cache — no action taken. Preserves
    /// cache-on-read semantics: cold slots stay cold until a query
    /// promotes them.
    NotCached,
    /// Ops included a shape we can't handle in-place (QueryOpSet,
    /// unparseable value, or a type-mismatched Add on a Single field).
    /// Caller should fall back to disk reload for this slot.
    NeedsFallback,
}

/// Convert a `serde_json::Value` (as carried by pg-sync Ops) to the
/// `query::Value` variant used by `FieldValue::Single` / `Multi`.
/// Returns `None` for shapes the cache can't represent (null, arrays,
/// nested objects). The caller routes those slots to the disk refresh
/// path so nothing silently diverges.
#[cfg(feature = "pg-sync")]
fn json_value_to_query_value(
    raw: &serde_json::Value,
) -> Option<crate::query::Value> {
    use crate::query::Value;
    match raw {
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(Value::Integer(i))
            } else {
                n.as_f64().map(Value::Float)
            }
        }
        serde_json::Value::Bool(b) => Some(Value::Bool(*b)),
        serde_json::Value::String(s) => Some(Value::String(s.clone())),
        _ => None,
    }
}

impl DocCache {
    /// Create a new generational document cache with one empty generation.
    pub fn new(config: DocCacheConfig) -> Self {
        let initial_gen = Arc::new(Generation::new());
        DocCache {
            generations: ArcSwap::from_pointee(vec![initial_gen]),
            config,
            max_bytes_override: AtomicU64::new(0),
            max_generations_override: AtomicUsize::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            max_seen_slot: AtomicU32::new(0),
            hits_by_gen_bucket: [
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
            ],
            misses_above_high_water: AtomicU64::new(0),
            misses_at_or_below_high_water: AtomicU64::new(0),
            writethrough_updated: AtomicU64::new(0),
            writethrough_skipped: AtomicU64::new(0),
        }
    }

    /// Look up a document in the cache. Scans from current to oldest generation.
    /// Promotes entries found in older generations to the current one.
    pub fn get(&self, slot_id: u32) -> Option<StoredDoc> {
        let gens = self.generations.load();

        for (i, gen) in gens.iter().enumerate() {
            if let Some(entry) = gen.entries.get(&slot_id) {
                let doc = entry.doc.clone();
                // Record hit bucket BEFORE any promotion so instrumentation
                // reflects where the entry actually was, not where it landed.
                let bucket = gen_bucket_index(i);
                self.hits_by_gen_bucket[bucket].fetch_add(1, Ordering::Relaxed);

                if i == 0 {
                    // Already in current generation — fast path
                    self.hits.fetch_add(1, Ordering::Relaxed);
                    return Some(doc);
                }
                // Promote: move from old gen to current
                let size = entry.size_bytes;
                drop(entry); // release DashMap ref before remove
                self.promote(slot_id, gen, &gens[0], size, doc.clone());
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Some(doc);
            }
        }

        // Miss: classify by slot_id vs max-seen high water mark.
        // This is NOT a proof of the miss cause; it's a structural split:
        //   above_high_water — slot_id is higher than any slot the cache
        //     has ever held (via insert/promote). Strong signal that
        //     it's a recent insert that queries are already hunting for.
        //   at_or_below_high_water — slot_id is within the range the
        //     cache has observed. Could be evicted, could be a slot
        //     that was never queried (cache-on-read skipped it), could
        //     be a slot with a persistent null doc. Requires cross-
        //     referencing with writethrough + hit_generation to
        //     interpret.
        self.misses.fetch_add(1, Ordering::Relaxed);
        let high_water = self.max_seen_slot.load(Ordering::Relaxed);
        if slot_id > high_water {
            self.misses_above_high_water.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses_at_or_below_high_water.fetch_add(1, Ordering::Relaxed);
        }
        None
    }

    /// Move an entry from one generation to another.
    fn promote(&self, slot_id: u32, from: &Generation, to: &Generation, size: u64, doc: StoredDoc) {
        // Remove from old generation (may be None if another thread promoted concurrently)
        if from.entries.remove(&slot_id).is_some() {
            from.size_bytes.fetch_sub(size, Ordering::Relaxed);
        }
        // Insert into current generation
        to.entries.insert(slot_id, CachedEntry { doc, size_bytes: size });
        to.size_bytes.fetch_add(size, Ordering::Relaxed);
    }

    /// Insert a document into the current (newest) generation.
    pub fn insert(&self, slot_id: u32, doc: StoredDoc) {
        let size = estimate_doc_size(&doc);
        let gens = self.generations.load();

        // Remove from EVERY generation before inserting. Normally only
        // one copy exists (promote/insert preserve single-entry), but
        // scan all defensively — a single stale entry in an older
        // generation would become visible after remove() exposes it.
        // Gemini caught this as a theoretical race worth closing.
        for gen in gens.iter() {
            if let Some((_, old)) = gen.entries.remove(&slot_id) {
                gen.size_bytes.fetch_sub(old.size_bytes, Ordering::Relaxed);
            }
        }

        // Insert into current generation [0]
        if let Some(current) = gens.first() {
            current.entries.insert(slot_id, CachedEntry { doc, size_bytes: size });
            current.size_bytes.fetch_add(size, Ordering::Relaxed);
        }

        // Bump high water mark used by miss-reason classification.
        self.bump_max_seen(slot_id);
    }

    /// Update max_seen_slot atomically. Only grows — never decreases.
    /// Called from insert paths so miss-reason classification knows
    /// which slot ids have ever lived in the cache. Uses `fetch_max`
    /// (stable since Rust 1.45) instead of a manual CAS loop.
    #[inline]
    fn bump_max_seen(&self, slot_id: u32) {
        self.max_seen_slot.fetch_max(slot_id, Ordering::Relaxed);
    }

    /// Insert a batch of documents into the cache.
    pub fn insert_batch(&self, docs: &[(u32, StoredDoc)]) {
        for (slot_id, doc) in docs {
            self.insert(*slot_id, doc.clone());
        }
    }

    /// Update documents that are already in the cache; skip new ones.
    ///
    /// Used by the flush thread for write-through: only update docs that
    /// queries have already loaded (cache-on-read). New docs from pg-sync
    /// mutations go straight to disk without filling the cache with cold
    /// entries that may never be queried.
    pub fn update_batch_if_cached(&self, docs: &[(u32, StoredDoc)]) {
        let gens = self.generations.load();

        let mut updated: u64 = 0;
        let mut skipped: u64 = 0;

        for (slot_id, doc) in docs {
            let new_size = estimate_doc_size(doc);

            // Find in any generation and update in-place (don't promote — writes aren't reads)
            let mut found = false;
            for gen in gens.iter() {
                if let Some(mut existing) = gen.entries.get_mut(slot_id) {
                    let old_size = existing.size_bytes;
                    existing.doc = doc.clone();
                    existing.size_bytes = new_size;
                    if new_size > old_size {
                        gen.size_bytes.fetch_add(new_size - old_size, Ordering::Relaxed);
                    } else {
                        gen.size_bytes.fetch_sub(old_size - new_size, Ordering::Relaxed);
                    }
                    found = true;
                    break;
                }
            }
            if found {
                updated += 1;
            } else {
                skipped += 1;
            }
            // Not found = skip. Doc goes to disk only.
        }

        if updated > 0 {
            self.writethrough_updated.fetch_add(updated, Ordering::Relaxed);
        }
        if skipped > 0 {
            self.writethrough_skipped.fetch_add(skipped, Ordering::Relaxed);
        }
    }

    /// Remove a document from the cache (on delete). Does NOT bump the
    /// `evictions` counter because that would conflate LRU/retention
    /// pressure with explicit application-driven deletes, muddying
    /// cache-pressure dashboards. Explicit deletes are tracked via the
    /// caller-side `doc_cache_live_update_total{result="deleted"}`
    /// counter instead. Removes from ALL generations defensively,
    /// even though insert() maintains the single-entry invariant —
    /// covers theoretical races that could leave a stale entry in an
    /// older generation.
    pub fn remove(&self, slot_id: u32) {
        let gens = self.generations.load();
        for gen in gens.iter() {
            if let Some((_, entry)) = gen.entries.remove(&slot_id) {
                gen.size_bytes.fetch_sub(entry.size_bytes, Ordering::Relaxed);
            }
        }
    }

    /// Check whether a slot is currently cached WITHOUT promoting it
    /// to the current generation. Used by maintenance paths (e.g. the
    /// ops-driven refresh path) where a `get()` would incorrectly
    /// count as a user hit and skew `hits_by_gen_bucket` stats.
    pub fn contains(&self, slot_id: u32) -> bool {
        let gens = self.generations.load();
        for gen in gens.iter() {
            if gen.entries.contains_key(&slot_id) {
                return true;
            }
        }
        false
    }

    /// Apr 11 2026: Apply pg-sync ops directly to a cached StoredDoc,
    /// avoiding the disk read in `doc_cache_refresh_slots`.
    ///
    /// This is the hot path for the WAL reader on steady-state sync.
    /// The previous `refresh_slots` implementation re-read every
    /// touched cached slot from disk via `DocStoreV3::get_many`, which
    /// dispatches to the global rayon pool and steals worker threads
    /// from query handlers. Under high cache saturation that contention
    /// dominated the latency tail (see `tokio_return_delay` spike at
    /// T+30 on v1.0.185). Applying ops in-place keeps the hot cache
    /// state coherent with zero disk I/O.
    ///
    /// The caller decides which ops are safe to apply in-place. This
    /// method assumes:
    /// - Set/Remove/Add on non-computed-source fields are safe
    /// - Fields that feed computed deps (e.g. `scannedAt` feeding
    ///   `sortAt`) need disk reload for the computed value to refresh
    /// - QueryOpSet needs query resolution first (not handled here)
    ///
    /// Returns `ApplyOutcome::NotCached` if the slot isn't in the
    /// cache at all — no-op, preserves cache-on-read semantics.
    #[cfg(feature = "pg-sync")]
    pub fn apply_ops_in_place(
        &self,
        slot_id: u32,
        ops: &[crate::pg_sync::ops::Op],
    ) -> ApplyOutcome {
        use crate::pg_sync::ops::Op;
        use crate::mutation::FieldValue;

        if ops.is_empty() {
            return ApplyOutcome::NotCached;
        }

        let gens = self.generations.load();

        // Find the slot's entry via DashMap get_mut. Holding the shard
        // lock across op application is fine — no disk I/O, no allocs
        // outside the entry's own HashMap. Single-threaded WAL reader
        // means no concurrent writer contention on the same entry.
        //
        // GPT review H1 — Apr 11 2026: apply ops to a *clone* of the
        // stored doc, then atomically swap it back only if every op
        // succeeded. The previous version mutated `entry.doc` in place
        // and early-returned `NeedsFallback` from inside the loop,
        // which left the cached doc in a half-applied state on bail.
        // While `doc_cache_apply_ops_batch`'s fallback path eventually
        // overwrote the entry from disk, the transient window had
        // divergent state and `size_bytes` was never updated for the
        // partial mutation. Clone-then-swap gives all-or-nothing
        // semantics: either every op lands or the entry is untouched
        // and the caller routes to the disk-refresh fallback. The
        // clone cost is ~1 doc per WAL batch (hundreds per second
        // in prod) — cheaper than one `get_many` disk read, which
        // is what we're replacing.
        for gen in gens.iter() {
            if let Some(mut entry) = gen.entries.get_mut(&slot_id) {
                let old_size = entry.size_bytes;

                // Apply to a clone. Delete short-circuits before the
                // swap because it drops the slot entirely — no swap
                // needed, just remove.
                let mut working = entry.doc.clone();
                let mut deleted = false;

                for i in 0..ops.len() {
                    let op = &ops[i];
                    match op {
                        Op::Set { field, value } => {
                            match json_value_to_query_value(value) {
                                Some(v) => {
                                    working.fields.insert(
                                        field.clone(),
                                        FieldValue::Single(v),
                                    );
                                }
                                None => return ApplyOutcome::NeedsFallback,
                            }
                        }
                        Op::Remove { field, value } => {
                            match working.fields.get_mut(field) {
                                Some(FieldValue::Multi(vec)) => {
                                    if let Some(target) = json_value_to_query_value(value) {
                                        vec.retain(|v| v != &target);
                                    }
                                }
                                Some(FieldValue::Single(_)) => {
                                    // Scalar remove is the "old value"
                                    // half of a remove/set pair in
                                    // pg-sync's ops protocol. Common
                                    // case, but nothing in the wire
                                    // format guarantees a paired Set
                                    // exists (SET NULL, trigger bugs,
                                    // future op shapes). Bail unless
                                    // a matching Set follows in the
                                    // same ops list so disk refresh
                                    // reloads authoritative state.
                                    // Gemini review H1 — Apr 11 2026.
                                    let has_paired_set = ops[i + 1..].iter().any(|later| {
                                        matches!(
                                            later,
                                            Op::Set { field: f, .. } if f == field
                                        )
                                    });
                                    if has_paired_set {
                                        working.fields.remove(field);
                                    } else {
                                        return ApplyOutcome::NeedsFallback;
                                    }
                                }
                                None => {
                                    // No current value — no-op.
                                }
                            }
                        }
                        Op::Add { field, value } => {
                            let target = match json_value_to_query_value(value) {
                                Some(v) => v,
                                None => return ApplyOutcome::NeedsFallback,
                            };
                            match working.fields.get_mut(field) {
                                Some(FieldValue::Multi(vec)) => {
                                    if !vec.iter().any(|v| v == &target) {
                                        vec.push(target);
                                    }
                                }
                                Some(FieldValue::Single(_)) => {
                                    // Type mismatch on a single-value
                                    // field. The upfront check in
                                    // `doc_cache_apply_ops_batch` only
                                    // knows about computed deps, not
                                    // type shape, so this is the
                                    // correct place to bail.
                                    return ApplyOutcome::NeedsFallback;
                                }
                                None => {
                                    working.fields.insert(
                                        field.clone(),
                                        FieldValue::Multi(vec![target]),
                                    );
                                }
                            }
                        }
                        Op::Delete => {
                            deleted = true;
                            break;
                        }
                        Op::Alive => {
                            // Creates-slot signal affects the alive
                            // bitmap only. StoredDoc already exists
                            // (that's why we found it in the cache).
                        }
                        Op::QueryOpSet { .. } => {
                            // Fan-out ops need query resolution; the
                            // caller routes these to the refresh path.
                            return ApplyOutcome::NeedsFallback;
                        }
                    }
                }

                if deleted {
                    // Drop the DashMap ref before calling remove() —
                    // remove() re-acquires the shard lock and would
                    // deadlock if we still held get_mut.
                    let freed = entry.size_bytes;
                    drop(entry);
                    gen.entries.remove(&slot_id);
                    gen.size_bytes.fetch_sub(freed, Ordering::Relaxed);
                    return ApplyOutcome::Deleted;
                }

                // Commit: swap the working clone into the entry and
                // update size accounting.
                let new_size = estimate_doc_size(&working);
                entry.doc = working;
                entry.size_bytes = new_size;
                drop(entry);
                if new_size >= old_size {
                    gen.size_bytes.fetch_add(new_size - old_size, Ordering::Relaxed);
                } else {
                    gen.size_bytes.fetch_sub(old_size - new_size, Ordering::Relaxed);
                }
                return ApplyOutcome::Applied;
            }
        }

        ApplyOutcome::NotCached
    }

    /// Push a new empty generation to the front (current position).
    /// If over max_generations, merges the two oldest first.
    pub fn push_new_generation(&self) {
        let old_gens = self.generations.load();
        let mut new_gens: Vec<Arc<Generation>> = Vec::with_capacity(old_gens.len() + 1);

        // New current generation at front
        new_gens.push(Arc::new(Generation::new()));

        // Copy existing generations
        for gen in old_gens.iter() {
            new_gens.push(Arc::clone(gen));
        }

        // If over cap, merge the two oldest into one. Loops so that
        // shrinking via runtime PATCH (e.g. 120 → 30) converges in a
        // single rotation instead of taking 90 rotations (~90 min) to
        // merge one pair per cycle. Gemini's catch.
        while new_gens.len() > self.effective_max_generations() {
            let before = new_gens.len();
            self.merge_oldest(&mut new_gens);
            if new_gens.len() >= before {
                // merge_oldest is a no-op if len < 2 — break to avoid
                // infinite loops in edge cases.
                break;
            }
        }

        self.generations.store(Arc::new(new_gens));
    }

    /// Merge the two oldest generations (last two in vec) into one.
    fn merge_oldest(&self, gens: &mut Vec<Arc<Generation>>) {
        if gens.len() < 2 {
            return;
        }

        let oldest = gens.pop().unwrap();
        let second_oldest = gens.pop().unwrap();

        // Determine which is smaller to iterate, merge into the larger
        let (smaller, larger) = if oldest.len() <= second_oldest.len() {
            (oldest, second_oldest)
        } else {
            (second_oldest, oldest)
        };

        // Use the older created_at to preserve eviction ordering
        let merged_created_at = if smaller.created_at < larger.created_at {
            smaller.created_at
        } else {
            larger.created_at
        };

        // Move entries from smaller into larger
        for entry in smaller.entries.iter() {
            let slot_id = *entry.key();
            // Only insert if not already present in larger (newer wins)
            if !larger.entries.contains_key(&slot_id) {
                let cached = entry.value();
                larger.entries.insert(slot_id, CachedEntry {
                    doc: cached.doc.clone(),
                    size_bytes: cached.size_bytes,
                });
                larger.size_bytes.fetch_add(cached.size_bytes, Ordering::Relaxed);
            }
        }

        // Create merged generation with correct timestamp
        let merged = Arc::new(Generation::with_created_at(merged_created_at));
        // Move all entries from larger into merged
        for entry in larger.entries.iter() {
            let slot_id = *entry.key();
            let cached = entry.value();
            merged.entries.insert(slot_id, CachedEntry {
                doc: cached.doc.clone(),
                size_bytes: cached.size_bytes,
            });
        }
        merged.size_bytes.store(
            larger.bytes() + smaller.entries.iter()
                .filter(|e| !larger.entries.contains_key(e.key()))
                .map(|e| e.value().size_bytes)
                .sum::<u64>(),
            Ordering::Relaxed,
        );

        // Actually, the simpler approach: just reuse larger's data since we already merged into it
        // But we can't change created_at on an existing Generation...
        // So let's just push the larger back — it has all the merged data
        // and we'll accept its created_at (which is close enough for eviction ordering)
        gens.push(larger);

        // Subtract smaller's bytes — they were already added to larger above
        // The smaller gen will be dropped when its Arc refcount hits zero
    }

    /// Drop the oldest generation. Returns the number of entries evicted.
    pub fn drop_oldest_generation(&self) -> usize {
        let old_gens = self.generations.load();
        if old_gens.len() <= 1 {
            return 0; // Never drop the current generation
        }

        let new_gens: Vec<Arc<Generation>> = old_gens[..old_gens.len() - 1].to_vec();
        let evicted_gen = &old_gens[old_gens.len() - 1];
        let evicted_count = evicted_gen.len();

        self.generations.store(Arc::new(new_gens));
        self.evictions.fetch_add(evicted_count as u64, Ordering::Relaxed);

        evicted_count
    }

    /// Total cache size in bytes across all generations.
    pub fn total_bytes(&self) -> u64 {
        let gens = self.generations.load();
        gens.iter().map(|g| g.bytes()).sum()
    }

    /// Alias for total_bytes (API compatibility).
    pub fn size_bytes(&self) -> u64 {
        self.total_bytes()
    }

    /// Number of entries across all generations.
    pub fn len(&self) -> usize {
        let gens = self.generations.load();
        gens.iter().map(|g| g.len()).sum()
    }

    /// Number of active generations.
    pub fn generation_count(&self) -> usize {
        self.generations.load().len()
    }

    /// Cache hit count.
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Cache miss count.
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    /// Cache eviction count.
    pub fn eviction_count(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }

    /// Check if eviction is needed. Provided for API compatibility but
    /// the eviction thread handles this — callers should not evict inline.
    pub fn needs_eviction(&self) -> bool {
        self.total_bytes() > self.effective_max_bytes()
    }

    /// Legacy eviction method — triggers drop of oldest generations until under budget.
    /// Prefer using the dedicated eviction thread instead.
    pub fn evict(&self) -> u64 {
        let max = self.effective_max_bytes();
        let mut total_evicted = 0u64;
        while self.total_bytes() > max {
            if self.generation_count() <= 1 {
                break;
            }
            total_evicted += self.drop_oldest_generation() as u64;
        }
        total_evicted
    }

    /// Clear the entire cache.
    pub fn clear(&self) {
        let new_gen = Arc::new(Generation::new());
        self.generations.store(Arc::new(vec![new_gen]));
    }

    /// Effective max_bytes: runtime override if set, otherwise config default.
    pub fn effective_max_bytes(&self) -> u64 {
        let ovr = self.max_bytes_override.load(Ordering::Relaxed);
        if ovr > 0 { ovr } else { self.config.max_bytes }
    }

    /// Get the max_bytes config value (ignores runtime override).
    pub fn max_bytes(&self) -> u64 {
        self.config.max_bytes
    }

    /// Set max_bytes at runtime. Takes effect immediately — the eviction
    /// thread will stop evicting once total_bytes drops below the new limit.
    /// Pass 0 to revert to the config default.
    pub fn set_max_bytes(&self, new_max: u64) {
        self.max_bytes_override.store(new_max, Ordering::Relaxed);
    }

    /// Get the generation interval in seconds.
    pub fn generation_interval_secs(&self) -> u64 {
        self.config.generation_interval_secs
    }

    /// Get the configured max generations count (ignores runtime override).
    /// Use `effective_max_generations()` for the value currently in force.
    pub fn configured_max_generations(&self) -> usize {
        self.config.max_generations
    }

    /// Effective max_generations: runtime override if set, otherwise config.
    pub fn effective_max_generations(&self) -> usize {
        let ovr = self.max_generations_override.load(Ordering::Relaxed);
        if ovr > 0 { ovr } else { self.config.max_generations }
    }

    /// Set max_generations at runtime. Takes effect on the next generation
    /// rotation in the eviction thread. Pass 0 to revert to the config default.
    /// Expands retention window without restart — the experiment lever for
    /// testing whether the floor is retention-bound.
    pub fn set_max_generations(&self, new_max: usize) {
        self.max_generations_override.store(new_max, Ordering::Relaxed);
    }

    // --- Apr 11 2026 diagnostic accessors (synced into IntGaugeVecs) ---

    /// Cumulative hits bucketed by which generation held the entry.
    /// Indexes align with `GEN_BUCKET_LABELS`.
    pub fn hits_by_gen(&self) -> [u64; 5] {
        [
            self.hits_by_gen_bucket[0].load(Ordering::Relaxed),
            self.hits_by_gen_bucket[1].load(Ordering::Relaxed),
            self.hits_by_gen_bucket[2].load(Ordering::Relaxed),
            self.hits_by_gen_bucket[3].load(Ordering::Relaxed),
            self.hits_by_gen_bucket[4].load(Ordering::Relaxed),
        ]
    }

    /// Cumulative misses where slot_id > max_seen_slot (hot insert region).
    pub fn misses_above_high_water(&self) -> u64 {
        self.misses_above_high_water.load(Ordering::Relaxed)
    }

    /// Cumulative misses where slot_id <= max_seen_slot (in range but
    /// not present: evicted, never queried, or persistent null doc).
    pub fn misses_at_or_below_high_water(&self) -> u64 {
        self.misses_at_or_below_high_water.load(Ordering::Relaxed)
    }

    /// Write-through outcomes: (updated, skipped).
    pub fn writethrough_counts(&self) -> (u64, u64) {
        (
            self.writethrough_updated.load(Ordering::Relaxed),
            self.writethrough_skipped.load(Ordering::Relaxed),
        )
    }

    /// High water mark for max slot_id ever observed by insert/promote.
    pub fn max_seen_slot(&self) -> u32 {
        self.max_seen_slot.load(Ordering::Relaxed)
    }
}

/// Run the doc cache eviction thread. Rotates generations and drops oldest
/// when over memory budget. Should be spawned as a dedicated thread.
pub fn eviction_thread(cache: Arc<DocCache>, shutdown: Arc<AtomicBool>) {
    let check_interval = Duration::from_secs(5);
    let gen_interval = Duration::from_secs(cache.config.generation_interval_secs);
    let mut last_rotation = Instant::now();

    while !shutdown.load(Ordering::Relaxed) {
        std::thread::sleep(check_interval);

        // Rotate: push new generation periodically
        if last_rotation.elapsed() >= gen_interval {
            cache.push_new_generation();
            last_rotation = Instant::now();
            tracing::debug!(
                "doc cache: rotated generation (now {} gens, {} entries, {} bytes)",
                cache.generation_count(),
                cache.len(),
                cache.total_bytes(),
            );
        }

        // Evict: drop oldest generations until under budget
        while cache.total_bytes() > cache.effective_max_bytes() {
            if cache.generation_count() <= 1 {
                break;
            }
            let evicted = cache.drop_oldest_generation();
            tracing::info!(
                "doc cache: evicted oldest generation ({evicted} entries, now {} gens, {} bytes)",
                cache.generation_count(),
                cache.total_bytes(),
            );
        }
    }
}

// -----------------------------------------------------------------------------
// Honest per-entry memory accounting for StoredDoc.
//
// Prior accounting (pre-v1.0.157) used a flat 128-byte "HashMap overhead"
// constant and counted String / Value payloads at their exact byte length
// (`24 + s.len()`). This undercounted real heap cost by ~4x because it ignored:
//
//   (1) The DashMap bucket overhead per cached entry.
//   (2) The CachedEntry + StoredDoc struct headers (sizes known at compile
//       time, not estimated).
//   (3) The hashbrown bucket table capacity for the inner
//       `HashMap<String, FieldValue>`, which for N entries is roughly
//       `next_power_of_two(ceil(N / 0.875)) * (sizeof(K) + sizeof(V) + 1)`.
//   (4) jemalloc size-class rounding on every String allocation. jemalloc
//       rounds small allocations to 8 / 16 / 32 / 48 / 64 / 80 / 96 / 112 /
//       128-byte classes, so a 7-byte field name ("hasMeta") costs 16 bytes
//       of heap, not 7.
//   (5) FieldValue::Multi's `Vec<Value>` backing-array capacity, which
//       follows the same power-of-two growth pattern.
//
// The v1.0.156 OOM was caused by the 4x undercount combined with the
// 1 -> 4 GiB doc_cache budget bump in PR #156: the "4 GiB" budget was
// actually holding ~16 GiB of real heap. The hotfix reverted the budget to
// 1 GiB; this function now makes the budget honest against the ~4 GiB real
// ceiling so the next budget bump can be informed.
//
// We deliberately over-estimate where the real answer depends on jemalloc
// internals. Over-counting wastes a small fraction of the cache budget;
// under-counting OOMs the pod.
// -----------------------------------------------------------------------------

/// Round a raw allocation length up to the nearest jemalloc small size class.
///
/// Size classes used here (bytes): 8, 16, 32, 48, 64, 80, 96, 112, 128, then
/// 160, 192, 224, 256, then powers of two. Anything >= 4096 is treated as
/// page-aligned. This matches jemalloc's default config closely enough for
/// budget accounting; we do not claim page-perfect accuracy.
fn jemalloc_rounded(bytes: u64) -> u64 {
    if bytes == 0 {
        return 0;
    }
    const SMALL: [u64; 9] = [8, 16, 32, 48, 64, 80, 96, 112, 128];
    for &c in &SMALL {
        if bytes <= c {
            return c;
        }
    }
    if bytes <= 256 {
        // 160 / 192 / 224 / 256 classes
        return ((bytes + 31) / 32) * 32;
    }
    if bytes <= 4096 {
        // Power-of-two classes up to the 4 KiB page.
        return bytes.next_power_of_two();
    }
    // Page-aligned beyond the small/large boundary.
    (bytes + 4095) & !4095
}

/// Backing-array bytes for a hashbrown `HashMap<K, V>` with `len` entries.
///
/// hashbrown targets ~87.5% load. Capacity is rounded up to the next power
/// of two, with a minimum of 4 buckets once the map is non-empty. Each
/// bucket stores `(K, V)` plus one control byte; the allocator then rounds
/// the whole table.
fn hashbrown_backing_bytes(len: usize, entry_bytes: u64) -> u64 {
    if len == 0 {
        // Empty HashMap doesn't allocate a backing table.
        return 0;
    }
    let target = ((len as u64 * 8) / 7).max(4);
    let buckets = target.next_power_of_two();
    // (K, V) + 1 control byte, rounded up to the entry alignment (assume 8).
    let per_bucket = ((entry_bytes + 1 + 7) / 8) * 8;
    jemalloc_rounded(buckets * per_bucket)
}

/// Heap cost of a `String` holding `len` bytes.
///
/// Header is accounted at the call site (it is either inline in a parent
/// struct or counted as part of that struct's `size_of`). This function
/// returns only the heap allocation cost for the bytes themselves.
fn string_heap_bytes(len: usize) -> u64 {
    // std::String allocates `cap` bytes. After a `to_string()` on `&str`,
    // `cap == len`. jemalloc rounds the allocation up to a size class.
    jemalloc_rounded(len as u64)
}

/// Estimate the in-memory footprint of a single cached StoredDoc entry,
/// including:
///
///   - The DashMap bucket overhead (one entry in the per-shard hashbrown).
///   - The `CachedEntry` + `StoredDoc` struct headers.
///   - The inner `HashMap<String, FieldValue>` backing table capacity.
///   - Every `String` key and every `String` payload inside `FieldValue`,
///     padded to jemalloc size classes.
///   - Every `Vec<Value>` backing array inside `FieldValue::Multi`.
fn estimate_doc_size(doc: &StoredDoc) -> u64 {
    use std::mem::size_of;
    use crate::mutation::FieldValue;

    let key_val_bytes = size_of::<String>() as u64 + size_of::<FieldValue>() as u64;

    // (1) DashMap bucket overhead: DashMap keeps one hashbrown shard per
    //     concurrency level, each holding `(u32, CachedEntry)` buckets.
    //     Amortize the shard's bucket-table cost across its entries by
    //     treating one `(u32, CachedEntry)` bucket as the per-entry cost.
    //     The CachedEntry itself is counted below.
    let dashmap_bucket = jemalloc_rounded(
        size_of::<u32>() as u64 + size_of::<CachedEntry>() as u64 + 1,
    );

    // (2) Fixed struct headers (CachedEntry wraps StoredDoc).
    //     size_of::<CachedEntry>() already contains StoredDoc's HashMap header
    //     and the schema_version byte, so no double counting.
    let fixed_header = size_of::<CachedEntry>() as u64;

    // (3) Inner HashMap<String, FieldValue> backing table.
    let inner_backing = hashbrown_backing_bytes(doc.fields.len(), key_val_bytes);

    // (4) + (5) Per-field String keys and FieldValue payloads.
    let mut fields_heap = 0u64;
    for (key, value) in &doc.fields {
        fields_heap += string_heap_bytes(key.len());
        fields_heap += estimate_field_value_size(value);
    }

    dashmap_bucket + fixed_header + inner_backing + fields_heap
}

/// Heap cost of a `FieldValue`'s payload (excluding the enum tag, which is
/// counted as part of the inner HashMap's value slot).
fn estimate_field_value_size(value: &crate::mutation::FieldValue) -> u64 {
    use crate::mutation::FieldValue;
    use std::mem::size_of;
    match value {
        FieldValue::Single(v) => estimate_value_payload_bytes(v),
        FieldValue::Multi(values) => {
            // Vec<Value> backing array: capacity rounded to jemalloc class.
            // A freshly-pushed Vec typically has capacity == len (after
            // shrink) or next_power_of_two(len) before shrink. We assume
            // the latter since StoredDocs are rarely shrink_to_fit'd.
            let cap = (values.len() as u64).max(1).next_power_of_two();
            let backing = jemalloc_rounded(cap * size_of::<crate::query::Value>() as u64);
            let payloads: u64 = values.iter()
                .map(estimate_value_payload_bytes)
                .sum();
            backing + payloads
        }
    }
}

/// Heap cost of a `Value`'s payload, NOT counting the inline enum storage
/// (that is counted by the parent container's `size_of`).
fn estimate_value_payload_bytes(value: &crate::query::Value) -> u64 {
    use crate::query::Value;
    match value {
        Value::Integer(_) | Value::Float(_) | Value::Bool(_) => 0,
        Value::String(s) => string_heap_bytes(s.len()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mutation::FieldValue;
    use crate::query::Value;

    fn make_doc(fields: Vec<(&str, FieldValue)>) -> StoredDoc {
        StoredDoc {
            fields: fields.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
            schema_version: 0,
        }
    }

    /// Build a doc that mimics Civitai image entries: ~11 fields, mix of
    /// integers, booleans, and one or two short strings. Used as the
    /// reference shape for the honest-accounting tests below.
    fn make_realistic_doc() -> StoredDoc {
        make_doc(vec![
            ("slot", FieldValue::Single(Value::Integer(123_456_789))),
            ("postId", FieldValue::Single(Value::Integer(98_765_432))),
            ("postedToId", FieldValue::Single(Value::Integer(1_234))),
            ("userId", FieldValue::Single(Value::Integer(5_678_901))),
            ("sortAt", FieldValue::Single(Value::Integer(1_717_000_000))),
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("isPublished", FieldValue::Single(Value::Bool(true))),
            ("hasMeta", FieldValue::Single(Value::Bool(true))),
            ("minor", FieldValue::Single(Value::Bool(false))),
            ("url", FieldValue::Single(Value::String(
                "xG1nkqKTMzGDvpLrqFT7WA/abcd1234-uuid-guid-ab12-0123456789ab".into(),
            ))),
            ("tagIds", FieldValue::Multi(vec![
                Value::Integer(1), Value::Integer(2), Value::Integer(3),
                Value::Integer(4), Value::Integer(5), Value::Integer(6),
            ])),
        ])
    }

    #[test]
    fn estimate_doc_size_accounts_for_real_overhead() {
        // A realistic Civitai doc should come out meaningfully larger than
        // the sum of its String bytes — the old estimator returned ~260
        // bytes for this shape, which was a ~4x undercount against the
        // real jemalloc footprint observed in prod. The new estimator
        // must clear a lower bound that reflects:
        //
        //   - 2x header overhead (DashMap bucket + CachedEntry/StoredDoc)
        //   - The inner HashMap's 16-bucket hashbrown backing table
        //   - String allocator padding on every field name and value
        //   - The Multi<Value> vec backing array
        //
        // We use >= 700 bytes as the floor (realistic minimum) and
        // <= 2000 bytes as the ceiling (guards against runaway estimation
        // if a future refactor inflates a constant).
        let doc = make_realistic_doc();
        let estimated = estimate_doc_size(&doc);
        assert!(
            estimated >= 700,
            "realistic doc estimate {} bytes is too low — \
             honest accounting should account for hashbrown backing, \
             DashMap bucket overhead, and allocator padding",
            estimated,
        );
        assert!(
            estimated <= 2000,
            "realistic doc estimate {} bytes is implausibly high — \
             check for double counting of the HashMap header or the \
             CachedEntry struct",
            estimated,
        );
    }

    #[test]
    fn estimate_doc_size_budget_capacity_is_honest() {
        // With the 1 GiB default budget and the realistic doc shape
        // above, the cache should hold at least ~500K and no more than
        // ~2M entries. The old estimator returned ~260 bytes/entry,
        // implying 4.1M entries at 1 GiB — which then ballooned into
        // real heap that crossed the pod limit. The new estimator
        // should land in a window that matches the ~550K-600K entry
        // counts observed in prod at the previous 1 GiB budget.
        let doc = make_realistic_doc();
        let per_entry = estimate_doc_size(&doc);
        let budget = 1_073_741_824u64; // 1 GiB
        let entries = budget / per_entry;
        assert!(
            (300_000..=2_000_000).contains(&entries),
            "1 GiB / {} bytes per entry = {} entries — outside the \
             plausible 300K-2M window for Civitai doc shapes",
            per_entry,
            entries,
        );
    }

    #[test]
    fn jemalloc_rounding_matches_size_classes() {
        assert_eq!(jemalloc_rounded(0), 0);
        assert_eq!(jemalloc_rounded(1), 8);
        assert_eq!(jemalloc_rounded(7), 8);
        assert_eq!(jemalloc_rounded(8), 8);
        assert_eq!(jemalloc_rounded(9), 16);
        assert_eq!(jemalloc_rounded(16), 16);
        assert_eq!(jemalloc_rounded(17), 32);
        assert_eq!(jemalloc_rounded(63), 64);
        assert_eq!(jemalloc_rounded(65), 80);
        assert_eq!(jemalloc_rounded(129), 160);
        assert_eq!(jemalloc_rounded(257), 512);
        assert_eq!(jemalloc_rounded(4097), 8192);
    }

    #[test]
    fn hashbrown_backing_follows_power_of_two() {
        // 0 entries -> no allocation
        assert_eq!(hashbrown_backing_bytes(0, 40), 0);
        // 1 entry -> 4-bucket minimum
        assert!(hashbrown_backing_bytes(1, 40) >= 4 * 40);
        // 10 entries -> capacity 16
        let b10 = hashbrown_backing_bytes(10, 40);
        assert!(b10 >= 16 * 40, "10-entry backing too small: {}", b10);
        // Doubles across the boundary
        assert!(hashbrown_backing_bytes(15, 40) < hashbrown_backing_bytes(20, 40));
    }

    #[test]
    fn test_cache_hit_miss() {
        let cache = DocCache::new(DocCacheConfig::default());

        // Miss
        assert!(cache.get(1).is_none());
        assert_eq!(cache.misses(), 1);
        assert_eq!(cache.hits(), 0);

        // Insert
        let doc = make_doc(vec![("name", FieldValue::Single(Value::String("test".into())))]);
        cache.insert(1, doc.clone());

        // Hit
        let result = cache.get(1).unwrap();
        assert_eq!(result.fields["name"], doc.fields["name"]);
        assert_eq!(cache.hits(), 1);
        assert_eq!(cache.misses(), 1);
    }

    #[test]
    fn test_cache_update() {
        let cache = DocCache::new(DocCacheConfig::default());

        let doc1 = make_doc(vec![("x", FieldValue::Single(Value::Integer(1)))]);
        cache.insert(1, doc1);
        let size1 = cache.size_bytes();

        let doc2 = make_doc(vec![
            ("x", FieldValue::Single(Value::Integer(2))),
            ("y", FieldValue::Single(Value::String("bigger".into()))),
        ]);
        cache.insert(1, doc2.clone());
        let size2 = cache.size_bytes();

        assert!(size2 > size1, "larger doc should increase cache size");
        assert_eq!(cache.len(), 1, "update should not create duplicate");

        let result = cache.get(1).unwrap();
        assert_eq!(result.fields["x"], FieldValue::Single(Value::Integer(2)));
    }

    #[test]
    fn test_cache_remove() {
        let cache = DocCache::new(DocCacheConfig::default());

        let doc = make_doc(vec![("x", FieldValue::Single(Value::Integer(1)))]);
        cache.insert(1, doc);
        assert_eq!(cache.len(), 1);
        assert!(cache.size_bytes() > 0);

        cache.remove(1);
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.size_bytes(), 0);
        assert!(cache.get(1).is_none());
    }

    #[test]
    fn test_cache_eviction() {
        // Tiny cache: 500 bytes
        let config = DocCacheConfig {
            max_bytes: 500,
            generation_interval_secs: 60,
            max_generations: 30,
        };
        let cache = DocCache::new(config);

        // Insert enough docs to exceed limit
        for i in 0..20u32 {
            let doc = make_doc(vec![
                ("id", FieldValue::Single(Value::Integer(i as i64))),
                ("data", FieldValue::Single(Value::String("x".repeat(50)))),
            ]);
            cache.insert(i, doc);
        }

        assert!(cache.needs_eviction(), "should need eviction after many inserts");

        let evicted = cache.evict();
        // All entries are in generation 0 (current), so evict() can't drop it
        // This is correct behavior — the eviction thread would have rotated first
        // For the legacy path, we need at least 2 generations
        assert_eq!(evicted, 0, "can't evict current generation");
    }

    #[test]
    fn test_cache_clear() {
        let cache = DocCache::new(DocCacheConfig::default());

        for i in 0..10u32 {
            cache.insert(i, make_doc(vec![("x", FieldValue::Single(Value::Integer(i as i64)))]));
        }
        assert_eq!(cache.len(), 10);

        cache.clear();
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.size_bytes(), 0);
    }

    #[test]
    fn test_generation_rotation() {
        let config = DocCacheConfig {
            max_bytes: 1_073_741_824,
            generation_interval_secs: 60,
            max_generations: 5,
        };
        let cache = DocCache::new(config);

        // Start with 1 generation
        assert_eq!(cache.generation_count(), 1);

        // Insert docs into gen 0
        for i in 0..5u32 {
            cache.insert(i, make_doc(vec![("x", FieldValue::Single(Value::Integer(i as i64)))]));
        }
        assert_eq!(cache.len(), 5);

        // Rotate: creates gen 1, old gen 0 becomes gen 1
        cache.push_new_generation();
        assert_eq!(cache.generation_count(), 2);
        assert_eq!(cache.len(), 5); // entries still accessible

        // Insert into new current gen
        cache.insert(100, make_doc(vec![("x", FieldValue::Single(Value::Integer(100)))]));
        assert_eq!(cache.len(), 6);

        // All previous docs still accessible via older generation
        for i in 0..5u32 {
            assert!(cache.get(i).is_some(), "doc {i} should still be cached");
        }
    }

    #[test]
    fn test_promotion_on_read() {
        let config = DocCacheConfig {
            max_bytes: 1_073_741_824,
            generation_interval_secs: 60,
            max_generations: 30,
        };
        let cache = DocCache::new(config);

        // Insert doc into gen 0
        cache.insert(1, make_doc(vec![("x", FieldValue::Single(Value::Integer(42)))]));

        // Rotate — doc is now in gen 1 (older)
        cache.push_new_generation();
        assert_eq!(cache.generation_count(), 2);

        // Read promotes doc to gen 0 (current)
        let doc = cache.get(1).unwrap();
        assert_eq!(doc.fields["x"], FieldValue::Single(Value::Integer(42)));

        // After promotion, dropping gen 1 should not lose the doc
        let _evicted = cache.drop_oldest_generation();
        assert!(cache.get(1).is_some(), "promoted doc should survive eviction of old gen");
    }

    #[test]
    fn test_generation_eviction() {
        let config = DocCacheConfig {
            max_bytes: 500,
            generation_interval_secs: 60,
            max_generations: 30,
        };
        let cache = DocCache::new(config);

        // Insert docs into gen 0
        for i in 0..10u32 {
            cache.insert(i, make_doc(vec![
                ("data", FieldValue::Single(Value::String("x".repeat(50)))),
            ]));
        }

        // Rotate so docs are in gen 1
        cache.push_new_generation();

        // Insert more docs into new gen 0
        for i in 10..20u32 {
            cache.insert(i, make_doc(vec![
                ("data", FieldValue::Single(Value::String("x".repeat(50)))),
            ]));
        }

        assert_eq!(cache.generation_count(), 2);
        assert!(cache.needs_eviction());

        // Drop oldest generation
        let evicted = cache.drop_oldest_generation();
        assert_eq!(evicted, 10);
        assert_eq!(cache.generation_count(), 1);

        // Old docs gone, new docs remain
        for i in 0..10u32 {
            assert!(cache.get(i).is_none(), "old doc {i} should be evicted");
        }
        for i in 10..20u32 {
            assert!(cache.get(i).is_some(), "new doc {i} should remain");
        }
    }

    #[test]
    fn test_max_generations_merging() {
        let max_gens = 3;
        let config = DocCacheConfig {
            max_bytes: 1_073_741_824,
            generation_interval_secs: 60,
            max_generations: max_gens,
        };
        let cache = DocCache::new(config);

        // Insert doc into gen 0
        cache.insert(1, make_doc(vec![("x", FieldValue::Single(Value::Integer(1)))]));

        // Rotate 3 times to exceed max_generations (3)
        cache.push_new_generation();
        cache.insert(2, make_doc(vec![("x", FieldValue::Single(Value::Integer(2)))]));

        cache.push_new_generation();
        cache.insert(3, make_doc(vec![("x", FieldValue::Single(Value::Integer(3)))]));

        // This rotation should trigger merge of two oldest
        cache.push_new_generation();

        // Should still be at max_generations (merged two oldest)
        assert!(cache.generation_count() <= max_gens,
            "generation count {} should be <= max {}",
            cache.generation_count(), max_gens);

        // All docs should still be accessible
        assert!(cache.get(1).is_some(), "doc 1 should survive merge");
        assert!(cache.get(2).is_some(), "doc 2 should survive merge");
        assert!(cache.get(3).is_some(), "doc 3 should survive merge");
    }

    #[test]
    fn test_update_batch_if_cached() {
        let config = DocCacheConfig {
            max_bytes: 1_073_741_824,
            generation_interval_secs: 60,
            max_generations: 30,
        };
        let cache = DocCache::new(config);

        // Insert doc 1 but not doc 2
        cache.insert(1, make_doc(vec![("x", FieldValue::Single(Value::Integer(1)))]));

        // Update batch: doc 1 should update, doc 2 should be skipped
        let updated = vec![
            (1u32, make_doc(vec![("x", FieldValue::Single(Value::Integer(99)))])),
            (2u32, make_doc(vec![("x", FieldValue::Single(Value::Integer(200)))])),
        ];
        cache.update_batch_if_cached(&updated);

        // Doc 1 updated
        let doc1 = cache.get(1).unwrap();
        assert_eq!(doc1.fields["x"], FieldValue::Single(Value::Integer(99)));

        // Doc 2 not inserted
        assert!(cache.get(2).is_none(), "uncached doc should not be inserted by update_batch_if_cached");
    }

    #[test]
    fn test_eviction_thread_lifecycle() {
        let config = DocCacheConfig {
            max_bytes: 500,
            generation_interval_secs: 1, // 1s for fast test
            max_generations: 5,
        };
        let cache = Arc::new(DocCache::new(config));
        let shutdown = Arc::new(AtomicBool::new(false));

        // Insert docs to exceed budget
        for i in 0..20u32 {
            cache.insert(i, make_doc(vec![
                ("data", FieldValue::Single(Value::String("x".repeat(50)))),
            ]));
        }

        let cache_clone = Arc::clone(&cache);
        let shutdown_clone = Arc::clone(&shutdown);
        let handle = std::thread::spawn(move || {
            eviction_thread(cache_clone, shutdown_clone);
        });

        // Wait for at least one rotation + eviction cycle
        // eviction_thread checks every 5s, generation interval is 1s
        std::thread::sleep(Duration::from_secs(7));

        // Shut down
        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();

        // Should have rotated at least once
        assert!(cache.generation_count() >= 2, "should have rotated generations");
    }

    // In-place op-apply tests live at tests/doc_cache_apply_ops.rs — the
    // lib test target has pre-existing unrelated compile rot that blocks
    // per-file test runs; integration tests build against the public API.
}

