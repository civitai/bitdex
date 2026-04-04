use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use crate::silos::doc_silo_adapter::DocSiloAdapter;

/// Run the janitor loop: compacts DataSilo, CacheSilo, and BitmapSilo
/// on every tick until `shutdown` is set.
///
/// Extracted from the `bitdex-merge` thread in `ConcurrentEngine::build()`.
/// Caller owns the thread spawn; this function owns the inner loop body.
pub fn run_janitor(
    shutdown: Arc<AtomicBool>,
    interval_ms: u64,
    dirty_flag: Arc<AtomicBool>,
    docstore: Arc<parking_lot::Mutex<DocSiloAdapter>>,
    cache_silo: Option<Arc<parking_lot::RwLock<crate::silos::cache_silo::CacheSilo>>>,
    bitmap_silo: Option<Arc<parking_lot::RwLock<crate::silos::bitmap_silo::BitmapSilo>>>,
) {
    let sleep_duration = Duration::from_millis(interval_ms);
    while !shutdown.load(Ordering::Relaxed) {
        thread::sleep(sleep_duration);

        // Compact DataSilo when dirty (apply pending doc ops to data file).
        let needs_write = dirty_flag.swap(false, Ordering::AcqRel);
        if needs_write {
            if let Err(e) = docstore.lock().compact() {
                eprintln!("janitor: DataSilo compaction failed: {e}");
            }
        }

        // Compact CacheSilo when it has accumulated enough dead space.
        if let Some(ref cs_arc) = cache_silo {
            let needs_compact = cs_arc.read().needs_compaction();
            if needs_compact {
                if let Err(e) = cs_arc.write().compact() {
                    eprintln!("janitor: CacheSilo compaction failed: {e}");
                }
            }
        }

        // Compact BitmapSilo when it has accumulated enough dead space.
        if let Some(ref bs_arc) = bitmap_silo {
            let needs_compact = bs_arc.read().needs_compaction();
            if needs_compact {
                if let Err(e) = bs_arc.write().compact() {
                    eprintln!("janitor: BitmapSilo compaction failed: {e}");
                }
            }
        }
    }
}
