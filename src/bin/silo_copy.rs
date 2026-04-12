//! Standalone DocStoreV3 → DocSilo bulk copy binary.
//!
//! Bypasses the full ConcurrentEngine (bitmap state, flush threads, cache)
//! so the copy process competes for less RAM with the docstore mmap.
//!
//! Usage:
//!     silo_copy --data-dir <path>
//!
//! Expects `<data-dir>/indexes/civitai/docs` to contain a populated
//! DocStoreV3 tree. Writes the silo at `<same>/docs/silo/silo/`.

use bitdex_v2::doc_silo::{encode_slot_bytes, slot_to_key, DocSilo};
use bitdex_v2::doc_wire_format::DocStoreV3;
use std::path::PathBuf;
use std::time::Instant;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut data_dir: Option<PathBuf> = None;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--data-dir" => {
                data_dir = Some(PathBuf::from(&args[i + 1]));
                i += 2;
            }
            _ => i += 1,
        }
    }
    let data_dir = data_dir.expect("--data-dir required");

    let docs_root = data_dir.join("indexes/civitai/docs");
    eprintln!("[silo_copy] opening DocStoreV3 at {}", docs_root.display());
    let ds = DocStoreV3::open(&docs_root).expect("open docstore");

    let silo_dir = docs_root.join("silo");
    eprintln!("[silo_copy] opening DocSilo at {}", silo_dir.display());
    let mut silo = DocSilo::open(&silo_dir).expect("open silo");

    // Translate docstore field-dict to silo field-dict space.
    let docstore_names: Vec<String> = ds.idx_to_field().iter().cloned().collect();
    let silo_idx_for: Vec<u16> = docstore_names
        .iter()
        .map(|n| silo.ensure_field_index(n))
        .collect();
    eprintln!("[silo_copy] field dict: {} names", silo_idx_for.len());

    // Iterate shards 0..max_shard. We don't have a clean "list populated shards"
    // API on DocStoreV3, so scan up to a bound based on filesystem inspection
    // and skip empty ones.
    let num_shards: u32 = 300_000; // > 245K needed for 107M Civitai
    eprintln!("[silo_copy] scanning up to {} shards", num_shards);

    let start = Instant::now();
    let mut encoded: Vec<(u64, Vec<u8>)> = Vec::new();
    let mut buf = Vec::with_capacity(512);
    let mut last_pct = -1i64;
    let mut docs_seen = 0u64;

    for shard_id in 0..num_shards {
        let shard = match ds.get_shard_packed(shard_id) {
            Ok(s) => s,
            Err(_) => continue,
        };
        if shard.is_empty() {
            continue;
        }
        for (slot_id, mut pairs) in shard {
            for (idx, _) in pairs.iter_mut() {
                let di = *idx as usize;
                if di < silo_idx_for.len() {
                    *idx = silo_idx_for[di];
                }
            }
            encode_slot_bytes(true, &pairs, &mut buf);
            encoded.push((slot_to_key(slot_id), buf.clone()));
            docs_seen += 1;
        }
        // Progress every 5% of shards or every 2M docs.
        let pct = ((shard_id as u64 * 100) / num_shards.max(1) as u64) as i64;
        if pct / 5 > last_pct / 5 {
            eprintln!(
                "[silo_copy] shards {}/{} ({}%), docs {}, elapsed {:.1}s",
                shard_id, num_shards, pct, docs_seen,
                start.elapsed().as_secs_f64()
            );
            last_pct = pct;
        }
    }

    eprintln!(
        "[silo_copy] scan done: {} docs in {:.1}s. Bulk loading silo...",
        docs_seen,
        start.elapsed().as_secs_f64()
    );

    let loaded = silo
        .bulk_load_encoded(encoded)
        .expect("bulk_load_encoded");

    silo.save_field_dict().expect("save field dict");
    eprintln!(
        "[silo_copy] loaded {} docs, data.bin = {:.1} MB, total elapsed {:.1}s",
        loaded,
        silo.data_bytes() as f64 / 1e6,
        start.elapsed().as_secs_f64()
    );
}
