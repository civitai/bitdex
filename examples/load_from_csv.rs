//! Load BitDex index from pre-fetched CSV files on disk.
//!
//! Usage:
//!   cargo run --release --features pg-sync --example load_from_csv -- \
//!     --stage-dir data/load_stage \
//!     --config configs/civitai-index.json \
//!     --data-dir data

use std::path::PathBuf;
use std::sync::Arc;

use bitdex_v2::concurrent_engine::ConcurrentEngine;
use bitdex_v2::pg_sync::config::IndexDefinition;
use bitdex_v2::pg_sync::progress::LoadProgress;
use bitdex_v2::pg_sync::single_pass;

fn arg_value(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|pos| args.get(pos + 1).cloned())
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let stage_dir = PathBuf::from(
        arg_value(&args, "--stage-dir").unwrap_or_else(|| "data/load_stage".into()),
    );
    let config_path = PathBuf::from(
        arg_value(&args, "--config").unwrap_or_else(|| "configs/civitai-index.json".into()),
    );
    let data_dir = PathBuf::from(
        arg_value(&args, "--data-dir").unwrap_or_else(|| "data".into()),
    );

    let index_def = IndexDefinition::from_file(&config_path).unwrap_or_else(|e| {
        eprintln!("Failed to load config: {e}");
        std::process::exit(1);
    });

    // Server layout: {data_dir}/indexes/{name}/bitmaps, docs, config.json
    let index_dir = data_dir.join("indexes").join(&index_def.name);
    let bitmap_dir = index_dir.join("bitmaps");
    let docs_dir = index_dir.join("docs");

    // Clean previous output
    let _ = std::fs::remove_dir_all(&bitmap_dir);
    let _ = std::fs::remove_dir_all(&docs_dir);
    std::fs::create_dir_all(&bitmap_dir).unwrap();
    std::fs::create_dir_all(&docs_dir).unwrap();
    let _ = std::fs::copy(&config_path, index_dir.join("config.json"));

    eprintln!("Stage:   {}", stage_dir.display());
    eprintln!("Config:  {}", config_path.display());
    eprintln!("Output:  {}", index_dir.display());

    let mut engine_config = index_def.config.clone();
    engine_config.storage.bitmap_path = Some(bitmap_dir);
    engine_config.headless = true;

    let engine = ConcurrentEngine::new_with_path(engine_config, &docs_dir).unwrap_or_else(|e| {
        eprintln!("Failed to create engine: {e}");
        std::process::exit(1);
    });

    let progress = Arc::new(LoadProgress::new());

    let result = single_pass::run_single_pass_v2(&engine, &index_def, &stage_dir, progress);

    match result {
        Ok(stats) => {
            eprintln!(
                "\nDone: {} records in {:.1}s ({:.0}/s)",
                stats.records_loaded,
                stats.elapsed.as_secs_f64(),
                stats.records_loaded as f64 / stats.elapsed.as_secs_f64()
            );
            eprintln!("Server: cargo run --release --features server --bin bitdex-server -- --port 3001 --data-dir {}", data_dir.display());
        }
        Err(e) => {
            eprintln!("Load failed: {e}");
            std::process::exit(1);
        }
    }
}
