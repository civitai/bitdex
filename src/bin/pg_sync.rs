//! bitdex-pg-sync binary: Postgres-to-Bitdex sync system.
//!
//! Subcommands:
//!   load  — Create BitdexOutbox table/triggers + bulk load from PG + save snapshot
//!   sync  — Outbox poller + ClickHouse metrics poller (steady-state)
//!   setup — Create BitdexOutbox table/triggers only

#[global_allocator]
static ALLOC: rpmalloc::RpMalloc = rpmalloc::RpMalloc;

use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use sqlx::postgres::PgPoolOptions;

use bitdex_v2::concurrent_engine::ConcurrentEngine;
use bitdex_v2::pg_sync::bitdex_client::BitdexClient;
use bitdex_v2::pg_sync::bulk_loader;
use bitdex_v2::pg_sync::config::{IndexDefinition, PgSyncConfig};
use bitdex_v2::pg_sync::metrics_poller;
use bitdex_v2::pg_sync::outbox_poller;
use bitdex_v2::pg_sync::progress::{self, LoadProgress};
use bitdex_v2::pg_sync::queries;
use bitdex_v2::pg_sync::single_pass;

#[derive(Parser)]
#[command(name = "pg-sync", about = "Postgres-to-Bitdex sync system")]
struct Cli {
    /// Path to sync config TOML file.
    #[arg(long, default_value = "sync.toml")]
    config: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Create BitdexOutbox table/triggers + bulk load from PG + save snapshot.
    Load,
    /// Run outbox poller + ClickHouse metrics poller (steady-state sync).
    Sync,
    /// Create BitdexOutbox table/triggers only (no data load).
    Setup,
    /// Validate config, paths, and CSV availability without loading.
    Validate,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();

    // Load sync config
    let sync_config = PgSyncConfig::from_file(&cli.config).unwrap_or_else(|e| {
        eprintln!("Failed to load config {}: {e}", cli.config.display());
        std::process::exit(1);
    });

    // Load index definition
    let index_def = IndexDefinition::from_dir(&sync_config.index_dir).unwrap_or_else(|e| {
        eprintln!(
            "Failed to load index definition from {}: {e}",
            sync_config.index_dir.display()
        );
        std::process::exit(1);
    });

    // Compute derived paths
    let index_storage_dir = sync_config
        .data_dir
        .join(&sync_config.index_subdir)
        .join(&index_def.name);
    let stage_dir = sync_config
        .stage_dir
        .clone()
        .unwrap_or_else(|| index_storage_dir.join("load_stage"));

    // Validate doesn't need PG — handle it before connecting
    if matches!(cli.command, Commands::Validate) {
        eprintln!("=== Validate ===");
        eprintln!("Config:       {}", cli.config.display());
        eprintln!("Index:        {} (from {})", index_def.name, sync_config.index_dir.display());
        eprintln!("Data dir:     {}", sync_config.data_dir.display());
        eprintln!("Storage dir:  {}", index_storage_dir.display());
        eprintln!("Bitmap dir:   {}", index_storage_dir.join(&sync_config.bitmap_subdir).display());
        eprintln!("Docs dir:     {}", index_storage_dir.join(&sync_config.docs_subdir).display());
        eprintln!("Stage dir:    {}", stage_dir.display());
        eprintln!("Postgres:     {}...{}", &sync_config.postgres_url[..sync_config.postgres_url.find('@').unwrap_or(20).min(20)], &sync_config.postgres_url[sync_config.postgres_url.rfind('/').unwrap_or(0)..]);
        eprintln!("ClickHouse:   {}", sync_config.clickhouse_url.as_deref().unwrap_or("(not configured)"));

        let mut ok = true;
        let csvs = ["tags.csv", "images.csv", "resources.csv", "posts.csv",
                     "tools.csv", "techniques.csv", "model_versions.csv", "models.csv", "metrics.csv"];
        eprintln!("\nCSV files in {}:", stage_dir.display());
        for csv in &csvs {
            let path = stage_dir.join(csv);
            if path.exists() {
                let meta = std::fs::metadata(&path).unwrap();
                let size_mb = meta.len() as f64 / (1024.0 * 1024.0);
                let done = stage_dir.join(format!("{csv}.done")).exists();
                eprintln!("  {csv:25} {size_mb:>10.1} MB {}", if done { "(done)" } else { "" });
            } else {
                eprintln!("  {csv:25} MISSING");
                if csv != &"metrics.csv" { ok = false; } // metrics optional
            }
        }

        let config_path = sync_config.index_dir.join("config.json");
        if !config_path.exists() {
            eprintln!("\nERROR: config.json not found at {}", config_path.display());
            ok = false;
        }

        eprintln!("\nFilter fields: {}", index_def.config.filter_fields.len());
        eprintln!("Sort fields:   {}", index_def.config.sort_fields.len());

        if ok {
            eprintln!("\nVALIDATION PASSED — ready to load.");
        } else {
            eprintln!("\nVALIDATION FAILED — missing required files.");
            std::process::exit(1);
        }
        return;
    }

    // Create PG connection pool (needed for load/sync/setup)
    let pool = PgPoolOptions::new()
        .max_connections(sync_config.pg_pool_size)
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                sqlx::query("SET statement_timeout = 0")
                    .execute(&mut *conn)
                    .await?;
                Ok(())
            })
        })
        .connect(&sync_config.postgres_url)
        .await
        .unwrap_or_else(|e| {
            eprintln!("Failed to connect to Postgres: {e}");
            std::process::exit(1);
        });

    eprintln!("Connected to Postgres (pool_size={})", sync_config.pg_pool_size);

    match cli.command {
        Commands::Validate => unreachable!(),
        Commands::Setup => {
            eprintln!("Running setup (BitdexOutbox table + triggers)...");
            queries::run_setup(&pool)
                .await
                .unwrap_or_else(|e| {
                    eprintln!("Setup failed: {e}");
                    std::process::exit(1);
                });
            eprintln!("Setup complete.");
        }

        Commands::Load => {
            // Ensure outbox table, triggers, and cursor table exist
            queries::run_setup(&pool)
                .await
                .unwrap_or_else(|e| {
                    eprintln!("Setup failed: {e}");
                    std::process::exit(1);
                });

            eprintln!("Starting bulk load...");

            // Build engine with storage paths matching the server's layout:
            //   {data_dir}/{index_subdir}/{name}/{bitmap_subdir}/
            //   {data_dir}/{index_subdir}/{name}/{docs_subdir}/
            std::fs::create_dir_all(&index_storage_dir).ok();

            let mut engine_config = index_def.config.clone();
            engine_config.storage.bitmap_path =
                Some(index_storage_dir.join(&sync_config.bitmap_subdir));

            let engine = ConcurrentEngine::new_with_path(
                engine_config,
                &index_storage_dir.join(&sync_config.docs_subdir),
            )
            .unwrap_or_else(|e| {
                eprintln!("Failed to create engine: {e}");
                std::process::exit(1);
            });

            // Copy config.json into the index storage dir so the server finds it
            let config_dest = index_storage_dir.join("config.json");
            if !config_dest.exists() {
                let config_src = sync_config.index_dir.join("config.json");
                if config_src.exists() {
                    std::fs::copy(&config_src, &config_dest).ok();
                    eprintln!("Copied config.json to {}", config_dest.display());
                }
            }

            // Snapshot the current outbox head BEFORE bulk load.
            // This becomes the cursor's starting point — anything that changes
            // during bulk load will be in the outbox after this ID and will be
            // picked up when the sidecar starts polling.
            let cursor_name = format!("pg-sync-{}", sync_config.replica_id);
            let outbox_head = queries::get_max_outbox_id(&pool)
                .await
                .unwrap_or_else(|e| {
                    eprintln!("Failed to get max outbox ID: {e}");
                    std::process::exit(1);
                });
            engine.set_cursor(cursor_name.clone(), outbox_head.to_string());
            eprintln!("Seeded cursor '{cursor_name}' at outbox head {outbox_head}");

            // Also register in PG so outbox cleanup knows about this replica
            queries::upsert_cursor(&pool, &cursor_name, outbox_head)
                .await
                .unwrap_or_else(|e| {
                    eprintln!("Failed to register cursor in PG: {e}");
                    std::process::exit(1);
                });

            // No enter_loading_mode — single_pass writes directly to BitmapFs.
            // Loading mode would trigger a snapshot save on exit that overwrites our bitmaps.

            // Set up progress tracking + HTTP endpoint
            let load_progress = Arc::new(LoadProgress::new());
            let progress_shutdown = if let Some(port) = sync_config.progress_port {
                let tx = progress::spawn_progress_server(port, Arc::clone(&load_progress));
                Some(tx)
            } else {
                None
            };

            // Phase 1a: Download PG CSVs to staging dir (reuses .done markers)
            eprintln!("Stage dir: {}", stage_dir.display());
            bulk_loader::download_all_tables(&pool, &stage_dir)
                .await
                .unwrap_or_else(|e| {
                    eprintln!("CSV download failed: {e}");
                    std::process::exit(1);
                });

            // Phase 1b: Download ClickHouse metrics (reactionCount, commentCount, collectedCount)
            if let Some(ref ch_url) = sync_config.clickhouse_url {
                bulk_loader::download_metrics_from_clickhouse(
                    &stage_dir,
                    ch_url,
                    sync_config.clickhouse_username.as_deref(),
                    sync_config.clickhouse_password.as_deref(),
                )
                .await
                .unwrap_or_else(|e| {
                    eprintln!("WARNING: ClickHouse metrics download failed: {e}");
                    eprintln!("Continuing without metrics — sort by reactionCount will use zeros");
                    0
                });
            } else {
                eprintln!("WARNING: No clickhouse_url configured — metric sort fields will be 0");
            }

            // Single-pass V2 loader: CSV → bitmaps + V2 docstore tuples in one pass
            eprintln!("=== Using single-pass V2 loader ===");
            let stats = single_pass::run_single_pass_v2(
                &engine,
                &index_def,
                &stage_dir,
                Arc::clone(&load_progress),
            )
            .unwrap_or_else(|e| {
                eprintln!("Single-pass V2 bulk load failed: {e}");
                std::process::exit(1);
            });

            // Shut down progress server
            if let Some(tx) = progress_shutdown {
                let _ = tx.send(());
            }

            // No exit_loading_mode needed — single_pass wrote everything to BitmapFs directly.
            // The process exits after this; the server will restore from disk on next start.

            eprintln!(
                "Bulk load complete: {} records in {:.1}s ({:.0}/s)",
                stats.records_loaded,
                stats.elapsed.as_secs_f64(),
                stats.records_loaded as f64 / stats.elapsed.as_secs_f64()
            );
        }

        Commands::Sync => {
            let bitdex_url = sync_config.bitdex_url.as_deref().unwrap_or("http://localhost:3000");
            let bitdex_client = BitdexClient::with_index(bitdex_url, Some(&index_def.name));

            eprintln!("Starting sync (bitdex={bitdex_url})...");

            // Run outbox poller and metrics poller concurrently
            let cursor_name = format!("pg-sync-{}", sync_config.replica_id);
            let outbox_fut = outbox_poller::run_outbox_poller(
                &pool,
                &bitdex_client,
                sync_config.poll_interval_secs,
                sync_config.outbox_batch_limit,
                &cursor_name,
            );

            if let Some(ref ch_url) = sync_config.clickhouse_url {
                let ch_config = metrics_poller::ClickHouseConfig {
                    url: ch_url.clone(),
                    username: sync_config.clickhouse_username.clone(),
                    password: sync_config.clickhouse_password.clone(),
                };
                let metrics_fut = metrics_poller::run_metrics_poller(
                    &pool,
                    &ch_config,
                    &bitdex_client,
                    sync_config.metrics_poll_interval_secs,
                );

                // Run both pollers concurrently
                tokio::select! {
                    res = outbox_fut => {
                        eprintln!("Outbox poller exited: {res:?}");
                    }
                    res = metrics_fut => {
                        eprintln!("Metrics poller exited: {res:?}");
                    }
                }
            } else {
                eprintln!("No clickhouse_url configured — outbox poller only");
                if let Err(e) = outbox_fut.await {
                    eprintln!("Outbox poller error: {e}");
                    std::process::exit(1);
                }
            }
        }
    }
}
