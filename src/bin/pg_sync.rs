//! bitdex-pg-sync binary: Postgres-to-Bitdex sync system.
//!
//! Subcommands:
//!   load  — Create BitdexOutbox table/triggers + bulk load from PG + save snapshot
//!   sync  — Outbox poller + ClickHouse metrics poller (steady-state)
//!   setup — Create BitdexOutbox table/triggers only

#[global_allocator]
static ALLOC: rpmalloc::RpMalloc = rpmalloc::RpMalloc;

use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use sqlx::postgres::PgPoolOptions;

use bitdex_v2::concurrent_engine::ConcurrentEngine;
use bitdex_v2::pg_sync::bitdex_client::BitdexClient;
use bitdex_v2::pg_sync::bulk_loader;
use bitdex_v2::pg_sync::config::{IndexDefinition, PgSyncConfig};
use bitdex_v2::pg_sync::metrics_poller;
use bitdex_v2::pg_sync::outbox_poller;
use bitdex_v2::pg_sync::queries;

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

    // Create PG connection pool
    let pool = PgPoolOptions::new()
        .max_connections(sync_config.pg_pool_size)
        .connect(&sync_config.postgres_url)
        .await
        .unwrap_or_else(|e| {
            eprintln!("Failed to connect to Postgres: {e}");
            std::process::exit(1);
        });

    eprintln!("Connected to Postgres (pool_size={})", sync_config.pg_pool_size);

    match cli.command {
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
            eprintln!("Starting bulk load...");

            // Build engine with storage paths matching the server's layout:
            //   {data_dir}/{index_subdir}/{name}/{bitmap_subdir}/
            //   {data_dir}/{index_subdir}/{name}/{docs_subdir}/
            let index_storage_dir = sync_config
                .data_dir
                .join(&sync_config.index_subdir)
                .join(&index_def.name);
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

            engine.enter_loading_mode();

            let progress = Arc::new(AtomicU64::new(0));
            let stats = bulk_loader::run_bulk_load(
                &pool,
                &engine,
                &index_def,
                sync_config.batch_size,
                progress,
            )
            .await
            .unwrap_or_else(|e| {
                eprintln!("Bulk load failed: {e}");
                std::process::exit(1);
            });

            engine.exit_loading_mode();

            eprintln!(
                "Bulk load complete: {} records in {:.1}s ({:.0}/s)",
                stats.records_loaded,
                stats.elapsed.as_secs_f64(),
                stats.records_loaded as f64 / stats.elapsed.as_secs_f64()
            );
        }

        Commands::Sync => {
            let bitdex_url = sync_config.bitdex_url.as_deref().unwrap_or("http://localhost:3000");
            let bitdex_client = BitdexClient::new(bitdex_url);

            eprintln!("Starting sync (bitdex={bitdex_url})...");

            // Run outbox poller and metrics poller concurrently
            let outbox_fut = outbox_poller::run_outbox_poller(
                &pool,
                &bitdex_client,
                sync_config.poll_interval_secs,
                sync_config.outbox_batch_limit,
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
