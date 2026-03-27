//! bitdex-sync binary: Config-driven sync system for BitDex.
//!
//! Subcommands:
//!   pg       — PG dump pipeline + ops poller (steady-state)
//!   ch       — ClickHouse metrics poller only
//!   all      — Both PG + ClickHouse (default)
//!   setup    — Create BitdexOps table/triggers only
//!   validate — Check config, paths, and CSV availability

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
use bitdex_v2::pg_sync::ops_poller;
use bitdex_v2::pg_sync::progress::{self, LoadProgress};
use bitdex_v2::pg_sync::queries;

#[derive(Parser)]
#[command(name = "bitdex-sync", about = "Config-driven sync system for BitDex")]
struct Cli {
    /// Path to sync config TOML file.
    #[arg(long, default_value = "sync.toml")]
    config: PathBuf,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// PG dump pipeline + ops poller (dump CSVs, seed cursor, run ops poller).
    Pg {
        /// Override the cursor value instead of using the current outbox head.
        #[arg(long)]
        cursor_override: Option<i64>,
    },
    /// ClickHouse metrics poller only.
    Ch,
    /// Both PG + ClickHouse (default). Equivalent to running `pg` and `ch` concurrently.
    All {
        /// Override the cursor value instead of using the current outbox head.
        #[arg(long)]
        cursor_override: Option<i64>,
    },
    /// Create BitdexOps table/triggers only (no data load).
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

    // Default to `all` if no subcommand specified
    let command = cli.command.unwrap_or(Commands::All { cursor_override: None });

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
    if matches!(command, Commands::Validate) {
        run_validate(&cli.config, &sync_config, &index_def, &index_storage_dir, &stage_dir);
        return;
    }

    // Create PG connection pool (needed for pg/all/setup)
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

    match command {
        Commands::Validate => unreachable!(),

        Commands::Setup => {
            eprintln!("Running setup (BitdexOps table + triggers)...");
            queries::run_setup(&pool)
                .await
                .unwrap_or_else(|e| {
                    eprintln!("Setup failed: {e}");
                    std::process::exit(1);
                });
            eprintln!("Setup complete.");
        }

        Commands::Pg { cursor_override } => {
            run_pg_pipeline(&pool, &sync_config, &index_def, &index_storage_dir, &stage_dir, cursor_override).await;
            run_sync_pg(&pool, &sync_config, &index_def).await;
        }

        Commands::Ch => {
            run_sync_ch(&sync_config, &index_def).await;
        }

        Commands::All { cursor_override } => {
            // Dump CSVs first (sequential)
            run_pg_pipeline(&pool, &sync_config, &index_def, &index_storage_dir, &stage_dir, cursor_override).await;

            // Then run both pollers concurrently
            let bitdex_url = sync_config.bitdex_url.as_deref().unwrap_or("http://localhost:3000");
            let bitdex_client = BitdexClient::with_index(bitdex_url, Some(&index_def.name));
            let cursor_name = format!("pg-sync-{}", sync_config.replica_id);

            let ops_fut = ops_poller::run_ops_poller(
                &pool,
                &bitdex_client,
                sync_config.poll_interval_secs,
                sync_config.outbox_batch_limit,
                &cursor_name,
                Some(sync_config.replica_id.as_str()),
            );

            if let Some(ref ch_url) = sync_config.clickhouse_url {
                let ch_config = metrics_poller::ClickHouseConfig {
                    url: ch_url.clone(),
                    username: sync_config.clickhouse_username.clone(),
                    password: sync_config.clickhouse_password.clone(),
                };
                let metrics_fut = metrics_poller::run_metrics_poller(
                    &ch_config,
                    &bitdex_client,
                    sync_config.metrics_poll_interval_secs,
                );

                eprintln!("Starting ops poller + metrics poller (bitdex={bitdex_url})...");
                tokio::select! {
                    res = ops_fut => eprintln!("Ops poller exited: {res:?}"),
                    res = metrics_fut => eprintln!("Metrics poller exited: {res:?}"),
                }
            } else {
                eprintln!("No clickhouse_url configured — ops poller only (bitdex={bitdex_url})");
                if let Err(e) = ops_fut.await {
                    eprintln!("Ops poller error: {e}");
                    std::process::exit(1);
                }
            }
        }
    }
}

/// PG dump pipeline: download CSVs, seed cursor, prepare engine.
async fn run_pg_pipeline(
    pool: &sqlx::PgPool,
    sync_config: &PgSyncConfig,
    index_def: &IndexDefinition,
    index_storage_dir: &std::path::Path,
    stage_dir: &std::path::Path,
    cursor_override: Option<i64>,
) {
    // Ensure outbox table, triggers, and cursor table exist
    queries::run_setup(pool)
        .await
        .unwrap_or_else(|e| {
            eprintln!("Setup failed: {e}");
            std::process::exit(1);
        });

    eprintln!("Starting bulk CSV download...");

    std::fs::create_dir_all(index_storage_dir).ok();

    let mut engine_config = index_def.config.clone();
    engine_config.storage.bitmap_path =
        Some(index_storage_dir.join(&sync_config.bitmap_subdir));
    engine_config.headless = true;

    let engine = ConcurrentEngine::new_with_path(
        engine_config,
        &index_storage_dir.join(&sync_config.docs_subdir),
    )
    .unwrap_or_else(|e| {
        eprintln!("Failed to create engine: {e}");
        std::process::exit(1);
    });

    engine.set_docstore_defaults(&index_def.data_schema);

    // Copy config into the index storage dir so the server finds it
    let config_dest = index_storage_dir.join("config.yaml");
    if !config_dest.exists() {
        // Try YAML first, then JSON
        let yaml_src = sync_config.index_dir.join("config.yaml");
        let json_src = sync_config.index_dir.join("config.json");
        if yaml_src.exists() {
            std::fs::copy(&yaml_src, &config_dest).ok();
            eprintln!("Copied config.yaml to {}", config_dest.display());
        } else if json_src.exists() {
            std::fs::copy(&json_src, &index_storage_dir.join("config.json")).ok();
            eprintln!("Copied config.json to {}", index_storage_dir.display());
        }
    }

    // Determine cursor value
    let cursor_name = format!("pg-sync-{}", sync_config.replica_id);
    let outbox_head = if let Some(val) = cursor_override {
        eprintln!("Using cursor override: {val}");
        val
    } else {
        let cursor_file = index_storage_dir.join("load_stage").join("cursor.txt");
        if cursor_file.exists() {
            match std::fs::read_to_string(&cursor_file) {
                Ok(s) => match s.trim().parse::<i64>() {
                    Ok(val) => {
                        eprintln!("Using cursor from {}: {val}", cursor_file.display());
                        val
                    }
                    Err(e) => {
                        eprintln!("Invalid cursor.txt content '{}': {e}", s.trim());
                        std::process::exit(1);
                    }
                },
                Err(e) => {
                    eprintln!("Failed to read {}: {e}", cursor_file.display());
                    std::process::exit(1);
                }
            }
        } else {
            queries::get_max_outbox_id(pool)
                .await
                .unwrap_or_else(|e| {
                    eprintln!("Failed to get max outbox ID: {e}");
                    std::process::exit(1);
                })
        }
    };
    engine.set_cursor(cursor_name.clone(), outbox_head.to_string());
    eprintln!("Seeded cursor '{cursor_name}' at {outbox_head}");

    queries::upsert_cursor(pool, &cursor_name, outbox_head)
        .await
        .unwrap_or_else(|e| {
            eprintln!("Failed to register cursor in PG: {e}");
            std::process::exit(1);
        });

    // Set up progress tracking
    let load_progress = Arc::new(LoadProgress::new());
    let progress_shutdown = if let Some(port) = sync_config.progress_port {
        let tx = progress::spawn_progress_server(port, Arc::clone(&load_progress));
        Some(tx)
    } else {
        None
    };

    // Download PG CSVs
    eprintln!("Stage dir: {}", stage_dir.display());
    bulk_loader::download_all_tables(pool, stage_dir)
        .await
        .unwrap_or_else(|e| {
            eprintln!("CSV download failed: {e}");
            std::process::exit(1);
        });

    // Download ClickHouse metrics
    if let Some(ref ch_url) = sync_config.clickhouse_url {
        bulk_loader::download_metrics_from_clickhouse(
            stage_dir,
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

    eprintln!("=== CSV download complete ===");
    eprintln!("CSVs staged at: {}", stage_dir.display());
    eprintln!("Use the V2 dump pipeline to load CSVs via the server's /dumps endpoint.");

    if let Some(tx) = progress_shutdown {
        let _ = tx.send(());
    }
}

/// Run PG ops poller (steady-state sync).
async fn run_sync_pg(
    pool: &sqlx::PgPool,
    sync_config: &PgSyncConfig,
    index_def: &IndexDefinition,
) {
    let bitdex_url = sync_config.bitdex_url.as_deref().unwrap_or("http://localhost:3000");
    let bitdex_client = BitdexClient::with_index(bitdex_url, Some(&index_def.name));
    let cursor_name = format!("pg-sync-{}", sync_config.replica_id);

    eprintln!("Starting ops poller (bitdex={bitdex_url})...");
    if let Err(e) = ops_poller::run_ops_poller(
        pool,
        &bitdex_client,
        sync_config.poll_interval_secs,
        sync_config.outbox_batch_limit,
        &cursor_name,
        Some(sync_config.replica_id.as_str()),
    ).await {
        eprintln!("Ops poller error: {e}");
        std::process::exit(1);
    }
}

/// Run ClickHouse metrics poller only.
async fn run_sync_ch(
    sync_config: &PgSyncConfig,
    index_def: &IndexDefinition,
) {
    let ch_url = sync_config.clickhouse_url.as_deref().unwrap_or_else(|| {
        eprintln!("ERROR: clickhouse_url not configured — cannot run `ch` subcommand");
        std::process::exit(1);
    });
    let bitdex_url = sync_config.bitdex_url.as_deref().unwrap_or("http://localhost:3000");
    let bitdex_client = BitdexClient::with_index(bitdex_url, Some(&index_def.name));

    let ch_config = metrics_poller::ClickHouseConfig {
        url: ch_url.to_string(),
        username: sync_config.clickhouse_username.clone(),
        password: sync_config.clickhouse_password.clone(),
    };

    eprintln!("Starting ClickHouse metrics poller (bitdex={bitdex_url})...");
    if let Err(e) = metrics_poller::run_metrics_poller(
        &ch_config,
        &bitdex_client,
        sync_config.metrics_poll_interval_secs,
    ).await {
        eprintln!("Metrics poller error: {e}");
        std::process::exit(1);
    }
}

/// Validate config, paths, and CSV availability.
fn run_validate(
    config_path: &std::path::Path,
    sync_config: &PgSyncConfig,
    index_def: &IndexDefinition,
    index_storage_dir: &std::path::Path,
    stage_dir: &std::path::Path,
) {
    eprintln!("=== Validate ===");
    eprintln!("Config:       {}", config_path.display());
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
            if csv != &"metrics.csv" { ok = false; }
        }
    }

    // Check for config file (YAML preferred, JSON fallback)
    let has_config = sync_config.index_dir.join("config.yaml").exists()
        || sync_config.index_dir.join("config.json").exists();
    if !has_config {
        eprintln!("\nERROR: No config.yaml or config.json found in {}", sync_config.index_dir.display());
        ok = false;
    }

    eprintln!("\nFilter fields: {}", index_def.config.filter_fields.len());
    eprintln!("Sort fields:   {}", index_def.config.sort_fields.len());

    if ok {
        eprintln!("\nVALIDATION PASSED — ready to sync.");
    } else {
        eprintln!("\nVALIDATION FAILED — missing required files.");
        std::process::exit(1);
    }
}
