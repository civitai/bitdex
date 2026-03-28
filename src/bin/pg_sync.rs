//! bitdex-sync binary: Config-driven sync system for BitDex.
//!
//! Fully autonomous: deploy to K8s and it handles everything —
//! connect to Postgres, pull CSVs, register dumps with BitDex,
//! set up triggers, seed cursors, start ops polling. Zero manual intervention.
//!
//! Subcommands:
//!   pg       — PG dump pipeline + ops poller (steady-state)
//!   ch       — ClickHouse metrics poller only
//!   all      — Both PG + ClickHouse (default)
//!   setup    — Create BitdexOps table/triggers only
//!   validate — Check config, paths, and CSV availability

#[global_allocator]
static ALLOC: rpmalloc::RpMalloc = rpmalloc::RpMalloc;

use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use sqlx::postgres::PgPoolOptions;

use bitdex_v2::pg_sync::bitdex_client::BitdexClient;
use bitdex_v2::pg_sync::bulk_loader;
use bitdex_v2::pg_sync::config::{IndexDefinition, PgSyncConfig};
use bitdex_v2::pg_sync::metrics_poller;
use bitdex_v2::pg_sync::ops_poller;
use bitdex_v2::pg_sync::queries;
use bitdex_v2::pg_sync::sync_config::FullSyncConfig;

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

    // Load sync config (TOML — connection info + paths)
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

    // Load V2 sync config YAML (triggers + dump phases) if available
    let full_sync_config = sync_config.sync_config_path.as_ref().and_then(|path| {
        match FullSyncConfig::from_file(path) {
            Ok(config) => {
                eprintln!(
                    "Loaded sync config: {} dump phases, {} triggers",
                    config.dump_phases.len(),
                    config.triggers.len()
                );
                Some(config)
            }
            Err(e) => {
                eprintln!("WARNING: Failed to load sync config {}: {e}", path.display());
                None
            }
        }
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

    // BitDex client (used by most commands)
    let bitdex_url = sync_config.bitdex_url.as_deref().unwrap_or("http://localhost:3000");
    let bitdex_client = BitdexClient::with_index(bitdex_url, Some(&index_def.name));

    // Validate doesn't need PG — handle it before connecting
    if matches!(command, Commands::Validate) {
        run_validate(&cli.config, &sync_config, &index_def, &index_storage_dir, &stage_dir, full_sync_config.as_ref());
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
            run_setup(&pool, full_sync_config.as_ref()).await;
        }

        Commands::Pg { cursor_override } => {
            run_boot_sequence(
                &pool, &sync_config, &index_def, &index_storage_dir,
                &stage_dir, &bitdex_client, full_sync_config.as_ref(), cursor_override,
            ).await;
            run_sync_pg(&pool, &sync_config, &index_def, &bitdex_client).await;
        }

        Commands::Ch => {
            run_sync_ch(&sync_config, &bitdex_client).await;
        }

        Commands::All { cursor_override } => {
            // Full autonomous boot: setup → dump → pollers
            run_boot_sequence(
                &pool, &sync_config, &index_def, &index_storage_dir,
                &stage_dir, &bitdex_client, full_sync_config.as_ref(), cursor_override,
            ).await;

            // Run both pollers concurrently
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

// ---------------------------------------------------------------------------
// Setup: V2 tables + config-driven trigger reconciliation
// ---------------------------------------------------------------------------

async fn run_setup(pool: &sqlx::PgPool, full_sync_config: Option<&FullSyncConfig>) {
    if let Some(config) = full_sync_config {
        // V2: config-driven trigger reconciliation
        eprintln!("Running V2 setup (BitdexOps + {} triggers)...", config.triggers.len());
        queries::run_setup_v2(pool, &config.triggers)
            .await
            .unwrap_or_else(|e| {
                eprintln!("V2 setup failed: {e}");
                std::process::exit(1);
            });
    } else {
        // Fallback: V1 setup (for backwards compatibility during transition)
        eprintln!("No sync config YAML — falling back to V1 setup...");
        queries::run_setup(pool)
            .await
            .unwrap_or_else(|e| {
                eprintln!("Setup failed: {e:?}");
                std::process::exit(1);
            });
    }
    eprintln!("Setup complete.");
}

// ---------------------------------------------------------------------------
// Boot sequence: fully autonomous initial load
// ---------------------------------------------------------------------------

/// Autonomous boot sequence for K8s deployment.
///
/// 1. Wait for BitDex health check
/// 2. V2 setup (BitdexOps + triggers from sync config)
/// 3. Capture pre_dump_cursor from BitdexOps
/// 4. Copy index config to storage dir
/// 5. Download CSVs from PG/ClickHouse
/// 6. Check dump history — skip already-complete phases
/// 7. Register remaining dumps with BitDex (PUT /dumps)
/// 8. Signal each dump loaded (POST /dumps/{name}/loaded)
/// 9. Poll until all dumps complete
/// 10. Seed cursor at pre_dump_cursor in bitdex_cursors
async fn run_boot_sequence(
    pool: &sqlx::PgPool,
    sync_config: &PgSyncConfig,
    index_def: &IndexDefinition,
    index_storage_dir: &Path,
    stage_dir: &Path,
    bitdex_client: &BitdexClient,
    full_sync_config: Option<&FullSyncConfig>,
    cursor_override: Option<i64>,
) {
    // Step 1: Wait for BitDex to be healthy
    eprintln!("Waiting for BitDex to be healthy...");
    bitdex_client
        .wait_for_healthy(60, 2) // up to 60 retries, starting at 2s backoff
        .await
        .unwrap_or_else(|e| {
            eprintln!("FATAL: {e}");
            std::process::exit(1);
        });
    eprintln!("BitDex is healthy.");

    // Step 2: V2 setup (triggers + tables)
    run_setup(pool, full_sync_config).await;

    // Step 3: Capture pre-dump cursor (catches ops that arrive during dump)
    let cursor_name = format!("pg-sync-{}", sync_config.replica_id);
    let pre_dump_cursor = if let Some(val) = cursor_override {
        eprintln!("Using cursor override: {val}");
        val
    } else {
        // Try BitdexOps first (V2), fall back to BitdexOutbox (V1 transition)
        match queries::get_max_ops_id(pool).await {
            Ok(id) => id,
            Err(e) => {
                eprintln!("WARNING: BitdexOps not available ({e}), trying BitdexOutbox...");
                queries::get_max_outbox_id(pool).await.unwrap_or(0)
            }
        }
    };
    eprintln!("Pre-dump cursor: {pre_dump_cursor}");

    // Step 4: Copy index config to storage dir
    std::fs::create_dir_all(index_storage_dir).ok();
    let config_dest = index_storage_dir.join("config.yaml");
    if !config_dest.exists() {
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

    // Step 5: Download CSVs from PG
    eprintln!("Stage dir: {}", stage_dir.display());
    std::fs::create_dir_all(stage_dir).ok();

    if let Some(config) = full_sync_config {
        // V2: config-driven download using copy_query from each dump phase
        bulk_loader::download_from_sync_config(pool, stage_dir, &config.dump_phases)
            .await
            .unwrap_or_else(|e| {
                eprintln!("Config-driven CSV download failed: {e}");
                std::process::exit(1);
            });
    } else {
        // V1 fallback: hardcoded table list
        bulk_loader::download_all_tables(pool, stage_dir)
            .await
            .unwrap_or_else(|e| {
                eprintln!("CSV download failed: {e}");
                std::process::exit(1);
            });
    }

    // Download ClickHouse metrics CSV (separate from PG COPY)
    if let Some(ref ch_url) = sync_config.clickhouse_url {
        let _ = bulk_loader::download_metrics_from_clickhouse(
            stage_dir,
            ch_url,
            sync_config.clickhouse_username.as_deref(),
            sync_config.clickhouse_password.as_deref(),
        )
        .await
        .map_err(|e| {
            eprintln!("WARNING: ClickHouse metrics download failed: {e}");
            eprintln!("Continuing without metrics — sort by reactionCount will use zeros");
        });
    }

    eprintln!("=== CSV download complete ===");

    // Steps 6-9: Register dumps with BitDex and poll for completion
    if let Some(config) = full_sync_config {
        run_dump_pipeline(bitdex_client, config, stage_dir).await;
    } else {
        eprintln!("No sync config YAML — skipping dump pipeline.");
        eprintln!("CSVs staged at: {}. Use /dumps endpoint manually.", stage_dir.display());
    }

    // Step 10: Seed cursor at pre_dump_cursor
    queries::upsert_cursor(pool, &cursor_name, pre_dump_cursor)
        .await
        .unwrap_or_else(|e| {
            eprintln!("Failed to seed cursor in PG: {e}");
            std::process::exit(1);
        });
    eprintln!("Seeded cursor '{cursor_name}' at {pre_dump_cursor}");

    eprintln!("=== Boot sequence complete — transitioning to steady-state ===");
}

/// Register dumps with BitDex and poll until complete.
///
/// For each dump phase:
/// 1. Check if already complete (GET /dumps)
/// 2. Build dump request body from sync config
/// 3. PUT /dumps to register
/// 4. POST /dumps/{name}/loaded to signal CSV is ready
///
/// Then poll until all dumps are complete.
async fn run_dump_pipeline(
    bitdex_client: &BitdexClient,
    config: &FullSyncConfig,
    stage_dir: &Path,
) {
    // Check existing dump status
    let existing_dumps = bitdex_client.get_dumps().await.ok();
    let completed_set: std::collections::HashSet<String> = existing_dumps
        .as_ref()
        .and_then(|d| d.get("dumps"))
        .and_then(|d| d.as_object())
        .map(|map| {
            map.iter()
                .filter(|(_, v)| v.get("status").and_then(|s| s.as_str()) == Some("Complete"))
                .map(|(k, _)| k.clone())
                .collect()
        })
        .unwrap_or_default();

    let mut completed = 0;
    let total = config.dump_phases.len();

    // Process dumps SEQUENTIALLY — server only handles one task at a time
    for phase in &config.dump_phases {
        let name = phase.dump_name();

        if completed_set.contains(&name) {
            eprintln!("[{}/{}] Dump '{name}' already complete — skipping", completed + 1, total);
            completed += 1;
            continue;
        }

        // Check if the CSV file exists
        let csv_ext = if phase.format == "tsv" { "tsv" } else { "csv" };
        let csv_filename = format!("{}.{}", phase.name, csv_ext);
        let csv_path = stage_dir.join(&csv_filename);

        if !csv_path.exists() {
            if phase.source.as_deref() == Some("clickhouse") {
                let tsv_path = stage_dir.join(format!("{}.tsv", phase.name));
                if !tsv_path.exists() {
                    eprintln!("[{}/{}] WARNING: Skipping dump '{name}' — {} not found", completed + 1, total, tsv_path.display());
                    continue;
                }
            } else {
                eprintln!("[{}/{}] WARNING: Skipping dump '{name}' — {} not found", completed + 1, total, csv_path.display());
                continue;
            }
        }

        // Register this dump
        let dump_request = phase.to_dump_request(stage_dir);
        eprintln!("[{}/{}] Registering dump '{name}'...", completed + 1, total);

        match bitdex_client.register_dump(&dump_request).await {
            Ok(resp) => {
                eprintln!("  Registered: {}", serde_json::to_string(&resp).unwrap_or_default());
            }
            Err(e) => {
                eprintln!("ERROR: Failed to register dump '{name}': {e}");
                std::process::exit(1);
            }
        }

        // Signal CSV is ready
        match bitdex_client.signal_dump_loaded(&name, 0).await {
            Ok(_) => eprintln!("  Signaled loaded: {name}"),
            Err(e) => {
                eprintln!("ERROR: Failed to signal dump loaded '{name}': {e}");
                std::process::exit(1);
            }
        }

        // Wait for THIS dump to complete before registering the next one
        eprintln!("  Waiting for '{name}' to complete...");
        bitdex_client
            .poll_dumps_until_complete(
                &[name.clone()],
                5,       // poll every 5s
                3600,    // 1 hour timeout per dump
            )
            .await
            .unwrap_or_else(|e| {
                eprintln!("FATAL: Dump '{name}' failed: {e}");
                std::process::exit(1);
            });
        eprintln!("  Dump '{name}' complete.");
        completed += 1;
    }

    eprintln!("=== All {completed}/{total} dumps complete ===");
}

// ---------------------------------------------------------------------------
// Steady-state pollers
// ---------------------------------------------------------------------------

/// Run PG ops poller (steady-state sync).
async fn run_sync_pg(
    pool: &sqlx::PgPool,
    sync_config: &PgSyncConfig,
    _index_def: &IndexDefinition,
    bitdex_client: &BitdexClient,
) {
    let cursor_name = format!("pg-sync-{}", sync_config.replica_id);

    eprintln!("Starting ops poller...");
    if let Err(e) = ops_poller::run_ops_poller(
        pool,
        bitdex_client,
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
    bitdex_client: &BitdexClient,
) {
    let ch_url = sync_config.clickhouse_url.as_deref().unwrap_or_else(|| {
        eprintln!("ERROR: clickhouse_url not configured — cannot run `ch` subcommand");
        std::process::exit(1);
    });

    let ch_config = metrics_poller::ClickHouseConfig {
        url: ch_url.to_string(),
        username: sync_config.clickhouse_username.clone(),
        password: sync_config.clickhouse_password.clone(),
    };

    eprintln!("Starting ClickHouse metrics poller...");
    if let Err(e) = metrics_poller::run_metrics_poller(
        &ch_config,
        bitdex_client,
        sync_config.metrics_poll_interval_secs,
    ).await {
        eprintln!("Metrics poller error: {e}");
        std::process::exit(1);
    }
}

// ---------------------------------------------------------------------------
// Validate
// ---------------------------------------------------------------------------

fn run_validate(
    config_path: &Path,
    sync_config: &PgSyncConfig,
    index_def: &IndexDefinition,
    index_storage_dir: &Path,
    stage_dir: &Path,
    full_sync_config: Option<&FullSyncConfig>,
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

    if let Some(ref path) = sync_config.sync_config_path {
        eprintln!("Sync config:  {}", path.display());
    } else {
        eprintln!("Sync config:  (not configured)");
    }

    let mut ok = true;

    // Check sync config
    if let Some(config) = full_sync_config {
        eprintln!("\nSync config: {} dump phases, {} triggers", config.dump_phases.len(), config.triggers.len());
        for phase in &config.dump_phases {
            eprintln!("  Dump phase: {} → {}", phase.name, phase.dump_name());
        }
        for trigger in &config.triggers {
            let name = bitdex_v2::pg_sync::trigger_gen::trigger_name(trigger);
            eprintln!("  Trigger: {} on {}", name, trigger.table);
        }
    } else {
        eprintln!("\nWARNING: No sync config YAML configured — V2 features unavailable");
    }

    // Check CSV files
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
