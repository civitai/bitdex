//! BitDex V2 HTTP Server
//!
//! Starts blank — create indexes and load data via the API.
//!
//! Configuration (in priority order):
//!   1. CLI flags (highest priority)
//!   2. bitdex.toml next to the executable
//!   3. Built-in defaults
//!
//! Usage:
//!   bitdex-server [OPTIONS]
//!
//! Options:
//!   --port <N>         HTTP port (default: 3000)
//!   --data-dir <PATH>  Data directory for index storage (default: data/ next to exe)
//!   --rebuild          Rebuild bitmap indexes from docstore on startup
//!   --config <PATH>    Path to config file (default: bitdex.toml next to exe)

#[global_allocator]
static ALLOC: rpmalloc::RpMalloc = rpmalloc::RpMalloc;

use std::net::SocketAddr;
use std::path::PathBuf;

use bitdex_v2::server::BitdexServer;

/// Default config embedded from bitdex.default.toml at compile time.
/// Edit that file in the repo to change defaults — no code changes needed.
const DEFAULT_CONFIG: &str = include_str!("../../bitdex.default.toml");

/// Resolved server configuration.
struct Config {
    port: u16,
    data_dir: PathBuf,
    rebuild: bool,
}

/// Get the directory containing the current executable.
fn exe_dir() -> PathBuf {
    std::env::current_exe()
        .expect("Failed to determine executable path")
        .parent()
        .expect("Executable has no parent directory")
        .to_path_buf()
}

/// Load config from a TOML file. Only reads [port, data_dir, rebuild].
fn load_config_file(path: &std::path::Path) -> Option<toml::Table> {
    let content = std::fs::read_to_string(path).ok()?;
    content.parse::<toml::Table>().ok()
}

/// Write the default config file if it doesn't exist. Returns the path.
fn ensure_config_file(exe_dir: &std::path::Path) -> PathBuf {
    let config_path = exe_dir.join("bitdex.toml");
    if !config_path.exists() {
        if let Err(e) = std::fs::write(&config_path, DEFAULT_CONFIG) {
            eprintln!("Warning: could not write default config to {}: {e}", config_path.display());
        } else {
            eprintln!("Generated default config: {}", config_path.display());
        }
    }
    config_path
}

fn parse_config() -> Config {
    let exe_dir = exe_dir();
    let cli_args: Vec<String> = std::env::args().collect();

    // --- First pass: parse all CLI flags ---
    let mut cli_port: Option<u16> = None;
    let mut cli_data_dir: Option<PathBuf> = None;
    let mut cli_rebuild = false;
    let mut cli_config: Option<PathBuf> = None;

    let mut i = 1;
    while i < cli_args.len() {
        match cli_args[i].as_str() {
            "--port" => {
                i += 1;
                cli_port = Some(cli_args[i].parse().expect("--port must be a number"));
            }
            "--data-dir" => {
                i += 1;
                cli_data_dir = Some(PathBuf::from(&cli_args[i]));
            }
            "--rebuild" => {
                cli_rebuild = true;
            }
            "--config" => {
                i += 1;
                cli_config = Some(PathBuf::from(&cli_args[i]));
            }
            other => {
                eprintln!("Unknown argument: {other}");
                std::process::exit(1);
            }
        }
        i += 1;
    }

    // --- Defaults (relative to exe) ---
    let mut port: u16 = 3000;
    let mut data_dir = exe_dir.join("data");
    let mut rebuild = false;

    // --- Config file ---
    // Only auto-generate bitdex.toml if no CLI args were passed at all
    let has_cli_args = cli_port.is_some() || cli_data_dir.is_some() || cli_rebuild || cli_config.is_some();
    let config_path = match &cli_config {
        Some(path) => path.clone(),
        None => {
            let default_path = exe_dir.join("bitdex.toml");
            if !has_cli_args {
                ensure_config_file(&exe_dir);
            }
            default_path
        }
    };

    // Load config file values (if the file exists)
    if let Some(table) = load_config_file(&config_path) {
        let config_dir = config_path.parent().unwrap_or(&exe_dir);

        if let Some(v) = table.get("port").and_then(|v| v.as_integer()) {
            port = v as u16;
        }
        if let Some(v) = table.get("data_dir").and_then(|v| v.as_str()) {
            let p = PathBuf::from(v);
            data_dir = if p.is_absolute() { p } else { config_dir.join(p) };
        }
        if let Some(v) = table.get("rebuild").and_then(|v| v.as_bool()) {
            rebuild = v;
        }
    }

    // --- CLI flags override everything ---
    if let Some(p) = cli_port {
        port = p;
    }
    if let Some(d) = cli_data_dir {
        data_dir = if d.is_absolute() { d } else { exe_dir.join(d) };
    }
    if cli_rebuild {
        rebuild = true;
    }

    Config { port, data_dir, rebuild }
}

#[tokio::main]
async fn main() {
    std::panic::set_hook(Box::new(|info| {
        eprintln!("FATAL PANIC: {info}");
        eprintln!("Backtrace: {:?}", std::backtrace::Backtrace::force_capture());
    }));

    let config = parse_config();

    // Ensure data directory exists
    if !config.data_dir.exists() {
        std::fs::create_dir_all(&config.data_dir).expect("Failed to create data directory");
        eprintln!("Created data directory: {}", config.data_dir.display());
    }

    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));

    eprintln!("BitDex V2 Server");
    eprintln!("  port: {}", config.port);
    eprintln!("  data-dir: {}", config.data_dir.display());
    if config.rebuild {
        eprintln!("  mode: REBUILD (will rebuild all bitmap indexes from docstore)");
    }

    let mut server = BitdexServer::new(config.data_dir);
    if config.rebuild {
        server = server.with_rebuild(true);
    }
    server.serve(addr).await.expect("Server failed");
}
