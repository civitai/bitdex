//! BitDex V2 HTTP Server
//!
//! Starts blank — create indexes and load data via the API.
//!
//! Usage:
//!   cargo run --release --features server --bin bitdex-server -- [OPTIONS]
//!
//! Options:
//!   --port <N>         HTTP port (default: 3000)
//!   --data-dir <PATH>  Data directory for index storage (default: ./data)

#[global_allocator]
static ALLOC: rpmalloc::RpMalloc = rpmalloc::RpMalloc;

use std::net::SocketAddr;
use std::path::PathBuf;

use bitdex_v2::server::BitdexServer;

struct Args {
    port: u16,
    data_dir: PathBuf,
}

fn parse_args() -> Args {
    let args: Vec<String> = std::env::args().collect();
    let mut port: u16 = 3000;
    let mut data_dir = PathBuf::from("./data");

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--port" => {
                i += 1;
                port = args[i].parse().expect("--port must be a number");
            }
            "--data-dir" => {
                i += 1;
                data_dir = PathBuf::from(&args[i]);
            }
            other => {
                eprintln!("Unknown argument: {other}");
                std::process::exit(1);
            }
        }
        i += 1;
    }

    Args { port, data_dir }
}

#[tokio::main]
async fn main() {
    std::panic::set_hook(Box::new(|info| {
        eprintln!("FATAL PANIC: {info}");
        eprintln!("Backtrace: {:?}", std::backtrace::Backtrace::force_capture());
    }));

    let args = parse_args();
    let addr = SocketAddr::from(([0, 0, 0, 0], args.port));

    eprintln!("BitDex V2 Server");
    eprintln!("  port: {}", args.port);
    eprintln!("  data-dir: {}", args.data_dir.display());

    let server = BitdexServer::new(args.data_dir);
    server.serve(addr).await.expect("Server failed");
}
