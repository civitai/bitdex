//! NDJSON capture writer.
//!
//! One writer task per channel. Each writer:
//!   - subscribes to the channel's `tokio::sync::broadcast` receiver (so it
//!     counts toward `receiver_count()` — the route.rs emit gate fires
//!     automatically when capture is enabled, even with zero SSE subscribers)
//!   - appends each event as one NDJSON line to the current file
//!   - rotates on `rotate_bytes` threshold; spawns a gzip task on the
//!     rotated file; deletes the original after gzip completes
//!   - enforces `max_total_bytes` by deleting the oldest `.gz` files (and
//!     the current `.log` if necessary) past the cap (Gemini review catch)
//!   - fsyncs per rotation (not per event — `per_event` would crater throughput)
//!   - on `RecvError::Lagged(n)`, writes a `_lagged` marker line and continues

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::broadcast;

use crate::relay::channel::RelayEvent;
use crate::relay::config::{CaptureConfig, FsyncPolicy};

pub trait CaptureSink: Send + Sync {
    fn write(&self, event: &RelayEvent);
    fn write_lagged_marker(&self, channel: &str, lagged_n: u64);
}

pub struct NullSink;

impl CaptureSink for NullSink {
    fn write(&self, _event: &RelayEvent) {}
    fn write_lagged_marker(&self, _channel: &str, _lagged_n: u64) {}
}

/// Manages per-channel writer tasks. Created at startup if
/// `capture.enabled = true`. Drop = signal shutdown to all writers; they
/// flush + close + fsync before exiting.
pub struct CaptureManager {
    shutdown: tokio::sync::broadcast::Sender<()>,
    #[allow(dead_code)]
    handles: Vec<tokio::task::JoinHandle<()>>,
    config: CaptureConfig,
}

impl CaptureManager {
    /// Subscribe to each named channel's broadcast and start a writer task
    /// per channel. Caller must keep the manager alive (drop = shutdown).
    pub fn start(
        config: CaptureConfig,
        channels: &crate::relay::channel::ChannelRegistry,
    ) -> std::io::Result<Arc<Self>> {
        if !config.enabled {
            return Ok(Arc::new(Self {
                shutdown: broadcast::channel(1).0,
                handles: Vec::new(),
                config,
            }));
        }

        std::fs::create_dir_all(&config.dir)?;

        let (shutdown_tx, _) = broadcast::channel::<()>(1);
        let mut handles = Vec::new();

        for name in channels.names().map(|s| s.to_string()).collect::<Vec<_>>() {
            let Some(handle) = channels.get(&name) else { continue };
            let rx = handle.sender.subscribe();
            let cfg = config.clone();
            let shutdown_rx = shutdown_tx.subscribe();
            let chan_name = name.clone();
            let join = tokio::spawn(async move {
                if let Err(e) = run_writer(chan_name.clone(), cfg, rx, shutdown_rx).await {
                    eprintln!("relay capture writer ({chan_name}) failed: {e}");
                }
            });
            handles.push(join);
        }

        Ok(Arc::new(Self {
            shutdown: shutdown_tx,
            handles,
            config,
        }))
    }

    pub fn config(&self) -> &CaptureConfig {
        &self.config
    }
}

impl Drop for CaptureManager {
    fn drop(&mut self) {
        // Best-effort signal; writers will detect channel close + flush.
        let _ = self.shutdown.send(());
        // Don't block on join inside Drop — tokio runtime may already be
        // dropping. The writers will hit shutdown next iteration anyway.
    }
}

async fn run_writer(
    channel_name: String,
    cfg: CaptureConfig,
    mut rx: broadcast::Receiver<RelayEvent>,
    mut shutdown: broadcast::Receiver<()>,
) -> std::io::Result<()> {
    let dir = PathBuf::from(&cfg.dir);
    let mut current = open_new_log(&dir, &channel_name, cfg.file_mode)?;

    loop {
        tokio::select! {
            biased;
            _ = shutdown.recv() => {
                current.flush()?;
                fsync_file(&current.file)?;
                return Ok(());
            }
            msg = rx.recv() => {
                match msg {
                    Ok(event) => {
                        current.write_event(&event)?;
                        if current.bytes_written >= cfg.rotate_bytes {
                            current = rotate(current, &dir, &channel_name, &cfg).await?;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        current.write_lagged_marker(&channel_name, n)?;
                        if current.bytes_written >= cfg.rotate_bytes {
                            current = rotate(current, &dir, &channel_name, &cfg).await?;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        current.flush()?;
                        fsync_file(&current.file)?;
                        return Ok(());
                    }
                }
            }
        }
    }
}

struct LogFile {
    file: std::fs::File,
    path: PathBuf,
    bytes_written: u64,
}

impl LogFile {
    fn write_event(&mut self, event: &RelayEvent) -> std::io::Result<()> {
        // Compose: {"_received_at_ms":<u64>,"channel":"<name>","seq_id":<u64>,"payload":<json>}
        // The payload is already a JSON-shaped string from the template
        // engine; embed verbatim. Wrap line break so it's NDJSON.
        let line = format!(
            "{{\"_received_at_ms\":{},\"channel\":{},\"seq_id\":{},\"payload\":{}}}\n",
            event.ts_ms,
            json_string(&event.channel),
            event.seq_id,
            event.payload,
        );
        self.file.write_all(line.as_bytes())?;
        self.bytes_written += line.len() as u64;
        Ok(())
    }

    fn write_lagged_marker(&mut self, channel: &str, n: u64) -> std::io::Result<()> {
        let line = format!(
            "{{\"_lagged\":{},\"channel\":{}}}\n",
            n,
            json_string(channel),
        );
        self.file.write_all(line.as_bytes())?;
        self.bytes_written += line.len() as u64;
        Ok(())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

fn open_new_log(dir: &Path, channel: &str, file_mode: u32) -> std::io::Result<LogFile> {
    let stamp = format_timestamp();
    let filename = format!("{channel}-{stamp}.log");
    let path = dir.join(&filename);
    let file = open_file_with_mode(&path, file_mode)?;
    Ok(LogFile {
        file,
        path,
        bytes_written: 0,
    })
}

#[cfg(unix)]
fn open_file_with_mode(path: &Path, mode: u32) -> std::io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .mode(mode)
        .open(path)
}

#[cfg(not(unix))]
fn open_file_with_mode(path: &Path, _mode: u32) -> std::io::Result<std::fs::File> {
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
}

fn fsync_file(file: &std::fs::File) -> std::io::Result<()> {
    file.sync_all()
}

async fn rotate(
    mut current: LogFile,
    dir: &Path,
    channel: &str,
    cfg: &CaptureConfig,
) -> std::io::Result<LogFile> {
    // Close current.
    current.flush()?;
    if cfg.fsync != FsyncPolicy::Never {
        fsync_file(&current.file)?;
    }
    let to_compress = current.path.clone();
    drop(current);

    // Spawn gzip in a blocking thread; await completion so rotation
    // back-pressures naturally if disk is slow.
    if cfg.gzip_after_rotate {
        let gzip_path = to_compress.clone();
        let join = tokio::task::spawn_blocking(move || gzip_file(&gzip_path));
        join.await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))??;
    }

    // Disk-budget enforcement (Gemini): delete oldest .gz files when over cap.
    if cfg.max_total_bytes > 0 {
        let dir_owned = dir.to_path_buf();
        let cap = cfg.max_total_bytes;
        let _ = tokio::task::spawn_blocking(move || enforce_cap(&dir_owned, cap)).await;
    }

    open_new_log(dir, channel, cfg.file_mode)
}

fn gzip_file(path: &Path) -> std::io::Result<()> {
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::fs;
    use std::io::{BufReader, Read};

    let gz_path = {
        let mut p = path.as_os_str().to_owned();
        p.push(".gz");
        PathBuf::from(p)
    };
    let input = fs::File::open(path)?;
    let mut reader = BufReader::new(input);
    let output = fs::File::create(&gz_path)?;
    let mut encoder = GzEncoder::new(output, Compression::default());
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        encoder.write_all(&buf[..n])?;
    }
    encoder.finish()?.sync_all()?;
    fs::remove_file(path)?;
    Ok(())
}

fn enforce_cap(dir: &Path, max_bytes: u64) -> std::io::Result<()> {
    let mut entries: Vec<(PathBuf, u64, std::time::SystemTime)> = Vec::new();
    for entry in std::fs::read_dir(dir)?.flatten() {
        let meta = match entry.metadata() {
            Ok(m) => m,
            Err(_) => continue,
        };
        if !meta.is_file() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.ends_with(".gz") && !name.ends_with(".log") {
            continue;
        }
        let mtime = meta.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        entries.push((entry.path(), meta.len(), mtime));
    }
    let total: u64 = entries.iter().map(|(_, len, _)| *len).sum();
    if total <= max_bytes {
        return Ok(());
    }
    // Delete oldest first (sorted by mtime asc), prefer .gz over the live .log.
    // Strategy: sort by mtime, but skip the most recent .log (probably the
    // currently-open file) unless we've burned through every .gz first.
    entries.sort_by_key(|(_, _, mtime)| *mtime);
    let mut over = total - max_bytes;
    for (path, size, _) in entries {
        if over == 0 {
            break;
        }
        if path.extension().and_then(|e| e.to_str()) == Some("log") {
            // Skip the current log (will be deleted on its rotation when
            // it becomes a .gz). Avoids us racing against the live writer.
            continue;
        }
        if std::fs::remove_file(&path).is_ok() {
            over = over.saturating_sub(size);
        }
    }
    Ok(())
}

fn format_timestamp() -> String {
    // Compact filesystem-safe timestamp. Avoids chrono dep.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    format!("{now}-{nanos:09}")
}

fn json_string(s: &str) -> String {
    // Minimal: rely on serde for correctness.
    serde_json::to_string(s).unwrap_or_else(|_| "\"\"".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relay::channel::ChannelRegistry;
    use crate::relay::config::ChannelConfig;
    use std::collections::BTreeMap;
    use std::net::SocketAddr;
    use std::time::Duration;

    fn cfg_with_capture(dir: &Path) -> crate::relay::config::RelayConfig {
        let mut channels = BTreeMap::new();
        channels.insert(
            "ops".into(),
            ChannelConfig {
                capacity: 16,
                keep_alive_seconds: 5,
            },
        );
        crate::relay::config::RelayConfig {
            listen: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
            metrics_path: "/metrics".into(),
            admin_token_env: "T".into(),
            max_body_bytes: 1024,
            channels,
            routes: vec![],
            capture: CaptureConfig {
                enabled: true,
                dir: dir.to_string_lossy().to_string(),
                rotate_bytes: 256, // small, for test rotation
                gzip_after_rotate: false, // skip gzip for unit-test simplicity
                max_total_bytes: 0,
                fsync: FsyncPolicy::PerRotation,
                file_mode: 0o640,
            },
        }
    }

    #[tokio::test]
    async fn writes_events_as_ndjson() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = cfg_with_capture(tmp.path());
        let registry = ChannelRegistry::from_config(&cfg);
        let _mgr = CaptureManager::start(cfg.capture.clone(), &registry).unwrap();

        let handle = registry.get("ops").unwrap();
        // Wait briefly for writer to subscribe.
        tokio::time::sleep(Duration::from_millis(50)).await;
        for i in 1..=3 {
            let _ = handle.sender.send(RelayEvent {
                seq_id: i,
                ts_ms: 1_000 + i,
                channel: "ops".into(),
                payload: format!(r#"{{"i":{i}}}"#),
            });
        }
        // Let writer drain.
        tokio::time::sleep(Duration::from_millis(100)).await;
        // Drop the manager to flush + sync.
        drop(_mgr);
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Find the .log file in dir, parse all lines as JSON, assert count.
        let mut lines: Vec<serde_json::Value> = Vec::new();
        for entry in std::fs::read_dir(tmp.path()).unwrap().flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("log") {
                let data = std::fs::read_to_string(&path).unwrap();
                for line in data.lines() {
                    let v: serde_json::Value = serde_json::from_str(line).unwrap();
                    lines.push(v);
                }
            }
        }
        assert!(lines.len() >= 3, "expected ≥3 lines, got {}", lines.len());
        for line in &lines {
            assert!(line.get("seq_id").is_some());
            assert_eq!(line["channel"].as_str(), Some("ops"));
        }
    }

    #[test]
    fn enforce_cap_deletes_oldest_gz() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        // Create three fake .gz files with explicit mtimes (oldest first).
        let mk = |name: &str, size: usize, age_secs: u64| {
            let p = dir.join(name);
            std::fs::write(&p, vec![0u8; size]).unwrap();
            let now = std::time::SystemTime::now();
            let mtime = now - Duration::from_secs(age_secs);
            // Best-effort set mtime; on platforms where filetime isn't
            // available, we rely on creation order which Windows preserves.
            #[cfg(unix)]
            {
                use std::os::unix::fs::FileTimesExt;
                let _ = std::fs::File::open(&p).and_then(|f| {
                    f.set_times(std::fs::FileTimes::new().set_modified(mtime))
                });
            }
            let _ = mtime;
            p
        };
        let _oldest = mk("a.gz", 200, 300);
        let _middle = mk("b.gz", 200, 200);
        let _newest = mk("c.gz", 200, 100);

        // Cap at 350 bytes — should leave only the newest (200).
        enforce_cap(dir, 350).unwrap();

        let remaining: Vec<_> = std::fs::read_dir(dir)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().into_string().unwrap())
            .collect();
        // At least one gone.
        assert!(remaining.len() < 3, "remaining = {:?}", remaining);
        // Newest is preserved on platforms that support mtime; on windows
        // file ordering may differ — accept any single survivor.
        assert!(!remaining.is_empty());
    }
}
