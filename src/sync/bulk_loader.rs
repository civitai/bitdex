//! Bulk loader utilities: PG CSV download + ClickHouse metrics download.
//!
//! The V1 in-process bulk load pipeline has been removed.
//! Use the config-driven dump processor via the pg-sync binary instead.
//!
//! Remaining functionality:
//!   - `download_phase_csvs`: Stream phase CSVs from PG to local files
//!   - `download_from_sync_config`: Download all phases from sync config
//!   - `download_metrics_from_clickhouse`: Fetch aggregate metrics from ClickHouse
//!   - `clear_done_markers`: Clear stale .done markers at boot

use std::time::Instant;

use sqlx::PgPool;

// ---------------------------------------------------------------------------
// PG CSV download
// ---------------------------------------------------------------------------

/// Download CSVs using copy_query from sync config dump phases.
///
/// Config-driven replacement for the old download_all_tables — uses the exact COPY SQL
/// from each DumpPhase (and its enrichment lookups) instead of hardcoded queries.
/// This ensures the CSVs match what the dump processor expects.
pub async fn download_from_sync_config(
    pool: &PgPool,
    stage_dir: &std::path::Path,
    dump_phases: &[super::sync_config::DumpPhase],
) -> Result<(), String> {
    std::fs::create_dir_all(stage_dir)
        .map_err(|e| format!("create stage dir: {e}"))?;

    eprintln!("\n=== Downloading CSVs from sync config ({} phases) ===", dump_phases.len());
    let start = Instant::now();
    let mut total_bytes = 0u64;

    for phase in dump_phases {
        // Skip non-PG sources (e.g., ClickHouse metrics)
        if phase.source.as_deref() == Some("clickhouse") {
            eprintln!("  {}: ClickHouse source — skipping PG download", phase.name);
            continue;
        }

        // Download the main table CSV
        if let Some(ref copy_query) = phase.copy_query {
            let ext = if phase.format == "tsv" { "tsv" } else { "csv" };
            let filename = format!("{}.{}", phase.name, ext);
            let header = if !phase.columns.is_empty() { Some(&phase.columns) } else { None };
            let bytes = download_copy_query(pool, stage_dir, &phase.name, &filename, copy_query, header).await?;
            total_bytes += bytes;
        }

        // Download enrichment lookup CSVs
        download_enrichment_csvs(pool, stage_dir, &phase.enrichment).await?;
    }

    eprintln!(
        "CSV download complete: {:.1} GB in {:.1}s",
        total_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
        start.elapsed().as_secs_f64(),
    );

    Ok(())
}

/// Download CSVs for a single dump phase (main table + enrichment lookups).
/// Used by the streaming pipeline to download per-phase instead of all-at-once.
pub async fn download_phase_csvs(
    pool: &PgPool,
    stage_dir: &std::path::Path,
    phase: &super::sync_config::DumpPhase,
) -> Result<u64, String> {
    std::fs::create_dir_all(stage_dir)
        .map_err(|e| format!("create stage dir: {e}"))?;

    let mut total_bytes = 0u64;

    if phase.source.as_deref() == Some("clickhouse") {
        return Ok(0); // ClickHouse handled separately
    }

    // Download the main table CSV
    if let Some(ref copy_query) = phase.copy_query {
        let ext = if phase.format == "tsv" { "tsv" } else { "csv" };
        let filename = format!("{}.{}", phase.name, ext);
        let header = if !phase.columns.is_empty() { Some(&phase.columns) } else { None };
        let bytes = download_copy_query(pool, stage_dir, &phase.name, &filename, copy_query, header).await?;
        total_bytes += bytes;
    }

    // Download enrichment lookup CSVs
    download_enrichment_csvs(pool, stage_dir, &phase.enrichment).await?;

    Ok(total_bytes)
}

/// Clear all .done markers in the stage directory.
/// Called at boot to prevent stale markers from a previous run (e.g., after PVC wipe)
/// from causing downloads to be skipped.
pub fn clear_done_markers(stage_dir: &std::path::Path) {
    if let Ok(entries) = std::fs::read_dir(stage_dir) {
        let mut cleared = 0;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("done") {
                if std::fs::remove_file(&path).is_ok() {
                    cleared += 1;
                }
            }
        }
        if cleared > 0 {
            eprintln!("Cleared {cleared} stale .done markers from {}", stage_dir.display());
        }
    }
}

/// Recursively download enrichment lookup CSVs.
async fn download_enrichment_csvs(
    pool: &PgPool,
    stage_dir: &std::path::Path,
    enrichments: &[super::sync_config::EnrichmentDef],
) -> Result<(), String> {
    for enrich in enrichments {
        if let (Some(ref lookup), Some(ref copy_query)) = (&enrich.lookup, &enrich.copy_query) {
            let name = enrich.table.as_deref().unwrap_or(lookup.trim_end_matches(".csv"));
            let header = if !enrich.columns.is_empty() { Some(&enrich.columns) } else { None };
            download_copy_query(pool, stage_dir, name, lookup, copy_query, header).await?;
        }
        // Recurse into nested enrichments
        if !enrich.enrichment.is_empty() {
            Box::pin(download_enrichment_csvs(pool, stage_dir, &enrich.enrichment)).await?;
        }
    }
    Ok(())
}

/// Execute a COPY query and stream results to a CSV file.
/// Skips if the .done marker already exists (idempotent on retry).
async fn download_copy_query(
    pool: &PgPool,
    stage_dir: &std::path::Path,
    name: &str,
    filename: &str,
    copy_query: &str,
    columns: Option<&Vec<String>>,
) -> Result<u64, String> {
    use futures_util::TryStreamExt;
    use sqlx::postgres::PgPoolCopyExt;
    use tokio::io::AsyncWriteExt;

    let csv_path = stage_dir.join(filename);
    let done_path = stage_dir.join(format!("{}.done", filename));

    // Skip if already downloaded
    if done_path.exists() {
        let size = std::fs::metadata(&csv_path).map(|m| m.len()).unwrap_or(0);
        eprintln!("  {}: already downloaded ({:.1} MB), skipping", name, size as f64 / 1048576.0);
        return Ok(size);
    }

    eprintln!("  {}: downloading via COPY...", name);

    let file = tokio::fs::File::create(&csv_path)
        .await
        .map_err(|e| format!("{name}: create file: {e}"))?;
    let mut writer = tokio::io::BufWriter::with_capacity(1024 * 1024, file);
    let mut bytes_written = 0u64;
    let start_time = Instant::now();

    // Prepend CSV header line when columns are specified (PG COPY TO STDOUT
    // doesn't support HEADER — we add it ourselves from the sync config).
    if let Some(cols) = columns {
        let header_line = format!("{}\n", cols.join(","));
        writer.write_all(header_line.as_bytes()).await
            .map_err(|e| format!("{name}: write header: {e}"))?;
        bytes_written += header_line.len() as u64;
    }

    // Use copy_out_raw — same API as copy_queries.rs
    let mut stream = pool
        .copy_out_raw(copy_query)
        .await
        .map_err(|e| format!("{name}: COPY start failed: {e}"))?;

    while let Some(chunk) = stream
        .try_next()
        .await
        .map_err(|e| format!("{name}: COPY stream: {e}"))?
    {
        writer
            .write_all(&chunk)
            .await
            .map_err(|e| format!("{name}: write: {e}"))?;
        bytes_written += chunk.len() as u64;
    }
    writer.flush().await.map_err(|e| format!("{name}: flush: {e}"))?;

    // Write .done marker
    std::fs::write(&done_path, b"ok")
        .map_err(|e| format!("{name}: write done marker: {e}"))?;

    let elapsed = start_time.elapsed();
    eprintln!(
        "  {}: {:.1} MB in {:.1}s ({:.0} MB/s)",
        name,
        bytes_written as f64 / 1048576.0,
        elapsed.as_secs_f64(),
        bytes_written as f64 / 1048576.0 / elapsed.as_secs_f64().max(0.001),
    );

    Ok(bytes_written)
}

// ---------------------------------------------------------------------------
// ClickHouse metrics download
// ---------------------------------------------------------------------------

/// Download all-time aggregate metrics from ClickHouse to a TSV file.
/// Query: entityMetricDailyAgg grouped by entityId for entityType='Image'.
/// Output: metrics.tsv in stage_dir (id\treactionCount\tcommentCount\tcollectedCount).
pub async fn download_metrics_from_clickhouse(
    stage_dir: &std::path::Path,
    ch_url: &str,
    ch_username: Option<&str>,
    ch_password: Option<&str>,
) -> Result<u64, String> {
    let done_path = stage_dir.join("metrics.tsv.done");
    if done_path.exists() {
        eprintln!("metrics.tsv already downloaded (found .done marker)");
        return Ok(0);
    }

    let csv_path = stage_dir.join("metrics.tsv");
    eprintln!("Downloading ClickHouse metrics to {} ...", csv_path.display());

    let query = r#"SELECT
        entityId as imageId,
        sumIf(total, metricType IN ('ReactionLike','ReactionHeart','ReactionLaugh','ReactionCry')) as reactionCount,
        sumIf(total, metricType = 'Comment') as commentCount,
        sumIf(total, metricType = 'Collection') as collectedCount
    FROM entityMetricDailyAgg
    WHERE entityType = 'Image'
    GROUP BY entityId
    FORMAT TSVWithNames"#;

    let http = reqwest::Client::new();
    let mut req = http.post(ch_url).body(query.to_string());

    if let Some(username) = ch_username {
        let password = ch_password.unwrap_or("");
        req = req.basic_auth(username, Some(password));
    }

    let resp = req
        .send()
        .await
        .map_err(|e| format!("ClickHouse request failed: {e}"))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("ClickHouse returned {status}: {body}"));
    }

    // Stream response to disk — don't buffer in memory (OOMKilled at 107M rows).
    let mut file = tokio::fs::File::create(&csv_path)
        .await
        .map_err(|e| format!("create metrics.tsv: {e}"))?;
    let mut bytes_written = 0u64;
    let mut row_count = 0u64;
    let mut stream = resp;
    while let Some(chunk) = stream.chunk().await.map_err(|e| format!("read CH chunk: {e}"))? {
        tokio::io::AsyncWriteExt::write_all(&mut file, &chunk)
            .await
            .map_err(|e| format!("write metrics.tsv: {e}"))?;
        row_count += chunk.iter().filter(|&&b| b == b'\n').count() as u64;
        bytes_written += chunk.len() as u64;
    }
    tokio::io::AsyncWriteExt::flush(&mut file)
        .await
        .map_err(|e| format!("flush metrics.tsv: {e}"))?;
    eprintln!(
        "Downloaded {} metric rows from ClickHouse ({:.1} MB)",
        row_count,
        bytes_written as f64 / 1048576.0
    );

    std::fs::write(&done_path, format!("{row_count}"))
        .map_err(|e| format!("write .done marker: {e}"))?;

    Ok(row_count)
}
