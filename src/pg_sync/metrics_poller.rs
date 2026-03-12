//! ClickHouse metrics poller: polls for recent metric events, fetches aggregate
//! counts, rebuilds full docs from PG, and pushes to Bitdex.
//!
//! ClickHouse is queried via its HTTP interface (POST with SQL).

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use reqwest::Client;
use sqlx::PgPool;
use tokio::time::{Duration, interval};

use super::bitdex_client::BitdexClient;
use super::queries;
use super::row_assembler::{assemble_batch, EnrichmentData, MetricInfo};

/// ClickHouse connection config.
pub struct ClickHouseConfig {
    pub url: String,
    pub username: Option<String>,
    pub password: Option<String>,
}

/// Run the ClickHouse metrics poller loop. Runs forever until cancelled.
pub async fn run_metrics_poller(
    pool: &PgPool,
    ch_config: &ClickHouseConfig,
    bitdex_client: &BitdexClient,
    poll_interval_secs: u64,
) -> Result<(), String> {
    let mut ticker = interval(Duration::from_secs(poll_interval_secs));
    let http = Client::new();
    let mut last_poll_ts = current_epoch_secs() - poll_interval_secs as i64;

    eprintln!(
        "Metrics poller started (ClickHouse={}, interval={poll_interval_secs}s)",
        ch_config.url
    );

    loop {
        ticker.tick().await;
        let now = current_epoch_secs();

        match poll_metrics_and_push(pool, &http, ch_config, bitdex_client, last_poll_ts).await {
            Ok(count) => {
                if count > 0 {
                    eprintln!("Metrics: updated {count} documents");
                }
                last_poll_ts = now;
            }
            Err(e) => {
                eprintln!("Metrics poll error: {e}");
                // Don't advance last_poll_ts — retry same window
            }
        }
    }
}

fn current_epoch_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

/// Single poll + push cycle.
async fn poll_metrics_and_push(
    pool: &PgPool,
    http: &Client,
    ch_config: &ClickHouseConfig,
    bitdex_client: &BitdexClient,
    since_ts: i64,
) -> Result<usize, String> {
    // Query ClickHouse for image IDs with recent metric events
    let metrics = fetch_metrics_from_clickhouse(http, ch_config, since_ts).await?;

    if metrics.is_empty() {
        return Ok(0);
    }

    let image_ids: Vec<i64> = metrics.keys().copied().collect();

    // Fetch full documents from PG (same enrichment pipeline as outbox)
    let images = queries::fetch_images_by_ids(pool, &image_ids)
        .await
        .map_err(|e| format!("fetch_images_by_ids: {e}"))?;

    if images.is_empty() {
        return Ok(0);
    }

    let fetched_ids: Vec<i64> = images.iter().map(|r| r.id).collect();

    let (tags, tools, techniques, resources) = tokio::try_join!(
        queries::fetch_tags(pool, &fetched_ids),
        queries::fetch_tools(pool, &fetched_ids),
        queries::fetch_techniques(pool, &fetched_ids),
        queries::fetch_resources(pool, &fetched_ids),
    )
    .map_err(|e| format!("enrichment queries: {e}"))?;

    let mut enrichment = EnrichmentData::from_rows(tags, tools, techniques, resources);

    // Merge ClickHouse metrics into enrichment
    enrichment.metrics = metrics;

    let docs = assemble_batch(&images, &enrichment);
    let count = docs.len();

    if !docs.is_empty() {
        bitdex_client.upsert_batch(&docs).await?;
    }

    Ok(count)
}

/// Query ClickHouse HTTP interface for aggregate metrics.
///
/// Finds entities with recent activity (day >= since_ts), then fetches their
/// ALL-TIME totals so the search index has cumulative counts.
///
/// Table schema: entityType (String), entityId (Int32), metricType (String),
///               day (Date), total (Int64)
/// Metric types for Image: ReactionLike, ReactionHeart, ReactionLaugh,
///                          ReactionCry, Comment, Collection, Buzz
async fn fetch_metrics_from_clickhouse(
    http: &Client,
    ch_config: &ClickHouseConfig,
    since_ts: i64,
) -> Result<HashMap<i64, MetricInfo>, String> {
    // Find entities with recent activity, then get their all-time totals
    let query = format!(
        r#"SELECT
            entityId as id,
            sumIf(total, metricType IN ('ReactionLike','ReactionHeart','ReactionLaugh','ReactionCry')) as reactionCount,
            sumIf(total, metricType = 'Comment') as commentCount,
            sumIf(total, metricType = 'Collection') as collectedCount
        FROM entityMetricDailyAgg
        WHERE entityType = 'Image'
          AND entityId IN (
            SELECT DISTINCT entityId
            FROM entityMetricDailyAgg
            WHERE entityType = 'Image'
              AND day >= toDate(fromUnixTimestamp({since_ts}))
          )
        GROUP BY entityId
        FORMAT JSONEachRow"#,
    );

    let mut req = http.post(&ch_config.url).body(query);

    // Add Basic auth if credentials provided
    if let Some(ref username) = ch_config.username {
        let password = ch_config.password.as_deref().unwrap_or("");
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

    let body = resp.text().await.map_err(|e| format!("read body: {e}"))?;
    let mut metrics = HashMap::new();

    for line in body.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let row: serde_json::Value =
            serde_json::from_str(line).map_err(|e| format!("parse CH row: {e}"))?;

        let id = row["id"]
            .as_i64()
            .ok_or_else(|| "missing id in CH response".to_string())?;
        let reaction_count = row["reactionCount"].as_i64().unwrap_or(0);
        let comment_count = row["commentCount"].as_i64().unwrap_or(0);
        let collected_count = row["collectedCount"].as_i64().unwrap_or(0);

        metrics.insert(
            id,
            MetricInfo {
                reaction_count,
                comment_count,
                collected_count,
            },
        );
    }

    Ok(metrics)
}
