//! ClickHouse metrics poller: polls for recent metric events, fetches aggregate
//! counts, and pushes sort-field ops to BitDex via the V2 ops pipeline.
//!
//! ClickHouse is queried via its HTTP interface (POST with SQL).
//! Metrics (reactionCount, commentCount, collectedCount) are sort-only fields,
//! so ops are sent with `creates_slot: false` — they update existing slots
//! without touching the alive bitmap.

use ahash::AHashMap as HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use reqwest::Client;
use serde_json::json;
use tokio::time::{Duration, interval};

use super::bitdex_client::BitdexClient;
use super::ops::{EntityOps, Op, OpsBatch, SyncMeta};

/// ClickHouse connection config.
pub struct ClickHouseConfig {
    pub url: String,
    pub username: Option<String>,
    pub password: Option<String>,
}

/// Aggregate metric counts for a single image from ClickHouse.
struct MetricInfo {
    reaction_count: i64,
    comment_count: i64,
    collected_count: i64,
}

/// Run the ClickHouse metrics poller loop. Runs forever until cancelled.
///
/// V2 pipeline: fetches aggregate counts from ClickHouse, converts them to
/// `Op::Set` ops for sort fields, and POSTs via the `/ops` endpoint.
/// No PG round-trip needed — metrics are self-contained sort-field updates.
pub async fn run_metrics_poller(
    ch_config: &ClickHouseConfig,
    bitdex_client: &BitdexClient,
    poll_interval_secs: u64,
) -> Result<(), String> {
    let mut ticker = interval(Duration::from_secs(poll_interval_secs));
    let http = Client::new();
    let mut last_poll_ts = current_epoch_secs() - poll_interval_secs as i64;
    let mut bitdex_was_down = false;

    eprintln!(
        "Metrics poller started (ClickHouse={}, interval={poll_interval_secs}s)",
        ch_config.url
    );

    loop {
        ticker.tick().await;

        // Health gate: skip ClickHouse fetch if BitDex is unreachable.
        if !bitdex_client.is_healthy().await {
            if !bitdex_was_down {
                eprintln!("Metrics: BitDex is unreachable, pausing until healthy");
                bitdex_was_down = true;
            }
            continue;
        }
        if bitdex_was_down {
            eprintln!("Metrics: BitDex is back, resuming");
            bitdex_was_down = false;
        }

        let now = current_epoch_secs();

        match poll_metrics_and_push(&http, ch_config, bitdex_client, last_poll_ts).await {
            Ok(count) => {
                if count > 0 {
                    eprintln!("Metrics: pushed {count} ops batches");
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

/// Maximum number of entity ops per HTTP request to `/ops`.
/// Keeps request bodies reasonable and avoids timeouts.
const OPS_BATCH_SIZE: usize = 5_000;

/// Single poll + push cycle. Fetches CH metrics, converts to V2 ops, POSTs to BitDex.
async fn poll_metrics_and_push(
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

    let entity_ops = metrics_to_entity_ops(metrics);

    let total = entity_ops.len();

    // Send in batches to keep request sizes manageable.
    for chunk in entity_ops.chunks(OPS_BATCH_SIZE) {
        let batch = OpsBatch {
            ops: chunk.to_vec(),
            meta: Some(SyncMeta {
                source: "clickhouse-metrics".into(),
                cursor: None,
                max_id: None,
                lag_rows: None,
            }),
        };
        bitdex_client.post_ops(&batch).await?;
    }

    Ok(total)
}

/// Query ClickHouse HTTP interface for aggregate metrics.
///
/// Two-phase approach:
/// 1. Discovery: `entityMetricEvents` — find image IDs with recent activity
///    (has `createdAt` for precise recency, unlike daily aggregates)
/// 2. Totals: `entityMetricDailyAgg` — fetch ALL-TIME cumulative counts
///    for those IDs so the search index stays correct.
///
/// Metric types for Image: ReactionLike, ReactionHeart, ReactionLaugh,
///                          ReactionCry, Comment, Collection, Buzz
async fn fetch_metrics_from_clickhouse(
    http: &Client,
    ch_config: &ClickHouseConfig,
    since_ts: i64,
) -> Result<HashMap<i64, MetricInfo>, String> {
    // Phase 1: discover IDs with recent metric events
    // Phase 2: get their all-time totals from the daily aggregate table
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
            FROM entityMetricEvents
            WHERE entityType = 'Image'
              AND entityId IS NOT NULL
              AND createdAt > fromUnixTimestamp({since_ts})
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

        // ClickHouse JSONEachRow quotes Int64 values as strings by default
        // (output_format_json_quote_64bit_integers=1), so as_i64() alone returns
        // None and silently zeroes every metric. Parse string form too.
        fn read_count(v: &serde_json::Value) -> i64 {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                .unwrap_or(0)
        }
        let id = row["id"]
            .as_i64()
            .or_else(|| row["id"].as_str().and_then(|s| s.parse().ok()))
            .ok_or_else(|| "missing id in CH response".to_string())?;
        let reaction_count = read_count(&row["reactionCount"]);
        let comment_count = read_count(&row["commentCount"]);
        let collected_count = read_count(&row["collectedCount"]);

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

/// Convert a map of CH metrics into V2 EntityOps.
///
/// Each image gets three `Op::Set` ops (reactionCount, commentCount, collectedCount).
/// `creates_slot` is false because these are sort-only field updates — they should
/// never create new alive slots.
fn metrics_to_entity_ops(metrics: HashMap<i64, MetricInfo>) -> Vec<EntityOps> {
    metrics
        .into_iter()
        .map(|(image_id, info)| {
            EntityOps::new(
                image_id,
                vec![
                    Op::Set {
                        field: "reactionCount".into(),
                        value: json!(info.reaction_count),
                    },
                    Op::Set {
                        field: "commentCount".into(),
                        value: json!(info.comment_count),
                    },
                    Op::Set {
                        field: "collectedCount".into(),
                        value: json!(info.collected_count),
                    },
                ],
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_to_entity_ops_single() {
        let mut metrics = HashMap::new();
        metrics.insert(
            42,
            MetricInfo {
                reaction_count: 100,
                comment_count: 5,
                collected_count: 3,
            },
        );

        let ops = metrics_to_entity_ops(metrics);
        assert_eq!(ops.len(), 1);

        let entity = &ops[0];
        assert_eq!(entity.entity_id, 42);
        assert!(!entity.creates_slot, "metrics ops must not create slots");
        assert_eq!(entity.ops.len(), 3);

        // Verify all three sort fields are present as Set ops
        let fields: Vec<&str> = entity
            .ops
            .iter()
            .filter_map(|op| match op {
                Op::Set { field, .. } => Some(field.as_str()),
                _ => None,
            })
            .collect();
        assert!(fields.contains(&"reactionCount"));
        assert!(fields.contains(&"commentCount"));
        assert!(fields.contains(&"collectedCount"));
    }

    #[test]
    fn test_metrics_to_entity_ops_values() {
        let mut metrics = HashMap::new();
        metrics.insert(
            99,
            MetricInfo {
                reaction_count: 1234,
                comment_count: 56,
                collected_count: 78,
            },
        );

        let ops = metrics_to_entity_ops(metrics);
        let entity = &ops[0];

        for op in &entity.ops {
            match op {
                Op::Set { field, value } => match field.as_str() {
                    "reactionCount" => assert_eq!(value, &json!(1234)),
                    "commentCount" => assert_eq!(value, &json!(56)),
                    "collectedCount" => assert_eq!(value, &json!(78)),
                    other => panic!("unexpected field: {other}"),
                },
                other => panic!("expected Op::Set, got {other:?}"),
            }
        }
    }

    #[test]
    fn test_metrics_to_entity_ops_empty() {
        let metrics = HashMap::new();
        let ops = metrics_to_entity_ops(metrics);
        assert!(ops.is_empty());
    }

    #[test]
    fn test_metrics_to_entity_ops_multiple_images() {
        let mut metrics = HashMap::new();
        for id in 1..=100 {
            metrics.insert(
                id,
                MetricInfo {
                    reaction_count: id * 10,
                    comment_count: id,
                    collected_count: id / 2,
                },
            );
        }

        let ops = metrics_to_entity_ops(metrics);
        assert_eq!(ops.len(), 100);

        // Every entry should have creates_slot = false and 3 ops
        for entity in &ops {
            assert!(!entity.creates_slot);
            assert_eq!(entity.ops.len(), 3);
        }
    }

    #[test]
    fn test_metrics_ops_batch_serialization() {
        let mut metrics = HashMap::new();
        metrics.insert(
            42,
            MetricInfo {
                reaction_count: 100,
                comment_count: 5,
                collected_count: 3,
            },
        );

        let entity_ops = metrics_to_entity_ops(metrics);
        let batch = OpsBatch {
            ops: entity_ops,
            meta: Some(SyncMeta {
                source: "clickhouse-metrics".into(),
                cursor: None,
                max_id: None,
                lag_rows: None,
            }),
        };

        // Verify it serializes to valid JSON matching the expected ops format
        let json = serde_json::to_value(&batch).unwrap();
        assert_eq!(json["meta"]["source"], "clickhouse-metrics");
        assert_eq!(json["ops"].as_array().unwrap().len(), 1);

        let first = &json["ops"][0];
        assert_eq!(first["entity_id"], 42);
        assert_eq!(first["creates_slot"], false);
        assert_eq!(first["ops"].as_array().unwrap().len(), 3);
    }

    #[test]
    fn test_metrics_zero_counts() {
        let mut metrics = HashMap::new();
        metrics.insert(
            1,
            MetricInfo {
                reaction_count: 0,
                comment_count: 0,
                collected_count: 0,
            },
        );

        let ops = metrics_to_entity_ops(metrics);
        assert_eq!(ops.len(), 1);
        // Zero counts should still produce Set ops (correct cumulative value)
        assert_eq!(ops[0].ops.len(), 3);
        for op in &ops[0].ops {
            if let Op::Set { value, .. } = op {
                assert_eq!(value, &json!(0));
            }
        }
    }
}
