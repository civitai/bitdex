//! ClickHouse metrics poller: polls for recent metric events, fetches aggregate
//! counts, and pushes sort-field ops to BitDex via the V2 ops pipeline.
//!
//! ClickHouse is queried via its HTTP interface (POST with SQL).
//! Metrics (reactionCount, commentCount, collectedCount) are sort-only fields,
//! so ops are sent with `creates_slot: false` — they update existing slots
//! without touching the alive bitmap.

use ahash::AHashMap as HashMap;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

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
/// Cursor name used to persist the metrics poller's high-water-mark on the
/// BitDex side via the `/cursors` API. The value is the unix-epoch upper
/// bound of the most recently *fully successful* poll cycle.
const METRICS_CURSOR_NAME: &str = "metrics-poller-civitai";

/// Optional env-var override read once at startup, ONLY honored on cold start
/// (no persisted cursor exists). If set to a unix-epoch integer, the poller
/// seeds the cold-start cursor from this value. Restarts after the first
/// successful cycle ignore the env var entirely — the persisted cursor wins,
/// so a leftover env var can't trigger a redundant backfill on every reboot.
///
/// Operational note: to *force* a manual backfill on a system with an existing
/// cursor, PUT a smaller timestamp directly via the BitDex `/cursors/<name>`
/// API. The env var is intentionally narrow.
const METRICS_SINCE_ENV: &str = "BITDEX_METRICS_SINCE";

/// Safety lag (seconds) subtracted from `now()` when computing the upper
/// bound of each poll window. Absorbs ClickHouse ingestion latency: events
/// generated at wall-clock T might not be visible in `entityMetricEvents`
/// until T+lag. Without this, an event generated immediately before
/// `cycle_upper_ts = now()` would be missed and never picked up by a future
/// cycle (because future cycles use `createdAt > upper_ts`).
///
/// 60s is conservative for our CH ingestion path. Tune via observation if
/// the gap proves too large or too small.
const INGESTION_SAFETY_LAG_SECS: i64 = 60;

pub async fn run_metrics_poller(
    ch_config: &ClickHouseConfig,
    bitdex_client: &BitdexClient,
    poll_interval_secs: u64,
) -> Result<(), String> {
    let mut ticker = interval(Duration::from_secs(poll_interval_secs));
    let http = Client::new();

    // Resolve starting `since_ts` from persisted cursor (with optional env
    // override on cold start). Fails CLOSED on transient API errors so a
    // restart during a BitDex outage doesn't silently skip the backlog.
    let mut last_poll_ts = match resolve_initial_since(bitdex_client, poll_interval_secs).await {
        Ok(ts) => ts,
        Err(e) => return Err(format!("Metrics: refusing to start: {e}")),
    };

    let mut bitdex_was_down = false;

    eprintln!(
        "Metrics poller started (ClickHouse={}, interval={poll_interval_secs}s, \
         since_ts={last_poll_ts}, ingestion_lag={INGESTION_SAFETY_LAG_SECS}s)",
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

        // Pin the upper bound of this cycle's window AT THE START minus a
        // safety lag for CH ingestion delay. The query uses a half-open
        // interval (since_ts, cycle_upper_ts], guaranteeing no gap and no
        // overlap between consecutive cycles. Events landing in CH after
        // `cycle_upper_ts` are the next cycle's responsibility.
        let cycle_upper_ts = current_epoch_secs() - INGESTION_SAFETY_LAG_SECS;

        // Monotonic guard: never process a cycle whose upper bound is at or
        // below the current cursor. Protects against system clock regressions
        // (NTP corrections, container clock skew) which would otherwise cause
        // the cursor to walk backwards and trigger huge replay churn.
        if cycle_upper_ts <= last_poll_ts {
            eprintln!(
                "Metrics: skipping cycle — upper_ts={cycle_upper_ts} is not ahead of \
                 last_poll_ts={last_poll_ts} (clock skew or interval shorter than lag?)"
            );
            continue;
        }

        let cycle_start = Instant::now();
        match poll_metrics_and_push(
            &http,
            ch_config,
            bitdex_client,
            last_poll_ts,
            cycle_upper_ts,
        )
        .await
        {
            Ok(count) => {
                let cycle_secs = cycle_start.elapsed().as_secs_f64();
                // Report timing to BitDex for `bitdex_pgsync_cycle_seconds`
                // histogram. Reused with `replica="clickhouse-metrics"` label so
                // the existing `pgsync_cycle_seconds` metric covers both the
                // ops poller (replica="default") and this CH metrics poller.
                bitdex_client
                    .report_pgsync_metrics(
                        "clickhouse-metrics",
                        cycle_secs,
                        count as u64,
                        cycle_upper_ts,
                    )
                    .await;
                if count > 0 {
                    eprintln!(
                        "Metrics: pushed {count} ops batches in {:.1}ms (window {}..{}]",
                        cycle_secs * 1000.0,
                        last_poll_ts,
                        cycle_upper_ts
                    );
                }
                // Persist BEFORE advancing in-memory state. If persist fails
                // we keep the old `last_poll_ts` and re-poll the same window
                // next cycle (Set ops are idempotent — no double-counting).
                if let Err(e) = bitdex_client
                    .set_cursor(METRICS_CURSOR_NAME, &cycle_upper_ts.to_string())
                    .await
                {
                    eprintln!("Metrics: cursor persist failed ({e}); will retry same window next cycle");
                    continue;
                }
                last_poll_ts = cycle_upper_ts;
            }
            Err(e) => {
                let cycle_secs = cycle_start.elapsed().as_secs_f64();
                bitdex_client
                    .report_pgsync_metrics(
                        "clickhouse-metrics",
                        cycle_secs,
                        0,
                        cycle_upper_ts,
                    )
                    .await;
                eprintln!("Metrics poll error after {:.1}ms: {e}", cycle_secs * 1000.0);
                // Don't advance — retry same window next cycle.
            }
        }
    }
}

/// Resolve the starting `since_ts` for the poller loop.
///
/// Behavior:
/// - If a persisted cursor exists and parses, use it. Always.
/// - If no cursor exists (true cold start) and `BITDEX_METRICS_SINCE` is set,
///   use the env value. (Lets operators bootstrap with a custom backfill
///   horizon without an extra `/cursors` PUT.)
/// - If no cursor and no env, fall back to `now - poll_interval`.
/// - On any *transient* API error fetching the cursor, returns Err so the
///   caller can fail closed instead of silently skipping the backlog.
/// - On *unparseable* persisted cursor, returns Err for the same reason —
///   silent fallback would convert state corruption into permanent data loss.
async fn resolve_initial_since(
    bitdex_client: &BitdexClient,
    poll_interval_secs: u64,
) -> Result<i64, String> {
    // Try persisted cursor first (must distinguish 404 → None from transient error → Err).
    match bitdex_client.get_cursor(METRICS_CURSOR_NAME).await? {
        Some(s) => match s.trim().parse::<i64>() {
            Ok(ts) if ts > 0 => {
                eprintln!("Metrics: resumed from persisted cursor (since_ts={ts})");
                Ok(ts)
            }
            _ => Err(format!(
                "persisted cursor {METRICS_CURSOR_NAME}={s:?} is unparseable. \
                 Recovery: PUT a valid unix-epoch integer to /cursors/{METRICS_CURSOR_NAME} \
                 (or DELETE it to force a cold start). Refusing to silently restart \
                 from now-interval — that would discard the backlog."
            )),
        },
        None => {
            // Cold start — env var override is honored only here.
            if let Ok(s) = std::env::var(METRICS_SINCE_ENV) {
                // Hard-fail on a malformed env var: the operator clearly meant
                // to override, and silently falling back to "now - interval"
                // would discard the backlog they were trying to recover.
                let ts: i64 = s.trim().parse().map_err(|e| {
                    format!(
                        "{METRICS_SINCE_ENV}={s:?} is not a valid unix-epoch integer ({e}). \
                         Either fix or unset the env var."
                    )
                })?;
                if ts <= 0 {
                    return Err(format!(
                        "{METRICS_SINCE_ENV}={ts} must be > 0. Either fix or unset the env var."
                    ));
                }
                eprintln!("Metrics: cold start, {METRICS_SINCE_ENV}={ts} override active");
                return Ok(ts);
            }
            // Cold start fallback: subtract BOTH the poll interval AND the
            // ingestion safety lag so the first cycle's `cycle_upper_ts`
            // (which is `now - lag`) is strictly greater than `last_poll_ts`,
            // and the first window covers any events in the lag tail. Without
            // this, when `lag > poll_interval` the monotonic guard would
            // skip cycles until `last_poll_ts` was overtaken — and the first
            // real window would silently exclude lag-delayed events.
            let ts = current_epoch_secs()
                - poll_interval_secs as i64
                - INGESTION_SAFETY_LAG_SECS;
            eprintln!("Metrics: cold start, no env override (since_ts={ts})");
            Ok(ts)
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
///
/// Uses a half-open window `(since_ts, upper_ts]` so consecutive cycles cover
/// the time line exactly once each. The upper bound is fixed at the start of
/// the cycle by the caller — events landing in CH after that timestamp are
/// the next cycle's responsibility.
async fn poll_metrics_and_push(
    http: &Client,
    ch_config: &ClickHouseConfig,
    bitdex_client: &BitdexClient,
    since_ts: i64,
    upper_ts: i64,
) -> Result<usize, String> {
    let metrics = fetch_metrics_from_clickhouse(http, ch_config, since_ts, upper_ts).await?;

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
    upper_ts: i64,
) -> Result<HashMap<i64, MetricInfo>, String> {
    // Phase 1: discover IDs with metric events in the closed window
    //          (since_ts, upper_ts] — half-open lower, inclusive upper.
    // Phase 2: get their all-time totals from the daily aggregate table.
    //
    // The bounded upper interval is the key correctness property: any event
    // landing in entityMetricEvents AFTER upper_ts is left for the next cycle,
    // whose `since_ts` will equal this cycle's `upper_ts`. No gap, no overlap.
    let query = format!(
        r#"SELECT
            entityId as id,
            sumIf(total, metricType IN ('Like','Heart','Laugh','Cry')) as reactionCount,
            sumIf(total, metricType = 'commentCount') as commentCount,
            sumIf(total, metricType = 'Collection') as collectedCount
        FROM entityMetricDailyAgg_v2
        WHERE entityType = 'Image'
          AND entityId IN (
            SELECT DISTINCT entityId
            FROM entityMetricEvents
            WHERE entityType = 'Image'
              AND entityId IS NOT NULL
              AND createdAt >  fromUnixTimestamp({since_ts})
              AND createdAt <= fromUnixTimestamp({upper_ts})
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
