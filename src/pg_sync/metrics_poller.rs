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
#[derive(Clone, Copy, PartialEq, Eq)]
struct MetricInfo {
    reaction_count: i64,
    comment_count: i64,
    collected_count: i64,
}

/// Runtime configuration for the metrics poller. All fields are sourced from
/// `PgSyncConfig` (TOML + env) so the table, cadence, and window widths are
/// tunable without a rebuild — see `docs/design/metrics-poller-overlap-window.md`.
pub struct MetricsPollerConfig {
    /// ClickHouse table scanned for recent metric activity (the complete CDC
    /// stream, e.g. `entityMetricEvents_month`).
    pub table: String,
    /// ClickHouse table/view read for all-time cumulative totals (argMax daily
    /// agg, e.g. `entityMetricDailyAgg_v2`).
    pub totals_table: String,
    /// `entityType` filtered in discovery + totals.
    pub entity_type: String,
    /// Persisted cursor name (BitDex `/cursors` key).
    pub cursor_name: String,
    /// Reconcile cadence in seconds.
    pub poll_interval_secs: u64,
    /// Steady-state trailing discovery window width in seconds.
    pub reconcile_window_secs: u64,
    /// Bounded forward chunk width in seconds used while catching up.
    pub backfill_chunk_secs: u64,
    /// `metricType` values summed into `reactionCount` / `commentCount` /
    /// `collectedCount`. Config-driven so an upstream vocab rename is a config
    /// edit, not a rebuild.
    pub reaction_types: Vec<String>,
    pub comment_types: Vec<String>,
    pub collected_types: Vec<String>,
    /// Hard cap on suppression-cache entries (memory safety valve).
    pub suppression_max_entries: usize,
}

/// Bounded per-id suppression cache. Holds the last-emitted `(reaction, comment,
/// collected)` tuple per entityId so a reconcile only pushes ops for ids whose
/// totals actually changed. Entries not re-seen within `retain_cycles` reconcile
/// cycles are swept, bounding memory to roughly the active working set.
///
/// Because the steady trailing window (>> settle lag) re-discovers each active id
/// every cycle, a not-yet-settled total is never permanently silenced: the id is
/// re-read on later cycles and emitted once its settled value differs.
struct SuppressionCache {
    map: HashMap<i64, (MetricInfo, u64)>,
    retain_cycles: u64,
    max_entries: usize,
}

impl SuppressionCache {
    fn new(retain_cycles: u64, max_entries: usize) -> Self {
        Self {
            map: HashMap::new(),
            retain_cycles: retain_cycles.max(1),
            max_entries: max_entries.max(1),
        }
    }

    /// Whether emitting `id` with `info` would change what was last sent (or it
    /// is newly seen). Pure read — does NOT mutate. The tuple is only committed
    /// via `record` after the ops POST succeeds, so a failed POST leaves the
    /// cache untouched and the update is re-sent next cycle.
    fn peek_changed(&self, id: i64, info: MetricInfo) -> bool {
        match self.map.get(&id) {
            Some((prev, _)) => *prev != info,
            None => true,
        }
    }

    /// Commit the last-sent tuple + last-seen cycle for `id`. Called for every
    /// discovered id (changed or not) after a successful POST, so unchanged
    /// in-window ids keep their last-seen fresh and survive the sweep.
    fn record(&mut self, id: i64, info: MetricInfo, cycle: u64) {
        self.map.insert(id, (info, cycle));
    }

    /// Drop entries not seen in the last `retain_cycles` cycles, then enforce the
    /// hard cap as a last-resort memory valve (clear on overflow — a one-time
    /// re-emit that suppression collapses again next cycle).
    fn sweep(&mut self, cycle: u64) {
        let cutoff = cycle.saturating_sub(self.retain_cycles);
        self.map.retain(|_, (_, seen)| *seen >= cutoff);
        if self.map.len() > self.max_entries {
            eprintln!(
                "Metrics: suppression cache exceeded {} entries — clearing (one-time re-emit)",
                self.max_entries
            );
            self.map.clear();
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.map.len()
    }
}

/// Run the ClickHouse metrics poller loop. Runs forever until cancelled.
///
/// V2 pipeline: fetches aggregate counts from ClickHouse, converts them to
/// `Op::Set` ops for sort fields, and POSTs via the `/ops` endpoint.
/// No PG round-trip needed — metrics are self-contained sort-field updates.
/// The cursor name (BitDex `/cursors` key) is config-driven — see
/// `MetricsPollerConfig::cursor_name`. Its value is the unix-epoch upper bound
/// of the most recently *fully successful* reconcile cycle.
///
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

/// Decide the `[since, upper]` createdAt window for a reconcile cycle.
///
/// - **Backfill** (cursor more than one reconcile window behind — fresh boot with
///   an old `BITDEX_METRICS_SINCE`, or long downtime): walk forward in a bounded,
///   non-overlapping chunk. Historical event partitions are immutable, so there is
///   no batch-flush jitter to guard against and no overlap is needed.
/// - **Steady** (caught up): an overlapping trailing window of width
///   `reconcile_window`. The overlap absorbs bursty batch-ingestion lag and the
///   totals settle interval.
///
/// Backfill advances the cursor until it lands within the trailing window, so the
/// final backfill chunk and the first steady window overlap — no seam gap.
///
/// Saturating arithmetic throughout so a corrupt/absurd persisted cursor can't
/// overflow (which would panic in debug and wrap in release).
fn compute_window(
    cursor: i64,
    now: i64,
    reconcile_window_secs: i64,
    backfill_chunk_secs: i64,
) -> (i64, i64) {
    if now.saturating_sub(cursor) > reconcile_window_secs {
        (cursor, cursor.saturating_add(backfill_chunk_secs).min(now))
    } else {
        (now.saturating_sub(reconcile_window_secs), now)
    }
}

pub async fn run_metrics_poller(
    ch_config: &ClickHouseConfig,
    bitdex_client: &BitdexClient,
    cfg: &MetricsPollerConfig,
) -> Result<(), String> {
    let mut ticker = interval(Duration::from_secs(cfg.poll_interval_secs.max(1)));
    let http = Client::new();

    // Resolve the starting cursor from the persisted cursor (with optional env
    // override on cold start). Fails CLOSED on transient API errors so a restart
    // during a BitDex outage doesn't silently skip the backlog.
    let mut cursor =
        match resolve_initial_since(bitdex_client, &cfg.cursor_name, cfg.reconcile_window_secs)
            .await
        {
            Ok(ts) => ts,
            Err(e) => return Err(format!("Metrics: refusing to start: {e}")),
        };

    // Retain suppression state for an id ~2 reconcile windows past its last
    // sighting — comfortably longer than the window that keeps re-discovering it.
    let retain_cycles =
        (cfg.reconcile_window_secs / cfg.poll_interval_secs.max(1)).saturating_mul(2) + 4;
    let mut suppression = SuppressionCache::new(retain_cycles, cfg.suppression_max_entries);
    let mut cycle_num: u64 = 0;

    let mut bitdex_was_down = false;

    eprintln!(
        "Metrics poller started (ClickHouse={}, table={}, entity_type={}, interval={}s, \
         reconcile_window={}s, backfill_chunk={}s, cursor={cursor})",
        ch_config.url,
        cfg.table,
        cfg.entity_type,
        cfg.poll_interval_secs,
        cfg.reconcile_window_secs,
        cfg.backfill_chunk_secs,
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

        // Clock-regression guard: if wall-clock stepped back below the cursor
        // (NTP correction / container skew), don't let the cursor walk backwards
        // and churn. Wait for the clock to catch up rather than re-scan.
        if now < cursor {
            eprintln!(
                "Metrics: clock regression (now={now} < cursor={cursor}); waiting for catch-up"
            );
            continue;
        }

        let (since, upper) = compute_window(
            cursor,
            now,
            cfg.reconcile_window_secs as i64,
            cfg.backfill_chunk_secs as i64,
        );

        // Nothing to scan (e.g. zero-width window from a boundary) — just wait.
        if upper <= since {
            continue;
        }

        cycle_num = cycle_num.wrapping_add(1);
        let backfilling = now - cursor > cfg.reconcile_window_secs as i64;

        let cycle_start = Instant::now();
        match poll_metrics_and_push(
            &http,
            ch_config,
            cfg,
            bitdex_client,
            since,
            upper,
            &mut suppression,
            cycle_num,
        )
        .await
        {
            Ok(Reconcile { discovered, emitted }) => {
                let cycle_secs = cycle_start.elapsed().as_secs_f64();
                // Report timing + work to BitDex. `rows_fetched` = ops actually
                // emitted (post-suppression), so the Grafana rate reflects real
                // update throughput. Reuses `replica="clickhouse-metrics"`.
                bitdex_client
                    .report_pgsync_metrics(
                        "clickhouse-metrics",
                        cycle_secs,
                        emitted as u64,
                        upper,
                    )
                    .await;
                if discovered > 0 {
                    eprintln!(
                        "Metrics: {} — discovered {discovered}, emitted {emitted} in {:.1}ms \
                         (window {since}..{upper}]",
                        if backfilling { "backfill" } else { "reconcile" },
                        cycle_secs * 1000.0,
                    );
                }
                // Persist BEFORE advancing in-memory state. If persist fails we
                // keep the old cursor and re-scan next cycle (Set ops are
                // idempotent, and suppression collapses the repeat to a no-op).
                if let Err(e) = bitdex_client
                    .set_cursor(&cfg.cursor_name, &upper.to_string())
                    .await
                {
                    eprintln!("Metrics: cursor persist failed ({e}); will retry next cycle");
                    continue;
                }
                cursor = upper;
                suppression.sweep(cycle_num);
            }
            Err(e) => {
                let cycle_secs = cycle_start.elapsed().as_secs_f64();
                bitdex_client
                    .report_pgsync_metrics("clickhouse-metrics", cycle_secs, 0, upper)
                    .await;
                eprintln!("Metrics poll error after {:.1}ms: {e}", cycle_secs * 1000.0);
                // Don't advance — retry the same window next cycle.
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
/// - If no cursor and no env, fall back to `now - reconcile_window` so the first
///   cycle runs as an ordinary steady trailing window.
/// - On any *transient* API error fetching the cursor, returns Err so the
///   caller can fail closed instead of silently skipping the backlog.
/// - On *unparseable* persisted cursor, returns Err for the same reason —
///   silent fallback would convert state corruption into permanent data loss.
async fn resolve_initial_since(
    bitdex_client: &BitdexClient,
    cursor_name: &str,
    reconcile_window_secs: u64,
) -> Result<i64, String> {
    // Try persisted cursor first (must distinguish 404 → None from transient error → Err).
    match bitdex_client.get_cursor(cursor_name).await? {
        Some(s) => match s.trim().parse::<i64>() {
            Ok(ts) if ts > 0 => {
                eprintln!("Metrics: resumed from persisted cursor (since_ts={ts})");
                Ok(ts)
            }
            _ => Err(format!(
                "persisted cursor {cursor_name}={s:?} is unparseable. \
                 Recovery: PUT a valid unix-epoch integer to /cursors/{cursor_name} \
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
            // Cold start fallback: seed the cursor one reconcile window back so
            // the first cycle runs as a normal steady trailing window rather than
            // triggering a spurious backfill.
            let ts = current_epoch_secs() - reconcile_window_secs as i64;
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

/// Outcome of one reconcile cycle.
struct Reconcile {
    /// Distinct entityIds discovered in the window.
    discovered: usize,
    /// Entity ops actually emitted after suppression (changed totals only).
    emitted: usize,
}

/// Single reconcile cycle. Discovers ids with metric activity in `(since_ts,
/// upper_ts]`, fetches their all-time totals, and POSTs `Op::Set` ops for the
/// ids whose totals changed since last emission (per `suppression`).
///
/// The window overlaps the previous cycle's (steady mode), so an id discovered
/// here may have been emitted before — suppression collapses that to a no-op.
///
/// Suppression is committed ONLY after the POSTs succeed: `peek_changed` (pure)
/// selects the emit set, and `record` runs afterwards. So a failed POST leaves
/// the cache untouched — the caller keeps the old cursor and the same window is
/// re-scanned and re-sent next cycle (no silently-dropped update).
async fn poll_metrics_and_push(
    http: &Client,
    ch_config: &ClickHouseConfig,
    cfg: &MetricsPollerConfig,
    bitdex_client: &BitdexClient,
    since_ts: i64,
    upper_ts: i64,
    suppression: &mut SuppressionCache,
    cycle: u64,
) -> Result<Reconcile, String> {
    let metrics = fetch_metrics_from_clickhouse(http, ch_config, cfg, since_ts, upper_ts).await?;

    let discovered = metrics.len();
    if discovered == 0 {
        return Ok(Reconcile { discovered: 0, emitted: 0 });
    }

    // Select ids whose totals changed since last emission — pure peek, no commit.
    let changed: Vec<(i64, MetricInfo)> = metrics
        .iter()
        .filter(|(id, info)| suppression.peek_changed(**id, **info))
        .map(|(id, info)| (*id, *info))
        .collect();

    let emitted = changed.len();

    if emitted > 0 {
        let entity_ops = metrics_to_entity_ops(changed.iter().copied());
        // Send in batches to keep request sizes manageable. A failure here
        // returns Err BEFORE any `record` below, so nothing is suppressed.
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
    }

    // Commit only now that the POSTs (if any) succeeded. Record EVERY discovered
    // id — including unchanged ones — so their last-seen cycle stays fresh and
    // they survive the sweep while still in the trailing window.
    for (id, info) in &metrics {
        suppression.record(*id, *info, cycle);
    }

    Ok(Reconcile { discovered, emitted })
}

/// Validate a config-supplied SQL identifier or string-literal value (table,
/// entityType, metricType). These come from operator config, not user input, but
/// a strict allowlist keeps a typo'd config from producing a malformed query and
/// blocks quote/paren/semicolon injection into the interpolated SQL. A single `.`
/// is allowed so `db.table`-qualified names work.
fn validate_ident(kind: &str, s: &str) -> Result<(), String> {
    if s.is_empty()
        || !s
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
    {
        return Err(format!(
            "invalid metrics {kind} {s:?}: must be non-empty ASCII alphanumeric/underscore/dot"
        ));
    }
    Ok(())
}

/// Build a `metricType IN ('a','b',...)` predicate from a config-supplied set of
/// metric-type values, validating each so nothing breaks out of the quotes.
fn metric_type_in_clause(kind: &str, types: &[String]) -> Result<String, String> {
    if types.is_empty() {
        return Err(format!("metrics {kind} must list at least one metricType"));
    }
    let mut quoted = Vec::with_capacity(types.len());
    for t in types {
        validate_ident(kind, t)?;
        quoted.push(format!("'{t}'"));
    }
    Ok(format!("metricType IN ({})", quoted.join(",")))
}

/// Build the two-phase reconcile query.
///
/// 1. Discovery: `{table}` (the complete CDC event stream) — distinct entityIds
///    with `createdAt` in `(since_ts, upper_ts]` (half-open lower, inclusive upper).
/// 2. Totals: `{totals_table}` (argMax daily-agg view) — ALL-TIME cumulative
///    counts for those ids, so the emitted values are absolute (idempotent Set),
///    which is what makes overlapping windows and re-scans safe.
///
/// The `metricType` groups (which CH types sum into reaction/comment/collected)
/// are config-driven, so an upstream vocab rename is a config edit, not a rebuild.
/// Discovery does NOT filter by metricType.
fn build_reconcile_query(
    cfg: &MetricsPollerConfig,
    since_ts: i64,
    upper_ts: i64,
) -> Result<String, String> {
    validate_ident("table", &cfg.table)?;
    validate_ident("totals_table", &cfg.totals_table)?;
    validate_ident("entity_type", &cfg.entity_type)?;
    let reaction_in = metric_type_in_clause("reaction_types", &cfg.reaction_types)?;
    let comment_in = metric_type_in_clause("comment_types", &cfg.comment_types)?;
    let collected_in = metric_type_in_clause("collected_types", &cfg.collected_types)?;
    Ok(format!(
        r#"SELECT
            entityId as id,
            sumIf(total, {reaction_in}) as reactionCount,
            sumIf(total, {comment_in}) as commentCount,
            sumIf(total, {collected_in}) as collectedCount
        FROM {totals}
        WHERE entityType = '{etype}'
          AND entityId IN (
            SELECT DISTINCT entityId
            FROM {table}
            WHERE entityType = '{etype}'
              AND createdAt >  fromUnixTimestamp({since_ts})
              AND createdAt <= fromUnixTimestamp({upper_ts})
          )
        GROUP BY entityId
        FORMAT JSONEachRow"#,
        totals = cfg.totals_table,
        etype = cfg.entity_type,
        table = cfg.table,
    ))
}

/// Query ClickHouse HTTP interface for aggregate metrics for ids active in the
/// given window. See `build_reconcile_query` for the two-phase shape.
async fn fetch_metrics_from_clickhouse(
    http: &Client,
    ch_config: &ClickHouseConfig,
    cfg: &MetricsPollerConfig,
    since_ts: i64,
    upper_ts: i64,
) -> Result<HashMap<i64, MetricInfo>, String> {
    let query = build_reconcile_query(cfg, since_ts, upper_ts)?;

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
fn metrics_to_entity_ops(
    metrics: impl IntoIterator<Item = (i64, MetricInfo)>,
) -> Vec<EntityOps> {
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

    fn test_cfg() -> MetricsPollerConfig {
        MetricsPollerConfig {
            table: "entityMetricEvents_month".into(),
            totals_table: "entityMetricDailyAgg_v2".into(),
            entity_type: "Image".into(),
            cursor_name: "metrics-poller-civitai".into(),
            poll_interval_secs: 30,
            reconcile_window_secs: 1200,
            backfill_chunk_secs: 3600,
            reaction_types: ["Like", "Heart", "Laugh", "Cry"].iter().map(|s| s.to_string()).collect(),
            comment_types: vec!["commentCount".into()],
            collected_types: vec!["Collection".into()],
            suppression_max_entries: 5_000_000,
        }
    }

    fn info(r: i64, c: i64, col: i64) -> MetricInfo {
        MetricInfo { reaction_count: r, comment_count: c, collected_count: col }
    }

    #[test]
    fn window_steady_when_caught_up() {
        // Cursor within the reconcile window -> trailing overlapping window.
        let now = 1_000_000;
        let (since, upper) = compute_window(now - 100, now, 1200, 3600);
        assert_eq!(upper, now);
        assert_eq!(since, now - 1200, "steady window trails a full reconcile width");
        // Overlaps prior cursor (now-100) -> no gap.
        assert!(since < now - 100);
    }

    #[test]
    fn window_backfill_walks_forward_in_bounded_chunks() {
        // Cursor far behind (2 days) -> forward chunk of backfill_chunk width.
        let now = 1_000_000;
        let cursor = now - 2 * 24 * 3600;
        let (since, upper) = compute_window(cursor, now, 1200, 3600);
        assert_eq!(since, cursor, "backfill resumes exactly at the cursor");
        assert_eq!(upper, cursor + 3600, "backfill advances one chunk, not to now");
    }

    #[test]
    fn window_backfill_final_chunk_clamps_to_now_and_overlaps_steady() {
        let now = 1_000_000;
        // Cursor 1.5x window behind: still backfill, but chunk clamps to now.
        let cursor = now - 1800;
        let (since, upper) = compute_window(cursor, now, 1200, 3600);
        assert_eq!(since, cursor);
        assert_eq!(upper, now, "final catch-up chunk clamps to now");
        // Next cycle cursor == now -> steady window starts at now-1200 <= cursor,
        // so the handoff overlaps (no seam gap).
        let (next_since, _) = compute_window(now, now + 30, 1200, 3600);
        assert!(next_since <= upper, "steady window overlaps last backfill chunk");
    }

    #[test]
    fn suppression_emits_on_change_skips_on_repeat() {
        let mut s = SuppressionCache::new(4, 1000);
        // First sighting -> emit; commit it.
        assert!(s.peek_changed(1, info(10, 0, 0)));
        s.record(1, info(10, 0, 0), 1);
        // Same total next cycle -> suppressed.
        assert!(!s.peek_changed(1, info(10, 0, 0)));
        // Total changes (e.g. settled value) -> emit again.
        assert!(s.peek_changed(1, info(11, 0, 0)));
        // New id always emits.
        assert!(s.peek_changed(2, info(0, 0, 0)));
    }

    #[test]
    fn suppression_peek_is_pure_until_recorded() {
        // BLOCKER regression: peek must NOT commit, so a failed POST (no record)
        // leaves the id re-emittable next cycle.
        let mut s = SuppressionCache::new(4, 1000);
        assert!(s.peek_changed(1, info(10, 0, 0)));
        assert!(s.peek_changed(1, info(10, 0, 0)), "peek alone must not suppress");
        // Only after record does it suppress.
        s.record(1, info(10, 0, 0), 1);
        assert!(!s.peek_changed(1, info(10, 0, 0)));
    }

    #[test]
    fn suppression_does_not_silence_unsettled_value() {
        // Discover with pre-settle total, emit; later the settled value differs
        // and must still be emitted (the settle-lag trap).
        let mut s = SuppressionCache::new(100, 1000);
        assert!(s.peek_changed(7, info(5, 0, 0))); // pre-settle
        s.record(7, info(5, 0, 0), 1);
        assert!(!s.peek_changed(7, info(5, 0, 0))); // unchanged re-read
        assert!(s.peek_changed(7, info(5, 3, 0))); // comment count settled -> emit
    }

    #[test]
    fn suppression_sweep_evicts_stale_ids() {
        let mut s = SuppressionCache::new(2, 1000);
        s.record(1, info(1, 0, 0), 1);
        s.record(2, info(1, 0, 0), 5);
        // Sweep at cycle 5 with retain=2 -> cutoff 3; id 1 (seen@1) evicted.
        s.sweep(5);
        assert_eq!(s.len(), 1);
        // Evicted id re-emits when seen again.
        assert!(s.peek_changed(1, info(1, 0, 0)));
    }

    #[test]
    fn suppression_hard_cap_clears_on_overflow() {
        let mut s = SuppressionCache::new(1_000_000, 3);
        for id in 0..10 {
            s.record(id, info(id, 0, 0), 1);
        }
        // retain is huge so the cycle-sweep keeps all 10; the hard cap (3) trips.
        s.sweep(1);
        assert_eq!(s.len(), 0, "cache cleared once it blew past max_entries");
    }

    #[test]
    fn reconcile_query_uses_config_tables_and_no_notnull() {
        let q = build_reconcile_query(&test_cfg(), 100, 200).unwrap();
        assert!(q.contains("FROM entityMetricDailyAgg_v2"), "totals table from config");
        assert!(q.contains("FROM entityMetricEvents_month"), "discovery table from config");
        assert!(q.contains("entityType = 'Image'"));
        assert!(q.contains("fromUnixTimestamp(100)"));
        assert!(q.contains("fromUnixTimestamp(200)"));
        assert!(!q.contains("IS NOT NULL"), "_month.entityId is non-nullable");
    }

    #[test]
    fn reconcile_query_rejects_bad_identifiers() {
        let mut cfg = test_cfg();
        cfg.table = "events; DROP TABLE x".into();
        assert!(build_reconcile_query(&cfg, 0, 1).is_err());
        let mut cfg2 = test_cfg();
        cfg2.entity_type = "".into();
        assert!(build_reconcile_query(&cfg2, 0, 1).is_err());
        // Injection attempt in a metricType value is rejected (would break quotes).
        let mut cfg3 = test_cfg();
        cfg3.reaction_types = vec!["Like') OR 1=1 --".into()];
        assert!(build_reconcile_query(&cfg3, 0, 1).is_err());
        // Empty metricType group is rejected.
        let mut cfg4 = test_cfg();
        cfg4.comment_types = vec![];
        assert!(build_reconcile_query(&cfg4, 0, 1).is_err());
    }

    #[test]
    fn reconcile_query_uses_config_vocab() {
        // A vocab rename is a config edit, not a rebuild.
        let mut cfg = test_cfg();
        cfg.reaction_types = vec!["ReactionLike".into(), "ReactionHeart".into()];
        let q = build_reconcile_query(&cfg, 0, 1).unwrap();
        assert!(q.contains("metricType IN ('ReactionLike','ReactionHeart')"));
        assert!(q.contains("metricType IN ('commentCount')"), "comment group still one value");
    }

    #[test]
    fn db_qualified_table_name_is_allowed() {
        let mut cfg = test_cfg();
        cfg.table = "default.entityMetricEvents_month".into();
        let q = build_reconcile_query(&cfg, 0, 1).unwrap();
        assert!(q.contains("FROM default.entityMetricEvents_month"));
    }
}
