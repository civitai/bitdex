//! Query trace collection with in-memory ring buffer.
//!
//! `QueryTraceCollector` is threaded through query execution to record per-clause
//! timing, cardinalities, and sort metrics. Traces are stored in a bounded in-memory
//! ring buffer (`TraceBuffer`) — no disk I/O. Retrieved via the `/traces` endpoint.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Instant, SystemTime};

use serde::{Deserialize, Serialize};

use crate::query::FilterClause;

// ---------------------------------------------------------------------------
// Trace structs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryTrace {
    pub ts: String,
    pub index: String,
    pub total_us: u64,
    pub plan_us: u64,
    pub filter_us: u64,
    pub sort_us: u64,
    pub result_count: u64,
    pub cache_hit: bool,
    pub lazy_load_us: u64,
    pub docs_us: u64,
    pub docs_count: u64,
    /// Cumulative time spent waiting to acquire `Mutex<UnifiedCache>` across
    /// all `unified_cache.lock()` calls in the query path. Trace data on
    /// v1.0.183 showed ~700-1000 ms of total query time unattributed to any
    /// existing sub-span — this counter exists to prove the cache mutex is
    /// where that time is going (cache_worker Phase A holds the lock during
    /// `remove_slots_from_all_batch` + `collect_filter_work` over up to 64 K
    /// cache entries, blocking every concurrent reader).
    #[serde(default)]
    pub cache_lock_us: u64,
    pub clauses: Vec<ClauseTrace>,
    pub sort: Option<SortTrace>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClauseTrace {
    pub ord: usize,
    pub field: String,
    pub op: String,
    pub values: String,
    pub card: u64,
    pub acc_before: u64,
    pub acc_after: u64,
    pub eval_us: u64,
    pub and_us: u64,
    pub mode: String,
    /// Range-scan specific telemetry. `None` for non-range clauses.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub range_scan: Option<RangeScanTrace>,
}

/// Per-clause telemetry emitted when a range predicate is evaluated.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeScanTrace {
    /// Number of distinct values scanned in the field's bitmap map.
    pub values_scanned: usize,
    /// Number of per-value bitmaps OR'd into the result.
    pub bitmaps_ored: usize,
    /// Wall-clock time for the full scan, in microseconds.
    pub scan_us: u64,
    /// Whether a time-bucket snap was used instead of a raw range scan.
    pub snap_used: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortTrace {
    pub field: String,
    pub dir: String,
    pub input: u64,
    pub output: u64,
    pub time_us: u64,
}

// ---------------------------------------------------------------------------
// Collector
// ---------------------------------------------------------------------------

pub struct QueryTraceCollector {
    start: Instant,
    pub plan_us: u64,
    pub lazy_load_us: u64,
    pub filter_us: u64,
    pub sort_us: u64,
    pub cache_hit: bool,
    pub cache_lock_us: u64,
    pub clauses: Vec<ClauseTrace>,
    pub sort: Option<SortTrace>,
}

impl QueryTraceCollector {
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
            plan_us: 0,
            lazy_load_us: 0,
            filter_us: 0,
            sort_us: 0,
            cache_hit: false,
            cache_lock_us: 0,
            clauses: Vec::new(),
            sort: None,
        }
    }

    /// Acquire `unified_cache.lock()` while charging the wait time to
    /// `cache_lock_us`. Wraps `parking_lot::Mutex::lock()` — caller still
    /// gets a normal lock guard.
    #[inline]
    pub fn timed_cache_lock<'a, T>(&mut self, m: &'a parking_lot::Mutex<T>) -> parking_lot::MutexGuard<'a, T> {
        let t0 = Instant::now();
        let g = m.lock();
        self.cache_lock_us = self.cache_lock_us.saturating_add(t0.elapsed().as_micros() as u64);
        g
    }

    pub fn record_clause(&mut self, trace: ClauseTrace) {
        self.clauses.push(trace);
    }

    pub fn record_sort(&mut self, trace: SortTrace) {
        self.sort = Some(trace);
    }

    pub fn finalize(self, index: &str, result_count: u64) -> QueryTrace {
        let total_us = self.start.elapsed().as_micros() as u64;
        let ts = iso_now();
        QueryTrace {
            ts,
            index: index.to_string(),
            total_us,
            plan_us: self.plan_us,
            filter_us: self.filter_us,
            sort_us: self.sort_us,
            result_count,
            cache_hit: self.cache_hit,
            lazy_load_us: self.lazy_load_us,
            docs_us: 0,
            docs_count: 0,
            cache_lock_us: self.cache_lock_us,
            clauses: self.clauses,
            sort: self.sort,
        }
    }
}

// ---------------------------------------------------------------------------
// In-memory ring buffer (bounded, no disk I/O)
// ---------------------------------------------------------------------------

const DEFAULT_CAPACITY: usize = 1000;

pub struct TraceBuffer {
    buf: Mutex<VecDeque<QueryTrace>>,
    capacity: AtomicUsize,
}

impl TraceBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            buf: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity: AtomicUsize::new(capacity),
        }
    }

    pub fn push(&self, trace: QueryTrace) {
        let cap = self.capacity.load(Ordering::Relaxed);
        let mut buf = self.buf.lock().unwrap();
        while buf.len() >= cap {
            buf.pop_front();
        }
        buf.push_back(trace);
    }

    pub fn last_n(&self, n: usize) -> Vec<QueryTrace> {
        let buf = self.buf.lock().unwrap();
        let skip = buf.len().saturating_sub(n);
        buf.iter().skip(skip).cloned().collect()
    }

    /// Resize the ring buffer capacity. If the new capacity is smaller than
    /// the current number of entries, the oldest entries are dropped.
    pub fn resize(&self, new_capacity: usize) {
        let mut buf = self.buf.lock().unwrap();
        // Drop oldest entries if shrinking
        while buf.len() > new_capacity {
            buf.pop_front();
        }
        self.capacity.store(new_capacity, Ordering::Relaxed);
    }

    /// Return the current capacity.
    pub fn capacity(&self) -> usize {
        self.capacity.load(Ordering::Relaxed)
    }
}

impl Default for TraceBuffer {
    fn default() -> Self {
        Self::new(DEFAULT_CAPACITY)
    }
}

// Keep legacy function signatures for compatibility during migration
// (delete these once all callers use TraceBuffer directly)

/// Read the last N traces — legacy adapter, reads from nothing now.
/// Use `TraceBuffer::last_n()` instead.
pub fn read_traces(_data_dir: &std::path::Path, _last: usize) -> Vec<QueryTrace> {
    Vec::new() // no-op — callers should use TraceBuffer
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn iso_now() -> String {
    let dur = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = dur.as_secs();
    let millis = dur.subsec_millis();
    // Manual ISO 8601 without chrono dependency
    let days_since_epoch = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    // Compute year/month/day from days since epoch (1970-01-01)
    let (year, month, day) = days_to_ymd(days_since_epoch);

    format!(
        "{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{seconds:02}.{millis:03}Z"
    )
}

fn days_to_ymd(mut days: u64) -> (u64, u64, u64) {
    // Algorithm from http://howardhinnant.github.io/date_algorithms.html
    days += 719468;
    let era = days / 146097;
    let doe = days - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

// ---------------------------------------------------------------------------
// Helper methods on FilterClause for trace extraction
// ---------------------------------------------------------------------------

impl FilterClause {
    /// Extract the field name from a filter clause, if applicable.
    pub fn field_name(&self) -> Option<&str> {
        match self {
            FilterClause::Eq(f, _)
            | FilterClause::NotEq(f, _)
            | FilterClause::In(f, _)
            | FilterClause::NotIn(f, _)
            | FilterClause::Gt(f, _)
            | FilterClause::Lt(f, _)
            | FilterClause::Gte(f, _)
            | FilterClause::Lte(f, _) => Some(f.as_str()),
            FilterClause::BucketBitmap { field, .. } => Some(field.as_str()),
            FilterClause::Not(inner) => inner.field_name(),
            FilterClause::And(_) => Some("AND"),
            FilterClause::Or(_) => Some("OR"),
            FilterClause::IsNull(f) | FilterClause::IsNotNull(f) => Some(f.as_str()),
        }
    }

    /// Return a short string describing the operator.
    pub fn op_name(&self) -> &'static str {
        match self {
            FilterClause::Eq(..) => "Eq",
            FilterClause::NotEq(..) => "NotEq",
            FilterClause::In(..) => "In",
            FilterClause::NotIn(..) => "NotIn",
            FilterClause::Gt(..) => "Gt",
            FilterClause::Lt(..) => "Lt",
            FilterClause::Gte(..) => "Gte",
            FilterClause::Lte(..) => "Lte",
            FilterClause::Not(..) => "Not",
            FilterClause::And(..) => "And",
            FilterClause::Or(..) => "Or",
            FilterClause::BucketBitmap { .. } => "BucketBitmap",
            FilterClause::IsNull(..) => "IsNull",
            FilterClause::IsNotNull(..) => "IsNotNull",
        }
    }

    /// Compact representation of the filter values.
    pub fn values_repr(&self) -> String {
        match self {
            FilterClause::Eq(_, v)
            | FilterClause::NotEq(_, v)
            | FilterClause::Gt(_, v)
            | FilterClause::Lt(_, v)
            | FilterClause::Gte(_, v)
            | FilterClause::Lte(_, v) => v.to_string(),
            FilterClause::In(_, vs) | FilterClause::NotIn(_, vs) => {
                if vs.len() <= 5 {
                    let strs: Vec<String> = vs.iter().map(|v| v.to_string()).collect();
                    format!("[{}]", strs.join(","))
                } else {
                    let strs: Vec<String> = vs.iter().take(3).map(|v| v.to_string()).collect();
                    format!("[{},...+{}]", strs.join(","), vs.len() - 3)
                }
            }
            FilterClause::Not(inner) => format!("NOT({})", inner.values_repr()),
            FilterClause::And(cs) => format!("{} clauses", cs.len()),
            FilterClause::Or(cs) => format!("{} clauses", cs.len()),
            FilterClause::BucketBitmap { bucket_name, .. } => bucket_name.clone(),
            FilterClause::IsNull(_) | FilterClause::IsNotNull(_) => String::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::Value;

    #[test]
    fn test_iso_now_format() {
        let ts = iso_now();
        // Should look like 2026-03-14T12:34:56.789Z
        assert!(ts.ends_with('Z'));
        assert_eq!(ts.len(), 24);
        assert_eq!(&ts[4..5], "-");
        assert_eq!(&ts[7..8], "-");
        assert_eq!(&ts[10..11], "T");
    }

    #[test]
    fn test_collector_finalize() {
        let mut collector = QueryTraceCollector::new();
        collector.record_clause(ClauseTrace {
            ord: 0,
            field: "nsfwLevel".into(),
            op: "In".into(),
            values: "[1,2,4]".into(),
            card: 50_000_000,
            acc_before: 0,
            acc_after: 50_000_000,
            eval_us: 100,
            and_us: 0,
            mode: "first".into(),
            range_scan: None,
        });
        collector.filter_us = 200;
        let trace = collector.finalize("test-index", 100);
        assert_eq!(trace.index, "test-index");
        assert_eq!(trace.result_count, 100);
        assert_eq!(trace.clauses.len(), 1);
        assert_eq!(trace.clauses[0].field, "nsfwLevel");
    }

    #[test]
    fn test_clause_helpers() {
        let clause = FilterClause::Eq("status".into(), Value::Integer(1));
        assert_eq!(clause.field_name(), Some("status"));
        assert_eq!(clause.op_name(), "Eq");
        assert_eq!(clause.values_repr(), "1");

        let clause = FilterClause::In("tags".into(), vec![Value::Integer(1), Value::Integer(2)]);
        assert_eq!(clause.op_name(), "In");
        assert_eq!(clause.values_repr(), "[1,2]");
    }

    #[test]
    fn test_trace_ring_buffer() {
        let buf = super::TraceBuffer::new(3); // capacity 3

        let make_trace = |n: u64| QueryTrace {
            ts: format!("2026-03-14T00:00:0{n}.000Z"),
            index: "test".into(),
            total_us: n * 100,
            plan_us: 0,
            filter_us: 0,
            sort_us: 0,
            result_count: n,
            docs_us: 0,
            docs_count: 0,
            cache_hit: false,
            clauses: vec![],
            sort: None,
            lazy_load_us: 0,
        };

        buf.push(make_trace(1));
        buf.push(make_trace(2));
        assert_eq!(buf.last_n(10).len(), 2);

        buf.push(make_trace(3));
        buf.push(make_trace(4)); // overwrites slot 0
        let recent = buf.last_n(10);
        assert_eq!(recent.len(), 3);
        // Oldest first (VecDeque order), newest last
        assert_eq!(recent[0].result_count, 2);
        assert_eq!(recent[2].result_count, 4);

        // Limit
        let limited = buf.last_n(1);
        assert_eq!(limited.len(), 1);
        assert_eq!(limited[0].result_count, 4);
    }

    #[test]
    fn test_trace_buffer_resize_grow() {
        let buf = super::TraceBuffer::new(2);
        let make_trace = |n: u64| QueryTrace {
            ts: String::new(), index: "t".into(), total_us: n, plan_us: 0,
            filter_us: 0, sort_us: 0, result_count: n, docs_us: 0,
            docs_count: 0, cache_hit: false, clauses: vec![], sort: None,
            lazy_load_us: 0,
        };

        buf.push(make_trace(1));
        buf.push(make_trace(2));
        assert_eq!(buf.capacity(), 2);
        assert_eq!(buf.last_n(10).len(), 2);

        // Grow: existing entries preserved, new pushes don't evict
        buf.resize(5);
        assert_eq!(buf.capacity(), 5);
        assert_eq!(buf.last_n(10).len(), 2);
        buf.push(make_trace(3));
        buf.push(make_trace(4));
        buf.push(make_trace(5));
        assert_eq!(buf.last_n(10).len(), 5);
    }

    #[test]
    fn test_trace_buffer_resize_shrink() {
        let buf = super::TraceBuffer::new(5);
        let make_trace = |n: u64| QueryTrace {
            ts: String::new(), index: "t".into(), total_us: n, plan_us: 0,
            filter_us: 0, sort_us: 0, result_count: n, docs_us: 0,
            docs_count: 0, cache_hit: false, clauses: vec![], sort: None,
            lazy_load_us: 0,
        };

        for i in 1..=5 {
            buf.push(make_trace(i));
        }
        assert_eq!(buf.last_n(10).len(), 5);

        // Shrink: oldest entries dropped immediately
        buf.resize(2);
        assert_eq!(buf.capacity(), 2);
        let traces = buf.last_n(10);
        assert_eq!(traces.len(), 2);
        // Should keep the two newest (4, 5)
        assert_eq!(traces[0].result_count, 4);
        assert_eq!(traces[1].result_count, 5);

        // Next push respects new capacity
        buf.push(make_trace(6));
        let traces = buf.last_n(10);
        assert_eq!(traces.len(), 2);
        assert_eq!(traces[0].result_count, 5);
        assert_eq!(traces[1].result_count, 6);
    }
}
