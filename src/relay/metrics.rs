//! Prometheus metrics for the relay.
//!
//! Uses a local `Registry` rather than the global default so the metrics
//! struct is freely instantiable across tests + processes without
//! `AlreadyReg` panics.

use prometheus::{
    CounterVec, GaugeVec, HistogramOpts, HistogramVec, Opts, Registry,
};

#[derive(Clone)]
pub struct RelayMetrics {
    pub registry: Registry,
    pub emit_total: CounterVec,
    pub emit_skipped_no_subscriber: CounterVec,
    pub emit_parse_error: CounterVec,
    pub drops_total: CounterVec,
    pub request_duration: HistogramVec,
    pub sse_subscribers: GaugeVec,
    pub sse_lagged_events: CounterVec,
    pub mode: GaugeVec,
}

impl RelayMetrics {
    pub fn new() -> Self {
        let registry = Registry::new();
        let emit_total = CounterVec::new(
            Opts::new(
                "relay_emit_total",
                "Number of events emitted to a channel from a route",
            ),
            &["channel", "route"],
        )
        .unwrap();
        let emit_skipped_no_subscriber = CounterVec::new(
            Opts::new(
                "relay_emit_skipped_no_subscriber_total",
                "Emit work skipped because no SSE subscriber and capture disabled",
            ),
            &["channel"],
        )
        .unwrap();
        let emit_parse_error = CounterVec::new(
            Opts::new(
                "relay_emit_parse_error_total",
                "Number of {body|json} token parse failures",
            ),
            &["route"],
        )
        .unwrap();
        let drops_total = CounterVec::new(
            Opts::new(
                "relay_drops_total",
                "Subscriber-side drops (lagged or capacity)",
            ),
            &["channel", "reason"],
        )
        .unwrap();
        let request_duration = HistogramVec::new(
            HistogramOpts::new(
                "relay_request_duration_seconds",
                "End-to-end handler latency",
            ),
            &["route"],
        )
        .unwrap();
        let sse_subscribers = GaugeVec::new(
            Opts::new("relay_sse_subscribers", "Active SSE connections"),
            &["channel"],
        )
        .unwrap();
        let sse_lagged_events = CounterVec::new(
            Opts::new(
                "relay_sse_lagged_events_total",
                "Sum of n from each RecvError::Lagged(n) seen by SSE subscribers",
            ),
            &["channel"],
        )
        .unwrap();
        let mode = GaugeVec::new(
            Opts::new("relay_mode", "Constant 1 with the BITDEX_MODE label"),
            &["mode"],
        )
        .unwrap();
        mode.with_label_values(&["relay"]).set(1.0);

        registry.register(Box::new(emit_total.clone())).unwrap();
        registry
            .register(Box::new(emit_skipped_no_subscriber.clone()))
            .unwrap();
        registry.register(Box::new(emit_parse_error.clone())).unwrap();
        registry.register(Box::new(drops_total.clone())).unwrap();
        registry.register(Box::new(request_duration.clone())).unwrap();
        registry.register(Box::new(sse_subscribers.clone())).unwrap();
        registry.register(Box::new(sse_lagged_events.clone())).unwrap();
        registry.register(Box::new(mode.clone())).unwrap();

        Self {
            registry,
            emit_total,
            emit_skipped_no_subscriber,
            emit_parse_error,
            drops_total,
            request_duration,
            sse_subscribers,
            sse_lagged_events,
            mode,
        }
    }
}

impl Default for RelayMetrics {
    fn default() -> Self {
        Self::new()
    }
}
