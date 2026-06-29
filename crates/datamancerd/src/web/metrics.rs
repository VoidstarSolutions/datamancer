//! Optional Prometheus `/metrics` endpoint (feature `metrics`).
//!
//! Off by default until a scraper is deployed. Translates the snapshot's numeric
//! fields into Prometheus gauges/counters. Every per-symbol series is **labelled
//! per `(instrument, kind)`** — never a single conflated global counter — to
//! preserve the per-symbol framing (CLAUDE.md). Cardinality is therefore bounded
//! by the number of *actively-subscribed* `(instrument, kind)` units; enable
//! this endpoint only once a scraper is actually deployed.
//!
//! # One-shot global recorder (hazard)
//!
//! `PrometheusBuilder::install_recorder()` installs a **process-global** recorder
//! and is **one-shot**: a second install errors. Call [`install`] exactly once at
//! daemon startup. The returned [`PrometheusHandle`] is stored in a process
//! `OnceLock`; the `/metrics` handler renders from it. Tests that exercise this
//! must isolate to a single process-wide install (hence the metrics test is
//! `#[ignore]`d by default).

use std::sync::OnceLock;

use axum::http::StatusCode;
use datamancer::SystemSnapshot;
use metrics::{counter, gauge};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

/// The recorder install outcome, computed exactly once. Storing the `Result`
/// (not just the handle) makes `install` idempotent **and** race-free: the
/// one-shot `install_recorder` runs inside `get_or_init`, so two concurrent
/// callers never both attempt it (which would fail the loser).
static HANDLE: OnceLock<Result<PrometheusHandle, String>> = OnceLock::new();

/// Install the process-global Prometheus recorder exactly once and retain its
/// render handle. Safe under concurrent calls: the underlying one-shot
/// `install_recorder` runs at most once via `get_or_init`.
///
/// # Errors
///
/// Returns the builder error if the global recorder could not be installed.
pub fn install() -> Result<(), String> {
    HANDLE
        .get_or_init(|| {
            PrometheusBuilder::new()
                .install_recorder()
                .map_err(|e| format!("install prometheus recorder: {e}"))
        })
        .as_ref()
        .map(|_| ())
        .map_err(Clone::clone)
}

/// The render handle, if the recorder has been installed successfully.
#[must_use]
pub fn handle() -> Option<&'static PrometheusHandle> {
    HANDLE.get().and_then(|r| r.as_ref().ok())
}

/// Update the recorded metrics from a freshly-assembled snapshot. Per-symbol
/// series carry `instrument`/`kind` labels. Driven by the live-refresh task
/// (so only reachable when `web-ui` is also enabled); a `metrics`-only build
/// retains it for the recorder + render surface.
#[cfg_attr(not(feature = "web-ui"), allow(dead_code))]
pub fn update_from_snapshot(snap: &SystemSnapshot) {
    for p in &snap.providers {
        let provider = p.provider.as_str().to_string();
        counter!("datamancerd_provider_history_fetches_total", "provider" => provider.clone())
            .absolute(p.history_fetches);
        counter!("datamancerd_provider_reconnects_total", "provider" => provider.clone())
            .absolute(p.reconnects);
        counter!("datamancerd_provider_messages_total", "provider" => provider.clone())
            .absolute(p.messages);
        if let Some(hits) = p.rate_limit_hits {
            counter!("datamancerd_provider_rate_limit_hits_total", "provider" => provider.clone())
                .absolute(hits);
        }
    }
    for s in &snap.authoritative_sessions {
        // Label by provider AND symbol: `symbol()` alone collapses two
        // providers' identically-named symbols (e.g. both `AAPL`) into one
        // series. `kind` completes the per-`(instrument, kind)` identity.
        let provider = s.instrument.provider().to_string();
        let inst = s.instrument.symbol().to_string();
        let kind = format!("{:?}", s.kind);
        gauge!("datamancerd_session_subscriber_refcount", "provider" => provider.clone(), "instrument" => inst.clone(), "kind" => kind.clone())
            .set(f64::from(s.subscriber_refcount));
        counter!("datamancerd_session_gaps_total", "provider" => provider.clone(), "instrument" => inst.clone(), "kind" => kind.clone())
            .absolute(s.gap_count);
        if let Some(latency) = s.latency_ns {
            #[allow(
                clippy::cast_precision_loss,
                reason = "observability gauge; precision loss on huge latencies is acceptable"
            )]
            gauge!("datamancerd_session_latency_ns", "provider" => provider.clone(), "instrument" => inst.clone(), "kind" => kind.clone())
                .set(latency as f64);
        }
    }
}

/// `GET /metrics` — render the Prometheus text exposition, or 503 if the
/// recorder was never installed.
pub(crate) async fn render_handler() -> (StatusCode, String) {
    match handle() {
        Some(h) => (StatusCode::OK, h.render()),
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            "metrics recorder not installed".to_string(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::web::testdata;

    // Installs the **process-global** one-shot Prometheus recorder, so it is
    // `#[ignore]`d by default to avoid colliding with any other test in the
    // process. Run explicitly: `cargo test -p datamancerd --features metrics -- --ignored`.
    #[test]
    #[ignore = "installs the process-global Prometheus recorder"]
    fn metrics_endpoint_renders() {
        install().expect("install recorder");
        update_from_snapshot(&testdata::snapshot());
        let rendered = handle().expect("handle present").render();
        assert!(
            rendered.contains("datamancerd_provider_messages_total"),
            "provider counter present: {rendered}"
        );
        assert!(
            rendered.contains("datamancerd_session_subscriber_refcount"),
            "per-symbol gauge present: {rendered}"
        );
        assert!(
            rendered.contains("instrument=\"AAPL\""),
            "per-symbol label present: {rendered}"
        );
    }
}
