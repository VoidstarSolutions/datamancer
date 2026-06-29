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

static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Install the process-global Prometheus recorder exactly once and retain its
/// render handle. Idempotent at the `OnceLock` level (a second call is a no-op),
/// but the underlying `install_recorder` is itself one-shot per process.
///
/// # Errors
///
/// Returns the builder error if the global recorder cannot be installed.
pub fn install() -> Result<(), String> {
    if HANDLE.get().is_some() {
        return Ok(());
    }
    let handle = PrometheusBuilder::new()
        .install_recorder()
        .map_err(|e| format!("install prometheus recorder: {e}"))?;
    let _ = HANDLE.set(handle);
    Ok(())
}

/// The render handle, if the recorder has been installed.
#[must_use]
pub fn handle() -> Option<&'static PrometheusHandle> {
    HANDLE.get()
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
        let inst = s.instrument.symbol().to_string();
        let kind = format!("{:?}", s.kind);
        gauge!("datamancerd_session_subscriber_refcount", "instrument" => inst.clone(), "kind" => kind.clone())
            .set(f64::from(s.subscriber_refcount));
        counter!("datamancerd_session_gaps_total", "instrument" => inst.clone(), "kind" => kind.clone())
            .absolute(s.gap_count);
        if let Some(latency) = s.latency_ns {
            #[allow(
                clippy::cast_precision_loss,
                reason = "observability gauge; precision loss on huge latencies is acceptable"
            )]
            gauge!("datamancerd_session_latency_ns", "instrument" => inst.clone(), "kind" => kind.clone())
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
