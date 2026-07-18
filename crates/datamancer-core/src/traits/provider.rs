//! The Provider extension point.
//!
//! A `Provider` represents one upstream source of market data (Alpaca,
//! Polygon, IBKR, a local replay file, etc.). Datamancer holds providers
//! behind `Box<dyn Provider>` so adding a new provider is purely additive
//! at the consumer layer.
//!
//! # Hot-path discipline
//!
//! Dynamic dispatch lives at the **cold** boundary — start, subscribe,
//! unsubscribe, and history fetch. Inside a provider's running task, the
//! per-message decode loop is monomorphized: the provider owns its own
//! concrete `mpsc::Sender<MarketEvent>` and yields fully-formed events into
//! it. Consumers only see the dyn vtable when calling these cold methods or
//! when polling the merged session stream — never per websocket frame.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::{
    adjustment::Adjustment,
    error::Result,
    event::{EventKind, MarketEvent, Timestamp},
    instrument::Instrument,
};

/// One configured upstream source of market data.
#[async_trait]
pub trait Provider: Send + Sync + 'static {
    /// Stable identifier for this provider (e.g. `"alpaca"`). Used in
    /// configuration, control events, and storage keys.
    fn id(&self) -> &str;

    /// Whether this provider can serve `kind` for `instrument`. Datamancer
    /// uses this when routing subscriptions across multiple registered
    /// providers; a provider that doesn't know the instrument should return
    /// `false` rather than fail later.
    fn supports(&self, instrument: &Instrument, kind: EventKind) -> bool;

    /// Open a live session against this provider.
    ///
    /// `sink` is the channel the provider's internal task pushes
    /// fully-formed `MarketEvent`s into; the channel is concrete (not dyn),
    /// keeping the per-message path monomorphized inside the provider crate.
    /// Datamancer assigns final `seq` values downstream — the provider's job
    /// is to surface ordered, decoded events.
    async fn start_live(&self, sink: mpsc::Sender<MarketEvent>) -> Result<Box<dyn LiveHandle>>;

    /// Fetch a bounded historical range, pushing events into `sink` in
    /// source-timestamp order. Returns once the range is exhausted; pagination
    /// and rate-limit handling are the provider's responsibility.
    async fn fetch_history(
        &self,
        request: HistoryRequest,
        sink: mpsc::Sender<MarketEvent>,
    ) -> Result<()>;

    /// Enumerate the instruments this provider can serve.
    ///
    /// Powers the consumer-facing instrument catalog: the UI's instrument
    /// picker, automated tests that need a real symbol list without
    /// hard-coding one, and the multi-provider routing layer that wants to
    /// know what's reachable before opening a session.
    ///
    /// Implementations should return only **tradable, currently active**
    /// instruments — surfacing delisted, halted, or otherwise unavailable
    /// rows would put symbols in the picker that fail at subscribe time.
    /// Each returned [`Instrument`] must carry this provider's
    /// [`crate::ProviderId`] in its `provider` field so the result is
    /// safe to feed back into [`Self::supports`] or `Session` construction
    /// without ambiguity.
    ///
    /// Default implementation returns an empty list — providers without a
    /// reference-data surface (test fakes, replay-only sources) can leave
    /// this alone. Network-backed providers should override it; the cold
    /// boundary already pays for dynamic dispatch, so per-call overhead is
    /// not a concern.
    async fn list_instruments(&self) -> Result<Vec<Instrument>> {
        Ok(Vec::new())
    }

    /// Optional accounting sink for metrics datamancer cannot observe at the
    /// cold boundary or from in-band [`crate::Control`] events — namely byte
    /// throughput and rate-limit hits, which live inside the provider's
    /// monomorphic decode loop / REST pagination.
    ///
    /// Default `None`: the provider reports nothing beyond what datamancer
    /// counts at the cold boundary (start/subscribe/fetch call counts) and
    /// derives from `Control` events (connection state, reconnects, gaps,
    /// errors). The diagnostics snapshot folds a returned sink's counters in,
    /// surfacing them as `Some` and leaving them `None` for providers that do
    /// not override this. Implementations should record off the hot per-message
    /// path (e.g. once per HTTP page or websocket frame batch).
    ///
    /// **Stability contract:** an implementation must return clones of one
    /// stable, long-lived `Arc<dyn ProviderMetrics>` — not a freshly allocated
    /// sink per call. `Datamancer::snapshot()` re-queries `metrics()` on every
    /// sample; returning a new sink each time would reset the cumulative
    /// counters (`bytes`, `rate_limit_hits`) to zero on each snapshot.
    fn metrics(&self) -> Option<Arc<dyn ProviderMetrics>> {
        None
    }

    /// Whether this provider is currently enabled. `Watch(None)` settings
    /// sources (daemon-parked) report `false`; the default covers providers
    /// without a runtime settings seam.
    fn enabled(&self) -> bool {
        true
    }

    /// One-shot most-recent value for a symbol, for immediate consumer feedback
    /// when a live subscription opens. Cold-boundary, off the per-message hot
    /// path — datamancer calls this at most once per authoritative live session
    /// and never per websocket frame.
    ///
    /// Returns the most recent [`MarketEvent`] of `kind` for `instrument`, or
    /// `None` when the provider has no snapshot surface or nothing is available.
    /// `seq` on the returned event is a placeholder (`Seq(0)`); the authoritative
    /// controller re-stamps it in canonical delivery order, exactly as for live
    /// and backfill data.
    ///
    /// Default returns `None` — providers without a snapshot/latest endpoint
    /// (test fakes, replay-only sources) leave this alone and the live-seed step
    /// gracefully no-ops.
    async fn latest(
        &self,
        instrument: &Instrument,
        kind: EventKind,
    ) -> Result<Option<MarketEvent>> {
        let _ = (instrument, kind);
        Ok(None)
    }
}

/// Provider-side accounting sink for metrics invisible at datamancer's cold
/// boundary. Returned by [`Provider::metrics`]; the diagnostics snapshot reads
/// the accumulated counters via [`ProviderMetrics::bytes`] /
/// [`ProviderMetrics::rate_limit_hits`].
///
/// Implementations use lock-free atomic adders (Relaxed); the snapshot is a
/// sampled view with no cross-field consistency guarantee.
pub trait ProviderMetrics: Send + Sync {
    /// Record `n` bytes received from the upstream transport.
    fn record_bytes(&self, n: u64);

    /// Record one rate-limit hit (a throttle / 429 / backoff from upstream).
    fn record_rate_limit(&self);

    /// Cumulative bytes received.
    fn bytes(&self) -> u64;

    /// Cumulative rate-limit hits.
    fn rate_limit_hits(&self) -> u64;
}

/// A handle to a running live provider session. Subscription mutation and
/// shutdown go through this handle.
///
/// In datamancer's session model each session is scoped to a single
/// `(instrument, kind)` pair, so the subscription primitives operate on one
/// pair at a time. Provider implementations may multiplex multiple pairs over
/// a single underlying connection.
#[async_trait]
pub trait LiveHandle: Send + Sync {
    /// Activate `(instrument, kind)` against the live session. Should return
    /// once the provider has acknowledged the change (or surfaces the result
    /// via a `ControlKind::SubscriptionChanged` entry on the event sink).
    async fn subscribe(&self, instrument: Instrument, kind: EventKind) -> Result<()>;

    /// Deactivate `(instrument, kind)`. Symmetric with `subscribe`.
    async fn unsubscribe(&self, instrument: Instrument, kind: EventKind) -> Result<()>;

    /// Tear down the live connection. Implementations should drop the event
    /// sink after final teardown so the consuming session sees a clean EOF.
    async fn close(self: Box<Self>) -> Result<()>;
}

/// Bounded request used by [`Provider::fetch_history`].
#[derive(Debug, Clone, PartialEq)]
pub struct HistoryRequest {
    pub instrument: Instrument,
    pub kind: EventKind,
    pub from: Timestamp,
    pub to: Timestamp,
    /// Corporate-action adjustment mode the provider should fetch under.
    /// Descends from the session's single source of truth; the provider reads
    /// this rather than carrying its own mode.
    pub adjustment: Adjustment,
}

#[cfg(test)]
mod tests {
    use super::{LiveHandle, Provider};
    use crate::{
        error::Result,
        event::{EventKind, MarketEvent},
        instrument::Instrument,
    };
    use async_trait::async_trait;
    use tokio::sync::mpsc;

    struct BareProvider;

    #[async_trait]
    impl Provider for BareProvider {
        #[allow(
            clippy::unnecessary_literal_bound,
            reason = "trait signature fixes the return type to &str"
        )]
        fn id(&self) -> &str {
            "bare"
        }
        fn supports(&self, _instrument: &Instrument, _kind: EventKind) -> bool {
            false
        }
        async fn start_live(
            &self,
            _sink: mpsc::Sender<MarketEvent>,
        ) -> Result<Box<dyn LiveHandle>> {
            unreachable!("not used in this test")
        }
        async fn fetch_history(
            &self,
            _request: super::HistoryRequest,
            _sink: mpsc::Sender<MarketEvent>,
        ) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn default_provider_metrics_is_none() {
        let p = BareProvider;
        assert!(p.metrics().is_none());
    }

    #[tokio::test]
    async fn default_provider_latest_is_none() {
        use crate::instrument::{AssetClass, ProviderId};
        let p = BareProvider;
        let inst = Instrument::new(ProviderId::from_static("bare"), AssetClass::Equity, "AAPL");
        let got = p.latest(&inst, EventKind::Trade).await.unwrap();
        assert!(got.is_none());
    }
}
