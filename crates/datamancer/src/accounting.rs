//! Per-provider call accounting for the Phase 3 introspection snapshot.
//!
//! [`ProviderAccounting`] is a lock-free struct of atomics held per provider id
//! in `DatamancerInner` and cloned into each authoritative/historical
//! controller. It collects counters at three points:
//!
//! - **Cold call sites** (`session.rs`): `start_live`, `subscribe`,
//!   `unsubscribe`, and each `fetch_history` spawn — these run where the
//!   `Datamancer`/controller holds the handle directly.
//! - **The single-flight re-tile**: a coalesced (deduplicated) historical fetch.
//! - **The stream (`forward`)**: live-data throughput and the connection /
//!   reconnect / gap / error state derived from in-band `Control` events.
//!
//! Byte throughput and rate-limit hits are invisible at all three points (they
//! live inside the provider's monomorphic loop), so they are folded in from
//! [`datamancer_core::ProviderMetrics`] at snapshot time and reported as
//! `Option`.
//!
//! Reads are sampled (`Relaxed`) with no cross-field consistency guarantee — the
//! snapshot is diagnostic, and determinism is per-symbol, so a few-nanosecond
//! skew across counters is acceptable.

use std::sync::atomic::{AtomicU64, Ordering};

use datamancer_core::{ConnectionState, ControlKind, MarketEvent};

/// Lock-free per-provider call/throughput counters.
#[derive(Debug)]
pub(crate) struct ProviderAccounting {
    history_fetches: AtomicU64,
    history_fetch_coalesced: AtomicU64,
    live_starts: AtomicU64,
    subscribes: AtomicU64,
    unsubscribes: AtomicU64,
    reconnects: AtomicU64,
    messages: AtomicU64,
    gaps_emitted: AtomicU64,
    /// Number of this provider's authoritative `(instrument, kind)` connections
    /// currently up. The provider is `Connected` while any are up; one
    /// substream dropping must not mark the whole provider down while others are
    /// still live. Each controller tracks its own up/down phase and notes
    /// transitions here, so per-substream churn aggregates correctly.
    active_connections: AtomicU64,
    /// Whether any connection has ever come up (so `active == 0` reads as
    /// `Disconnected`, not `Unknown`).
    ever_connected: std::sync::atomic::AtomicBool,
    /// Last `ProviderError` message, behind a mutex (cold, set rarely).
    last_error: std::sync::Mutex<Option<String>>,
}

impl Default for ProviderAccounting {
    fn default() -> Self {
        Self {
            history_fetches: AtomicU64::new(0),
            history_fetch_coalesced: AtomicU64::new(0),
            live_starts: AtomicU64::new(0),
            subscribes: AtomicU64::new(0),
            unsubscribes: AtomicU64::new(0),
            reconnects: AtomicU64::new(0),
            messages: AtomicU64::new(0),
            gaps_emitted: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            ever_connected: std::sync::atomic::AtomicBool::new(false),
            last_error: std::sync::Mutex::new(None),
        }
    }
}

impl ProviderAccounting {
    // --- write side ---------------------------------------------------------

    /// One upstream `fetch_history` call (per gap *segment*, not per `session()`).
    pub(crate) fn record_history_fetch(&self) {
        self.history_fetches.fetch_add(1, Ordering::Relaxed);
    }

    /// One historical fetch elided because a concurrent single-flight winner
    /// already filled the byte-identical range.
    pub(crate) fn record_history_fetch_coalesced(&self) {
        self.history_fetch_coalesced.fetch_add(1, Ordering::Relaxed);
    }

    /// One `start_live` call.
    pub(crate) fn record_live_start(&self) {
        self.live_starts.fetch_add(1, Ordering::Relaxed);
    }

    /// One upstream `subscribe` call (call count, not active-subscription delta).
    pub(crate) fn record_subscribe(&self) {
        self.subscribes.fetch_add(1, Ordering::Relaxed);
    }

    /// One upstream `unsubscribe` call.
    pub(crate) fn record_unsubscribe(&self) {
        self.unsubscribes.fetch_add(1, Ordering::Relaxed);
    }

    /// Fold one forwarded event into the stream-derived counters. `live` is true
    /// for an authoritative live session (fan-out present); only then does a data
    /// event count as a `message` (live-data throughput). Control-derived state
    /// (connection, reconnect, gap, error) is recorded regardless.
    pub(crate) fn record_forwarded(&self, ev: &MarketEvent, live: bool) {
        match ev {
            MarketEvent::Trade(_) | MarketEvent::Quote(_) | MarketEvent::Bar(_) => {
                if live {
                    self.messages.fetch_add(1, Ordering::Relaxed);
                }
            }
            MarketEvent::Control(c) => match &c.kind {
                ControlKind::ProviderError { message, .. } => {
                    if let Ok(mut slot) = self.last_error.lock() {
                        *slot = Some(message.clone());
                    }
                }
                ControlKind::Gap { .. } => {
                    self.gaps_emitted.fetch_add(1, Ordering::Relaxed);
                }
                // Connection up/down is recorded by the owning controller via
                // `record_connection_up`/`record_connection_down`, which know the
                // per-substream phase needed to aggregate correctly (folding it
                // here off a single shared flag mis-attributed reconnects and let
                // one substream's drop mark the whole provider down). The
                // remaining controls carry no provider-level counter.
                ControlKind::ProviderConnected { .. }
                | ControlKind::ProviderDisconnected { .. }
                | ControlKind::SubscriptionChanged { .. }
                | ControlKind::SessionClosing => {}
            },
            _ => {}
        }
    }

    /// One authoritative connection for this provider came up. `reconnect` is
    /// true when the owning controller was previously disconnected (a genuine
    /// reconnect), false on its first connect. Call once per up transition.
    pub(crate) fn record_connection_up(&self, reconnect: bool) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
        self.ever_connected.store(true, Ordering::Relaxed);
        if reconnect {
            self.reconnects.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// One authoritative connection for this provider went down. Call once per
    /// down transition (saturating, so a stray disconnect never underflows).
    pub(crate) fn record_connection_down(&self) {
        let _ = self.active_connections.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |n| Some(n.saturating_sub(1)),
        );
    }
}

// --- read side (sampled; consumed by `Datamancer::snapshot`) ----------------
impl ProviderAccounting {
    pub(crate) fn history_fetches(&self) -> u64 {
        self.history_fetches.load(Ordering::Relaxed)
    }

    pub(crate) fn history_fetch_coalesced(&self) -> u64 {
        self.history_fetch_coalesced.load(Ordering::Relaxed)
    }

    pub(crate) fn live_starts(&self) -> u64 {
        self.live_starts.load(Ordering::Relaxed)
    }

    pub(crate) fn subscribes(&self) -> u64 {
        self.subscribes.load(Ordering::Relaxed)
    }

    pub(crate) fn unsubscribes(&self) -> u64 {
        self.unsubscribes.load(Ordering::Relaxed)
    }

    pub(crate) fn reconnects(&self) -> u64 {
        self.reconnects.load(Ordering::Relaxed)
    }

    pub(crate) fn messages(&self) -> u64 {
        self.messages.load(Ordering::Relaxed)
    }

    pub(crate) fn gaps_emitted(&self) -> u64 {
        self.gaps_emitted.load(Ordering::Relaxed)
    }

    pub(crate) fn connection_state(&self) -> ConnectionState {
        // Aggregate across the provider's substreams: Connected while any are
        // up; Disconnected once all are down having been up; Unknown until the
        // first connect.
        if self.active_connections.load(Ordering::Relaxed) > 0 {
            ConnectionState::Connected
        } else if self.ever_connected.load(Ordering::Relaxed) {
            ConnectionState::Disconnected
        } else {
            ConnectionState::Unknown
        }
    }

    pub(crate) fn last_error(&self) -> Option<String> {
        self.last_error.lock().ok().and_then(|s| s.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::ProviderAccounting;
    use datamancer_core::{
        AssetClass, ConnectionState, Control, ControlKind, GapSpan, Instrument, MarketEvent, Price,
        ProviderId, Seq, Timestamp, Trade,
    };

    fn trade() -> MarketEvent {
        MarketEvent::Trade(Trade {
            instrument: Instrument::new(ProviderId::from_static("p"), AssetClass::Equity, "X"),
            source_ts: Timestamp(1),
            rx_ts: Timestamp(2),
            seq: Seq(0),
            price: Price::from_f64_round(1.0),
            size: 1,
        })
    }

    fn control(kind: ControlKind) -> MarketEvent {
        MarketEvent::Control(Control {
            source_ts: Timestamp(0),
            rx_ts: Timestamp(0),
            seq: Seq(0),
            kind,
        })
    }

    #[test]
    fn cold_counters_increment() {
        let a = ProviderAccounting::default();
        a.record_history_fetch();
        a.record_history_fetch();
        a.record_history_fetch_coalesced();
        a.record_live_start();
        a.record_subscribe();
        a.record_unsubscribe();
        assert_eq!(a.history_fetches(), 2);
        assert_eq!(a.history_fetch_coalesced(), 1);
        assert_eq!(a.live_starts(), 1);
        assert_eq!(a.subscribes(), 1);
        assert_eq!(a.unsubscribes(), 1);
    }

    #[test]
    fn messages_count_only_when_live() {
        let a = ProviderAccounting::default();
        a.record_forwarded(&trade(), false); // historical forward: not a message
        a.record_forwarded(&trade(), true); // live forward: counts
        assert_eq!(a.messages(), 1);
    }

    #[test]
    fn connection_and_reconnect_from_transitions() {
        let a = ProviderAccounting::default();
        assert_eq!(a.connection_state(), ConnectionState::Unknown);
        a.record_connection_up(false); // first connect
        assert_eq!(a.connection_state(), ConnectionState::Connected);
        assert_eq!(a.reconnects(), 0); // first connect is not a reconnect
        a.record_connection_down();
        assert_eq!(a.connection_state(), ConnectionState::Disconnected);
        a.record_connection_up(true); // reconnect
        assert_eq!(a.connection_state(), ConnectionState::Connected);
        assert_eq!(a.reconnects(), 1);
    }

    #[test]
    fn one_substream_drop_does_not_mark_provider_down() {
        // Two substreams of the same provider; one dropping must not flip the
        // whole provider to Disconnected, and a second substream's first connect
        // is not a reconnect.
        let a = ProviderAccounting::default();
        a.record_connection_up(false); // substream A first connect
        a.record_connection_up(false); // substream B first connect
        assert_eq!(a.reconnects(), 0);
        assert_eq!(a.connection_state(), ConnectionState::Connected);
        a.record_connection_down(); // A drops; B still up
        assert_eq!(a.connection_state(), ConnectionState::Connected);
        a.record_connection_down(); // B drops; now all down
        assert_eq!(a.connection_state(), ConnectionState::Disconnected);
        assert_eq!(a.reconnects(), 0);
    }

    #[test]
    fn gaps_and_last_error_from_control() {
        let a = ProviderAccounting::default();
        a.record_forwarded(
            &control(ControlKind::Gap {
                provider: "p".to_string(),
                instrument: Instrument::new(ProviderId::from_static("p"), AssetClass::Equity, "X"),
                span: GapSpan {
                    from_source_ts: Timestamp(1),
                    to_source_ts: Timestamp(2),
                },
            }),
            true,
        );
        assert_eq!(a.gaps_emitted(), 1);
        assert_eq!(a.last_error(), None);
        a.record_forwarded(
            &control(ControlKind::ProviderError {
                provider: "p".to_string(),
                message: "boom".to_string(),
            }),
            true,
        );
        assert_eq!(a.last_error(), Some("boom".to_string()));
    }
}
