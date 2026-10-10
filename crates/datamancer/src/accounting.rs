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

use datamancer_core::{
    ConnectionState, Control, ControlKind, DisconnectCause, MarketEvent, StreamConnection,
};

use crate::client_session::ConnectionChange;

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
    /// Whether any of this provider's substreams has ever connected. The live
    /// connection state itself is **not** kept here: it is derived at snapshot
    /// time from the active substreams' own phases
    /// ([`Self::connection_state`]), so a parallel counter cannot drift from
    /// them. This flag only decides how a provider with no active substream
    /// reads (`Disconnected`, not `Unknown`).
    ever_connected: std::sync::atomic::AtomicBool,
    /// Last `ProviderError` message, behind a mutex (cold, set rarely).
    last_error: std::sync::Mutex<Option<String>>,
    /// Set when a disconnect carried `DisconnectCause::Unauthenticated`;
    /// cleared on the next successful connect. Sticky so a provider whose last
    /// substream has gone keeps reporting the rejection.
    auth_failed: std::sync::atomic::AtomicBool,
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
            ever_connected: std::sync::atomic::AtomicBool::new(false),
            last_error: std::sync::Mutex::new(None),
            auth_failed: std::sync::atomic::AtomicBool::new(false),
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

    /// Fold one forwarded event into the throughput counters. `live` is true
    /// for an authoritative live session (fan-out present); only then does a data
    /// event count as a `message` (live-data throughput). A `Gap` the controller
    /// emits is counted here too. Provider-originated controls are folded at
    /// receipt instead ([`Self::record_provider_control`]), so a control held in
    /// a backfill ring, or evicted from it, is never missed.
    pub(crate) fn record_forwarded(&self, ev: &MarketEvent, live: bool) {
        match ev {
            MarketEvent::Trade(_) | MarketEvent::Quote(_) | MarketEvent::Bar(_) => {
                if live {
                    self.messages.fetch_add(1, Ordering::Relaxed);
                }
            }
            MarketEvent::Control(Control {
                kind: ControlKind::Gap { .. },
                ..
            }) => {
                self.gaps_emitted.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    /// Fold one control **received from the provider** (before any buffering
    /// or delivery), together with the phase transition the owning substream
    /// recorded for it. Connect/disconnect counters move only on a real
    /// transition — a duplicate connect is `ConnectionChange::None` and counts
    /// nothing — while the auth classification and the error text are taken
    /// from every control that carries them.
    pub(crate) fn record_provider_control(&self, kind: &ControlKind, change: ConnectionChange) {
        if let ConnectionChange::Connected { reconnect } = change {
            self.ever_connected.store(true, Ordering::Relaxed);
            self.auth_failed.store(false, Ordering::Relaxed);
            if reconnect {
                self.reconnects.fetch_add(1, Ordering::Relaxed);
            }
        }
        match kind {
            ControlKind::ProviderDisconnected { reason, cause, .. }
                if *cause == DisconnectCause::Unauthenticated =>
            {
                self.auth_failed.store(true, Ordering::Relaxed);
                self.record_error(reason);
            }
            ControlKind::ProviderError { message, .. } => self.record_error(message),
            _ => {}
        }
    }

    /// Record the provider's last error text (a `ProviderError`, an auth
    /// rejection's reason, or a failed fetch the controller surfaced itself).
    pub(crate) fn record_error(&self, message: &str) {
        if let Ok(mut slot) = self.last_error.lock() {
            *slot = Some(message.to_string());
        }
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

    /// The provider's connection state, derived from the phases of its active
    /// substreams sampled in the same snapshot (`streams`), so it can never
    /// disagree with them: `Connected` while any is `Up`; otherwise
    /// `Unauthenticated` if any is down on an auth rejection; otherwise
    /// `Disconnected` if any is down; otherwise (all still `Pending`) `Unknown`,
    /// a connect in flight. With no active substream the sticky facts decide:
    /// the last rejection (`Unauthenticated`), ever having connected
    /// (`Disconnected`), else `Unknown`.
    pub(crate) fn connection_state(
        &self,
        streams: impl IntoIterator<Item = StreamConnection>,
    ) -> ConnectionState {
        let (mut any, mut up, mut down, mut unauthenticated) = (false, false, false, false);
        for phase in streams {
            any = true;
            match phase {
                StreamConnection::Up { .. } => up = true,
                StreamConnection::Down { cause, .. } => {
                    down = true;
                    unauthenticated |= cause == DisconnectCause::Unauthenticated;
                }
                // `Pending`, and any phase a future core adds (`non_exhaustive`):
                // not evidence either way.
                _ => {}
            }
        }
        if up {
            ConnectionState::Connected
        } else if unauthenticated || (!any && self.auth_failed.load(Ordering::Relaxed)) {
            ConnectionState::Unauthenticated
        } else if down || (!any && self.ever_connected.load(Ordering::Relaxed)) {
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
    use super::{ConnectionChange, ProviderAccounting};
    use datamancer_core::{
        AssetClass, ConnectionState, Control, ControlKind, DisconnectCause, GapSpan, Instrument,
        MarketEvent, Price, ProviderId, Seq, StreamConnection, Timestamp, Trade,
    };

    fn trade() -> MarketEvent {
        MarketEvent::Trade(Trade {
            instrument: Instrument::new(ProviderId::from_static("p"), AssetClass::Equity, "X"),
            source_ts: Timestamp(1),
            rx_ts: Timestamp(2),
            seq: Seq(0),
            price: Price::from_f64_round(1.0),
            size: datamancer_core::Quantity::from_units(1),
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

    fn up() -> StreamConnection {
        StreamConnection::Up {
            since: Timestamp(1),
        }
    }

    fn down(cause: DisconnectCause) -> StreamConnection {
        StreamConnection::Down {
            since: Timestamp(2),
            cause,
        }
    }

    fn connected() -> ControlKind {
        ControlKind::ProviderConnected {
            provider: "p".to_string(),
        }
    }

    fn disconnected(reason: &str, cause: DisconnectCause) -> ControlKind {
        ControlKind::ProviderDisconnected {
            provider: "p".to_string(),
            reason: reason.to_string(),
            cause,
        }
    }

    #[test]
    fn connection_state_is_derived_from_the_active_substreams() {
        let a = ProviderAccounting::default();
        // Nothing active, never connected: Unknown.
        assert_eq!(a.connection_state([]), ConnectionState::Unknown);
        // A connect in flight: still Unknown (Connecting), not Disconnected.
        assert_eq!(
            a.connection_state([StreamConnection::Pending]),
            ConnectionState::Unknown
        );
        // Any Up wins, whatever the others are doing.
        assert_eq!(
            a.connection_state([
                down(DisconnectCause::Error),
                up(),
                StreamConnection::Pending
            ]),
            ConnectionState::Connected
        );
        // All down: Disconnected; an auth rejection among them wins.
        assert_eq!(
            a.connection_state([down(DisconnectCause::Error), StreamConnection::Pending]),
            ConnectionState::Disconnected
        );
        assert_eq!(
            a.connection_state([
                down(DisconnectCause::Error),
                down(DisconnectCause::Unauthenticated),
            ]),
            ConnectionState::Unauthenticated
        );
    }

    #[test]
    fn reconnects_count_transitions_only() {
        let a = ProviderAccounting::default();
        a.record_provider_control(
            &connected(),
            ConnectionChange::Connected { reconnect: false },
        );
        assert_eq!(a.reconnects(), 0, "a first connect is not a reconnect");
        // A duplicate connect while already up is no transition: nothing moves.
        a.record_provider_control(&connected(), ConnectionChange::None);
        assert_eq!(a.reconnects(), 0);
        a.record_provider_control(
            &disconnected("ws closed", DisconnectCause::Error),
            ConnectionChange::Disconnected,
        );
        a.record_provider_control(
            &connected(),
            ConnectionChange::Connected { reconnect: true },
        );
        assert_eq!(a.reconnects(), 1);
    }

    #[test]
    fn sticky_facts_decide_with_no_active_substream() {
        let a = ProviderAccounting::default();
        // Rejected credentials, then the substream is gone: still Unauthenticated.
        a.record_provider_control(
            &disconnected("auth rejected", DisconnectCause::Unauthenticated),
            ConnectionChange::Disconnected,
        );
        assert_eq!(a.connection_state([]), ConnectionState::Unauthenticated);
        assert_eq!(a.last_error(), Some("auth rejected".to_string()));
        // A successful connect clears the rejection; once that substream is
        // gone too the provider reads Disconnected (it has connected before).
        a.record_provider_control(
            &connected(),
            ConnectionChange::Connected { reconnect: true },
        );
        assert_eq!(a.connection_state([up()]), ConnectionState::Connected);
        assert_eq!(a.connection_state([]), ConnectionState::Disconnected);
    }

    #[test]
    fn plain_disconnect_does_not_mark_unauthenticated() {
        let a = ProviderAccounting::default();
        a.record_provider_control(
            &disconnected("ws closed", DisconnectCause::Error),
            ConnectionChange::Disconnected,
        );
        assert_eq!(a.connection_state([]), ConnectionState::Unknown);
        assert_eq!(a.last_error(), None);
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
        a.record_provider_control(
            &ControlKind::ProviderError {
                provider: "p".to_string(),
                message: "boom".to_string(),
            },
            ConnectionChange::None,
        );
        assert_eq!(a.last_error(), Some("boom".to_string()));
    }
}
