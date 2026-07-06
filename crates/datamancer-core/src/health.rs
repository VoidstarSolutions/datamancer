//! App-facing health reduction of [`SystemSnapshot`] (spec 2026-07-05, cycle 1).
//!
//! Pure types + a pure reduction — assembly stays in `datamancer`, transport
//! in `datamancer-client`. Per-symbol only: no cross-instrument aggregate is
//! ever computed. `Liveness`/`latency` derive from wall-clock fields
//! (`captured_at`, `last_rx_ts`, `latency_ns`) — observability only, never
//! engine logic.

use serde::{Deserialize, Serialize};

use crate::{
    event::{EventKind, Timestamp},
    instrument::{Instrument, ProviderId},
    snapshot::{ConnectionState, SystemSnapshot},
};

/// A typed, versioned, app-renderable reduction of [`SystemSnapshot`]:
/// "is my market data healthy?" for an end-user application. One entry per
/// `(instrument, kind)` in [`streams`](Self::streams) — no cross-symbol
/// aggregate exists or is implied.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HealthView {
    /// Shape version of this view; bump on breaking changes to the reduction.
    pub schema_version: u32,
    pub daemon: DaemonHealth,
    pub providers: Vec<ProviderHealth>,
    pub streams: Vec<StreamHealth>,
}

/// Daemon-process-level health.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DaemonHealth {
    /// The serving process's version. `None` out of the pure reduction — the
    /// caller (facade after the `ping` handshake, or the embedding library)
    /// assigns it, because the snapshot does not carry it.
    pub version: Option<String>,
    /// The daemon's active credential-store backend (`"keychain"`,
    /// `"secret-service"`, `"file"`). `None` out of the pure reduction — the
    /// caller stamps it (facade, from the `ping` handshake), and `None` also
    /// means "daemon predates the credential broker". A surprising `"file"`
    /// on a desktop host is visible here rather than silent.
    pub credential_backend: Option<String>,
    /// Wall-clock at snapshot assembly (observability).
    pub captured_at: Timestamp,
}

/// One provider's connection health.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderHealth {
    pub provider: ProviderId,
    pub state: ProviderState,
    /// Human-renderable detail (e.g. the provider's last error). Free text,
    /// non-contractual.
    pub detail: Option<String>,
}

/// Provider connection state, app-facing.
///
/// `Unauthenticated` and `CompanionUnreachable` are **reserved** (spec
/// appendix: IBKR attaches to a local TWS/IB Gateway that can be down or
/// needing re-auth); nothing produces them in cycle 1, but they exist now so
/// shipped consumers already parse them.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderState {
    Connected,
    /// Not yet observed connected (initial / connecting / reconnecting).
    Connecting,
    Disconnected,
    /// Credentials rejected or an auth session lapsed (reserved).
    Unauthenticated,
    /// A required companion process (e.g. IB Gateway) is unreachable (reserved).
    CompanionUnreachable,
}

/// Per-`(instrument, kind)` stream health. Per-symbol only.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamHealth {
    pub instrument: Instrument,
    pub kind: EventKind,
    pub liveness: Liveness,
    /// Provider-reported market time of the last event.
    pub last_event_source_ts: Option<Timestamp>,
    /// Cumulative per-symbol `Control::Gap` count.
    pub gap_count: u64,
    /// `rx_ts`-derived; observability only, never engine logic.
    pub latency: Option<LatencySummary>,
}

/// Stream liveness, judged on wall-clock receipt (`rx_ts` vs the snapshot's
/// `captured_at`) — observability only.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Liveness {
    /// Subscribed, no data event observed yet.
    Idle,
    Live,
    /// No event within the staleness threshold; `since` is the last receipt.
    Stale {
        since: Timestamp,
    },
}

/// Latency summary for one stream (cycle 1: last observation only).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LatencySummary {
    /// Last `rx_ts - source_ts`. Signed: straddles two clocks and may be
    /// negative under clock skew (see `AuthoritativeSessionSnapshot::latency_ns`).
    pub last_ns: i64,
}

impl HealthView {
    /// Shape version of the current reduction.
    pub const SCHEMA_VERSION: u32 = 1;
    /// Default staleness threshold: 5 seconds without a received event.
    pub const DEFAULT_STALE_AFTER_NS: i64 = 5_000_000_000;

    /// Reduce a [`SystemSnapshot`] to the app-facing view. Pure: no clock is
    /// read — staleness compares `last_rx_ts` to the snapshot's own
    /// `captured_at` against `stale_after_ns`.
    #[must_use]
    pub fn from_snapshot(snapshot: &SystemSnapshot, stale_after_ns: i64) -> Self {
        let providers = snapshot
            .providers
            .iter()
            .map(|p| ProviderHealth {
                provider: p.provider.clone(),
                state: match p.connection_state {
                    ConnectionState::Connected => ProviderState::Connected,
                    ConnectionState::Disconnected => ProviderState::Disconnected,
                    ConnectionState::Unknown => ProviderState::Connecting,
                    // Interim: Task 3 rewrites this reduction to map
                    // Unauthenticated properly (and factor in `enabled`).
                    ConnectionState::Unauthenticated => ProviderState::Unauthenticated,
                },
                detail: p.last_error.clone(),
            })
            .collect();
        let streams = snapshot
            .authoritative_sessions
            .iter()
            .map(|s| StreamHealth {
                instrument: s.instrument.clone(),
                kind: s.kind,
                liveness: match s.last_rx_ts {
                    None => Liveness::Idle,
                    Some(rx) if snapshot.captured_at.0 - rx.0 > stale_after_ns => {
                        Liveness::Stale { since: rx }
                    }
                    Some(_) => Liveness::Live,
                },
                last_event_source_ts: s.last_source_ts,
                gap_count: s.gap_count,
                latency: s.latency_ns.map(|last_ns| LatencySummary { last_ns }),
            })
            .collect();
        Self {
            schema_version: Self::SCHEMA_VERSION,
            daemon: DaemonHealth {
                version: None,
                credential_backend: None,
                captured_at: snapshot.captured_at,
            },
            providers,
            streams,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{HealthView, Liveness, ProviderState};
    use crate::{
        AssetClass, AuthoritativeSessionSnapshot, CacheSnapshot, ConnectionState, EventKind,
        Instrument, ProviderId, ProviderSnapshot, SystemSnapshot, Timestamp,
    };

    fn provider_snapshot(state: ConnectionState, last_error: Option<&str>) -> ProviderSnapshot {
        ProviderSnapshot::new(
            ProviderId::from_static("alpaca-crypto"),
            state,
            0,
            0,
            1,
            1,
            0,
            0,
            10,
            0,
            last_error.map(str::to_string),
        )
    }

    fn stream_snapshot(last_rx_ns: Option<i64>) -> AuthoritativeSessionSnapshot {
        let inst = Instrument::new(
            ProviderId::from_static("alpaca-crypto"),
            AssetClass::Crypto,
            "BTC/USD",
        );
        AuthoritativeSessionSnapshot::new(inst, EventKind::Trade, 1, 2).with_timestamps(
            last_rx_ns.map(|n| Timestamp(n - 7)),
            last_rx_ns.map(Timestamp),
        )
    }

    fn snapshot(
        providers: Vec<ProviderSnapshot>,
        sessions: Vec<AuthoritativeSessionSnapshot>,
        captured_at_ns: i64,
    ) -> SystemSnapshot {
        SystemSnapshot::new(
            Timestamp(captured_at_ns),
            providers,
            CacheSnapshot::new(Vec::new(), None),
            sessions,
            Vec::new(),
        )
    }

    #[test]
    fn provider_states_map_from_connection_state() {
        let snap = snapshot(
            vec![
                provider_snapshot(ConnectionState::Connected, None),
                provider_snapshot(ConnectionState::Disconnected, Some("ws closed")),
                provider_snapshot(ConnectionState::Unknown, None),
            ],
            vec![],
            1_000,
        );
        let view = HealthView::from_snapshot(&snap, HealthView::DEFAULT_STALE_AFTER_NS);
        assert_eq!(view.schema_version, HealthView::SCHEMA_VERSION);
        assert_eq!(view.daemon.version, None); // filled by the caller, not the reduction
        assert_eq!(view.daemon.credential_backend, None); // filled by the caller, not the reduction
        assert_eq!(view.daemon.captured_at, Timestamp(1_000));
        let states: Vec<_> = view.providers.iter().map(|p| p.state).collect();
        assert_eq!(
            states,
            vec![
                ProviderState::Connected,
                ProviderState::Disconnected,
                ProviderState::Connecting,
            ]
        );
        assert_eq!(view.providers[1].detail.as_deref(), Some("ws closed"));
    }

    #[test]
    fn liveness_is_idle_live_or_stale_per_symbol() {
        let now = 100_000_000_000_i64; // 100s in ns
        let snap = snapshot(
            vec![provider_snapshot(ConnectionState::Connected, None)],
            vec![
                stream_snapshot(None),                       // no data yet -> Idle
                stream_snapshot(Some(now - 1_000_000_000)),  // 1s ago -> Live
                stream_snapshot(Some(now - 30_000_000_000)), // 30s ago -> Stale
            ],
            now,
        );
        let view = HealthView::from_snapshot(&snap, HealthView::DEFAULT_STALE_AFTER_NS);
        assert_eq!(view.streams.len(), 3); // one entry per (instrument, kind); never aggregated
        assert_eq!(view.streams[0].liveness, Liveness::Idle);
        assert!(view.streams[0].latency.is_none());
        assert_eq!(view.streams[1].liveness, Liveness::Live);
        assert_eq!(view.streams[1].latency.map(|l| l.last_ns), Some(7));
        assert_eq!(
            view.streams[2].liveness,
            Liveness::Stale {
                since: Timestamp(now - 30_000_000_000)
            }
        );
        assert_eq!(view.streams[2].gap_count, 2);
    }

    #[test]
    fn ibkr_reserved_states_serde_round_trip() {
        // Nothing produces these in cycle 1; they are reserved for IBKR
        // (spec appendix). Guard the wire names now so shipped apps parse them
        // when cycle 4 starts emitting them.
        for (state, wire) in [
            (ProviderState::Unauthenticated, "\"unauthenticated\""),
            (
                ProviderState::CompanionUnreachable,
                "\"companion_unreachable\"",
            ),
        ] {
            let json = serde_json::to_string(&state).unwrap();
            assert_eq!(json, wire);
            let back: ProviderState = serde_json::from_str(&json).unwrap();
            assert_eq!(back, state);
        }
    }

    #[test]
    fn health_view_serde_round_trips() {
        let snap = snapshot(
            vec![provider_snapshot(ConnectionState::Connected, None)],
            vec![stream_snapshot(Some(50))],
            1_000,
        );
        let view = HealthView::from_snapshot(&snap, HealthView::DEFAULT_STALE_AFTER_NS);
        let json = serde_json::to_string(&view).unwrap();
        let back: HealthView = serde_json::from_str(&json).unwrap();
        assert_eq!(view, back);
    }
}
