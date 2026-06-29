//! Introspection identifiers and the system-state snapshot type surface,
//! shared across the orchestrator and (later) the diagnostics plane and UI.
//!
//! These types live in `datamancer-core` so both crates — and any future
//! transport crate — share them and they are serde-capable. The **assembly**
//! logic (reaching into providers, cache, and the session registry) lives in
//! `datamancer`; this module is pure data.
//!
//! # Consistency contract
//!
//! A [`SystemSnapshot`] is a **sampled** point-in-time view, not a transactional
//! one: per-symbol fields are read from `Relaxed` atomics and the registry lock
//! is held only long enough to clone handles. Fields may skew across symbols by
//! nanoseconds — acceptable because the snapshot is diagnostic and determinism
//! is **per-symbol** (cross-symbol consistency is a non-goal).
//!
//! `rx_ts` / `latency_ns` are **observability only** (per CLAUDE.md): the
//! latency field is exactly that sanctioned use and must never feed engine
//! logic. The snapshot implies no cross-instrument ordering.

use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use crate::{
    event::{EventKind, Seq, Timestamp},
    instrument::{Instrument, ProviderId},
    traits::storage::CacheCatalogEntry,
};

/// Process-scoped identity for a multiplexing client session.
///
/// Allocated from a monotonic process-global counter via [`ClientSessionId::next`];
/// never persisted and not meaningful across processes. Phase 3 surfaces it in
/// the live-state snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClientSessionId(pub u64);

impl ClientSessionId {
    /// Allocate the next process-global client-session id.
    #[must_use]
    pub fn next() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        Self(COUNTER.fetch_add(1, Ordering::Relaxed))
    }
}

/// A provider's connection state, derived purely from in-band
/// [`crate::Control`] events.
///
/// `Unknown` is the initial state before any connection event is observed.
/// There is no `Reconnecting` variant: the `Control` model exposes only
/// connected/disconnected, and a `ProviderDisconnected` already documents that
/// a reconnect is scheduled or in flight.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionState {
    Unknown,
    Connected,
    Disconnected,
}

/// A consolidated, serializable view of datamancer's runtime state: provider
/// accounting, the cache catalog, and per-symbol live state (authoritative +
/// client sessions). The single artifact consumed by the in-process embedder
/// ([`crate`] re-exports the assembler), the diagnostics plane, and the UI.
///
/// All aggregate types are `#[non_exhaustive]` so later phases can add fields
/// without a breaking change (forward-compat = add optional fields only).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SystemSnapshot {
    /// Wall-clock at assembly (observability).
    pub captured_at: Timestamp,
    pub providers: Vec<ProviderSnapshot>,
    pub cache: CacheSnapshot,
    pub authoritative_sessions: Vec<AuthoritativeSessionSnapshot>,
    pub client_sessions: Vec<ClientSessionSnapshot>,
}

/// Per-provider call/throughput accounting.
///
/// Call counts are **not** active-subscription deltas (stock subscribe is a
/// full-snapshot and a reconnect re-applies the full list); `messages` counts
/// live data forwarded to consumers only (not cache-replay/backfill);
/// `history_fetches` counts per gap *segment*, not per `session()` call.
/// `bytes`/`rate_limit_hits` are `None` until a provider's
/// [`crate::ProviderMetrics`] hook reports them.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderSnapshot {
    pub provider: ProviderId,
    pub connection_state: ConnectionState,
    pub history_fetches: u64,
    pub history_fetch_coalesced: u64,
    pub live_starts: u64,
    pub subscribes: u64,
    pub unsubscribes: u64,
    pub reconnects: u64,
    pub rate_limit_hits: Option<u64>,
    pub messages: u64,
    pub bytes: Option<u64>,
    pub gaps_emitted: u64,
    pub last_error: Option<String>,
}

/// The cache catalog plus an optional whole-store footprint.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CacheSnapshot {
    pub entries: Vec<CacheCatalogEntry>,
    /// Whole-store on-disk footprint, if computed (a filesystem walk of a
    /// file-backed cache). `None` for non-file backends (e.g. in-memory).
    pub total_disk_bytes: Option<u64>,
}

/// Per-`(instrument, kind)` authoritative live state. Sampled; per-symbol only.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuthoritativeSessionSnapshot {
    pub instrument: Instrument,
    pub kind: EventKind,
    /// Number of client referrers onto this authoritative session.
    pub subscriber_refcount: u32,
    /// Last-assigned per-symbol source `seq`, or `None` before any event.
    pub seq_position: Option<Seq>,
    pub last_source_ts: Option<Timestamp>,
    pub last_rx_ts: Option<Timestamp>,
    /// Last `rx_ts - source_ts` (observability only). It straddles two clocks —
    /// the provider's `source_ts` and our wall-clock `rx_ts` — so it is **signed
    /// and may be negative** when the local clock lags the provider's (clock
    /// skew). It is not a pure network latency and must never feed engine logic.
    pub latency_ns: Option<i64>,
    /// Per-symbol provider/source `Control::Gap` count. Per-client resume-buffer
    /// drops live on [`ClientSessionSnapshot`] instead.
    pub gap_count: u64,
}

/// One client session's per-client resume buffer occupancy.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResumeBufferSnapshot {
    pub capacity: usize,
    pub occupancy: usize,
    /// Cumulative events this client missed to overflow eviction.
    pub dropped_events: u64,
}

/// One active multiplexing client session.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClientSessionSnapshot {
    pub id: ClientSessionId,
    pub subscriptions: Vec<SubscriptionRef>,
    pub resume_buffer: ResumeBufferSnapshot,
}

/// One `(instrument, kind)` subscription a client session holds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubscriptionRef {
    pub instrument: Instrument,
    pub kind: EventKind,
}

// Constructors. The snapshot aggregates are `#[non_exhaustive]` (forward-compat
// for the diagnostics plane / UI), so the in-workspace assembler in
// `datamancer` builds them through these rather than struct literals.

impl SystemSnapshot {
    #[must_use]
    pub fn new(
        captured_at: Timestamp,
        providers: Vec<ProviderSnapshot>,
        cache: CacheSnapshot,
        authoritative_sessions: Vec<AuthoritativeSessionSnapshot>,
        client_sessions: Vec<ClientSessionSnapshot>,
    ) -> Self {
        Self {
            captured_at,
            providers,
            cache,
            authoritative_sessions,
            client_sessions,
        }
    }
}

impl ProviderSnapshot {
    /// Construct from the counts datamancer tracks. `rate_limit_hits` and
    /// `bytes` are `None` unless a provider's metrics hook reports them; set
    /// them with [`with_rate_limit_hits`](Self::with_rate_limit_hits) /
    /// [`with_bytes`](Self::with_bytes).
    #[must_use]
    #[allow(
        clippy::too_many_arguments,
        reason = "flat accounting record; one in-workspace call site"
    )]
    pub fn new(
        provider: ProviderId,
        connection_state: ConnectionState,
        history_fetches: u64,
        history_fetch_coalesced: u64,
        live_starts: u64,
        subscribes: u64,
        unsubscribes: u64,
        reconnects: u64,
        messages: u64,
        gaps_emitted: u64,
        last_error: Option<String>,
    ) -> Self {
        Self {
            provider,
            connection_state,
            history_fetches,
            history_fetch_coalesced,
            live_starts,
            subscribes,
            unsubscribes,
            reconnects,
            rate_limit_hits: None,
            messages,
            bytes: None,
            gaps_emitted,
            last_error,
        }
    }

    /// Set the provider-reported rate-limit-hit count.
    #[must_use]
    pub fn with_rate_limit_hits(mut self, hits: Option<u64>) -> Self {
        self.rate_limit_hits = hits;
        self
    }

    /// Set the provider-reported byte throughput.
    #[must_use]
    pub fn with_bytes(mut self, bytes: Option<u64>) -> Self {
        self.bytes = bytes;
        self
    }
}

impl CacheSnapshot {
    #[must_use]
    pub fn new(entries: Vec<CacheCatalogEntry>, total_disk_bytes: Option<u64>) -> Self {
        Self {
            entries,
            total_disk_bytes,
        }
    }
}

impl AuthoritativeSessionSnapshot {
    /// Construct from required identity + refcount + gap count. The sampled
    /// `LiveStats` fields (`seq_position`, timestamps, `latency_ns`) default to
    /// `None`; set them with the `with_*` builders.
    #[must_use]
    pub fn new(
        instrument: Instrument,
        kind: EventKind,
        subscriber_refcount: u32,
        gap_count: u64,
    ) -> Self {
        Self {
            instrument,
            kind,
            subscriber_refcount,
            seq_position: None,
            last_source_ts: None,
            last_rx_ts: None,
            latency_ns: None,
            gap_count,
        }
    }

    /// Set the last-assigned per-symbol source `seq`.
    #[must_use]
    pub fn with_seq_position(mut self, seq: Option<Seq>) -> Self {
        self.seq_position = seq;
        self
    }

    /// Set the last data-event timestamps and derive `latency_ns` from them.
    #[must_use]
    pub fn with_timestamps(
        mut self,
        last_source_ts: Option<Timestamp>,
        last_rx_ts: Option<Timestamp>,
    ) -> Self {
        self.last_source_ts = last_source_ts;
        self.last_rx_ts = last_rx_ts;
        self.latency_ns = match (last_source_ts, last_rx_ts) {
            (Some(s), Some(r)) => Some(r.0 - s.0),
            _ => None,
        };
        self
    }
}

impl ResumeBufferSnapshot {
    #[must_use]
    pub fn new(capacity: usize, occupancy: usize, dropped_events: u64) -> Self {
        Self {
            capacity,
            occupancy,
            dropped_events,
        }
    }
}

impl ClientSessionSnapshot {
    #[must_use]
    pub fn new(
        id: ClientSessionId,
        subscriptions: Vec<SubscriptionRef>,
        resume_buffer: ResumeBufferSnapshot,
    ) -> Self {
        Self {
            id,
            subscriptions,
            resume_buffer,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AuthoritativeSessionSnapshot, CacheSnapshot, ClientSessionId, ClientSessionSnapshot,
        ConnectionState, ProviderSnapshot, ResumeBufferSnapshot, SubscriptionRef, SystemSnapshot,
    };
    use crate::{
        Adjustment, AssetClass, BarInterval, CacheCatalogEntry, EventKind, GapSpan, Instrument,
        ProviderId, Seq, Timestamp,
    };

    #[test]
    fn ids_are_monotonic_and_distinct() {
        let a = ClientSessionId::next();
        let b = ClientSessionId::next();
        assert_ne!(a, b);
        assert!(b.0 > a.0);
    }

    #[test]
    fn snapshot_serde_round_trips() {
        let inst = Instrument::new(ProviderId::from_static("p"), AssetClass::Equity, "AAPL");
        let snapshot = SystemSnapshot {
            captured_at: Timestamp(1_700_000_000),
            providers: vec![ProviderSnapshot {
                provider: ProviderId::from_static("p"),
                connection_state: ConnectionState::Connected,
                history_fetches: 3,
                history_fetch_coalesced: 1,
                live_starts: 2,
                subscribes: 4,
                unsubscribes: 1,
                reconnects: 1,
                rate_limit_hits: Some(5),
                messages: 99,
                bytes: Some(4096),
                gaps_emitted: 2,
                last_error: Some("boom".to_string()),
            }],
            cache: CacheSnapshot {
                entries: vec![
                    CacheCatalogEntry::new(
                        ProviderId::from_static("p"),
                        "AAPL".to_string(),
                        EventKind::Bar(BarInterval::OneMinute),
                        Adjustment::All,
                        vec![GapSpan {
                            from_source_ts: Timestamp(0),
                            to_source_ts: Timestamp(100),
                        }],
                        10,
                    )
                    .with_asset_class(Some(AssetClass::Equity))
                    .with_est_bytes(Some(560)),
                ],
                total_disk_bytes: None,
            },
            authoritative_sessions: vec![AuthoritativeSessionSnapshot {
                instrument: inst.clone(),
                kind: EventKind::Trade,
                subscriber_refcount: 2,
                seq_position: Some(Seq(7)),
                last_source_ts: Some(Timestamp(123)),
                last_rx_ts: Some(Timestamp(130)),
                latency_ns: Some(7),
                gap_count: 1,
            }],
            client_sessions: vec![ClientSessionSnapshot {
                id: ClientSessionId(42),
                subscriptions: vec![SubscriptionRef {
                    instrument: inst,
                    kind: EventKind::Trade,
                }],
                resume_buffer: ResumeBufferSnapshot {
                    capacity: 1024,
                    occupancy: 3,
                    dropped_events: 0,
                },
            }],
        };
        let json = serde_json::to_string(&snapshot).unwrap();
        let back: SystemSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(snapshot, back);
    }
}
