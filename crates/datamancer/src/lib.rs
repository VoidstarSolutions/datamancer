//! Datamancer — a unified subscription and replay layer for financial market
//! data.
//!
//! Datamancer talks to whatever providers it's configured against, normalizes
//! their messages into typed [`MarketEvent`]s, and presents them through a
//! multiplexed [`ClientSession`] stream that downstream consumers (analysis
//! engines, persistence sinks, UIs) consume without caring which provider any
//! given event came from. Ordering is **per symbol** (`(instrument, seq)`,
//! source-stamped within each instrument; arrival-order across instruments) —
//! the multiplex interleaves rather than computing a global order.
//!
//! See `README.md` in this crate for the design rationale and intended scope.
//! Provider and storage implementations live in sibling crates and depend on
//! [`datamancer-core`](datamancer_core) — the types and trait surface this
//! crate re-exports — without pulling in the session orchestrator below.
//!
//! # Layering
//!
//! - [`Price`] and [`Instrument`] — primitive types every event uses.
//! - [`MarketEvent`] and friends — the public output enum.
//! - [`traits`] — every extension point: [`Provider`] and [`LiveHandle`] for
//!   upstream sources (dyn-dispatched at the cold boundary; per-message
//!   decode loops stay monomorphic inside each provider), and [`TapLog`],
//!   [`HistoricalCache`], [`ReplaySource`] for the persistence layer.
//! - [`Datamancer`] / [`ClientSession`] / [`Session`] — orchestrator, the
//!   primary multiplexing consumer handle, and the single-pair handle.

#![forbid(unsafe_code)]

mod accounting;
#[path = "client.rs"]
mod client_session;
mod fetch_locks;
pub mod providers;
mod session;
pub mod storage;

pub use client_session::ClientSession;
pub use datamancer_core::traits;
pub use datamancer_core::{
    Adjustment, AssetClass, AuthoritativeSessionSnapshot, Bar, BarInterval, CacheCatalogEntry,
    CacheCoverage, CacheKey, CacheSnapshot, ClientSessionId, ClientSessionSnapshot,
    ConnectionState, Control, ControlKind, DaemonHealth, DisconnectCause, Error, EventKind,
    GapSpan, HealthView, HistoricalCache, HistoryRequest, Instrument, InstrumentCapabilities,
    InstrumentEntry, InstrumentInfo, LatencySummary, LiveHandle, Liveness, MarketEvent, Price,
    Provider, ProviderHealth, ProviderId, ProviderMetrics, ProviderSnapshot, ProviderState,
    Quantity, Quote, ReplayRequest, ReplaySource, Result, ResumeBufferSnapshot, Seq, StreamHealth,
    SubscriptionRef, Surface, SystemSnapshot, TapLog, Timestamp, Trade,
};
#[cfg(feature = "provider-alpaca")]
pub use providers::{
    AlpacaCredentials, AlpacaCryptoSettings, AlpacaSettings, CredentialsSource, SettingsSource,
};
pub use session::{
    Datamancer, DatamancerBuilder, EventStream, PersistenceOptions, ReconnectPolicy, Scope, Session,
};

/// iceoryx2 zero-copy transport (data + diagnostics planes), gated behind the
/// `transport-iceoryx2` feature. Re-exports the
/// [`datamancer-transport-iceoryx2`](datamancer_transport_iceoryx2) crate so
/// embedders enable the transport through this single feature flag. The sink is
/// an additional [`EventSink`](datamancer_core::EventSink) selected like any
/// other; `SymbolId`/interning stay sink-local (never core).
#[cfg(feature = "transport-iceoryx2")]
pub mod transport {
    pub use datamancer_transport_iceoryx2::*;
}

/// WebSocket client transport, gated behind the `transport-ws` feature.
/// Re-exports the
/// [`datamancer-transport-ws`](datamancer_transport_ws) crate so
/// embedders enable the transport through this single feature flag. The sink is
/// an additional [`EventSink`](datamancer_core::EventSink) selected like any
/// other; `SymbolId`/interning stay sink-local (never core).
#[cfg(feature = "transport-ws")]
pub mod transport_ws {
    pub use datamancer_transport_ws::*;
}

/// Consumer-side client crate, gated behind the `client-ws` / `client-iceoryx2`
/// features. Re-exports [`datamancer-client`](datamancer_client) — the
/// generic [`Client`](datamancer_client::Client) trait plus the shared control
/// vocabulary (`spec`, `codes`, `protocol`) — so embedders pull in a consumer
/// client through the same feature-flag pattern as the transport re-exports
/// above, without depending on `datamancer-client` directly.
#[cfg(any(feature = "client-ws", feature = "client-iceoryx2"))]
pub use datamancer_client as client;
