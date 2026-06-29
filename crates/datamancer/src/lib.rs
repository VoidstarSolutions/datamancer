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
mod client;
mod fetch_locks;
pub mod providers;
mod session;
pub mod storage;

pub use client::ClientSession;
pub use datamancer_core::traits;
pub use datamancer_core::{
    Adjustment, AssetClass, Bar, BarInterval, CacheCatalogEntry, CacheCoverage, CacheKey,
    ClientSessionId, ConnectionState, Control, ControlKind, Error, EventKind, GapSpan,
    HistoricalCache, HistoryRequest, Instrument, LiveHandle, MarketEvent, Price, Provider,
    ProviderId, ProviderMetrics, Quote, ReplayRequest, ReplaySource, Result, Seq, TapLog, Timestamp,
    Trade,
};
pub use session::{
    Datamancer, DatamancerBuilder, EventStream, PersistenceOptions, ReconnectPolicy, Scope, Session,
};
