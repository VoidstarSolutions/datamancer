//! Datamancer — a unified subscription and replay layer for financial market
//! data.
//!
//! Datamancer talks to whatever providers it's configured against, normalizes
//! their messages into typed [`MarketEvent`]s, and produces a single ordered
//! event stream that downstream consumers (analysis engines, persistence
//! sinks, UIs) consume without caring which provider any given event came
//! from.
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
//! - [`Datamancer`] / [`Session`] — orchestrator and session handle.

#![forbid(unsafe_code)]

mod fetch_locks;
pub mod providers;
mod session;
pub mod storage;

pub use datamancer_core::traits;
pub use datamancer_core::{
    Adjustment, AssetClass, Bar, BarInterval, CacheCoverage, CacheKey, ClientSessionId, Control,
    ControlKind, Error, EventKind, GapSpan, HistoricalCache, HistoryRequest, Instrument, LiveHandle,
    MarketEvent, Price, Provider, ProviderId, Quote, ReplayRequest, ReplaySource, Result, Seq,
    TapLog, Timestamp, Trade,
};
pub use session::{
    Datamancer, DatamancerBuilder, EventStream, PersistenceOptions, ReconnectPolicy, Scope, Session,
};
