//! Core types and trait surface for datamancer.
//!
//! This crate is everything a provider or storage backend needs in order to
//! integrate with datamancer: the event model, instrument identity, price
//! representation, error type, and the [`Provider`] / [`TapLog`] /
//! [`HistoricalCache`] / [`ReplaySource`] traits.
//!
//! Application code normally depends on the `datamancer` crate, which
//! re-exports this surface and adds the session orchestrator. Provider and
//! storage *implementation* crates depend only on `datamancer-core` so they
//! can ship without pulling the orchestrator into their dependency graph.

#![forbid(unsafe_code)]

mod adjustment;
mod error;
mod event;
mod instrument;
mod price;
mod quantity;
mod snapshot;
pub mod traits;

pub use adjustment::Adjustment;
pub use error::{Error, Result};
pub use event::{
    Bar, BarInterval, Control, ControlKind, EventKind, GapSpan, MarketEvent, Quote, Seq, Timestamp,
    Trade,
};
pub use instrument::{AssetClass, Instrument, InstrumentInfo, ProviderId};
pub use price::Price;
pub use quantity::Quantity;
pub use snapshot::{
    AuthoritativeSessionSnapshot, CacheSnapshot, ClientSessionId, ClientSessionSnapshot,
    ConnectionState, ProviderSnapshot, ResumeBufferSnapshot, SubscriptionRef, SystemSnapshot,
};
pub use traits::{
    CacheCatalogEntry, CacheCoverage, CacheKey, EventSink, HistoricalCache, HistoryRequest,
    LiveHandle, Provider, ProviderMetrics, PublishOutcome, ReplayRequest, ReplaySource, TapLog,
};
