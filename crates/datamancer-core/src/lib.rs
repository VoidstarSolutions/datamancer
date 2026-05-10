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

mod error;
mod event;
mod instrument;
mod price;
pub mod traits;

pub use error::{Error, Result};
pub use event::{
    Bar, BarInterval, Control, ControlKind, EventKind, GapSpan, MarketEvent, Quote, Seq, Timestamp,
    Trade,
};
pub use instrument::Instrument;
pub use price::Price;
pub use traits::{
    CacheCoverage, CacheKey, HistoricalCache, HistoryRequest, LiveHandle, Provider, ReplayRequest,
    ReplaySource, TapLog,
};
