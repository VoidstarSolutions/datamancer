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
//! This module exposes the public type surface; per-provider integrations and
//! storage backends live in sibling crates (or, for now, behind the
//! [`Provider`], [`TapLog`], and [`HistoricalCache`] traits).
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

mod error;
mod event;
mod instrument;
mod price;
mod session;
pub mod traits;

pub use error::{Error, Result};
pub use event::{
    Bar, BarInterval, Control, ControlKind, EventKind, GapSpan, MarketEvent, Quote, Seq,
    Subscription, Timestamp, Trade,
};
pub use instrument::Instrument;
pub use price::Price;
pub use session::{
    Datamancer, DatamancerBuilder, EventStream, LiveConfig, ReconnectPolicy, ReplayConfig,
    ReplaySourceSpec, Session, StitchConfig,
};
pub use traits::{
    CacheCoverage, CacheKey, HistoricalCache, HistoryRequest, LiveHandle, Provider, ReplayRequest,
    ReplaySource, TapLog,
};
