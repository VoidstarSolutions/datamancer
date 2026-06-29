//! The datamancer trait surface.
//!
//! This module collects every extension point a consumer (or implementor) of
//! datamancer interacts with: provider integrations and the persistence
//! layer. Each submodule owns a single concern, with its trait(s) and the
//! parameter/return types those traits use.
//!
//! Concrete data types (`MarketEvent`, `Subscription`, `Price`, `Instrument`,
//! etc.) live in their own modules at the crate root — this module is the
//! contract surface, not the data surface.

pub mod provider;
pub mod sink;
pub mod storage;

pub use provider::{HistoryRequest, LiveHandle, Provider, ProviderMetrics};
pub use sink::{EventSink, PublishOutcome};
pub use storage::{CacheCoverage, CacheKey, HistoricalCache, ReplayRequest, ReplaySource, TapLog};
