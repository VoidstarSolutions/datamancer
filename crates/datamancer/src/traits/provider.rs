//! The Provider extension point.
//!
//! A `Provider` represents one upstream source of market data (Alpaca,
//! Polygon, IBKR, a local replay file, etc.). Datamancer holds providers
//! behind `Box<dyn Provider>` so adding a new provider is purely additive
//! at the consumer layer.
//!
//! # Hot-path discipline
//!
//! Dynamic dispatch lives at the **cold** boundary — start, subscribe,
//! unsubscribe, and history fetch. Inside a provider's running task, the
//! per-message decode loop is monomorphized: the provider owns its own
//! concrete `mpsc::Sender<MarketEvent>` and yields fully-formed events into
//! it. Consumers only see the dyn vtable when calling these cold methods or
//! when polling the merged session stream — never per websocket frame.

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::{
    error::Result,
    event::{EventKind, MarketEvent, Subscription, Timestamp},
    instrument::Instrument,
};

/// One configured upstream source of market data.
#[async_trait]
pub trait Provider: Send + Sync + 'static {
    /// Stable identifier for this provider (e.g. `"alpaca"`). Used in
    /// configuration, control events, and storage keys.
    fn id(&self) -> &str;

    /// Whether this provider can serve `kind` for `instrument`. Datamancer
    /// uses this when routing subscriptions across multiple registered
    /// providers; a provider that doesn't know the instrument should return
    /// `false` rather than fail later.
    fn supports(&self, instrument: &Instrument, kind: EventKind) -> bool;

    /// Open a live session against this provider.
    ///
    /// `sink` is the channel the provider's internal task pushes
    /// fully-formed `MarketEvent`s into; the channel is concrete (not dyn),
    /// keeping the per-message path monomorphized inside the provider crate.
    /// Datamancer assigns final `seq` values downstream — the provider's job
    /// is to surface ordered, decoded events.
    async fn start_live(&self, sink: mpsc::Sender<MarketEvent>) -> Result<Box<dyn LiveHandle>>;

    /// Fetch a bounded historical range, pushing events into `sink` in
    /// source-timestamp order. Returns once the range is exhausted; pagination
    /// and rate-limit handling are the provider's responsibility.
    async fn fetch_history(&self, request: HistoryRequest, sink: mpsc::Sender<MarketEvent>) -> Result<()>;
}

/// A handle to a running live provider session. Subscription mutation and
/// shutdown go through this handle.
#[async_trait]
pub trait LiveHandle: Send + Sync {
    /// Activate `sub` against the live session. Should return once the
    /// provider has acknowledged the change (or surfaces the result via a
    /// `ControlKind::SubscriptionChanged` entry on the event sink).
    async fn subscribe(&self, sub: Subscription) -> Result<()>;

    /// Deactivate `sub`. Symmetric with `subscribe`.
    async fn unsubscribe(&self, sub: Subscription) -> Result<()>;

    /// Tear down the live connection. Implementations should drop the event
    /// sink after final teardown so the consuming session sees a clean EOF.
    async fn close(self: Box<Self>) -> Result<()>;
}

/// Bounded request used by [`Provider::fetch_history`].
#[derive(Debug, Clone, PartialEq)]
pub struct HistoryRequest {
    pub instrument: Instrument,
    pub kind: EventKind,
    pub from: Timestamp,
    pub to: Timestamp,
}
