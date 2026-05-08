//! Sessions and the top-level [`Datamancer`] orchestrator.
//!
//! A session is the unit of consumption. Three constructors —
//! [`Datamancer::live`], [`Datamancer::replay`], [`Datamancer::stitched`] —
//! all return the same [`Session`] type, so consumers don't have to change
//! shape based on the data origin.
//!
//! # Internal shape (planned)
//!
//! The session is a thin handle over a controller task that owns the
//! provider [`LiveHandle`]s and merges their per-provider event streams into
//! one ordered output. Communication from the public handle to the controller
//! goes through a typed command channel; events flow back through a single
//! `mpsc::Receiver<MarketEvent>` that the [`EventStream`] wraps.
//!
//! # Hot-path discipline
//!
//! Each provider task pushes into its own concrete `mpsc::Sender<MarketEvent>`,
//! avoiding dyn dispatch on the per-message decode loop. The merger reads
//! from concrete `Receiver`s, tags each event with a session-monotonic
//! [`Seq`](crate::Seq), and forwards into the consumer-facing channel. The
//! only remaining dyn calls are at session construction (one per provider)
//! and at subscription-mutation time.

use std::sync::Arc;

use datamancer_core::{
    Error, EventKind, HistoricalCache, Instrument, MarketEvent, Provider, Result, Subscription,
    TapLog, Timestamp,
};
use futures::stream::Stream;
use tokio::sync::mpsc;

/// Top-level entry point. Owns provider instances and optional persistence,
/// then hands them to per-session controllers.
///
/// Construct via [`DatamancerBuilder`]; once built, `Datamancer` is `Arc`-
/// shareable across tasks.
#[derive(Clone)]
pub struct Datamancer {
    inner: Arc<DatamancerInner>,
}

struct DatamancerInner {
    providers: Vec<Box<dyn Provider>>,
    tap_log: Option<Box<dyn TapLog>>,
    historical_cache: Option<Box<dyn HistoricalCache>>,
}

impl Datamancer {
    pub fn builder() -> DatamancerBuilder {
        DatamancerBuilder::default()
    }

    /// Open a live session against the configured providers.
    pub async fn live(&self, _cfg: LiveConfig) -> Result<Session> {
        // Per provider:
        //   1. Allocate a concrete mpsc channel.
        //   2. Call provider.start_live(sender) to get a LiveHandle.
        //   3. Spawn a merger task that drains the receiver, assigns seq,
        //      optionally tees to tap_log, and forwards into the session
        //      output channel.
        // Then: spawn a controller task that owns the LiveHandles and the
        // command receiver, and return a Session handle.
        todo!("wire provider.start_live + merger + controller")
    }

    /// Open a replay session over a previously-captured source.
    ///
    /// Replay sessions fix their subscription set at construction — that set
    /// is part of what defines a reproducible analysis — so [`Session::subscribe`]
    /// returns an error on a replay session.
    pub async fn replay(&self, _cfg: ReplayConfig) -> Result<Session> {
        // Open the ReplaySource, spawn a forwarder that pushes into the
        // session output channel, return a Session whose controller rejects
        // subscription mutation.
        todo!("wire ReplaySource → events forwarder")
    }

    /// Open a stitched session: backfill from the configured replay window,
    /// then continue live. Any gap or overlap at the seam is reported in-band
    /// as a `ControlKind::Gap` entry.
    pub async fn stitched(&self, _cfg: StitchConfig) -> Result<Session> {
        // Sequence:
        //   1. Open the ReplaySource for the backfill window; drain into the
        //      session output, recording the last source_ts seen.
        //   2. Start live providers; on first live event, compare its
        //      source_ts against the recorded backfill end and emit a Gap
        //      Control event if there is a discontinuity.
        //   3. Continue forwarding live events.
        todo!("wire backfill → seam handling → live")
    }

    /// Look up a registered provider by id. Returns `Err(UnknownProvider)`
    /// if no such provider was registered with the builder.
    pub fn provider(&self, id: &str) -> Result<&dyn Provider> {
        self.inner
            .providers
            .iter()
            .find(|p| p.id() == id)
            .map(|p| p.as_ref())
            .ok_or_else(|| Error::UnknownProvider(id.to_string()))
    }
}

#[derive(Default)]
pub struct DatamancerBuilder {
    providers: Vec<Box<dyn Provider>>,
    tap_log: Option<Box<dyn TapLog>>,
    historical_cache: Option<Box<dyn HistoricalCache>>,
}

impl DatamancerBuilder {
    /// Register a provider. Provider ids must be unique within a Datamancer
    /// instance; conflicts surface from [`build`](Self::build).
    pub fn provider(mut self, p: Box<dyn Provider>) -> Self {
        self.providers.push(p);
        self
    }

    /// Attach a tap log; every event a live session emits will be appended.
    pub fn tap_log(mut self, log: Box<dyn TapLog>) -> Self {
        self.tap_log = Some(log);
        self
    }

    /// Attach a historical cache; `fetch_history` calls will read-through and
    /// write-through this cache before hitting the upstream provider.
    pub fn historical_cache(mut self, cache: Box<dyn HistoricalCache>) -> Self {
        self.historical_cache = Some(cache);
        self
    }

    pub fn build(self) -> Result<Datamancer> {
        let mut ids: Vec<&str> = self.providers.iter().map(|p| p.id()).collect();
        ids.sort();
        if let Some(dup) = ids.windows(2).find(|w| w[0] == w[1]) {
            return Err(Error::Config(format!("duplicate provider id: {}", dup[0])));
        }
        Ok(Datamancer {
            inner: Arc::new(DatamancerInner {
                providers: self.providers,
                tap_log: self.tap_log,
                historical_cache: self.historical_cache,
            }),
        })
    }
}

/// A consumer-facing handle to a running session.
///
/// Held separately from the [`EventStream`] so that subscription mutation and
/// stream consumption can happen on independent tasks without interior
/// mutability tricks at the call site.
pub struct Session {
    cmd_tx: mpsc::Sender<SessionCommand>,
    events: Option<EventStream>,
    kind: SessionKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionKind {
    Live,
    Replay,
    Stitched,
}

impl Session {
    /// Take the event stream. Can only be called once per session; subsequent
    /// calls return `Err(EventsAlreadyTaken)`.
    pub fn take_events(&mut self) -> Result<EventStream> {
        self.events.take().ok_or(Error::EventsAlreadyTaken)
    }

    /// Add a subscription to the active set. Errors on a replay session
    /// (whose subscription set is fixed at construction).
    pub async fn subscribe(&self, sub: Subscription) -> Result<()> {
        if matches!(self.kind, SessionKind::Replay) {
            return Err(Error::Config(
                "replay sessions fix subscriptions at construction".into(),
            ));
        }
        self.cmd_tx
            .send(SessionCommand::Subscribe(sub))
            .await
            .map_err(|_| Error::SessionClosed)
    }

    /// Remove a subscription from the active set. Same constraint as
    /// `subscribe` for replay sessions.
    pub async fn unsubscribe(&self, sub: Subscription) -> Result<()> {
        if matches!(self.kind, SessionKind::Replay) {
            return Err(Error::Config(
                "replay sessions fix subscriptions at construction".into(),
            ));
        }
        self.cmd_tx
            .send(SessionCommand::Unsubscribe(sub))
            .await
            .map_err(|_| Error::SessionClosed)
    }

    /// Explicitly tear the session down. Necessary (rather than relying on
    /// drop) so persistence sinks have a deterministic flush point.
    pub async fn close(self) -> Result<()> {
        let _ = self.cmd_tx.send(SessionCommand::Close).await;
        Ok(())
    }
}

/// Internal command channel between [`Session`] and its controller task.
#[derive(Debug)]
enum SessionCommand {
    Subscribe(Subscription),
    Unsubscribe(Subscription),
    Close,
}

/// The session's output stream.
///
/// `Stream<Item = MarketEvent>`. Wraps a concrete `mpsc::Receiver` so the
/// per-event poll path is monomorphic.
pub struct EventStream {
    rx: mpsc::Receiver<MarketEvent>,
}

impl EventStream {
    pub(crate) fn new(rx: mpsc::Receiver<MarketEvent>) -> Self {
        Self { rx }
    }
}

impl Stream for EventStream {
    type Item = MarketEvent;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for [`Datamancer::live`].
#[derive(Debug, Clone, Default)]
pub struct LiveConfig {
    /// Initial subscriptions to request once the session is open. Live
    /// sessions can mutate this set later via [`Session::subscribe`].
    pub initial_subscriptions: Vec<Subscription>,

    /// Per-instrument provider mapping. Empty means "any registered provider
    /// that supports the requested kind" — datamancer picks deterministically
    /// (by registration order). Populate this to pin specific instruments to
    /// specific providers.
    pub instrument_provider: Vec<(Instrument, String)>,

    /// Reconnect/retry policy, applied per provider. `None` uses the
    /// provider's own default.
    pub reconnect: Option<ReconnectPolicy>,

    /// Bounded buffer size for the session output channel. Once exceeded,
    /// backpressure propagates to providers; behavior beyond that is per the
    /// provider implementation.
    pub buffer_size: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct ReconnectPolicy {
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub jitter: bool,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            initial_backoff_ms: 500,
            max_backoff_ms: 30_000,
            jitter: true,
        }
    }
}

/// Configuration for [`Datamancer::replay`].
#[derive(Debug, Clone)]
pub struct ReplayConfig {
    pub source: ReplaySourceSpec,
    pub instruments: Vec<Instrument>,
    pub kinds: Vec<EventKind>,
    pub from: Timestamp,
    pub to: Timestamp,
}

/// Where a replay session's events come from.
#[derive(Debug, Clone)]
pub enum ReplaySourceSpec {
    /// Read from the configured tap log.
    TapLog,
    /// Read from the configured historical cache.
    HistoricalCache,
    /// Pull historical data live from the named provider; events flow through
    /// the historical cache (write-through) if one is configured.
    Provider { id: String },
}

/// Configuration for [`Datamancer::stitched`].
#[derive(Debug, Clone)]
pub struct StitchConfig {
    pub backfill: ReplayConfig,
    pub live: LiveConfig,
}
