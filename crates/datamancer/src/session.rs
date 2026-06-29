//! Sessions and the top-level [`Datamancer`] orchestrator.
//!
//! A [`Session`] is scoped to exactly one `(instrument, kind)` pair and one
//! [`Scope`] — bounded historical, pure live, or live with a historical
//! backfill. Construction is eager: provider subscription / fetch starts
//! immediately so the session begins capturing events even if the consumer
//! hasn't taken the [`EventStream`] yet.
//!
//! The multiplexing handle is [`crate::ClientSession`] (a mutable subscription
//! set behind one interleaved stream; see `client.rs`). Both it and the live
//! [`Session`] are refcounted **referrers** onto a shared per-`(instrument,
//! kind)` authoritative session, which owns the provider connection and stamps
//! the per-symbol `seq` once at the source before any sink. `seq` is therefore
//! identical across every consumer of a symbol; the multiplex orders by
//! `(instrument, seq)` and never re-stamps. Historical sessions keep the
//! single-consumer controller and do not participate in the registry.
//!
//! # Lifecycle
//!
//! [`Session::take_events`] is **multi-shot for live scope**: dropping the
//! stream detaches the consumer while the session keeps running (and
//! recording, when configured); a later call re-attaches, first surfacing a
//! single [`ControlKind::Gap`] for anything the bounded resume buffer had to
//! evict. The `Session` handle anchors a live session's lifetime — dropping
//! it (or calling [`Session::close`]) tears the session down even while a
//! stream is held. Historical sessions stay single-shot and fetch-anchored.
//!
//! `Scope::Live { backfill_from: Some(t) }` runs a real backfill over
//! `[t, B)` (B = the wall-clock live edge at session start) through the
//! historical read-through machinery, buffering live arrivals and splicing
//! them in after the backfill output. A healthy seam emits no synthetic
//! control; `Gap` appears only for real, known loss (fetch failure, buffer
//! overflow).
//!
//! Recording (write-through to persistence) is a separate axis. It defaults
//! to whatever was passed at construction and can be toggled at runtime via
//! [`Session::set_persistence`].
//!
//! # Auto-cleanup
//!
//! - **Live**: alive while the `Session` handle is held. Dropping the handle
//!   unsubscribes upstream and shuts down; dropping just the `EventStream`
//!   only detaches the consumer.
//! - **Historical**: alive while the fetch is running. After the fetch
//!   completes the controller waits for the held stream to drain (or drop)
//!   and then shuts down. If the consumer never took the stream, the
//!   session terminates immediately when the fetch finishes — there's
//!   nobody to drain to.
//!
//! Explicit [`Session::close`] is always available for forced termination.

use crate::accounting::ProviderAccounting;
use crate::client::{
    AuthoritativeSession, ClientHandle, ClientSession, FanOut, LiveStats, SubscriberGuard,
    SubscriberId, spawn_client,
};
use crate::fetch_locks::FetchLocks;

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use async_trait::async_trait;
use datamancer_core::{
    Adjustment, Bar, CacheKey, Control, ControlKind, Error, EventKind, EventSink, GapSpan,
    HistoricalCache, HistoryRequest, Instrument, LiveHandle, MarketEvent, Provider, PublishOutcome,
    Quote, ReplayRequest, Result, Seq, TapLog, Timestamp, Trade,
};
use futures::StreamExt;
use futures::stream::Stream;
use tokio::sync::{Mutex, mpsc, oneshot};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// What data a session covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    /// Bounded source-time range. The session's stream completes when `to`
    /// is reached.
    Historical { from: Timestamp, to: Timestamp },
    /// Unbounded live subscription. With `backfill_from = Some(t)`, the
    /// session fetches history from `t` up to the live edge and seams into
    /// the live tail; with `None`, it starts purely from "now."
    Live { backfill_from: Option<Timestamp> },
}

/// How a session interacts with the configured persistence layer.
///
/// The two cache axes compose into the full historical option space; the
/// `write_tap_log` axis is orthogonal and governs live capture only:
///
/// | `read_cache` | `write_cache` | mode      | behavior                                    |
/// |--------------|---------------|-----------|---------------------------------------------|
/// | `false`      | `false`       | ephemeral | always hit the provider, store nothing      |
/// | `true`       | `true`        | cached    | serve covered ranges, fetch & store gaps    |
/// | `true`       | `false`       | read-only | serve cache + fetch gaps, don't persist     |
/// | `false`      | `true`        | refresh   | ignore coverage, re-fetch range, overwrite  |
///
/// `write_tap_log` is independent of scope mode above: when set on a `Live`
/// session, every data event is teed to the configured [`crate::TapLog`].
///
/// `#[non_exhaustive]`: later work (resume) adds axes additively. Construct via
/// the presets and `with_tap_log`, or mutate the public fields on an owned value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct PersistenceOptions {
    /// Historical scope: serve covered subranges from the cache and fetch only
    /// the gaps. When false, always fetch the full range from the provider.
    pub read_cache: bool,
    /// Historical scope: write fetched gap data back to the cache.
    pub write_cache: bool,
    /// Live scope: tee every data event to the configured tap log.
    pub write_tap_log: bool,
}

impl PersistenceOptions {
    /// No persistence: always hit the provider, store nothing.
    #[must_use]
    pub const fn none() -> Self {
        Self {
            read_cache: false,
            write_cache: false,
            write_tap_log: false,
        }
    }

    /// Read-through cache: serve covered ranges, fetch and store only gaps.
    #[must_use]
    pub const fn cached() -> Self {
        Self {
            read_cache: true,
            write_cache: true,
            write_tap_log: false,
        }
    }

    /// Serve from cache and fetch gaps for this run, but do not persist them.
    #[must_use]
    pub const fn read_only() -> Self {
        Self {
            read_cache: true,
            write_cache: false,
            write_tap_log: false,
        }
    }

    /// Ignore cached coverage, re-fetch the whole range, overwrite the cache.
    #[must_use]
    pub const fn refresh() -> Self {
        Self {
            read_cache: false,
            write_cache: true,
            write_tap_log: false,
        }
    }

    /// Return a copy with the live tap-log axis set to `on`.
    #[must_use]
    pub const fn with_tap_log(mut self, on: bool) -> Self {
        self.write_tap_log = on;
        self
    }

    /// True if either axis touches the historical cache.
    #[must_use]
    pub const fn uses_cache(self) -> bool {
        self.read_cache || self.write_cache
    }
}

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
    providers: Vec<Arc<dyn Provider>>,
    tap_log: Option<Arc<dyn TapLog>>,
    historical_cache: Option<Arc<dyn HistoricalCache>>,
    /// Per-instrument provider pinning. When a session is opened for an
    /// instrument that appears here, the named provider is preferred over
    /// the first-supports-it match.
    instrument_provider: Vec<(Instrument, String)>,
    /// Live-session registry: at most one live session may be active per
    /// `(instrument, kind)` pair. Each live `Session` holds an `Arc` to a
    /// [`RegistrySentinel`]; the map keeps a `Weak` so a stale entry can be
    /// distinguished from a live one via `strong_count()`.
    live_sessions: LiveSessionRegistry,
    resume_buffer_events: usize,
    /// Single source of truth for the corporate-action adjustment mode.
    /// Stamped into every `HistoryRequest` (so the provider fetches adjusted)
    /// and every `CacheKey` (so the cache segregates by mode). Never set
    /// independently on the provider or cache instances.
    adjustment: Adjustment,
    /// Per-`CacheKey` single-flight registry: at most one outstanding
    /// provider fetch per key (see `fetch_locks`).
    fetch_locks: FetchLocks,
    /// Per-provider call/throughput accounting, keyed by provider id. Cloned
    /// into each controller so the cold-site and stream-derived counters land
    /// on the same handle the diagnostics snapshot reads.
    provider_accounting: HashMap<datamancer_core::ProviderId, Arc<ProviderAccounting>>,
}

impl DatamancerInner {
    /// The accounting handle for `provider_id`, creating nothing — every
    /// registered provider gets a handle at build time, so an unknown id
    /// (only test fakes routed outside the registry) falls back to a detached
    /// handle whose counters are never read.
    fn accounting_for(&self, provider_id: &str) -> Arc<ProviderAccounting> {
        self.provider_accounting
            .iter()
            .find(|(id, _)| id.as_str() == provider_id)
            .map_or_else(
                || Arc::new(ProviderAccounting::default()),
                |(_, a)| a.clone(),
            )
    }
}

pub(crate) type LiveSessionRegistry =
    Arc<std::sync::Mutex<HashMap<(Instrument, EventKind), Weak<AuthoritativeSession>>>>;

impl Datamancer {
    #[must_use]
    pub fn builder() -> DatamancerBuilder {
        DatamancerBuilder::default()
    }

    /// Open a session for one `(instrument, kind)` pair.
    ///
    /// Eager: live subscription / historical fetch begins before this returns.
    ///
    /// # Errors
    ///
    /// Fail-fast on:
    /// - [`Error::UnsupportedEventKind`] — no registered provider serves
    ///   `(instrument, kind)`.
    /// - [`Error::LiveSessionConflict`] — a live session for this pair is
    ///   already active (Live scope only; multiple Historical sessions for
    ///   the same pair are stateless reads and run concurrently).
    /// - [`Error::PersistenceRequired`] — `options` requires a cache but no
    ///   [`HistoricalCache`] is configured.
    ///
    /// # Panics
    ///
    /// Panics if the internal live-session registry mutex is poisoned —
    /// indicates a prior panic inside a registry-holding code path.
    pub async fn session(
        &self,
        instrument: Instrument,
        kind: EventKind,
        scope: Scope,
        options: PersistenceOptions,
    ) -> Result<Session> {
        if options.uses_cache() && self.inner.historical_cache.is_none() {
            return Err(Error::PersistenceRequired);
        }
        if options.write_tap_log && self.inner.tap_log.is_none() {
            return Err(Error::PersistenceRequired);
        }

        match scope {
            Scope::Historical { from, to } => {
                // Historical sessions do not participate in the live registry
                // (concurrent reads for the same pair run independently) and keep
                // the single-consumer controller verbatim.
                let provider = self.route(&instrument, kind)?;
                let accounting = self.inner.accounting_for(provider.id());
                let (cmd_tx, cmd_rx) = mpsc::channel::<SessionCommand>(8);
                let (events_tx, events_rx) = mpsc::channel::<MarketEvent>(default_buffer());
                let inner = Arc::new(SessionInner {
                    instrument: instrument.clone(),
                    kind,
                    scope,
                    events_holder: Mutex::new(Some(events_rx)),
                    persistence: std::sync::Mutex::new(options),
                    stream_taken: std::sync::atomic::AtomicBool::new(false),
                    cmd_tx,
                    adjustment: self.inner.adjustment,
                });
                let (provider_tx, provider_rx) = mpsc::channel::<MarketEvent>(default_buffer());
                let controller = Controller {
                    inner: inner.clone(),
                    provider,
                    tap_log: self.inner.tap_log.clone(),
                    historical_cache: self.inner.historical_cache.clone(),
                    sink: Sink::Attached(InProcessSink { tx: events_tx }),
                    ring_capacity: self.inner.resume_buffer_events,
                    fetch_locks: self.inner.fetch_locks.clone(),
                    next_seq: 0,
                    fanout: None,
                    stats: Arc::new(LiveStats::new()),
                    accounting,
                };
                tokio::spawn(controller.run_historical(from, to, provider_tx, provider_rx, cmd_rx));
                Ok(Session {
                    variant: SessionVariant::Historical(HistoricalSession { inner }),
                })
            }
            Scope::Live { .. } => {
                // Live sessions are refcounted referrers onto the shared
                // authoritative session: a second opener for the same pair shares
                // it (no longer `LiveSessionConflict`). The single-pair `Session`
                // is implemented as a one-subscription client referrer.
                let (authoritative, guard, rx) = self
                    .authoritative(instrument.clone(), kind, scope, options)
                    .await?;
                let provider = authoritative.provider_id.clone();
                let handle = spawn_client(
                    self.clone(),
                    self.inner.resume_buffer_events,
                    Some(((instrument.clone(), kind), guard, rx, provider)),
                );
                Ok(Session {
                    variant: SessionVariant::Live(LiveSession {
                        handle,
                        authoritative,
                        instrument,
                        kind,
                        scope,
                        persistence: std::sync::Mutex::new(options),
                    }),
                })
            }
        }
    }

    /// Create-or-share the authoritative session for a live `(instrument, kind)`
    /// pair and register one subscriber. The lock is held across probe-and-decide
    /// so two concurrent openers cannot both create; the share path adds a
    /// subscriber to the existing session, the create path starts a fresh
    /// provider connection.
    pub(crate) async fn authoritative(
        &self,
        instrument: Instrument,
        kind: EventKind,
        scope: Scope,
        options: PersistenceOptions,
    ) -> Result<(
        Arc<AuthoritativeSession>,
        SubscriberGuard,
        mpsc::Receiver<MarketEvent>,
    )> {
        let provider = self.route(&instrument, kind)?;
        let key = (instrument.clone(), kind);

        // Probe under the lock; if a live session exists, try to share it.
        let existing = {
            let map = self
                .inner
                .live_sessions
                .lock()
                .expect("live-session registry mutex poisoned");
            map.get(&key).and_then(Weak::upgrade)
        };
        if let Some(existing) = existing
            && let Ok((id, rx)) = existing.add_subscriber().await
        {
            let guard = SubscriberGuard::new(existing.clone(), id);
            return Ok((existing, guard, rx));
        }

        self.create_authoritative(provider, instrument, kind, scope, options, key)
            .await
    }

    async fn create_authoritative(
        &self,
        provider: Arc<dyn Provider>,
        instrument: Instrument,
        kind: EventKind,
        scope: Scope,
        options: PersistenceOptions,
        key: (Instrument, EventKind),
    ) -> Result<(
        Arc<AuthoritativeSession>,
        SubscriberGuard,
        mpsc::Receiver<MarketEvent>,
    )> {
        let accounting = self.inner.accounting_for(provider.id());
        let (cmd_tx, cmd_rx) = mpsc::channel::<SessionCommand>(default_buffer());
        let (remove_tx, remove_rx) = mpsc::unbounded_channel::<SubscriberId>();
        let stats = Arc::new(LiveStats::new());
        let authoritative = Arc::new(AuthoritativeSession::new(
            instrument.clone(),
            kind,
            provider.id().to_string(),
            scope,
            cmd_tx.clone(),
            remove_tx,
            stats.clone(),
            self.inner.live_sessions.clone(),
            key.clone(),
        ));

        // Re-check under the lock and either defer to a racing winner or claim
        // the slot. Our `authoritative` has spawned nothing yet, so discarding
        // it on a lost race is free. The guard is dropped before any await.
        let winner = {
            let mut map = self
                .inner
                .live_sessions
                .lock()
                .expect("live-session registry mutex poisoned");
            if let Some(winner) = map.get(&key).and_then(Weak::upgrade) {
                Some(winner)
            } else {
                map.insert(key, Arc::downgrade(&authoritative));
                None
            }
        };
        if let Some(winner) = winner {
            let (id, rx) = winner.add_subscriber().await?;
            let guard = SubscriberGuard::new(winner.clone(), id);
            return Ok((winner, guard, rx));
        }

        // Pre-seed the opener as the first fan-out subscriber *before* spawning
        // the controller, so events emitted during a creation-time backfill (a
        // covered cache segment can stream immediately) are never lost to an
        // empty fan-out. Later referrers join via `AddSubscriber`.
        let first_id = authoritative.alloc_subscriber_id();
        let (first_tx, first_rx) = mpsc::channel::<MarketEvent>(default_buffer());
        let mut fanout = FanOut::new();
        fanout.add(first_id, first_tx);

        // Lock released. Start the provider connection and spawn the controller.
        // On error, `authoritative` drops as we return and its `Drop` clears the
        // registry slot.
        let inner = Arc::new(SessionInner {
            instrument: instrument.clone(),
            kind,
            scope,
            events_holder: Mutex::new(None),
            persistence: std::sync::Mutex::new(options),
            stream_taken: std::sync::atomic::AtomicBool::new(false),
            cmd_tx,
            adjustment: self.inner.adjustment,
        });
        let (provider_tx, provider_rx) = mpsc::channel::<MarketEvent>(default_buffer());
        let controller = Controller {
            inner,
            provider: provider.clone(),
            tap_log: self.inner.tap_log.clone(),
            historical_cache: self.inner.historical_cache.clone(),
            sink: Sink::Detached(EventRing::new(1)),
            ring_capacity: self.inner.resume_buffer_events,
            fetch_locks: self.inner.fetch_locks.clone(),
            next_seq: 0,
            fanout: Some(fanout),
            stats,
            accounting: accounting.clone(),
        };
        let backfill_from = match scope {
            Scope::Live { backfill_from } => backfill_from,
            Scope::Historical { .. } => None,
        };
        accounting.record_live_start();
        let live = provider.start_live(provider_tx).await?;
        accounting.record_subscribe();
        live.subscribe(instrument, kind).await?;
        tokio::spawn(controller.run_live(live, backfill_from, provider_rx, cmd_rx, remove_rx));

        let guard = SubscriberGuard::new(authoritative.clone(), first_id);
        Ok((authoritative, guard, first_rx))
    }

    /// Open a new multiplexing [`ClientSession`] with an empty subscription set.
    #[must_use]
    pub fn client_session(&self) -> ClientSession {
        let handle = spawn_client(self.clone(), self.inner.resume_buffer_events, None);
        ClientSession::new(handle)
    }

    /// Look up a registered provider by id.
    ///
    /// # Errors
    ///
    /// Returns `Error::UnknownProvider` if no provider with that id is
    /// registered.
    pub fn provider(&self, id: &str) -> Result<&dyn Provider> {
        self.inner
            .providers
            .iter()
            .find(|p| p.id() == id)
            .map(|p| p.as_ref() as &dyn Provider)
            .ok_or_else(|| Error::UnknownProvider(id.to_string()))
    }

    fn route(&self, instrument: &Instrument, kind: EventKind) -> Result<Arc<dyn Provider>> {
        let pinned = self
            .inner
            .instrument_provider
            .iter()
            .find(|(i, _)| i == instrument)
            .map(|(_, p)| p.as_str());

        let candidate = if let Some(id) = pinned {
            self.inner
                .providers
                .iter()
                .find(|p| p.id() == id && p.supports(instrument, kind))
        } else {
            self.inner
                .providers
                .iter()
                .find(|p| p.supports(instrument, kind))
        };

        candidate
            .cloned()
            .ok_or_else(|| Error::UnsupportedEventKind {
                kind,
                instrument: instrument.clone(),
            })
    }
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct DatamancerBuilder {
    providers: Vec<Arc<dyn Provider>>,
    tap_log: Option<Arc<dyn TapLog>>,
    historical_cache: Option<Arc<dyn HistoricalCache>>,
    instrument_provider: Vec<(Instrument, String)>,
    resume_buffer_events: Option<usize>,
    /// Corporate-action adjustment mode. `Adjustment`'s `Default` is `All`, so
    /// `DatamancerBuilder::default()` already yields fully-adjusted bars; call
    /// [`adjustment`](Self::adjustment) to override.
    adjustment: Adjustment,
}

impl DatamancerBuilder {
    /// Register a provider. Provider ids must be unique within a Datamancer
    /// instance; conflicts surface from [`build`](Self::build).
    #[must_use]
    pub fn provider(mut self, p: Box<dyn Provider>) -> Self {
        self.providers.push(Arc::from(p));
        self
    }

    /// Register a provider held behind an `Arc`. Useful when the caller keeps
    /// a reference for direct API calls outside of a session.
    #[must_use]
    pub fn provider_arc(mut self, p: Arc<dyn Provider>) -> Self {
        self.providers.push(p);
        self
    }

    /// Pin `instrument` to a specific provider id. When more than one
    /// registered provider supports the pair, the pinned one wins.
    #[must_use]
    pub fn pin(mut self, instrument: Instrument, provider_id: impl Into<String>) -> Self {
        self.instrument_provider
            .push((instrument, provider_id.into()));
        self
    }

    /// Attach a tap log; live events from sessions configured to record will
    /// be appended.
    #[must_use]
    pub fn tap_log(mut self, log: Box<dyn TapLog>) -> Self {
        self.tap_log = Some(Arc::from(log));
        self
    }

    /// Register a tap log held behind an `Arc`. Useful when the caller keeps a
    /// reference to replay the captured stream after the session ends.
    #[must_use]
    pub fn tap_log_arc(mut self, log: Arc<dyn TapLog>) -> Self {
        self.tap_log = Some(log);
        self
    }

    /// Attach a historical cache; historical fetches from sessions with
    /// `PersistenceOptions::write_cache` enabled (e.g. `PersistenceOptions::cached()`)
    /// write-through this cache before returning to the consumer.
    #[must_use]
    pub fn historical_cache(mut self, cache: Box<dyn HistoricalCache>) -> Self {
        self.historical_cache = Some(Arc::from(cache));
        self
    }

    /// Same as [`historical_cache`](Self::historical_cache) but accepts an
    /// already-shared `Arc`.
    #[must_use]
    pub fn historical_cache_arc(mut self, cache: Arc<dyn HistoricalCache>) -> Self {
        self.historical_cache = Some(cache);
        self
    }

    /// Bound (in events) for a live session's detached resume buffer and the
    /// stitched backfill's pending-live buffer. Overflow evicts the oldest
    /// events and reports them as one in-band `Control::Gap` on the next
    /// attach. Defaults to 65 536.
    #[must_use]
    pub fn resume_buffer_events(mut self, events: usize) -> Self {
        self.resume_buffer_events = Some(events);
        self
    }

    /// Set the corporate-action adjustment mode applied to historical bar
    /// fetches and used to segregate the cache. Defaults to [`Adjustment::All`]
    /// (split + dividend + spin-off). The mode is stamped into both the
    /// provider request and the cache key from this single source of truth.
    #[must_use]
    pub fn adjustment(mut self, adjustment: Adjustment) -> Self {
        self.adjustment = adjustment;
        self
    }

    /// Finalize the builder.
    ///
    /// # Errors
    ///
    /// Returns `Error::Config` if two registered providers share an id.
    pub fn build(self) -> Result<Datamancer> {
        let mut ids: Vec<&str> = self.providers.iter().map(|p| p.id()).collect();
        ids.sort_unstable();
        for window in ids.windows(2) {
            if let [a, b] = window
                && a == b
            {
                return Err(Error::Config(format!("duplicate provider id: {a}")));
            }
        }
        let provider_accounting = self
            .providers
            .iter()
            .map(|p| {
                (
                    datamancer_core::ProviderId::new(p.id().to_string()),
                    Arc::new(ProviderAccounting::default()),
                )
            })
            .collect();
        Ok(Datamancer {
            inner: Arc::new(DatamancerInner {
                providers: self.providers,
                tap_log: self.tap_log,
                historical_cache: self.historical_cache,
                instrument_provider: self.instrument_provider,
                live_sessions: Arc::new(std::sync::Mutex::new(HashMap::new())),
                resume_buffer_events: self
                    .resume_buffer_events
                    .unwrap_or(DEFAULT_RESUME_BUFFER_EVENTS),
                adjustment: self.adjustment,
                fetch_locks: FetchLocks::default(),
                provider_accounting,
            }),
        })
    }
}

// ---------------------------------------------------------------------------
// Session handle
// ---------------------------------------------------------------------------

/// A handle to a running session for one `(instrument, kind)` pair. Single-owner;
/// not `Clone`.
///
/// `Scope::Historical` keeps the single-consumer controller (fetch/stream
/// anchored). `Scope::Live` is a one-subscription **referrer** onto the shared
/// authoritative session: dropping the handle releases the referrer, and the
/// authoritative session tears down when its last referrer leaves.
pub struct Session {
    variant: SessionVariant,
}

enum SessionVariant {
    Historical(HistoricalSession),
    Live(LiveSession),
}

struct HistoricalSession {
    inner: Arc<SessionInner>,
}

struct LiveSession {
    handle: ClientHandle,
    authoritative: Arc<AuthoritativeSession>,
    instrument: Instrument,
    kind: EventKind,
    scope: Scope,
    /// Locally cached persistence options for the synchronous getter. The
    /// authoritative session is the source of truth (shared across referrers);
    /// the single-owner live `Session` keeps this in step via `set_persistence`.
    persistence: std::sync::Mutex<PersistenceOptions>,
}

struct SessionInner {
    instrument: Instrument,
    kind: EventKind,
    scope: Scope,
    /// Holder for the consumer-facing receiver. `Some` when no stream is
    /// held (historical scope only — pre-created); `None` when taken or for
    /// live sessions (which attach via `SessionCommand::Take`).
    events_holder: Mutex<Option<mpsc::Receiver<MarketEvent>>>,
    persistence: std::sync::Mutex<PersistenceOptions>,
    /// Set to true the first (and only) time `take_events` succeeds. The
    /// historical controller reads it after fetch completion: if the consumer
    /// never took the stream, there's nobody to drain to and the session
    /// shuts down immediately rather than hanging on `events_tx.closed()`.
    stream_taken: std::sync::atomic::AtomicBool,
    cmd_tx: mpsc::Sender<SessionCommand>,
    /// Corporate-action adjustment mode for this session, copied from
    /// `DatamancerInner.adjustment`. Stamped into every `HistoryRequest` and
    /// `CacheKey` the controller builds.
    adjustment: Adjustment,
}

impl Session {
    /// Take the event stream.
    ///
    /// **Live scope — multi-shot.** The first call attaches a stream; after
    /// the consumer drops it, a later call re-attaches. Events arriving while
    /// detached are buffered (bounded — see
    /// [`DatamancerBuilder::resume_buffer_events`]); on re-attach a single
    /// [`ControlKind::Gap`] reports anything the buffer had to evict, then
    /// the buffered events flow, then live continues. Events still sitting
    /// undelivered in a dropped stream's channel are lost without a gap
    /// (bounded by the channel buffer); they are never `seq`-stamped, so the
    /// delivered stream stays contiguous. Drain before dropping if you need
    /// every event, or record via the tap log.
    ///
    /// **Historical scope — single-shot.** The first call returns the
    /// stream; subsequent calls error.
    ///
    /// # Errors
    ///
    /// - [`Error::EventsAlreadyTaken`] — a previous stream is still
    ///   outstanding and open (live), or was already taken (historical).
    /// - [`Error::SessionClosed`] — the controller has shut down.
    pub async fn take_events(&self) -> Result<EventStream> {
        match &self.variant {
            SessionVariant::Historical(h) => {
                let rx = h
                    .inner
                    .events_holder
                    .lock()
                    .await
                    .take()
                    .ok_or(Error::EventsAlreadyTaken)
                    .inspect(|_| {
                        h.inner
                            .stream_taken
                            .store(true, std::sync::atomic::Ordering::Release);
                    })?;
                Ok(EventStream::new(rx))
            }
            SessionVariant::Live(l) => l.handle.take_events().await,
        }
    }

    /// Replace the persistence options at runtime. Affects future writes;
    /// an in-flight historical fetch keeps the plan it started with.
    ///
    /// For a live session the tap-log tee is a property of the shared
    /// authoritative session, so this updates it for every referrer.
    ///
    /// # Errors
    ///
    /// Returns `Error::PersistenceRequired` if the new options require a cache
    /// that is not configured; `Error::SessionClosed` if the controller has
    /// shut down.
    ///
    /// # Panics
    ///
    /// Panics if the persistence mutex is poisoned (a prior panic inside a
    /// persistence-holding code path).
    pub async fn set_persistence(&self, options: PersistenceOptions) -> Result<()> {
        match &self.variant {
            SessionVariant::Historical(h) => {
                let (tx, rx) = oneshot::channel();
                h.inner
                    .cmd_tx
                    .send(SessionCommand::SetPersistence(options, tx))
                    .await
                    .map_err(|_| Error::SessionClosed)?;
                rx.await.map_err(|_| Error::SessionClosed)?
            }
            SessionVariant::Live(l) => {
                l.authoritative.set_persistence(options).await?;
                *l.persistence.lock().expect("persistence mutex poisoned") = options;
                Ok(())
            }
        }
    }

    /// Returns the current persistence options for this session.
    ///
    /// # Panics
    ///
    /// Panics if the persistence mutex is poisoned (indicates a prior panic
    /// inside a persistence-holding code path).
    #[must_use]
    pub fn persistence(&self) -> PersistenceOptions {
        match &self.variant {
            SessionVariant::Historical(h) => *h
                .inner
                .persistence
                .lock()
                .expect("persistence mutex poisoned"),
            SessionVariant::Live(l) => *l.persistence.lock().expect("persistence mutex poisoned"),
        }
    }

    /// Explicit termination. Auto-cleanup also handles natural-completion
    /// cases (historical fetch exhausted, or live + handle-dropped).
    ///
    /// # Errors
    ///
    /// Currently infallible; the `Result` shape is reserved for future
    /// flush-error reporting from persistence sinks.
    pub async fn close(self) -> Result<()> {
        match &self.variant {
            SessionVariant::Historical(h) => {
                let (tx, rx) = oneshot::channel();
                let _ = h.inner.cmd_tx.send(SessionCommand::Close(tx)).await;
                let _ = rx.await;
                Ok(())
            }
            SessionVariant::Live(l) => l.handle.close().await,
        }
    }

    #[must_use]
    pub fn instrument(&self) -> &Instrument {
        match &self.variant {
            SessionVariant::Historical(h) => &h.inner.instrument,
            SessionVariant::Live(l) => &l.instrument,
        }
    }

    #[must_use]
    pub fn kind(&self) -> EventKind {
        match &self.variant {
            SessionVariant::Historical(h) => h.inner.kind,
            SessionVariant::Live(l) => l.kind,
        }
    }

    #[must_use]
    pub fn scope(&self) -> Scope {
        match &self.variant {
            SessionVariant::Historical(h) => h.inner.scope,
            SessionVariant::Live(l) => l.scope,
        }
    }
}

/// The session's output stream. Drop it to stop emission; re-take from the
/// owning [`Session`] if you want events again.
///
/// `seq` is **not** stamped here. The authoritative controller stamps each
/// event once at the source, in canonical delivery order, before it reaches
/// the sink (see [`Controller::stamp`]); this stream is a pass-through.
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

pub(crate) enum SessionCommand {
    SetPersistence(PersistenceOptions, oneshot::Sender<Result<()>>),
    Close(oneshot::Sender<()>),
    /// Register a fan-out subscriber on a live authoritative session. The
    /// controller inserts `sender` into the fan-out (replaying the cached
    /// `SubscriptionChanged`) and acks.
    AddSubscriber {
        id: SubscriberId,
        sender: mpsc::Sender<MarketEvent>,
        ack: oneshot::Sender<()>,
    },
}

// ---------------------------------------------------------------------------
// Controller
// ---------------------------------------------------------------------------

pub(crate) fn default_buffer() -> usize {
    1024
}

/// Default bound for the detached resume buffer and the backfill pending-live
/// buffer, in events.
const DEFAULT_RESUME_BUFFER_EVENTS: usize = 65_536;

/// The controller's consumer-facing side. `Attached` delivers into the
/// outstanding [`EventStream`]'s channel (with backpressure); `Detached`
/// buffers into a bounded [`EventRing`] until the next `take_events`. The
/// resume buffer is core-side (the `Detached` arm); the [`EventSink`] only
/// delivers.
enum Sink {
    Attached(InProcessSink),
    Detached(EventRing),
}

/// In-process [`EventSink`]: wraps the consumer-facing channel. `publish`
/// hands back a rejected event (consumer dropped its stream) so the controller
/// can divert it to the resume buffer. `flush` is a no-op — the channel has no
/// transport-side buffer to drain.
struct InProcessSink {
    tx: mpsc::Sender<MarketEvent>,
}

#[async_trait]
impl EventSink for InProcessSink {
    async fn publish(&self, ev: MarketEvent) -> PublishOutcome {
        match self.tx.send(ev).await {
            Ok(()) => PublishOutcome::Delivered,
            Err(tokio::sync::mpsc::error::SendError(ev)) => PublishOutcome::Rejected(ev),
        }
    }

    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}

struct Controller {
    inner: Arc<SessionInner>,
    provider: Arc<dyn Provider>,
    tap_log: Option<Arc<dyn TapLog>>,
    historical_cache: Option<Arc<dyn HistoricalCache>>,
    sink: Sink,
    /// Capacity for rings created on detach (from the builder knob).
    ring_capacity: usize,
    fetch_locks: FetchLocks,
    /// Single-writer `seq` counter. The controller is the only task that
    /// stamps, so a plain `u64` (no atomic, no sharing) structurally encodes
    /// "stamped once at the source." Initialized to `0` at construction;
    /// [`Controller::stamp`] reads and increments it. Control events and
    /// evicted-then-numbered events both consume slots, so a resume-buffer
    /// overflow is a real `seq` hole reported as `Control::Gap`.
    next_seq: u64,
    /// `Some` for a live authoritative session: events fan out to every
    /// referrer's bounded channel. `None` for a historical session, which
    /// delivers to the single consumer `sink`.
    fanout: Option<FanOut>,
    /// Per-symbol live stats (lock-free atomics) read by the Phase 3 diagnostics
    /// plane. Updated on the fan-out path only; unused on the historical path.
    stats: Arc<LiveStats>,
    /// Per-provider call/throughput accounting for the diagnostics snapshot.
    /// Shared (cloned `Arc`) with `DatamancerInner` and every other controller
    /// for the same provider.
    accounting: Arc<ProviderAccounting>,
}

impl Controller {
    /// Drain the backfill `pending` ring at the seam. Its events were pushed
    /// **unstamped** (`buffer_live_arrival` does a raw push) so their `seq` is
    /// deferred to here — stamping at the seam places the live arrivals *after*
    /// all backfill segments in canonical order and preserves monotonicity.
    /// One `Gap` for anything the ring evicted, then the survivors, all through
    /// `emit` (which stamps). Behavior-identical to the pre-split `flush_ring`.
    async fn flush_backfill_pending(&mut self, ring: EventRing) {
        let (dropped, _dropped_first_seq, events) = ring.into_parts();
        if let Some(span) = dropped {
            self.emit_gap(span.from_source_ts, span.from_source_ts, span.to_source_ts)
                .await;
        }
        for ev in events {
            self.emit(ev).await;
        }
    }

    /// Historical scope: spawn the provider's fetch, forward events as they
    /// arrive, exit when fetch completes and the stream is drained or
    /// dropped.
    async fn run_historical(
        mut self,
        from: Timestamp,
        to: Timestamp,
        provider_tx: mpsc::Sender<MarketEvent>,
        mut provider_rx: mpsc::Receiver<MarketEvent>,
        mut cmd_rx: mpsc::Receiver<SessionCommand>,
    ) {
        let options = *self
            .inner
            .persistence
            .lock()
            .expect("persistence mutex poisoned");
        if options.uses_cache() && self.historical_cache.is_some() {
            // The cached read-through path runs its own per-gap channels; the
            // default single-fetch plumbing is unused here.
            drop(provider_tx);
            drop(provider_rx);
            self.run_historical_cached(from, to, options, &mut cmd_rx)
                .await;
            return;
        }

        let provider = self.provider.clone();
        let request = HistoryRequest {
            instrument: self.inner.instrument.clone(),
            kind: self.inner.kind,
            from,
            to,
            adjustment: self.inner.adjustment,
        };
        // Spawn the fetch with `provider_tx` so it owns the only producer
        // side; when the fetch returns, the channel closes and `recv` yields
        // `None`, signalling exhaustion to the loop below.
        self.accounting.record_history_fetch();
        let fetch_task =
            tokio::spawn(async move { provider.fetch_history(request, provider_tx).await });

        // Track the latest forwarded source timestamp so a failure control is
        // stamped at the confirmed end of the data already emitted, not back at
        // the range start (which would make `source_ts` move backwards).
        let mut max_data_ts: Option<Timestamp> = None;
        loop {
            tokio::select! {
                cmd = cmd_rx.recv() => {
                    if !self.handle_command(cmd).await {
                        fetch_task.abort();
                        return;
                    }
                }
                ev = provider_rx.recv() => {
                    match ev {
                        Some(ev) => {
                            max_data_ts = max_data_ts.max(data_source_ts(&ev));
                            self.forward(ev).await;
                        }
                        None => break, // fetch exhausted
                    }
                }
            }
        }

        // The fetch task owns the only `provider_tx`; the loop broke because
        // that sender dropped — which also happens when the fetch returns
        // `Err` before sending anything (e.g. missing credentials). Join it so
        // a fetch failure surfaces as an in-band `ProviderError` instead of an
        // empty, success-looking result.
        let error_ts = max_data_ts.unwrap_or(from);
        match fetch_task.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                self.emit_provider_error(error_ts, provider_error_message(&e))
                    .await;
            }
            Err(join_err) if join_err.is_cancelled() => {}
            Err(join_err) => {
                self.emit_provider_error(error_ts, format!("fetch task panicked: {join_err}"))
                    .await;
            }
        }

        self.finish_historical(&mut cmd_rx).await;
    }

    /// Shared post-fetch handshake for historical scopes. If the consumer
    /// never took the stream, shut down immediately (nobody to drain to).
    /// Otherwise emit `SessionClosing`, then wait for the stream to drain or
    /// drop and auto-close.
    async fn finish_historical(&mut self, cmd_rx: &mut mpsc::Receiver<SessionCommand>) {
        if !self
            .inner
            .stream_taken
            .load(std::sync::atomic::Ordering::Acquire)
        {
            self.shutdown().await;
            return;
        }
        let now = wall_clock_ts();
        self.emit(MarketEvent::Control(Control {
            source_ts: now,
            rx_ts: now,
            seq: Seq(0),
            kind: ControlKind::SessionClosing,
        }))
        .await;
        // Wait for the consumer to drain (channel closes on drop). If the
        // consumer already detached, there is nobody left to drain to.
        let tx = match &self.sink {
            Sink::Attached(s) if !s.tx.is_closed() => s.tx.clone(),
            _ => {
                self.shutdown().await;
                return;
            }
        };
        loop {
            tokio::select! {
                cmd = cmd_rx.recv() => {
                    if !self.handle_command(cmd).await {
                        return;
                    }
                }
                () = tx.closed() => break,
            }
        }
        self.shutdown().await;
    }

    /// Forward an in-band `Gap` control covering `[from, to)` for this
    /// session's instrument, stamped at `source_ts`. Goes through `forward()`
    /// so it gets a `seq`. The caller chooses `source_ts`: the live backfill
    /// seam uses the wall clock (the seam is "now"); historical paths pass an
    /// in-range timestamp so the control stays ordered within the source-time
    /// stream rather than jumping to wall-clock ahead of older events.
    async fn emit_gap(&mut self, source_ts: Timestamp, from: Timestamp, to: Timestamp) {
        self.forward(MarketEvent::Control(Control {
            source_ts,
            rx_ts: source_ts,
            seq: Seq(0),
            kind: ControlKind::Gap {
                provider: self.provider.id().to_string(),
                instrument: self.inner.instrument.clone(),
                span: GapSpan {
                    from_source_ts: from,
                    to_source_ts: to,
                },
            },
        }))
        .await;
    }

    /// Forward an in-band `ProviderError` control surfacing a failed provider
    /// fetch (missing credentials, an auth rejection, a mid-stream transport
    /// fault). Historical fetches run the provider call detached, so without
    /// this the failure is invisible to the consumer — the event stream just
    /// ends with no data, indistinguishable from an empty range. Stamped at an
    /// in-range `source_ts` so it stays ordered within the source-time stream.
    async fn emit_provider_error(&mut self, source_ts: Timestamp, message: String) {
        self.forward(MarketEvent::Control(Control {
            source_ts,
            rx_ts: source_ts,
            seq: Seq(0),
            kind: ControlKind::ProviderError {
                provider: self.provider.id().to_string(),
                message,
            },
        }))
        .await;
    }

    /// Stream a planned segment sequence: covered segments replay from the
    /// cache, gap segments fetch from the provider (forwarded and, when
    /// `write_cache`, stored back). With a `BackfillSide`, live arrivals are
    /// teed + buffered concurrently, the segment touching `edge` gets a
    /// conservative coverage claim (history endpoints lag the live feed), and
    /// a failed fetch gaps through to `edge`. On a gap-fetch failure only the
    /// confirmed prefix is claimed, an in-band `Gap` is emitted for the
    /// remainder, and the remaining segments are abandoned.
    #[allow(
        clippy::too_many_lines,
        reason = "linear per-segment dispatch kept inline; extraction would obscure the covered/gap handling symmetry"
    )]
    async fn stream_segments(
        &mut self,
        segments: Vec<Segment>,
        options: PersistenceOptions,
        cmd_rx: &mut mpsc::Receiver<SessionCommand>,
        live: Option<BackfillSide<'_>>,
    ) -> SegmentOutcome {
        let instrument = self.inner.instrument.clone();
        let kind = self.inner.kind;
        let edge = live.as_ref().map(|l| l.edge);
        let (mut live_rx, mut pending, mut drop_rx) = match live {
            Some(l) => (Some(l.provider_rx), Some(l.pending), Some(l.drop_rx)),
            None => (None, None, None),
        };

        for seg in segments {
            match seg {
                Segment::Covered { from: f, to: t } => {
                    let Some(cache) = self.historical_cache.clone() else {
                        // Covered segments only arise from a cache's gaps()
                        // report; defensively gap it if the cache vanished.
                        self.emit_gap(f, f, t).await;
                        continue;
                    };
                    let source = cache.as_replay_source(CacheKey {
                        instrument: instrument.clone(),
                        kind,
                        from: f,
                        to: t,
                        adjustment: self.inner.adjustment,
                    });
                    let req = ReplayRequest {
                        instruments: vec![instrument.clone()],
                        kinds: vec![kind],
                        from: f,
                        to: t,
                    };
                    let mut stream = match source.open(req).await {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::warn!(
                                instrument = %self.inner.instrument,
                                from = f.0,
                                to = t.0,
                                error = %e,
                                "cache replay open failed; emitting gap for covered segment"
                            );
                            // Stamp the gap at the segment start (in-range) so
                            // it stays ordered ahead of later segments.
                            self.emit_gap(f, f, t).await;
                            continue;
                        }
                    };
                    loop {
                        tokio::select! {
                            cmd = cmd_rx.recv() => {
                                if !self.handle_command(cmd).await { return SegmentOutcome::Closed; }
                            }
                            () = session_dropped(&mut drop_rx) => {
                                self.shutdown().await;
                                return SegmentOutcome::Closed;
                            }
                            ev = recv_live(&mut live_rx) => {
                                self.buffer_live_arrival(ev, &mut live_rx, &mut pending).await;
                            }
                            ev = stream.next() => {
                                match ev {
                                    // Replayed data: emit directly, never tee
                                    // (the tap log captures the live tail only).
                                    Some(ev) => self.emit(ev).await,
                                    None => break,
                                }
                            }
                        }
                    }
                }
                Segment::Gap { from: f, to: t } => {
                    let (tx, mut rx) = mpsc::channel::<MarketEvent>(default_buffer());
                    let provider = self.provider.clone();
                    let request = HistoryRequest {
                        instrument: instrument.clone(),
                        kind,
                        from: f,
                        to: t,
                        adjustment: self.inner.adjustment,
                    };
                    self.accounting.record_history_fetch();
                    let fetch_task =
                        tokio::spawn(async move { provider.fetch_history(request, tx).await });

                    // `batch` exists solely for the cache write; coverage
                    // claims need only the max data `source_ts` seen.
                    let store = options.write_cache && self.historical_cache.is_some();
                    let mut batch: Vec<MarketEvent> = Vec::new();
                    let mut max_data_ts: Option<Timestamp> = None;
                    loop {
                        tokio::select! {
                            cmd = cmd_rx.recv() => {
                                if !self.handle_command(cmd).await {
                                    fetch_task.abort();
                                    return SegmentOutcome::Closed;
                                }
                            }
                            () = session_dropped(&mut drop_rx) => {
                                fetch_task.abort();
                                self.shutdown().await;
                                return SegmentOutcome::Closed;
                            }
                            ev = recv_live(&mut live_rx) => {
                                self.buffer_live_arrival(ev, &mut live_rx, &mut pending).await;
                            }
                            ev = rx.recv() => {
                                match ev {
                                    Some(ev) => {
                                        max_data_ts = max_data_ts.max(data_source_ts(&ev));
                                        if store { batch.push(ev.clone()); }
                                        // Fetched data: emit directly, never tee
                                        // (backfill data belongs to the cache).
                                        self.emit(ev).await;
                                    }
                                    None => break,
                                }
                            }
                        }
                    }

                    let fetch_error = match fetch_task.await {
                        Ok(Ok(())) => None,
                        Ok(Err(e)) => Some(provider_error_message(&e)),
                        Err(join_err) if join_err.is_cancelled() => None,
                        Err(join_err) => Some(format!("fetch task panicked: {join_err}")),
                    };
                    if fetch_error.is_none() {
                        // A segment ending at the live edge gets a conservative
                        // claim: history endpoints lag the live feed, so a
                        // "successful" fetch may silently lack the last
                        // seconds. Claim only through the last received event;
                        // the unmaterialized sliver stays a gap.
                        let claim_to = if edge == Some(t) {
                            confirmed_prefix_end(max_data_ts, f, t)
                        } else {
                            t
                        };
                        if options.write_cache
                            && let Some(cache) = &self.historical_cache
                        {
                            let store_key = CacheKey {
                                instrument: instrument.clone(),
                                kind,
                                from: f,
                                to: claim_to,
                                adjustment: self.inner.adjustment,
                            };
                            if let Err(e) = cache.store(&store_key, &batch).await {
                                tracing::warn!(
                                    instrument = %self.inner.instrument,
                                    error = %e,
                                    "historical cache store failed; data delivered but not persisted"
                                );
                            }
                        }
                    } else {
                        // Honest coverage: claim only the confirmed prefix of
                        // [f, t); the unfetched remainder is gapped through to
                        // the live edge when one exists.
                        let confirmed_to = confirmed_prefix_end(max_data_ts, f, t);
                        if options.write_cache
                            && let Some(cache) = &self.historical_cache
                        {
                            let store_key = CacheKey {
                                instrument: instrument.clone(),
                                kind,
                                from: f,
                                to: confirmed_to,
                                adjustment: self.inner.adjustment,
                            };
                            if let Err(e) = cache.store(&store_key, &batch).await {
                                tracing::warn!(
                                    instrument = %self.inner.instrument,
                                    error = %e,
                                    "historical cache store failed; data delivered but not persisted"
                                );
                            }
                        }
                        let gap_to = edge.unwrap_or(t);
                        if confirmed_to < gap_to {
                            self.emit_gap(confirmed_to, confirmed_to, gap_to).await;
                        }
                        // The gap above only records coverage; surface the
                        // underlying cause so the consumer can tell a failed
                        // fetch from a legitimately empty range. Stamp it at the
                        // confirmed prefix boundary (matching the gap) so the
                        // control stays ordered after the data already emitted.
                        if let Some(message) = fetch_error {
                            self.emit_provider_error(confirmed_to, message).await;
                        }
                        break;
                    }
                }
            }
        }
        SegmentOutcome::Done
    }

    /// Read-through historical fetch. Tiles `[from, to)` into covered/gap
    /// segments (from `gaps()` when `read_cache`, else the whole range), then
    /// streams them in order: covered segments replay from the cache, gap
    /// segments fetch from the provider -- forwarded to the consumer and, when
    /// `write_cache`, stored back. On a gap-fetch failure, only the confirmed
    /// prefix is claimed, an in-band `Gap` is emitted for the remainder, and
    /// the remaining segments are abandoned.
    async fn run_historical_cached(
        &mut self,
        from: Timestamp,
        to: Timestamp,
        options: PersistenceOptions,
        cmd_rx: &mut mpsc::Receiver<SessionCommand>,
    ) {
        let cache = self
            .historical_cache
            .clone()
            .expect("cached path requires a historical cache");
        let plan_key = CacheKey {
            instrument: self.inner.instrument.clone(),
            kind: self.inner.kind,
            from,
            to,
            adjustment: self.inner.adjustment,
        };

        // Single-flight: an unlocked pre-check lets a fully-covered range
        // replay without ever touching the fetch slot. If there is anything
        // to fetch, acquire the per-key slot and RE-TILE against fresh
        // coverage — a concurrent winner may have just filled some or all of
        // it. We hold the slot across the fetch only when we actually fetch.
        //
        // The slot keys on the full `plan_key` (range included), so this
        // coalesces byte-identical requests (the cold-cache sweep). Two
        // concurrent sessions over *overlapping but non-identical* ranges take
        // different slots and may each fetch the overlap — range-precise
        // coalescing is a deliberate non-goal (coverage dedups it next time).
        let whole_range = || {
            vec![GapSpan {
                from_source_ts: from,
                to_source_ts: to,
            }]
        };
        let mut fetch_guard = None;
        let gaps = if options.read_cache {
            let initial = cache.gaps(&plan_key).await.unwrap_or_else(|e| {
                tracing::warn!(
                    instrument = %self.inner.instrument,
                    error = %e,
                    "cache gaps() failed; treating whole range as a gap"
                );
                whole_range()
            });
            if initial.is_empty() {
                initial
            } else {
                let guard = self.fetch_locks.acquire(&plan_key).await;
                let regaps = cache.gaps(&plan_key).await.unwrap_or_else(|e| {
                    tracing::warn!(
                        instrument = %self.inner.instrument,
                        error = %e,
                        "cache gaps() failed after acquiring fetch slot; \
                         treating whole range as a gap"
                    );
                    whole_range()
                });
                if regaps.is_empty() {
                    // We intended to fetch (initial gaps non-empty) but a
                    // concurrent single-flight winner filled the byte-identical
                    // range while we waited for the slot: a coalesced fetch.
                    // (Backfill bypasses FetchLocks, so it never coalesces.)
                    self.accounting.record_history_fetch_coalesced();
                } else {
                    fetch_guard = Some(guard);
                }
                regaps
            }
        } else {
            whole_range()
        };
        let segments = tile(from, to, &gaps);

        let outcome = self.stream_segments(segments, options, cmd_rx, None).await;
        // Release the fetch slot (if held) before finishing, so a queued
        // waiter proceeds as soon as our store has landed.
        drop(fetch_guard);
        if outcome == SegmentOutcome::Closed {
            return;
        }

        self.finish_historical(cmd_rx).await;
    }

    /// Phases 1–2 of a stitched live session: plan `[from, B)` exactly like
    /// the historical read-through path (whole-range gap when there is no
    /// cache or `read_cache` is off), stream the segments while buffering
    /// live arrivals, then drain the buffer at the seam. Returns `false`
    /// when the controller must exit (Close command / Session dropped).
    async fn run_backfill(
        &mut self,
        from: Timestamp,
        provider_rx: &mut mpsc::Receiver<MarketEvent>,
        cmd_rx: &mut mpsc::Receiver<SessionCommand>,
    ) -> bool {
        let edge = wall_clock_ts();
        if from >= edge {
            return true;
        }
        let options = *self
            .inner
            .persistence
            .lock()
            .expect("persistence mutex poisoned");

        let whole = vec![GapSpan {
            from_source_ts: from,
            to_source_ts: edge,
        }];
        // NOTE: single-flight (see `run_historical_cached`) is deliberately
        // NOT wired here. Backfill is the stitched-live path and out of scope
        // for the cold-sweep coalescer, which only runs over `Scope::Historical`
        // — so the two paths never race on the same key in the target workload.
        // A follow-up could factor the acquire+re-tile into a shared helper and
        // wire it here.
        let gaps = match (&self.historical_cache, options.read_cache) {
            (Some(cache), true) => {
                let plan_key = CacheKey {
                    instrument: self.inner.instrument.clone(),
                    kind: self.inner.kind,
                    from,
                    to: edge,
                    adjustment: self.inner.adjustment,
                };
                match cache.gaps(&plan_key).await {
                    Ok(g) => g,
                    Err(e) => {
                        tracing::warn!(
                            instrument = %self.inner.instrument,
                            error = %e,
                            "cache gaps() failed; treating whole backfill as a gap"
                        );
                        whole
                    }
                }
            }
            _ => whole,
        };
        let segments = tile(from, edge, &gaps);

        let mut pending = EventRing::new(self.ring_capacity);
        // The live lifecycle anchor is now the subscriber refcount, not a
        // Session drop guard. Keep a never-firing drop channel so the shared
        // `stream_segments` backfill path compiles without a real anchor: a
        // referrer that leaves mid-backfill is observed after the backfill seam
        // (the controller's teardown check), not by aborting the fetch.
        let (_drop_keepalive, mut drop_rx) = oneshot::channel::<()>();
        let outcome = self
            .stream_segments(
                segments,
                options,
                cmd_rx,
                Some(BackfillSide {
                    provider_rx,
                    pending: &mut pending,
                    drop_rx: &mut drop_rx,
                    edge,
                }),
            )
            .await;
        if outcome == SegmentOutcome::Closed {
            return false;
        }
        // The seam: live arrivals buffered during the backfill, in arrival
        // order (already teed at receipt — flush_backfill_pending stamps them
        // here via emit). Stamping at the seam (in reception order, not
        // source_ts order) places these live arrivals after all backfill
        // segments in canonical delivery order and keeps `seq` monotonic.
        self.flush_backfill_pending(pending).await;
        true
    }

    /// Live scope (authoritative): subscribe (already done by the caller), run
    /// any creation-time backfill, then drain provider events through the
    /// seq/forward/fan-out pipeline. Honors `AddSubscriber`/`SetPersistence`
    /// commands and subscriber removals; tears the upstream connection down when
    /// the fan-out empties (the last referrer left).
    async fn run_live(
        mut self,
        live: Box<dyn LiveHandle>,
        backfill_from: Option<Timestamp>,
        mut provider_rx: mpsc::Receiver<MarketEvent>,
        mut cmd_rx: mpsc::Receiver<SessionCommand>,
        mut remove_rx: mpsc::UnboundedReceiver<SubscriberId>,
    ) {
        let live = Arc::new(Mutex::new(Some(live)));
        if let Some(from) = backfill_from
            && !self.run_backfill(from, &mut provider_rx, &mut cmd_rx).await
        {
            self.teardown_upstream(&live).await;
            return;
        }
        loop {
            if self.live_should_teardown() {
                break;
            }
            tokio::select! {
                cmd = cmd_rx.recv() => {
                    if !self.handle_command(cmd).await {
                        break;
                    }
                }
                id = remove_rx.recv() => {
                    match id {
                        Some(id) => self.remove_subscriber(id),
                        None => break,
                    }
                }
                ev = provider_rx.recv() => {
                    let Some(ev) = ev else {
                        break; // provider task exited
                    };
                    self.forward(ev).await;
                }
            }
        }
        self.teardown_upstream(&live).await;
    }

    /// The refcounted-teardown trigger: a subscriber has been added and the
    /// fan-out is now empty (the last referrer left).
    fn live_should_teardown(&self) -> bool {
        self.fanout.as_ref().is_some_and(FanOut::should_teardown)
    }

    fn remove_subscriber(&mut self, id: SubscriberId) {
        if let Some(fanout) = self.fanout.as_mut() {
            fanout.remove(id);
        }
    }

    /// Release the upstream provider subscription and flush the tap log. No
    /// `SessionClosing` is emitted: by teardown the fan-out is empty (each
    /// referrer emits its own `SessionClosing` on `close`).
    async fn teardown_upstream(&mut self, live: &Arc<Mutex<Option<Box<dyn LiveHandle>>>>) {
        if let Some(h) = live.lock().await.take() {
            self.accounting.record_unsubscribe();
            let _ = h
                .unsubscribe(self.inner.instrument.clone(), self.inner.kind)
                .await;
            let _ = h.close().await;
        }
        if let Some(log) = &self.tap_log {
            let _ = log.flush().await;
        }
    }

    /// Assign the next source `seq`, advancing the single-writer counter. The
    /// one site that touches `next_seq`, so `seq` is stamped exactly once per
    /// event, in this authoritative session's canonical delivery order.
    fn stamp(&mut self, ev: MarketEvent) -> MarketEvent {
        let seq = Seq(self.next_seq);
        self.next_seq += 1;
        stamp_seq(ev, seq)
    }

    /// Hand a fully-formed event to the sink; on rejection (consumer dropped
    /// its stream) or while detached, buffer into the resume ring. Does **not**
    /// stamp — callers stamp (or not) first. Because the stamp happens before
    /// the ring push, an evicted event is already numbered, so a ring overflow
    /// is a real `seq` hole reported as `Control::Gap`. While detached there is
    /// no other producer into the outbound order, so push order == canonical
    /// delivery order.
    async fn deliver(&mut self, ev: MarketEvent) {
        // Live authoritative session: fan the stamped event out to every
        // referrer. Each referrer owns its per-client resume buffer, so there is
        // no core-side ring here.
        if self.fanout.is_some() {
            self.stats.record_event(&ev);
            if let Some(fanout) = self.fanout.as_mut() {
                fanout.fanout(&ev);
            }
            return;
        }
        let ev = match &self.sink {
            Sink::Attached(s) if !s.tx.is_closed() => match s.publish(ev).await {
                PublishOutcome::Delivered => return,
                PublishOutcome::Rejected(ev) => ev,
            },
            _ => ev,
        };
        // Consumer gone (or never attached): buffer until the next take.
        if matches!(self.sink, Sink::Attached(_)) {
            self.sink = Sink::Detached(EventRing::new(self.ring_capacity));
        }
        if let Sink::Detached(ring) = &mut self.sink {
            ring.push(ev);
        }
    }

    /// Stamp `seq` at the source, then deliver toward the consumer. Used by the
    /// direct-emit paths (cache replay, gap fetch, `SessionClosing`) that do
    /// not tee.
    async fn emit(&mut self, ev: MarketEvent) {
        let ev = self.stamp(ev);
        self.deliver(ev).await;
    }

    /// Stamp `seq` at the source, tee the (stamped) data event to the tap log
    /// (when configured for live capture), then deliver it. Stamping **before**
    /// the tee means the tap log records the source `seq` verbatim, so tap-log
    /// replay reproduces the delivered stream's `seq`. Replayed/backfill data
    /// must not come through here — `stream_segments` emits it directly, so only
    /// the live tail lands in the log (backfill data belongs to the cache).
    async fn forward(&mut self, ev: MarketEvent) {
        let ev = self.stamp(ev);
        // Stream-derived provider accounting: messages count as live-data
        // throughput only on the authoritative live path (fan-out present);
        // connection/reconnect/gap/error state is derived from in-band Control
        // regardless of scope.
        self.accounting.record_forwarded(&ev, self.fanout.is_some());
        self.tee(&ev).await;
        self.deliver(ev).await;
    }

    /// Backfill seam: a live arrival during the backfill is teed at receipt
    /// (durability — it must reach the tap log even if later evicted from
    /// `pending`) and buffered in arrival order for the post-backfill flush.
    /// A closed live side is parked (`live_rx = None`); it is surfaced after
    /// the backfill completes.
    async fn buffer_live_arrival(
        &mut self,
        ev: Option<MarketEvent>,
        live_rx: &mut Option<&mut mpsc::Receiver<MarketEvent>>,
        pending: &mut Option<&mut EventRing>,
    ) {
        match ev {
            Some(ev) => {
                self.tee(&ev).await;
                if let Some(p) = pending.as_mut() {
                    p.push(ev);
                }
            }
            None => *live_rx = None,
        }
    }

    /// Tee a data event to the tap log when this session is configured for
    /// live capture. Append before forwarding so a consumer that observes the
    /// event can rely on it having been enqueued for persistence; `append` is
    /// a non-blocking enqueue, so this never stalls the live stream.
    async fn tee(&self, ev: &MarketEvent) {
        if !matches!(
            ev,
            MarketEvent::Trade(_) | MarketEvent::Quote(_) | MarketEvent::Bar(_)
        ) {
            return;
        }
        if !matches!(self.inner.scope, Scope::Live { .. }) {
            return;
        }
        let Some(log) = &self.tap_log else { return };
        let write = self
            .inner
            .persistence
            .lock()
            .expect("persistence mutex poisoned")
            .write_tap_log;
        if write {
            let _ = log.append(ev).await;
        }
    }

    /// Returns false if the controller should exit.
    async fn handle_command(&mut self, cmd: Option<SessionCommand>) -> bool {
        match cmd {
            Some(SessionCommand::SetPersistence(options, ack)) => {
                let res = self.apply_persistence(options);
                let _ = ack.send(res);
                true
            }
            Some(SessionCommand::AddSubscriber { id, sender, ack }) => {
                if let Some(fanout) = self.fanout.as_mut() {
                    fanout.add(id, sender);
                }
                let _ = ack.send(());
                true
            }
            Some(SessionCommand::Close(ack)) => {
                self.shutdown().await;
                let _ = ack.send(());
                false
            }
            None => false,
        }
    }

    fn apply_persistence(&self, options: PersistenceOptions) -> Result<()> {
        if options.uses_cache() && self.historical_cache.is_none() {
            return Err(Error::PersistenceRequired);
        }
        if options.write_tap_log && self.tap_log.is_none() {
            return Err(Error::PersistenceRequired);
        }
        *self
            .inner
            .persistence
            .lock()
            .expect("persistence mutex poisoned") = options;
        Ok(())
    }

    async fn shutdown(&mut self) {
        let now = wall_clock_ts();
        self.emit(MarketEvent::Control(Control {
            source_ts: now,
            rx_ts: now,
            seq: Seq(0),
            kind: ControlKind::SessionClosing,
        }))
        .await;
        if let Some(log) = &self.tap_log {
            let _ = log.flush().await;
        }
        // Flush the transport-side buffer of the attached sink (no-op for the
        // in-process sink; forward-compat for buffering transports). Log and
        // swallow — in-process flush cannot fail and shutdown must not stall.
        if let Sink::Attached(s) = &self.sink
            && let Err(e) = s.flush().await
        {
            tracing::warn!(error = %e, "event sink flush failed at shutdown");
        }
    }
}

/// One slice of a requested historical range: either already in the cache
/// (`Covered`) or not yet fetched (`Gap`). Half-open `[from, to)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Segment {
    Covered { from: Timestamp, to: Timestamp },
    Gap { from: Timestamp, to: Timestamp },
}

/// Outcome of streaming a planned segment sequence.
#[derive(Debug, PartialEq, Eq)]
enum SegmentOutcome {
    /// Every segment streamed, or a failed gap-fetch was gapped and the
    /// remaining segments abandoned — the scope's normal continuation applies.
    Done,
    /// A Close command or Session drop ended the controller mid-stream.
    /// `shutdown()` has already run on this path — callers must not call it
    /// again (a second `SessionClosing` would be emitted).
    Closed,
}

/// Live-side plumbing threaded through `stream_segments` by the backfill
/// phase of a stitched session. `None` on the plain historical path.
struct BackfillSide<'a> {
    /// Live arrivals during the backfill; teed at receipt and buffered in
    /// `pending` so they splice in after the backfill output.
    provider_rx: &'a mut mpsc::Receiver<MarketEvent>,
    pending: &'a mut EventRing,
    /// Session-handle drop signal (live lifecycle anchor).
    drop_rx: &'a mut oneshot::Receiver<()>,
    /// The live boundary `B`: the gap segment touching it gets a
    /// conservative coverage claim, and a failed fetch gaps through to it.
    edge: Timestamp,
}

/// Receive from an optional live receiver; pend forever when absent (or
/// after the live side reported closure).
async fn recv_live(rx: &mut Option<&mut mpsc::Receiver<MarketEvent>>) -> Option<MarketEvent> {
    match rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

/// Resolve when the Session handle drops; pend forever when no guard is
/// threaded through (historical scope).
async fn session_dropped(rx: &mut Option<&mut oneshot::Receiver<()>>) {
    match rx {
        Some(rx) => {
            let _ = (&mut **rx).await;
        }
        None => std::future::pending().await,
    }
}

/// Largest data-event `source_ts` in `batch` + 1, clamped to `[f, t)`; `f`
/// when no data events arrived. The confirmed contiguous prefix of a fetch.
fn confirmed_prefix_end(max_data_ts: Option<Timestamp>, f: Timestamp, t: Timestamp) -> Timestamp {
    max_data_ts.map_or(f, |m| Timestamp(m.0.saturating_add(1).clamp(f.0, t.0)))
}

/// Partition `[from, to)` into ordered, disjoint segments. `gaps` are the
/// uncovered subranges (ordered, disjoint, within `[from, to)`) as reported by
/// [`HistoricalCache::gaps`]; everything between them is covered. Zero-width
/// pieces are never emitted.
fn tile(from: Timestamp, to: Timestamp, gaps: &[datamancer_core::GapSpan]) -> Vec<Segment> {
    let mut out = Vec::with_capacity(gaps.len() * 2 + 1);
    let mut cursor = from;
    for g in gaps {
        let g_from = g.from_source_ts;
        let g_to = g.to_source_ts;
        if g_from > cursor {
            out.push(Segment::Covered {
                from: cursor,
                to: g_from,
            });
        }
        if g_to > g_from {
            out.push(Segment::Gap {
                from: g_from,
                to: g_to,
            });
        }
        cursor = cursor.max(g_to);
    }
    if cursor < to {
        out.push(Segment::Covered { from: cursor, to });
    }
    out
}

/// Bounded FIFO of pending consumer events with honest overflow accounting.
///
/// While no consumer stream is attached, the controller buffers events here.
/// Pushing beyond capacity evicts the oldest event and extends `dropped` to
/// cover the evicted event's `source_ts` (an evicted [`ControlKind::Gap`]
/// contributes its embedded span instead, so eviction never erases loss
/// accounting); the span is surfaced as one in-band [`ControlKind::Gap`] when
/// a consumer (re)attaches.
///
/// On the resume path events are stamped at push (`emit` → `deliver` → push),
/// so eviction is both a reported gap **and** a real `seq` hole. `note_drop`
/// records the first-evicted event's `seq` in `dropped_first_seq` — FIFO
/// eviction (`pop_front`) guarantees the first eviction is the lowest `seq`, so
/// that is where the hole starts. The backfill `pending` ring pushes unstamped
/// events and never consults `dropped_first_seq`.
struct EventRing {
    capacity: usize,
    buf: std::collections::VecDeque<MarketEvent>,
    dropped: Option<GapSpan>,
    /// `seq` of the first event this ring evicted (the hole start), set once on
    /// the first eviction. `None` until an eviction occurs. Read only on the
    /// resume path, whose events are stamped at push.
    dropped_first_seq: Option<Seq>,
}

impl EventRing {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            buf: std::collections::VecDeque::new(),
            dropped: None,
            dropped_first_seq: None,
        }
    }

    fn push(&mut self, ev: MarketEvent) {
        if self.buf.len() == self.capacity
            && let Some(evicted) = self.buf.pop_front()
        {
            self.note_drop(&evicted);
        }
        self.buf.push_back(ev);
    }

    /// Extend the dropped span over an evicted event. Data events count via
    /// their `source_ts`; an evicted `Control::Gap` contributes its embedded
    /// span (it carries real loss accounting — eviction must not erase it).
    /// Other controls are skipped: their wall-clock `source_ts` would skew
    /// the market-data span.
    fn note_drop(&mut self, ev: &MarketEvent) {
        // Capture the hole start on the first eviction (any variant — controls
        // occupy `seq` slots too). FIFO eviction makes this the lowest `seq`.
        // Every variant the ring can hold is `seq`-stamped, so `seq()` is
        // `Some`; the `.expect` documents that a future metadata-only control
        // with `seq() == None` would be a deliberate re-plan point.
        self.dropped_first_seq.get_or_insert_with(|| {
            ev.seq()
                .expect("buffered data/control events are seq-stamped")
        });
        let (from, to) = if let MarketEvent::Control(Control {
            kind: ControlKind::Gap { span, .. },
            ..
        }) = ev
        {
            (span.from_source_ts, span.to_source_ts)
        } else {
            let Some(ts) = data_source_ts(ev) else { return };
            (ts, Timestamp(ts.0.saturating_add(1)))
        };
        match &mut self.dropped {
            Some(span) => {
                span.from_source_ts = span.from_source_ts.min(from);
                span.to_source_ts = span.to_source_ts.max(to);
            }
            None => {
                self.dropped = Some(GapSpan {
                    from_source_ts: from,
                    to_source_ts: to,
                });
            }
        }
    }

    fn into_parts(
        self,
    ) -> (
        Option<GapSpan>,
        Option<Seq>,
        std::collections::VecDeque<MarketEvent>,
    ) {
        (self.dropped, self.dropped_first_seq, self.buf)
    }
}

/// `source_ts` of data events only (`Trade | Quote | Bar`); `None` for
/// controls and any future non-data variants.
fn data_source_ts(ev: &MarketEvent) -> Option<Timestamp> {
    match ev {
        MarketEvent::Trade(_) | MarketEvent::Quote(_) | MarketEvent::Bar(_) => source_ts(ev),
        _ => None,
    }
}

/// Provider-local message for an in-band `ProviderError` control. The control
/// already carries the provider id separately, so `Error::Provider`'s
/// `provider {id}: …` Display prefix would duplicate it — strip it down to the
/// inner message. Other error variants keep their full Display.
fn provider_error_message(e: &Error) -> String {
    match e {
        Error::Provider { message, .. } => message.clone(),
        other => other.to_string(),
    }
}

/// Overwrite an event's `seq`. Called exactly once per event, at the source,
/// via [`Controller::stamp`] (the single counter site).
fn stamp_seq(ev: MarketEvent, seq: Seq) -> MarketEvent {
    match ev {
        MarketEvent::Trade(t) => MarketEvent::Trade(Trade { seq, ..t }),
        MarketEvent::Quote(q) => MarketEvent::Quote(Quote { seq, ..q }),
        MarketEvent::Bar(b) => MarketEvent::Bar(Bar { seq, ..b }),
        MarketEvent::Control(c) => MarketEvent::Control(Control { seq, ..c }),
        other => other,
    }
}

fn source_ts(ev: &MarketEvent) -> Option<Timestamp> {
    match ev {
        MarketEvent::Trade(t) => Some(t.source_ts),
        MarketEvent::Quote(q) => Some(q.source_ts),
        MarketEvent::Bar(b) => Some(b.source_ts),
        MarketEvent::Control(c) => Some(c.source_ts),
        _ => None,
    }
}

fn wall_clock_ts() -> Timestamp {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| {
        #[allow(
            clippy::cast_possible_truncation,
            reason = "i64 nanos since epoch representable until year 2262"
        )]
        let n = d.as_nanos() as i64;
        n
    });
    Timestamp(nanos)
}

// ---------------------------------------------------------------------------
// Reconnect policy (carried through from the previous shape; consumed by the
// Alpaca provider's streaming task).
// ---------------------------------------------------------------------------

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

#[cfg(test)]
mod tile_tests {
    use super::{Segment, tile};
    use datamancer_core::{GapSpan, Timestamp};

    fn gap(a: i64, b: i64) -> GapSpan {
        GapSpan {
            from_source_ts: Timestamp(a),
            to_source_ts: Timestamp(b),
        }
    }

    fn segs(v: &[Segment]) -> Vec<(char, i64, i64)> {
        v.iter()
            .map(|s| match *s {
                Segment::Covered { from, to } => ('C', from.0, to.0),
                Segment::Gap { from, to } => ('G', from.0, to.0),
            })
            .collect()
    }

    #[test]
    fn no_gaps_is_one_covered_segment() {
        let t = tile(Timestamp(0), Timestamp(100), &[]);
        assert_eq!(segs(&t), vec![('C', 0, 100)]);
    }

    #[test]
    fn whole_range_gap_is_one_gap_segment() {
        let t = tile(Timestamp(0), Timestamp(100), &[gap(0, 100)]);
        assert_eq!(segs(&t), vec![('G', 0, 100)]);
    }

    #[test]
    fn leading_trailing_and_middle_gaps_interleave() {
        // covered [10,20) and [40,50); gaps [0,10),[20,40),[50,60)
        let t = tile(
            Timestamp(0),
            Timestamp(60),
            &[gap(0, 10), gap(20, 40), gap(50, 60)],
        );
        assert_eq!(
            segs(&t),
            vec![
                ('G', 0, 10),
                ('C', 10, 20),
                ('G', 20, 40),
                ('C', 40, 50),
                ('G', 50, 60)
            ]
        );
    }

    #[test]
    fn gap_flush_with_start_emits_no_empty_covered() {
        // gap begins exactly at `from`: no zero-width Covered prefix.
        let t = tile(Timestamp(0), Timestamp(30), &[gap(0, 10)]);
        assert_eq!(segs(&t), vec![('G', 0, 10), ('C', 10, 30)]);
    }

    #[test]
    fn gap_flush_with_end_emits_no_empty_covered() {
        // gap ends exactly at `to`: no zero-width Covered suffix.
        let t = tile(Timestamp(0), Timestamp(30), &[gap(20, 30)]);
        assert_eq!(segs(&t), vec![('C', 0, 20), ('G', 20, 30)]);
    }
}

#[cfg(test)]
mod event_ring_tests {
    use super::{EventRing, stamp_seq};
    use datamancer_core::{
        AssetClass, Control, ControlKind, Instrument, MarketEvent, Price, ProviderId, Seq,
        Timestamp, Trade,
    };

    fn trade(ts: i64) -> MarketEvent {
        MarketEvent::Trade(Trade {
            instrument: Instrument::new(ProviderId::from_static("t"), AssetClass::Equity, "X"),
            source_ts: Timestamp(ts),
            rx_ts: Timestamp(ts),
            seq: Seq(0),
            price: Price::from_f64_round(1.0),
            size: 1,
        })
    }

    fn stamped_trade(ts: i64, seq: u64) -> MarketEvent {
        stamp_seq(trade(ts), Seq(seq))
    }

    fn control(ts: i64) -> MarketEvent {
        MarketEvent::Control(Control {
            source_ts: Timestamp(ts),
            rx_ts: Timestamp(ts),
            seq: Seq(0),
            kind: ControlKind::SessionClosing,
        })
    }

    fn gap_control(ts: i64, from: i64, to: i64) -> MarketEvent {
        use datamancer_core::GapSpan;
        MarketEvent::Control(Control {
            source_ts: Timestamp(ts),
            rx_ts: Timestamp(ts),
            seq: Seq(0),
            kind: ControlKind::Gap {
                provider: "t".to_string(),
                instrument: Instrument::new(ProviderId::from_static("t"), AssetClass::Equity, "X"),
                span: GapSpan {
                    from_source_ts: Timestamp(from),
                    to_source_ts: Timestamp(to),
                },
            },
        })
    }

    fn tss(events: &std::collections::VecDeque<MarketEvent>) -> Vec<i64> {
        events
            .iter()
            .map(|e| match e {
                MarketEvent::Trade(t) => t.source_ts.0,
                MarketEvent::Control(c) => c.source_ts.0,
                other => panic!("unexpected {other:?}"),
            })
            .collect()
    }

    #[test]
    fn under_capacity_keeps_everything_with_no_span() {
        let mut ring = EventRing::new(4);
        for ts in [100, 200, 300] {
            ring.push(trade(ts));
        }
        let (dropped, _first, events) = ring.into_parts();
        assert!(dropped.is_none());
        assert_eq!(tss(&events), vec![100, 200, 300]);
    }

    #[test]
    fn overflow_evicts_oldest_and_tracks_span() {
        let mut ring = EventRing::new(2);
        for ts in [100, 200, 300, 400] {
            ring.push(trade(ts));
        }
        let (dropped, _first, events) = ring.into_parts();
        // 100 and 200 evicted; half-open span keeps 200 inside.
        let span = dropped.expect("overflow must record a span");
        assert_eq!(span.from_source_ts.0, 100);
        assert_eq!(span.to_source_ts.0, 201);
        assert_eq!(tss(&events), vec![300, 400]);
    }

    #[test]
    fn span_covers_out_of_order_source_ts() {
        // Arrival order != source_ts order: span is min..max+1, not first..last+1.
        let mut ring = EventRing::new(1);
        for ts in [300, 100, 200, 50] {
            ring.push(trade(ts));
        }
        let (dropped, _first, events) = ring.into_parts();
        let span = dropped.expect("span");
        assert_eq!(span.from_source_ts.0, 100); // min of evicted {300, 100, 200}
        assert_eq!(span.to_source_ts.0, 301); // max of evicted + 1
        assert_eq!(tss(&events), vec![50]);
    }

    #[test]
    fn evicted_controls_do_not_extend_the_span() {
        let mut ring = EventRing::new(1);
        ring.push(control(999));
        ring.push(trade(100)); // evicts the control
        ring.push(trade(200)); // evicts trade 100
        let (dropped, _first, events) = ring.into_parts();
        let span = dropped.expect("span");
        assert_eq!(span.from_source_ts.0, 100);
        assert_eq!(span.to_source_ts.0, 101);
        assert_eq!(tss(&events), vec![200]);
    }

    #[test]
    fn evicted_gap_control_preserves_its_span() {
        // A buffered Control::Gap carries real loss accounting (e.g. it was
        // re-buffered after a failed flush). Evicting it must not erase that
        // record: its embedded span survives as the ring's dropped span.
        let mut ring = EventRing::new(1);
        ring.push(gap_control(999, 100, 201));
        ring.push(trade(500)); // evicts the gap control
        let (dropped, _first, events) = ring.into_parts();
        let span = dropped.expect("evicted gap control must preserve its span");
        assert_eq!(span.from_source_ts.0, 100);
        assert_eq!(span.to_source_ts.0, 201);
        assert_eq!(tss(&events), vec![500]);
    }

    #[test]
    fn evicted_gap_control_span_merges_with_evicted_data() {
        let mut ring = EventRing::new(1);
        ring.push(gap_control(999, 100, 201));
        ring.push(trade(300)); // evicts the gap control
        ring.push(trade(400)); // evicts trade 300
        let (dropped, _first, events) = ring.into_parts();
        let span = dropped.expect("span");
        // Union of the gap control's [100, 201) and evicted trade's [300, 301).
        assert_eq!(span.from_source_ts.0, 100);
        assert_eq!(span.to_source_ts.0, 301);
        assert_eq!(tss(&events), vec![400]);
    }

    #[test]
    fn zero_capacity_is_clamped_to_one() {
        let mut ring = EventRing::new(0);
        ring.push(trade(100));
        let (dropped, _first, events) = ring.into_parts();
        assert!(dropped.is_none());
        assert_eq!(tss(&events), vec![100]);
    }

    #[test]
    fn records_first_evicted_seq_on_overflow() {
        // Push stamped events past capacity: the first eviction's seq is the
        // hole start (FIFO eviction == lowest seq). Survivors keep push-time
        // seq; into_parts surfaces the first-evicted seq and the data span.
        let mut ring = EventRing::new(2);
        for (ts, seq) in [(100, 0), (200, 1), (300, 2), (400, 3)] {
            ring.push(stamped_trade(ts, seq));
        }
        let (dropped, first, events) = ring.into_parts();
        assert_eq!(first, Some(Seq(0)), "hole starts at the first-evicted seq");
        let span = dropped.expect("overflow must record a span");
        assert_eq!(span.from_source_ts.0, 100);
        assert_eq!(span.to_source_ts.0, 201);
        assert_eq!(tss(&events), vec![300, 400]);
    }

    #[test]
    fn no_first_evicted_seq_without_overflow() {
        let mut ring = EventRing::new(4);
        for (ts, seq) in [(100, 0), (200, 1), (300, 2)] {
            ring.push(stamped_trade(ts, seq));
        }
        let (dropped, first, events) = ring.into_parts();
        assert!(dropped.is_none());
        assert!(first.is_none(), "no eviction means no hole");
        assert_eq!(tss(&events), vec![100, 200, 300]);
    }
}

#[cfg(test)]
mod sink_tests {
    use super::InProcessSink;
    use datamancer_core::{
        AssetClass, EventSink, Instrument, MarketEvent, Price, ProviderId, PublishOutcome, Seq,
        Timestamp, Trade,
    };
    use tokio::sync::mpsc;

    fn trade(ts: i64) -> MarketEvent {
        MarketEvent::Trade(Trade {
            instrument: Instrument::new(ProviderId::from_static("t"), AssetClass::Equity, "X"),
            source_ts: Timestamp(ts),
            rx_ts: Timestamp(ts),
            seq: Seq(0),
            price: Price::from_f64_round(1.0),
            size: 1,
        })
    }

    #[tokio::test]
    async fn event_sink_in_process_round_trips() {
        let (tx, mut rx) = mpsc::channel(4);
        let sink = InProcessSink { tx };

        // Delivered while the receiver is live; the same event arrives.
        match sink.publish(trade(100)).await {
            PublishOutcome::Delivered => {}
            rejected @ PublishOutcome::Rejected(_) => {
                panic!("expected Delivered, got {rejected:?}")
            }
        }
        match rx.recv().await {
            Some(MarketEvent::Trade(t)) => assert_eq!(t.source_ts.0, 100),
            other => panic!("expected the delivered trade, got {other:?}"),
        }

        // After the receiver is dropped, publish hands the event back.
        drop(rx);
        match sink.publish(trade(200)).await {
            PublishOutcome::Rejected(MarketEvent::Trade(t)) => assert_eq!(t.source_ts.0, 200),
            other => panic!("expected Rejected(same event), got {other:?}"),
        }

        // flush is a no-op for the in-process sink.
        sink.flush().await.unwrap();
    }
}

#[cfg(test)]
mod persistence_options_tests {
    use super::PersistenceOptions;

    #[test]
    fn presets_compose_the_four_modes() {
        assert_eq!(PersistenceOptions::none(), PersistenceOptions::default());
        assert!(!PersistenceOptions::none().read_cache && !PersistenceOptions::none().write_cache);
        assert!(
            PersistenceOptions::cached().read_cache && PersistenceOptions::cached().write_cache
        );
        assert!(
            PersistenceOptions::read_only().read_cache
                && !PersistenceOptions::read_only().write_cache
        );
        assert!(
            !PersistenceOptions::refresh().read_cache && PersistenceOptions::refresh().write_cache
        );
    }

    #[test]
    fn uses_cache_is_true_when_any_axis_set() {
        assert!(!PersistenceOptions::none().uses_cache());
        assert!(PersistenceOptions::cached().uses_cache());
        assert!(PersistenceOptions::read_only().uses_cache());
        assert!(PersistenceOptions::refresh().uses_cache());
    }

    #[test]
    fn tap_log_axis_defaults_off_and_presets_stay_cache_only() {
        assert!(!PersistenceOptions::none().write_tap_log);
        assert!(!PersistenceOptions::cached().write_tap_log);
        assert!(!PersistenceOptions::read_only().write_tap_log);
        assert!(!PersistenceOptions::refresh().write_tap_log);
        assert!(!PersistenceOptions::default().write_tap_log);
    }

    #[test]
    fn with_tap_log_sets_only_the_tap_axis() {
        let opts = PersistenceOptions::none().with_tap_log(true);
        assert!(opts.write_tap_log);
        assert!(!opts.read_cache);
        assert!(!opts.write_cache);
        // Stacks onto a cache preset without disturbing the cache axes.
        let stacked = PersistenceOptions::cached().with_tap_log(true);
        assert!(stacked.read_cache && stacked.write_cache && stacked.write_tap_log);
        // uses_cache() still reflects only the cache axes.
        assert!(!PersistenceOptions::none().with_tap_log(true).uses_cache());
    }
}
