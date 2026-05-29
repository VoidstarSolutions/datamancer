//! Sessions and the top-level [`Datamancer`] orchestrator.
//!
//! A session is the unit of consumption. Each session is scoped to exactly
//! one `(instrument, kind)` pair and one [`Scope`] — bounded historical, pure
//! live, or live with a historical backfill. Construction is eager: provider
//! subscription / fetch starts immediately so the session begins capturing
//! events even if the consumer hasn't taken the [`EventStream`] yet.
//!
//! # Lifecycle
//!
//! [`Session::take_events`] is **single-shot** in this iteration. The first
//! call hands the consumer the [`EventStream`]; subsequent calls return
//! [`Error::EventsAlreadyTaken`] whether or not the stream is still alive.
//! Re-take after drop, plus the historical→live backfill seam, both depend
//! on the resume primitive (query persistence for everything since
//! `last_emitted_source_ts`, emit a [`ControlKind::Gap`] for what's missing,
//! then continue live). Until that lands, dropping a Live session's stream
//! tears the session down; if you want to keep events flowing, hold the
//! stream.
//!
//! Recording (write-through to persistence) is a separate axis. It defaults
//! to whatever was passed at construction and can be toggled at runtime via
//! [`Session::set_persistence`].
//!
//! # Auto-cleanup
//!
//! - **Live**: alive while the [`EventStream`] is held. Once the consumer
//!   drops it, the session unsubscribes upstream and shuts down.
//! - **Historical**: alive while the fetch is running. After the fetch
//!   completes the controller waits for the held stream to drain (or drop)
//!   and then shuts down. If the consumer never took the stream, the
//!   session terminates immediately when the fetch finishes — there's
//!   nobody to drain to.
//!
//! Explicit [`Session::close`] is always available for forced termination.
//!
//! # Status
//!
//! This is the captured-API-shape stage. Several internals are explicitly
//! stubbed and marked with TODO:
//!
//! - **Resume primitive.** Re-take after drop and the historical→live seam
//!   on stitched sessions both currently surface a single placeholder Gap
//!   rather than replaying-from-persistence. Once the resume primitive
//!   lands, [`Session::take_events`] will accept multiple calls (returning
//!   the receiver to the slot on `EventStream` drop), and stitched sessions
//!   will replay through the gap.
//! - **Write-through persistence.** `PersistenceOptions::cached()` is
//!   accepted at construction but events are not yet written to `TapLog` or
//!   `HistoricalCache`. Wiring lands when the resume primitive does.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use datamancer_core::{
    Bar, Control, ControlKind, Error, EventKind, HistoricalCache, HistoryRequest, Instrument,
    LiveHandle, MarketEvent, Provider, Quote, Result, Seq, TapLog, Timestamp, Trade,
};
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
/// The two cache axes compose into the full historical option space:
///
/// | `read_cache` | `write_cache` | mode      | behavior                                    |
/// |--------------|---------------|-----------|---------------------------------------------|
/// | `false`      | `false`       | ephemeral | always hit the provider, store nothing      |
/// | `true`       | `true`        | cached    | serve covered ranges, fetch & store gaps    |
/// | `true`       | `false`       | read-only | serve cache + fetch gaps, don't persist     |
/// | `false`      | `true`        | refresh   | ignore coverage, re-fetch range, overwrite  |
///
/// `#[non_exhaustive]`: later work (tap log, resume) adds axes additively.
/// Construct via the presets, or mutate the public fields on an owned value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct PersistenceOptions {
    /// Historical scope: serve covered subranges from the cache and fetch only
    /// the gaps. When false, always fetch the full range from the provider.
    pub read_cache: bool,
    /// Historical scope: write fetched gap data back to the cache.
    pub write_cache: bool,
}

impl PersistenceOptions {
    /// No persistence: always hit the provider, store nothing.
    #[must_use]
    pub const fn none() -> Self {
        Self {
            read_cache: false,
            write_cache: false,
        }
    }

    /// Read-through cache: serve covered ranges, fetch and store only gaps.
    #[must_use]
    pub const fn cached() -> Self {
        Self {
            read_cache: true,
            write_cache: true,
        }
    }

    /// Serve from cache and fetch gaps for this run, but do not persist them.
    #[must_use]
    pub const fn read_only() -> Self {
        Self {
            read_cache: true,
            write_cache: false,
        }
    }

    /// Ignore cached coverage, re-fetch the whole range, overwrite the cache.
    #[must_use]
    pub const fn refresh() -> Self {
        Self {
            read_cache: false,
            write_cache: true,
        }
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
}

type LiveSessionRegistry =
    Arc<std::sync::Mutex<HashMap<(Instrument, EventKind), Weak<RegistrySentinel>>>>;

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
        // tap_log write axis is deferred (later spec); only the cache is required here.
        if options.uses_cache() && self.inner.historical_cache.is_none() {
            return Err(Error::PersistenceRequired);
        }

        let provider = self.route(&instrument, kind)?;

        // Probe-and-reserve the live-session registry slot. Only `Scope::Live`
        // participates: concurrent historical fetches for the same pair are
        // stateless reads with no conflict. The lock is held across probe and
        // insert so two concurrent `session()` calls can't both reserve.
        let registry_anchor = if matches!(scope, Scope::Live { .. }) {
            let key = (instrument.clone(), kind);
            let mut map = self
                .inner
                .live_sessions
                .lock()
                .expect("live-session registry mutex poisoned");
            if let Some(weak) = map.get(&key)
                && weak.strong_count() > 0
            {
                return Err(Error::LiveSessionConflict { instrument, kind });
            }
            let anchor = Arc::new(RegistrySentinel {
                registry: self.inner.live_sessions.clone(),
                key: key.clone(),
            });
            map.insert(key, Arc::downgrade(&anchor));
            // Drop the guard before any await: if `provider.start_live()` /
            // `live.subscribe()` below errors, the local `anchor` drops as the
            // function returns and `RegistrySentinel::drop` clears the entry.
            drop(map);
            Some(anchor)
        } else {
            None
        };

        let (events_tx, events_rx) = mpsc::channel::<MarketEvent>(default_buffer());
        let (cmd_tx, cmd_rx) = mpsc::channel::<SessionCommand>(8);

        let inner = Arc::new(SessionInner {
            instrument: instrument.clone(),
            kind,
            scope,
            events_holder: Mutex::new(SessionStreamSlot::Available(events_rx)),
            persistence: std::sync::Mutex::new(options),
            stream_taken: std::sync::atomic::AtomicBool::new(false),
            cmd_tx,
        });

        // Provider tasks push raw events into `provider_tx`. The controller
        // drains `provider_rx`, stamps seq, optionally tees to persistence,
        // and forwards to the consumer-facing `events_tx`. Keeping these
        // separate is what gives the controller a place to interpose.
        let (provider_tx, provider_rx) = mpsc::channel::<MarketEvent>(default_buffer());

        let controller = Controller {
            inner: inner.clone(),
            provider: provider.clone(),
            tap_log: self.inner.tap_log.clone(),
            historical_cache: self.inner.historical_cache.clone(),
            events_tx,
            next_seq: 0,
            last_emitted_source_ts: None,
        };

        match scope {
            Scope::Historical { from, to } => {
                tokio::spawn(controller.run_historical(from, to, provider_tx, provider_rx, cmd_rx));
            }
            Scope::Live { backfill_from } => {
                let live = provider.start_live(provider_tx).await?;
                live.subscribe(instrument.clone(), kind).await?;
                tokio::spawn(controller.run_live(live, backfill_from, provider_rx, cmd_rx));
            }
        }

        Ok(Session {
            inner,
            _registry_anchor: registry_anchor,
        })
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
        Ok(Datamancer {
            inner: Arc::new(DatamancerInner {
                providers: self.providers,
                tap_log: self.tap_log,
                historical_cache: self.historical_cache,
                instrument_provider: self.instrument_provider,
                live_sessions: Arc::new(std::sync::Mutex::new(HashMap::new())),
            }),
        })
    }
}

// ---------------------------------------------------------------------------
// Session handle
// ---------------------------------------------------------------------------

/// A handle to a running session. Single-owner; not `Clone`.
pub struct Session {
    inner: Arc<SessionInner>,
    /// Keeps the live-session registry slot occupied for the lifetime of this
    /// `Session`. `None` for `Scope::Historical` (no registry participation).
    /// On drop, [`RegistrySentinel::drop`] clears the slot if no successor has
    /// taken it.
    _registry_anchor: Option<Arc<RegistrySentinel>>,
}

struct SessionInner {
    instrument: Instrument,
    kind: EventKind,
    scope: Scope,
    /// Holder for the consumer-facing receiver. Available when no stream is
    /// held; Taken when a stream is currently outstanding.
    events_holder: Mutex<SessionStreamSlot>,
    persistence: std::sync::Mutex<PersistenceOptions>,
    /// Set to true the first (and only) time `take_events` succeeds. The
    /// historical controller reads it after fetch completion: if the consumer
    /// never took the stream, there's nobody to drain to and the session
    /// shuts down immediately rather than hanging on `events_tx.closed()`.
    stream_taken: std::sync::atomic::AtomicBool,
    cmd_tx: mpsc::Sender<SessionCommand>,
}

enum SessionStreamSlot {
    Available(mpsc::Receiver<MarketEvent>),
    Taken,
}

impl Session {
    /// Take the event stream. **Single-shot in this iteration**: the first
    /// call returns the stream; every subsequent call returns
    /// [`Error::EventsAlreadyTaken`], whether or not the original stream is
    /// still alive.
    ///
    /// Re-take after drop is gated on the resume primitive landing (see the
    /// module-level `# Status`). Once it does, this method will accept
    /// multiple calls — each new take will query persistence for
    /// `(last_emitted_source_ts, now]`, replay what's there, emit a
    /// [`ControlKind::Gap`] for what isn't (or the entire silence if
    /// `read_cache` is off), then continue live.
    ///
    /// # Errors
    ///
    /// Returns `Error::EventsAlreadyTaken` if the stream has already been
    /// taken from this session.
    pub fn take_events(&mut self) -> Result<EventStream> {
        let mut slot = self
            .inner
            .events_holder
            .try_lock()
            .map_err(|_| Error::EventsAlreadyTaken)?;
        match std::mem::replace(&mut *slot, SessionStreamSlot::Taken) {
            SessionStreamSlot::Available(rx) => {
                self.inner
                    .stream_taken
                    .store(true, std::sync::atomic::Ordering::Release);
                Ok(EventStream { rx })
            }
            SessionStreamSlot::Taken => {
                *slot = SessionStreamSlot::Taken;
                Err(Error::EventsAlreadyTaken)
            }
        }
    }

    /// Replace the persistence options at runtime. Affects future writes;
    /// an in-flight historical fetch keeps the plan it started with.
    ///
    /// # Errors
    ///
    /// Returns `Error::PersistenceRequired` if the new options require a cache
    /// that is not configured; `Error::SessionClosed` if the controller has
    /// shut down.
    pub async fn set_persistence(&self, options: PersistenceOptions) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SessionCommand::SetPersistence(options, tx))
            .await
            .map_err(|_| Error::SessionClosed)?;
        rx.await.map_err(|_| Error::SessionClosed)?
    }

    /// Returns the current persistence options for this session.
    ///
    /// # Panics
    ///
    /// Panics if the persistence mutex is poisoned (indicates a prior panic
    /// inside a persistence-holding code path).
    #[must_use]
    pub fn persistence(&self) -> PersistenceOptions {
        *self
            .inner
            .persistence
            .lock()
            .expect("persistence mutex poisoned")
    }

    /// Explicit termination. Auto-cleanup also handles natural-completion
    /// cases (historical fetch exhausted, or live + stream-dropped +
    /// with no persistence configured).
    ///
    /// # Errors
    ///
    /// Currently infallible; the `Result` shape is reserved for future
    /// flush-error reporting from persistence sinks.
    pub async fn close(self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let _ = self.inner.cmd_tx.send(SessionCommand::Close(tx)).await;
        let _ = rx.await;
        Ok(())
    }

    #[must_use]
    pub fn instrument(&self) -> &Instrument {
        &self.inner.instrument
    }

    #[must_use]
    pub fn kind(&self) -> EventKind {
        self.inner.kind
    }

    #[must_use]
    pub fn scope(&self) -> Scope {
        self.inner.scope
    }
}

/// The session's output stream. Drop it to stop emission; re-take from the
/// owning [`Session`] if you want events again.
pub struct EventStream {
    rx: mpsc::Receiver<MarketEvent>,
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

#[derive(Debug)]
enum SessionCommand {
    SetPersistence(PersistenceOptions, oneshot::Sender<Result<()>>),
    Close(oneshot::Sender<()>),
}

// ---------------------------------------------------------------------------
// Controller
// ---------------------------------------------------------------------------

fn default_buffer() -> usize {
    1024
}

struct Controller {
    inner: Arc<SessionInner>,
    provider: Arc<dyn Provider>,
    tap_log: Option<Arc<dyn TapLog>>,
    historical_cache: Option<Arc<dyn HistoricalCache>>,
    events_tx: mpsc::Sender<MarketEvent>,
    next_seq: u64,
    /// Source-ts of the last data event handed to the consumer's stream. Used
    /// by the resume primitive on stream re-take and at the historical→live
    /// seam.
    last_emitted_source_ts: Option<Timestamp>,
}

impl Controller {
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
        let provider = self.provider.clone();
        let request = HistoryRequest {
            instrument: self.inner.instrument.clone(),
            kind: self.inner.kind,
            from,
            to,
        };
        // Spawn the fetch with `provider_tx` so it owns the only producer
        // side; when the fetch returns, the channel closes and `recv` yields
        // `None`, signalling exhaustion to the loop below.
        let fetch_task =
            tokio::spawn(async move { provider.fetch_history(request, provider_tx).await });

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
                        Some(ev) => self.forward(ev).await,
                        None => break, // fetch exhausted
                    }
                }
            }
        }

        // Fetch is done. If the consumer never took the stream, the receiver
        // is parked in `events_holder` (Slot::Available) and `events_tx.closed()`
        // would never fire — so we'd hang forever. Shut down immediately in
        // that case. Otherwise tell the consumer the historical run has
        // exhausted (so a `next().await` that's blocked waiting for more
        // events resolves to a `SessionClosing` and the consumer can drop
        // the stream), then wait for the held stream to drain or drop and
        // auto-close.
        if !self
            .inner
            .stream_taken
            .load(std::sync::atomic::Ordering::Acquire)
        {
            self.shutdown().await;
            return;
        }
        let now = wall_clock_ts();
        let seq = Seq(self.next_seq);
        self.next_seq += 1;
        let _ = self
            .events_tx
            .send(MarketEvent::Control(Control {
                source_ts: now,
                rx_ts: now,
                seq,
                kind: ControlKind::SessionClosing,
            }))
            .await;
        loop {
            tokio::select! {
                cmd = cmd_rx.recv() => {
                    if !self.handle_command(cmd).await {
                        return;
                    }
                }
                () = self.events_tx.closed() => break,
            }
        }
        self.shutdown().await;
    }

    /// Live scope: subscribe (already done by the caller), drain provider
    /// events through the seq/forward pipeline, honor commands, auto-close
    /// when the consumer drops the stream and we're not persisting.
    /// Backfill seam is stubbed — see module-level TODO.
    async fn run_live(
        mut self,
        live: Box<dyn LiveHandle>,
        backfill_from: Option<Timestamp>,
        mut provider_rx: mpsc::Receiver<MarketEvent>,
        mut cmd_rx: mpsc::Receiver<SessionCommand>,
    ) {
        if let Some(from) = backfill_from {
            // TODO(resume-primitive): kick off historical fetch from `from`
            // to live-edge, bridge the seam (extend fetch to first live
            // source_ts; emit ControlKind::Gap only if the provider can't
            // reach). For now we surface a Gap span [from, now) so the
            // placeholder is visible to consumers; live events follow.
            let now = wall_clock_ts();
            let gap = MarketEvent::Control(Control {
                source_ts: now,
                rx_ts: now,
                seq: Seq(0),
                kind: ControlKind::Gap {
                    provider: self.provider.id().to_string(),
                    instrument: self.inner.instrument.clone(),
                    span: datamancer_core::GapSpan {
                        from_source_ts: from,
                        to_source_ts: now,
                    },
                },
            });
            self.forward(gap).await;
        }

        let live = Arc::new(Mutex::new(Some(live)));
        loop {
            tokio::select! {
                cmd = cmd_rx.recv() => {
                    if !self.handle_command(cmd).await {
                        if let Some(h) = live.lock().await.take() {
                            let _ = h.unsubscribe(self.inner.instrument.clone(), self.inner.kind).await;
                            let _ = h.close().await;
                        }
                        return;
                    }
                }
                ev = provider_rx.recv() => {
                    let Some(ev) = ev else {
                        // Provider task exited unexpectedly.
                        self.shutdown().await;
                        return;
                    };
                    self.forward(ev).await;
                }
                () = self.events_tx.closed() => {
                    // Consumer dropped the stream.
                    // TODO(persistence): when write-through lands, keep
                    // running while persistence.write_cache is set so
                    // recording-only mode is observable. Today, no
                    // write-through means there's
                    // nothing useful to do without an emitting consumer.
                    if let Some(h) = live.lock().await.take() {
                        let _ = h.unsubscribe(self.inner.instrument.clone(), self.inner.kind).await;
                        let _ = h.close().await;
                    }
                    self.shutdown().await;
                    return;
                }
            }
        }
    }

    /// Stamp `seq`, optionally tee to `TapLog` (TODO), forward to the consumer
    /// stream when held. Updates `last_emitted_source_ts` on data events.
    async fn forward(&mut self, ev: MarketEvent) {
        let stamped = self.assign_seq(ev);
        if let Some(ts) = source_ts(&stamped)
            && matches!(
                stamped,
                MarketEvent::Trade(_) | MarketEvent::Quote(_) | MarketEvent::Bar(_)
            )
        {
            self.last_emitted_source_ts = Some(ts);
        }
        // TODO(persistence): tee to TapLog / HistoricalCache when persistence.uses_cache().
        let _ = self.events_tx.send(stamped).await;
    }

    fn assign_seq(&mut self, ev: MarketEvent) -> MarketEvent {
        let seq = Seq(self.next_seq);
        self.next_seq += 1;
        match ev {
            MarketEvent::Trade(t) => MarketEvent::Trade(Trade { seq, ..t }),
            MarketEvent::Quote(q) => MarketEvent::Quote(Quote { seq, ..q }),
            MarketEvent::Bar(b) => MarketEvent::Bar(Bar { seq, ..b }),
            MarketEvent::Control(c) => MarketEvent::Control(Control { seq, ..c }),
            other => other,
        }
    }

    /// Returns false if the controller should exit.
    async fn handle_command(&self, cmd: Option<SessionCommand>) -> bool {
        match cmd {
            Some(SessionCommand::SetPersistence(options, ack)) => {
                let res = self.apply_persistence(options);
                let _ = ack.send(res);
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
        *self
            .inner
            .persistence
            .lock()
            .expect("persistence mutex poisoned") = options;
        Ok(())
    }

    async fn shutdown(&self) {
        let now = wall_clock_ts();
        let _ = self
            .events_tx
            .send(MarketEvent::Control(Control {
                source_ts: now,
                rx_ts: now,
                seq: Seq(self.next_seq),
                kind: ControlKind::SessionClosing,
            }))
            .await;
        if let Some(log) = &self.tap_log {
            let _ = log.flush().await;
        }
    }
}

// ---------------------------------------------------------------------------
// Live-session registry sentinel
// ---------------------------------------------------------------------------

/// Holds a live-session registry slot for one `(instrument, kind)` pair.
///
/// Each `Session` opened with `Scope::Live` owns an `Arc<RegistrySentinel>`;
/// the registry stores a `Weak<RegistrySentinel>` so a stale entry can be
/// distinguished from a live one. When the owning `Session` drops, the last
/// `Arc` drops, this `Drop` fires, and the slot is cleared.
struct RegistrySentinel {
    registry: LiveSessionRegistry,
    key: (Instrument, EventKind),
}

impl Drop for RegistrySentinel {
    fn drop(&mut self) {
        // By the time this fires, the strong count for *this* allocation is 0.
        // If `weak.strong_count() == 0` the entry is still ours (no successor
        // has registered); remove it. If a successor has replaced our entry,
        // its sentinel is a different allocation with strong_count >= 1 and we
        // leave it alone. Poisoned-lock is benign — a stale entry is replaced
        // by the next `session()` call which sees `strong_count() == 0`.
        if let Ok(mut map) = self.registry.lock()
            && let Some(weak) = map.get(&self.key)
            && weak.strong_count() == 0
        {
            map.remove(&self.key);
        }
    }
}

/// One slice of a requested historical range: either already in the cache
/// (`Covered`) or not yet fetched (`Gap`). Half-open `[from, to)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(
    dead_code,
    reason = "consumed by the read-through cache planner (upcoming task)"
)]
enum Segment {
    Covered { from: Timestamp, to: Timestamp },
    Gap { from: Timestamp, to: Timestamp },
}

/// Partition `[from, to)` into ordered, disjoint segments. `gaps` are the
/// uncovered subranges (ordered, disjoint, within `[from, to)`) as reported by
/// [`HistoricalCache::gaps`]; everything between them is covered. Zero-width
/// pieces are never emitted.
#[allow(
    dead_code,
    reason = "consumed by the read-through cache planner (upcoming task)"
)]
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
}
