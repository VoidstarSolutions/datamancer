//! Sessions and the top-level [`Datamancer`] orchestrator.
//!
//! A session is the unit of consumption. Three constructors —
//! [`Datamancer::live`], [`Datamancer::replay`], [`Datamancer::stitched`] —
//! all return the same [`Session`] type, so consumers don't have to change
//! shape based on the data origin.
//!
//! # Internal shape
//!
//! Each session is backed by a controller task that owns:
//!
//! - the live [`LiveHandle`]s (none for replay),
//! - a single internal `mpsc::Receiver<MarketEvent>` fed by every provider
//!   sink and, for replay/stitched, the replay forwarder,
//! - a typed command receiver from the public [`Session`] handle.
//!
//! The controller drains the internal receiver, assigns a session-monotonic
//! [`Seq`] to every event, optionally tees to the configured [`TapLog`], and
//! forwards into the consumer's output [`mpsc::Receiver<MarketEvent>`] which
//! [`EventStream`] wraps.
//!
//! # Hot-path discipline
//!
//! Each provider task pushes into its own concrete `mpsc::Sender<MarketEvent>`,
//! avoiding dyn dispatch on the per-message decode loop. The controller reads
//! from concrete `Receiver`s, tags each event with a session-monotonic
//! [`Seq`](crate::Seq), and forwards into the consumer-facing channel. The
//! only remaining dyn calls are at session construction (one per provider)
//! and at subscription-mutation time.

use std::sync::Arc;

use datamancer_core::{
    Bar, Control, ControlKind, Error, EventKind, GapSpan, HistoricalCache, Instrument, LiveHandle,
    MarketEvent, Provider, Quote, ReplayRequest, ReplaySource, Result, Seq, Subscription, TapLog,
    Timestamp, Trade,
};
use futures::StreamExt;
use futures::stream::Stream;
use tokio::sync::{Mutex, mpsc, oneshot};

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
}

impl Datamancer {
    pub fn builder() -> DatamancerBuilder {
        DatamancerBuilder::default()
    }

    /// Open a live session against the configured providers.
    pub async fn live(&self, cfg: LiveConfig) -> Result<Session> {
        let buffer = effective_buffer(cfg.buffer_size);
        let (events_tx, events_rx) = mpsc::channel::<MarketEvent>(buffer);
        let (cmd_tx, cmd_rx) = mpsc::channel::<SessionCommand>(32);
        let (merge_tx, merge_rx) = mpsc::channel::<MarketEvent>(buffer);

        let mut handles: Vec<NamedHandle> = Vec::with_capacity(self.inner.providers.len());
        for p in &self.inner.providers {
            let sink = merge_tx.clone();
            let h = p.start_live(sink).await?;
            handles.push(NamedHandle {
                provider: p.clone(),
                handle: Arc::new(Mutex::new(Some(h))),
            });
        }
        // Drop our local merge_tx — the controller keeps a clone via the
        // sinks held inside the provider tasks. When all provider tasks
        // close their sinks, the merge_rx closes and the controller exits.
        drop(merge_tx);

        // Apply initial subscriptions before starting the controller — that
        // way the consumer sees a coherent ordering.
        for sub in &cfg.initial_subscriptions {
            route_subscribe(&handles, &cfg.instrument_provider, sub.clone(), true).await?;
        }

        let controller = Controller::new(
            SessionKind::Live,
            handles,
            self.inner.tap_log.clone(),
            cfg.instrument_provider.clone(),
        );
        tokio::spawn(controller.run(merge_rx, cmd_rx, events_tx));

        Ok(Session {
            cmd_tx,
            events: Some(EventStream { rx: events_rx }),
            kind: SessionKind::Live,
        })
    }

    /// Open a replay session over a previously-captured source.
    pub async fn replay(&self, cfg: ReplayConfig) -> Result<Session> {
        let buffer = effective_buffer(0);
        let (events_tx, events_rx) = mpsc::channel::<MarketEvent>(buffer);
        let (cmd_tx, cmd_rx) = mpsc::channel::<SessionCommand>(32);
        let (merge_tx, merge_rx) = mpsc::channel::<MarketEvent>(buffer);

        let source = self.open_replay_source(&cfg)?;
        let request = ReplayRequest {
            instruments: cfg.instruments.clone(),
            kinds: cfg.kinds.clone(),
            from: cfg.from,
            to: cfg.to,
        };
        let stream = source.open(request).await?;
        spawn_replay_forwarder(stream, merge_tx);

        let controller = Controller::new(SessionKind::Replay, Vec::new(), self.inner.tap_log.clone(), Vec::new());
        tokio::spawn(controller.run(merge_rx, cmd_rx, events_tx));

        Ok(Session {
            cmd_tx,
            events: Some(EventStream { rx: events_rx }),
            kind: SessionKind::Replay,
        })
    }

    /// Open a stitched session: backfill from the configured replay window,
    /// then continue live. Any gap or overlap at the seam is reported in-band
    /// as a `ControlKind::Gap` entry.
    pub async fn stitched(&self, cfg: StitchConfig) -> Result<Session> {
        let buffer = effective_buffer(cfg.live.buffer_size);
        let (events_tx, events_rx) = mpsc::channel::<MarketEvent>(buffer);
        let (cmd_tx, cmd_rx) = mpsc::channel::<SessionCommand>(32);
        // Live events go through merge_rx; backfill events go through their
        // own channel so the controller can drain backfill in full before
        // touching anything live — this is what gives stitching deterministic
        // ordering even when live events start arriving before backfill ends.
        let (merge_tx, merge_rx) = mpsc::channel::<MarketEvent>(buffer);
        let (backfill_tx, backfill_rx) = mpsc::channel::<MarketEvent>(buffer);

        let source = self.open_replay_source(&cfg.backfill)?;
        let request = ReplayRequest {
            instruments: cfg.backfill.instruments.clone(),
            kinds: cfg.backfill.kinds.clone(),
            from: cfg.backfill.from,
            to: cfg.backfill.to,
        };
        let stream = source.open(request).await?;
        spawn_replay_forwarder(stream, backfill_tx);

        let mut handles: Vec<NamedHandle> = Vec::with_capacity(self.inner.providers.len());
        for p in &self.inner.providers {
            let sink = merge_tx.clone();
            let h = p.start_live(sink).await?;
            handles.push(NamedHandle {
                provider: p.clone(),
                handle: Arc::new(Mutex::new(Some(h))),
            });
        }
        drop(merge_tx);

        for sub in &cfg.live.initial_subscriptions {
            route_subscribe(&handles, &cfg.live.instrument_provider, sub.clone(), true).await?;
        }

        let controller = Controller::new(
            SessionKind::Stitched,
            handles,
            self.inner.tap_log.clone(),
            cfg.live.instrument_provider.clone(),
        );
        tokio::spawn(controller.run_stitched(backfill_rx, merge_rx, cmd_rx, events_tx));

        Ok(Session {
            cmd_tx,
            events: Some(EventStream { rx: events_rx }),
            kind: SessionKind::Stitched,
        })
    }

    fn open_replay_source(&self, cfg: &ReplayConfig) -> Result<Box<dyn ReplaySource>> {
        match &cfg.source {
            ReplaySourceSpec::TapLog => {
                let log = self
                    .inner
                    .tap_log
                    .as_ref()
                    .ok_or_else(|| Error::Config("no TapLog configured".into()))?;
                Ok(log.as_replay_source())
            }
            ReplaySourceSpec::HistoricalCache => {
                let cache = self
                    .inner
                    .historical_cache
                    .as_ref()
                    .ok_or_else(|| Error::Config("no HistoricalCache configured".into()))?;
                // The cache's as_replay_source needs a single CacheKey. For
                // multi-instrument/multi-kind requests, the controller would
                // open multiple sources and merge — for now require the
                // request to specify exactly one instrument/kind pair, which
                // is the common research path.
                let instrument = cfg
                    .instruments
                    .first()
                    .ok_or_else(|| Error::Config("replay request needs at least one instrument".into()))?
                    .clone();
                let kind = *cfg
                    .kinds
                    .first()
                    .ok_or_else(|| Error::Config("replay request needs at least one event kind".into()))?;
                let provider = self
                    .inner
                    .providers
                    .iter()
                    .find(|p| p.supports(&instrument, kind))
                    .map(|p| p.id().to_string())
                    .unwrap_or_else(|| "unknown".to_string());
                let key = datamancer_core::CacheKey {
                    provider,
                    instrument,
                    kind,
                    from: cfg.from,
                    to: cfg.to,
                };
                Ok(cache.as_replay_source(key))
            }
            ReplaySourceSpec::Provider { id } => {
                let _ = self
                    .inner
                    .providers
                    .iter()
                    .find(|p| p.id() == id)
                    .ok_or_else(|| Error::UnknownProvider(id.clone()))?;
                Err(Error::Config(
                    "ReplaySourceSpec::Provider is not yet implemented; use HistoricalCache for now"
                        .into(),
                ))
            }
        }
    }

    /// Look up a registered provider by id. Returns `Err(UnknownProvider)`
    /// if no such provider was registered with the builder.
    pub fn provider(&self, id: &str) -> Result<&dyn Provider> {
        self.inner
            .providers
            .iter()
            .find(|p| p.id() == id)
            .map(|p| p.as_ref() as &dyn Provider)
            .ok_or_else(|| Error::UnknownProvider(id.to_string()))
    }
}

#[derive(Default)]
pub struct DatamancerBuilder {
    providers: Vec<Arc<dyn Provider>>,
    tap_log: Option<Arc<dyn TapLog>>,
    historical_cache: Option<Arc<dyn HistoricalCache>>,
}

impl DatamancerBuilder {
    /// Register a provider. Provider ids must be unique within a Datamancer
    /// instance; conflicts surface from [`build`](Self::build).
    pub fn provider(mut self, p: Box<dyn Provider>) -> Self {
        self.providers.push(Arc::from(p));
        self
    }

    /// Register a provider held behind an `Arc`. Useful when the caller
    /// keeps a reference for direct API calls (e.g. one-off historical
    /// fetches) outside of a session.
    pub fn provider_arc(mut self, p: Arc<dyn Provider>) -> Self {
        self.providers.push(p);
        self
    }

    /// Attach a tap log; every event a live session emits will be appended.
    pub fn tap_log(mut self, log: Box<dyn TapLog>) -> Self {
        self.tap_log = Some(Arc::from(log));
        self
    }

    /// Attach a historical cache; `fetch_history` calls will read-through and
    /// write-through this cache before hitting the upstream provider.
    pub fn historical_cache(mut self, cache: Box<dyn HistoricalCache>) -> Self {
        self.historical_cache = Some(Arc::from(cache));
        self
    }

    /// Same as [`historical_cache`](Self::historical_cache) but accepts an
    /// already-shared `Arc` so the cache can be queried from outside the
    /// session in addition to the controller.
    pub fn historical_cache_arc(mut self, cache: Arc<dyn HistoricalCache>) -> Self {
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

// ---------------------------------------------------------------------------
// Session handle
// ---------------------------------------------------------------------------

/// A consumer-facing handle to a running session.
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
    /// Take the event stream. Can only be called once per session.
    pub fn take_events(&mut self) -> Result<EventStream> {
        self.events.take().ok_or(Error::EventsAlreadyTaken)
    }

    pub async fn subscribe(&self, sub: Subscription) -> Result<()> {
        if matches!(self.kind, SessionKind::Replay) {
            return Err(Error::Config(
                "replay sessions fix subscriptions at construction".into(),
            ));
        }
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SessionCommand::Subscribe(sub, tx))
            .await
            .map_err(|_| Error::SessionClosed)?;
        rx.await.map_err(|_| Error::SessionClosed)?
    }

    pub async fn unsubscribe(&self, sub: Subscription) -> Result<()> {
        if matches!(self.kind, SessionKind::Replay) {
            return Err(Error::Config(
                "replay sessions fix subscriptions at construction".into(),
            ));
        }
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SessionCommand::Unsubscribe(sub, tx))
            .await
            .map_err(|_| Error::SessionClosed)?;
        rx.await.map_err(|_| Error::SessionClosed)?
    }

    pub async fn close(self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let _ = self.cmd_tx.send(SessionCommand::Close(tx)).await;
        let _ = rx.await;
        Ok(())
    }
}

#[derive(Debug)]
enum SessionCommand {
    Subscribe(Subscription, oneshot::Sender<Result<()>>),
    Unsubscribe(Subscription, oneshot::Sender<Result<()>>),
    Close(oneshot::Sender<()>),
}

/// The session's output stream.
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

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct LiveConfig {
    pub initial_subscriptions: Vec<Subscription>,
    pub instrument_provider: Vec<(Instrument, String)>,
    pub reconnect: Option<ReconnectPolicy>,
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

#[derive(Debug, Clone)]
pub struct ReplayConfig {
    pub source: ReplaySourceSpec,
    pub instruments: Vec<Instrument>,
    pub kinds: Vec<EventKind>,
    pub from: Timestamp,
    pub to: Timestamp,
}

#[derive(Debug, Clone)]
pub enum ReplaySourceSpec {
    TapLog,
    HistoricalCache,
    Provider { id: String },
}

#[derive(Debug, Clone)]
pub struct StitchConfig {
    pub backfill: ReplayConfig,
    pub live: LiveConfig,
}

// ---------------------------------------------------------------------------
// Controller and helpers
// ---------------------------------------------------------------------------

fn effective_buffer(requested: usize) -> usize {
    if requested == 0 { 1024 } else { requested }
}

#[derive(Clone)]
struct NamedHandle {
    provider: Arc<dyn Provider>,
    handle: Arc<Mutex<Option<Box<dyn LiveHandle>>>>,
}

struct Controller {
    kind: SessionKind,
    handles: Vec<NamedHandle>,
    tap_log: Option<Arc<dyn TapLog>>,
    instrument_provider: Vec<(Instrument, String)>,
    /// Session-monotonic sequence counter.
    next_seq: u64,
    /// For stitched sessions: last source-ts seen in the backfill stream
    /// (used to detect a gap when live takes over).
    last_backfill_ts: Option<Timestamp>,
    /// Whether we've already emitted (or decided not to emit) a seam-Gap
    /// for the stitched session.
    stitched_seam_handled: bool,
}

impl Controller {
    fn new(
        kind: SessionKind,
        handles: Vec<NamedHandle>,
        tap_log: Option<Arc<dyn TapLog>>,
        instrument_provider: Vec<(Instrument, String)>,
    ) -> Self {
        Self {
            kind,
            handles,
            tap_log,
            instrument_provider,
            next_seq: 0,
            last_backfill_ts: None,
            stitched_seam_handled: false,
        }
    }

    /// Live and replay sessions share this loop: drain `merge_rx`, assign
    /// seq, optionally tee, forward.
    async fn run(
        mut self,
        mut merge_rx: mpsc::Receiver<MarketEvent>,
        mut cmd_rx: mpsc::Receiver<SessionCommand>,
        events_tx: mpsc::Sender<MarketEvent>,
    ) {
        let mut sealed_seam: Option<Timestamp> = None;
        loop {
            tokio::select! {
                cmd = cmd_rx.recv() => {
                    if !self.handle_command(cmd, &events_tx).await {
                        return;
                    }
                }
                ev = merge_rx.recv() => {
                    let Some(ev) = ev else { return };
                    self.handle_live_or_replay_event(ev, &mut sealed_seam, &events_tx).await;
                }
            }
        }
    }

    /// Stitched sessions: phase 1 drains backfill in full, phase 2 switches
    /// to live with optional seam-gap insertion.
    async fn run_stitched(
        mut self,
        mut backfill_rx: mpsc::Receiver<MarketEvent>,
        mut merge_rx: mpsc::Receiver<MarketEvent>,
        mut cmd_rx: mpsc::Receiver<SessionCommand>,
        events_tx: mpsc::Sender<MarketEvent>,
    ) {
        // Phase 1: backfill. Concurrently honor commands so the consumer can
        // close the session early; live events accumulate in `merge_rx`'s
        // buffer untouched.
        loop {
            tokio::select! {
                biased;
                cmd = cmd_rx.recv() => {
                    if !self.handle_command(cmd, &events_tx).await {
                        return;
                    }
                }
                ev = backfill_rx.recv() => {
                    match ev {
                        Some(ev) => self.handle_backfill_event(ev, &events_tx).await,
                        None => break, // backfill exhausted
                    }
                }
            }
        }

        let mut sealed_seam: Option<Timestamp> = self.last_backfill_ts;

        // Phase 2: live.
        loop {
            tokio::select! {
                cmd = cmd_rx.recv() => {
                    if !self.handle_command(cmd, &events_tx).await {
                        return;
                    }
                }
                ev = merge_rx.recv() => {
                    let Some(ev) = ev else { return };
                    self.handle_live_or_replay_event(ev, &mut sealed_seam, &events_tx).await;
                }
            }
        }
    }

    /// Returns false if the controller should exit.
    async fn handle_command(
        &self,
        cmd: Option<SessionCommand>,
        events_tx: &mpsc::Sender<MarketEvent>,
    ) -> bool {
        match cmd {
            Some(SessionCommand::Subscribe(sub, ack)) => {
                let res =
                    route_subscribe(&self.handles, &self.instrument_provider, sub, true).await;
                let _ = ack.send(res);
                true
            }
            Some(SessionCommand::Unsubscribe(sub, ack)) => {
                let res =
                    route_subscribe(&self.handles, &self.instrument_provider, sub, false).await;
                let _ = ack.send(res);
                true
            }
            Some(SessionCommand::Close(ack)) => {
                self.shutdown(events_tx).await;
                let _ = ack.send(());
                false
            }
            None => {
                self.shutdown(events_tx).await;
                false
            }
        }
    }

    async fn handle_backfill_event(&mut self, ev: MarketEvent, events_tx: &mpsc::Sender<MarketEvent>) {
        if let Some(ts) = source_ts(&ev)
            && self.last_backfill_ts.map(|prev| ts > prev).unwrap_or(true)
        {
            self.last_backfill_ts = Some(ts);
        }
        let stamped = self.assign_seq(ev);
        self.tee_to_log(&stamped).await;
        let _ = events_tx.send(stamped).await;
    }

    async fn handle_live_or_replay_event(
        &mut self,
        ev: MarketEvent,
        sealed_seam: &mut Option<Timestamp>,
        events_tx: &mpsc::Sender<MarketEvent>,
    ) {
        // Stitched: insert a seam Gap on the very first live event if its
        // source_ts is past last_backfill_ts. We only do this once.
        if matches!(self.kind, SessionKind::Stitched) && !self.stitched_seam_handled {
            let live_ts = source_ts(&ev);
            if let (Some(seam), Some(now)) = (*sealed_seam, live_ts)
                && now.0 > seam.0
            {
                let gap_ev = self.build_gap_control(seam, now, &ev);
                let stamped = self.assign_seq(gap_ev);
                self.tee_to_log(&stamped).await;
                let _ = events_tx.send(stamped).await;
            }
            self.stitched_seam_handled = true;
            *sealed_seam = None;
        }
        let stamped = self.assign_seq(ev);
        self.tee_to_log(&stamped).await;
        let _ = events_tx.send(stamped).await;
    }

    fn build_gap_control(
        &self,
        from: Timestamp,
        to: Timestamp,
        first_live: &MarketEvent,
    ) -> MarketEvent {
        let provider = first_live_provider_id(&self.handles).unwrap_or_else(|| "unknown".into());
        let instrument = event_instrument(first_live).unwrap_or_else(|| Instrument::new("unknown"));
        MarketEvent::Control(Control {
            source_ts: to,
            rx_ts: to,
            seq: Seq(0),
            kind: ControlKind::Gap {
                provider,
                instrument,
                span: GapSpan {
                    from_source_ts: from,
                    to_source_ts: to,
                },
            },
        })
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

    async fn tee_to_log(&self, ev: &MarketEvent) {
        if let Some(log) = &self.tap_log {
            let _ = log.append(ev).await;
        }
    }

    async fn shutdown(&self, events_tx: &mpsc::Sender<MarketEvent>) {
        let now = wall_clock_ts();
        let _ = events_tx
            .send(MarketEvent::Control(Control {
                source_ts: now,
                rx_ts: now,
                seq: Seq(self.next_seq),
                kind: ControlKind::SessionClosing,
            }))
            .await;
        for h in &self.handles {
            let mut guard = h.handle.lock().await;
            if let Some(handle) = guard.take() {
                let _ = handle.close().await;
            }
        }
        if let Some(log) = &self.tap_log {
            let _ = log.flush().await;
        }
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

fn event_instrument(ev: &MarketEvent) -> Option<Instrument> {
    match ev {
        MarketEvent::Trade(t) => Some(t.instrument.clone()),
        MarketEvent::Quote(q) => Some(q.instrument.clone()),
        MarketEvent::Bar(b) => Some(b.instrument.clone()),
        _ => None,
    }
}

fn first_live_provider_id(handles: &[NamedHandle]) -> Option<String> {
    handles.first().map(|h| h.provider.id().to_string())
}

fn wall_clock_ts() -> Timestamp {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0);
    Timestamp(nanos)
}

async fn route_subscribe(
    handles: &[NamedHandle],
    pinning: &[(Instrument, String)],
    sub: Subscription,
    add: bool,
) -> Result<()> {
    if handles.is_empty() {
        return Err(Error::Config("no providers registered".into()));
    }
    // Group sub.kinds by which provider serves them.
    use std::collections::HashMap;
    let mut by_provider: HashMap<String, Vec<EventKind>> = HashMap::new();
    let pinned = pinning
        .iter()
        .find(|(i, _)| *i == sub.instrument)
        .map(|(_, p)| p.as_str());

    for kind in &sub.kinds {
        let chosen = if let Some(id) = pinned {
            handles
                .iter()
                .find(|h| h.provider.id() == id && h.provider.supports(&sub.instrument, *kind))
        } else {
            handles
                .iter()
                .find(|h| h.provider.supports(&sub.instrument, *kind))
        };
        let Some(named) = chosen else {
            return Err(Error::UnsupportedEventKind {
                kind: *kind,
                instrument: sub.instrument.clone(),
            });
        };
        by_provider
            .entry(named.provider.id().to_string())
            .or_default()
            .push(*kind);
    }

    for (pid, kinds) in by_provider {
        let named = handles
            .iter()
            .find(|h| h.provider.id() == pid)
            .expect("by_provider id was just produced from handles");
        let scoped = Subscription::new(sub.instrument.clone(), kinds);
        let guard = named.handle.lock().await;
        let handle = guard.as_ref().ok_or(Error::SessionClosed)?;
        if add {
            handle.subscribe(scoped).await?;
        } else {
            handle.unsubscribe(scoped).await?;
        }
    }
    Ok(())
}

fn spawn_replay_forwarder(
    mut stream: futures::stream::BoxStream<'static, MarketEvent>,
    sink: mpsc::Sender<MarketEvent>,
) {
    tokio::spawn(async move {
        while let Some(ev) = stream.next().await {
            if sink.send(ev).await.is_err() {
                return;
            }
        }
    });
}

