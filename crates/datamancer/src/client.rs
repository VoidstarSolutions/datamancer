//! Client sessions, authoritative sessions, and the multiplex controller.
//!
//! Phase 2 introduces the **client session** as the primary consumer handle: it
//! holds a mutable set of `(instrument, kind)` subscriptions and presents **one
//! multiplexed stream** combining them. The authoritative per-`(instrument,
//! kind)` session is the deterministic singleton that owns the provider
//! connection and stamps `seq` at the source; client sessions (and the retained
//! single-pair [`crate::Session`] on its live path) are **refcounted
//! referrers** onto it.
//!
//! # Ordering
//!
//! The multiplex **interleaves** — it does not merge-sort. The ordering key is
//! `(instrument, kind, seq)`: monotonic *within* each `(instrument, kind)` pair
//! (source-stamped, from Phase 1), arrival-order across pairs. There is no
//! cross-pair order to compute, which is what makes the interleave cheap. A
//! `Gap` names the instrument only, so with two kinds on one instrument it does
//! not say which kind.
//!
//! # Lifecycle
//!
//! The authoritative session lives while at least one referrer holds a
//! [`SubscriberGuard`]. The authoritative controller holds a **`Weak`** view of
//! its own liveness — it never pins a strong `Arc<AuthoritativeSession>` (that
//! would deadlock refcounted teardown) and instead treats **fan-out-map
//! emptiness** as the teardown trigger. When the last referrer drops, the
//! controller runs upstream `unsubscribe` + `close` and exits, and the
//! `Arc<AuthoritativeSession>` drop clears the registry slot.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use datamancer_core::{
    ClientSessionId, Control, ControlKind, Error, ErrorOrigin, EventKind, FanoutId, GapSpan,
    Instrument, LiveScope, MarketEvent, Result, Seq, StreamConnection, Subscribed, SubscriptionRef,
    Timestamp,
};
use futures::StreamExt as _;
use futures::stream::FuturesUnordered;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_stream::StreamMap;
use tokio_stream::wrappers::ReceiverStream;

use crate::session::{
    Caller, ClientSessionRegistry, Datamancer, EventStream, LiveSessionRegistry,
    PersistenceOptions, SessionCommand, default_buffer,
};

// ---------------------------------------------------------------------------
// Authoritative session
// ---------------------------------------------------------------------------

/// Identifies one fan-out subscriber on an [`AuthoritativeSession`]. Allocated
/// from the session's `AtomicU64`; meaningful only within that session.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct SubscriberId(pub(crate) u64);

/// The deterministic per-`(instrument, kind)` singleton. Owns the provider
/// connection lifecycle (via its controller), stamps `seq` at the source, tees
/// to the tap log once, and fans the stamped stream out to every referrer.
///
/// Held behind `Arc`; refcounted by [`SubscriberGuard`]s. The registry stores a
/// `Weak` so a stale slot is distinguishable from a live one. This type absorbs
/// the former `RegistrySentinel`: its `Drop` clears the registry slot when no
/// successor has taken it.
pub(crate) struct AuthoritativeSession {
    // `instrument`/`kind`/`stats` are read by the Phase 3 diagnostics snapshot
    // (it iterates the registry); Phase 2 only writes them.
    pub(crate) instrument: Instrument,
    pub(crate) kind: EventKind,
    pub(crate) provider_id: String,
    /// The session's actual scope, shared across every referrer. A second opener
    /// attaches to this (its requested scope is not re-applied), so referrer
    /// handles must report *this*, not their own requested value.
    pub(crate) scope: LiveScope,
    /// The session's current persistence options, shared across every referrer
    /// and the source of truth for the synchronous getter. Updated by
    /// [`Self::set_persistence`]; a second opener never re-applies its own
    /// requested options, so a referrer must report this rather than a stale
    /// per-handle copy.
    persistence: std::sync::Mutex<PersistenceOptions>,
    /// Command channel to the authoritative controller (`AddSubscriber`,
    /// `SetPersistence`).
    cmd_tx: mpsc::Sender<SessionCommand>,
    /// Subscriber-removal channel. Unbounded so the synchronous
    /// [`SubscriberGuard::drop`] can signal removal without blocking.
    remove_tx: mpsc::UnboundedSender<SubscriberId>,
    next_subscriber_id: AtomicU64,
    pub(crate) stats: Arc<LiveStats>,
    registry: LiveSessionRegistry,
    key: (Instrument, EventKind),
}

impl AuthoritativeSession {
    #[allow(
        clippy::too_many_arguments,
        reason = "internal constructor wiring the authoritative session's collaborators"
    )]
    pub(crate) fn new(
        instrument: Instrument,
        kind: EventKind,
        provider_id: String,
        scope: LiveScope,
        persistence: PersistenceOptions,
        cmd_tx: mpsc::Sender<SessionCommand>,
        remove_tx: mpsc::UnboundedSender<SubscriberId>,
        stats: Arc<LiveStats>,
        registry: LiveSessionRegistry,
        key: (Instrument, EventKind),
    ) -> Self {
        Self {
            instrument,
            kind,
            provider_id,
            scope,
            persistence: std::sync::Mutex::new(persistence),
            cmd_tx,
            remove_tx,
            next_subscriber_id: AtomicU64::new(0),
            stats,
            registry,
            key,
        }
    }

    /// Allocate a fresh subscriber id. Used to pre-seed the opener into the
    /// fan-out *before* the controller is spawned, so events emitted during a
    /// creation-time backfill are never fanned out to an empty set.
    pub(crate) fn alloc_subscriber_id(&self) -> SubscriberId {
        SubscriberId(self.next_subscriber_id.fetch_add(1, Ordering::Relaxed))
    }

    /// Register a new fan-out subscriber, returning its id and the receiver the
    /// referrer drains. An unseeded one that joins while the seed is pending
    /// starts after the seed's slot. Fails with [`Error::SessionClosed`] if the
    /// controller has already torn down.
    pub(crate) async fn add_subscriber(&self, seeded: bool) -> Result<NewSubscriber> {
        let id = SubscriberId(self.next_subscriber_id.fetch_add(1, Ordering::Relaxed));
        let (tx, rx) = mpsc::channel(default_buffer());
        let (ack, ack_rx) = oneshot::channel();
        self.cmd_tx
            .send(SessionCommand::AddSubscriber {
                id,
                sender: tx,
                seeded,
                ack,
            })
            .await
            .map_err(|_| Error::SessionClosed)?;
        let state = ack_rx.await.map_err(|_| Error::SessionClosed)?;
        Ok(NewSubscriber {
            id,
            rx,
            backfill_in_flight: state.backfill_in_flight,
            seed_pending: state.seed_pending,
        })
    }

    /// Signal removal of a subscriber. Best-effort and non-blocking — invoked
    /// from [`SubscriberGuard::drop`].
    fn remove_subscriber(&self, id: SubscriberId) {
        let _ = self.remove_tx.send(id);
    }

    /// Replace the authoritative session's persistence options. Shared across
    /// all referrers (the tap-log tee is a property of the singleton). On
    /// success the shared copy is updated so every referrer's `persistence()`
    /// getter reflects it.
    pub(crate) async fn set_persistence(&self, options: PersistenceOptions) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SessionCommand::SetPersistence(options, tx))
            .await
            .map_err(|_| Error::SessionClosed)?;
        rx.await.map_err(|_| Error::SessionClosed)??;
        *self.persistence.lock().expect("persistence mutex poisoned") = options;
        Ok(())
    }

    /// The session's current persistence options (shared across referrers).
    pub(crate) fn persistence(&self) -> PersistenceOptions {
        *self.persistence.lock().expect("persistence mutex poisoned")
    }
}

impl Drop for AuthoritativeSession {
    fn drop(&mut self) {
        // The folded `RegistrySentinel` logic: by the time this fires the strong
        // count for this allocation is 0. If the slot still points at a `Weak`
        // whose `strong_count() == 0`, it is still ours (no successor took it) —
        // remove it. A successor's entry is a different allocation with
        // `strong_count() >= 1`; leave it alone.
        if let Ok(mut map) = self.registry.lock()
            && let Some(weak) = map.get(&self.key)
            && weak.strong_count() == 0
        {
            map.remove(&self.key);
        }
    }
}

/// RAII referrer onto an [`AuthoritativeSession`]. Holds a strong `Arc` (keeping
/// the registry slot non-stale) and the subscriber id. On drop it queues a
/// `RemoveSubscriber`; when the authoritative fan-out empties, the controller
/// tears the upstream connection down.
pub(crate) struct SubscriberGuard {
    authoritative: Arc<AuthoritativeSession>,
    id: SubscriberId,
}

impl SubscriberGuard {
    pub(crate) fn new(authoritative: Arc<AuthoritativeSession>, id: SubscriberId) -> Self {
        Self { authoritative, id }
    }

    pub(crate) fn provider(&self) -> &str {
        &self.authoritative.provider_id
    }
}

impl Drop for SubscriberGuard {
    fn drop(&mut self) {
        self.authoritative.remove_subscriber(self.id);
    }
}

/// Resolves once an attached sink has room.
async fn sink_room(sink: &ClientSink) {
    if let ClientSink::Attached(tx) = sink {
        let _ = tx.reserve().await;
    }
}

/// How long closing waits for a consumer to take what it is owed.
const CLOSE_DELIVERY_WAIT: Duration = Duration::from_secs(2);

/// How many client-local controls may wait behind a drained ring.
const CONTROL_BACKLOG: usize = 1024;

/// A subscriber the controller has added.
pub(crate) struct NewSubscriber {
    pub(crate) id: SubscriberId,
    pub(crate) rx: mpsc::Receiver<MarketEvent>,
    pub(crate) backfill_in_flight: bool,
    pub(crate) seed_pending: bool,
}

/// One subscriber registered on an [`AuthoritativeSession`].
pub(crate) struct Attached {
    pub(crate) session: Arc<AuthoritativeSession>,
    pub(crate) guard: SubscriberGuard,
    pub(crate) rx: mpsc::Receiver<MarketEvent>,
    pub(crate) outcome: Subscribed,
}

impl Attached {
    pub(crate) fn joined(session: Arc<AuthoritativeSession>, subscriber: NewSubscriber) -> Self {
        let mut outcome = Subscribed::joined(session.scope);
        if subscriber.backfill_in_flight {
            outcome = outcome.with_backfill_in_flight();
        }
        if subscriber.seed_pending {
            outcome = outcome.with_seed_pending();
        }
        Self {
            guard: SubscriberGuard::new(session.clone(), subscriber.id),
            rx: subscriber.rx,
            outcome,
            session,
        }
    }
}

// ---------------------------------------------------------------------------
// Live stats (per-symbol atomics; Phase 3 reads these)
// ---------------------------------------------------------------------------

/// Per-symbol, per-field atomic counters on an [`AuthoritativeSession`].
///
/// Lock-free reads for the Phase 3 diagnostics plane; **no composite-consistency
/// guarantee** across fields (each is read independently). Phase 2 only updates
/// them; Phase 3 adds the snapshot readers.
pub(crate) struct LiveStats {
    has_seq: AtomicBool,
    last_seq: AtomicU64,
    has_ts: AtomicBool,
    last_source_ts: AtomicI64,
    last_rx_ts: AtomicI64,
    gap_count: AtomicU64,
    /// Recent gap spans (bounded ring; cold mutex, written only on Gap).
    recent_gaps: std::sync::Mutex<VecDeque<GapSpan>>,
    has_gap_rx: AtomicBool,
    last_gap_rx_ts: AtomicI64,
    backfilling: AtomicBool,
    /// Live fan-out subscriber count (referrers actually attached). Distinct
    /// from the registry `Arc` strong count, which over-counts (a single
    /// referrer holds several strong `Arc<AuthoritativeSession>`).
    subscribers: AtomicU64,
    /// This substream's connection phase from its in-band connect/disconnect
    /// controls (cold mutex, written only on those controls).
    connection: std::sync::Mutex<StreamConnection>,
}

/// Bounded per-symbol recent-gap detail (oldest evicted).
const RECENT_GAPS_CAP: usize = 8;

/// What a connect/disconnect control did to a substream's phase
/// ([`LiveStats::record_connection`]); the controller folds the provider-level
/// consequences (`ever_connected`, `reconnects`) from it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConnectionChange {
    /// No transition: a duplicate connect, a repeated disconnect, or a control
    /// that is not connection evidence.
    None,
    /// `Pending`/`Down` → `Up`; `reconnect` when it was `Down`.
    Connected { reconnect: bool },
    /// `Pending`/`Up` → `Down`.
    Disconnected,
}

impl LiveStats {
    pub(crate) fn new() -> Self {
        Self {
            has_seq: AtomicBool::new(false),
            last_seq: AtomicU64::new(0),
            has_ts: AtomicBool::new(false),
            last_source_ts: AtomicI64::new(0),
            last_rx_ts: AtomicI64::new(0),
            gap_count: AtomicU64::new(0),
            recent_gaps: std::sync::Mutex::new(VecDeque::new()),
            has_gap_rx: AtomicBool::new(false),
            last_gap_rx_ts: AtomicI64::new(0),
            backfilling: AtomicBool::new(false),
            subscribers: AtomicU64::new(0),
            connection: std::sync::Mutex::new(StreamConnection::Pending),
        }
    }

    /// Record the current fan-out subscriber count (set after each add/remove).
    pub(crate) fn set_subscribers(&self, n: usize) {
        self.subscribers
            .store(u64::try_from(n).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    /// Current fan-out subscriber count.
    pub(crate) fn subscriber_count(&self) -> u64 {
        self.subscribers.load(Ordering::Relaxed)
    }

    /// Record one fan-out event: advance the last-`seq`/timestamps and bump the
    /// gap counter on a `Control::Gap`.
    pub(crate) fn record_event(&self, ev: &MarketEvent) {
        self.record_seq(ev);
        match ev {
            MarketEvent::Trade(_) | MarketEvent::Quote(_) | MarketEvent::Bar(_) => {
                if let (Some(source), Some(rx)) = (data_source_ts(ev), data_rx_ts(ev)) {
                    self.last_source_ts.store(source.0, Ordering::Relaxed);
                    self.last_rx_ts.store(rx.0, Ordering::Relaxed);
                    self.has_ts.store(true, Ordering::Relaxed);
                }
            }
            MarketEvent::Control(c) => match &c.kind {
                ControlKind::Gap { span, .. } => {
                    self.gap_count.fetch_add(1, Ordering::Relaxed);
                    self.last_gap_rx_ts.store(c.rx_ts.0, Ordering::Relaxed);
                    self.has_gap_rx.store(true, Ordering::Relaxed);
                    if let Ok(mut ring) = self.recent_gaps.lock() {
                        if ring.len() == RECENT_GAPS_CAP {
                            ring.pop_front();
                        }
                        ring.push_back(span.clone());
                    }
                }
                // The connection phase is recorded at receipt
                // (`record_connection`), not at delivery: a control held in the
                // backfill ring must still count, and one the ring evicts must
                // not be lost. A `ProviderError` is connection-scoped and is the
                // provider's `last_error`, not per-stream evidence.
                ControlKind::ProviderConnected { .. }
                | ControlKind::ProviderDisconnected { .. }
                | ControlKind::ProviderError { .. }
                | ControlKind::SubscriptionChanged { .. }
                | ControlKind::SessionClosing => {}
            },
            _ => {}
        }
    }

    /// Record one replayed event: its `seq` position, and a control as
    /// [`Self::record_event`] does, with no data timestamps.
    pub(crate) fn record_replay(&self, ev: &MarketEvent) {
        if matches!(ev, MarketEvent::Control(_)) {
            self.record_event(ev);
        } else {
            self.record_seq(ev);
        }
    }

    /// Record the pure-live latest-value seed: its `seq` position only. The
    /// seed is a `Provider::latest` snapshot, not a live arrival, so it leaves
    /// the last timestamps (and the latency derived from them) unset until the
    /// first live data event.
    pub(crate) fn record_seed(&self, ev: &MarketEvent) {
        self.record_seq(ev);
    }

    fn record_seq(&self, ev: &MarketEvent) {
        if let Some(seq) = ev.seq() {
            self.last_seq.store(seq.0, Ordering::Relaxed);
            self.has_seq.store(true, Ordering::Relaxed);
        }
    }

    /// Last source-stamped `seq` seen, or `None` before any event.
    pub(crate) fn seq_position(&self) -> Option<Seq> {
        self.has_seq
            .load(Ordering::Relaxed)
            .then(|| Seq(self.last_seq.load(Ordering::Relaxed)))
    }

    /// Last live data-event `source_ts`, or `None` before any live data event
    /// (the latest-value seed is not one; see [`Self::record_seed`]).
    pub(crate) fn last_source_ts(&self) -> Option<Timestamp> {
        self.has_ts
            .load(Ordering::Relaxed)
            .then(|| Timestamp(self.last_source_ts.load(Ordering::Relaxed)))
    }

    /// Last live data-event `rx_ts`, or `None` before any live data event.
    pub(crate) fn last_rx_ts(&self) -> Option<Timestamp> {
        self.has_ts
            .load(Ordering::Relaxed)
            .then(|| Timestamp(self.last_rx_ts.load(Ordering::Relaxed)))
    }

    /// Cumulative `Control::Gap` count for this symbol.
    pub(crate) fn gap_count(&self) -> u64 {
        self.gap_count.load(Ordering::Relaxed)
    }

    /// Recent gap spans, oldest first (bounded at `RECENT_GAPS_CAP`).
    pub(crate) fn recent_gaps(&self) -> Vec<GapSpan> {
        self.recent_gaps
            .lock()
            .map(|ring| ring.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Wall-clock receipt of the most recent gap, or `None` before any.
    pub(crate) fn last_gap_rx_ts(&self) -> Option<Timestamp> {
        self.has_gap_rx
            .load(Ordering::Relaxed)
            .then(|| Timestamp(self.last_gap_rx_ts.load(Ordering::Relaxed)))
    }

    /// Whether a historical→live backfill is currently in progress.
    pub(crate) fn backfilling(&self) -> bool {
        self.backfilling.load(Ordering::Relaxed)
    }

    /// Mark backfill in progress (set by `run_backfill`, cleared at the seam
    /// flush and on every backfill exit path).
    pub(crate) fn set_backfilling(&self, active: bool) {
        self.backfilling.store(active, Ordering::Relaxed);
    }

    /// Record a provider connect/disconnect control **at receipt** and report
    /// the phase transition it caused. This is the one record of the
    /// substream's connection; the provider's state is derived from it at
    /// snapshot time, so there is no parallel counter to drift.
    ///
    /// Transitions: `Pending`/`Down` → `Up` on a connect (a reconnect when it
    /// was `Down`); `Pending`/`Up` → `Down` on a disconnect. A connect while
    /// already `Up` is a duplicate and changes nothing. A disconnect while
    /// already `Down` keeps `since` (the outage start) and adopts the newer
    /// `cause`, so a connect failure reclassified as an auth rejection shows
    /// as one without restarting the clock. Other controls are not connection
    /// evidence and return [`ConnectionChange::None`].
    pub(crate) fn record_connection(&self, c: &Control) -> ConnectionChange {
        let Ok(mut slot) = self.connection.lock() else {
            return ConnectionChange::None;
        };
        match (&c.kind, *slot) {
            (ControlKind::ProviderConnected { .. }, StreamConnection::Up { .. }) => {
                ConnectionChange::None
            }
            (ControlKind::ProviderConnected { .. }, previous) => {
                *slot = StreamConnection::Up { since: c.rx_ts };
                ConnectionChange::Connected {
                    reconnect: matches!(previous, StreamConnection::Down { .. }),
                }
            }
            (
                ControlKind::ProviderDisconnected { cause, .. },
                StreamConnection::Down { since, .. },
            ) => {
                *slot = StreamConnection::Down {
                    since,
                    cause: *cause,
                };
                ConnectionChange::None
            }
            (ControlKind::ProviderDisconnected { cause, .. }, _) => {
                *slot = StreamConnection::Down {
                    since: c.rx_ts,
                    cause: *cause,
                };
                ConnectionChange::Disconnected
            }
            _ => ConnectionChange::None,
        }
    }

    /// This substream's connection phase (`Pending` before any connect).
    pub(crate) fn connection(&self) -> StreamConnection {
        self.connection
            .lock()
            .map_or(StreamConnection::Pending, |slot| *slot)
    }
}

fn data_source_ts(ev: &MarketEvent) -> Option<Timestamp> {
    match ev {
        MarketEvent::Trade(t) => Some(t.source_ts),
        MarketEvent::Quote(q) => Some(q.source_ts),
        MarketEvent::Bar(b) => Some(b.source_ts),
        _ => None,
    }
}

fn data_rx_ts(ev: &MarketEvent) -> Option<Timestamp> {
    match ev {
        MarketEvent::Trade(t) => Some(t.rx_ts),
        MarketEvent::Quote(q) => Some(q.rx_ts),
        MarketEvent::Bar(b) => Some(b.rx_ts),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Client stats (per-client resume-buffer + subscription view; Phase 3 reads)
// ---------------------------------------------------------------------------

/// Per-client-session introspection state, written by the [`ClientController`]
/// and read (sampled) by the Phase 3 diagnostics snapshot. Held strong by the
/// controller; the registry keeps a `Weak`, so a finished controller drops out
/// automatically (see [`Drop`]).
pub(crate) struct ClientStats {
    id: ClientSessionId,
    /// Resume-buffer capacity (events) — the builder knob, constant per client.
    capacity: usize,
    /// Current per-client resume-buffer occupancy: the detached ring, or the
    /// backlog owed to an attached consumer.
    occupancy: AtomicUsize,
    /// Cumulative events evicted from the resume buffer (overflow).
    dropped_events: AtomicU64,
    /// Current subscription set, mirrored from the controller's `entries`.
    subscriptions: std::sync::Mutex<Vec<(Instrument, EventKind)>>,
    registry: ClientSessionRegistry,
}

impl ClientStats {
    pub(crate) fn new(
        id: ClientSessionId,
        capacity: usize,
        registry: ClientSessionRegistry,
    ) -> Self {
        Self {
            id,
            capacity,
            occupancy: AtomicUsize::new(0),
            dropped_events: AtomicU64::new(0),
            subscriptions: std::sync::Mutex::new(Vec::new()),
            registry,
        }
    }

    /// Record `n` evicted events (resume-buffer overflow).
    pub(crate) fn record_drops(&self, n: u64) {
        if n > 0 {
            self.dropped_events.fetch_add(n, Ordering::Relaxed);
        }
    }

    /// Set the current resume-buffer occupancy.
    pub(crate) fn set_occupancy(&self, n: usize) {
        self.occupancy.store(n, Ordering::Relaxed);
    }

    /// Mirror the controller's current subscription set.
    pub(crate) fn set_subscriptions(&self, subs: Vec<(Instrument, EventKind)>) {
        if let Ok(mut slot) = self.subscriptions.lock() {
            *slot = subs;
        }
    }

    pub(crate) fn id(&self) -> ClientSessionId {
        self.id
    }

    /// A point-in-time [`datamancer_core::ResumeBufferSnapshot`] for this client.
    pub(crate) fn resume_buffer_snapshot(&self) -> datamancer_core::ResumeBufferSnapshot {
        datamancer_core::ResumeBufferSnapshot::new(
            self.capacity,
            self.occupancy.load(Ordering::Relaxed),
            self.dropped_events.load(Ordering::Relaxed),
        )
    }

    /// The current subscription set as serializable refs.
    pub(crate) fn subscription_refs(&self) -> Vec<SubscriptionRef> {
        self.subscriptions
            .lock()
            .map(|s| {
                s.iter()
                    .map(|(instrument, kind)| SubscriptionRef {
                        instrument: instrument.clone(),
                        kind: *kind,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

impl Drop for ClientStats {
    fn drop(&mut self) {
        if let Ok(mut map) = self.registry.lock() {
            map.remove(&self.id);
        }
    }
}

// ---------------------------------------------------------------------------
// Fan-out (lives inside the authoritative controller)
// ---------------------------------------------------------------------------

/// A referrer's bounded channel plus what a full channel left owed to it. A
/// dropped data event is folded into `pending` (a per-instrument span), and a
/// dropped control other than a `Gap` is kept in `owed`. Both go out in `seq`
/// order ahead of the next event once the channel has room.
struct Referrer {
    tx: mpsc::Sender<MarketEvent>,
    pending: Option<PendingGap>,
    /// Controls dropped on a full channel, in `seq` order, at most one per
    /// kind (see [`supersedes`]).
    owed: Vec<MarketEvent>,
    /// This referrer does not pace a replay.
    unpaced: bool,
    /// The replay's current wait is on this referrer.
    blocking: bool,
    /// How long the replay has waited on this referrer since it last freed a
    /// slot.
    stalled: Duration,
    /// How long, in all, the replay has waited on this referrer while another
    /// referrer had room.
    held_back: Duration,
    /// Its channel's free slots when the replay's current wait began.
    free_at_wait: usize,
    /// This referrer joined unseeded while the seed was pending: it is passed
    /// nothing until [`FanOut::seed_settled`].
    awaits_seed: bool,
    /// The [`FanOut::subscription_changes`] count replayed to it at its add.
    subscription_changes: u64,
    /// The [`FanOut::connection_changes`] count at its add.
    connection_changes: u64,
}

impl Referrer {
    fn new(tx: mpsc::Sender<MarketEvent>) -> Self {
        Self {
            tx,
            pending: None,
            owed: Vec::new(),
            unpaced: false,
            blocking: false,
            stalled: Duration::ZERO,
            held_back: Duration::ZERO,
            free_at_wait: 0,
            awaits_seed: false,
            subscription_changes: 0,
            connection_changes: 0,
        }
    }

    /// Whether a `Gap` or a control is owed to it.
    fn owes(&self) -> bool {
        self.pending.is_some() || !self.owed.is_empty()
    }

    /// Slots the next event takes: its own, and one for each `Gap` and control
    /// still owed.
    fn slots_needed(&self) -> usize {
        1 + usize::from(self.pending.is_some()) + self.owed.len()
    }

    /// Record `ev`, which the full channel refused: data and `Gap`s fold into
    /// the pending `Gap`; another control replaces an owed one it supersedes.
    fn owe(&mut self, ev: &MarketEvent) {
        if let MarketEvent::Control(control) = ev
            && !matches!(control.kind, ControlKind::Gap { .. })
        {
            self.owed.retain(|owed| match owed {
                MarketEvent::Control(older) => !supersedes(&control.kind, &older.kind),
                _ => true,
            });
            self.owed.push(ev.clone());
        } else {
            absorb_drop(&mut self.pending, ev);
        }
    }

    /// Send what is owed, in `seq` order, until nothing is or the channel
    /// refuses.
    fn send_owed(&mut self) -> std::result::Result<(), Refused> {
        loop {
            match (&self.pending, self.owed.first()) {
                (None, None) => return Ok(()),
                (Some(gap), control)
                    if control
                        .and_then(MarketEvent::seq)
                        .is_none_or(|seq| gap.first_seq <= seq) =>
                {
                    self.tx.try_send(gap.to_event()).map_err(Refused::from)?;
                    self.pending = None;
                }
                _ => {
                    self.tx
                        .try_send(self.owed[0].clone())
                        .map_err(Refused::from)?;
                    self.owed.remove(0);
                }
            }
        }
    }

    /// Whether its channel is short of the slots the next event takes.
    fn short(&self) -> bool {
        self.tx.capacity() < self.slots_needed()
    }
}

/// Accumulated, undelivered loss for one backed-up referrer. `FanOut` is
/// per-`(instrument, kind)`, so this is a single per-instrument span.
struct PendingGap {
    instrument: Instrument,
    provider: String,
    /// First lost `seq` — the hole start; the emitted `Gap` carries it (events
    /// are source-stamped and never renumbered).
    first_seq: Seq,
    span: GapSpan,
}

impl PendingGap {
    fn absorb(&mut self, from: Timestamp, to: Timestamp) {
        self.span.from_source_ts = self.span.from_source_ts.min(from);
        self.span.to_source_ts = self.span.to_source_ts.max(to);
    }

    fn to_event(&self) -> MarketEvent {
        MarketEvent::Control(Control {
            source_ts: self.span.from_source_ts,
            rx_ts: self.span.from_source_ts,
            seq: self.first_seq,
            kind: ControlKind::Gap {
                provider: self.provider.clone(),
                instrument: self.instrument.clone(),
                span: self.span.clone(),
            },
        })
    }
}

/// Extract `(instrument, from, to, provider, seq)` for an event that can be
/// represented as a per-instrument `Gap`. Data events span `[ts, ts+1)`; an
/// already-`Gap` control contributes its embedded span. Other controls return
/// `None`.
fn gap_coords(ev: &MarketEvent) -> Option<(Instrument, Timestamp, Timestamp, String, Seq)> {
    match ev {
        MarketEvent::Control(Control {
            kind:
                ControlKind::Gap {
                    instrument,
                    span,
                    provider,
                },
            seq,
            ..
        }) => Some((
            instrument.clone(),
            span.from_source_ts,
            span.to_source_ts,
            provider.clone(),
            *seq,
        )),
        MarketEvent::Trade(_) | MarketEvent::Quote(_) | MarketEvent::Bar(_) => {
            let instrument = data_instrument(ev)?;
            let ts = data_source_ts(ev)?;
            let provider = instrument.provider().to_string();
            Some((
                instrument,
                ts,
                Timestamp(ts.0.saturating_add(1)),
                provider,
                ev.seq().unwrap_or(Seq::SYNTHETIC),
            ))
        }
        _ => None,
    }
}

/// Why a referrer's channel refused a send.
enum Refused {
    Full,
    Closed,
}

impl From<mpsc::error::TrySendError<MarketEvent>> for Refused {
    fn from(e: mpsc::error::TrySendError<MarketEvent>) -> Self {
        match e {
            mpsc::error::TrySendError::Full(_) => Self::Full,
            mpsc::error::TrySendError::Closed(_) => Self::Closed,
        }
    }
}

/// Whether `newer` replaces `older` as an owed control: a connect or disconnect
/// replaces the provider's last one, a `ProviderError` the provider's last
/// error, and a `SubscriptionChanged` the instrument's.
fn supersedes(newer: &ControlKind, older: &ControlKind) -> bool {
    fn connection_provider(kind: &ControlKind) -> Option<&str> {
        match kind {
            ControlKind::ProviderConnected { provider }
            | ControlKind::ProviderDisconnected { provider, .. } => Some(provider),
            _ => None,
        }
    }
    match (newer, older) {
        (
            ControlKind::ProviderError { provider: a, .. },
            ControlKind::ProviderError { provider: b, .. },
        ) => a == b,
        (
            ControlKind::SubscriptionChanged { instrument: a, .. },
            ControlKind::SubscriptionChanged { instrument: b, .. },
        ) => a == b,
        (ControlKind::SessionClosing, ControlKind::SessionClosing) => true,
        _ => connection_provider(newer)
            .is_some_and(|provider| connection_provider(older) == Some(provider)),
    }
}

/// Fold a dropped event into a referrer's pending gap (starting one if needed).
/// A drop with no per-instrument identity is not recorded here.
fn absorb_drop(pending: &mut Option<PendingGap>, ev: &MarketEvent) {
    let Some((instrument, from, to, provider, seq)) = gap_coords(ev) else {
        return;
    };
    match pending {
        Some(p) => p.absorb(from, to),
        None => {
            *pending = Some(PendingGap {
                instrument,
                provider,
                first_seq: seq,
                span: GapSpan {
                    from_source_ts: from,
                    to_source_ts: to,
                },
            });
        }
    }
}

/// The authoritative controller's consumer-facing side: one bounded channel per
/// referrer. The controller **`try_send`s** so a slow/wedged referrer never
/// stalls the provider, and its co-subscribers only within the rules below. A
/// referrer that closes its channel is removed. A referrer whose bounded channel
/// is full has a dropped data event folded into a pending per-instrument `Gap`,
/// and a dropped control kept until a newer one of its kind replaces it. Both
/// reach it in `seq` order and ahead of its next event: in the live phase as
/// soon as its channel has room, during a replay ahead of the next replayed
/// event.
///
/// A backfill's replay comes at fetch speed, so the controller paces it:
///
/// 1. A replay, history or seam flush, sends only when every pacing referrer
///    has room.
/// 2. A referrer stops pacing on either of two 30 s clocks
///    ([`next_wait`](Self::next_wait)). One counts the time since it last freed
///    a slot. The other sums the waits on it that began with another referrer
///    already having room, and nothing resets it. Both belong to the referrer,
///    so a resubscribe, which adds a new one, starts again from zero.
/// 3. A seam flush that has lost a whole ring stops pacing for referrers full at
///    that moment ([`stop_pacing_full`](Self::stop_pacing_full)).
///
/// A referrer that stops pacing gets a `Gap`.
pub(crate) struct FanOut {
    subscribers: HashMap<SubscriberId, Referrer>,
    /// Ends a [`next_wait`](Self::next_wait) when the referrers pacing a replay
    /// change.
    pacers_changed: Arc<Notify>,
    /// Some referrer had room when the replay's current wait began.
    room_at_wait: bool,
    /// Last per-symbol `SubscriptionChanged { active: true }` (a real,
    /// source-stamped event), replayed to each new subscriber so a late join
    /// sees the subscription state without a fresh provider ack.
    last_subscription_changed: Option<MarketEvent>,
    /// How many times [`Self::last_subscription_changed`] has been set.
    subscription_changes: u64,
    /// Last `ProviderConnected` or `ProviderDisconnected`, replayed at
    /// [`Self::seed_settled`] to a referrer that waited through it.
    last_connection: Option<MarketEvent>,
    /// How many times [`Self::last_connection`] has been set.
    connection_changes: u64,
    /// Set once the first subscriber is added. Distinguishes "never had a
    /// subscriber yet" (do not tear down) from "last subscriber left".
    had_subscriber: bool,
    /// A referrer may be owed a `Gap` or a control (see [`Self::owed_room`]).
    owing: bool,
}

impl FanOut {
    pub(crate) fn new() -> Self {
        Self {
            subscribers: HashMap::new(),
            pacers_changed: Arc::new(Notify::new()),
            room_at_wait: false,
            last_subscription_changed: None,
            subscription_changes: 0,
            last_connection: None,
            connection_changes: 0,
            had_subscriber: false,
            owing: false,
        }
    }

    /// Add a referrer, replaying the cached subscription state to it. One that
    /// `awaits_seed` is passed nothing further until [`Self::seed_settled`]
    /// replays what it missed of the subscription and connection state.
    pub(crate) fn add(
        &mut self,
        id: SubscriberId,
        sender: mpsc::Sender<MarketEvent>,
        awaits_seed: bool,
    ) {
        self.had_subscriber = true;
        // Replay the cached subscription state to the joiner (best-effort).
        if let Some(ev) = &self.last_subscription_changed {
            let _ = sender.try_send(ev.clone());
        }
        self.subscribers.insert(
            id,
            Referrer {
                awaits_seed,
                subscription_changes: self.subscription_changes,
                connection_changes: self.connection_changes,
                ..Referrer::new(sender)
            },
        );
        self.pacers_changed.notify_one();
    }

    /// The seed was delivered, resolved empty, or is about to be beaten by a
    /// data event: every referrer is passed what follows. One that waited is
    /// first passed the cached subscription state and connection state, each
    /// if it changed while it waited, in `seq` order.
    pub(crate) fn seed_settled(&mut self) {
        for referrer in self.subscribers.values_mut() {
            if !std::mem::take(&mut referrer.awaits_seed) {
                continue;
            }
            let mut missed: Vec<&MarketEvent> = Vec::with_capacity(2);
            if referrer.subscription_changes != self.subscription_changes
                && let Some(ev) = &self.last_subscription_changed
            {
                missed.push(ev);
            }
            if referrer.connection_changes != self.connection_changes
                && let Some(ev) = &self.last_connection
            {
                missed.push(ev);
            }
            missed.sort_by_key(|ev| ev.seq());
            for ev in missed {
                let _ = referrer.tx.try_send(ev.clone());
            }
        }
    }

    /// Charge `waited` to the referrers the last wait was blocked on and stop
    /// those that reach `limit` on either clock from pacing: its stall, which
    /// a slot freed during the wait clears, and its held-back time, charged
    /// when another referrer had room as the wait began and never cleared.
    /// `None` when every pacing referrer has room.
    ///
    /// Otherwise the next wait, blocked on the pacing referrers short of room.
    /// It resolves when a referrer short of room gets it, pacing or not, when
    /// the referrers pacing the replay change, or when a blocking referrer
    /// reaches `limit`. Any of those can change whom the replay waits on, so
    /// the caller asks again.
    pub(crate) fn next_wait(
        &mut self,
        waited: Duration,
        limit: Duration,
    ) -> Option<impl Future<Output = ()> + Send + 'static> {
        let room_at_wait = std::mem::take(&mut self.room_at_wait);
        let mut pacer_short = false;
        for referrer in self.subscribers.values_mut() {
            let free = referrer.tx.capacity();
            let short = free < referrer.slots_needed();
            if referrer.blocking {
                if free > referrer.free_at_wait {
                    referrer.stalled = Duration::ZERO;
                } else {
                    referrer.stalled += waited;
                }
                if room_at_wait {
                    referrer.held_back += waited;
                }
                referrer.unpaced |= referrer.stalled >= limit || referrer.held_back >= limit;
            }
            let closed = referrer.tx.is_closed();
            self.room_at_wait |= !closed && !short;
            referrer.blocking = !closed && short;
            referrer.free_at_wait = free;
            pacer_short |= referrer.blocking && !referrer.unpaced;
        }
        if !pacer_short {
            for referrer in self.subscribers.values_mut() {
                referrer.blocking = false;
            }
            return None;
        }
        let short: Vec<_> = self
            .subscribers
            .values()
            .filter(|r| r.blocking)
            .map(|r| (r.tx.clone(), r.slots_needed()))
            .collect();
        for referrer in self.subscribers.values_mut() {
            referrer.blocking &= !referrer.unpaced;
        }
        let timer = self
            .subscribers
            .values()
            .filter(|r| r.blocking)
            .map(|r| {
                let held_back = if self.room_at_wait {
                    r.held_back
                } else {
                    Duration::ZERO
                };
                limit.saturating_sub(r.stalled.max(held_back))
            })
            .min();
        let pacers_changed = self.pacers_changed.clone();
        Some(async move {
            let mut room: FuturesUnordered<_> = short
                .iter()
                .map(|(tx, slots)| tx.reserve_many(*slots))
                .collect();
            tokio::select! {
                _ = room.next() => {}
                () = pacers_changed.notified() => {}
                () = tokio::time::sleep(timer.unwrap_or_default()), if timer.is_some() => {}
            }
        })
    }

    /// A wait that ends when a referrer owed a `Gap` or a control has room for
    /// one event; `None` when nothing is owed. The caller then calls
    /// [`Self::flush_owed`].
    pub(crate) fn owed_room(&mut self) -> Option<impl Future<Output = ()> + Send + 'static> {
        if !self.owing {
            return None;
        }
        let owing: Vec<_> = self
            .subscribers
            .values()
            .filter(|r| r.owes())
            .map(|r| r.tx.clone())
            .collect();
        if owing.is_empty() {
            self.owing = false;
            return None;
        }
        Some(async move {
            let mut room: FuturesUnordered<_> = owing.iter().map(mpsc::Sender::reserve).collect();
            room.next().await;
        })
    }

    /// Send each referrer what it is owed, as far as its channel has room.
    pub(crate) fn flush_owed(&mut self) {
        let mut dead: Vec<SubscriberId> = Vec::new();
        for (id, r) in &mut self.subscribers {
            if r.owes() && matches!(r.send_owed(), Err(Refused::Closed)) {
                dead.push(*id);
            }
        }
        for id in dead {
            self.subscribers.remove(&id);
        }
        self.owing = self.subscribers.values().any(Referrer::owes);
    }

    /// Referrers full now stop pacing this session's replay.
    pub(crate) fn stop_pacing_full(&mut self) {
        for referrer in self.subscribers.values_mut() {
            referrer.unpaced |= referrer.short();
        }
        self.pacers_changed.notify_one();
    }

    pub(crate) fn remove(&mut self, id: SubscriberId) {
        self.subscribers.remove(&id);
        self.pacers_changed.notify_one();
    }

    /// Remove every referrer whose receiver is gone.
    pub(crate) fn remove_closed(&mut self) {
        let before = self.subscribers.len();
        self.subscribers.retain(|_, r| !r.tx.is_closed());
        if self.subscribers.len() != before {
            self.pacers_changed.notify_one();
        }
    }

    /// True once a subscriber has been added and the fan-out is now empty —
    /// the refcounted-teardown trigger.
    pub(crate) fn should_teardown(&self) -> bool {
        self.had_subscriber && self.subscribers.is_empty()
    }

    /// Number of attached referrers — the true subscriber count (the registry
    /// `Arc` strong count over-counts).
    pub(crate) fn subscriber_count(&self) -> usize {
        self.subscribers.len()
    }

    /// Deliver one fully-stamped event to every referrer. Caches a
    /// `SubscriptionChanged { active: true }` for late-join replay. Removes any
    /// referrer whose channel has closed.
    pub(crate) fn fanout(&mut self, ev: &MarketEvent) {
        if let MarketEvent::Control(Control {
            kind: ControlKind::SubscriptionChanged { active: true, .. },
            ..
        }) = ev
        {
            self.last_subscription_changed = Some(ev.clone());
            self.subscription_changes += 1;
        }
        if let MarketEvent::Control(Control {
            kind: ControlKind::ProviderConnected { .. } | ControlKind::ProviderDisconnected { .. },
            ..
        }) = ev
        {
            self.last_connection = Some(ev.clone());
            self.connection_changes += 1;
        }
        let mut dead: Vec<SubscriberId> = Vec::new();
        for (id, r) in &mut self.subscribers {
            if r.awaits_seed {
                continue;
            }
            // What a full channel left owed goes first. If the channel is still
            // full, this event is owed too, and nothing waits: neither a
            // co-subscriber nor the provider stalls on a slow referrer.
            if r.owes() {
                match r.send_owed() {
                    Ok(()) => {}
                    Err(Refused::Full) => {
                        r.owe(ev);
                        self.owing = true;
                        continue;
                    }
                    Err(Refused::Closed) => {
                        dead.push(*id);
                        continue;
                    }
                }
            }
            match r.tx.try_send(ev.clone()) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    r.owe(ev);
                    self.owing = true;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => dead.push(*id),
            }
        }
        for id in dead {
            self.subscribers.remove(&id);
        }
    }
}

// ---------------------------------------------------------------------------
// Per-client resume ring (per-instrument gap accounting)
// ---------------------------------------------------------------------------

/// One affected instrument's eviction record on flush: `(first_evicted_seq,
/// instrument, span, provider)`.
type InstrumentGap = (Seq, Instrument, GapSpan, String);

/// The drained contents of a [`ClientRing`]: per-instrument gaps and the
/// surviving events (each paired with its provider id).
type RingParts = (Vec<InstrumentGap>, VecDeque<(MarketEvent, String)>);

/// One instrument's eviction accounting inside a [`ClientRing`].
struct DroppedSpan {
    span: GapSpan,
    first_seq: Seq,
    provider: String,
}

/// Bounded per-client FIFO with **per-instrument** loss accounting.
///
/// A single per-client ring backs the multiplexed stream, but overflow must not
/// conflate losses across symbols: each evicted data event (or evicted
/// `Control::Gap`) extends *its instrument's* span and records the first-evicted
/// `seq` (the hole start). Flush emits one `Control::Gap` per affected
/// instrument in first-evicted-`seq` order, then replays survivors in arrival
/// order — neither is renumbered (events are already source-stamped).
pub(crate) struct ClientRing {
    capacity: usize,
    buf: VecDeque<(MarketEvent, String)>,
    dropped: HashMap<Instrument, DroppedSpan>,
}

impl ClientRing {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            buf: VecDeque::new(),
            dropped: HashMap::new(),
        }
    }

    /// Push one event; returns `true` if it evicted the oldest (overflow).
    pub(crate) fn push(&mut self, ev: MarketEvent, provider: String) -> bool {
        let evicted = if self.buf.len() == self.capacity
            && let Some((evicted, evicted_provider)) = self.buf.pop_front()
        {
            self.note_drop(&evicted, &evicted_provider);
            true
        } else {
            false
        };
        self.buf.push_back((ev, provider));
        evicted
    }

    /// The event the next [`Self::push`] evicts, if the ring is full.
    fn oldest_if_full(&self) -> Option<&(MarketEvent, String)> {
        if self.buf.len() == self.capacity {
            self.buf.front()
        } else {
            None
        }
    }

    /// Remove the buffered copy of fan-out `id` from `provider`, if any.
    fn remove_fanout(&mut self, provider: &str, id: FanoutId) -> bool {
        let found = self.buf.iter().position(|(ev, p)| {
            p == provider
                && matches!(
                    ev,
                    MarketEvent::Control(Control {
                        kind: ControlKind::ProviderError {
                            origin: ErrorOrigin::Fanout(buffered),
                            ..
                        },
                        ..
                    }) if *buffered == id
                )
        });
        found.and_then(|i| self.buf.remove(i)).is_some()
    }

    /// Current buffered event count.
    pub(crate) fn len(&self) -> usize {
        self.buf.len()
    }

    /// Extend the dropped span for the evicted event's instrument. Data events
    /// count as `[ts, ts+1)`; an evicted `Control::Gap` contributes its embedded
    /// span (and its own provider id). Non-instrument controls are skipped —
    /// they are coalesced upstream and carry no market-data span.
    fn note_drop(&mut self, ev: &MarketEvent, provider: &str) {
        let (instrument, from, to, gap_provider) = match ev {
            MarketEvent::Control(Control {
                kind:
                    ControlKind::Gap {
                        instrument,
                        span,
                        provider: gap_provider,
                    },
                ..
            }) => (
                instrument.clone(),
                span.from_source_ts,
                span.to_source_ts,
                gap_provider.clone(),
            ),
            MarketEvent::Trade(_) | MarketEvent::Quote(_) | MarketEvent::Bar(_) => {
                let Some(instrument) = data_instrument(ev) else {
                    return;
                };
                let Some(ts) = data_source_ts(ev) else {
                    return;
                };
                (
                    instrument,
                    ts,
                    Timestamp(ts.0.saturating_add(1)),
                    provider.to_string(),
                )
            }
            _ => return,
        };
        let seq = ev.seq().unwrap_or(Seq::SYNTHETIC);
        match self.dropped.get_mut(&instrument) {
            Some(existing) => {
                existing.span.from_source_ts = existing.span.from_source_ts.min(from);
                existing.span.to_source_ts = existing.span.to_source_ts.max(to);
            }
            None => {
                self.dropped.insert(
                    instrument,
                    DroppedSpan {
                        span: GapSpan {
                            from_source_ts: from,
                            to_source_ts: to,
                        },
                        first_seq: seq,
                        provider: gap_provider,
                    },
                );
            }
        }
    }

    /// `(gaps, events)`. Gaps are `(first_seq, instrument, span, provider)`
    /// sorted by first-evicted `seq`; events are the survivors in arrival order
    /// paired with their provider id.
    fn into_parts(self) -> RingParts {
        let mut gaps: Vec<InstrumentGap> = self
            .dropped
            .into_iter()
            .map(|(instrument, d)| (d.first_seq, instrument, d.span, d.provider))
            .collect();
        gaps.sort_by_key(|(seq, _, _, _)| seq.0);
        (gaps, self.buf)
    }
}

fn data_instrument(ev: &MarketEvent) -> Option<Instrument> {
    match ev {
        MarketEvent::Trade(t) => Some(t.instrument.clone()),
        MarketEvent::Quote(q) => Some(q.instrument.clone()),
        MarketEvent::Bar(b) => Some(b.instrument.clone()),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Client controller (the multiplex)
// ---------------------------------------------------------------------------

/// Recorded provider connection state for the connection-scoped control
/// coalescer.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ConnState {
    Connected,
    Disconnected,
}

/// One subscription's referrer state held by the [`ClientController`].
struct SubEntry {
    _guard: SubscriberGuard,
    provider: String,
}

/// Item type for an interleaved substream: a stamped event, then a terminal
/// `Ended` marker when the authoritative substream closes (so the controller can
/// distinguish a closed substream from a merely idle one).
enum SubMsg {
    Event(MarketEvent),
    Ended,
}

/// Commands to the [`ClientController`] task.
enum ClientCommand {
    Subscribe {
        instrument: Instrument,
        kind: EventKind,
        scope: LiveScope,
        options: PersistenceOptions,
        reply: oneshot::Sender<Result<Subscribed>>,
    },
    Unsubscribe {
        instrument: Instrument,
        kind: EventKind,
        reply: oneshot::Sender<Result<()>>,
    },
    Take(oneshot::Sender<Result<mpsc::Receiver<MarketEvent>>>),
    Subscriptions(oneshot::Sender<Vec<(Instrument, EventKind)>>),
    Close(oneshot::Sender<()>),
}

/// The client-facing delivery side: an attached consumer channel, or a detached
/// per-client resume ring buffering until the next take.
enum ClientSink {
    Attached(mpsc::Sender<MarketEvent>),
    Detached(ClientRing),
}

type SubStream = std::pin::Pin<Box<dyn futures::Stream<Item = SubMsg> + Send>>;

/// The multiplex task. Owns the subscription set, the interleave, the
/// per-client resume buffer, and the connection-scoped control coalescer.
struct ClientController {
    dm: Datamancer,
    cmd_rx: mpsc::Receiver<ClientCommand>,
    streams: StreamMap<(Instrument, EventKind), SubStream>,
    entries: HashMap<(Instrument, EventKind), SubEntry>,
    sink: ClientSink,
    /// Events owed to the sink ahead of the substreams: a drained ring and the
    /// client's own controls. The run loop delivers them as the sink has room.
    backlog: VecDeque<(MarketEvent, String)>,
    ring_capacity: usize,
    seen_provider_state: HashMap<String, ConnState>,
    /// The fan-out ids delivered per provider, kept for the client's life:
    /// a substream can carry an old copy at any time, including one it joined
    /// with in flight.
    delivered_fanouts: HashMap<String, HashSet<FanoutId>>,
    /// Per-client introspection state read by the diagnostics snapshot.
    stats: Arc<ClientStats>,
}

fn substream(rx: mpsc::Receiver<MarketEvent>) -> SubStream {
    Box::pin(
        ReceiverStream::new(rx)
            .map(SubMsg::Event)
            .chain(futures::stream::once(async { SubMsg::Ended })),
    )
}

impl ClientController {
    async fn run(mut self) {
        loop {
            let ready = self.sink_ready();
            let backlogged = !self.backlog.is_empty();
            tokio::select! {
                () = sink_room(&self.sink), if !ready => {}
                () = std::future::ready(()), if ready && backlogged => {
                    if let Some((ev, provider)) = self.backlog.pop_front() {
                        self.deliver(ev, provider).await;
                        self.sync_occupancy();
                    }
                }
                maybe = self.streams.next(), if ready && !backlogged && !self.streams.is_empty() => {
                    if let Some((key, msg)) = maybe {
                        match msg {
                            SubMsg::Event(ev) => self.route(key, ev).await,
                            SubMsg::Ended => self.handle_substream_end(&key),
                        }
                    }
                }
                cmd = self.cmd_rx.recv() => {
                    let Some(cmd) = cmd else {
                        // Handle dropped without an explicit close: shut down.
                        self.emit_session_closing().await;
                        break;
                    };
                    if !self.handle_command(cmd).await {
                        break;
                    }
                }
            }
        }
        // Dropping `entries` releases every SubscriberGuard -> authoritative
        // teardown when each was the last referrer.
    }

    /// Whether the sink can take an event without waiting.
    fn sink_ready(&self) -> bool {
        match &self.sink {
            ClientSink::Attached(tx) => tx.capacity() > 0 || tx.is_closed(),
            ClientSink::Detached(_) => true,
        }
    }

    /// Returns false when the controller should exit.
    async fn handle_command(&mut self, cmd: ClientCommand) -> bool {
        match cmd {
            ClientCommand::Subscribe {
                instrument,
                kind,
                scope,
                options,
                reply,
            } => {
                let res = self
                    .subscribe(instrument.clone(), kind, scope, options)
                    .await;
                if matches!(reply.send(res), Err(Ok(_))) {
                    let _ = self.unsubscribe(instrument, kind);
                }
                true
            }
            ClientCommand::Unsubscribe {
                instrument,
                kind,
                reply,
            } => {
                let res = self.unsubscribe(instrument, kind);
                let _ = reply.send(res);
                true
            }
            ClientCommand::Take(reply) => {
                match self.prepare_attach() {
                    Ok((rx, prior)) => {
                        let _ = reply.send(Ok(rx));
                        if let Some(ring) = prior {
                            self.flush_ring(ring);
                        }
                    }
                    Err(e) => {
                        let _ = reply.send(Err(e));
                    }
                }
                true
            }
            ClientCommand::Subscriptions(reply) => {
                let _ = reply.send(self.entries.keys().cloned().collect());
                true
            }
            ClientCommand::Close(reply) => {
                self.emit_session_closing().await;
                let _ = reply.send(());
                false
            }
        }
    }

    async fn subscribe(
        &mut self,
        instrument: Instrument,
        kind: EventKind,
        scope: LiveScope,
        options: PersistenceOptions,
    ) -> Result<Subscribed> {
        let key = (instrument.clone(), kind);
        if self.entries.contains_key(&key) {
            return Err(Error::DuplicateSubscription { instrument, kind });
        }
        let Attached {
            guard, rx, outcome, ..
        } = self
            .dm
            .authoritative(instrument.clone(), kind, scope, options, Caller::Client)
            .await?;
        let provider = guard.provider().to_string();
        self.entries.insert(
            key.clone(),
            SubEntry {
                _guard: guard,
                provider,
            },
        );
        self.streams.insert(key, substream(rx));
        self.sync_subscriptions();
        Ok(outcome)
    }

    fn unsubscribe(&mut self, instrument: Instrument, kind: EventKind) -> Result<()> {
        let key = (instrument.clone(), kind);
        let Some(entry) = self.entries.remove(&key) else {
            return Err(Error::NotSubscribed { instrument, kind });
        };
        let provider = entry.provider.clone();
        // Drop the guard (-> RemoveSubscriber -> maybe authoritative teardown)
        // and the substream, then surface a client-local control. This one is
        // genuinely client-local (the authoritative session stays up for other
        // clients), so it is synthetic and rides `Seq::SYNTHETIC`.
        drop(entry);
        self.streams.remove(&key);
        self.sync_subscriptions();
        self.queue_control(
            subscription_changed(&instrument, kind, &provider, false),
            provider,
        );
        Ok(())
    }

    /// A substream closed without an explicit unsubscribe (authoritative
    /// teardown or fan-out removal). Surface a per-symbol
    /// `SubscriptionChanged { active: false }` and drop the entry. If the key is
    /// already gone (explicit unsubscribe handled it), this is a no-op.
    fn handle_substream_end(&mut self, key: &(Instrument, EventKind)) {
        let Some(entry) = self.entries.remove(key) else {
            return;
        };
        let provider = entry.provider.clone();
        drop(entry);
        self.sync_subscriptions();
        self.queue_control(
            subscription_changed(&key.0, key.1, &provider, false),
            provider,
        );
    }

    /// Route one substream event into the per-client stream. Per-symbol controls
    /// (`Gap`, `SubscriptionChanged`) ride through unchanged; connection-scoped
    /// controls are coalesced so each distinct transition appears once;
    /// `SessionClosing` from a substream is suppressed (the client emits its own
    /// on `close`). **No `seq` stamping, no sorting** — arrival order across
    /// substreams; `seq` is already source-stamped.
    async fn route(&mut self, key: (Instrument, EventKind), ev: MarketEvent) {
        let ring_provider = self
            .entries
            .get(&key)
            .map_or_else(|| key.0.provider().to_string(), |e| e.provider.clone());
        if let MarketEvent::Control(c) = &ev {
            match &c.kind {
                ControlKind::SessionClosing => return,
                ControlKind::ProviderConnected { provider } => {
                    if !self.note_conn(provider, ConnState::Connected) {
                        return;
                    }
                }
                ControlKind::ProviderDisconnected { provider, .. } => {
                    if !self.note_conn(provider, ConnState::Disconnected) {
                        return;
                    }
                }
                ControlKind::ProviderError {
                    origin: ErrorOrigin::Fanout(id),
                    ..
                } => {
                    if !self
                        .delivered_fanouts
                        .entry(ring_provider.clone())
                        .or_default()
                        .insert(*id)
                    {
                        // A copy still in the detached ring is not delivered
                        // yet: carry this newer copy in its place, so the one
                        // copy the ring holds is the last to be evicted.
                        let ClientSink::Detached(ring) = &mut self.sink else {
                            return;
                        };
                        if !ring.remove_fanout(&ring_provider, *id) {
                            return;
                        }
                    }
                }
                ControlKind::ProviderError { .. }
                | ControlKind::SubscriptionChanged { .. }
                | ControlKind::Gap { .. } => {}
            }
        }
        self.deliver(ev, ring_provider).await;
    }

    /// Returns true when the transition for `provider` is new (forward it).
    fn note_conn(&mut self, provider: &str, new: ConnState) -> bool {
        match self.seen_provider_state.get(provider) {
            Some(current) if *current == new => false,
            _ => {
                self.seen_provider_state.insert(provider.to_string(), new);
                true
            }
        }
    }

    /// Queue a client-local control. Past [`CONTROL_BACKLOG`] on top of a
    /// drained ring the consumer has stopped reading, and the control is lost.
    fn queue_control(&mut self, ev: MarketEvent, provider: String) {
        if self.backlog.len() < self.ring_capacity.saturating_add(CONTROL_BACKLOG) {
            self.backlog.push_back((ev, provider));
            self.sync_occupancy();
        }
    }

    /// Deliver one event to the client sink (emit-only — never tee; the tap-log
    /// tee already ran once at the authoritative source). On a gone/closed
    /// consumer the sink flips to a detached per-client ring.
    async fn deliver(&mut self, ev: MarketEvent, provider: String) {
        let ev = match &self.sink {
            ClientSink::Attached(tx) if !tx.is_closed() => match tx.send(ev).await {
                Ok(()) => return,
                Err(mpsc::error::SendError(ev)) => ev,
            },
            _ => ev,
        };
        if matches!(self.sink, ClientSink::Attached(_)) {
            self.sink = ClientSink::Detached(ClientRing::new(self.ring_capacity));
        }
        let (evicted, occupancy) = if let ClientSink::Detached(ring) = &mut self.sink {
            // A fan-out error the ring evicts was never delivered: forget its
            // id, so a later copy on another subscription is delivered.
            if let Some((
                MarketEvent::Control(Control {
                    kind:
                        ControlKind::ProviderError {
                            origin: ErrorOrigin::Fanout(id),
                            ..
                        },
                    ..
                }),
                oldest_provider,
            )) = ring.oldest_if_full()
                && let Some(ids) = self.delivered_fanouts.get_mut(oldest_provider)
            {
                ids.remove(id);
            }
            let evicted = ring.push(ev, provider);
            (evicted, ring.len())
        } else {
            (false, 0)
        };
        if evicted {
            self.stats.record_drops(1);
        }
        self.stats.set_occupancy(occupancy);
    }

    fn prepare_attach(&mut self) -> Result<(mpsc::Receiver<MarketEvent>, Option<ClientRing>)> {
        if let ClientSink::Attached(tx) = &self.sink
            && !tx.is_closed()
        {
            return Err(Error::EventsAlreadyTaken);
        }
        let (tx, rx) = mpsc::channel(default_buffer());
        let prior = std::mem::replace(&mut self.sink, ClientSink::Attached(tx));
        // The drained ring moves to the backlog, which `flush_ring` counts.
        // (Its cumulative `dropped_events` is retained on `ClientStats`.)
        Ok((
            rx,
            match prior {
                ClientSink::Detached(ring) => Some(ring),
                ClientSink::Attached(_) => None,
            },
        ))
    }

    /// Mirror the current subscription set onto [`ClientStats`] for the snapshot.
    fn sync_subscriptions(&self) {
        self.stats
            .set_subscriptions(self.entries.keys().cloned().collect());
    }

    /// Drain a detached ring on re-attach: one `Gap` per affected instrument
    /// (first-evicted-`seq` order), then survivors in arrival order. Replayed
    /// events keep their source-stamped `seq` — the eviction `Gap` rides the
    /// first-evicted slot.
    fn flush_ring(&mut self, ring: ClientRing) {
        let (gaps, events) = ring.into_parts();
        let mut drained: VecDeque<_> = gaps
            .into_iter()
            .map(|(first_seq, instrument, span, provider)| {
                let gap = MarketEvent::Control(Control {
                    source_ts: span.from_source_ts,
                    rx_ts: span.from_source_ts,
                    seq: first_seq,
                    kind: ControlKind::Gap {
                        provider: provider.clone(),
                        instrument,
                        span,
                    },
                });
                (gap, provider)
            })
            .collect();
        drained.extend(events);
        drained.append(&mut self.backlog);
        self.backlog = drained;
        self.sync_occupancy();
    }

    /// Report the events owed to an attached consumer as the resume buffer's
    /// occupancy. A detached client's occupancy is its ring.
    fn sync_occupancy(&self) {
        if matches!(self.sink, ClientSink::Attached(_)) {
            self.stats.set_occupancy(self.backlog.len());
        }
    }

    /// Hand the backlog and a terminal `SessionClosing` to the consumer, for up
    /// to [`CLOSE_DELIVERY_WAIT`] in all. What it has not taken by then is lost.
    async fn emit_session_closing(&mut self) {
        self.backlog.push_back((
            MarketEvent::Control(Control {
                source_ts: wall_clock_ts(),
                rx_ts: wall_clock_ts(),
                seq: Seq::SYNTHETIC,
                kind: ControlKind::SessionClosing,
            }),
            String::new(),
        ));
        let deadline = tokio::time::Instant::now() + CLOSE_DELIVERY_WAIT;
        while let Some((ev, provider)) = self.backlog.pop_front() {
            match &self.sink {
                ClientSink::Attached(tx) if !tx.is_closed() => {
                    match tokio::time::timeout_at(deadline, tx.send(ev)).await {
                        Ok(Ok(())) => {}
                        Ok(Err(mpsc::error::SendError(ev))) => self.deliver(ev, provider).await,
                        Err(_) => return,
                    }
                }
                _ => self.deliver(ev, provider).await,
            }
        }
    }
}

fn subscription_changed(
    instrument: &Instrument,
    kind: EventKind,
    provider: &str,
    active: bool,
) -> MarketEvent {
    MarketEvent::Control(Control {
        source_ts: wall_clock_ts(),
        rx_ts: wall_clock_ts(),
        seq: Seq::SYNTHETIC,
        kind: ControlKind::SubscriptionChanged {
            provider: provider.to_string(),
            instrument: instrument.clone(),
            kind,
            active,
        },
    })
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
// Client handle + public ClientSession
// ---------------------------------------------------------------------------

/// An initial subscription handed to [`spawn_client`]: the pair, its referrer
/// guard, the authoritative substream receiver, and the provider id.
type InitialSubscription = (
    (Instrument, EventKind),
    SubscriberGuard,
    mpsc::Receiver<MarketEvent>,
    String,
);

/// Internal handle to a spawned [`ClientController`]. Shared by the public
/// [`ClientSession`] and the retained single-pair [`crate::Session`] on its live
/// path.
pub(crate) struct ClientHandle {
    cmd_tx: mpsc::Sender<ClientCommand>,
    id: ClientSessionId,
}

impl ClientHandle {
    pub(crate) fn id(&self) -> ClientSessionId {
        self.id
    }

    pub(crate) async fn take_events(&self) -> Result<EventStream> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(ClientCommand::Take(tx))
            .await
            .map_err(|_| Error::SessionClosed)?;
        let receiver = rx.await.map_err(|_| Error::SessionClosed)??;
        Ok(EventStream::new(receiver))
    }

    pub(crate) async fn subscribe(
        &self,
        instrument: Instrument,
        kind: EventKind,
        scope: LiveScope,
        options: PersistenceOptions,
    ) -> Result<Subscribed> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(ClientCommand::Subscribe {
                instrument,
                kind,
                scope,
                options,
                reply: tx,
            })
            .await
            .map_err(|_| Error::SessionClosed)?;
        rx.await.map_err(|_| Error::SessionClosed)?
    }

    pub(crate) async fn unsubscribe(&self, instrument: Instrument, kind: EventKind) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(ClientCommand::Unsubscribe {
                instrument,
                kind,
                reply: tx,
            })
            .await
            .map_err(|_| Error::SessionClosed)?;
        rx.await.map_err(|_| Error::SessionClosed)?
    }

    pub(crate) async fn subscriptions(&self) -> Vec<(Instrument, EventKind)> {
        let (tx, rx) = oneshot::channel();
        if self
            .cmd_tx
            .send(ClientCommand::Subscriptions(tx))
            .await
            .is_err()
        {
            return Vec::new();
        }
        rx.await.unwrap_or_default()
    }

    pub(crate) async fn close(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let _ = self.cmd_tx.send(ClientCommand::Close(tx)).await;
        let _ = rx.await;
        Ok(())
    }
}

/// Spawn a [`ClientController`] with an optional initial subscription, used by
/// the single-pair live [`crate::Session`] (which carries its own
/// authoritative referrer); a bare [`ClientSession`] starts empty.
pub(crate) fn spawn_client(
    dm: Datamancer,
    ring_capacity: usize,
    initial: Option<InitialSubscription>,
) -> ClientHandle {
    let (cmd_tx, cmd_rx) = mpsc::channel(default_buffer());
    let mut entries = HashMap::new();
    let mut streams = StreamMap::new();
    if let Some((key, guard, rx, provider)) = initial {
        entries.insert(
            key.clone(),
            SubEntry {
                _guard: guard,
                provider,
            },
        );
        streams.insert(key, substream(rx));
    }
    let id = ClientSessionId::next();
    let registry = dm.client_registry();
    let stats = Arc::new(ClientStats::new(id, ring_capacity, registry.clone()));
    stats.set_subscriptions(entries.keys().cloned().collect());
    if let Ok(mut map) = registry.lock() {
        map.insert(id, Arc::downgrade(&stats));
    }
    let controller = ClientController {
        dm,
        cmd_rx,
        streams,
        entries,
        sink: ClientSink::Detached(ClientRing::new(ring_capacity)),
        backlog: VecDeque::new(),
        ring_capacity,
        seen_provider_state: HashMap::new(),
        delivered_fanouts: HashMap::new(),
        stats,
    };
    tokio::spawn(controller.run());
    ClientHandle { cmd_tx, id }
}

/// The primary consumer handle: a mutable set of `(instrument, kind)`
/// subscriptions presented as **one multiplexed stream**. Single-owner; not
/// `Clone`, mirroring [`crate::Session`].
///
/// Determinism is **per symbol only**: each instrument's substream is
/// `seq`-monotonic (source-stamped); there is no cross-instrument ordering
/// (arrival order). Subscriptions are unbounded: pure-live, or a backfill
/// seamed into the live tail.
pub struct ClientSession {
    handle: ClientHandle,
}

impl ClientSession {
    pub(crate) fn new(handle: ClientHandle) -> Self {
        Self { handle }
    }

    /// Add a subscription to this client's set and return what it opened.
    ///
    /// The pair's authoritative session is created with the first subscriber's
    /// `scope` and every later subscriber joins it as is.
    ///
    /// # Errors
    ///
    /// - [`Error::DuplicateSubscription`] — `(instrument, kind)` is already in
    ///   this client's set.
    /// - [`Error::UnknownProvider`] — no registered provider has the
    ///   instrument's [`provider`](Instrument::provider) id.
    /// - [`Error::PersistenceRequired`] — `options` needs a cache or tap log
    ///   that is not configured, on a joiner as on a creator.
    /// - [`Error::BackfillTooOld`] — a backfill starts before the limit set by
    ///   [`DatamancerBuilder::max_client_backfill`](crate::DatamancerBuilder::max_client_backfill),
    ///   on a joiner as on a creator.
    /// - [`Error::BackfillFromNotInPast`] — this subscribe would create a
    ///   session whose backfill starts at or past the live edge. A joiner is
    ///   not checked.
    /// - [`Error::UnsupportedEventKind`] — this subscribe would create a
    ///   session and that provider lacks a surface the scope needs (live, plus
    ///   history under backfill). A joiner is not checked: a
    ///   [`LiveScope::LiveBackfill`] subscribe on a pair a live-only provider
    ///   already holds under [`LiveScope::Live`] answers
    ///   [`Subscribed::Joined`] with that `Live` session.
    /// - [`Error::ProviderDisabled`] — the session's provider is disabled,
    ///   whether this subscribe would create or join.
    /// - [`Error::SessionClosed`] — the controller has shut down.
    pub async fn subscribe(
        &self,
        instrument: Instrument,
        kind: EventKind,
        scope: LiveScope,
        options: PersistenceOptions,
    ) -> Result<Subscribed> {
        self.handle
            .subscribe(instrument, kind, scope, options)
            .await
    }

    /// Remove a subscription. Emits a client-local
    /// `SubscriptionChanged { active: false }` for the symbol.
    ///
    /// # Errors
    ///
    /// - [`Error::NotSubscribed`] — the pair is not in this client's set.
    /// - [`Error::SessionClosed`] — the controller has shut down.
    pub async fn unsubscribe(&self, instrument: Instrument, kind: EventKind) -> Result<()> {
        self.handle.unsubscribe(instrument, kind).await
    }

    /// Take the multiplexed event stream. Multi-shot: dropping the stream
    /// detaches the consumer (events buffer into the per-client resume ring); a
    /// later call re-attaches, first surfacing one `Gap` per affected instrument
    /// for anything the buffer evicted. Events still unread in a dropped
    /// stream's channel are lost with no `Gap`, and the next stream goes on
    /// past their `seq` slots: drain before dropping to keep them.
    ///
    /// # Errors
    ///
    /// - [`Error::EventsAlreadyTaken`] — a previous stream is still open.
    /// - [`Error::SessionClosed`] — the controller has shut down.
    pub async fn take_events(&self) -> Result<EventStream> {
        self.handle.take_events().await
    }

    /// The current subscription set (for introspection).
    pub async fn subscriptions(&self) -> Vec<(Instrument, EventKind)> {
        self.handle.subscriptions().await
    }

    /// This client session's process-scoped id.
    #[must_use]
    pub fn id(&self) -> ClientSessionId {
        self.handle.id()
    }

    /// Explicit termination. Emits a single `Control::SessionClosing` then tears
    /// down every subscription (releasing the authoritative refcounts). A
    /// consumer that has not read what it is owed within two seconds loses the
    /// rest, the terminal control included.
    ///
    /// # Errors
    ///
    /// Currently infallible; the `Result` shape is reserved for future
    /// flush-error reporting.
    pub async fn close(self) -> Result<()> {
        self.handle.close().await
    }
}

#[cfg(test)]
mod live_stats_tests {
    use super::LiveStats;
    use datamancer_core::{
        AssetClass, Control, ControlKind, GapSpan, Instrument, MarketEvent, Price, ProviderId, Seq,
        Timestamp, Trade,
    };

    fn trade(ts: i64, seq: u64) -> MarketEvent {
        MarketEvent::Trade(Trade {
            instrument: Instrument::new(ProviderId::from_static("t"), AssetClass::Equity, "X"),
            source_ts: Timestamp(ts),
            rx_ts: Timestamp(ts + 5),
            seq: Seq(seq),
            price: Price::from_f64_round(1.0),
            size: datamancer_core::Quantity::from_units(1),
        })
    }

    fn gap() -> MarketEvent {
        MarketEvent::Control(Control {
            source_ts: Timestamp(0),
            rx_ts: Timestamp(0),
            seq: Seq(7),
            kind: ControlKind::Gap {
                provider: "t".to_string(),
                instrument: Instrument::new(ProviderId::from_static("t"), AssetClass::Equity, "X"),
                span: GapSpan {
                    from_source_ts: Timestamp(1),
                    to_source_ts: Timestamp(2),
                },
            },
        })
    }

    #[test]
    fn starts_empty() {
        let s = LiveStats::new();
        assert_eq!(s.seq_position(), None);
        assert_eq!(s.last_source_ts(), None);
        assert_eq!(s.last_rx_ts(), None);
        assert_eq!(s.gap_count(), 0);
    }

    #[test]
    fn records_data_and_gaps() {
        let s = LiveStats::new();
        s.record_event(&trade(100, 0));
        s.record_event(&trade(200, 1));
        assert_eq!(s.seq_position(), Some(Seq(1)));
        assert_eq!(s.last_source_ts(), Some(Timestamp(200)));
        assert_eq!(s.last_rx_ts(), Some(Timestamp(205)));
        assert_eq!(s.gap_count(), 0);
        s.record_event(&gap());
        assert_eq!(s.gap_count(), 1);
        // The gap also occupies a seq slot.
        assert_eq!(s.seq_position(), Some(Seq(7)));
    }

    #[test]
    fn a_replayed_event_records_position_and_gaps_but_no_timestamps() {
        let s = LiveStats::new();
        s.record_replay(&trade(100, 3));
        assert_eq!(s.seq_position(), Some(Seq(3)));
        assert_eq!(s.last_source_ts(), None);
        assert_eq!(s.last_rx_ts(), None);
        s.record_replay(&gap());
        assert_eq!(s.gap_count(), 1);
    }

    #[test]
    fn live_stats_retain_bounded_recent_gap_spans() {
        let stats = LiveStats::new();
        for i in 0..10_i64 {
            stats.record_event(&MarketEvent::Control(Control {
                source_ts: Timestamp(i),
                rx_ts: Timestamp(1_000 + i),
                seq: Seq(u64::try_from(i).unwrap()),
                kind: ControlKind::Gap {
                    provider: "p".to_string(),
                    instrument: Instrument::new(
                        ProviderId::from_static("p"),
                        AssetClass::Equity,
                        "X",
                    ),
                    span: GapSpan {
                        from_source_ts: Timestamp(i),
                        to_source_ts: Timestamp(i + 1),
                    },
                },
            }));
        }
        let spans = stats.recent_gaps();
        assert_eq!(spans.len(), 8); // RECENT_GAPS_CAP — oldest two evicted
        assert_eq!(spans[0].from_source_ts, Timestamp(2));
        assert_eq!(stats.last_gap_rx_ts(), Some(Timestamp(1_009)));
        assert_eq!(stats.gap_count(), 10);
    }

    #[test]
    fn backfilling_flag_sets_and_clears() {
        let stats = LiveStats::new();
        assert!(!stats.backfilling());
        stats.set_backfilling(true);
        assert!(stats.backfilling());
        stats.set_backfilling(false);
        assert!(!stats.backfilling());
    }
}

#[cfg(test)]
mod client_ring_tests {
    use super::ClientRing;
    use datamancer_core::{
        AssetClass, Control, ControlKind, GapSpan, Instrument, MarketEvent, Price, ProviderId, Seq,
        Timestamp, Trade,
    };

    fn inst(symbol: &str) -> Instrument {
        Instrument::new(ProviderId::from_static("p"), AssetClass::Equity, symbol)
    }

    fn trade(symbol: &str, ts: i64, seq: u64) -> MarketEvent {
        MarketEvent::Trade(Trade {
            instrument: inst(symbol),
            source_ts: Timestamp(ts),
            rx_ts: Timestamp(ts),
            seq: Seq(seq),
            price: Price::from_f64_round(1.0),
            size: datamancer_core::Quantity::from_units(1),
        })
    }

    fn gap(symbol: &str, seq: u64, from: i64, to: i64) -> MarketEvent {
        MarketEvent::Control(Control {
            source_ts: Timestamp(from),
            rx_ts: Timestamp(from),
            seq: Seq(seq),
            kind: ControlKind::Gap {
                provider: "p".to_string(),
                instrument: inst(symbol),
                span: GapSpan {
                    from_source_ts: Timestamp(from),
                    to_source_ts: Timestamp(to),
                },
            },
        })
    }

    #[test]
    fn under_capacity_records_no_gaps() {
        let mut ring = ClientRing::new(4);
        ring.push(trade("AAPL", 100, 0), "p".to_string());
        ring.push(trade("MSFT", 200, 1), "p".to_string());
        let (gaps, events) = ring.into_parts();
        assert!(gaps.is_empty());
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn overflow_accounts_per_instrument() {
        // Capacity 2; interleave two instruments and overflow so each loses one.
        let mut ring = ClientRing::new(2);
        ring.push(trade("AAPL", 100, 0), "p".to_string()); // evicted
        ring.push(trade("MSFT", 200, 1), "p".to_string()); // evicted
        ring.push(trade("AAPL", 300, 2), "p".to_string());
        ring.push(trade("MSFT", 400, 3), "p".to_string());
        let (gaps, events) = ring.into_parts();
        // Two affected instruments -> two spans, in first-evicted-seq order.
        assert_eq!(gaps.len(), 2);
        assert_eq!(gaps[0].0, Seq(0)); // AAPL evicted first
        assert_eq!(gaps[0].1, inst("AAPL"));
        assert_eq!(gaps[0].2.from_source_ts, Timestamp(100));
        assert_eq!(gaps[0].2.to_source_ts, Timestamp(101));
        assert_eq!(gaps[1].0, Seq(1)); // MSFT evicted second
        assert_eq!(gaps[1].1, inst("MSFT"));
        assert_eq!(gaps[1].2.from_source_ts, Timestamp(200));
        assert_eq!(gaps[1].2.to_source_ts, Timestamp(201));
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn evicted_gap_control_folds_into_its_instrument() {
        let mut ring = ClientRing::new(1);
        ring.push(gap("AAPL", 5, 100, 201), "p".to_string());
        ring.push(trade("AAPL", 300, 6), "p".to_string()); // evicts the gap control
        let (gaps, events) = ring.into_parts();
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0].1, inst("AAPL"));
        assert_eq!(gaps[0].2.from_source_ts, Timestamp(100));
        assert_eq!(gaps[0].2.to_source_ts, Timestamp(201));
        assert_eq!(events.len(), 1);
    }
}

#[cfg(test)]
mod fanout_tests {
    use std::time::Duration;

    use futures::FutureExt as _;

    use super::{FanOut, SubscriberId};
    use datamancer_core::{
        AssetClass, ControlKind, Instrument, MarketEvent, Price, ProviderId, Seq, Timestamp, Trade,
    };
    use tokio::sync::mpsc;

    const LIMIT: Duration = Duration::from_secs(30);

    fn inst(symbol: &str) -> Instrument {
        Instrument::new(ProviderId::from_static("p"), AssetClass::Equity, symbol)
    }

    fn trade(ts: i64, seq: u64) -> MarketEvent {
        MarketEvent::Trade(Trade {
            instrument: inst("AAPL"),
            source_ts: Timestamp(ts),
            rx_ts: Timestamp(ts),
            seq: Seq(seq),
            price: Price::from_f64_round(1.0),
            size: datamancer_core::Quantity::from_units(1),
        })
    }

    #[test]
    fn full_channel_surfaces_loss_as_gap_not_silent_drop() {
        // A slow referrer's full channel must not silently lose data: the dropped
        // event becomes a per-instrument Gap, delivered ahead of resumed events.
        let mut fo = FanOut::new();
        let (tx, mut rx) = mpsc::channel(1);
        fo.add(SubscriberId(1), tx, false);

        fo.fanout(&trade(100, 0)); // delivered; the single slot is now full
        fo.fanout(&trade(200, 1)); // channel full -> recorded as pending loss

        // Drain the delivered event, freeing the slot.
        assert_eq!(rx.try_recv().unwrap().seq(), Some(Seq(0)));

        // The next fan-out flushes the owed Gap before anything else.
        fo.fanout(&trade(300, 2));
        match rx.try_recv().unwrap() {
            MarketEvent::Control(c) => match c.kind {
                ControlKind::Gap {
                    instrument, span, ..
                } => {
                    assert_eq!(c.seq, Seq(1)); // hole start = first lost seq
                    assert_eq!(instrument, inst("AAPL"));
                    assert_eq!(span.from_source_ts, Timestamp(200));
                }
                other => panic!("expected Gap, got {other:?}"),
            },
            other => panic!("expected a Control::Gap, got {other:?}"),
        }
    }

    fn control(seq: u64, kind: ControlKind) -> MarketEvent {
        MarketEvent::Control(datamancer_core::Control {
            source_ts: Timestamp(0),
            rx_ts: Timestamp(0),
            seq: Seq(seq),
            kind,
        })
    }

    fn disconnected(seq: u64) -> MarketEvent {
        control(
            seq,
            ControlKind::ProviderDisconnected {
                provider: "p".to_string(),
                reason: "reset".to_string(),
                cause: datamancer_core::DisconnectCause::Error,
            },
        )
    }

    fn connected(seq: u64) -> MarketEvent {
        control(
            seq,
            ControlKind::ProviderConnected {
                provider: "p".to_string(),
            },
        )
    }

    fn subscription_changed(seq: u64, active: bool) -> MarketEvent {
        control(
            seq,
            ControlKind::SubscriptionChanged {
                provider: "p".to_string(),
                instrument: inst("AAPL"),
                kind: datamancer_core::EventKind::Trade,
                active,
            },
        )
    }

    /// A fan-out with one referrer whose channel holds `depth` events and is
    /// full, plus the receiver and a sender for filling it again.
    fn full_referrer(depth: usize) -> (FanOut, mpsc::Receiver<MarketEvent>) {
        let (tx, rx) = mpsc::channel(depth);
        for _ in 0..depth {
            tx.try_send(trade(1, 0)).unwrap();
        }
        let mut fanout = FanOut::new();
        fanout.add(SubscriberId(1), tx, false);
        (fanout, rx)
    }

    /// Empty the channel, fan `next` out, and return what the referrer then
    /// receives as `(seq, what)`.
    fn drain_then(
        fanout: &mut FanOut,
        rx: &mut mpsc::Receiver<MarketEvent>,
        next: &MarketEvent,
    ) -> Vec<(u64, &'static str)> {
        while rx.try_recv().is_ok() {}
        fanout.fanout(next);
        let mut got = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            let what = match &ev {
                MarketEvent::Control(c) => match c.kind {
                    ControlKind::Gap { .. } => "gap",
                    ControlKind::ProviderConnected { .. } => "connected",
                    ControlKind::ProviderDisconnected { .. } => "disconnected",
                    ControlKind::ProviderError { .. } => "error",
                    ControlKind::SubscriptionChanged { active: true, .. } => "active",
                    ControlKind::SubscriptionChanged { active: false, .. } => "inactive",
                    _ => "other control",
                },
                _ => "data",
            };
            got.push((ev.seq().unwrap().0, what));
        }
        got
    }

    #[test]
    fn a_control_dropped_on_a_full_channel_comes_before_the_next_event() {
        let (mut fanout, mut rx) = full_referrer(2);
        fanout.fanout(&disconnected(2));
        assert_eq!(
            drain_then(&mut fanout, &mut rx, &trade(3, 3)),
            [(2, "disconnected"), (3, "data")]
        );
        let (mut fanout, mut rx) = full_referrer(2);
        fanout.fanout(&subscription_changed(2, false));
        assert_eq!(
            drain_then(&mut fanout, &mut rx, &trade(3, 3)),
            [(2, "inactive"), (3, "data")]
        );
    }

    #[test]
    fn owed_controls_and_a_gap_come_in_seq_order() {
        let (mut fanout, mut rx) = full_referrer(4);
        fanout.fanout(&trade(2, 2));
        fanout.fanout(&disconnected(3));
        fanout.fanout(&trade(4, 4));
        assert_eq!(
            drain_then(&mut fanout, &mut rx, &trade(5, 5)),
            [(2, "gap"), (3, "disconnected"), (5, "data")]
        );
        let (mut fanout, mut rx) = full_referrer(4);
        fanout.fanout(&subscription_changed(2, false));
        fanout.fanout(&trade(3, 3));
        fanout.fanout(&disconnected(4));
        assert_eq!(
            drain_then(&mut fanout, &mut rx, &trade(5, 5)),
            [
                (2, "inactive"),
                (3, "gap"),
                (4, "disconnected"),
                (5, "data")
            ]
        );
    }

    #[test]
    fn a_newer_owed_control_of_its_kind_replaces_the_older() {
        let (mut fanout, mut rx) = full_referrer(4);
        fanout.fanout(&disconnected(2));
        fanout.fanout(&subscription_changed(3, false));
        fanout.fanout(&connected(4));
        fanout.fanout(&subscription_changed(5, true));
        assert_eq!(
            drain_then(&mut fanout, &mut rx, &trade(6, 6)),
            [(4, "connected"), (5, "active"), (6, "data")]
        );
    }

    fn provider_error(seq: u64) -> MarketEvent {
        control(
            seq,
            ControlKind::subscription_error("p", "re-subscribe failed"),
        )
    }

    #[test]
    fn an_owed_error_leaves_the_owed_connection_state() {
        let (mut fanout, mut rx) = full_referrer(4);
        fanout.fanout(&disconnected(2));
        fanout.fanout(&connected(3));
        fanout.fanout(&provider_error(4));
        fanout.fanout(&provider_error(5));
        assert_eq!(
            drain_then(&mut fanout, &mut rx, &trade(6, 6)),
            [(3, "connected"), (5, "error"), (6, "data")]
        );
    }

    #[test]
    fn owed_controls_left_over_wait_for_the_next_room() {
        let (mut fanout, mut rx) = full_referrer(1);
        fanout.fanout(&trade(2, 2));
        fanout.fanout(&disconnected(3));
        // Room for the Gap only: the control and the event stay owed.
        assert_eq!(drain_then(&mut fanout, &mut rx, &trade(4, 4)), [(2, "gap")]);
        assert_eq!(
            drain_then(&mut fanout, &mut rx, &trade(5, 5)),
            [(3, "disconnected")]
        );
        assert_eq!(drain_then(&mut fanout, &mut rx, &trade(6, 6)), [(4, "gap")]);
    }

    #[tokio::test]
    async fn what_is_owed_goes_out_once_the_channel_has_room() {
        let (mut fanout, mut rx) = full_referrer(2);
        fanout.fanout(&trade(2, 2));
        fanout.fanout(&disconnected(3));
        let room = fanout.owed_room().expect("something is owed");
        while rx.try_recv().is_ok() {}
        tokio::time::timeout(Duration::from_secs(1), room)
            .await
            .expect("room was not seen");
        fanout.flush_owed();
        let got: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok())
            .map(|ev| ev.seq().unwrap().0)
            .collect();
        assert_eq!(got, [2, 3], "the Gap, then the disconnect");
        assert!(fanout.owed_room().is_none(), "nothing is owed");
    }

    #[tokio::test]
    async fn a_closed_referrer_that_owes_is_removed_by_the_flush() {
        let (mut fanout, rx) = full_referrer(2);
        fanout.fanout(&disconnected(2));
        let room = fanout.owed_room().expect("something is owed");
        drop(rx);
        tokio::time::timeout(Duration::from_secs(1), room)
            .await
            .expect("a closed channel ends the wait");
        fanout.flush_owed();
        assert_eq!(fanout.subscriber_count(), 0);
        assert!(fanout.owed_room().is_none(), "nothing is owed");
    }

    #[tokio::test]
    async fn a_referrer_owed_a_control_needs_room_for_it() {
        let (mut fanout, mut rx) = full_referrer(2);
        fanout.fanout(&disconnected(2));
        rx.recv().await;
        assert!(
            fanout.next_wait(Duration::ZERO, LIMIT).is_some(),
            "the control takes the only free slot"
        );
        rx.recv().await;
        assert!(fanout.next_wait(Duration::ZERO, LIMIT).is_none());
    }

    #[tokio::test]
    async fn a_referrer_owed_a_gap_needs_room_for_two() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(2);
        let mut fanout = FanOut::new();
        fanout.add(SubscriberId(1), tx, false);
        let trade = |ts| {
            datamancer_core::MarketEvent::Trade(datamancer_core::Trade {
                instrument: datamancer_core::Instrument::new(
                    datamancer_core::ProviderId::from_static("fake"),
                    datamancer_core::AssetClass::Equity,
                    "AAPL",
                ),
                source_ts: datamancer_core::Timestamp(ts),
                rx_ts: datamancer_core::Timestamp(ts),
                seq: datamancer_core::Seq(0),
                price: datamancer_core::Price(1),
                size: datamancer_core::Quantity::from_raw(1),
            })
        };
        for ts in 1..=3 {
            fanout.fanout(&trade(ts));
        }
        rx.recv().await;
        assert!(
            fanout.next_wait(Duration::ZERO, LIMIT).is_some(),
            "the Gap takes the only free slot"
        );
        rx.recv().await;
        assert!(fanout.next_wait(Duration::ZERO, LIMIT).is_none());
    }

    #[tokio::test]
    async fn a_slot_free_before_the_wait_is_not_progress() {
        let (mut fanout, mut rx) = full_referrer(2);
        fanout.fanout(&trade(3, 3));
        rx.recv().await;
        assert!(
            fanout.next_wait(Duration::ZERO, LIMIT).is_some(),
            "the Gap takes the only free slot"
        );
        assert!(
            fanout.next_wait(LIMIT, LIMIT).is_none(),
            "a full wait with nothing read is the limit"
        );
    }

    /// The timer on the wait `next_wait` plans: how long it runs with nothing
    /// else happening, `None` if that is forever.
    async fn timer(fanout: &mut FanOut, waited: Duration) -> Option<Duration> {
        let _ = fanout.pacers_changed.notified().now_or_never();
        let wait = fanout.next_wait(waited, LIMIT).expect("a wait");
        let started = tokio::time::Instant::now();
        tokio::time::timeout(Duration::from_hours(1), wait)
            .await
            .ok()
            .map(|()| started.elapsed())
    }

    /// A full channel and one with room, both added to a new fan-out.
    fn stopped_and_reading() -> (
        FanOut,
        tokio::sync::mpsc::Receiver<datamancer_core::MarketEvent>,
        tokio::sync::mpsc::Receiver<datamancer_core::MarketEvent>,
    ) {
        let (full_tx, full_rx) = tokio::sync::mpsc::channel(1);
        let (room_tx, room_rx) = tokio::sync::mpsc::channel(1);
        full_tx.try_send(trade(1, 1)).unwrap();
        let mut fanout = FanOut::new();
        fanout.add(SubscriberId(1), full_tx, false);
        fanout.add(SubscriberId(2), room_tx, false);
        (fanout, full_rx, room_rx)
    }

    #[tokio::test(start_paused = true)]
    async fn a_lone_full_referrer_stops_pacing_at_the_limit_in_total() {
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        tx.try_send(trade(1, 1)).unwrap();
        let mut fanout = FanOut::new();
        fanout.add(SubscriberId(1), tx, false);
        assert_eq!(timer(&mut fanout, Duration::ZERO).await, Some(LIMIT));
        assert_eq!(
            timer(&mut fanout, Duration::from_secs(20)).await,
            Some(Duration::from_secs(10))
        );
        assert!(fanout.next_wait(Duration::from_secs(10), LIMIT).is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn a_full_referrer_stops_pacing_at_the_limit_in_total() {
        let (mut fanout, _full, _room) = stopped_and_reading();
        assert_eq!(timer(&mut fanout, Duration::ZERO).await, Some(LIMIT));
        assert_eq!(
            timer(&mut fanout, Duration::from_secs(20)).await,
            Some(Duration::from_secs(10))
        );
        assert!(fanout.next_wait(Duration::from_secs(10), LIMIT).is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn a_join_is_not_charged_for_the_wait_before_it() {
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        tx.try_send(trade(1, 1)).unwrap();
        let mut fanout = FanOut::new();
        fanout.add(SubscriberId(1), tx, false);
        assert_eq!(timer(&mut fanout, Duration::ZERO).await, Some(LIMIT));
        let (joiner, _joiner_rx) = tokio::sync::mpsc::channel(1);
        joiner.try_send(trade(1, 1)).unwrap();
        fanout.add(SubscriberId(2), joiner, false);
        assert_eq!(
            timer(&mut fanout, Duration::from_secs(20)).await,
            Some(Duration::from_secs(10))
        );
        assert_eq!(
            timer(&mut fanout, Duration::from_secs(10)).await,
            Some(Duration::from_secs(20)),
            "the joiner is charged from its join"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_unpaced_referrer_is_not_waited_on() {
        let (full_tx, _full) = tokio::sync::mpsc::channel(2);
        let (room_tx, _room) = tokio::sync::mpsc::channel(1);
        full_tx.try_send(trade(1, 1)).unwrap();
        full_tx.try_send(trade(1, 1)).unwrap();
        let mut fanout = FanOut::new();
        fanout.add(SubscriberId(1), full_tx, false);
        fanout.add(SubscriberId(2), room_tx, false);
        assert_eq!(timer(&mut fanout, Duration::ZERO).await, Some(LIMIT));
        assert!(
            fanout.next_wait(LIMIT, LIMIT).is_none(),
            "the first is charged out"
        );
        // The other fills; the replay waits on it alone, from zero.
        fanout.fanout(&trade(2, 2));
        assert_eq!(timer(&mut fanout, Duration::ZERO).await, Some(LIMIT));
    }

    #[tokio::test]
    async fn room_for_one_of_two_short_referrers_ends_the_wait() {
        let (first_tx, mut first_rx) = tokio::sync::mpsc::channel(1);
        let (second_tx, _second_rx) = tokio::sync::mpsc::channel(1);
        first_tx.try_send(trade(1, 1)).unwrap();
        second_tx.try_send(trade(1, 1)).unwrap();
        let mut fanout = FanOut::new();
        fanout.add(SubscriberId(1), first_tx, false);
        fanout.add(SubscriberId(2), second_tx, false);
        let _ = fanout.pacers_changed.notified().now_or_never();
        let wait = fanout.next_wait(Duration::ZERO, LIMIT).expect("a wait");
        first_rx.recv().await;
        tokio::time::timeout(Duration::from_secs(1), wait)
            .await
            .expect("a wake-up was lost");
    }
}
