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
//! `(instrument, seq)`: monotonic *within* each instrument (source-stamped, from
//! Phase 1), arrival-order across instruments. There is no cross-symbol order to
//! compute, which is what makes the interleave cheap.
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

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};

use datamancer_core::{
    ClientSessionId, Control, ControlKind, Error, EventKind, GapSpan, Instrument, MarketEvent,
    Result, Seq, SubscriptionRef, Timestamp,
};
use futures::StreamExt as _;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamMap;
use tokio_stream::wrappers::ReceiverStream;

use crate::session::{
    ClientSessionRegistry, Datamancer, EventStream, LiveSessionRegistry, PersistenceOptions, Scope,
    SessionCommand, default_buffer,
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
    pub(crate) scope: Scope,
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
        scope: Scope,
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
    /// referrer drains. Fails with [`Error::SessionClosed`] if the controller
    /// has already torn down.
    pub(crate) async fn add_subscriber(
        &self,
    ) -> Result<(SubscriberId, mpsc::Receiver<MarketEvent>)> {
        let id = SubscriberId(self.next_subscriber_id.fetch_add(1, Ordering::Relaxed));
        let (tx, rx) = mpsc::channel(default_buffer());
        let (ack, ack_rx) = oneshot::channel();
        self.cmd_tx
            .send(SessionCommand::AddSubscriber {
                id,
                sender: tx,
                ack,
            })
            .await
            .map_err(|_| Error::SessionClosed)?;
        ack_rx.await.map_err(|_| Error::SessionClosed)?;
        Ok((id, rx))
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
}

/// Bounded per-symbol recent-gap detail (oldest evicted).
const RECENT_GAPS_CAP: usize = 8;

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
        if let Some(seq) = ev.seq() {
            self.last_seq.store(seq.0, Ordering::Relaxed);
            self.has_seq.store(true, Ordering::Relaxed);
        }
        match ev {
            MarketEvent::Trade(_) | MarketEvent::Quote(_) | MarketEvent::Bar(_) => {
                if let (Some(source), Some(rx)) = (data_source_ts(ev), data_rx_ts(ev)) {
                    self.last_source_ts.store(source.0, Ordering::Relaxed);
                    self.last_rx_ts.store(rx.0, Ordering::Relaxed);
                    self.has_ts.store(true, Ordering::Relaxed);
                }
            }
            MarketEvent::Control(c) => {
                if let ControlKind::Gap { span, .. } = &c.kind {
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
            }
            _ => {}
        }
    }

    /// Last source-stamped `seq` seen, or `None` before any event.
    pub(crate) fn seq_position(&self) -> Option<Seq> {
        self.has_seq
            .load(Ordering::Relaxed)
            .then(|| Seq(self.last_seq.load(Ordering::Relaxed)))
    }

    /// Last data-event `source_ts`, or `None` before any data event.
    pub(crate) fn last_source_ts(&self) -> Option<Timestamp> {
        self.has_ts
            .load(Ordering::Relaxed)
            .then(|| Timestamp(self.last_source_ts.load(Ordering::Relaxed)))
    }

    /// Last data-event `rx_ts`, or `None` before any data event.
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
    /// Current per-client resume-buffer occupancy (0 while attached).
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

/// A referrer's bounded channel plus any loss owed to it as a `Gap`. When the
/// channel is full the dropped event is folded into `pending` (a per-instrument
/// span) rather than discarded, and the accumulated `Gap` is flushed ahead of
/// resumed live delivery once the channel drains.
struct Referrer {
    tx: mpsc::Sender<MarketEvent>,
    pending: Option<PendingGap>,
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
/// already-`Gap` control contributes its embedded span. Connection-scoped
/// controls carry no instrument and return `None` — a dropped one is not folded
/// into a data gap (it is re-derivable from the diagnostics snapshot / on
/// reconnect).
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

/// Fold a dropped event into a referrer's pending gap (starting one if needed).
/// A drop with no per-instrument identity (a connection-scoped control) is not
/// recorded here.
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
/// stalls its co-subscribers or the provider. A referrer that closes its
/// channel is removed; a referrer whose bounded channel is momentarily full has
/// the dropped event folded into a pending per-instrument `Gap` (surfaced to it
/// once the channel drains) — never silently lost.
pub(crate) struct FanOut {
    subscribers: HashMap<SubscriberId, Referrer>,
    /// Last per-symbol `SubscriptionChanged { active: true }` (a real,
    /// source-stamped event), replayed to each new subscriber so a late join
    /// sees the subscription state without a fresh provider ack.
    last_subscription_changed: Option<MarketEvent>,
    /// Set once the first subscriber is added. Distinguishes "never had a
    /// subscriber yet" (do not tear down) from "last subscriber left".
    had_subscriber: bool,
}

impl FanOut {
    pub(crate) fn new() -> Self {
        Self {
            subscribers: HashMap::new(),
            last_subscription_changed: None,
            had_subscriber: false,
        }
    }

    pub(crate) fn add(&mut self, id: SubscriberId, sender: mpsc::Sender<MarketEvent>) {
        self.had_subscriber = true;
        // Replay the cached subscription state to the joiner (best-effort).
        if let Some(ev) = &self.last_subscription_changed {
            let _ = sender.try_send(ev.clone());
        }
        self.subscribers.insert(
            id,
            Referrer {
                tx: sender,
                pending: None,
            },
        );
    }

    pub(crate) fn remove(&mut self, id: SubscriberId) {
        self.subscribers.remove(&id);
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
        }
        let mut dead: Vec<SubscriberId> = Vec::new();
        for (id, r) in &mut self.subscribers {
            // Flush any loss owed to this referrer first, so the `Gap` is ordered
            // ahead of resumed live delivery. If the channel is still full, fold
            // this event into the pending gap and move on — never stall a
            // co-subscriber or the provider.
            if let Some(p) = &r.pending {
                match r.tx.try_send(p.to_event()) {
                    Ok(()) => r.pending = None,
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        absorb_drop(&mut r.pending, ev);
                        continue;
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        dead.push(*id);
                        continue;
                    }
                }
            }
            match r.tx.try_send(ev.clone()) {
                Ok(()) => {}
                // A momentarily-full channel means a slow referrer: record the
                // loss as a per-instrument `Gap` (delivered when it drains)
                // rather than dropping it silently.
                Err(mpsc::error::TrySendError::Full(_)) => absorb_drop(&mut r.pending, ev),
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
        scope: Scope,
        options: PersistenceOptions,
        reply: oneshot::Sender<Result<()>>,
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
    ring_capacity: usize,
    seen_provider_state: HashMap<String, ConnState>,
    last_provider_error: HashMap<String, String>,
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
            tokio::select! {
                maybe = self.streams.next(), if !self.streams.is_empty() => {
                    if let Some((key, msg)) = maybe {
                        match msg {
                            SubMsg::Event(ev) => self.route(key, ev).await,
                            SubMsg::Ended => self.handle_substream_end(key).await,
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
                let res = self.subscribe(instrument, kind, scope, options).await;
                let _ = reply.send(res);
                true
            }
            ClientCommand::Unsubscribe {
                instrument,
                kind,
                reply,
            } => {
                let res = self.unsubscribe(instrument, kind).await;
                let _ = reply.send(res);
                true
            }
            ClientCommand::Take(reply) => {
                match self.prepare_attach() {
                    Ok((rx, prior)) => {
                        let _ = reply.send(Ok(rx));
                        if let Some(ring) = prior {
                            self.flush_ring(ring).await;
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
        scope: Scope,
        options: PersistenceOptions,
    ) -> Result<()> {
        let key = (instrument.clone(), kind);
        if self.entries.contains_key(&key) {
            return Err(Error::DuplicateSubscription { instrument, kind });
        }
        // Phase 2: client subscriptions are pure-live. A shared authoritative
        // session has one creation-time scope, so a per-client backfill /
        // historical join would break the identical-`(seq, source_ts)`
        // guarantee. Reject the unsupported variants with a clear error.
        if !matches!(
            scope,
            Scope::Live {
                backfill_from: None
            }
        ) {
            return Err(Error::UnsupportedClientScope);
        }
        let (_auth, guard, rx) = self
            .dm
            .authoritative(instrument.clone(), kind, scope, options)
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
        Ok(())
    }

    async fn unsubscribe(&mut self, instrument: Instrument, kind: EventKind) -> Result<()> {
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
        self.deliver(
            subscription_changed(&instrument, kind, &provider, false),
            provider,
        )
        .await;
        Ok(())
    }

    /// A substream closed without an explicit unsubscribe (authoritative
    /// teardown or fan-out removal). Surface a per-symbol
    /// `SubscriptionChanged { active: false }` and drop the entry. If the key is
    /// already gone (explicit unsubscribe handled it), this is a no-op.
    async fn handle_substream_end(&mut self, key: (Instrument, EventKind)) {
        let Some(entry) = self.entries.remove(&key) else {
            return;
        };
        let provider = entry.provider.clone();
        drop(entry);
        self.sync_subscriptions();
        self.deliver(
            subscription_changed(&key.0, key.1, &provider, false),
            provider,
        )
        .await;
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
                ControlKind::ProviderError { provider, message } => {
                    if self.duplicate_error(provider, message) {
                        return;
                    }
                }
                ControlKind::SubscriptionChanged { .. } | ControlKind::Gap { .. } => {}
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

    /// Returns true when this `ProviderError` exactly repeats the last one for
    /// the provider (suppress it).
    fn duplicate_error(&mut self, provider: &str, message: &str) -> bool {
        if self
            .last_provider_error
            .get(provider)
            .is_some_and(|last| last == message)
        {
            true
        } else {
            self.last_provider_error
                .insert(provider.to_string(), message.to_string());
            false
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
        // Attached: nothing sits in the resume buffer. (The drained ring's
        // cumulative `dropped_events` is retained on `ClientStats`.)
        self.stats.set_occupancy(0);
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
    async fn flush_ring(&mut self, ring: ClientRing) {
        let (gaps, events) = ring.into_parts();
        for (first_seq, instrument, span, provider) in gaps {
            self.deliver(
                MarketEvent::Control(Control {
                    source_ts: span.from_source_ts,
                    rx_ts: span.from_source_ts,
                    seq: first_seq,
                    kind: ControlKind::Gap {
                        provider: provider.clone(),
                        instrument,
                        span,
                    },
                }),
                provider,
            )
            .await;
        }
        for (ev, provider) in events {
            self.deliver(ev, provider).await;
        }
    }

    async fn emit_session_closing(&mut self) {
        self.deliver(
            MarketEvent::Control(Control {
                source_ts: wall_clock_ts(),
                rx_ts: wall_clock_ts(),
                seq: Seq::SYNTHETIC,
                kind: ControlKind::SessionClosing,
            }),
            String::new(),
        )
        .await;
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

/// A seed subscription handed to [`spawn_client`]: the pair, its referrer guard,
/// the authoritative substream receiver, and the provider id.
type SeedSubscription = (
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
        scope: Scope,
        options: PersistenceOptions,
    ) -> Result<()> {
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

/// Spawn a [`ClientController`] with an optional seed subscription. The seed is
/// used by the single-pair live [`crate::Session`] (which carries its own
/// authoritative referrer); a bare [`ClientSession`] starts empty.
pub(crate) fn spawn_client(
    dm: Datamancer,
    ring_capacity: usize,
    seed: Option<SeedSubscription>,
) -> ClientHandle {
    let (cmd_tx, cmd_rx) = mpsc::channel(default_buffer());
    let mut entries = HashMap::new();
    let mut streams = StreamMap::new();
    if let Some((key, guard, rx, provider)) = seed {
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
        ring_capacity,
        seen_provider_state: HashMap::new(),
        last_provider_error: HashMap::new(),
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
/// (arrival order). Subscriptions are pure-live in Phase 2.
pub struct ClientSession {
    handle: ClientHandle,
}

impl ClientSession {
    pub(crate) fn new(handle: ClientHandle) -> Self {
        Self { handle }
    }

    /// Add a subscription to this client's set.
    ///
    /// # Errors
    ///
    /// - [`Error::DuplicateSubscription`] — `(instrument, kind)` is already in
    ///   this client's set.
    /// - [`Error::UnsupportedClientScope`] — Phase 2 client subscriptions are
    ///   pure-live; `Scope::Historical` and `Live { backfill_from: Some(_) }`
    ///   are rejected.
    /// - [`Error::UnsupportedEventKind`] — no provider serves the pair.
    /// - [`Error::SessionClosed`] — the controller has shut down.
    pub async fn subscribe(
        &self,
        instrument: Instrument,
        kind: EventKind,
        scope: Scope,
        options: PersistenceOptions,
    ) -> Result<()> {
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
    /// for anything the buffer evicted.
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
    /// down every subscription (releasing the authoritative refcounts).
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
    use super::{FanOut, SubscriberId};
    use datamancer_core::{
        AssetClass, ControlKind, Instrument, MarketEvent, Price, ProviderId, Seq, Timestamp, Trade,
    };
    use tokio::sync::mpsc;

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
        fo.add(SubscriberId(1), tx);

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
}
