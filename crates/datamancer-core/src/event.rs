//! Public event model.
//!
//! Datamancer's output is a single ordered stream of [`MarketEvent`]. Variants
//! cover live and historical market data plus in-band [`Control`] entries for
//! connectivity, subscription state, and gap reporting.

use serde::{Deserialize, Serialize};

use crate::{Instrument, Price, Quantity};

/// A per-`(instrument, kind)` ordering identifier stamped **once at the
/// source** of that pair's authoritative session, in the session's canonical
/// delivery order, before any sink.
///
/// **The sole ordering field for the stream.** Invariants:
///
/// - **Per `(instrument, kind)`, not global.** `seq` orders one pair's stream;
///   there is no order across pairs. The multiplex ordering key is
///   `(instrument, kind, seq)`. A [`ControlKind::Gap`] names the instrument
///   only, so with two kinds on one instrument it does not say which kind.
/// - **Stamped at the source.** The authoritative controller assigns `seq` once,
///   in delivery order, before the event reaches any sink — so it is a property
///   of the shared stream, not of a particular consumer's poll timing.
/// - **Identical across a pair's consumers.** Every consumer of one pair's
///   authoritative stream observes the same `(seq, source_ts)` for each event.
/// - **Controls occupy slots.** In-band [`Control`] events are stamped here too,
///   so they consume a `seq` slot like data events.
/// - **Holes are real.** A consumer that loses data (a resume-buffer or
///   backfill-seam overflow) observes a real `seq` hole, surfaced in-band as
///   [`ControlKind::Gap`]. In a client session's stream as the session
///   delivers it, a hole with no `Gap` is one of these, and only the first
///   two are loss (a transport that carries the stream on may add holes of
///   its own, see its docs):
///   - events still unread in a stream's channel when its consumer drops it
///     are gone, and the next stream goes on past their slots;
///   - with two kinds on one instrument, what a detached client's resume
///     buffer evicted of both folds into one `Gap` at the first evicted slot,
///     and the other kind's hole has no `Gap` at its slot;
///   - a connection control (`ProviderConnected`, `ProviderDisconnected`,
///     `ProviderError`) the client has already delivered for that provider,
///     on another instrument or earlier, is dropped and its slot stays empty.
///     A [`ControlKind::ProviderError`] is already delivered only as a copy of
///     an [`ErrorOrigin::Fanout`] whose [`FanoutId`] the client delivered
///     from another of its subscriptions; an [`ErrorOrigin::Subscription`]
///     error never is;
///   - a connection control or `SubscriptionChanged` that a detached client's
///     resume buffer evicted is gone and its slot stays empty, with no `Gap`
///     since no data was lost; the diagnostics plane still carries the
///     provider's connection and the subscription state;
///   - a joiner's stream opens with the session's last active
///     `SubscriptionChanged`, replayed with its original `seq`, and goes on
///     from the session's current `seq`;
///   - a `seed: false` joiner that joins while the latest-value seed is
///     pending starts after the seed's slot. Of what the session delivers
///     before the seed settles, it is passed only the last subscription state
///     and the last `ProviderConnected` or `ProviderDisconnected`, replayed
///     with their original `seq` when the seed settles;
///   - a connection control or `SubscriptionChanged` that met a full consumer
///     channel, and was superseded by a newer one for the same provider or
///     instrument before the channel had room, is dropped and its slot stays
///     empty.
/// - **`SYNTHETIC` is exempt.** [`Seq::SYNTHETIC`] tags out-of-band synthetic
///   control events and is exempt from per-`(instrument, kind)` monotonicity.
///
/// In a live session `seq` is assigned in arrival order; for historical fetch
/// it is assigned in source-timestamp order during fetch, so `seq` order
/// matches market order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Seq(pub u64);

impl Seq {
    /// Sentinel `seq` for synthetic control events that do not belong to an
    /// authoritative stream's monotonic order (e.g. per-client controls minted
    /// outside the source counter). `Seq(u64::MAX)` is **reserved**: the source
    /// counter stamps only in `[0, u64::MAX - 1]` and treats the reservation
    /// boundary as counter exhaustion, so this value never collides with a
    /// stamped event. It is exempt from per-`(instrument, kind)` monotonicity.
    pub const SYNTHETIC: Seq = Seq(u64::MAX);
}

/// A timestamp expressed in nanoseconds since the Unix epoch.
///
/// Used for both `source_ts` (provider-reported) and `rx_ts` (wall-clock at
/// receipt). The two roles are documented on each event variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Timestamp(pub i64);

/// Bar interval. Skeletal — extended as additional intervals become first-class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum BarInterval {
    OneSecond,
    OneMinute,
    FiveMinute,
    FifteenMinute,
    OneHour,
    OneDay,
}

impl BarInterval {
    /// Every interval, in ascending duration order. `BarInterval` is a small
    /// closed enum, so the kind space is finitely enumerable — this powers
    /// [`EventKind::enumerate`] and, through it, per-instrument capability
    /// discovery.
    pub const ALL: [BarInterval; 6] = [
        BarInterval::OneSecond,
        BarInterval::OneMinute,
        BarInterval::FiveMinute,
        BarInterval::FifteenMinute,
        BarInterval::OneHour,
        BarInterval::OneDay,
    ];
}

/// Selector used in subscriptions. Each variant maps 1:1 with a [`MarketEvent`]
/// data variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum EventKind {
    Trade,
    Quote,
    Bar(BarInterval),
}

impl EventKind {
    /// Every subscribable kind: `Trade`, `Quote`, and one `Bar` per
    /// [`BarInterval`]. Used to derive an instrument's capability list by
    /// probing [`crate::Provider::supports`] over the full kind space, once per
    /// [`Surface`].
    pub fn enumerate() -> impl Iterator<Item = EventKind> {
        [EventKind::Trade, EventKind::Quote]
            .into_iter()
            .chain(BarInterval::ALL.into_iter().map(EventKind::Bar))
    }
}

/// Which of a provider's two data surfaces a capability question is about.
///
/// The two genuinely differ and neither contains the other. Alpaca's equity
/// websocket streams only minute and daily bars while its REST bar endpoint
/// serves five intervals; conversely a provider may stream a kind it cannot
/// backfill at all. Collapsing them into one predicate makes a provider either
/// over-promise (advertise a backfill it cannot serve) or under-promise (reject
/// a historical request it could serve) — datamancer shipped both bugs before
/// this axis existed.
///
/// Deliberately **not** `#[non_exhaustive]`. Marking it so would force `_ =>`
/// arms in every out-of-crate provider, meaning a future variant would silently
/// inherit whatever answer that wildcard gives — reintroducing exactly the
/// class of bug this type exists to prevent. An exhaustive enum makes adding a
/// surface a compile error every provider must consciously answer.
/// [`crate::Provider::latest`] is such a future variant: it is a third surface
/// in all but name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Surface {
    /// The streaming subscription path ([`crate::Provider::start_live`]).
    Live,
    /// The bounded historical-fetch path ([`crate::Provider::fetch_history`]).
    History,
}

impl Surface {
    /// Both surfaces, in declaration order. Lets callers probe the full
    /// capability matrix without hard-coding the variant list.
    pub const ALL: [Surface; 2] = [Surface::Live, Surface::History];
}

/// The unified output stream entry.
///
/// Every data variant carries three fields whose roles must not be conflated:
///
/// - `source_ts` — when the event happened in the market. The **only**
///   timestamp engine logic should reason about.
/// - `seq` — datamancer's per-`(instrument, kind)`, source-stamped ordering
///   field.
/// - `rx_ts` — wall-clock at byte receipt. **Observability only.** Engine
///   decision logic must never depend on `rx_ts`. For replay-from-historical,
///   `rx_ts` collapses to `source_ts`.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub enum MarketEvent {
    Trade(Trade),
    Quote(Quote),
    Bar(Bar),
    Control(Control),
}

impl MarketEvent {
    /// The session-monotonic ordering field. `None` for control events that
    /// do not occupy a sequence slot (none currently — reserved for future
    /// metadata-only entries).
    #[must_use]
    #[allow(
        clippy::unnecessary_wraps,
        reason = "Option is forward-compat: future metadata-only control entries will return None"
    )]
    pub fn seq(&self) -> Option<Seq> {
        match self {
            MarketEvent::Trade(t) => Some(t.seq),
            MarketEvent::Quote(q) => Some(q.seq),
            MarketEvent::Bar(b) => Some(b.seq),
            MarketEvent::Control(c) => Some(c.seq),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Trade {
    pub instrument: Instrument,
    pub source_ts: Timestamp,
    pub rx_ts: Timestamp,
    pub seq: Seq,
    pub price: Price,
    pub size: Quantity,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Quote {
    pub instrument: Instrument,
    pub source_ts: Timestamp,
    pub rx_ts: Timestamp,
    pub seq: Seq,
    pub bid: Price,
    pub bid_size: Quantity,
    pub ask: Price,
    pub ask_size: Quantity,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Bar {
    pub instrument: Instrument,
    pub interval: BarInterval,
    pub source_ts: Timestamp,
    pub rx_ts: Timestamp,
    pub seq: Seq,
    pub open: Price,
    pub high: Price,
    pub low: Price,
    pub close: Price,
    pub volume: Quantity,
}

/// In-band session-control entry. Rides the same stream as data events because
/// connectivity changes are part of the session's truth — a gap can invalidate
/// downstream signals, and forcing consumers to acknowledge them in-band is
/// safer than offering a separate stream they may forget to subscribe to.
#[derive(Debug, Clone, PartialEq)]
pub struct Control {
    pub source_ts: Timestamp,
    pub rx_ts: Timestamp,
    pub seq: Seq,
    pub kind: ControlKind,
}

/// Why a provider connection is down — app-renderable classification.
/// `Unauthenticated`: credentials rejected or an auth session lapsed;
/// retrying without new credentials cannot help. `Disabled`: the provider
/// was deliberately disabled (its settings were removed), so the drop is
/// not a fault and no reconnect is coming until it is enabled again.
/// `CompanionUnreachable` is reserved (spec appendix: IBKR's local gateway)
/// — nothing emits it yet.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DisconnectCause {
    /// A transport or provider fault; a reconnect is scheduled or in flight.
    #[default]
    Error,
    /// Credentials rejected or an auth session lapsed; retrying without new
    /// credentials cannot help.
    Unauthenticated,
    /// A required companion process is unreachable (reserved; nothing emits
    /// it yet).
    CompanionUnreachable,
    /// The provider was deliberately disabled; not a fault, and no reconnect
    /// is coming until it is enabled again.
    Disabled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ControlKind {
    /// Provider connection established or re-established.
    ProviderConnected { provider: String },
    /// Provider connection lost; a reconnect attempt is scheduled or in flight.
    ProviderDisconnected {
        provider: String,
        reason: String,
        /// Classified cause; `Error` on frames that predate the field.
        #[serde(default)]
        cause: DisconnectCause,
    },
    /// Subscription state changed (acknowledged by the provider). Each
    /// session subscribes to exactly one `(instrument, kind)` pair, so the
    /// notification carries the same shape.
    SubscriptionChanged {
        provider: String,
        instrument: Instrument,
        kind: EventKind,
        active: bool,
    },
    /// Datamancer detected a gap in a provider's stream (sequence break,
    /// dropped messages, or a reconnect with missed window). It names the
    /// instrument only, so with two kinds on one instrument it does not say
    /// which kind.
    Gap {
        provider: String,
        instrument: Instrument,
        span: GapSpan,
    },
    /// A non-fatal provider error worth surfacing to the consumer. Build it
    /// with [`ControlKind::provider_error`]; its [`ErrorOrigin`] says whether
    /// it is one copy of an error sent to every subscription or an error
    /// about this subscription alone.
    #[non_exhaustive]
    ProviderError {
        /// The provider that reported the error.
        provider: String,
        /// The provider's error text.
        message: String,
        /// Where the error came from; [`ErrorOrigin::Subscription`] on frames
        /// that predate the field.
        #[serde(default = "subscription_origin")]
        origin: ErrorOrigin,
    },
    /// The session is closing in response to an explicit `close()`.
    SessionClosing,
}

impl ControlKind {
    /// Builds a [`ControlKind::ProviderError`].
    ///
    /// # Arguments
    ///
    /// * `provider` - the provider that reported the error.
    /// * `message` - the provider's error text.
    /// * `origin` - an [`ErrorOrigin::Fanout`] for one error sent to every
    ///   subscription, an [`ErrorOrigin::Subscription`] for an error about
    ///   one subscription.
    #[must_use]
    pub fn provider_error(
        provider: impl Into<String>,
        message: impl Into<String>,
        origin: ErrorOrigin,
    ) -> Self {
        Self::ProviderError {
            provider: provider.into(),
            message: message.into(),
            origin,
        }
    }
}

/// Where a [`ControlKind::ProviderError`] came from, which decides whether a
/// client session delivers it.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorOrigin {
    /// One error a provider sent to every subscription. Its copies share the
    /// [`FanoutId`], and a client session delivers one of them (see [`Seq`]).
    Fanout(FanoutId),
    /// An error about this one subscription. A client session always
    /// delivers it, never treating it as a copy of another (see [`Seq`]).
    Subscription,
}

fn subscription_origin() -> ErrorOrigin {
    ErrorOrigin::Subscription
}

/// The identity of one [`ErrorOrigin::Fanout`] error, shared by its copies.
///
/// A provider allocates these from one [`FanoutCounter`] per connection hub,
/// in emission order, so each subscription receives a hub's fan-outs in
/// increasing [`FanoutId`] order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FanoutId(u64);

/// Allocates [`FanoutId`]s in increasing order: a provider keeps one per
/// connection hub and allocates once per error it sends to every
/// subscription.
#[derive(Debug, Default)]
pub struct FanoutCounter {
    next: u64,
}

impl FanoutCounter {
    /// Allocates the next [`FanoutId`], greater than every one this counter
    /// allocated before.
    #[must_use]
    pub fn allocate(&mut self) -> FanoutId {
        let id = FanoutId(self.next);
        self.next += 1;
        id
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GapSpan {
    pub from_source_ts: Timestamp,
    pub to_source_ts: Timestamp,
}

#[cfg(test)]
mod serde_tests {
    use super::{
        BarInterval, ControlKind, DisconnectCause, ErrorOrigin, EventKind, FanoutCounter, GapSpan,
        Seq, Timestamp,
    };

    #[test]
    fn provider_error_origin_defaults_on_old_frames() {
        let old = r#"{"ProviderError":{"provider":"p","message":"HTTP 403"}}"#;
        let kind: ControlKind = serde_json::from_str(old).unwrap();
        assert_eq!(
            kind,
            ControlKind::provider_error("p", "HTTP 403", ErrorOrigin::Subscription)
        );
    }

    #[test]
    fn provider_error_round_trips_with_its_origin() {
        let mut fanouts = FanoutCounter::default();
        let _ = fanouts.allocate();
        for origin in [
            ErrorOrigin::Fanout(fanouts.allocate()),
            ErrorOrigin::Subscription,
        ] {
            let kind = ControlKind::provider_error("p", "HTTP 403", origin);
            let json = serde_json::to_string(&kind).unwrap();
            assert_eq!(serde_json::from_str::<ControlKind>(&json).unwrap(), kind);
        }
        let json = serde_json::to_string(&ErrorOrigin::Fanout(fanouts.allocate())).unwrap();
        assert_eq!(json, r#"{"fanout":2}"#);
    }

    #[test]
    fn fanout_counter_allocates_increasing_ids() {
        let mut fanouts = FanoutCounter::default();
        let first = fanouts.allocate();
        let second = fanouts.allocate();
        assert!(first < second);
    }

    #[test]
    fn provider_disconnected_cause_defaults_on_old_frames() {
        // A pre-cycle-4 frame has no `cause`; it must parse as Error.
        let old = r#"{"ProviderDisconnected":{"provider":"p","reason":"ws closed"}}"#;
        let kind: ControlKind = serde_json::from_str(old).unwrap();
        assert!(matches!(
            kind,
            ControlKind::ProviderDisconnected {
                cause: DisconnectCause::Error,
                ..
            }
        ));
    }

    #[test]
    fn disconnect_cause_wire_names_are_stable() {
        for (cause, wire) in [
            (DisconnectCause::Error, "\"error\""),
            (DisconnectCause::Unauthenticated, "\"unauthenticated\""),
            (
                DisconnectCause::CompanionUnreachable,
                "\"companion_unreachable\"",
            ),
            (DisconnectCause::Disabled, "\"disabled\""),
        ] {
            let json = serde_json::to_string(&cause).unwrap();
            assert_eq!(json, wire);
            assert_eq!(
                serde_json::from_str::<DisconnectCause>(&json).unwrap(),
                cause
            );
        }
    }

    #[test]
    fn seq_round_trips_transparently() {
        let json = serde_json::to_string(&Seq(42)).unwrap();
        assert_eq!(json, "42");
        assert_eq!(serde_json::from_str::<Seq>(&json).unwrap(), Seq(42));
    }

    #[test]
    fn event_kind_round_trips() {
        for k in [
            EventKind::Trade,
            EventKind::Quote,
            EventKind::Bar(BarInterval::OneMinute),
            EventKind::Bar(BarInterval::OneDay),
        ] {
            let json = serde_json::to_string(&k).unwrap();
            assert_eq!(serde_json::from_str::<EventKind>(&json).unwrap(), k);
        }
    }

    #[test]
    fn gap_span_round_trips() {
        let g = GapSpan {
            from_source_ts: Timestamp(100),
            to_source_ts: Timestamp(200),
        };
        let json = serde_json::to_string(&g).unwrap();
        assert_eq!(serde_json::from_str::<GapSpan>(&json).unwrap(), g);
    }

    #[test]
    fn event_kind_enumerate_covers_the_full_kind_space() {
        let kinds: Vec<EventKind> = EventKind::enumerate().collect();
        // Trade + Quote + one Bar per interval.
        assert_eq!(kinds.len(), 2 + BarInterval::ALL.len());
        assert!(kinds.contains(&EventKind::Trade));
        assert!(kinds.contains(&EventKind::Quote));
        for interval in BarInterval::ALL {
            assert!(kinds.contains(&EventKind::Bar(interval)));
        }
        // No duplicates.
        let mut dedup = kinds.clone();
        dedup.sort_unstable();
        dedup.dedup();
        assert_eq!(dedup.len(), kinds.len());
    }
}
