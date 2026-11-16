//! Public event model.
//!
//! Datamancer's output is a single ordered stream of [`MarketEvent`]. Variants
//! cover live and historical market data plus in-band [`Control`] entries for
//! connectivity, subscription state, and gap reporting.

use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use crate::{Instrument, Price, Quantity, SubscriptionRef};

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
///     For a [`ControlKind::ProviderError`] that means the copies of one
///     [`ErrorOrigin::Fanout`]: of them a client session delivers one, on
///     whichever subscription, and the others' slots stay empty. A copy
///     handed to the consumer's stream is the delivered one even if that
///     stream is then dropped unread. The session remembers a bounded
///     number of delivered fan-outs, so a copy that arrives long after may
///     be delivered again. An [`ErrorOrigin::Subscription`] error is never
///     dropped as a copy of another;
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
///     empty;
///   - a subscription error that met a full consumer channel, with 64 newer
///     ones owed to it before the channel had room, is dropped and its slot
///     stays empty.
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
    /// with [`ControlKind::fanout_error`] for one copy of an error sent to
    /// every subscription, or [`ControlKind::subscription_error`] for an error
    /// about this subscription alone; its [`ErrorOrigin`] records which.
    ///
    /// Two fan-out errors are equal only as copies of one fan-out: equal
    /// provider and text do not make them equal, and a deserialized one is
    /// never equal to the one that was serialized.
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
        /// The subscription a [`ControlKind::subscription_error`] is about;
        /// `None` for a fan-out error, and on frames that predate the field.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        subscription: Option<SubscriptionRef>,
    },
    /// The session is closing in response to an explicit `close()`.
    SessionClosing,
}

impl ControlKind {
    /// Builds a [`ControlKind::ProviderError`] a provider sends to every
    /// subscription, with a fresh [`FanoutId`].
    ///
    /// Build it once and send a clone to each route: the clones share the id,
    /// which is what lets a client session deliver the error once and not
    /// once per route ([`ErrorOrigin::Fanout`] states the rule and its
    /// limits).
    /// A second call makes a different fan-out, whatever its text.
    ///
    /// # Arguments
    ///
    /// * `provider` - the provider that reported the error.
    /// * `message` - the provider's error text.
    ///
    /// # Examples
    ///
    /// ```
    /// use datamancer_core::ControlKind;
    ///
    /// let error = ControlKind::fanout_error("venue", "HTTP 403");
    /// let copy_for_another_route = error.clone();
    /// assert_eq!(error, copy_for_another_route);
    ///
    /// let built_again = ControlKind::fanout_error("venue", "HTTP 403");
    /// assert_ne!(error, built_again);
    /// ```
    #[must_use]
    pub fn fanout_error(provider: impl Into<String>, message: impl Into<String>) -> Self {
        Self::provider_error(
            provider,
            message,
            ErrorOrigin::Fanout(FanoutId::next()),
            None,
        )
    }

    /// Builds a [`ControlKind::ProviderError`] about one subscription, which a
    /// client session never drops as a copy of another. It names the
    /// subscription, so a consumer of a multiplexed stream can tell which
    /// pair failed.
    ///
    /// # Arguments
    ///
    /// * `provider` - the provider that reported the error.
    /// * `instrument` - the subscription's instrument.
    /// * `kind` - the subscription's event kind.
    /// * `message` - the provider's error text.
    #[must_use]
    pub fn subscription_error(
        provider: impl Into<String>,
        instrument: Instrument,
        kind: EventKind,
        message: impl Into<String>,
    ) -> Self {
        Self::provider_error(
            provider,
            message,
            ErrorOrigin::Subscription,
            Some(SubscriptionRef { instrument, kind }),
        )
    }

    fn provider_error(
        provider: impl Into<String>,
        message: impl Into<String>,
        origin: ErrorOrigin,
        subscription: Option<SubscriptionRef>,
    ) -> Self {
        Self::ProviderError {
            provider: provider.into(),
            message: message.into(),
            origin,
            subscription,
        }
    }
}

/// Where a [`ControlKind::ProviderError`] came from, which decides whether a
/// client session delivers it.
///
/// On the wire it is its tag alone, `"fanout"` or `"subscription"`: a
/// [`FanoutId`] is never serialized, and each deserialized fan-out takes a
/// fresh id of the receiving process.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(from = "OriginTag", into = "OriginTag")]
pub enum ErrorOrigin {
    /// One error a provider sent to every subscription, built by
    /// [`ControlKind::fanout_error`]. Its copies share the [`FanoutId`], and
    /// of them a client session normally delivers exactly one. It can
    /// deliver none: a control is not data, and a buffer a copy waits in
    /// may drop it with no `Gap` ([`Seq`] describes when a control is
    /// dropped). It can deliver more than one once it has forgotten the id.
    /// Two of these are equal only as copies of one fan-out.
    Fanout(FanoutId),
    /// An error about this one subscription, built by
    /// [`ControlKind::subscription_error`]. A client session never treats it
    /// as a copy of another (see [`Seq`]).
    Subscription,
}

fn subscription_origin() -> ErrorOrigin {
    ErrorOrigin::Subscription
}

/// The wire form of an [`ErrorOrigin`].
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OriginTag {
    Fanout,
    Subscription,
}

impl From<ErrorOrigin> for OriginTag {
    fn from(origin: ErrorOrigin) -> Self {
        match origin {
            ErrorOrigin::Fanout(_) => Self::Fanout,
            ErrorOrigin::Subscription => Self::Subscription,
        }
    }
}

impl From<OriginTag> for ErrorOrigin {
    fn from(tag: OriginTag) -> Self {
        match tag {
            OriginTag::Fanout => Self::Fanout(FanoutId::next()),
            OriginTag::Subscription => Self::Subscription,
        }
    }
}

/// The identity of one [`ErrorOrigin::Fanout`] error, shared by its copies.
///
/// Ids come from a single process-wide sequence, so no two fan-outs in one
/// process share one, and two ids are equal only as the same fan-out's. An
/// id has no wire form and never leaves its process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FanoutId(u64);

impl FanoutId {
    fn next() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
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
        BarInterval, ControlKind, DisconnectCause, ErrorOrigin, EventKind, FanoutId, GapSpan, Seq,
        Timestamp,
    };
    use crate::{AssetClass, Instrument, ProviderId, SubscriptionRef};

    fn aapl() -> Instrument {
        Instrument::new(ProviderId::from_static("p"), AssetClass::Equity, "AAPL")
    }

    #[test]
    fn provider_error_origin_defaults_on_old_frames() {
        let old = r#"{"ProviderError":{"provider":"p","message":"HTTP 403"}}"#;
        let kind: ControlKind = serde_json::from_str(old).unwrap();
        assert!(matches!(
            kind,
            ControlKind::ProviderError {
                origin: ErrorOrigin::Subscription,
                subscription: None,
                ..
            }
        ));
    }

    #[test]
    fn a_subscription_error_names_its_pair_on_the_wire() {
        let error = ControlKind::subscription_error(
            "p",
            aapl(),
            EventKind::Bar(BarInterval::OneDay),
            "HTTP 403",
        );
        let json = serde_json::to_string(&error).unwrap();
        let back: ControlKind = serde_json::from_str(&json).unwrap();
        assert_eq!(back, error);
        assert!(matches!(
            back,
            ControlKind::ProviderError {
                subscription: Some(SubscriptionRef {
                    kind: EventKind::Bar(BarInterval::OneDay),
                    ..
                }),
                ..
            }
        ));
    }

    #[test]
    fn provider_error_round_trips_with_its_origin() {
        let subscription =
            ControlKind::subscription_error("p", aapl(), EventKind::Trade, "HTTP 403");
        let json = serde_json::to_string(&subscription).unwrap();
        assert_eq!(
            serde_json::from_str::<ControlKind>(&json).unwrap(),
            subscription
        );

        let json = serde_json::to_string(&ControlKind::fanout_error("p", "HTTP 403")).unwrap();
        assert!(matches!(
            serde_json::from_str::<ControlKind>(&json).unwrap(),
            ControlKind::ProviderError {
                origin: ErrorOrigin::Fanout(_),
                ..
            }
        ));
    }

    /// A fan-out's id has no wire form: the origin serializes as its tag, and
    /// each deserialized copy takes an id of this process, so it cannot
    /// collide with one made here.
    #[test]
    fn a_deserialized_fanout_takes_a_fresh_local_id() {
        let sent = ErrorOrigin::Fanout(FanoutId::next());
        let json = serde_json::to_string(&sent).unwrap();
        assert_eq!(json, r#""fanout""#);

        let first: ErrorOrigin = serde_json::from_str(&json).unwrap();
        let second: ErrorOrigin = serde_json::from_str(&json).unwrap();
        assert!(matches!(first, ErrorOrigin::Fanout(_)), "{first:?}");
        assert_ne!(first, sent);
        assert_ne!(first, second);
        assert_eq!(
            serde_json::from_str::<ErrorOrigin>(r#""subscription""#).unwrap(),
            ErrorOrigin::Subscription
        );
    }

    #[test]
    fn fanout_ids_are_distinct() {
        assert_ne!(FanoutId::next(), FanoutId::next());
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
