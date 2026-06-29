//! Public event model.
//!
//! Datamancer's output is a single ordered stream of [`MarketEvent`]. Variants
//! cover live and historical market data plus in-band [`Control`] entries for
//! connectivity, subscription state, and gap reporting.

use serde::{Deserialize, Serialize};

use crate::{Instrument, Price};

/// A per-symbol ordering identifier stamped **once at the source** of an
/// authoritative per-`(instrument, kind)` session, in that session's canonical
/// delivery order, before any sink.
///
/// **The sole ordering field for the stream.** Invariants:
///
/// - **Per-symbol, not global.** `seq` orders one symbol's stream; there is no
///   cross-instrument order. The multiplex ordering key is `(instrument, seq)`.
/// - **Stamped at the source.** The authoritative controller assigns `seq` once,
///   in delivery order, before the event reaches any sink — so it is a property
///   of the shared stream, not of a particular consumer's poll timing.
/// - **Identical across a symbol's consumers.** Every consumer of one symbol's
///   authoritative stream observes the same `(seq, source_ts)` for each event.
/// - **Controls occupy slots.** In-band [`Control`] events are stamped here too,
///   so they consume a `seq` slot like data events.
/// - **Holes are real.** A consumer that misses events (resume-buffer eviction,
///   late join) observes a real `seq` hole, surfaced in-band as
///   [`ControlKind::Gap`]. The delivered stream is contiguous *only while
///   nothing is lost*.
/// - **`SYNTHETIC` is exempt.** [`Seq::SYNTHETIC`] tags out-of-band synthetic
///   control events and is exempt from per-symbol monotonicity.
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
    /// outside the source counter). `Seq(u64::MAX)` is unreachable by the
    /// monotonic source counter, so it never collides with a stamped event, and
    /// it is documented exempt from per-symbol monotonicity.
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

/// Selector used in subscriptions. Each variant maps 1:1 with a [`MarketEvent`]
/// data variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum EventKind {
    Trade,
    Quote,
    Bar(BarInterval),
}

/// The unified output stream entry.
///
/// Every data variant carries three fields whose roles must not be conflated:
///
/// - `source_ts` — when the event happened in the market. The **only**
///   timestamp engine logic should reason about.
/// - `seq` — datamancer's per-symbol, source-stamped ordering field.
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
    pub size: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Quote {
    pub instrument: Instrument,
    pub source_ts: Timestamp,
    pub rx_ts: Timestamp,
    pub seq: Seq,
    pub bid: Price,
    pub bid_size: u64,
    pub ask: Price,
    pub ask_size: u64,
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
    pub volume: u64,
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

#[derive(Debug, Clone, PartialEq)]
pub enum ControlKind {
    /// Provider connection established or re-established.
    ProviderConnected { provider: String },
    /// Provider connection lost; a reconnect attempt is scheduled or in flight.
    ProviderDisconnected { provider: String, reason: String },
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
    /// dropped messages, or a reconnect with missed window).
    Gap {
        provider: String,
        instrument: Instrument,
        span: GapSpan,
    },
    /// A non-fatal provider error worth surfacing to the consumer.
    ProviderError { provider: String, message: String },
    /// The session is closing in response to an explicit `close()`.
    SessionClosing,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GapSpan {
    pub from_source_ts: Timestamp,
    pub to_source_ts: Timestamp,
}

#[cfg(test)]
mod serde_tests {
    use super::{BarInterval, EventKind, GapSpan, Seq, Timestamp};

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
}
