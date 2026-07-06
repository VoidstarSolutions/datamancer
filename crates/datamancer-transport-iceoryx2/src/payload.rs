//! The flat `#[repr(C)]` POD data payload and its logical <-> wire conversions.
//!
//! The body is a **flat struct of all possible fields** sized to the largest
//! variant rather than a `union`: union field access is `unsafe`, which would
//! break the crate's `#![forbid(unsafe_code)]`. The wasted bytes are acceptable
//! at this payload size.
//!
//! Connection-scoped controls (`ProviderConnected`/`ProviderDisconnected`/
//! `ProviderError`) are **suppressed** here — they ride the diagnostics plane
//! only — so [`to_pod`] returns `None` for them. `SessionClosing` is a bare tag
//! routed to [`SymbolId::CONNECTION`]; per-symbol `Gap`/`SubscriptionChanged`
//! carry their real [`SymbolId`]. The `provider` string on those controls is
//! recovered from the resolved [`Instrument`] (Alpaca-only), never carried on
//! the hot path.

use datamancer_core::{
    Bar, BarInterval, Control, ControlKind, EventKind, GapSpan, Instrument, MarketEvent, Price,
    Quantity, Quote, Seq, Timestamp, Trade,
};
use iceoryx2::prelude::ZeroCopySend;

use crate::symbol_table::{InstrumentTooLong, SymbolId, SymbolResolver, SymbolTable};

/// Top-level payload discriminant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PayloadKind {
    Trade = 0,
    Quote = 1,
    Bar = 2,
    Control = 3,
}

/// Sub-discriminant for [`PayloadKind::Control`] payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ControlTag {
    Gap = 0,
    SubscriptionChanged = 1,
    SessionClosing = 2,
}

const BAR_TAG_TRADE: u8 = 0;
const BAR_TAG_QUOTE: u8 = 1;
const BAR_TAG_BAR: u8 = 2;

fn interval_to_tag(interval: BarInterval) -> u8 {
    match interval {
        BarInterval::OneSecond => 0,
        BarInterval::OneMinute => 1,
        BarInterval::FiveMinute => 2,
        BarInterval::FifteenMinute => 3,
        BarInterval::OneHour => 4,
        BarInterval::OneDay => 5,
    }
}

fn tag_to_interval(tag: u8) -> Option<BarInterval> {
    Some(match tag {
        0 => BarInterval::OneSecond,
        1 => BarInterval::OneMinute,
        2 => BarInterval::FiveMinute,
        3 => BarInterval::FifteenMinute,
        4 => BarInterval::OneHour,
        5 => BarInterval::OneDay,
        _ => return None,
    })
}

fn event_kind_to_tags(kind: EventKind) -> (u8, u8) {
    match kind {
        EventKind::Trade => (BAR_TAG_TRADE, 0),
        EventKind::Quote => (BAR_TAG_QUOTE, 0),
        EventKind::Bar(interval) => (BAR_TAG_BAR, interval_to_tag(interval)),
    }
}

fn tags_to_event_kind(kind_tag: u8, interval_tag: u8) -> Option<EventKind> {
    Some(match kind_tag {
        BAR_TAG_TRADE => EventKind::Trade,
        BAR_TAG_QUOTE => EventKind::Quote,
        BAR_TAG_BAR => EventKind::Bar(tag_to_interval(interval_tag)?),
        _ => return None,
    })
}

/// The flat, fixed-size, `Copy` wire record. All variants share one layout;
/// only the fields meaningful for `kind` (and `control_tag` when `kind` is
/// `Control`) are populated. Timestamp/order fields (`source_ts`, `rx_ts`,
/// `seq`) are preserved end-to-end; `seq` carries [`Seq::SYNTHETIC`] verbatim.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ZeroCopySend)]
#[repr(C)]
pub struct DataPayload {
    /// [`PayloadKind`] discriminant.
    pub kind: u8,
    /// [`ControlTag`] discriminant (only when `kind == Control`).
    pub control_tag: u8,
    /// Bar interval tag (Bar variant) or `SubscriptionChanged`'s bar interval.
    pub interval: u8,
    /// `SubscriptionChanged`'s `EventKind` tag.
    pub sub_kind: u8,
    /// `SubscriptionChanged`'s `active` flag (0/1).
    pub active: u8,
    /// Symbol handle; [`SymbolId::CONNECTION`] for connection-scoped controls.
    pub symbol: SymbolId,
    pub seq: u64,
    pub source_ts: i64,
    pub rx_ts: i64,
    /// Trade price / Quote bid / Bar open / Gap `from_source_ts`.
    pub a: i64,
    /// Quote ask / Bar high / Gap `to_source_ts`.
    pub b: i64,
    /// Bar low.
    pub c: i64,
    /// Bar close.
    pub d: i64,
    /// Trade size / Quote `bid_size` / Bar volume, in raw `Quantity` units
    /// (1e-9 of a base unit). Layout is unchanged from the pre-`Quantity`
    /// `u64`; only the interpretation is now fixed-point — which is exactly
    /// why the service names embed [`WIRE_VERSION`](crate::WIRE_VERSION):
    /// a mixed-version peer must fail to attach, not read 1e9x-wrong sizes.
    pub size0: u64,
    /// Quote `ask_size`, in raw `Quantity` units (1e-9 of a base unit).
    pub size1: u64,
}

impl DataPayload {
    fn blank(kind: PayloadKind, symbol: SymbolId, source_ts: i64, rx_ts: i64, seq: u64) -> Self {
        Self {
            kind: kind as u8,
            control_tag: 0,
            interval: 0,
            sub_kind: 0,
            active: 0,
            symbol,
            seq,
            source_ts,
            rx_ts,
            a: 0,
            b: 0,
            c: 0,
            d: 0,
            size0: 0,
            size1: 0,
        }
    }
}

/// Convert a logical event to its wire payload, interning its instrument.
///
/// Returns `Ok(None)` for connection-scoped controls that are **suppressed on
/// the data plane** (`ProviderConnected`/`ProviderDisconnected`/
/// `ProviderError`); those surface on the diagnostics plane instead.
///
/// # Errors
///
/// Returns [`InstrumentTooLong`] if the event's instrument cannot be interned
/// (its encoded tuple exceeds the announcement capacity).
pub fn to_pod(
    ev: &MarketEvent,
    table: &mut SymbolTable,
) -> Result<Option<DataPayload>, InstrumentTooLong> {
    let payload = match ev {
        MarketEvent::Trade(t) => {
            let symbol = table.intern(&t.instrument)?;
            let mut p = DataPayload::blank(
                PayloadKind::Trade,
                symbol,
                t.source_ts.0,
                t.rx_ts.0,
                t.seq.0,
            );
            p.a = t.price.0;
            p.size0 = t.size.raw();
            p
        }
        MarketEvent::Quote(q) => {
            let symbol = table.intern(&q.instrument)?;
            let mut p = DataPayload::blank(
                PayloadKind::Quote,
                symbol,
                q.source_ts.0,
                q.rx_ts.0,
                q.seq.0,
            );
            p.a = q.bid.0;
            p.b = q.ask.0;
            p.size0 = q.bid_size.raw();
            p.size1 = q.ask_size.raw();
            p
        }
        MarketEvent::Bar(b) => {
            let symbol = table.intern(&b.instrument)?;
            let mut p =
                DataPayload::blank(PayloadKind::Bar, symbol, b.source_ts.0, b.rx_ts.0, b.seq.0);
            p.interval = interval_to_tag(b.interval);
            p.a = b.open.0;
            p.b = b.high.0;
            p.c = b.low.0;
            p.d = b.close.0;
            p.size0 = b.volume.raw();
            p
        }
        MarketEvent::Control(c) => return control_to_pod(c, table),
        // `MarketEvent` is `#[non_exhaustive]`: a future data variant is not
        // known to this transport version, so it is not forwarded on the data
        // plane (it would surface via a newer transport build).
        _ => return Ok(None),
    };
    Ok(Some(payload))
}

fn control_to_pod(
    c: &Control,
    table: &mut SymbolTable,
) -> Result<Option<DataPayload>, InstrumentTooLong> {
    let payload = match &c.kind {
        // Connection-scoped, free-text: suppressed on the data plane.
        ControlKind::ProviderConnected { .. }
        | ControlKind::ProviderDisconnected { .. }
        | ControlKind::ProviderError { .. } => return Ok(None),
        ControlKind::SessionClosing => {
            let mut p = DataPayload::blank(
                PayloadKind::Control,
                SymbolId::CONNECTION,
                c.source_ts.0,
                c.rx_ts.0,
                c.seq.0,
            );
            p.control_tag = ControlTag::SessionClosing as u8;
            p
        }
        ControlKind::Gap {
            instrument, span, ..
        } => {
            let symbol = table.intern(instrument)?;
            let mut p = DataPayload::blank(
                PayloadKind::Control,
                symbol,
                c.source_ts.0,
                c.rx_ts.0,
                c.seq.0,
            );
            p.control_tag = ControlTag::Gap as u8;
            p.a = span.from_source_ts.0;
            p.b = span.to_source_ts.0;
            p
        }
        ControlKind::SubscriptionChanged {
            instrument,
            kind,
            active,
            ..
        } => {
            let symbol = table.intern(instrument)?;
            let mut p = DataPayload::blank(
                PayloadKind::Control,
                symbol,
                c.source_ts.0,
                c.rx_ts.0,
                c.seq.0,
            );
            p.control_tag = ControlTag::SubscriptionChanged as u8;
            let (kind_tag, interval_tag) = event_kind_to_tags(*kind);
            p.sub_kind = kind_tag;
            p.interval = interval_tag;
            p.active = u8::from(*active);
            p
        }
    };
    Ok(Some(payload))
}

/// Error reconstructing a logical event from a wire payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FromPodError {
    /// The payload's `symbol` has no announcement yet; the subscriber must hold
    /// the sample until its [`SymbolAnnouncement`](crate::SymbolAnnouncement)
    /// arrives.
    Unresolved(SymbolId),
    /// A discriminant byte did not map to a known variant.
    BadDiscriminant,
}

impl std::fmt::Display for FromPodError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unresolved(id) => write!(f, "unresolved symbol id {}", id.0),
            Self::BadDiscriminant => f.write_str("unknown payload discriminant"),
        }
    }
}

impl std::error::Error for FromPodError {}

fn resolve(resolver: &SymbolResolver, id: SymbolId) -> Result<&Instrument, FromPodError> {
    resolver.resolve(id).ok_or(FromPodError::Unresolved(id))
}

/// Reconstruct a logical event from a wire payload, resolving its symbol.
///
/// `rx_ts` is carried through verbatim — never synthesized. The control
/// `provider` string is recovered from the resolved instrument (Alpaca-only).
///
/// # Errors
///
/// Returns [`FromPodError::Unresolved`] if the payload references a symbol with
/// no announcement yet (the caller holds it), or
/// [`FromPodError::BadDiscriminant`] for an unknown discriminant.
pub fn from_pod(p: &DataPayload, resolver: &SymbolResolver) -> Result<MarketEvent, FromPodError> {
    let source_ts = Timestamp(p.source_ts);
    let rx_ts = Timestamp(p.rx_ts);
    let seq = Seq(p.seq);
    match p.kind {
        x if x == PayloadKind::Trade as u8 => {
            let instrument = resolve(resolver, p.symbol)?.clone();
            Ok(MarketEvent::Trade(Trade {
                instrument,
                source_ts,
                rx_ts,
                seq,
                price: Price(p.a),
                size: Quantity::from_raw(p.size0),
            }))
        }
        x if x == PayloadKind::Quote as u8 => {
            let instrument = resolve(resolver, p.symbol)?.clone();
            Ok(MarketEvent::Quote(Quote {
                instrument,
                source_ts,
                rx_ts,
                seq,
                bid: Price(p.a),
                bid_size: Quantity::from_raw(p.size0),
                ask: Price(p.b),
                ask_size: Quantity::from_raw(p.size1),
            }))
        }
        x if x == PayloadKind::Bar as u8 => {
            let instrument = resolve(resolver, p.symbol)?.clone();
            let interval = tag_to_interval(p.interval).ok_or(FromPodError::BadDiscriminant)?;
            Ok(MarketEvent::Bar(Bar {
                instrument,
                interval,
                source_ts,
                rx_ts,
                seq,
                open: Price(p.a),
                high: Price(p.b),
                low: Price(p.c),
                close: Price(p.d),
                volume: Quantity::from_raw(p.size0),
            }))
        }
        x if x == PayloadKind::Control as u8 => {
            control_from_pod(p, resolver, source_ts, rx_ts, seq)
        }
        _ => Err(FromPodError::BadDiscriminant),
    }
}

fn control_from_pod(
    p: &DataPayload,
    resolver: &SymbolResolver,
    source_ts: Timestamp,
    rx_ts: Timestamp,
    seq: Seq,
) -> Result<MarketEvent, FromPodError> {
    let kind = match p.control_tag {
        x if x == ControlTag::SessionClosing as u8 => ControlKind::SessionClosing,
        x if x == ControlTag::Gap as u8 => {
            let instrument = resolve(resolver, p.symbol)?.clone();
            let provider = instrument.provider().as_str().to_string();
            ControlKind::Gap {
                provider,
                instrument,
                span: GapSpan {
                    from_source_ts: Timestamp(p.a),
                    to_source_ts: Timestamp(p.b),
                },
            }
        }
        x if x == ControlTag::SubscriptionChanged as u8 => {
            let instrument = resolve(resolver, p.symbol)?.clone();
            let provider = instrument.provider().as_str().to_string();
            let kind =
                tags_to_event_kind(p.sub_kind, p.interval).ok_or(FromPodError::BadDiscriminant)?;
            ControlKind::SubscriptionChanged {
                provider,
                instrument,
                kind,
                active: p.active != 0,
            }
        }
        _ => return Err(FromPodError::BadDiscriminant),
    };
    Ok(MarketEvent::Control(Control {
        source_ts,
        rx_ts,
        seq,
        kind,
    }))
}

#[cfg(test)]
mod tests {
    use super::{DataPayload, FromPodError, PayloadKind, from_pod, to_pod};
    use crate::symbol_table::{SymbolId, SymbolResolver, SymbolTable};
    use datamancer_core::{
        AssetClass, Bar, BarInterval, Control, ControlKind, DisconnectCause, EventKind, GapSpan,
        Instrument, MarketEvent, Price, ProviderId, Quantity, Quote, Seq, Timestamp, Trade,
    };

    fn inst(symbol: &str) -> Instrument {
        Instrument::new(
            ProviderId::from_static("alpaca"),
            AssetClass::Crypto,
            symbol,
        )
    }

    /// Round-trip a logical event through `to_pod` -> `from_pod`, building a
    /// resolver from the table's announcements.
    fn round_trip(ev: &MarketEvent) -> MarketEvent {
        let mut table = SymbolTable::new();
        let pod = to_pod(ev, &mut table).unwrap().unwrap();
        let mut resolver = SymbolResolver::new();
        for a in table.announcements() {
            resolver.apply(&a).unwrap();
        }
        from_pod(&pod, &resolver).unwrap()
    }

    #[test]
    fn trade_round_trips() {
        let ev = MarketEvent::Trade(Trade {
            instrument: inst("BTC/USD"),
            source_ts: Timestamp(111),
            rx_ts: Timestamp(222),
            seq: Seq(7),
            price: Price(123_456),
            // 0.004 BTC in raw Quantity units — a fractional size must survive
            // the POD round trip intact.
            size: Quantity::from_raw(4_000_000),
        });
        assert_eq!(round_trip(&ev), ev);
    }

    #[test]
    fn quote_round_trips() {
        let ev = MarketEvent::Quote(Quote {
            instrument: inst("ETH/USD"),
            source_ts: Timestamp(1),
            rx_ts: Timestamp(2),
            seq: Seq(3),
            bid: Price(100),
            bid_size: Quantity::from_raw(10),
            ask: Price(200),
            ask_size: Quantity::from_raw(20),
        });
        assert_eq!(round_trip(&ev), ev);
    }

    #[test]
    fn bar_round_trips_each_interval() {
        for interval in [
            BarInterval::OneSecond,
            BarInterval::OneMinute,
            BarInterval::FiveMinute,
            BarInterval::FifteenMinute,
            BarInterval::OneHour,
            BarInterval::OneDay,
        ] {
            let ev = MarketEvent::Bar(Bar {
                instrument: inst("BTC/USD"),
                interval,
                source_ts: Timestamp(10),
                rx_ts: Timestamp(20),
                seq: Seq(5),
                open: Price(1),
                high: Price(4),
                low: Price(0),
                close: Price(3),
                volume: Quantity::from_raw(1000),
            });
            assert_eq!(round_trip(&ev), ev, "interval {interval:?}");
        }
    }

    #[test]
    fn gap_control_round_trips_with_provider() {
        let ev = MarketEvent::Control(Control {
            source_ts: Timestamp(1),
            rx_ts: Timestamp(2),
            seq: Seq(9),
            kind: ControlKind::Gap {
                provider: "alpaca".to_string(),
                instrument: inst("BTC/USD"),
                span: GapSpan {
                    from_source_ts: Timestamp(100),
                    to_source_ts: Timestamp(200),
                },
            },
        });
        assert_eq!(round_trip(&ev), ev);
    }

    #[test]
    fn subscription_changed_round_trips() {
        let ev = MarketEvent::Control(Control {
            source_ts: Timestamp(1),
            rx_ts: Timestamp(2),
            seq: Seq(9),
            kind: ControlKind::SubscriptionChanged {
                provider: "alpaca".to_string(),
                instrument: inst("BTC/USD"),
                kind: EventKind::Bar(BarInterval::FiveMinute),
                active: true,
            },
        });
        assert_eq!(round_trip(&ev), ev);
    }

    #[test]
    fn session_closing_routes_to_connection_sentinel() {
        let ev = MarketEvent::Control(Control {
            source_ts: Timestamp(1),
            rx_ts: Timestamp(2),
            seq: Seq::SYNTHETIC,
            kind: ControlKind::SessionClosing,
        });
        let mut table = SymbolTable::new();
        let pod = to_pod(&ev, &mut table).unwrap().unwrap();
        assert_eq!(pod.symbol, SymbolId::CONNECTION);
        // No instrument needed to reconstruct it.
        let resolver = SymbolResolver::new();
        assert_eq!(from_pod(&pod, &resolver).unwrap(), ev);
    }

    #[test]
    fn synthetic_seq_survives_round_trip() {
        let ev = MarketEvent::Control(Control {
            source_ts: Timestamp(1),
            rx_ts: Timestamp(2),
            seq: Seq::SYNTHETIC,
            kind: ControlKind::SessionClosing,
        });
        let mut table = SymbolTable::new();
        let pod = to_pod(&ev, &mut table).unwrap().unwrap();
        assert_eq!(pod.seq, u64::MAX);
        assert_eq!(round_trip(&ev), ev);
    }

    #[test]
    fn connection_scoped_controls_are_suppressed() {
        for kind in [
            ControlKind::ProviderConnected {
                provider: "alpaca".to_string(),
            },
            ControlKind::ProviderDisconnected {
                provider: "alpaca".to_string(),
                reason: "boom".to_string(),
                cause: DisconnectCause::Error,
            },
            ControlKind::ProviderError {
                provider: "alpaca".to_string(),
                message: "oops".to_string(),
            },
        ] {
            let ev = MarketEvent::Control(Control {
                source_ts: Timestamp(1),
                rx_ts: Timestamp(2),
                seq: Seq(3),
                kind,
            });
            let mut table = SymbolTable::new();
            assert!(to_pod(&ev, &mut table).unwrap().is_none());
        }
    }

    #[test]
    fn per_symbol_controls_carry_real_symbol_not_sentinel() {
        let ev = MarketEvent::Control(Control {
            source_ts: Timestamp(1),
            rx_ts: Timestamp(2),
            seq: Seq(3),
            kind: ControlKind::Gap {
                provider: "alpaca".to_string(),
                instrument: inst("BTC/USD"),
                span: GapSpan {
                    from_source_ts: Timestamp(0),
                    to_source_ts: Timestamp(1),
                },
            },
        });
        let mut table = SymbolTable::new();
        let pod = to_pod(&ev, &mut table).unwrap().unwrap();
        assert_ne!(pod.symbol, SymbolId::CONNECTION);
        assert_eq!(pod.symbol, SymbolId(0));
    }

    #[test]
    fn rx_ts_is_carried_not_synthesized() {
        let ev = MarketEvent::Trade(Trade {
            instrument: inst("BTC/USD"),
            source_ts: Timestamp(111),
            rx_ts: Timestamp(999_999),
            seq: Seq(1),
            price: Price(1),
            size: Quantity::from_raw(1),
        });
        let mut table = SymbolTable::new();
        let pod = to_pod(&ev, &mut table).unwrap().unwrap();
        assert_eq!(pod.rx_ts, 999_999);
        assert_ne!(pod.rx_ts, pod.source_ts);
    }

    #[test]
    fn unresolved_symbol_is_held_not_dropped() {
        let pod = DataPayload {
            kind: PayloadKind::Trade as u8,
            ..DataPayload {
                kind: 0,
                control_tag: 0,
                interval: 0,
                sub_kind: 0,
                active: 0,
                symbol: SymbolId(42),
                seq: 1,
                source_ts: 1,
                rx_ts: 2,
                a: 1,
                b: 0,
                c: 0,
                d: 0,
                size0: 1,
                size1: 0,
            }
        };
        let resolver = SymbolResolver::new();
        assert_eq!(
            from_pod(&pod, &resolver),
            Err(FromPodError::Unresolved(SymbolId(42)))
        );
    }
}
