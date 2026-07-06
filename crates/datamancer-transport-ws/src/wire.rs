//! The JSON event frame and its logical <-> wire conversions.
//!
//! Unlike the iceoryx2 POD, the instrument is carried **inline** on every frame
//! (JSON is self-describing; no `SymbolId` interning / announcement race).
//! Prices and quantities cross as raw fixed-point integers (`price` as `i64`,
//! sizes/`volume` as `u64`) because core `Price`/`Quantity` do not derive
//! `Serialize` — the plain field names carry raw 1e-9 units, not whole units.
//! Control kinds are flattened into top-level `type` tags. Connection-scoped
//! controls are suppressed (`to_wire` returns `None`), matching the iceoryx2
//! routing rule; a remote client reads connectivity from the `snapshot` reply.

use datamancer_core::{
    Bar, Control, ControlKind, EventKind, GapSpan, Instrument, MarketEvent, Price, Quantity, Quote,
    Seq, Timestamp, Trade,
};
use serde::{Deserialize, Serialize};

/// WebSocket subprotocol token naming this wire format's version, negotiated
/// on the handshake (`Sec-WebSocket-Protocol`) so mixed-version peers are
/// rejected before any frame crosses. The JSON field names alone cannot
/// protect against a *reinterpretation* of a field (`size: 100` parses fine
/// whether it means whole units or raw 1e-9 units). History: v1 (implicit —
/// no subprotocol) carried sizes/volumes as whole base units; v2 carries them
/// as raw 1e-9 `Quantity` units.
///
/// Servers must require and echo this token; clients must offer it and verify
/// the echo (a pre-versioning server silently ignores the offer, so the
/// missing echo is the only mismatch signal on the client side).
pub const WS_SUBPROTOCOL: &str = "datamancer.v2";

/// The tagged JSON event frame. One `type` per data/control kind.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EventFrame {
    Trade {
        instrument: Instrument,
        seq: Seq,
        source_ts: Timestamp,
        rx_ts: Timestamp,
        price: i64,
        size: u64,
    },
    Quote {
        instrument: Instrument,
        seq: Seq,
        source_ts: Timestamp,
        rx_ts: Timestamp,
        bid: i64,
        bid_size: u64,
        ask: i64,
        ask_size: u64,
    },
    Bar {
        instrument: Instrument,
        interval: BarInterval,
        seq: Seq,
        source_ts: Timestamp,
        rx_ts: Timestamp,
        open: i64,
        high: i64,
        low: i64,
        close: i64,
        volume: u64,
    },
    Gap {
        instrument: Instrument,
        provider: String,
        seq: Seq,
        source_ts: Timestamp,
        rx_ts: Timestamp,
        from_source_ts: Timestamp,
        to_source_ts: Timestamp,
    },
    SubscriptionChanged {
        instrument: Instrument,
        provider: String,
        kind: EventKind,
        active: bool,
        seq: Seq,
        source_ts: Timestamp,
        rx_ts: Timestamp,
    },
    SessionClosing {
        seq: Seq,
        source_ts: Timestamp,
        rx_ts: Timestamp,
    },
}

// Re-export `BarInterval` at the field position via its core path.
use datamancer_core::BarInterval;

/// Convert a logical event to its wire frame.
///
/// Returns `None` for connection-scoped controls suppressed on the event stream
/// (`ProviderConnected`/`ProviderDisconnected`/`ProviderError`) **and** for any
/// unknown future non-`Control` data variant (`MarketEvent` is `#[non_exhaustive]`);
/// the sink distinguishes the two.
#[must_use]
pub fn to_wire(ev: &MarketEvent) -> Option<EventFrame> {
    match ev {
        MarketEvent::Trade(t) => Some(EventFrame::Trade {
            instrument: t.instrument.clone(),
            seq: t.seq,
            source_ts: t.source_ts,
            rx_ts: t.rx_ts,
            price: t.price.0,
            size: t.size.raw(),
        }),
        MarketEvent::Quote(q) => Some(EventFrame::Quote {
            instrument: q.instrument.clone(),
            seq: q.seq,
            source_ts: q.source_ts,
            rx_ts: q.rx_ts,
            bid: q.bid.0,
            bid_size: q.bid_size.raw(),
            ask: q.ask.0,
            ask_size: q.ask_size.raw(),
        }),
        MarketEvent::Bar(b) => Some(EventFrame::Bar {
            instrument: b.instrument.clone(),
            interval: b.interval,
            seq: b.seq,
            source_ts: b.source_ts,
            rx_ts: b.rx_ts,
            open: b.open.0,
            high: b.high.0,
            low: b.low.0,
            close: b.close.0,
            volume: b.volume.raw(),
        }),
        MarketEvent::Control(c) => control_to_wire(c),
        _ => None,
    }
}

fn control_to_wire(c: &Control) -> Option<EventFrame> {
    match &c.kind {
        ControlKind::ProviderConnected { .. }
        | ControlKind::ProviderDisconnected { .. }
        | ControlKind::ProviderError { .. } => None,
        ControlKind::Gap {
            provider,
            instrument,
            span,
        } => Some(EventFrame::Gap {
            instrument: instrument.clone(),
            provider: provider.clone(),
            seq: c.seq,
            source_ts: c.source_ts,
            rx_ts: c.rx_ts,
            from_source_ts: span.from_source_ts,
            to_source_ts: span.to_source_ts,
        }),
        ControlKind::SubscriptionChanged {
            provider,
            instrument,
            kind,
            active,
        } => Some(EventFrame::SubscriptionChanged {
            instrument: instrument.clone(),
            provider: provider.clone(),
            kind: *kind,
            active: *active,
            seq: c.seq,
            source_ts: c.source_ts,
            rx_ts: c.rx_ts,
        }),
        ControlKind::SessionClosing => Some(EventFrame::SessionClosing {
            seq: c.seq,
            source_ts: c.source_ts,
            rx_ts: c.rx_ts,
        }),
    }
}

/// Reconstruct a logical event from a wire frame. `rx_ts` is carried verbatim.
#[must_use]
pub fn from_wire(f: &EventFrame) -> MarketEvent {
    match f {
        EventFrame::Trade {
            instrument,
            seq,
            source_ts,
            rx_ts,
            price,
            size,
        } => MarketEvent::Trade(Trade {
            instrument: instrument.clone(),
            source_ts: *source_ts,
            rx_ts: *rx_ts,
            seq: *seq,
            price: Price(*price),
            size: Quantity::from_raw(*size),
        }),
        EventFrame::Quote {
            instrument,
            seq,
            source_ts,
            rx_ts,
            bid,
            bid_size,
            ask,
            ask_size,
        } => MarketEvent::Quote(Quote {
            instrument: instrument.clone(),
            source_ts: *source_ts,
            rx_ts: *rx_ts,
            seq: *seq,
            bid: Price(*bid),
            bid_size: Quantity::from_raw(*bid_size),
            ask: Price(*ask),
            ask_size: Quantity::from_raw(*ask_size),
        }),
        EventFrame::Bar {
            instrument,
            interval,
            seq,
            source_ts,
            rx_ts,
            open,
            high,
            low,
            close,
            volume,
        } => MarketEvent::Bar(Bar {
            instrument: instrument.clone(),
            interval: *interval,
            source_ts: *source_ts,
            rx_ts: *rx_ts,
            seq: *seq,
            open: Price(*open),
            high: Price(*high),
            low: Price(*low),
            close: Price(*close),
            volume: Quantity::from_raw(*volume),
        }),
        frame @ (EventFrame::Gap { .. }
        | EventFrame::SubscriptionChanged { .. }
        | EventFrame::SessionClosing { .. }) => control_from_wire(frame),
    }
}

/// Reconstruct the `Control` variants of [`from_wire`]. Split out purely to
/// keep `from_wire` under clippy's `too_many_lines`; the two together form
/// one exhaustive frame-to-event mapping.
fn control_from_wire(frame: &EventFrame) -> MarketEvent {
    match frame {
        EventFrame::Gap {
            instrument,
            provider,
            seq,
            source_ts,
            rx_ts,
            from_source_ts,
            to_source_ts,
        } => MarketEvent::Control(Control {
            source_ts: *source_ts,
            rx_ts: *rx_ts,
            seq: *seq,
            kind: ControlKind::Gap {
                provider: provider.clone(),
                instrument: instrument.clone(),
                span: GapSpan {
                    from_source_ts: *from_source_ts,
                    to_source_ts: *to_source_ts,
                },
            },
        }),
        EventFrame::SubscriptionChanged {
            instrument,
            provider,
            kind,
            active,
            seq,
            source_ts,
            rx_ts,
        } => MarketEvent::Control(Control {
            source_ts: *source_ts,
            rx_ts: *rx_ts,
            seq: *seq,
            kind: ControlKind::SubscriptionChanged {
                provider: provider.clone(),
                instrument: instrument.clone(),
                kind: *kind,
                active: *active,
            },
        }),
        EventFrame::SessionClosing {
            seq,
            source_ts,
            rx_ts,
        } => MarketEvent::Control(Control {
            source_ts: *source_ts,
            rx_ts: *rx_ts,
            seq: *seq,
            kind: ControlKind::SessionClosing,
        }),
        EventFrame::Trade { .. } | EventFrame::Quote { .. } | EventFrame::Bar { .. } => {
            unreachable!("data variants are handled by from_wire before dispatch")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{EventFrame, from_wire, to_wire};
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

    fn round_trip(ev: &MarketEvent) -> MarketEvent {
        let frame = to_wire(ev).expect("encodable");
        let json = serde_json::to_string(&frame).expect("ser");
        let back: EventFrame = serde_json::from_str(&json).expect("de");
        from_wire(&back)
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
            // the JSON wire round trip intact.
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
    fn volumes_beyond_2_pow_53_round_trip_exactly() {
        // Raw fixed-point u64 values routinely exceed the IEEE-754 double
        // exact-integer range (a 10M-share daily bar volume is 1e16 raw).
        // serde_json must carry them exactly — a double-based JSON parser
        // cannot, which is why the README requires 64-bit integer decoding.
        let volume = Quantity::from_units(50_000_000); // 5e16 raw > 2^53
        assert!(volume.raw() > (1u64 << 53));
        let ev = MarketEvent::Bar(Bar {
            instrument: inst("AAPL"),
            interval: BarInterval::OneDay,
            source_ts: Timestamp(10),
            rx_ts: Timestamp(20),
            seq: Seq(5),
            open: Price(1),
            high: Price(4),
            low: Price(0),
            close: Price(3),
            volume,
        });
        assert_eq!(round_trip(&ev), ev);
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
    fn session_closing_round_trips_with_synthetic_seq() {
        let ev = MarketEvent::Control(Control {
            source_ts: Timestamp(1),
            rx_ts: Timestamp(2),
            seq: Seq::SYNTHETIC,
            kind: ControlKind::SessionClosing,
        });
        let frame = to_wire(&ev).expect("encodable");
        let json = serde_json::to_string(&frame).expect("ser");
        assert!(
            json.contains("18446744073709551615"),
            "SYNTHETIC seq verbatim"
        );
        assert_eq!(round_trip(&ev), ev);
    }

    #[test]
    fn rx_ts_carried_not_synthesized() {
        let ev = MarketEvent::Trade(Trade {
            instrument: inst("BTC/USD"),
            source_ts: Timestamp(111),
            rx_ts: Timestamp(999_999),
            seq: Seq(1),
            price: Price(1),
            size: Quantity::from_raw(1),
        });
        let back = round_trip(&ev);
        let MarketEvent::Trade(t) = back else {
            panic!("wrong variant")
        };
        assert_eq!(t.rx_ts, Timestamp(999_999));
        assert_ne!(t.rx_ts, t.source_ts);
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
            assert!(
                to_wire(&ev).is_none(),
                "connection-scoped control suppressed"
            );
        }
    }
}
