# WebSocket Client Interface Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a second client-transport worked example to datamancerd: a single bidirectional WebSocket connection carrying reused JSON control frames inbound and inline-instrument JSON event frames outbound, so a remote/cross-host, language-agnostic consumer can subscribe and read a client's multiplexed stream without linking iceoryx2.

**Architecture:** A new `datamancer-transport-ws` crate (peer of `datamancer-transport-iceoryx2`, `datamancer-core` + `tokio-tungstenite` only) owns the JSON wire format, a channel-backed `WsDataSink: EventSink`, and the single-writer socket task. `datamancerd` owns the listener, per-connection glue (accept → handshake+bearer-auth → split → open `dm.client_session()` → pump events into the sink → read control frames and drive the session → drain-teardown), and the `[ws]` config block. One WS connection = one client; `open-client` is implicit on connect.

**Tech Stack:** Rust (edition 2024), `tokio`, `tokio-tungstenite`, `futures`, `serde`/`serde_json`, `async-trait`, `axum` (already present, unchanged).

## Global Constraints

- `#![forbid(unsafe_code)]` in the new crate (matches all four existing crates).
- `[lints] workspace = true` in the new crate (`clippy::pedantic = deny`); build must pass `cargo clippy --all-targets -- -D warnings`.
- New crate depends on **`datamancer-core` only** among workspace crates — never on `datamancer` or `datamancerd`.
- New crate package: `version = "0.1.0"`, `edition = "2024"`, `license = "MIT OR Apache-2.0"`.
- Wire frames are JSON **text** frames. Prices cross the wire as raw `i64` (core `Price` does **not** derive `Serialize`); `Instrument`, `Seq`, `Timestamp`, `EventKind`, `BarInterval` embed directly (they do derive serde).
- Timestamp triple (`source_ts`, `seq`, `rx_ts`) preserved end-to-end; `rx_ts` never synthesized on decode. `Seq::SYNTHETIC` (`u64::MAX`) survives round-trip verbatim.
- Control routing mirrors iceoryx2: connection-scoped controls (`ProviderConnected`/`ProviderDisconnected`/`ProviderError`) are suppressed from the event stream (`to_wire` → `None`); per-symbol `Gap`/`SubscriptionChanged` and `SessionClosing` are carried.
- Feature gating: `datamancer` gets a `transport-ws` feature (off by default); `datamancerd` gets a `ws` feature (off by default).
- Reuse the existing stable error `codes` table (`crate::control::codes`) and `error_code()` for WS control errors — do not mint a parallel table.

---

### Task 1: Scaffold `datamancer-transport-ws` crate + JSON wire format

**Files:**
- Create: `crates/datamancer-transport-ws/Cargo.toml`
- Create: `crates/datamancer-transport-ws/src/lib.rs`
- Create: `crates/datamancer-transport-ws/src/error.rs`
- Create: `crates/datamancer-transport-ws/src/wire.rs`
- Modify: `Cargo.toml` (workspace `members`)

**Interfaces:**
- Produces: `EventFrame` (serde tagged enum), `pub fn to_wire(ev: &MarketEvent) -> Option<EventFrame>`, `pub fn from_wire(f: &EventFrame) -> MarketEvent`, `WsTransportError`, `pub type Result<T, E = WsTransportError>`.

- [ ] **Step 1: Add the crate to the workspace members**

Modify `Cargo.toml` (root) `members` list to include the new crate:

```toml
[workspace]
members = [
    "crates/datamancer",
    "crates/datamancer-core",
    "crates/datamancer-transport-iceoryx2",
    "crates/datamancer-transport-ws",
    "crates/datamancerd",
]
resolver = "3"
```

- [ ] **Step 2: Create the crate manifest**

Create `crates/datamancer-transport-ws/Cargo.toml`:

```toml
[package]
name = "datamancer-transport-ws"
version = "0.1.0"
edition = "2024"
license = "MIT OR Apache-2.0"
description = "WebSocket client transport for datamancer (single bidirectional connection: JSON control + event frames)"

[dependencies]
datamancer-core = { path = "../datamancer-core" }
async-trait = "0.1"
futures = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
tokio = { workspace = true }

[dev-dependencies]
tokio = { workspace = true }

[lints]
workspace = true
```

Then add `tokio-tungstenite` with a workspace-compatible version:

Run: `cargo add -p datamancer-transport-ws tokio-tungstenite`
Expected: adds a `tokio-tungstenite = "<version>"` line; `cargo metadata` resolves.

- [ ] **Step 3: Create `error.rs`**

Create `crates/datamancer-transport-ws/src/error.rs`:

```rust
//! Transport-crate error type. Mirrors the iceoryx2 crate: funnel wire/socket
//! failures into one stringly-typed error that converts into the core
//! `datamancer_core::Error` via `std::io::Error` so no core change is needed.

/// An error originating in the WebSocket transport.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WsTransportError {
    /// Serializing an event frame to JSON failed.
    Encode(String),
    /// The outbound channel is closed (writer task gone / connection dropped).
    Closed,
    /// A `MarketEvent` variant this transport build cannot encode reached the
    /// sink (core gained a data variant newer than this transport).
    Unsupported(String),
}

impl std::fmt::Display for WsTransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Encode(m) => write!(f, "ws frame encode error: {m}"),
            Self::Closed => f.write_str("ws outbound channel closed"),
            Self::Unsupported(m) => write!(f, "unsupported event for ws transport: {m}"),
        }
    }
}

impl std::error::Error for WsTransportError {}

impl From<WsTransportError> for datamancer_core::Error {
    fn from(e: WsTransportError) -> Self {
        datamancer_core::Error::Io(std::io::Error::other(e.to_string()))
    }
}

/// Convenience alias for transport-crate results.
pub type Result<T, E = WsTransportError> = std::result::Result<T, E>;
```

- [ ] **Step 4: Write the failing wire round-trip tests**

Create `crates/datamancer-transport-ws/src/wire.rs` with only the tests first (types come next step):

```rust
#[cfg(test)]
mod tests {
    use super::{EventFrame, from_wire, to_wire};
    use datamancer_core::{
        AssetClass, Bar, BarInterval, Control, ControlKind, EventKind, GapSpan, Instrument,
        MarketEvent, Price, ProviderId, Quote, Seq, Timestamp, Trade,
    };

    fn inst(symbol: &str) -> Instrument {
        Instrument::new(ProviderId::from_static("alpaca"), AssetClass::Crypto, symbol)
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
            size: 99,
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
            bid_size: 10,
            ask: Price(200),
            ask_size: 20,
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
                volume: 1000,
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
                span: GapSpan { from_source_ts: Timestamp(100), to_source_ts: Timestamp(200) },
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
        assert!(json.contains("18446744073709551615"), "SYNTHETIC seq verbatim");
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
            size: 1,
        });
        let back = round_trip(&ev);
        let MarketEvent::Trade(t) = back else { panic!("wrong variant") };
        assert_eq!(t.rx_ts, Timestamp(999_999));
        assert_ne!(t.rx_ts, t.source_ts);
    }

    #[test]
    fn connection_scoped_controls_are_suppressed() {
        for kind in [
            ControlKind::ProviderConnected { provider: "alpaca".to_string() },
            ControlKind::ProviderDisconnected { provider: "alpaca".to_string(), reason: "boom".to_string() },
            ControlKind::ProviderError { provider: "alpaca".to_string(), message: "oops".to_string() },
        ] {
            let ev = MarketEvent::Control(Control {
                source_ts: Timestamp(1),
                rx_ts: Timestamp(2),
                seq: Seq(3),
                kind,
            });
            assert!(to_wire(&ev).is_none(), "connection-scoped control suppressed");
        }
    }
}
```

- [ ] **Step 5: Run the tests to confirm they fail to compile**

Run: `cargo test -p datamancer-transport-ws --lib wire`
Expected: FAIL — `cannot find function to_wire` / `EventFrame not found`.

- [ ] **Step 6: Implement the wire types + mapping**

Prepend to `crates/datamancer-transport-ws/src/wire.rs` (above the `#[cfg(test)] mod tests`):

```rust
//! The JSON event frame and its logical <-> wire conversions.
//!
//! Unlike the iceoryx2 POD, the instrument is carried **inline** on every frame
//! (JSON is self-describing; no `SymbolId` interning / announcement race).
//! Prices cross as raw `i64` because core `Price` does not derive `Serialize`.
//! Control kinds are flattened into top-level `type` tags. Connection-scoped
//! controls are suppressed (`to_wire` returns `None`), matching the iceoryx2
//! routing rule; a remote client reads connectivity from the `snapshot` reply.

use datamancer_core::{
    Bar, Control, ControlKind, EventKind, GapSpan, Instrument, MarketEvent, Price, Quote, Seq,
    Timestamp, Trade,
};
use serde::{Deserialize, Serialize};

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
            size: t.size,
        }),
        MarketEvent::Quote(q) => Some(EventFrame::Quote {
            instrument: q.instrument.clone(),
            seq: q.seq,
            source_ts: q.source_ts,
            rx_ts: q.rx_ts,
            bid: q.bid.0,
            bid_size: q.bid_size,
            ask: q.ask.0,
            ask_size: q.ask_size,
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
            volume: b.volume,
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
        ControlKind::Gap { provider, instrument, span } => Some(EventFrame::Gap {
            instrument: instrument.clone(),
            provider: provider.clone(),
            seq: c.seq,
            source_ts: c.source_ts,
            rx_ts: c.rx_ts,
            from_source_ts: span.from_source_ts,
            to_source_ts: span.to_source_ts,
        }),
        ControlKind::SubscriptionChanged { provider, instrument, kind, active } => {
            Some(EventFrame::SubscriptionChanged {
                instrument: instrument.clone(),
                provider: provider.clone(),
                kind: *kind,
                active: *active,
                seq: c.seq,
                source_ts: c.source_ts,
                rx_ts: c.rx_ts,
            })
        }
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
        EventFrame::Trade { instrument, seq, source_ts, rx_ts, price, size } => {
            MarketEvent::Trade(Trade {
                instrument: instrument.clone(),
                source_ts: *source_ts,
                rx_ts: *rx_ts,
                seq: *seq,
                price: Price(*price),
                size: *size,
            })
        }
        EventFrame::Quote { instrument, seq, source_ts, rx_ts, bid, bid_size, ask, ask_size } => {
            MarketEvent::Quote(Quote {
                instrument: instrument.clone(),
                source_ts: *source_ts,
                rx_ts: *rx_ts,
                seq: *seq,
                bid: Price(*bid),
                bid_size: *bid_size,
                ask: Price(*ask),
                ask_size: *ask_size,
            })
        }
        EventFrame::Bar {
            instrument, interval, seq, source_ts, rx_ts, open, high, low, close, volume,
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
            volume: *volume,
        }),
        EventFrame::Gap { instrument, provider, seq, source_ts, rx_ts, from_source_ts, to_source_ts } => {
            MarketEvent::Control(Control {
                source_ts: *source_ts,
                rx_ts: *rx_ts,
                seq: *seq,
                kind: ControlKind::Gap {
                    provider: provider.clone(),
                    instrument: instrument.clone(),
                    span: GapSpan { from_source_ts: *from_source_ts, to_source_ts: *to_source_ts },
                },
            })
        }
        EventFrame::SubscriptionChanged { instrument, provider, kind, active, seq, source_ts, rx_ts } => {
            MarketEvent::Control(Control {
                source_ts: *source_ts,
                rx_ts: *rx_ts,
                seq: *seq,
                kind: ControlKind::SubscriptionChanged {
                    provider: provider.clone(),
                    instrument: instrument.clone(),
                    kind: *kind,
                    active: *active,
                },
            })
        }
        EventFrame::SessionClosing { seq, source_ts, rx_ts } => MarketEvent::Control(Control {
            source_ts: *source_ts,
            rx_ts: *rx_ts,
            seq: *seq,
            kind: ControlKind::SessionClosing,
        }),
    }
}
```

- [ ] **Step 7: Create `lib.rs` exporting the surface**

Create `crates/datamancer-transport-ws/src/lib.rs`:

```rust
//! WebSocket client transport for datamancer.
//!
//! One bidirectional WebSocket connection is one client: inbound JSON control
//! frames drive the client's `ClientSession`; the client's multiplexed
//! `EventStream` is serialized outbound as [`wire::EventFrame`]s. The instrument
//! is carried inline on every event frame (no interning). This crate owns the
//! wire format, the channel-backed [`WsDataSink`], and the single-writer socket
//! task; `datamancerd` owns the listener and the per-connection glue that
//! touches the orchestrator.
#![forbid(unsafe_code)]

mod error;
mod sink;
mod wire;
mod writer;

pub use error::{Result, WsTransportError};
pub use sink::WsDataSink;
pub use wire::{EventFrame, from_wire, to_wire};
pub use writer::run_writer;
```

Note: `sink` and `writer` modules are created in Task 2. To keep this task's build green, temporarily comment out the `mod sink;`, `mod writer;`, `pub use sink::...`, and `pub use writer::...` lines, or create empty stubs. Simplest: create `src/sink.rs` and `src/writer.rs` as empty files now and fill them in Task 2. Create both empty files:

Run: `: > crates/datamancer-transport-ws/src/sink.rs && : > crates/datamancer-transport-ws/src/writer.rs`
Then temporarily remove the `pub use sink::WsDataSink;` and `pub use writer::run_writer;` lines until Task 2 (leave `mod sink;`/`mod writer;` pointing at the empty files — empty modules compile).

- [ ] **Step 8: Run the tests to verify they pass**

Run: `cargo test -p datamancer-transport-ws --lib`
Expected: PASS (8 wire tests green).

- [ ] **Step 9: Lint**

Run: `cargo clippy -p datamancer-transport-ws --all-targets -- -D warnings`
Expected: no warnings.

- [ ] **Step 10: Commit**

```bash
git add Cargo.toml crates/datamancer-transport-ws/
git commit -m "feat(transport-ws): scaffold crate + JSON event wire format"
```

---

### Task 2: `WsDataSink` (EventSink) + single-writer task

**Files:**
- Modify: `crates/datamancer-transport-ws/src/sink.rs`
- Modify: `crates/datamancer-transport-ws/src/writer.rs`
- Modify: `crates/datamancer-transport-ws/src/lib.rs` (re-enable exports)

**Interfaces:**
- Consumes: `to_wire` (Task 1), `WsTransportError` (Task 1).
- Produces:
  - `WsDataSink::new(tx: tokio::sync::mpsc::Sender<String>) -> WsDataSink`, `impl EventSink for WsDataSink`.
  - `pub async fn run_writer<S>(rx: tokio::sync::mpsc::Receiver<String>, write: S) where S: futures::Sink<tokio_tungstenite::tungstenite::Message> + Unpin` — drains the channel, wrapping each string in `Message::Text`, until the channel closes or a send fails.

- [ ] **Step 1: Write the failing sink tests**

Replace the contents of `crates/datamancer-transport-ws/src/sink.rs` with tests first:

```rust
#[cfg(test)]
mod tests {
    use super::WsDataSink;
    use datamancer_core::{
        AssetClass, Control, ControlKind, EventSink, Instrument, MarketEvent, Price, ProviderId,
        PublishOutcome, Seq, Timestamp, Trade,
    };

    fn trade() -> MarketEvent {
        MarketEvent::Trade(Trade {
            instrument: Instrument::new(ProviderId::from_static("alpaca"), AssetClass::Crypto, "BTC/USD"),
            source_ts: Timestamp(1),
            rx_ts: Timestamp(2),
            seq: Seq(1),
            price: Price(42),
            size: 7,
        })
    }

    #[tokio::test]
    async fn publish_delivers_json_into_channel() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(4);
        let sink = WsDataSink::new(tx);
        assert!(matches!(sink.publish(trade()).await, PublishOutcome::Delivered));
        let line = rx.recv().await.expect("frame");
        assert!(line.contains("\"type\":\"trade\""));
        assert!(line.contains("\"price\":42"));
    }

    #[tokio::test]
    async fn full_channel_rejects_and_hands_event_back() {
        // Capacity 1, no reader draining: second publish finds the channel full.
        let (tx, _rx) = tokio::sync::mpsc::channel::<String>(1);
        let sink = WsDataSink::new(tx);
        assert!(matches!(sink.publish(trade()).await, PublishOutcome::Delivered));
        match sink.publish(trade()).await {
            PublishOutcome::Rejected(MarketEvent::Trade(t)) => assert_eq!(t.price, Price(42)),
            other => panic!("expected Rejected, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn connection_scoped_control_is_suppressed_but_delivered() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(4);
        let sink = WsDataSink::new(tx);
        let ev = MarketEvent::Control(Control {
            source_ts: Timestamp(1),
            rx_ts: Timestamp(2),
            seq: Seq(3),
            kind: ControlKind::ProviderConnected { provider: "alpaca".to_string() },
        });
        // Suppressed frames are acked as Delivered but put nothing on the wire.
        assert!(matches!(sink.publish(ev).await, PublishOutcome::Delivered));
        assert!(rx.try_recv().is_err(), "no frame emitted for suppressed control");
    }
}
```

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test -p datamancer-transport-ws --lib sink`
Expected: FAIL — `WsDataSink` not found.

- [ ] **Step 3: Implement `WsDataSink`**

Prepend to `crates/datamancer-transport-ws/src/sink.rs` (above the tests):

```rust
//! The channel-backed WS data-plane sink (`impl EventSink`).
//!
//! `publish` serializes the event to a JSON frame and `try_send`s it into a
//! bounded channel drained by [`run_writer`](crate::run_writer). A **full**
//! channel (a remote consumer too slow to keep up) yields
//! `PublishOutcome::Rejected`, handing the event back — the pump then stops and
//! the connection is torn down. Delivery is therefore lossy-on-overrun by
//! disconnection, never by silent drop.

use async_trait::async_trait;
use datamancer_core::{EventSink, MarketEvent, PublishOutcome, Result as CoreResult};
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::error::TrySendError;

use crate::wire::to_wire;

/// Per-connection WebSocket data-plane sink. Serializes events to JSON frames
/// and enqueues them for the connection's single writer task.
pub struct WsDataSink {
    tx: Sender<String>,
}

impl WsDataSink {
    /// Build a sink over the outbound frame channel.
    #[must_use]
    pub fn new(tx: Sender<String>) -> Self {
        Self { tx }
    }
}

#[async_trait]
impl EventSink for WsDataSink {
    async fn publish(&self, ev: MarketEvent) -> PublishOutcome {
        let Some(frame) = to_wire(&ev) else {
            // `to_wire` returns `None` for intentionally-suppressed
            // connection-scoped controls (legitimately "delivered") and for
            // unknown future non-`Control` data variants (must be surfaced).
            if matches!(ev, MarketEvent::Control(_)) {
                return PublishOutcome::Delivered;
            }
            return PublishOutcome::Rejected(ev);
        };
        let json = match serde_json::to_string(&frame) {
            Ok(json) => json,
            Err(_) => return PublishOutcome::Rejected(ev),
        };
        match self.tx.try_send(json) {
            Ok(()) => PublishOutcome::Delivered,
            Err(TrySendError::Full(_) | TrySendError::Closed(_)) => PublishOutcome::Rejected(ev),
        }
    }

    async fn flush(&self) -> CoreResult<()> {
        // No application-side buffer beyond the channel; the writer task drains
        // it. Nothing to force here.
        Ok(())
    }
}
```

- [ ] **Step 4: Write the failing writer test**

Replace the contents of `crates/datamancer-transport-ws/src/writer.rs` with tests first:

```rust
#[cfg(test)]
mod tests {
    use super::run_writer;
    use futures::StreamExt as _;
    use tokio_tungstenite::tungstenite::Message;

    #[tokio::test]
    async fn writer_wraps_strings_as_text_and_stops_on_close() {
        // A futures mpsc as the "sink" side; collect what the writer sends.
        let (sink_tx, sink_rx) = futures::channel::mpsc::unbounded::<Message>();
        let (tx, rx) = tokio::sync::mpsc::channel::<String>(4);

        tx.send("hello".to_string()).await.unwrap();
        tx.send("world".to_string()).await.unwrap();
        drop(tx); // closes the channel -> writer returns

        run_writer(rx, sink_tx).await;

        let got: Vec<Message> = sink_rx.collect().await;
        assert_eq!(got, vec![
            Message::Text("hello".to_string().into()),
            Message::Text("world".to_string().into()),
        ]);
    }
}
```

Note: `Message::Text` in the pinned `tokio-tungstenite` may take a `Utf8Bytes` (`.into()` from `String`) or a `String` directly depending on version. If `Message::Text("hello".to_string().into())` fails to compile, use `Message::text("hello")` (the constructor) in both the assertion and the implementation.

- [ ] **Step 5: Run to confirm failure**

Run: `cargo test -p datamancer-transport-ws --lib writer`
Expected: FAIL — `run_writer` not found.

- [ ] **Step 6: Implement `run_writer`**

Prepend to `crates/datamancer-transport-ws/src/writer.rs` (above the tests):

```rust
//! The single-writer socket task: drains the outbound frame channel and writes
//! each frame as a WebSocket text message. One writer per connection means
//! event frames and control replies (both enqueued as strings) never interleave
//! mid-frame and their order is deterministic.

use futures::{Sink, SinkExt as _};
use tokio::sync::mpsc::Receiver;
use tokio_tungstenite::tungstenite::Message;

/// Drain `rx`, sending each string as `Message::Text` on `write`, until the
/// channel closes (all senders dropped) or a socket send fails. Generic over the
/// sink so it is unit-testable without a real socket.
pub async fn run_writer<S>(mut rx: Receiver<String>, mut write: S)
where
    S: Sink<Message> + Unpin,
{
    while let Some(text) = rx.recv().await {
        if write.send(Message::Text(text.into())).await.is_err() {
            break;
        }
    }
    let _ = write.close().await;
}
```

- [ ] **Step 7: Re-enable the `lib.rs` exports**

In `crates/datamancer-transport-ws/src/lib.rs`, restore the two export lines removed in Task 1 Step 7:

```rust
pub use sink::WsDataSink;
pub use writer::run_writer;
```

- [ ] **Step 8: Run all crate tests**

Run: `cargo test -p datamancer-transport-ws`
Expected: PASS (wire + sink + writer tests).

- [ ] **Step 9: Lint**

Run: `cargo clippy -p datamancer-transport-ws --all-targets -- -D warnings`
Expected: no warnings.

- [ ] **Step 10: Commit**

```bash
git add crates/datamancer-transport-ws/
git commit -m "feat(transport-ws): channel-backed WsDataSink + single-writer task"
```

---

### Task 3: `datamancer` re-export behind `transport-ws` feature

**Files:**
- Modify: `crates/datamancer/Cargo.toml`
- Modify: `crates/datamancer/src/lib.rs`

**Interfaces:**
- Produces: `datamancer::transport_ws` (re-export of the crate) when the `transport-ws` feature is on.

- [ ] **Step 1: Add the optional dependency + feature**

In `crates/datamancer/Cargo.toml`, add under `[features]` (next to `transport-iceoryx2`):

```toml
transport-ws = ["dep:datamancer-transport-ws"]
```

And under `[dependencies]` (next to the iceoryx2 transport dep), add:

```toml
datamancer-transport-ws = { path = "../datamancer-transport-ws", optional = true }
```

- [ ] **Step 2: Add the re-export**

Find the existing `transport-iceoryx2` re-export in `crates/datamancer/src/lib.rs`:

Run: `grep -n "transport" crates/datamancer/src/lib.rs`
Expected: shows a `#[cfg(feature = "transport-iceoryx2")] pub use datamancer_transport_iceoryx2 as transport;` line (or similar `pub mod transport`).

Add immediately below it, mirroring that exact style:

```rust
#[cfg(feature = "transport-ws")]
pub use datamancer_transport_ws as transport_ws;
```

- [ ] **Step 3: Verify it builds with the feature**

Run: `cargo build -p datamancer --features transport-ws`
Expected: builds; `datamancer::transport_ws::WsDataSink` is reachable.

- [ ] **Step 4: Verify default build is unaffected**

Run: `cargo build -p datamancer`
Expected: builds; no `datamancer-transport-ws` compiled (off by default).

- [ ] **Step 5: Commit**

```bash
git add crates/datamancer/Cargo.toml crates/datamancer/src/lib.rs
git commit -m "feat(datamancer): re-export transport-ws behind feature (off by default)"
```

---

### Task 4: `datamancerd` `[ws]` config block

**Files:**
- Modify: `crates/datamancerd/src/config.rs`

**Interfaces:**
- Produces: `pub struct WsConfig { pub enabled: bool, pub bind: String, pub port: u16, pub auth_token: Option<String>, pub channel_depth: usize, pub keepalive_secs: u64 }`; `Config` gains `pub ws: Option<WsConfig>`.

- [ ] **Step 1: Write the failing config tests**

Add to the `#[cfg(test)] mod tests` in `crates/datamancerd/src/config.rs`:

```rust
#[test]
fn ws_config_parses_with_defaults() {
    let cfg = Config::parse(
        "[provider.alpaca]\naccount_type = \"paper\"\n\n[ws]\nenabled = true\nport = 9001\n",
    )
    .expect("parse");
    let ws = cfg.ws.expect("ws present");
    assert!(ws.enabled);
    assert_eq!(ws.bind, "127.0.0.1");
    assert_eq!(ws.port, 9001);
    assert_eq!(ws.auth_token, None);
    assert_eq!(ws.channel_depth, 1024);
    assert_eq!(ws.keepalive_secs, 30);
}

#[test]
fn ws_config_absent_is_none() {
    let cfg = Config::parse("[provider.alpaca]\naccount_type = \"paper\"\n").expect("parse");
    assert!(cfg.ws.is_none());
}

#[test]
fn ws_config_rejects_unknown_field() {
    let err = Config::parse(
        "[provider.alpaca]\naccount_type = \"paper\"\n\n[ws]\nenabled = true\nport = 9001\nbogus = 1\n",
    );
    assert!(err.is_err(), "unknown [ws] field must be rejected");
}
```

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test -p datamancerd --lib config::tests::ws_config`
Expected: FAIL — `cfg.ws` field does not exist.

- [ ] **Step 3: Add the `ws` field to `Config`**

In the `pub struct Config { ... }` definition (around `crates/datamancerd/src/config.rs:32`), add next to the `web_ui` field:

```rust
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ws: Option<WsConfig>,
```

- [ ] **Step 4: Define `WsConfig`**

Add near `WebUiConfig` (around `crates/datamancerd/src/config.rs:280`):

```rust
/// The remote WebSocket client surface. Mutating and network-reachable — its own
/// posture, separate from the loopback read-only web UI. Off unless `enabled`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WsConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_ws_bind")]
    pub bind: String,
    #[serde(default = "default_ws_port")]
    pub port: u16,
    /// Optional shared bearer token checked at the WS handshake. When unset the
    /// daemon logs a prominent warning (louder off-loopback).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_token: Option<String>,
    #[serde(default = "default_ws_channel_depth")]
    pub channel_depth: usize,
    #[serde(default = "default_ws_keepalive")]
    pub keepalive_secs: u64,
}

fn default_ws_bind() -> String {
    "127.0.0.1".to_string()
}

const fn default_ws_port() -> u16 {
    9001
}

const fn default_ws_channel_depth() -> usize {
    1024
}

const fn default_ws_keepalive() -> u64 {
    30
}
```

- [ ] **Step 5: Run to verify pass**

Run: `cargo test -p datamancerd --lib config::tests::ws_config`
Expected: PASS.

- [ ] **Step 6: Lint + commit**

Run: `cargo clippy -p datamancerd --all-targets -- -D warnings`
Expected: no warnings.

```bash
git add crates/datamancerd/src/config.rs
git commit -m "feat(datamancerd): [ws] config block (off by default)"
```

---

### Task 5: `datamancerd` WS control frame types

**Files:**
- Create: `crates/datamancerd/src/ws/mod.rs`
- Create: `crates/datamancerd/src/ws/protocol.rs`
- Modify: `crates/datamancerd/src/main.rs` (or wherever modules are declared — add `mod ws;`)

**Interfaces:**
- Consumes: `crate::control::{SubscriptionSpec, codes, error_code}` and `crate::config::{AssetClassCfg, EventKindCfg}`.
- Produces:
  - `WsRequest` (serde, `#[serde(tag = "op", rename_all = "kebab-case")]`) variants `Subscribe { id, #[serde(flatten)] spec: SubscriptionSpec }`, `Unsubscribe { id, provider, asset_class, symbol, kind }`, `Snapshot { id }`, `CloseClient { id }`.
  - `WsReply { id: u64, ok: bool, code: Option<String>, message: Option<String>, snapshot: Option<SystemSnapshot> }` with constructors `WsReply::ok(id)`, `WsReply::error(id, code, message)`, `WsReply::snapshot(id, snapshot)`, and `WsReply::from_library_error(id, &datamancer::Error)`.

- [ ] **Step 1: Define a minimal `ws` feature + declare the module**

The protocol types are pure serde (no `tokio-tungstenite`), so gate them behind an
**empty** `ws` feature now; Task 6 extends this same feature with the socket deps.

In `crates/datamancerd/Cargo.toml` under `[features]`, add:

```toml
# The remote WebSocket client surface. Extended with socket deps in the listener task.
ws = []
```

Then add `mod ws;` to the module list where `mod control;`, `mod server;` etc. are declared.

Run: `grep -rn "^mod \|^pub mod " crates/datamancerd/src/main.rs`
Expected: shows the module declarations; add `#[cfg(feature = "ws")] mod ws;` alongside them.

- [ ] **Step 2: Write the failing protocol tests**

Create `crates/datamancerd/src/ws/protocol.rs` with tests first:

```rust
#[cfg(test)]
mod tests {
    use super::{WsReply, WsRequest};
    use crate::config::EventKindCfg;

    #[test]
    fn ws_subscribe_parses_with_id_and_shared_spec() {
        let line = r#"{"id":7,"op":"subscribe","provider":"alpaca-crypto","asset_class":"crypto","symbol":"BTC/USD","kind":"trade"}"#;
        let req: WsRequest = serde_json::from_str(line).expect("de");
        match req {
            WsRequest::Subscribe { id, spec } => {
                assert_eq!(id, 7);
                assert_eq!(spec.symbol, "BTC/USD");
                assert_eq!(spec.kind, EventKindCfg::Trade);
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn ws_snapshot_and_close_and_unsubscribe_parse() {
        assert!(matches!(
            serde_json::from_str::<WsRequest>(r#"{"id":1,"op":"snapshot"}"#).unwrap(),
            WsRequest::Snapshot { id: 1 }
        ));
        assert!(matches!(
            serde_json::from_str::<WsRequest>(r#"{"id":2,"op":"close-client"}"#).unwrap(),
            WsRequest::CloseClient { id: 2 }
        ));
        let u = serde_json::from_str::<WsRequest>(
            r#"{"id":3,"op":"unsubscribe","provider":"alpaca-crypto","asset_class":"crypto","symbol":"BTC/USD","kind":"trade"}"#,
        )
        .unwrap();
        assert!(matches!(u, WsRequest::Unsubscribe { id: 3, .. }));
    }

    #[test]
    fn ws_reply_serialization_omits_empty_fields_and_carries_id() {
        let ok = serde_json::to_value(WsReply::ok(5)).unwrap();
        assert_eq!(ok["id"], 5);
        assert_eq!(ok["ok"], serde_json::Value::Bool(true));
        assert!(ok.get("code").is_none());
        assert!(ok.get("snapshot").is_none());

        let err = serde_json::to_value(WsReply::error(6, "bad_request", "nope")).unwrap();
        assert_eq!(err["id"], 6);
        assert_eq!(err["ok"], serde_json::Value::Bool(false));
        assert_eq!(err["code"], "bad_request");
    }

    #[test]
    fn ws_control_vocabulary_shares_subscription_spec_with_uds() {
        // The same subscribe spec body parses under both control surfaces,
        // guarding the "one control vocabulary" claim.
        let spec_json = r#"{"provider":"alpaca-crypto","asset_class":"crypto","symbol":"BTC/USD","kind":"trade"}"#;
        let uds: crate::control::SubscriptionSpec = serde_json::from_str(spec_json).unwrap();
        let ws_line = format!(r#"{{"id":1,"op":"subscribe",{}}}"#, &spec_json[1..spec_json.len() - 1]);
        let ws: WsRequest = serde_json::from_str(&ws_line).unwrap();
        match ws {
            WsRequest::Subscribe { spec, .. } => assert_eq!(spec, uds),
            other => panic!("wrong variant: {other:?}"),
        }
    }
}
```

- [ ] **Step 3: Run to confirm failure**

Run: `cargo test -p datamancerd --features ws --lib ws::protocol`
Expected: FAIL — types not found. (The empty `ws` feature was defined in Step 1; Task 6 extends it with socket deps.)

- [ ] **Step 4: Implement the protocol types**

Prepend to `crates/datamancerd/src/ws/protocol.rs`:

```rust
//! The WebSocket control protocol: JSON frames over the one WS connection.
//!
//! Reuses the UDS control vocabulary — [`SubscriptionSpec`](crate::control::SubscriptionSpec)
//! and the stable [`codes`](crate::control::codes) table — but drops the
//! per-request `client` field (the connection identifies the client) and adds a
//! correlation `id` echoed on the reply, because event frames interleave with
//! replies on the shared socket. `open-client` is implicit on connect and has no
//! request.

use datamancer::SystemSnapshot;
use serde::{Deserialize, Serialize};

use crate::config::{AssetClassCfg, EventKindCfg};
use crate::control::{SubscriptionSpec, error_code};

/// A WS control request (one JSON text frame).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "kebab-case")]
pub enum WsRequest {
    /// Add a subscription to this connection's client.
    Subscribe {
        id: u64,
        #[serde(flatten)]
        spec: SubscriptionSpec,
    },
    /// Remove a subscription.
    Unsubscribe {
        id: u64,
        provider: String,
        asset_class: AssetClassCfg,
        symbol: String,
        kind: EventKindCfg,
    },
    /// Return the current diagnostics snapshot.
    Snapshot { id: u64 },
    /// Gracefully close this connection's client.
    CloseClient { id: u64 },
}

impl WsRequest {
    /// The correlation id carried by every request.
    #[must_use]
    pub fn id(&self) -> u64 {
        match self {
            Self::Subscribe { id, .. }
            | Self::Unsubscribe { id, .. }
            | Self::Snapshot { id }
            | Self::CloseClient { id } => *id,
        }
    }
}

/// A WS control reply (one JSON text frame), echoing the request `id`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WsReply {
    pub id: u64,
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<SystemSnapshot>,
}

impl WsReply {
    #[must_use]
    pub fn ok(id: u64) -> Self {
        Self { id, ok: true, code: None, message: None, snapshot: None }
    }

    #[must_use]
    pub fn error(id: u64, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self { id, ok: false, code: Some(code.into()), message: Some(message.into()), snapshot: None }
    }

    #[must_use]
    pub fn snapshot(id: u64, snapshot: SystemSnapshot) -> Self {
        Self { id, ok: true, code: None, message: None, snapshot: Some(snapshot) }
    }

    #[must_use]
    pub fn from_library_error(id: u64, err: &datamancer::Error) -> Self {
        Self::error(id, error_code(err), err.to_string())
    }
}
```

- [ ] **Step 5: Create `ws/mod.rs` exposing the protocol module**

Create `crates/datamancerd/src/ws/mod.rs`:

```rust
//! The remote WebSocket client surface (single bidirectional connection = one
//! client). Owns the listener, per-connection bridge, and the WS control
//! protocol. The event wire format + sink + writer live in the
//! `datamancer-transport-ws` crate; this module owns the part that touches the
//! orchestrator (`ClientSession`).

mod protocol;

pub use protocol::{WsReply, WsRequest};
```

- [ ] **Step 6: Run to verify pass**

Run: `cargo test -p datamancerd --features ws --lib ws::protocol`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/datamancerd/src/ws/ crates/datamancerd/src/main.rs crates/datamancerd/Cargo.toml
git commit -m "feat(datamancerd): WS control frame protocol (WsRequest/WsReply, id echo)"
```

---

### Task 6: `datamancerd` WS listener + per-connection bridge + bearer auth + server wiring

**Files:**
- Modify: `crates/datamancerd/Cargo.toml` (add `ws` feature + deps)
- Create: `crates/datamancerd/src/ws/conn.rs`
- Create: `crates/datamancerd/src/ws/listener.rs`
- Modify: `crates/datamancerd/src/ws/mod.rs` (wire submodules + public `serve` entry)
- Modify: `crates/datamancerd/src/server.rs` (spawn/shutdown the WS listener)
- Create: `crates/datamancerd/tests/ws_e2e.rs`

**Interfaces:**
- Consumes: `datamancer::transport_ws::{WsDataSink, run_writer}`, `datamancer::{Datamancer, ClientSession, Scope, EventStream}`, `crate::ws::{WsRequest, WsReply}`, `crate::config::WsConfig`, `crate::control::codes`.
- Produces:
  - `pub async fn serve(dm: Datamancer, cfg: WsConfig, shutdown: impl Future<Output = ()> + Send + 'static) -> std::io::Result<()>` in `ws/listener.rs`.
  - `async fn handle_connection(tcp: TcpStream, peer: SocketAddr, dm: Datamancer, auth_token: Option<Arc<String>>, channel_depth: usize)` in `ws/conn.rs`.

- [ ] **Step 1: Add the `ws` feature + dependencies**

In `crates/datamancerd/Cargo.toml` under `[features]`, **replace** the empty `ws = []`
line from Task 5 with the dep-carrying form:

```toml
# The remote WebSocket client surface (second transport worked example).
ws = ["dep:tokio-tungstenite", "datamancer/transport-ws"]
```

Then add the dependency (run so the version matches the transport crate):

Run: `cargo add -p datamancerd tokio-tungstenite --optional`
Expected: adds `tokio-tungstenite = { version = "<version>", optional = true }`.

Confirm `futures` is available to datamancerd (it is used by `server.rs`); if not present as a direct dep, add it:

Run: `grep -n "^futures" crates/datamancerd/Cargo.toml || cargo add -p datamancerd futures`
Expected: `futures` is a dependency.

- [ ] **Step 2: Implement the per-connection bridge**

Create `crates/datamancerd/src/ws/conn.rs`:

```rust
//! One accepted WebSocket connection = one client. Mirrors the UDS-control +
//! iceoryx2-sink pairing in `server.rs`, but over a single socket: inbound
//! control frames drive this connection's `ClientSession`; its multiplexed
//! `EventStream` is pumped to the socket via the crate's `WsDataSink` +
//! single-writer task. Replies and event frames funnel through the one writer.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use datamancer::transport_ws::{WsDataSink, run_writer};
use datamancer::traits::{EventSink, PublishOutcome};
use datamancer::{ClientSession, Datamancer, Instrument, ProviderId, Scope};
use futures::{SinkExt as _, StreamExt as _};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{
    ErrorResponse, Request as HsRequest, Response as HsResponse,
};

use crate::control::codes;
use crate::ws::{WsReply, WsRequest};

/// Accept the WS handshake (enforcing the bearer token if configured), then run
/// the bridge until the socket closes or the client session ends.
pub async fn handle_connection(
    tcp: TcpStream,
    peer: SocketAddr,
    dm: Datamancer,
    auth_token: Option<Arc<String>>,
    channel_depth: usize,
) {
    let ws = match accept_with_auth(tcp, auth_token).await {
        Ok(ws) => ws,
        Err(e) => {
            tracing::warn!(%peer, error = %e, "ws handshake rejected");
            return;
        }
    };
    tracing::info!(%peer, "ws client connected");

    let (write, mut read) = ws.split();

    // Single writer: both event frames (via the sink) and control replies enqueue
    // strings on this channel; `run_writer` drains it to the socket.
    let (tx, rx) = mpsc::channel::<String>(channel_depth);
    let writer = tokio::spawn(run_writer(rx, write));

    // Open this connection's client and start pumping its stream into the sink.
    let session = dm.client_session();
    let sink: Arc<dyn EventSink> = Arc::new(WsDataSink::new(tx.clone()));
    let stream = match session.take_events().await {
        Ok(stream) => stream,
        Err(e) => {
            tracing::warn!(%peer, error = %e, "take_events failed");
            let _ = tx.try_send(serde_json::to_string(&WsReply::from_library_error(0, &e)).unwrap_or_default());
            return;
        }
    };
    let pump = spawn_pump(stream, sink);

    // Control loop: read frames, dispatch against the session, reply on `tx`.
    let mut closed_by_client = false;
    while let Some(msg) = read.next().await {
        let text = match msg {
            Ok(Message::Text(t)) => t.to_string(),
            Ok(Message::Close(_)) | Err(_) => break,
            Ok(Message::Ping(_) | Message::Pong(_) | Message::Binary(_) | Message::Frame(_)) => {
                continue;
            }
        };
        let reply = dispatch(&session, &dm, &text).await;
        let close_after = matches!(
            serde_json::from_str::<WsRequest>(&text),
            Ok(WsRequest::CloseClient { .. })
        ) && reply.ok;
        if let Ok(line) = serde_json::to_string(&reply) {
            if tx.send(line).await.is_err() {
                break;
            }
        }
        if close_after {
            closed_by_client = true;
            break;
        }
    }

    // Teardown: close the session (emits terminal `session_closing` on the
    // stream), let the pump drain under a bound, then drop the writer.
    let _ = session.close().await;
    if tokio::time::timeout(Duration::from_secs(2), pump).await.is_err() {
        tracing::warn!(%peer, "ws pump did not drain in time");
    }
    // Dropping `tx` lets `run_writer` finish once the channel empties.
    drop(tx);
    let _ = tokio::time::timeout(Duration::from_secs(2), writer).await;
    tracing::info!(%peer, closed_by_client, "ws client disconnected");
}

/// Perform the tungstenite server handshake, rejecting the upgrade with 401 if a
/// configured bearer token is missing or wrong.
async fn accept_with_auth(
    tcp: TcpStream,
    auth_token: Option<Arc<String>>,
) -> Result<WebSocketStream<TcpStream>, tokio_tungstenite::tungstenite::Error> {
    tokio_tungstenite::accept_hdr_async(tcp, move |req: &HsRequest, resp: HsResponse| {
        if let Some(expected) = auth_token.as_ref() {
            let presented = req
                .headers()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "));
            if presented != Some(expected.as_str()) {
                let mut err = ErrorResponse::new(Some("missing or invalid bearer token".into()));
                *err.status_mut() = tokio_tungstenite::tungstenite::http::StatusCode::UNAUTHORIZED;
                return Err(err);
            }
        }
        Ok(resp)
    })
    .await
}

/// Dispatch one parsed control frame against the connection's session.
async fn dispatch(session: &ClientSession, dm: &Datamancer, line: &str) -> WsReply {
    let req = match serde_json::from_str::<WsRequest>(line) {
        Ok(req) => req,
        Err(e) => return WsReply::error(0, codes::BAD_REQUEST, format!("invalid request: {e}")),
    };
    let id = req.id();
    match req {
        WsRequest::Subscribe { spec, .. } => {
            let instrument = Instrument::new(
                ProviderId::new(spec.provider.clone()),
                spec.asset_class.into(),
                spec.symbol.clone(),
            );
            match session
                .subscribe(instrument, spec.kind.into(), Scope::Live { backfill_from: None }, spec.persistence.options())
                .await
            {
                Ok(()) => WsReply::ok(id),
                Err(e) => WsReply::from_library_error(id, &e),
            }
        }
        WsRequest::Unsubscribe { provider, asset_class, symbol, kind, .. } => {
            let instrument = Instrument::new(ProviderId::new(provider), asset_class.into(), symbol);
            match session.unsubscribe(instrument, kind.into()).await {
                Ok(()) => WsReply::ok(id),
                Err(e) => WsReply::from_library_error(id, &e),
            }
        }
        WsRequest::Snapshot { .. } => match dm.snapshot().await {
            Ok(snapshot) => WsReply::snapshot(id, snapshot),
            Err(e) => WsReply::from_library_error(id, &e),
        },
        WsRequest::CloseClient { .. } => WsReply::ok(id),
    }
}

/// Pump the client's multiplexed stream into the WS sink, in arrival order.
/// Stops when the stream ends or the sink rejects (slow-consumer overrun).
fn spawn_pump(
    mut stream: datamancer::EventStream,
    sink: Arc<dyn EventSink>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(ev) = stream.next().await {
            match sink.publish(ev).await {
                PublishOutcome::Delivered => {}
                PublishOutcome::Rejected(_) => {
                    tracing::warn!("ws sink rejected event (slow consumer); stopping pump");
                    break;
                }
            }
        }
        let _ = sink.flush().await;
    })
}
```

Note: the `Message` variant set and `Message::Text(t)` payload type must match the pinned `tokio-tungstenite`. If `t.to_string()` does not exist on the text payload, use `String::from(t)` or `t.as_str().to_owned()`. If `Message::Frame` is not a variant in this version, drop it from the match arm. Verify against the compiler in Step 6.

Note on keepalive: `tokio-tungstenite` auto-replies to inbound `Ping` with `Pong`, so a client-driven keepalive works with this bridge as written. The `[ws].keepalive_secs` config field is **reserved** for a future *server-initiated* periodic-ping liveness probe (an added `tokio::time::interval` branch in the read loop); it is intentionally not consumed yet. Do not delete the field — it is part of the documented config surface — but do not add the interval in this task. If a clippy `unused`/dead-code lint fires on `keepalive_secs`, the field is still read by config (de)serialization, so no `#[allow]` is needed.

- [ ] **Step 3: Implement the listener**

Create `crates/datamancerd/src/ws/listener.rs`:

```rust
//! The WS listener: bind, accept, spawn one `handle_connection` per socket.
//! Its own bind/posture, separate from the loopback read-only web UI, because
//! this surface is mutating and network-reachable.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use datamancer::Datamancer;
use tokio::net::TcpListener;

use crate::config::WsConfig;
use crate::ws::conn::handle_connection;

/// Serve the WS client surface until `shutdown` resolves. New accepts stop once
/// shutdown fires; in-flight connections are dropped by their own teardown.
///
/// # Errors
///
/// Propagates the bind error.
pub async fn serve(
    dm: Datamancer,
    cfg: WsConfig,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    let addr: SocketAddr = format!("{}:{}", cfg.bind, cfg.port)
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("ws bind address: {e}")))?;
    let listener = TcpListener::bind(addr).await?;

    if cfg.auth_token.is_none() {
        if addr.ip().is_loopback() {
            tracing::warn!(%addr, "ws surface has no auth_token (loopback only; set [ws].auth_token before exposing)");
        } else {
            tracing::warn!(%addr, "ws surface bound OFF-LOOPBACK with NO auth_token — unauthenticated remote access; set [ws].auth_token");
        }
    }
    tracing::info!(%addr, "datamancerd ws client surface listening");

    let auth_token = cfg.auth_token.map(Arc::new);
    let channel_depth = cfg.channel_depth;
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            () = &mut shutdown => break,
            accepted = listener.accept() => match accepted {
                Ok((tcp, peer)) => {
                    let dm = dm.clone();
                    let auth_token = auth_token.clone();
                    tokio::spawn(handle_connection(tcp, peer, dm, auth_token, channel_depth));
                }
                Err(e) => tracing::warn!(error = %e, "ws accept failed"),
            },
        }
    }
    Ok(())
}
```

- [ ] **Step 4: Wire the submodules into `ws/mod.rs`**

Replace `crates/datamancerd/src/ws/mod.rs` with:

```rust
//! The remote WebSocket client surface (single bidirectional connection = one
//! client). Owns the listener, per-connection bridge, and the WS control
//! protocol. The event wire format + sink + writer live in the
//! `datamancer-transport-ws` crate; this module owns the part that touches the
//! orchestrator (`ClientSession`).

mod conn;
mod listener;
mod protocol;

pub use listener::serve;
pub use protocol::{WsReply, WsRequest};
```

- [ ] **Step 5: Spawn + shut down the listener in `server.rs`**

In `crates/datamancerd/src/server.rs`, mirror the `web` handling. After the web server start (around the `start_web` call in `run`), add a WS listener start. First add a field/config on `Server` for the ws config, populated in `bootstrap` from `config.ws`:

In `bootstrap`, near the `web` capture (`crates/datamancerd/src/server.rs:155-158`):

```rust
        #[cfg(feature = "ws")]
        let ws = config.ws.clone();
```

Add to the `Server` struct (near the `web` field, ~line 130):

```rust
    #[cfg(feature = "ws")]
    ws: Option<crate::config::WsConfig>,
```

And to the `Ok(Self { ... })` initializer (near `web,`):

```rust
            #[cfg(feature = "ws")]
            ws,
```

In `run`, after `let mut web_handles = self.start_web().await?;`, add:

```rust
        #[cfg(feature = "ws")]
        let (ws_task, ws_shutdown) = self.start_ws();
```

And add this method to `impl Server` (near `start_web`):

```rust
    /// Start the WS client surface if enabled. Returns the serve task and its
    /// shutdown trigger (both `None`-equivalent when disabled: a no-op task and a
    /// dropped sender).
    #[cfg(feature = "ws")]
    fn start_ws(&self) -> (tokio::task::JoinHandle<std::io::Result<()>>, Option<oneshot::Sender<()>>) {
        let Some(cfg) = self.ws.as_ref().filter(|w| w.enabled).cloned() else {
            return (tokio::spawn(async { Ok(()) }), None);
        };
        let (shutdown, shutdown_rx) = oneshot::channel::<()>();
        let dm = self.dm.clone();
        let task = tokio::spawn(async move {
            crate::ws::serve(dm, cfg, async move {
                let _ = shutdown_rx.await;
            })
            .await
        });
        (task, Some(shutdown))
    }
```

In the drain section of `run` (after the web `handles.shutdown().await;` block, before building the `DrainRecorder`), add:

```rust
        #[cfg(feature = "ws")]
        {
            if let Some(trigger) = ws_shutdown {
                let _ = trigger.send(());
            }
            if tokio::time::timeout(Duration::from_secs(5), ws_task).await.is_err() {
                tracing::warn!("ws surface did not drain within timeout");
            }
        }
```

- [ ] **Step 6: Build the feature**

Run: `cargo build -p datamancerd --features ws`
Expected: builds. Fix any `Message` variant / text-payload mismatches flagged by the compiler per the notes in Steps 2.

- [ ] **Step 7: Write the gated end-to-end test**

Create `crates/datamancerd/tests/ws_e2e.rs`:

```rust
//! End-to-end WS client surface tests. `#[ignore]`d — they bind a socket and
//! drive a real `Datamancer`; run with:
//!   cargo test -p datamancerd --features ws --test ws_e2e -- --ignored
//!
//! These assume a paper-Alpaca-capable build; they exercise wiring
//! (handshake/auth, subscribe reply + id echo, snapshot reply, teardown), not
//! live market data.

#![cfg(feature = "ws")]

use futures::{SinkExt as _, StreamExt as _};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

// Helper: build a Datamancer + WsConfig, spawn `serve`, return the bound URL and
// a shutdown trigger. Implement using the same construction the daemon's
// integration tests use (see `tests/daemon_e2e.rs` for the Config/bootstrap
// pattern); bind port 0 and read back the actual port.

#[tokio::test]
#[ignore = "binds a socket; run with --ignored"]
async fn subscribe_reply_echoes_id_and_snapshot_returns() {
    // 1. Start the ws surface on 127.0.0.1:0 with no auth_token.
    // 2. connect_async("ws://127.0.0.1:<port>/").
    // 3. Send {"id":7,"op":"snapshot"}; assert reply {"id":7,"ok":true,"snapshot":{...}}.
    // 4. Send {"id":8,"op":"subscribe",...paper crypto...}; assert {"id":8,"ok":true}.
    // 5. Close; assert the server tears down (no panic; task joins on shutdown).
    todo!("fill in using the daemon_e2e Config/bootstrap helper");
}

#[tokio::test]
#[ignore = "binds a socket; run with --ignored"]
async fn missing_bearer_token_is_rejected() {
    // Start with auth_token = Some("secret"); connect WITHOUT the header.
    // Assert connect_async fails (401 during handshake).
    let _ = (
        connect_async as fn(_) -> _,
        Message::text("x"),
        <&str as IntoClientRequest>::into_client_request,
    ); // keep imports referenced until the body is filled in
    todo!("fill in using the daemon_e2e Config/bootstrap helper");
}
```

Note: model the `Datamancer`/`Config` construction on `crates/datamancerd/tests/daemon_e2e.rs`. Because these are `#[ignore]`d and environment-dependent, replace each `todo!()` with the concrete harness following that file's pattern; keep them ignored so normal CI stays green.

- [ ] **Step 8: Verify the ignored tests compile**

Run: `cargo test -p datamancerd --features ws --test ws_e2e -- --list`
Expected: lists the two tests without compiling errors (bodies may be `todo!()` but must type-check the harness imports).

- [ ] **Step 9: Confirm default build still green (feature off)**

Run: `cargo build -p datamancerd && cargo test -p datamancerd --lib`
Expected: builds and passes without the `ws` feature.

- [ ] **Step 10: Lint the feature**

Run: `cargo clippy -p datamancerd --features ws --all-targets -- -D warnings`
Expected: no warnings.

- [ ] **Step 11: Commit**

```bash
git add crates/datamancerd/
git commit -m "feat(datamancerd): WS listener + per-connection bridge + bearer auth"
```

---

### Task 7: Documentation

**Files:**
- Create: `crates/datamancer-transport-ws/README.md`
- Create: `crates/datamancer-transport-ws/CLAUDE.md`
- Modify: `crates/datamancerd/README.md`
- Modify: `CLAUDE.md` (workspace crate list)

**Interfaces:** none (docs only).

- [ ] **Step 1: Crate README**

Create `crates/datamancer-transport-ws/README.md` describing: single-connection-per-client model; inbound `WsRequest` control (with `id`, no `client`) reusing the UDS `SubscriptionSpec` + stable `codes`; outbound `EventFrame` JSON (inline instrument, prices as `i64`, timestamp triple preserved, `Seq::SYNTHETIC` verbatim); control routing (connection-scoped suppressed, per-symbol carried); backpressure (bounded channel; overrun → disconnect, documented as lossy-on-overrun); security posture (bearer token, TLS via reverse proxy, worked-example scope, not yet hardened).

- [ ] **Step 2: Crate CLAUDE.md**

Create `crates/datamancer-transport-ws/CLAUDE.md` capturing the invariants: `#![forbid(unsafe_code)]`; core-only workspace dep; wire format is transport-internal (`to_wire`/`from_wire` the supported path); prices cross as `i64`; single-writer task ordering; overrun policy; the goal that this crate + `datamancer-transport-iceoryx2` are the two worked examples for a future unified client-transport trait.

- [ ] **Step 3: datamancerd README — WS section**

Add a `## WebSocket client surface` section to `crates/datamancerd/README.md`: the `[ws]` config table (from the design's Security section), the request/reply JSON examples, the shared `codes`, and the security caveats (mutating + network-reachable; separate from the loopback read-only web UI).

- [ ] **Step 4: Workspace CLAUDE.md crate list**

In `CLAUDE.md` (root), add `datamancer-transport-ws` to the workspace crate description list (a sibling to `datamancer-transport-iceoryx2`), noting it is a WS client transport gated behind `datamancer`'s `transport-ws` feature and `datamancerd`'s `ws` feature, both off by default.

- [ ] **Step 5: Commit**

```bash
git add crates/datamancer-transport-ws/README.md crates/datamancer-transport-ws/CLAUDE.md crates/datamancerd/README.md CLAUDE.md
git commit -m "docs: document the WebSocket client transport + surface"
```

---

## Final verification

- [ ] `cargo build` (default features) — green.
- [ ] `cargo build -p datamancerd --features ws` — green.
- [ ] `cargo test` (default) — green.
- [ ] `cargo test -p datamancer-transport-ws` — green.
- [ ] `cargo clippy --all-targets -- -D warnings` and `cargo clippy -p datamancerd --features ws --all-targets -- -D warnings` — clean.
- [ ] `cargo fmt --check` — clean.
