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
        let Ok(json) = serde_json::to_string(&frame) else {
            return PublishOutcome::Rejected(ev);
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

#[cfg(test)]
mod tests {
    use super::WsDataSink;
    use datamancer_core::{
        AssetClass, Control, ControlKind, EventSink, Instrument, MarketEvent, Price, ProviderId,
        PublishOutcome, Seq, Timestamp, Trade,
    };

    fn trade() -> MarketEvent {
        MarketEvent::Trade(Trade {
            instrument: Instrument::new(
                ProviderId::from_static("alpaca"),
                AssetClass::Crypto,
                "BTC/USD",
            ),
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
        assert!(matches!(
            sink.publish(trade()).await,
            PublishOutcome::Delivered
        ));
        let line = rx.recv().await.expect("frame");
        assert!(line.contains("\"type\":\"trade\""));
        assert!(line.contains("\"price\":42"));
    }

    #[tokio::test]
    async fn full_channel_rejects_and_hands_event_back() {
        // Capacity 1, no reader draining: second publish finds the channel full.
        let (tx, _rx) = tokio::sync::mpsc::channel::<String>(1);
        let sink = WsDataSink::new(tx);
        assert!(matches!(
            sink.publish(trade()).await,
            PublishOutcome::Delivered
        ));
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
            kind: ControlKind::ProviderConnected {
                provider: "alpaca".to_string(),
            },
        });
        // Suppressed frames are acked as Delivered but put nothing on the wire.
        assert!(matches!(sink.publish(ev).await, PublishOutcome::Delivered));
        assert!(
            rx.try_recv().is_err(),
            "no frame emitted for suppressed control"
        );
    }
}
