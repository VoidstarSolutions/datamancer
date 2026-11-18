//! A session's teardown against a provider hub that serves every route from
//! one loop, as the Alpaca hubs do: while the hub is blocked sending into a
//! session's full channel it reads no command, so the session must stop
//! holding that channel before it waits on the hub's unsubscribe.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use datamancer::{
    AssetClass, Datamancer, EventKind, Instrument, LiveHandle, MarketEvent, PersistenceOptions,
    Price, Provider, ProviderId, ReplaySource, Result, Scope, Seq, Surface, TapLog, Timestamp,
    Trade,
};
use datamancer_core::HistoryRequest;
use tokio::sync::{Notify, mpsc, oneshot};

fn inst() -> Instrument {
    Instrument::new(ProviderId::from_static("hub"), AssetClass::Equity, "AAPL")
}

fn trade(ts: i64) -> MarketEvent {
    MarketEvent::Trade(Trade {
        instrument: inst(),
        source_ts: Timestamp(ts),
        rx_ts: Timestamp(ts),
        seq: Seq(0),
        price: Price::from_f64_round(1.0),
        size: datamancer::Quantity::from_units(1),
    })
}

/// A provider whose hub floods the one route with trades and reads its
/// commands only between sends.
struct FloodingHub {
    closed: Arc<Notify>,
}

#[async_trait]
impl Provider for FloodingHub {
    fn id(&self) -> &'static str {
        "hub"
    }
    fn supports(&self, _instrument: &Instrument, _kind: EventKind, _surface: Surface) -> bool {
        true
    }
    async fn start_live(&self, sink: mpsc::Sender<MarketEvent>) -> Result<Box<dyn LiveHandle>> {
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<oneshot::Sender<()>>(8);
        tokio::spawn(async move {
            let mut ts = 0;
            loop {
                if let Ok(ack) = cmd_rx.try_recv() {
                    let _ = ack.send(());
                    continue;
                }
                if cmd_rx.is_closed() {
                    return;
                }
                ts += 1;
                if sink.send(trade(ts)).await.is_err() {
                    // The route is gone: wait for the next command.
                    match cmd_rx.recv().await {
                        Some(ack) => {
                            let _ = ack.send(());
                        }
                        None => return,
                    }
                }
            }
        });
        Ok(Box::new(HubHandle {
            cmd_tx,
            closed: Arc::clone(&self.closed),
        }))
    }
    async fn fetch_history(
        &self,
        _request: HistoryRequest,
        _sink: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        Ok(())
    }
}

struct HubHandle {
    cmd_tx: mpsc::Sender<oneshot::Sender<()>>,
    closed: Arc<Notify>,
}

#[async_trait]
impl LiveHandle for HubHandle {
    async fn subscribe(&self, _i: Instrument, _k: EventKind) -> Result<()> {
        Ok(())
    }
    async fn unsubscribe(&self, _i: Instrument, _k: EventKind) -> Result<()> {
        let (ack, acked) = oneshot::channel();
        if self.cmd_tx.send(ack).await.is_ok() {
            let _ = acked.await;
        }
        Ok(())
    }
    async fn close(self: Box<Self>) -> Result<()> {
        self.closed.notify_one();
        Ok(())
    }
}

/// A tap log whose first append waits for `release`: the session stops
/// draining its provider channel meanwhile, as a slow step would.
struct HeldTap {
    held: AtomicBool,
    release: Notify,
}

#[async_trait]
impl TapLog for HeldTap {
    async fn append(&self, _ev: &MarketEvent) -> Result<()> {
        if self.held.swap(false, Ordering::SeqCst) {
            self.release.notified().await;
        }
        Ok(())
    }
    async fn flush(&self) -> Result<()> {
        Ok(())
    }
    fn as_replay_source(&self) -> Box<dyn ReplaySource> {
        unreachable!("the test replays nothing")
    }
}

#[tokio::test]
async fn a_session_tears_down_while_its_hub_is_blocked_on_its_full_channel() {
    let closed = Arc::new(Notify::new());
    let tap = Arc::new(HeldTap {
        held: AtomicBool::new(true),
        release: Notify::new(),
    });
    let dm = Datamancer::builder()
        .provider_arc(Arc::new(FloodingHub {
            closed: Arc::clone(&closed),
        }))
        .tap_log_arc(tap.clone())
        .build()
        .expect("build");
    let session = dm
        .session(
            inst(),
            EventKind::Trade,
            Scope::live(),
            PersistenceOptions::none().with_tap_log(true),
        )
        .await
        .expect("live session opens");

    // The held append stops the session draining while the hub fills its
    // channel and blocks; then the session's last referrer leaves.
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(session);
    tap.release.notify_one();

    tokio::time::timeout(Duration::from_secs(5), closed.notified())
        .await
        .expect("the teardown never got past the hub's unsubscribe");
}
