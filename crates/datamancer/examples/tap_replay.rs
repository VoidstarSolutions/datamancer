//! Live tap-log demo (no credentials, no network).
//!
//! A synthetic provider pushes a few trades into a live session configured to
//! tee to an embedded `SurrealKV` tap log. We then reopen the log as a replay
//! source and confirm the captured stream comes back in the exact arrival
//! order the session emitted it.
//!
//! Run with:
//!
//! ```text
//! cargo run --example tap_replay
//! ```

use std::sync::Arc;

use async_trait::async_trait;
use datamancer::storage::{SurrealTapLog, SurrealTapLogConfig};
use datamancer::{
    AssetClass, Datamancer, EventKind, HistoryRequest, Instrument, LiveHandle, MarketEvent,
    PersistenceOptions, Price, Provider, ProviderId, ReplayRequest, Result, Scope, Seq, TapLog,
    Timestamp, Trade,
};
use futures::StreamExt;
use tokio::sync::{Mutex, mpsc};

const PROVIDER: &str = "synthetic";

struct SyntheticProvider {
    sink: Arc<Mutex<Option<mpsc::Sender<MarketEvent>>>>,
}

#[async_trait]
impl Provider for SyntheticProvider {
    fn id(&self) -> &str {
        PROVIDER
    }

    fn supports(&self, _instrument: &Instrument, kind: EventKind) -> bool {
        matches!(kind, EventKind::Trade)
    }

    async fn start_live(&self, sink: mpsc::Sender<MarketEvent>) -> Result<Box<dyn LiveHandle>> {
        *self.sink.lock().await = Some(sink);
        Ok(Box::new(SyntheticLiveHandle))
    }

    async fn fetch_history(
        &self,
        _request: HistoryRequest,
        _sink: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        Ok(())
    }
}

struct SyntheticLiveHandle;

#[async_trait]
impl LiveHandle for SyntheticLiveHandle {
    async fn subscribe(&self, _instrument: Instrument, _kind: EventKind) -> Result<()> {
        Ok(())
    }

    async fn unsubscribe(&self, _instrument: Instrument, _kind: EventKind) -> Result<()> {
        Ok(())
    }

    async fn close(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

fn instrument() -> Instrument {
    Instrument::new(
        ProviderId::from_static(PROVIDER),
        AssetClass::Equity,
        "ACME",
    )
}

fn trade(source_ts: i64, price: i64) -> MarketEvent {
    MarketEvent::Trade(Trade {
        instrument: instrument(),
        source_ts: Timestamp(source_ts),
        rx_ts: Timestamp(source_ts),
        seq: Seq(0),
        price: Price::from_units(price),
        size: 1,
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let dir = tempfile::tempdir().expect("temp dir");
    let sink = Arc::new(Mutex::new(None));
    let provider = SyntheticProvider { sink: sink.clone() };
    let log = Arc::new(SurrealTapLog::open(SurrealTapLogConfig::embedded(dir.path())).await?);

    let dm = Datamancer::builder()
        .provider_arc(Arc::new(provider))
        .tap_log_arc(log.clone())
        .build()?;

    let mut session = dm
        .session(
            instrument(),
            EventKind::Trade,
            Scope::Live {
                backfill_from: None,
            },
            PersistenceOptions::none().with_tap_log(true),
        )
        .await?;
    let mut stream = session.take_events().expect("take events");

    // Push three trades through the live handle, deliberately NOT in source_ts
    // order, then consume them so we know forward() (and the tee) has run.
    if let Some(tx) = sink.lock().await.as_ref() {
        let _ = tx.send(trade(300, 30)).await;
        let _ = tx.send(trade(100, 10)).await;
        let _ = tx.send(trade(200, 20)).await;
    }
    let mut emitted = Vec::new();
    while emitted.len() < 3 {
        if let Some(MarketEvent::Trade(t)) = stream.next().await {
            emitted.push(t.source_ts.0);
        }
    }
    log.flush().await?;
    println!("live session emitted (arrival order): {emitted:?}");

    // Replay the captured stream.
    let source = log.as_replay_source();
    let mut replay = source
        .open(ReplayRequest {
            instruments: vec![instrument()],
            kinds: vec![EventKind::Trade],
            from: Timestamp(i64::MIN),
            to: Timestamp(i64::MAX),
        })
        .await?;
    let mut replayed = Vec::new();
    while let Some(MarketEvent::Trade(t)) = replay.next().await {
        replayed.push(t.source_ts.0);
    }
    println!("tap log replayed (arrival order): {replayed:?}");

    assert_eq!(emitted, replayed, "replay reproduces arrival order exactly");
    assert_eq!(
        replayed,
        vec![300, 100, 200],
        "arrival order, not source_ts order"
    );
    println!("\n\u{2713} the tap log replayed the live stream in arrival order.");
    Ok(())
}
