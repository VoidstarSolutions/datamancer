//! Resume primitive demo (no credentials, no network).
//!
//! A synthetic provider serves a fixed set of historical trades and emits a
//! live trade every 50 ms. We open one stitched live session
//! (`backfill_from`), watch the backfill splice into the live tail, then
//! drop the stream, let events accumulate in the session's bounded resume
//! buffer, and re-take — the stream picks up with contiguous `seq` and no
//! gap.
//!
//! Run with:
//!
//! ```text
//! cargo run --example resume
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use datamancer::Surface;
use datamancer::storage::{TursoCache, TursoCacheConfig};
use datamancer::{
    AssetClass, Datamancer, EventKind, Instrument, LiveHandle, MarketEvent, PersistenceOptions,
    Price, Provider, ProviderId, Result, Scope, Seq, Timestamp, Trade,
};
use datamancer_core::HistoryRequest;
use futures::StreamExt;
use tokio::sync::mpsc;

const PROVIDER: &str = "synthetic";

fn instrument() -> Instrument {
    Instrument::new(
        ProviderId::from_static(PROVIDER),
        AssetClass::Equity,
        "ACME",
    )
}

fn trade(ts: i64) -> MarketEvent {
    MarketEvent::Trade(Trade {
        instrument: instrument(),
        source_ts: Timestamp(ts),
        rx_ts: Timestamp(ts),
        seq: Seq(0),
        price: Price::from_f64_round(100.0),
        size: datamancer::Quantity::from_units(1),
    })
}

/// Historical trades at fixed timestamps; live trades on a 50 ms ticker.
struct SyntheticProvider {
    live_stop: Arc<AtomicBool>,
    next_live_ts: Arc<AtomicI64>,
}

#[async_trait]
impl Provider for SyntheticProvider {
    fn id(&self) -> &str {
        PROVIDER
    }
    fn supports(&self, _instrument: &Instrument, kind: EventKind, _surface: Surface) -> bool {
        kind == EventKind::Trade
    }
    async fn start_live(&self, sink: mpsc::Sender<MarketEvent>) -> Result<Box<dyn LiveHandle>> {
        let stop = self.live_stop.clone();
        let next_ts = self.next_live_ts.clone();
        tokio::spawn(async move {
            while !stop.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(50)).await;
                let ts = next_ts.fetch_add(1_000, Ordering::SeqCst);
                if sink.send(trade(ts)).await.is_err() {
                    break;
                }
            }
        });
        Ok(Box::new(SyntheticLive {
            stop: self.live_stop.clone(),
        }))
    }
    async fn fetch_history(
        &self,
        request: HistoryRequest,
        sink: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        for ts in [100, 200, 300, 400, 500] {
            if ts >= request.from.0 && ts < request.to.0 && sink.send(trade(ts)).await.is_err() {
                return Ok(());
            }
        }
        Ok(())
    }
}

struct SyntheticLive {
    stop: Arc<AtomicBool>,
}

#[async_trait]
impl LiveHandle for SyntheticLive {
    async fn subscribe(&self, _i: Instrument, _k: EventKind) -> Result<()> {
        Ok(())
    }
    async fn unsubscribe(&self, _i: Instrument, _k: EventKind) -> Result<()> {
        Ok(())
    }
    async fn close(self: Box<Self>) -> Result<()> {
        self.stop.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let provider = SyntheticProvider {
        live_stop: Arc::new(AtomicBool::new(false)),
        next_live_ts: Arc::new(AtomicI64::new(1_000_000)),
    };
    let cache = TursoCache::open(TursoCacheConfig::Memory).await?;
    let dm = Datamancer::builder()
        .provider_arc(Arc::new(provider))
        .historical_cache(Box::new(cache))
        .build()?;

    let session = dm
        .session(
            instrument(),
            EventKind::Trade,
            Scope::Live {
                backfill_from: Some(Timestamp(0)),
            },
            PersistenceOptions::cached(),
        )
        .await?;

    // Phase 1: backfill (5 historical trades) splices ahead of the live tail.
    let mut stream = session.take_events().await?;
    let mut last_seq = 0;
    println!("first take — backfill + live tail:");
    for _ in 0..8 {
        if let Some(MarketEvent::Trade(t)) = stream.next().await {
            println!("  trade ts={:>9} seq={}", t.source_ts.0, t.seq.0);
            last_seq = t.seq.0;
        }
    }

    // Phase 2: drop the stream. The session keeps running (the Session
    // handle is the anchor); live events buffer in the bounded resume ring.
    drop(stream);
    println!("\nstream dropped; capturing continues detached\u{2026}");
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Phase 3: re-take. Buffered events flow with contiguous seq and no Gap.
    let mut stream = session.take_events().await?;
    println!("re-taken — buffered events resume:");
    for _ in 0..5 {
        if let Some(MarketEvent::Trade(t)) = stream.next().await {
            assert_eq!(t.seq.0, last_seq + 1, "seq contiguous across the re-take");
            last_seq = t.seq.0;
            println!("  trade ts={:>9} seq={}", t.source_ts.0, t.seq.0);
        }
    }

    session.close().await?;
    println!("\n\u{2713} re-take resumed with contiguous seq and no gap.");
    Ok(())
}
