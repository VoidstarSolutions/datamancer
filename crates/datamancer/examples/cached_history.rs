//! Historical read-through cache demo (no credentials, no network).
//!
//! A synthetic provider serves a fixed set of daily bars and counts how many
//! times it is asked to fetch. We open the same historical session twice
//! against an embedded `SurrealKV` cache:
//!
//! 1. Cold run — the cache is empty, so the provider is hit once and the data
//!    is stored.
//! 2. Warm run — the same range is fully covered, so the provider is NOT hit
//!    and every bar is served from disk.
//!
//! Run with:
//!
//! ```text
//! cargo run --example cached_history
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use datamancer::storage::{SurrealCache, SurrealCacheConfig};
use datamancer::{
    AssetClass, Bar, BarInterval, ControlKind, Datamancer, EventKind, HistoryRequest, Instrument,
    LiveHandle, MarketEvent, PersistenceOptions, Price, Provider, ProviderId, Result, Scope, Seq,
    Session, Timestamp,
};
use futures::StreamExt;
use tokio::sync::mpsc;

const PROVIDER: &str = "synthetic";

/// Serves `count` daily bars and tracks how many fetches it has served.
struct SyntheticProvider {
    bars: Vec<MarketEvent>,
    fetch_count: Arc<AtomicUsize>,
}

impl SyntheticProvider {
    fn new(symbol: &str, count: i64, fetch_count: Arc<AtomicUsize>) -> Self {
        const DAY_NS: i64 = 86_400 * 1_000_000_000;
        let bars = (0..count)
            .map(|i| {
                MarketEvent::Bar(Bar {
                    instrument: instrument(symbol),
                    interval: BarInterval::OneDay,
                    source_ts: Timestamp(i * DAY_NS),
                    // rx_ts collapses to source_ts in pure-historical replay.
                    rx_ts: Timestamp(i * DAY_NS),
                    // Placeholder: datamancer assigns the real session seq at receipt.
                    seq: Seq(0),
                    open: Price::from_units(100 + i),
                    high: Price::from_units(101 + i),
                    low: Price::from_units(99 + i),
                    close: Price::from_units(100 + i),
                    volume: datamancer::Quantity::from_units(1_000),
                })
            })
            .collect();
        Self { bars, fetch_count }
    }
}

#[async_trait]
impl Provider for SyntheticProvider {
    fn id(&self) -> &str {
        PROVIDER
    }

    fn supports(&self, _instrument: &Instrument, kind: EventKind) -> bool {
        matches!(kind, EventKind::Bar(BarInterval::OneDay))
    }

    async fn start_live(&self, _sink: mpsc::Sender<MarketEvent>) -> Result<Box<dyn LiveHandle>> {
        Err(datamancer::Error::Provider {
            provider: PROVIDER.to_string(),
            message: "synthetic provider is historical-only".to_string(),
        })
    }

    async fn fetch_history(
        &self,
        request: HistoryRequest,
        sink: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        self.fetch_count.fetch_add(1, Ordering::SeqCst);
        for ev in &self.bars {
            if let MarketEvent::Bar(b) = ev
                && b.source_ts.0 >= request.from.0
                && b.source_ts.0 < request.to.0
                && sink.send(ev.clone()).await.is_err()
            {
                return Ok(());
            }
        }
        Ok(())
    }
}

fn instrument(symbol: &str) -> Instrument {
    Instrument::new(
        ProviderId::from_static(PROVIDER),
        AssetClass::Equity,
        symbol,
    )
}

async fn run_once(dm: &Datamancer, label: &str) -> usize {
    let session: Session = dm
        .session(
            instrument("ACME"),
            EventKind::Bar(BarInterval::OneDay),
            Scope::Historical {
                from: Timestamp(0),
                to: Timestamp(i64::MAX),
            },
            PersistenceOptions::cached(),
        )
        .await
        .expect("open session");
    let mut stream = session.take_events().await.expect("take events");
    let mut bars = 0usize;
    while let Some(ev) = stream.next().await {
        match ev {
            MarketEvent::Bar(_) => bars += 1,
            MarketEvent::Control(c) if matches!(c.kind, ControlKind::SessionClosing) => break,
            _ => {}
        }
    }
    println!("{label}: received {bars} bars");
    bars
}

#[tokio::main]
async fn main() -> Result<()> {
    // A unique, auto-cleaned temp dir so concurrent or interrupted runs of the
    // demo never collide on a shared path. Removed when `dir` drops at exit.
    let dir = tempfile::tempdir().expect("create temp dir");

    let fetch_count = Arc::new(AtomicUsize::new(0));
    let provider = SyntheticProvider::new("ACME", 30, fetch_count.clone());
    let cache = SurrealCache::open(SurrealCacheConfig::embedded(dir.path())).await?;

    let dm = Datamancer::builder()
        .provider_arc(Arc::new(provider))
        .historical_cache(Box::new(cache))
        .build()?;

    let cold = run_once(&dm, "cold run").await;
    let warm = run_once(&dm, "warm run").await;

    let fetches = fetch_count.load(Ordering::SeqCst);
    println!("\nprovider fetches total: {fetches}");
    println!("cold bars == warm bars: {}", cold == warm);
    assert_eq!(cold, warm, "both runs return the same data");
    assert_eq!(fetches, 1, "warm run served entirely from cache");
    println!("\n\u{2713} the warm run hit the cache, not the provider.");

    Ok(())
}
