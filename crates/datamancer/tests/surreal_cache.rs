//! Integration tests for the Surreal-backed [`HistoricalCache`].
//!
//! Uses the in-memory engine so the suite stays self-contained and fast.
//! A separate `embedded_round_trip` test exercises the on-disk SurrealKV
//! engine against a tempdir to confirm the persistent path actually works.

#![cfg(feature = "storage-surreal")]

use datamancer::{
    Bar, BarInterval, CacheKey, EventKind, GapSpan, HistoricalCache, Instrument, MarketEvent,
    Price, Seq, Timestamp, Trade,
};
use datamancer::storage::{SurrealCache, SurrealCacheConfig};
use datamancer_core::ReplayRequest;
use futures::StreamExt;

const PROVIDER: &str = "alpaca";

fn trade(symbol: &str, ts: i64, price: f64, size: u64) -> MarketEvent {
    MarketEvent::Trade(Trade {
        instrument: Instrument::new(symbol),
        source_ts: Timestamp(ts),
        rx_ts: Timestamp(ts),
        seq: Seq(0),
        price: Price::from_f64_round(price),
        size,
    })
}

fn bar(symbol: &str, ts: i64, close: f64) -> MarketEvent {
    MarketEvent::Bar(Bar {
        instrument: Instrument::new(symbol),
        interval: BarInterval::OneMinute,
        source_ts: Timestamp(ts),
        rx_ts: Timestamp(ts),
        seq: Seq(0),
        open: Price::from_f64_round(close),
        high: Price::from_f64_round(close),
        low: Price::from_f64_round(close),
        close: Price::from_f64_round(close),
        volume: 100,
    })
}

fn key(kind: EventKind, from: i64, to: i64) -> CacheKey {
    CacheKey {
        provider: PROVIDER.to_string(),
        instrument: Instrument::new("AAPL"),
        kind,
        from: Timestamp(from),
        to: Timestamp(to),
    }
}

#[tokio::test]
async fn lookup_returns_none_for_empty_cache() {
    let cache = SurrealCache::open(SurrealCacheConfig::Memory).await.unwrap();
    let k = key(EventKind::Trade, 0, 1_000_000);
    assert!(cache.lookup(&k).await.unwrap().is_none());
}

#[tokio::test]
async fn store_then_replay_round_trip_preserves_order_and_values() {
    let cache = SurrealCache::open(SurrealCacheConfig::Memory).await.unwrap();
    let k = key(EventKind::Trade, 100, 400);
    let events = vec![
        trade("AAPL", 100, 150.10, 1),
        trade("AAPL", 250, 150.25, 2),
        trade("AAPL", 399, 150.40, 3),
    ];
    cache.store(&k, &events).await.unwrap();

    let coverage = cache.lookup(&k).await.unwrap().expect("coverage");
    assert_eq!(coverage.from.0, 100);
    assert!(coverage.to.0 >= 400);
    assert_eq!(coverage.event_count, 3);

    let source = cache.as_replay_source(k.clone());
    let request = ReplayRequest {
        instruments: vec![Instrument::new("AAPL")],
        kinds: vec![EventKind::Trade],
        from: Timestamp(100),
        to: Timestamp(400),
    };
    let mut stream = source.open(request).await.unwrap();
    let mut got = Vec::new();
    while let Some(ev) = stream.next().await {
        got.push(ev);
    }
    assert_eq!(got.len(), 3);
    for (a, b) in events.iter().zip(got.iter()) {
        match (a, b) {
            (MarketEvent::Trade(a), MarketEvent::Trade(b)) => {
                assert_eq!(a.source_ts, b.source_ts);
                assert_eq!(a.price, b.price);
                assert_eq!(a.size, b.size);
                assert_eq!(a.instrument, b.instrument);
            }
            _ => panic!("non-trade in replay"),
        }
    }
}

#[tokio::test]
async fn gaps_reports_uncovered_subranges() {
    let cache = SurrealCache::open(SurrealCacheConfig::Memory).await.unwrap();
    // First, ingest events for [100, 200) and [300, 400).
    let k1 = key(EventKind::Bar(BarInterval::OneMinute), 100, 200);
    cache
        .store(&k1, &[bar("AAPL", 100, 1.0), bar("AAPL", 199, 1.5)])
        .await
        .unwrap();
    let k2 = key(EventKind::Bar(BarInterval::OneMinute), 300, 400);
    cache
        .store(&k2, &[bar("AAPL", 300, 2.0), bar("AAPL", 399, 2.5)])
        .await
        .unwrap();

    // Ask about [50, 500): gaps should be [50,100), [200,300), [400,500).
    let probe = key(EventKind::Bar(BarInterval::OneMinute), 50, 500);
    let gaps = cache.gaps(&probe).await.unwrap();
    let want = vec![
        GapSpan {
            from_source_ts: Timestamp(50),
            to_source_ts: Timestamp(100),
        },
        GapSpan {
            from_source_ts: Timestamp(200),
            to_source_ts: Timestamp(300),
        },
        GapSpan {
            from_source_ts: Timestamp(400),
            to_source_ts: Timestamp(500),
        },
    ];
    assert_eq!(gaps, want);

    // After a backfill of [200,300), the middle gap closes.
    let k3 = key(EventKind::Bar(BarInterval::OneMinute), 200, 300);
    cache
        .store(&k3, &[bar("AAPL", 250, 1.75)])
        .await
        .unwrap();
    let gaps = cache.gaps(&probe).await.unwrap();
    assert_eq!(
        gaps,
        vec![
            GapSpan {
                from_source_ts: Timestamp(50),
                to_source_ts: Timestamp(100),
            },
            GapSpan {
                from_source_ts: Timestamp(400),
                to_source_ts: Timestamp(500),
            },
        ],
    );
}

#[tokio::test]
async fn fully_covered_range_reports_no_gaps() {
    let cache = SurrealCache::open(SurrealCacheConfig::Memory).await.unwrap();
    let k = key(EventKind::Trade, 0, 1000);
    cache
        .store(&k, &[trade("AAPL", 0, 1.0, 1), trade("AAPL", 999, 1.0, 1)])
        .await
        .unwrap();
    let gaps = cache.gaps(&k).await.unwrap();
    assert!(gaps.is_empty(), "expected no gaps, got {gaps:?}");
}

#[tokio::test]
async fn embedded_round_trip_persists_to_disk() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kv");
    let cfg = SurrealCacheConfig::embedded(&path);
    let cache = SurrealCache::open(cfg.clone()).await.unwrap();
    let k = key(EventKind::Trade, 0, 100);
    cache
        .store(&k, &[trade("AAPL", 10, 50.0, 1), trade("AAPL", 50, 51.0, 2)])
        .await
        .unwrap();
    drop(cache);
    // SurrealKV's lock is released on drop, but the spawned engine task
    // holding it needs a tick to finish unwinding.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Reopen and confirm the data is still there.
    let cache = SurrealCache::open(cfg).await.unwrap();
    let coverage = cache.lookup(&k).await.unwrap().expect("coverage");
    assert_eq!(coverage.event_count, 2);
}

// Sanity: the public re-exports we lean on stay in place after the API
// reshape — Instrument constructs from a &str and EventKind is reachable.
#[test]
fn reexports_are_consistent() {
    let _inst: Instrument = Instrument::new("AAPL");
    let _kind: EventKind = EventKind::Trade;
}
