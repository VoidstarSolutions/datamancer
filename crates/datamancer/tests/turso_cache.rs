//! Integration tests for the Turso-backed [`HistoricalCache`].
//!
//! Uses the in-memory engine so the suite stays self-contained and fast.
//! A separate `embedded_round_trip` test exercises the on-disk file-backed
//! engine against a tempdir to confirm the persistent path actually works.
//! Ported 1:1 from the retired prior backend's parity suite.

#![cfg(feature = "storage-turso")]

use datamancer::storage::{TursoCache, TursoCacheConfig};
use datamancer::{
    Adjustment, AssetClass, Bar, BarInterval, CacheKey, EventKind, GapSpan, HistoricalCache,
    Instrument, MarketEvent, Price, ProviderId, Quantity, Seq, Timestamp, Trade,
};
use datamancer_core::ReplayRequest;
use futures::StreamExt;

fn inst(symbol: &str) -> Instrument {
    Instrument::new(
        ProviderId::from_static("alpaca"),
        AssetClass::Equity,
        symbol,
    )
}

fn trade(symbol: &str, ts: i64, price: f64, size: u64) -> MarketEvent {
    MarketEvent::Trade(Trade {
        instrument: inst(symbol),
        source_ts: Timestamp(ts),
        rx_ts: Timestamp(ts),
        seq: Seq(0),
        price: Price::from_f64_round(price),
        // `size` is whole shares, matching the tap-log suite's fixtures.
        size: Quantity::from_units(size),
    })
}

fn bar(symbol: &str, ts: i64, close: f64) -> MarketEvent {
    MarketEvent::Bar(Bar {
        instrument: inst(symbol),
        interval: BarInterval::OneMinute,
        source_ts: Timestamp(ts),
        rx_ts: Timestamp(ts),
        seq: Seq(0),
        open: Price::from_f64_round(close),
        high: Price::from_f64_round(close),
        low: Price::from_f64_round(close),
        close: Price::from_f64_round(close),
        volume: Quantity::from_units(100),
    })
}

fn key(kind: EventKind, from: i64, to: i64) -> CacheKey {
    key_adj(kind, from, to, Adjustment::default())
}

fn key_adj(kind: EventKind, from: i64, to: i64, adjustment: Adjustment) -> CacheKey {
    CacheKey {
        instrument: inst("AAPL"),
        kind,
        from: Timestamp(from),
        to: Timestamp(to),
        adjustment,
    }
}

#[tokio::test]
async fn lookup_returns_none_for_empty_cache() {
    let cache = TursoCache::open(TursoCacheConfig::Memory).await.unwrap();
    let k = key(EventKind::Trade, 0, 1_000_000);
    assert!(cache.lookup(&k).await.unwrap().is_none());
}

#[tokio::test]
async fn store_then_replay_round_trip_preserves_order_and_values() {
    let cache = TursoCache::open(TursoCacheConfig::Memory).await.unwrap();
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
        instruments: vec![inst("AAPL")],
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
    let cache = TursoCache::open(TursoCacheConfig::Memory).await.unwrap();
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
    cache.store(&k3, &[bar("AAPL", 250, 1.75)]).await.unwrap();
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
    let cache = TursoCache::open(TursoCacheConfig::Memory).await.unwrap();
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
    let cfg = TursoCacheConfig::embedded(&path);
    let cache = TursoCache::open(cfg.clone()).await.unwrap();
    let k = key(EventKind::Trade, 0, 100);
    cache
        .store(
            &k,
            &[trade("AAPL", 10, 50.0, 1), trade("AAPL", 50, 51.0, 2)],
        )
        .await
        .unwrap();
    drop(cache);
    // The previous handle's file lock is released on drop, but it retries
    // reopen while that release is still in flight — the underlying file
    // lock can take a tick to clear. A fixed sleep here is flaky on slow CI;
    // poll `open` until the lock clears, capped at ~5 s total. On any error
    // we just retry — by the deadline we either succeed or surface the most
    // recent error so the failure mode is diagnosable.
    let cache = {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            match TursoCache::open(cfg.clone()).await {
                Ok(c) => break c,
                Err(e) if std::time::Instant::now() >= deadline => {
                    panic!("embedded reopen never succeeded within 5s: {e}");
                }
                Err(_) => {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            }
        }
    };
    let coverage = cache.lookup(&k).await.unwrap().expect("coverage");
    assert_eq!(coverage.event_count, 2);
}

#[tokio::test]
async fn store_claims_exactly_the_key_range_not_the_event_span() {
    let cache = TursoCache::open(TursoCacheConfig::Memory).await.unwrap();
    // Key range is [100, 200) but the events sit at 100 and 250 — outside the
    // key's upper bound. Coverage must NOT extend to 250.
    let k = key(EventKind::Trade, 100, 200);
    cache
        .store(
            &k,
            &[trade("AAPL", 100, 1.0, 1), trade("AAPL", 250, 2.0, 1)],
        )
        .await
        .unwrap();

    // A probe of [200, 300) must report a gap (the 250 event did not extend
    // coverage past 200).
    let probe = key(EventKind::Trade, 200, 300);
    let gaps = cache.gaps(&probe).await.unwrap();
    assert_eq!(
        gaps,
        vec![GapSpan {
            from_source_ts: Timestamp(200),
            to_source_ts: Timestamp(300),
        }]
    );
}

#[tokio::test]
async fn store_of_empty_range_marks_it_covered() {
    let cache = TursoCache::open(TursoCacheConfig::Memory).await.unwrap();
    // A successful fetch that returned no events must still mark the
    // requested range covered, so it is not re-fetched as a gap.
    let k = key(EventKind::Trade, 0, 1000);
    cache.store(&k, &[]).await.unwrap();
    assert!(
        cache.gaps(&k).await.unwrap().is_empty(),
        "an empty successful fetch should leave no gap in its range"
    );
}

#[tokio::test]
async fn store_replaces_existing_rows_in_the_claimed_range() {
    let cache = TursoCache::open(TursoCacheConfig::Memory).await.unwrap();
    let k = key(EventKind::Bar(BarInterval::OneMinute), 0, 1000);
    // Initial store deposits a (soon-to-be stale) bar at 500.
    cache.store(&k, &[bar("AAPL", 500, 99.0)]).await.unwrap();
    // Re-store the same range (a refresh) with different, fewer events.
    cache.store(&k, &[bar("AAPL", 100, 1.0)]).await.unwrap();

    // Replay must return only the fresh bar; the stale 500 row is gone.
    let source = cache.as_replay_source(k.clone());
    let request = ReplayRequest {
        instruments: vec![inst("AAPL")],
        kinds: vec![EventKind::Bar(BarInterval::OneMinute)],
        from: Timestamp(0),
        to: Timestamp(1000),
    };
    let mut stream = source.open(request).await.unwrap();
    let mut got = Vec::new();
    while let Some(ev) = stream.next().await {
        if let MarketEvent::Bar(b) = ev {
            got.push(b.source_ts.0);
        }
    }
    assert_eq!(got, vec![100], "refresh must not leave the stale 500 row");
}

#[tokio::test]
async fn bars_segregate_by_adjustment_mode() {
    let cache = TursoCache::open(TursoCacheConfig::Memory).await.unwrap();
    let kind = EventKind::Bar(BarInterval::OneMinute);
    // Same (symbol, range) under two modes, with deliberately different close
    // prices so a mode mix-up is observable.
    let raw = key_adj(kind, 0, 1000, Adjustment::Raw);
    let all = key_adj(kind, 0, 1000, Adjustment::All);

    cache.store(&raw, &[bar("AAPL", 100, 10.0)]).await.unwrap();
    // Storing the All-mode bar must NOT delete the raw-mode row in the same
    // (symbol, range): the store DELETE is mode-scoped.
    cache.store(&all, &[bar("AAPL", 100, 20.0)]).await.unwrap();

    // Coverage counts are per-mode, not pooled.
    assert_eq!(cache.lookup(&raw).await.unwrap().unwrap().event_count, 1);
    assert_eq!(cache.lookup(&all).await.unwrap().unwrap().event_count, 1);

    // A read under each mode returns only that mode's bar — no orphaned
    // cross-mode rows leak through the symbol/time-filtered SELECT.
    assert_eq!(read_closes(&cache, &all).await, vec![20.0]);
    assert_eq!(read_closes(&cache, &raw).await, vec![10.0]);
}

async fn read_closes(cache: &TursoCache, k: &CacheKey) -> Vec<f64> {
    use datamancer::ReplayRequest;
    use futures::StreamExt;
    let source = cache.as_replay_source(k.clone());
    let request = ReplayRequest {
        instruments: vec![k.instrument.clone()],
        kinds: vec![k.kind],
        from: k.from,
        to: k.to,
    };
    let mut stream = source.open(request).await.unwrap();
    let mut closes = Vec::new();
    while let Some(ev) = stream.next().await {
        if let MarketEvent::Bar(b) = ev {
            closes.push(b.close.to_f64());
        }
    }
    closes
}

// Sanity: the public re-exports we lean on stay in place after the API
// reshape — Instrument constructs from a &str and EventKind is reachable.
#[test]
fn reexports_are_consistent() {
    let i: Instrument = inst("AAPL");
    let kind: EventKind = EventKind::Trade;
    let _ = (i, kind);
}

// --- catalog enumeration ----------------------------------------------------

#[tokio::test]
async fn catalog_empty_when_nothing_stored() {
    let cache = TursoCache::open(TursoCacheConfig::Memory).await.unwrap();
    assert!(cache.catalog().await.unwrap().is_empty());
}

#[tokio::test]
async fn catalog_survives_separator_characters_in_symbols() {
    // `Instrument` accepts any symbol string; a symbol containing `|` (or
    // anything else) must round-trip through store → catalog intact now that
    // coverage keys are real columns rather than a delimiter-joined id.
    let cache = TursoCache::open(TursoCacheConfig::Memory).await.unwrap();
    let weird = "A|B|bars_1d|raw";
    let k = CacheKey {
        instrument: inst(weird),
        kind: EventKind::Trade,
        from: Timestamp(100),
        to: Timestamp(400),
        adjustment: Adjustment::default(),
    };
    cache
        .store(&k, &[trade(weird, 100, 1.0, 1), trade(weird, 300, 2.0, 1)])
        .await
        .unwrap();

    let catalog = cache.catalog().await.unwrap();
    assert_eq!(
        catalog.len(),
        1,
        "the pipe-symbol entry must not be skipped"
    );
    assert_eq!(catalog[0].symbol, weird);
    assert_eq!(catalog[0].kind, EventKind::Trade);
    assert_eq!(catalog[0].event_count, 2);
}

#[tokio::test]
async fn catalog_roundtrips_stored_ranges() {
    use datamancer::CacheCatalogEntry;

    let cache = TursoCache::open(TursoCacheConfig::Memory).await.unwrap();

    // A trade range (stored under Raw regardless of the key's mode) ...
    let trade_key = key_adj(EventKind::Trade, 100, 400, Adjustment::All);
    cache
        .store(
            &trade_key,
            &[trade("AAPL", 100, 1.0, 1), trade("AAPL", 300, 2.0, 1)],
        )
        .await
        .unwrap();

    // ... and one bar range under each of two adjustment modes.
    let bar_all = key_adj(
        EventKind::Bar(BarInterval::OneMinute),
        0,
        200,
        Adjustment::All,
    );
    let bar_raw = key_adj(
        EventKind::Bar(BarInterval::OneMinute),
        0,
        200,
        Adjustment::Raw,
    );
    cache
        .store(&bar_all, &[bar("AAPL", 0, 1.0), bar("AAPL", 60, 2.0)])
        .await
        .unwrap();
    cache.store(&bar_raw, &[bar("AAPL", 0, 1.0)]).await.unwrap();

    let catalog = cache.catalog().await.unwrap();
    assert_eq!(catalog.len(), 3, "trade + two bar-adjustment keys");

    let find = |kind: EventKind, adj: Adjustment| -> CacheCatalogEntry {
        catalog
            .iter()
            .find(|e| e.kind == kind && e.adjustment == adj)
            .cloned()
            .unwrap_or_else(|| panic!("missing catalog entry for {kind:?} / {adj:?}"))
    };

    // Trades always store under Raw, even though the key requested All.
    let t = find(EventKind::Trade, Adjustment::Raw);
    assert_eq!(t.symbol, "AAPL");
    assert_eq!(t.provider.as_str(), "alpaca");
    assert_eq!(t.asset_class, Some(AssetClass::Equity));
    assert_eq!(t.event_count, 2);
    assert_eq!(
        t.segments,
        vec![GapSpan {
            from_source_ts: Timestamp(100),
            to_source_ts: Timestamp(400),
        }]
    );
    assert!(t.est_bytes.is_some_and(|b| b > 0));

    let b_all = find(EventKind::Bar(BarInterval::OneMinute), Adjustment::All);
    assert_eq!(b_all.event_count, 2);
    assert_eq!(b_all.asset_class, Some(AssetClass::Equity));
    let b_raw = find(EventKind::Bar(BarInterval::OneMinute), Adjustment::Raw);
    assert_eq!(b_raw.event_count, 1);
}
