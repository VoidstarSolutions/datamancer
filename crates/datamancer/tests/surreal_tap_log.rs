//! Integration tests for the Surreal-backed [`TapLog`].
//!
//! Uses the in-memory engine for the fast suite; one embedded test exercises
//! the on-disk `SurrealKV` path.

#![cfg(feature = "storage-surreal")]

use datamancer::storage::{SurrealTapLog, SurrealTapLogConfig};
use datamancer::{
    AssetClass, Bar, BarInterval, EventKind, Instrument, MarketEvent, Price, ProviderId, Quote,
    ReplayRequest, Seq, TapLog, Timestamp, Trade,
};
use futures::StreamExt;

fn inst(symbol: &str) -> Instrument {
    Instrument::new(
        ProviderId::from_static("alpaca"),
        AssetClass::Equity,
        symbol,
    )
}

fn trade(symbol: &str, source_ts: i64, rx_ts: i64, price: f64, size: u64) -> MarketEvent {
    MarketEvent::Trade(Trade {
        instrument: inst(symbol),
        source_ts: Timestamp(source_ts),
        rx_ts: Timestamp(rx_ts),
        seq: Seq(0),
        price: Price::from_f64_round(price),
        size,
    })
}

fn bar(symbol: &str, source_ts: i64, close: f64) -> MarketEvent {
    MarketEvent::Bar(Bar {
        instrument: inst(symbol),
        interval: BarInterval::OneMinute,
        source_ts: Timestamp(source_ts),
        rx_ts: Timestamp(source_ts),
        seq: Seq(0),
        open: Price::from_f64_round(close),
        high: Price::from_f64_round(close),
        low: Price::from_f64_round(close),
        close: Price::from_f64_round(close),
        volume: 100,
    })
}

fn full_request(symbol: &str, kind: EventKind) -> ReplayRequest {
    ReplayRequest {
        instruments: vec![inst(symbol)],
        kinds: vec![kind],
        from: Timestamp(i64::MIN),
        to: Timestamp(i64::MAX),
    }
}

#[tokio::test]
async fn append_then_flush_persists_and_replays_in_order() {
    let log = SurrealTapLog::open(SurrealTapLogConfig::Memory)
        .await
        .unwrap();
    log.append(&trade("AAPL", 100, 100, 150.10, 1))
        .await
        .unwrap();
    log.append(&trade("AAPL", 250, 250, 150.25, 2))
        .await
        .unwrap();
    log.append(&trade("AAPL", 399, 399, 150.40, 3))
        .await
        .unwrap();
    log.flush().await.unwrap();

    let source = log.as_replay_source();
    let mut stream = source
        .open(full_request("AAPL", EventKind::Trade))
        .await
        .unwrap();
    let mut got = Vec::new();
    while let Some(ev) = stream.next().await {
        if let MarketEvent::Trade(t) = ev {
            got.push((t.source_ts.0, t.size, t.seq.0));
        }
    }
    // Arrival order preserved; seq is store-canonical and contiguous from 1.
    assert_eq!(got, vec![(100, 1, 1), (250, 2, 2), (399, 3, 3)]);
}

#[tokio::test]
async fn writer_creates_one_shard_per_instrument_kind() {
    let log = SurrealTapLog::open(SurrealTapLogConfig::Memory)
        .await
        .unwrap();
    // Two instruments, one kind each, plus a bar for AAPL → 3 distinct shards.
    log.append(&trade("AAPL", 1, 1, 10.0, 1)).await.unwrap();
    log.append(&trade("MSFT", 2, 2, 20.0, 1)).await.unwrap();
    log.append(&bar("AAPL", 3, 30.0)).await.unwrap();
    log.flush().await.unwrap();

    // A second flush is a clean no-op (no buffered error).
    log.flush().await.unwrap();
}

#[tokio::test]
async fn open_empty_log_replays_nothing() {
    let log = SurrealTapLog::open(SurrealTapLogConfig::Memory)
        .await
        .unwrap();
    let source = log.as_replay_source();
    let mut stream = source
        .open(full_request("AAPL", EventKind::Trade))
        .await
        .unwrap();
    assert!(stream.next().await.is_none());
}

fn quote(symbol: &str, source_ts: i64, rx_ts: i64, bid: f64, ask: f64) -> MarketEvent {
    MarketEvent::Quote(Quote {
        instrument: inst(symbol),
        source_ts: Timestamp(source_ts),
        rx_ts: Timestamp(rx_ts),
        seq: Seq(0),
        bid: Price::from_f64_round(bid),
        bid_size: 1,
        ask: Price::from_f64_round(ask),
        ask_size: 1,
    })
}

#[tokio::test]
async fn replay_preserves_arrival_order_not_source_ts_order() {
    let log = SurrealTapLog::open(SurrealTapLogConfig::Memory)
        .await
        .unwrap();
    // Arrival order: quote@300, trade@200, quote@250 — deliberately NOT sorted
    // by source_ts. Replay must reproduce arrival (seq) order.
    log.append(&quote("AAPL", 300, 1000, 9.0, 11.0))
        .await
        .unwrap();
    log.append(&trade("AAPL", 200, 1001, 10.0, 5))
        .await
        .unwrap();
    log.append(&quote("AAPL", 250, 1002, 9.5, 10.5))
        .await
        .unwrap();
    log.flush().await.unwrap();

    let source = log.as_replay_source();
    let request = ReplayRequest {
        instruments: vec![inst("AAPL")],
        kinds: vec![EventKind::Trade, EventKind::Quote],
        from: Timestamp(i64::MIN),
        to: Timestamp(i64::MAX),
    };
    let mut stream = source.open(request).await.unwrap();
    let mut order = Vec::new();
    while let Some(ev) = stream.next().await {
        match ev {
            MarketEvent::Quote(q) => order.push(("q", q.source_ts.0, q.seq.0)),
            MarketEvent::Trade(t) => order.push(("t", t.source_ts.0, t.seq.0)),
            _ => {}
        }
    }
    // Arrival/seq order, NOT source_ts order (which would be t@200,q@250,q@300).
    assert_eq!(order, vec![("q", 300, 1), ("t", 200, 2), ("q", 250, 3)]);
}

#[tokio::test]
async fn replay_merges_shards_by_seq_across_instruments() {
    let log = SurrealTapLog::open(SurrealTapLogConfig::Memory)
        .await
        .unwrap();
    // Interleave two instruments; each lands in its own shard. Replay must
    // merge them back into global seq order.
    log.append(&trade("AAPL", 10, 10, 1.0, 1)).await.unwrap(); // seq 1
    log.append(&trade("MSFT", 11, 11, 2.0, 1)).await.unwrap(); // seq 2
    log.append(&trade("AAPL", 12, 12, 3.0, 1)).await.unwrap(); // seq 3
    log.append(&trade("MSFT", 13, 13, 4.0, 1)).await.unwrap(); // seq 4
    log.flush().await.unwrap();

    let source = log.as_replay_source();
    let request = ReplayRequest {
        instruments: vec![inst("AAPL"), inst("MSFT")],
        kinds: vec![EventKind::Trade],
        from: Timestamp(i64::MIN),
        to: Timestamp(i64::MAX),
    };
    let mut stream = source.open(request).await.unwrap();
    let mut seqs = Vec::new();
    while let Some(ev) = stream.next().await {
        if let MarketEvent::Trade(t) = ev {
            seqs.push((t.instrument.symbol().to_string(), t.seq.0));
        }
    }
    assert_eq!(
        seqs,
        vec![
            ("AAPL".to_string(), 1),
            ("MSFT".to_string(), 2),
            ("AAPL".to_string(), 3),
            ("MSFT".to_string(), 4),
        ]
    );
}

#[tokio::test]
async fn replay_windows_by_source_ts() {
    let log = SurrealTapLog::open(SurrealTapLogConfig::Memory)
        .await
        .unwrap();
    log.append(&trade("AAPL", 100, 100, 1.0, 1)).await.unwrap();
    log.append(&trade("AAPL", 200, 200, 2.0, 1)).await.unwrap();
    log.append(&trade("AAPL", 300, 300, 3.0, 1)).await.unwrap();
    log.flush().await.unwrap();

    let source = log.as_replay_source();
    let request = ReplayRequest {
        instruments: vec![inst("AAPL")],
        kinds: vec![EventKind::Trade],
        from: Timestamp(150),
        to: Timestamp(300), // half-open: 300 excluded
    };
    let mut stream = source.open(request).await.unwrap();
    let mut tss = Vec::new();
    while let Some(ev) = stream.next().await {
        if let MarketEvent::Trade(t) = ev {
            tss.push(t.source_ts.0);
        }
    }
    assert_eq!(tss, vec![200]);
}

#[tokio::test]
async fn awkward_symbol_round_trips() {
    let log = SurrealTapLog::open(SurrealTapLogConfig::Memory)
        .await
        .unwrap();
    let crypto = Instrument::new(
        ProviderId::from_static("alpaca"),
        AssetClass::Crypto,
        "BTC/USD",
    );
    let ev = MarketEvent::Trade(Trade {
        instrument: crypto.clone(),
        source_ts: Timestamp(1),
        rx_ts: Timestamp(1),
        seq: Seq(0),
        price: Price::from_f64_round(60000.0),
        size: 1,
    });
    log.append(&ev).await.unwrap();
    log.flush().await.unwrap();

    let source = log.as_replay_source();
    let request = ReplayRequest {
        instruments: vec![crypto.clone()],
        kinds: vec![EventKind::Trade],
        from: Timestamp(i64::MIN),
        to: Timestamp(i64::MAX),
    };
    let mut stream = source.open(request).await.unwrap();
    let ev = stream.next().await.expect("one event");
    match ev {
        MarketEvent::Trade(t) => assert_eq!(t.instrument, crypto),
        _ => panic!("expected trade"),
    }
}

#[tokio::test]
async fn embedded_round_trip_persists_and_continues_seq() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kv");
    let cfg = SurrealTapLogConfig::embedded(&path);

    {
        let log = SurrealTapLog::open(cfg.clone()).await.unwrap();
        log.append(&trade("AAPL", 1, 1, 10.0, 1)).await.unwrap(); // seq 1
        log.append(&trade("AAPL", 2, 2, 11.0, 1)).await.unwrap(); // seq 2
        log.flush().await.unwrap();
    }

    // Reopen the same on-disk store; poll until the SurrealKV lock clears
    // (mirrors the cache's embedded test).
    let log = {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            match SurrealTapLog::open(cfg.clone()).await {
                Ok(l) => break l,
                Err(e) if std::time::Instant::now() >= deadline => {
                    panic!("embedded reopen never succeeded within 5s: {e}");
                }
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
            }
        }
    };
    // A new append must continue the seq from the persisted high-water mark.
    log.append(&trade("AAPL", 3, 3, 12.0, 1)).await.unwrap(); // seq 3
    log.flush().await.unwrap();

    let source = log.as_replay_source();
    let mut stream = source
        .open(full_request("AAPL", EventKind::Trade))
        .await
        .unwrap();
    let mut seqs = Vec::new();
    while let Some(ev) = stream.next().await {
        if let MarketEvent::Trade(t) = ev {
            seqs.push(t.seq.0);
        }
    }
    assert_eq!(seqs, vec![1, 2, 3], "seq continues across reopen, no reset");
}

#[tokio::test]
async fn replay_empty_when_window_is_degenerate() {
    let log = SurrealTapLog::open(SurrealTapLogConfig::Memory)
        .await
        .unwrap();
    log.append(&trade("AAPL", 100, 100, 1.0, 1)).await.unwrap();
    log.flush().await.unwrap();

    let source = log.as_replay_source();
    // from == to and from > to both yield nothing.
    for (from, to) in [(100, 100), (300, 200)] {
        let request = ReplayRequest {
            instruments: vec![inst("AAPL")],
            kinds: vec![EventKind::Trade],
            from: Timestamp(from),
            to: Timestamp(to),
        };
        let mut stream = source.open(request).await.unwrap();
        assert!(
            stream.next().await.is_none(),
            "degenerate window [{from}, {to}) must replay nothing"
        );
    }
}

#[tokio::test]
async fn replay_empty_kinds_matches_all_kinds() {
    let log = SurrealTapLog::open(SurrealTapLogConfig::Memory)
        .await
        .unwrap();
    log.append(&trade("AAPL", 100, 100, 1.0, 1)).await.unwrap(); // seq 1
    log.append(&bar("AAPL", 200, 5.0)).await.unwrap(); // seq 2
    log.flush().await.unwrap();

    let source = log.as_replay_source();
    let request = ReplayRequest {
        instruments: vec![inst("AAPL")],
        kinds: vec![], // empty = match all kinds
        from: Timestamp(i64::MIN),
        to: Timestamp(i64::MAX),
    };
    let mut stream = source.open(request).await.unwrap();
    let mut seen = Vec::new();
    while let Some(ev) = stream.next().await {
        match ev {
            MarketEvent::Trade(t) => seen.push(("trade", t.seq.0)),
            MarketEvent::Bar(b) => seen.push(("bar", b.seq.0)),
            _ => {}
        }
    }
    assert_eq!(seen, vec![("trade", 1), ("bar", 2)]);
}
