//! Integration tests for the Turso-backed [`TapLog`].
//!
//! Full parity suite ported from the retired tap-log parity suite, exercising the
//! `TursoTapReplaySource` implemented in Task 7.

#![cfg(feature = "storage-turso")]

use datamancer::storage::{TursoTapLog, TursoTapLogConfig};
use datamancer::{
    AssetClass, Instrument, MarketEvent, Price, ProviderId, Quantity, Seq, TapLog, Timestamp, Trade,
};

fn inst(symbol: &str) -> Instrument {
    Instrument::new(
        ProviderId::from_static("alpaca"),
        AssetClass::Equity,
        symbol,
    )
}

// The tap log persists the source `seq` verbatim (it no longer mints its own),
// so test inputs carry the `seq` the controller would have stamped, and replay
// reproduces exactly that value.
fn trade(symbol: &str, source_ts: i64, rx_ts: i64, seq: u64, price: f64, size: u64) -> MarketEvent {
    MarketEvent::Trade(Trade {
        instrument: inst(symbol),
        source_ts: Timestamp(source_ts),
        rx_ts: Timestamp(rx_ts),
        seq: Seq(seq),
        price: Price::from_f64_round(price),
        // `size` is whole shares (fractional sizes are covered explicitly by
        // `awkward_symbol_round_trips` below).
        size: Quantity::from_units(size),
    })
}

#[tokio::test]
async fn append_then_flush_reports_ok() {
    let log = TursoTapLog::open(TursoTapLogConfig::Memory).await.unwrap();
    log.append(&trade("AAPL", 100, 100, 0, 150.10, 1))
        .await
        .unwrap();
    log.flush().await.unwrap();
}

use datamancer::{Bar, BarInterval, EventKind, Quote, ReplayRequest};
use futures::StreamExt;

fn bar(symbol: &str, source_ts: i64, seq: u64, close: f64) -> MarketEvent {
    MarketEvent::Bar(Bar {
        instrument: inst(symbol),
        interval: BarInterval::OneMinute,
        source_ts: Timestamp(source_ts),
        rx_ts: Timestamp(source_ts),
        seq: Seq(seq),
        open: Price::from_f64_round(close),
        high: Price::from_f64_round(close),
        low: Price::from_f64_round(close),
        close: Price::from_f64_round(close),
        volume: Quantity::from_units(100),
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

/// Open `req` against `source` and count the events it yields.
async fn replay_count(source: &dyn datamancer_core::ReplaySource, req: ReplayRequest) -> usize {
    let mut stream = source.open(req).await.unwrap();
    let mut n = 0;
    while stream.next().await.is_some() {
        n += 1;
    }
    n
}

#[tokio::test]
async fn append_then_flush_persists_and_replays_in_order() {
    let log = TursoTapLog::open(TursoTapLogConfig::Memory).await.unwrap();
    log.append(&trade("AAPL", 100, 100, 0, 150.10, 1))
        .await
        .unwrap();
    log.append(&trade("AAPL", 250, 250, 1, 150.25, 2))
        .await
        .unwrap();
    log.append(&trade("AAPL", 399, 399, 2, 150.40, 3))
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
    // Arrival order preserved; the source `seq` is persisted verbatim.
    assert_eq!(
        got,
        vec![
            (100, Quantity::from_units(1), 0),
            (250, Quantity::from_units(2), 1),
            (399, Quantity::from_units(3), 2),
        ]
    );
}

#[tokio::test]
async fn writer_creates_one_shard_per_instrument_kind() {
    let log = TursoTapLog::open(TursoTapLogConfig::Memory).await.unwrap();
    // Two instruments, one kind each, plus a bar for AAPL → 3 distinct shards.
    log.append(&trade("AAPL", 1, 1, 0, 10.0, 1)).await.unwrap();
    log.append(&trade("MSFT", 2, 2, 1, 20.0, 1)).await.unwrap();
    log.append(&bar("AAPL", 3, 2, 30.0)).await.unwrap();
    log.flush().await.unwrap();

    // A second flush is a clean no-op (no buffered error).
    log.flush().await.unwrap();

    // Each (instrument, kind) is isolated in its own shard: replaying one pair
    // returns exactly that pair's events and nothing from the others.
    let source = log.as_replay_source();
    assert_eq!(
        replay_count(&*source, full_request("AAPL", EventKind::Trade)).await,
        1
    );
    assert_eq!(
        replay_count(&*source, full_request("MSFT", EventKind::Trade)).await,
        1
    );
    assert_eq!(
        replay_count(
            &*source,
            full_request("AAPL", EventKind::Bar(BarInterval::OneMinute))
        )
        .await,
        1
    );
}

#[tokio::test]
async fn open_empty_log_replays_nothing() {
    let log = TursoTapLog::open(TursoTapLogConfig::Memory).await.unwrap();
    let source = log.as_replay_source();
    let mut stream = source
        .open(full_request("AAPL", EventKind::Trade))
        .await
        .unwrap();
    assert!(stream.next().await.is_none());
}

fn quote(symbol: &str, source_ts: i64, rx_ts: i64, seq: u64, bid: f64, ask: f64) -> MarketEvent {
    MarketEvent::Quote(Quote {
        instrument: inst(symbol),
        source_ts: Timestamp(source_ts),
        rx_ts: Timestamp(rx_ts),
        seq: Seq(seq),
        bid: Price::from_f64_round(bid),
        bid_size: Quantity::from_units(1),
        ask: Price::from_f64_round(ask),
        ask_size: Quantity::from_units(1),
    })
}

#[tokio::test]
async fn replay_preserves_arrival_order_not_source_ts_order() {
    let log = TursoTapLog::open(TursoTapLogConfig::Memory).await.unwrap();
    // Arrival order: quote@300, trade@200, quote@250 — deliberately NOT sorted
    // by source_ts. Replay must reproduce arrival (seq) order.
    log.append(&quote("AAPL", 300, 1000, 0, 9.0, 11.0))
        .await
        .unwrap();
    log.append(&trade("AAPL", 200, 1001, 1, 10.0, 5))
        .await
        .unwrap();
    log.append(&quote("AAPL", 250, 1002, 2, 9.5, 10.5))
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
    assert_eq!(order, vec![("q", 300, 0), ("t", 200, 1), ("q", 250, 2)]);
}

#[tokio::test]
async fn replay_merges_shards_by_seq_across_instruments() {
    let log = TursoTapLog::open(TursoTapLogConfig::Memory).await.unwrap();
    // Interleave two instruments; each lands in its own shard. Replay must
    // merge them back into global seq order.
    log.append(&trade("AAPL", 10, 10, 1, 1.0, 1)).await.unwrap();
    log.append(&trade("MSFT", 11, 11, 2, 2.0, 1)).await.unwrap();
    log.append(&trade("AAPL", 12, 12, 3, 3.0, 1)).await.unwrap();
    log.append(&trade("MSFT", 13, 13, 4, 4.0, 1)).await.unwrap();
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
    let log = TursoTapLog::open(TursoTapLogConfig::Memory).await.unwrap();
    log.append(&trade("AAPL", 100, 100, 0, 1.0, 1))
        .await
        .unwrap();
    log.append(&trade("AAPL", 200, 200, 1, 2.0, 1))
        .await
        .unwrap();
    log.append(&trade("AAPL", 300, 300, 2, 3.0, 1))
        .await
        .unwrap();
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
    let log = TursoTapLog::open(TursoTapLogConfig::Memory).await.unwrap();
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
        // 0.004 BTC — a fractional size must survive the tap-log round trip
        // through the renamed `size_raw` column, not truncate to zero.
        size: Quantity::from_f64_round(0.004),
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
        MarketEvent::Trade(t) => {
            assert_eq!(t.instrument, crypto);
            assert_eq!(t.size, Quantity::from_raw(4_000_000));
        }
        _ => panic!("expected trade"),
    }
}

#[tokio::test]
async fn embedded_round_trip_persists_and_continues_seq() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kv");
    let cfg = TursoTapLogConfig::embedded(&path);

    {
        let log = TursoTapLog::open(cfg.clone()).await.unwrap();
        log.append(&trade("AAPL", 1, 1, 1, 10.0, 1)).await.unwrap(); // seq 1
        log.append(&trade("AAPL", 2, 2, 2, 11.0, 1)).await.unwrap(); // seq 2
        log.flush().await.unwrap();
    }

    // Reopen the same on-disk store.
    let log = TursoTapLog::open(cfg.clone()).await.unwrap();
    // A post-reopen append persists its own source seq; the shard registry and
    // earlier rows survive the reopen.
    log.append(&trade("AAPL", 3, 3, 3, 12.0, 1)).await.unwrap(); // seq 3
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
    assert_eq!(
        seqs,
        vec![1, 2, 3],
        "persisted source seqs survive reopen, no reset"
    );
}

#[tokio::test]
async fn replay_empty_when_window_is_degenerate() {
    let log = TursoTapLog::open(TursoTapLogConfig::Memory).await.unwrap();
    log.append(&trade("AAPL", 100, 100, 0, 1.0, 1))
        .await
        .unwrap();
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
    let log = TursoTapLog::open(TursoTapLogConfig::Memory).await.unwrap();
    log.append(&trade("AAPL", 100, 100, 1, 1.0, 1))
        .await
        .unwrap(); // seq 1
    log.append(&bar("AAPL", 200, 2, 5.0)).await.unwrap(); // seq 2
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
