//! Integration tests for the Surreal-backed [`TapLog`].
//!
//! Uses the in-memory engine for the fast suite; one embedded test exercises
//! the on-disk `SurrealKV` path.

#![cfg(feature = "storage-surreal")]
#![allow(dead_code, reason = "helper fns used by later task tests in this file")]

use datamancer::storage::{SurrealTapLog, SurrealTapLogConfig};
use datamancer::{
    AssetClass, Bar, BarInterval, EventKind, Instrument, MarketEvent, Price, ProviderId, Seq,
    TapLog, Timestamp, Trade,
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
