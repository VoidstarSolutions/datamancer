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
