//! Real-Alpaca smoke tests. Marked `#[ignore]` so they don't run in CI;
//! invoke explicitly with `cargo test --test alpaca_real -- --ignored`.
//!
//! These tests require Alpaca credentials in the environment:
//!   `ALPACA_PAPER_API_KEY_ID`
//!   `ALPACA_PAPER_API_SECRET_KEY`
//!
//! They use the synthetic `Test` streaming feed (available outside market
//! hours) so the assertions don't depend on market state.

#![cfg(feature = "provider-alpaca")]

use std::time::Duration;

use datamancer::providers::{
    AlpacaProvider, AlpacaProviderConfig, AlpacaSettings, AlpacaStreamFeed, SettingsSource,
};
use datamancer::{
    AssetClass, Datamancer, EventKind, Instrument, MarketEvent, PersistenceOptions, Provider,
    ProviderId, Scope,
};
use futures::StreamExt;
use oxidized_alpaca::AccountType;

#[tokio::test]
#[ignore = "requires real Alpaca credentials; invoke with `cargo test --test alpaca_real -- --ignored`"]
async fn live_test_feed_yields_an_event() {
    let provider = AlpacaProvider::new(AlpacaProviderConfig {
        settings: SettingsSource::Static(AlpacaSettings {
            account_type: AccountType::Paper,
        }),
        stream_feed: AlpacaStreamFeed::Test,
        ..Default::default()
    });
    let dm = Datamancer::builder()
        .provider(Box::new(provider))
        .build()
        .unwrap();
    let session = dm
        .session(
            Instrument::new(
                ProviderId::from_static("alpaca"),
                AssetClass::Equity,
                "FAKEPACA",
            ),
            EventKind::Trade,
            Scope::Live {
                backfill_from: None,
            },
            PersistenceOptions::none(),
        )
        .await
        .expect("session open");

    let mut stream = session.take_events().await.unwrap();

    // Wait up to 30 seconds for any event (control or data).
    let ev = tokio::time::timeout(Duration::from_secs(30), stream.next())
        .await
        .expect("got an event in time")
        .expect("stream not closed");
    eprintln!("first event: {ev:?}");
    assert!(matches!(
        ev,
        MarketEvent::Trade(_)
            | MarketEvent::Quote(_)
            | MarketEvent::Bar(_)
            | MarketEvent::Control(_)
    ));
    let _ = session.close().await;
}

/// Stronger smoke test: drain past the connect/subscribe control events and
/// wait for a real synthetic trade on FAKEPACA. Validates that the full
/// pipeline (websocket → translation → seq assignment → output stream) is
/// delivering decoded data, not just connectivity controls.
#[tokio::test]
#[ignore = "requires real Alpaca credentials; invoke with `cargo test --test alpaca_real -- --ignored`"]
async fn live_test_feed_delivers_a_trade() {
    let provider = AlpacaProvider::new(AlpacaProviderConfig {
        settings: SettingsSource::Static(AlpacaSettings {
            account_type: AccountType::Paper,
        }),
        stream_feed: AlpacaStreamFeed::Test,
        ..Default::default()
    });
    let dm = Datamancer::builder()
        .provider(Box::new(provider))
        .build()
        .unwrap();
    let session = dm
        .session(
            Instrument::new(
                ProviderId::from_static("alpaca"),
                AssetClass::Equity,
                "FAKEPACA",
            ),
            EventKind::Trade,
            Scope::Live {
                backfill_from: None,
            },
            PersistenceOptions::none(),
        )
        .await
        .expect("session open");

    let mut stream = session.take_events().await.unwrap();

    // Allow up to 60s overall for at least one Trade. The test feed emits
    // synthetic FAKEPACA trades roughly once per second, so this is generous.
    let deadline = tokio::time::Instant::now() + Duration::from_mins(1);
    let mut control_count: usize = 0;
    let mut last_seq: Option<u64> = None;

    let trade = loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let ev = tokio::time::timeout(remaining, stream.next())
            .await
            .expect("timed out waiting for a Trade")
            .expect("stream closed before a Trade arrived");

        // Sanity-check session-monotonic seq while we're here.
        if let Some(seq) = match &ev {
            MarketEvent::Trade(t) => Some(t.seq.0),
            MarketEvent::Quote(q) => Some(q.seq.0),
            MarketEvent::Bar(b) => Some(b.seq.0),
            MarketEvent::Control(c) => Some(c.seq.0),
            _ => None,
        } {
            if let Some(prev) = last_seq {
                assert_eq!(seq, prev + 1, "seq must be strictly monotonic");
            }
            last_seq = Some(seq);
        }

        match ev {
            MarketEvent::Control(c) => {
                control_count += 1;
                eprintln!("control #{control_count}: {:?}", c.kind);
            }
            MarketEvent::Trade(t) => break t,
            other => eprintln!("unexpected non-trade event: {other:?}"),
        }
    };

    eprintln!("first trade: {trade:?}");
    assert_eq!(trade.instrument.symbol(), "FAKEPACA");
    assert!(
        trade.size > datamancer::Quantity::ZERO,
        "trade size should be positive"
    );
    assert!(
        trade.price.raw() > 0,
        "trade price should be positive in raw units"
    );
    assert!(
        trade.source_ts.0 > 0,
        "trade source_ts should be a real wall-clock timestamp"
    );
    // rx_ts should be at-or-after source_ts (we capture rx_ts on the local
    // side after the bytes arrive).
    assert!(
        trade.rx_ts.0 >= trade.source_ts.0,
        "rx_ts ({}) should be ≥ source_ts ({})",
        trade.rx_ts.0,
        trade.source_ts.0
    );

    let _ = session.close().await;
}

/// Reference-data smoke test: `list_instruments` against Alpaca's real
/// `/v2/assets` surface. Doesn't open a session; just exercises the catalog
/// path. Looks for a few well-known tickers that have been tradable on
/// Alpaca for years (AAPL, MSFT, SPY) — these can vanish in theory but in
/// practice are stable enough for a smoke check.
#[tokio::test]
#[ignore = "requires real Alpaca credentials; invoke with `cargo test --test alpaca_real -- --ignored`"]
async fn list_instruments_returns_known_symbols() {
    let provider = AlpacaProvider::new(AlpacaProviderConfig::default());
    let instruments = provider
        .list_instruments()
        .await
        .expect("list_instruments succeeds with real credentials");
    eprintln!("alpaca returned {} tradable equities", instruments.len());
    assert!(
        instruments.len() > 1000,
        "expected a sizable equity catalog, got {}",
        instruments.len()
    );
    let symbols: std::collections::HashSet<&str> =
        instruments.iter().map(Instrument::symbol).collect();
    for expected in ["AAPL", "MSFT", "SPY"] {
        assert!(
            symbols.contains(expected),
            "expected {expected} in the Alpaca equity catalog"
        );
    }
    // Every returned instrument should be stamped with our provider id and
    // (for the equity surface) the Equity asset class.
    for i in &instruments {
        assert_eq!(i.provider().as_str(), "alpaca");
        assert_eq!(i.asset_class(), AssetClass::Equity);
    }
}
