//! Real-Alpaca smoke tests. Marked `#[ignore]` so they don't run in CI;
//! invoke explicitly with `cargo test --test alpaca_real -- --ignored`.
//!
//! These tests require Alpaca credentials in the environment:
//!   ALPACA_PAPER_API_KEY_ID
//!   ALPACA_PAPER_API_SECRET_KEY
//!
//! They use the synthetic `Test` streaming feed (available outside market
//! hours) so the assertions don't depend on market state.

#![cfg(feature = "provider-alpaca")]

use std::time::Duration;

use datamancer::providers::{AlpacaProvider, AlpacaProviderConfig, AlpacaStreamFeed};
use datamancer::{
    Datamancer, EventKind, LiveConfig, MarketEvent, Subscription,
};
use futures::StreamExt;
use oxidized_alpaca::AccountType;

#[tokio::test]
#[ignore]
async fn live_test_feed_yields_an_event() {
    let provider = AlpacaProvider::new(AlpacaProviderConfig {
        account_type: AccountType::Paper,
        stream_feed: AlpacaStreamFeed::Test,
        ..Default::default()
    });
    let dm = Datamancer::builder()
        .provider(Box::new(provider))
        .build()
        .unwrap();
    let mut session = dm
        .live(LiveConfig {
            initial_subscriptions: vec![Subscription::new("FAKEPACA", [EventKind::Trade])],
            buffer_size: 64,
            ..Default::default()
        })
        .await
        .expect("session open");

    let mut stream = session.take_events().unwrap();

    // Wait up to 30 seconds for any event (control or data).
    let ev = tokio::time::timeout(Duration::from_secs(30), stream.next())
        .await
        .expect("got an event in time")
        .expect("stream not closed");
    eprintln!("first event: {ev:?}");
    assert!(matches!(
        ev,
        MarketEvent::Trade(_) | MarketEvent::Quote(_) | MarketEvent::Bar(_) | MarketEvent::Control(_)
    ));
    let _ = session.close().await;
}
