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
    AlpacaCredentials, AlpacaCryptoProvider, AlpacaCryptoProviderConfig, AlpacaProvider,
    AlpacaProviderConfig, AlpacaSettings, AlpacaStreamFeed, CredentialsSource, SettingsSource,
};
use datamancer::{
    AssetClass, Datamancer, DisconnectCause, EventKind, Instrument, MarketEvent,
    PersistenceOptions, Provider, ProviderId, Scope,
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

/// Bad-credentials smoke test: connecting the streaming client with a
/// deliberately-invalid explicit key pair must land as an in-band
/// `ProviderDisconnected { cause: Unauthenticated, .. }` control — the
/// `Error::StreamingAuth` classification wired up for oxidized-alpaca 0.0.9's
/// "return `StreamingAuth` on rejected market-data credentials" fix. Uses the
/// synthetic `Test` feed's connect/auth path; Alpaca rejects the bad key
/// pair before any market data would flow, so this doesn't depend on market
/// hours or the feed's synthetic tick cadence.
///
/// Drives `Provider::start_live` directly (bypassing `Datamancer::session`):
/// with a `Static` credential source the streaming task can't park for a
/// rotation on auth rejection (nothing will ever rotate it), so it falls
/// straight into the backoff-retry loop. A `session()`-issued subscribe sent
/// while that first backoff sleep is in flight would race and fail fast with
/// "provider is reconnecting" — the `ProviderDisconnected` control the test
/// wants to observe doesn't depend on a subscription existing at all.
#[tokio::test]
#[ignore = "requires network access to Alpaca (but not valid credentials); invoke with `cargo test --test alpaca_real -- --ignored`"]
async fn bad_credentials_yield_unauthenticated_disconnect() {
    let provider = AlpacaProvider::new(AlpacaProviderConfig {
        settings: SettingsSource::Static(AlpacaSettings {
            account_type: AccountType::Paper,
        }),
        stream_feed: AlpacaStreamFeed::Test,
        credentials: CredentialsSource::Static(AlpacaCredentials {
            key_id: "definitely-not-a-real-key-id".to_string(),
            secret: "definitely-not-a-real-secret".to_string(),
        }),
        ..Default::default()
    });

    let (sink, mut events) = tokio::sync::mpsc::channel(32);
    let handle = provider.start_live(sink).await.expect("start_live");

    // Generous timeout: this only needs one failed connect round-trip, but
    // gives room for network latency.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let control = loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let ev = tokio::time::timeout(remaining, events.recv())
            .await
            .expect("timed out waiting for a ProviderDisconnected control")
            .expect("event channel closed before a control arrived");
        eprintln!("event: {ev:?}");
        if let MarketEvent::Control(c) = ev {
            break c;
        }
    };

    match control.kind {
        datamancer::ControlKind::ProviderDisconnected { cause, reason, .. } => {
            assert_eq!(cause, DisconnectCause::Unauthenticated, "reason={reason:?}");
        }
        other => panic!("expected ProviderDisconnected, got {other:?}"),
    }

    let _ = handle.close().await;
}

/// Latest-value smoke test (stock): `Provider::latest` against Alpaca's real
/// stock-snapshot surface — the live-seed source. `stock_snapshot` returns a
/// single snapshot (no symbol-keyed map), so this just confirms the snapshot
/// path decodes and maps to the requested `kind`. Off-hours the snapshot's
/// `latest_trade` can be absent, so a `None` is acceptable; we only require the
/// call to succeed and, when present, to be a `Trade`.
#[tokio::test]
#[ignore = "requires real Alpaca credentials; invoke with `cargo test --test alpaca_real -- --ignored`"]
async fn stock_latest_returns_snapshot_event() {
    let provider = AlpacaProvider::new(AlpacaProviderConfig::default());
    let inst = Instrument::new(
        ProviderId::from_static("alpaca"),
        AssetClass::Equity,
        "AAPL",
    );
    let got = provider
        .latest(&inst, EventKind::Trade)
        .await
        .expect("latest() succeeds with real credentials");
    eprintln!("stock latest(AAPL, Trade) = {got:?}");
    if let Some(ev) = got {
        assert!(
            matches!(ev, MarketEvent::Trade(_)),
            "latest(Trade) must map to a Trade, got {ev:?}"
        );
    }
}

/// Latest-value smoke test (crypto): `Provider::latest` against Alpaca's real
/// crypto-snapshot surface. Unlike the stock path this goes through
/// `crypto_snapshots(&[symbol], ..)`, which returns a **symbol-keyed map** the
/// impl indexes with `snaps.remove(symbol)`. If Alpaca ever keyed the response
/// by a normalized symbol (e.g. dropping the `/`), the lookup would silently
/// miss and the seed would never fire — so this asserts a real `Some` for a
/// 24/7 pair, directly guarding that key convention.
#[tokio::test]
#[ignore = "requires real Alpaca credentials; invoke with `cargo test --test alpaca_real -- --ignored`"]
async fn crypto_latest_returns_snapshot_event() {
    let provider = AlpacaCryptoProvider::new(AlpacaCryptoProviderConfig::default());
    let inst = Instrument::new(
        ProviderId::from_static("alpaca-crypto"),
        AssetClass::Crypto,
        "BTC/USD",
    );
    let got = provider
        .latest(&inst, EventKind::Trade)
        .await
        .expect("latest() succeeds with real credentials");
    eprintln!("crypto latest(BTC/USD, Trade) = {got:?}");
    let ev = got.expect(
        "crypto snapshot for a 24/7 pair should yield a latest trade; a None here likely means \
         the response map key no longer matches the requested symbol",
    );
    assert!(
        matches!(ev, MarketEvent::Trade(_)),
        "latest(Trade) must map to a Trade, got {ev:?}"
    );
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
