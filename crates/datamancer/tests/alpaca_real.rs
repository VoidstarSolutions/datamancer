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
    Adjustment, AssetClass, BarInterval, Datamancer, DisconnectCause, EventKind, HistoryRequest,
    Instrument, MarketEvent, PersistenceOptions, Provider, ProviderId, Scope, Timestamp,
};
use futures::StreamExt;
use oxidized_alpaca::AccountType;

#[tokio::test]
#[ignore = "requires real Alpaca credentials; invoke with `cargo test --test alpaca_real -- --ignored`"]
async fn live_test_feed_yields_an_event() {
    let provider = AlpacaProvider::new(AlpacaProviderConfig {
        settings: SettingsSource::Static(AlpacaSettings {
            account_type: AccountType::Paper,
            ..Default::default()
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
            Scope::live(),
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
            ..Default::default()
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
            Scope::live(),
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

/// Paper keys must authenticate on the **IEX** stream. `oxidized_alpaca`
/// routes `AccountType::Paper` to the Broker-API sandbox host, which rejects
/// ordinary paper keys with `402 auth failed`; the provider overrides the
/// host choice. The `Test` feed never exposed this (it has no sandbox URL),
/// so this test uses the real IEX endpoint. Outside market hours no data
/// flows, but the connect/auth handshake still completes, so the assertion is
/// on the first control: `ProviderConnected`, never an `Unauthenticated`
/// disconnect. Drives `start_live` directly, as the bad-credentials test does.
#[tokio::test]
#[ignore = "requires real Alpaca paper credentials; invoke with `cargo test --test alpaca_real -- --ignored`"]
async fn paper_keys_authenticate_on_the_iex_stream() {
    // `oxidized_alpaca` collapses every rejected auth response into a
    // payload-less `StreamingAuth` and logs the server's envelope (402 auth
    // failed vs 406 connection limit exceeded) at `error!`; surface that log
    // so a failure here is diagnosable. `RUST_LOG=error` is enough.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let provider = AlpacaProvider::new(AlpacaProviderConfig {
        settings: SettingsSource::Static(AlpacaSettings {
            account_type: AccountType::Paper,
            ..Default::default()
        }),
        stream_feed: AlpacaStreamFeed::Iex,
        ..Default::default()
    });

    let (sink, mut events) = tokio::sync::mpsc::channel(32);
    let handle = provider.start_live(sink).await.expect("start_live");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let control = loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let ev = tokio::time::timeout(remaining, events.recv())
            .await
            .expect("timed out waiting for the connect control")
            .expect("event channel closed before a control arrived");
        eprintln!("event: {ev:?}");
        if let MarketEvent::Control(c) = ev {
            break c;
        }
    };

    match control.kind {
        datamancer::ControlKind::ProviderConnected { provider } => assert_eq!(provider, "alpaca"),
        other => panic!("expected ProviderConnected on the IEX stream, got {other:?}"),
    }

    let _ = handle.close().await;
}

/// Two live sessions on one provider share one websocket. Before the hub,
/// each authoritative `(instrument, kind)` session opened its own socket and
/// Alpaca refused the second with `406 connection limit exceeded`, which the
/// provider then mistook for rejected credentials and parked — so trades and
/// quotes on one symbol, or two symbols, could never stream together. Opens
/// FAKEPACA Trade and FAKEPACA Quote on the `Test` feed through
/// `Datamancer::session` (two authoritative sessions) and requires a trade on
/// the first and a quote on the second, with no `Unauthenticated` disconnect
/// on either.
#[tokio::test]
#[ignore = "requires real Alpaca credentials; invoke with `cargo test --test alpaca_real -- --ignored`"]
async fn two_live_sessions_share_one_stream_connection() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let provider = AlpacaProvider::new(AlpacaProviderConfig {
        settings: SettingsSource::Static(AlpacaSettings {
            account_type: AccountType::Paper,
            ..Default::default()
        }),
        stream_feed: AlpacaStreamFeed::Test,
        ..Default::default()
    });
    let dm = Datamancer::builder()
        .provider(Box::new(provider))
        .build()
        .unwrap();
    let fakepaca = || {
        Instrument::new(
            ProviderId::from_static("alpaca"),
            AssetClass::Equity,
            "FAKEPACA",
        )
    };
    let open = |kind| dm.session(fakepaca(), kind, Scope::live(), PersistenceOptions::none());
    let trades = open(EventKind::Trade).await.expect("trade session open");
    let quotes = open(EventKind::Quote).await.expect("quote session open");
    let mut trade_stream = trades.take_events().await.unwrap();
    let mut quote_stream = quotes.take_events().await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_mins(1);
    let (mut got_trade, mut got_quote) = (false, false);
    while !(got_trade && got_quote) {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let ev = tokio::select! {
            ev = trade_stream.next() => ev,
            ev = quote_stream.next() => ev,
            () = tokio::time::sleep(remaining) => {
                panic!("timed out: trade={got_trade} quote={got_quote}");
            }
        }
        .expect("stream closed early");
        eprintln!("event: {ev:?}");
        match ev {
            MarketEvent::Trade(t) => {
                assert_eq!(t.instrument.symbol(), "FAKEPACA");
                got_trade = true;
            }
            MarketEvent::Quote(q) => {
                assert_eq!(q.instrument.symbol(), "FAKEPACA");
                got_quote = true;
            }
            MarketEvent::Control(c) => {
                if let datamancer::ControlKind::ProviderDisconnected { cause, reason, .. } = &c.kind
                {
                    assert_ne!(
                        *cause,
                        DisconnectCause::Unauthenticated,
                        "second session must not be refused as a credential rejection: {reason}"
                    );
                }
            }
            _ => {}
        }
    }

    let _ = trades.close().await;
    let _ = quotes.close().await;
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
            ..Default::default()
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

/// History smoke test on the free data plan: daily AAPL bars for a window
/// ending **now**. With no feed named, Alpaca defaults to SIP and a free plan
/// answers `403 subscription does not permit querying recent SIP data` for
/// any window reaching into the last fifteen minutes — the empty-chart bug
/// (and the history endpoints reject `delayed_sip` with `400 invalid feed`,
/// so IEX is the only free-plan option). The default `history_feed` (IEX for
/// an IEX stream) must make this fetch succeed and return bars. A paid
/// (unlimited) plan passes trivially.
#[tokio::test]
#[ignore = "requires real Alpaca credentials; invoke with `cargo test --test alpaca_real -- --ignored`"]
async fn recent_daily_bars_fetch_on_the_default_history_feed() {
    let provider = AlpacaProvider::new(AlpacaProviderConfig::default());
    let now_nanos = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    )
    .unwrap();
    let thirty_days = 30 * 24 * 60 * 60 * 1_000_000_000_i64;
    let request = HistoryRequest {
        instrument: Instrument::new(
            ProviderId::from_static("alpaca"),
            AssetClass::Equity,
            "AAPL",
        ),
        kind: EventKind::Bar(BarInterval::OneDay),
        from: Timestamp(now_nanos - thirty_days),
        to: Timestamp(now_nanos),
        adjustment: Adjustment::All,
    };
    let (sink, mut events) = tokio::sync::mpsc::channel(4096);
    provider
        .fetch_history(request, sink)
        .await
        .expect("daily bars ending now must be served on the default history feed");
    let mut bars = 0usize;
    while let Some(ev) = events.recv().await {
        match ev {
            MarketEvent::Bar(b) => {
                assert_eq!(b.instrument.symbol(), "AAPL");
                assert_eq!(b.interval, BarInterval::OneDay);
                bars += 1;
            }
            other => panic!("expected only bars, got {other:?}"),
        }
    }
    eprintln!("daily bars in the last 30 days: {bars}");
    assert!(bars >= 15, "expected a month of trading days, got {bars}");
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
    let symbols: std::collections::HashSet<&str> = instruments
        .iter()
        .map(|entry| entry.instrument.symbol())
        .collect();
    for expected in ["AAPL", "MSFT", "SPY"] {
        assert!(
            symbols.contains(expected),
            "expected {expected} in the Alpaca equity catalog"
        );
    }
    // Every returned instrument should be stamped with our provider id and
    // (for the equity surface) the Equity asset class.
    for entry in &instruments {
        assert_eq!(entry.instrument.provider().as_str(), "alpaca");
        assert_eq!(entry.instrument.asset_class(), AssetClass::Equity);
    }
}
