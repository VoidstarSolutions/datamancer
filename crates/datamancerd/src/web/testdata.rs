//! Shared fixtures for the web-module unit tests: a known two-symbol snapshot
//! exercising per-symbol framing (distinct `seq` positions per instrument).

#![cfg(test)]

use datamancer::{
    Adjustment, AssetClass, AuthoritativeSessionSnapshot, BarInterval, CacheCatalogEntry,
    CacheSnapshot, ClientSessionId, ClientSessionSnapshot, ConnectionState, EventKind, GapSpan,
    Instrument, ProviderId, ProviderSnapshot, ResumeBufferSnapshot, Seq, SubscriptionRef,
    SystemSnapshot, Timestamp,
};

/// Two distinct instruments under one provider.
#[must_use]
pub(crate) fn aapl() -> Instrument {
    Instrument::new(
        ProviderId::from_static("alpaca"),
        AssetClass::Equity,
        "AAPL",
    )
}

#[must_use]
pub(crate) fn msft() -> Instrument {
    Instrument::new(
        ProviderId::from_static("alpaca"),
        AssetClass::Equity,
        "MSFT",
    )
}

/// A known snapshot with two per-symbol authoritative sessions (distinct `seq`),
/// one provider, one cache entry, and one client session.
#[must_use]
pub(crate) fn snapshot() -> SystemSnapshot {
    let providers = vec![
        ProviderSnapshot::new(
            ProviderId::from_static("alpaca"),
            ConnectionState::Connected,
            3,
            1,
            2,
            4,
            1,
            0,
            99,
            1,
            None,
        )
        .with_bytes(Some(4096))
        .with_rate_limit_hits(Some(2)),
    ];

    let cache = CacheSnapshot::new(
        vec![
            CacheCatalogEntry::new(
                ProviderId::from_static("alpaca"),
                "AAPL".to_string(),
                EventKind::Bar(BarInterval::OneMinute),
                Adjustment::All,
                vec![GapSpan {
                    from_source_ts: Timestamp(0),
                    to_source_ts: Timestamp(100),
                }],
                42,
            )
            .with_asset_class(Some(AssetClass::Equity))
            .with_est_bytes(Some(560)),
        ],
        Some(8192),
    );

    let authoritative = vec![
        AuthoritativeSessionSnapshot::new(aapl(), EventKind::Trade, 2, 0)
            .with_seq_position(Some(Seq(7)))
            .with_timestamps(Some(Timestamp(100)), Some(Timestamp(130))),
        AuthoritativeSessionSnapshot::new(msft(), EventKind::Trade, 1, 0)
            .with_seq_position(Some(Seq(12)))
            .with_timestamps(Some(Timestamp(200)), Some(Timestamp(205))),
    ];

    let clients = vec![ClientSessionSnapshot::new(
        ClientSessionId(1),
        vec![SubscriptionRef {
            instrument: aapl(),
            kind: EventKind::Trade,
        }],
        ResumeBufferSnapshot::new(1024, 3, 0),
    )];

    SystemSnapshot::new(
        Timestamp(1_700_000_000),
        providers,
        cache,
        authoritative,
        clients,
    )
}
