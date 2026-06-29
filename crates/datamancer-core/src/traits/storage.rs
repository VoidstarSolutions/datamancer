//! Persistence trait surface.
//!
//! Three concerns, three traits:
//!
//! - [`TapLog`] — append-only record of every event a live session emits, so
//!   the consumer's experience can be replayed bit-for-bit.
//! - [`HistoricalCache`] — keyed read/write store of historical fetches, so
//!   re-running a research job does not re-hit the upstream provider.
//! - [`ReplaySource`] — anything that can be opened as an ordered event
//!   stream. Both a tap log and a historical cache implement this; so does a
//!   future direct-from-provider replay.
//!
//! All three are `dyn`-friendly. None are wired up yet — the API is stubbed
//! out so the session and configuration layers can take them as parameters
//! without locking in a particular backend.

use async_trait::async_trait;
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};

use crate::{
    adjustment::Adjustment,
    error::Result,
    event::{EventKind, GapSpan, MarketEvent, Seq, Timestamp},
    instrument::{AssetClass, Instrument, ProviderId},
};

/// Append-only log of events received in a live session.
///
/// Implementations capture both `rx_ts` and `seq` so that replay reproduces
/// the engine's experience exactly (including arrival ordering and gaps).
#[async_trait]
pub trait TapLog: Send + Sync {
    /// Append one event. Implementations may buffer; durability is bounded by
    /// the next `flush` call.
    async fn append(&self, ev: &MarketEvent) -> Result<()>;

    /// Flush any buffered events to durable storage.
    async fn flush(&self) -> Result<()>;

    /// Open this log as a replay source. Returns a [`ReplaySource`] sharing
    /// the same underlying storage handle.
    fn as_replay_source(&self) -> Box<dyn ReplaySource>;
}

/// Canonical store of historical fetches keyed by `(provider, instrument,
/// kind, range)`.
#[async_trait]
pub trait HistoricalCache: Send + Sync {
    /// Look up a cached range. Returns the cached coverage description (which
    /// may be a subset of `key`'s range) or `None` if nothing for this key
    /// exists.
    async fn lookup(&self, key: &CacheKey) -> Result<Option<CacheCoverage>>;

    /// Store a batch of events under `key`. Implementations may merge with
    /// existing coverage.
    async fn store(&self, key: &CacheKey, events: &[MarketEvent]) -> Result<()>;

    /// Enumerate the source-timestamp gaps within `key`'s requested range
    /// that the cache does not yet cover. Returned spans are non-overlapping
    /// and ordered by `from_source_ts`. Empty result means the requested
    /// range is fully covered.
    ///
    /// Default implementation derives a coarse answer from
    /// [`HistoricalCache::lookup`]: it reports the leading and trailing
    /// uncovered fringes of the requested range. Backends that track
    /// internal gaps (multi-segment coverage) should override this to
    /// surface mid-range holes that `lookup` cannot.
    async fn gaps(&self, key: &CacheKey) -> Result<Vec<GapSpan>> {
        let coverage = self.lookup(key).await?;
        let mut spans = Vec::new();
        match coverage {
            None => spans.push(GapSpan {
                from_source_ts: key.from,
                to_source_ts: key.to,
            }),
            Some(c) => {
                if key.from < c.from {
                    spans.push(GapSpan {
                        from_source_ts: key.from,
                        to_source_ts: c.from,
                    });
                }
                if c.to < key.to {
                    spans.push(GapSpan {
                        from_source_ts: c.to,
                        to_source_ts: key.to,
                    });
                }
            }
        }
        Ok(spans)
    }

    /// Enumerate everything this cache currently holds. Each entry describes one
    /// stored `(provider, symbol, kind, adjustment)` key, the source-time
    /// segments actually covered, the event count, and a best-effort logical
    /// volume estimate.
    ///
    /// Distinct from [`gaps`](HistoricalCache::gaps) / [`lookup`](HistoricalCache::lookup),
    /// which answer coverage for *one* key: `catalog` is a whole-cache
    /// enumeration with each key's real covered segments. It carries **no
    /// `seq`** — `seq` is a live, per-symbol property, never a cache property.
    ///
    /// Default returns an empty catalog; backends that cannot enumerate opt out.
    async fn catalog(&self) -> Result<Vec<CacheCatalogEntry>> {
        Ok(Vec::new())
    }

    /// Open the cached range for `key` as a replay source.
    fn as_replay_source(&self, key: CacheKey) -> Box<dyn ReplaySource>;
}

/// One enumerated cache key, as reported by [`HistoricalCache::catalog`].
///
/// Carries the *recoverable* identity components rather than a fabricated
/// [`Instrument`]: the legacy cache row/coverage shapes do not all store
/// `asset_class`, so a faithful instrument cannot always be reconstructed.
/// Consumers needing a full `Instrument` rebuild it only when `asset_class` is
/// `Some`.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CacheCatalogEntry {
    pub provider: ProviderId,
    pub symbol: String,
    /// `Some` only when the backend records it; `None` for rows written before
    /// the catalog write-path change.
    pub asset_class: Option<AssetClass>,
    pub kind: EventKind,
    /// The adjustment the rows are *stored* under. Trades/quotes always store
    /// under [`Adjustment::Raw`] regardless of the requested mode; only bars
    /// segregate by mode.
    pub adjustment: Adjustment,
    /// Covered source-time segments `[from, to)`. Reuses the span type.
    pub segments: Vec<GapSpan>,
    pub event_count: u64,
    /// Best-effort *logical* volume estimate in bytes (`event_count ×
    /// bytes_per_row`); ignores index / coverage-doc / MVCC overhead. `None`
    /// when unknown.
    pub est_bytes: Option<u64>,
}

impl CacheCatalogEntry {
    /// Construct an entry from its required identity + coverage components.
    /// `asset_class` defaults to `None` and `est_bytes` to `None`; set them with
    /// [`with_asset_class`](Self::with_asset_class) /
    /// [`with_est_bytes`](Self::with_est_bytes). This constructor (rather than a
    /// struct literal) lets backend crates build entries while the type stays
    /// `#[non_exhaustive]` for forward-compatible field additions.
    #[must_use]
    pub fn new(
        provider: ProviderId,
        symbol: String,
        kind: EventKind,
        adjustment: Adjustment,
        segments: Vec<GapSpan>,
        event_count: u64,
    ) -> Self {
        Self {
            provider,
            symbol,
            asset_class: None,
            kind,
            adjustment,
            segments,
            event_count,
            est_bytes: None,
        }
    }

    /// Set the recovered asset class (`None` for legacy rows that did not record it).
    #[must_use]
    pub fn with_asset_class(mut self, asset_class: Option<AssetClass>) -> Self {
        self.asset_class = asset_class;
        self
    }

    /// Set the best-effort logical volume estimate.
    #[must_use]
    pub fn with_est_bytes(mut self, est_bytes: Option<u64>) -> Self {
        self.est_bytes = est_bytes;
        self
    }
}

/// Anything that can be opened as an ordered event stream.
#[async_trait]
pub trait ReplaySource: Send + Sync {
    /// Open the stream over `request`. Implementations yield events in
    /// `seq` order; the returned stream completes when the request range is
    /// exhausted.
    async fn open(&self, request: ReplayRequest) -> Result<BoxStream<'static, MarketEvent>>;
}

/// Identifies one cached range. The provider is carried inside
/// [`Instrument`] (see [`Instrument::provider`]) so a `CacheKey` is fully
/// self-describing — backends derive their row keys from the qualifying
/// tuple inside `instrument` without an external lookup.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CacheKey {
    pub instrument: Instrument,
    pub kind: EventKind,
    pub from: Timestamp,
    pub to: Timestamp,
    /// Corporate-action adjustment mode this range is stored/served under.
    /// Backends segregate rows by this so adjusted and raw bars for the same
    /// `(symbol, range)` never collide. Descends from the same session source
    /// of truth as [`HistoryRequest::adjustment`](crate::HistoryRequest).
    pub adjustment: Adjustment,
}

/// What a `HistoricalCache` reports about a cached range.
#[derive(Debug, Clone, PartialEq)]
pub struct CacheCoverage {
    /// The actual range covered (may be a subset of the requested `CacheKey`).
    pub from: Timestamp,
    pub to: Timestamp,
    pub event_count: u64,
    pub first_seq: Option<Seq>,
    pub last_seq: Option<Seq>,
}

/// Parameters passed to [`ReplaySource::open`].
#[derive(Debug, Clone, PartialEq)]
pub struct ReplayRequest {
    pub instruments: Vec<Instrument>,
    pub kinds: Vec<EventKind>,
    pub from: Timestamp,
    pub to: Timestamp,
}

#[cfg(test)]
mod tests {
    use super::{
        AssetClass, CacheCatalogEntry, CacheKey, EventKind, GapSpan, HistoricalCache, ProviderId,
        ReplaySource, Result, Timestamp,
    };
    use crate::{Adjustment, BarInterval, MarketEvent};
    use async_trait::async_trait;

    struct BareCache;

    #[async_trait]
    impl HistoricalCache for BareCache {
        async fn lookup(&self, _key: &CacheKey) -> Result<Option<super::CacheCoverage>> {
            Ok(None)
        }
        async fn store(&self, _key: &CacheKey, _events: &[MarketEvent]) -> Result<()> {
            Ok(())
        }
        fn as_replay_source(&self, _key: CacheKey) -> Box<dyn ReplaySource> {
            unreachable!("not used in this test")
        }
    }

    #[tokio::test]
    async fn default_catalog_is_empty() {
        let cache = BareCache;
        assert!(cache.catalog().await.unwrap().is_empty());
    }

    #[test]
    fn cache_catalog_entry_serde_round_trips() {
        for asset_class in [None, Some(AssetClass::Equity)] {
            let entry = CacheCatalogEntry {
                provider: ProviderId::from_static("p"),
                symbol: "AAPL".to_string(),
                asset_class,
                kind: EventKind::Bar(BarInterval::OneMinute),
                adjustment: Adjustment::All,
                segments: vec![GapSpan {
                    from_source_ts: Timestamp(0),
                    to_source_ts: Timestamp(100),
                }],
                event_count: 42,
                est_bytes: Some(1024),
            };
            let json = serde_json::to_string(&entry).unwrap();
            let back: CacheCatalogEntry = serde_json::from_str(&json).unwrap();
            assert_eq!(entry, back);
        }
    }
}
