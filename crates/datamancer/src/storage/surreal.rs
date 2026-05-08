//! SurrealDB-backed historical cache.
//!
//! Stub — see milestone 3.

#![allow(dead_code)]

use datamancer_core::{CacheCoverage, CacheKey, GapSpan, HistoricalCache, MarketEvent, ReplaySource, Result};

/// Configuration for [`SurrealCache`].
#[derive(Clone, Debug)]
pub struct SurrealCacheConfig {
    pub path: std::path::PathBuf,
}

/// Placeholder until milestone 3 wires up the real backend.
pub struct SurrealCache;

impl SurrealCache {
    pub async fn open(_cfg: SurrealCacheConfig) -> Result<Self> {
        Ok(Self)
    }
}

#[async_trait::async_trait]
impl HistoricalCache for SurrealCache {
    async fn lookup(&self, _key: &CacheKey) -> Result<Option<CacheCoverage>> {
        Ok(None)
    }

    async fn store(&self, _key: &CacheKey, _events: &[MarketEvent]) -> Result<()> {
        Ok(())
    }

    fn as_replay_source(&self, _key: CacheKey) -> Box<dyn ReplaySource> {
        unimplemented!("placeholder until milestone 3")
    }
}

#[allow(dead_code)]
fn _gap(_a: i64, _b: i64) -> GapSpan {
    use datamancer_core::Timestamp;
    GapSpan {
        from_source_ts: Timestamp(_a),
        to_source_ts: Timestamp(_b),
    }
}
