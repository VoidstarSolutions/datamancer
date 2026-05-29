//! SurrealDB-backed [`HistoricalCache`] (and [`ReplaySource`]).
//!
//! # Backend choice
//!
//! Default is **`SurrealKV`**, `SurrealDB`'s own native Rust storage engine.
//! Compared to `RocksDB` it has no C++ build dependency (so the workspace
//! builds cleanly on any platform without extra system libraries) and is
//! purpose-built for the same record-id range scans we use here. `RocksDB`
//! is only marginally more battle-tested for our access pattern and the
//! build cost isn't worth it. Tests use **Mem** (in-process, ephemeral) for
//! speed.
//!
//! # Schema
//!
//! Tables are declared `SCHEMALESS` so the `SurrealValue`-derived row
//! structs in this module round-trip directly. There's one table per kind:
//!
//! - `trades` — `{ provider, symbol, source_ts, rx_ts, price_raw, size }`
//! - `quotes` — `{ provider, symbol, source_ts, rx_ts, bid_raw, bid_size,
//!   ask_raw, ask_size }`
//! - `bars_1s`, `bars_1m`, `bars_5m`, `bars_15m`, `bars_1h`, `bars_1d`
//!   — one table per supported [`BarInterval`]; OHLCV columns plus the
//!   common `provider`, `symbol`, `source_ts`, `rx_ts`.
//!
//! Each row's record id is the string `"{provider}|{symbol}|{ts:020}"` — the
//! 20-digit zero-padded `source_ts` (nanoseconds since epoch as `i64`)
//! ensures that lexicographic ordering on the record id matches
//! source-time ordering. The id is doing two jobs: it gives upserts a
//! natural primary key (re-ingest of the same `(provider, symbol, ts)`
//! overwrites rather than duplicates), and it groups rows for one
//! instrument together on disk.
//!
//! Reads use plain `SELECT … FROM <table> WHERE provider = $prov AND
//! symbol = $sym AND source_ts >= $from AND source_ts < $to
//! ORDER BY source_ts ASC` — half-open range filter on the indexed
//! `source_ts` column. The tables are SCHEMALESS so no indices are defined
//! today; this is fine for the test/dev access pattern but a follow-up
//! should consider explicit indices on `(provider, symbol, source_ts)`
//! if range scans get heavy.
//!
//! Coverage of stored ranges is recorded in a per-key `coverage` table —
//! one document per `(provider, symbol, kind)` holding a list of merged,
//! non-overlapping `[from, to]` segments. `lookup` reports the segment
//! that intersects the requested range; `gaps` enumerates the holes.

use std::path::Path;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use datamancer_core::{
    Bar, BarInterval, CacheCoverage, CacheKey, Error, EventKind, GapSpan, HistoricalCache,
    Instrument, MarketEvent, Price, Quote, ReplayRequest, ReplaySource, Result, Seq, Timestamp,
    Trade,
};
use futures::stream::{self, BoxStream, StreamExt};
use serde::{Deserialize, Serialize};
use surrealdb::{Surreal, engine::local::Db, types::SurrealValue};

/// Where the cache is stored.
#[derive(Clone, Debug)]
pub enum SurrealCacheConfig {
    /// In-process, ephemeral. Good for tests.
    Memory,
    /// Embedded `SurrealKV` at `path`. Created if absent.
    Embedded { path: std::path::PathBuf },
    /// Remote `SurrealDB` at the given URL. Wired through the `any` engine if
    /// the consumer enables that surrealdb feature; for now we keep the
    /// runtime restricted to embedded mode.
    Remote { url: String },
}

impl SurrealCacheConfig {
    /// Convenience: an embedded cache at `path`.
    pub fn embedded(path: impl AsRef<Path>) -> Self {
        Self::Embedded {
            path: path.as_ref().to_path_buf(),
        }
    }
}

/// SurrealDB-backed historical cache.
pub struct SurrealCache {
    db: Surreal<Db>,
}

impl SurrealCache {
    /// Open the cache, creating tables on first use.
    ///
    /// # Errors
    ///
    /// Returns `Error::Storage` if the underlying `SurrealDB` engine fails
    /// to open, the namespace/database statement fails, or initial table
    /// creation fails.
    pub async fn open(cfg: SurrealCacheConfig) -> Result<Self> {
        let db: Surreal<Db> = match cfg {
            SurrealCacheConfig::Memory => Surreal::new::<surrealdb::engine::local::Mem>(())
                .await
                .map_err(map_err)?,
            SurrealCacheConfig::Embedded { path } => {
                Surreal::new::<surrealdb::engine::local::SurrealKv>(
                    path.to_string_lossy().into_owned(),
                )
                .await
                .map_err(map_err)?
            }
            SurrealCacheConfig::Remote { .. } => {
                return Err(Error::Storage(
                    "remote SurrealDB connections require additional surrealdb feature flags; \
                     enable them in Cargo.toml and revise SurrealCache::open"
                        .to_string(),
                ));
            }
        };
        db.use_ns("datamancer")
            .use_db("cache")
            .await
            .map_err(map_err)?;
        let cache = Self { db };
        cache.init_schema().await?;
        Ok(cache)
    }

    async fn init_schema(&self) -> Result<()> {
        // SurrealDB 3.0 rejects SELECTs against undefined tables. Define
        // every table this cache uses up front; SCHEMALESS keeps the row
        // shape flexible while letting `SurrealValue`-derived rows
        // round-trip directly. Safe to run repeatedly.
        let stmts = "
            DEFINE TABLE IF NOT EXISTS coverage SCHEMALESS;
            DEFINE TABLE IF NOT EXISTS trades SCHEMALESS;
            DEFINE TABLE IF NOT EXISTS quotes SCHEMALESS;
            DEFINE TABLE IF NOT EXISTS bars_1s SCHEMALESS;
            DEFINE TABLE IF NOT EXISTS bars_1m SCHEMALESS;
            DEFINE TABLE IF NOT EXISTS bars_5m SCHEMALESS;
            DEFINE TABLE IF NOT EXISTS bars_15m SCHEMALESS;
            DEFINE TABLE IF NOT EXISTS bars_1h SCHEMALESS;
            DEFINE TABLE IF NOT EXISTS bars_1d SCHEMALESS;
        ";
        self.db.query(stmts).await.map_err(map_err)?;
        Ok(())
    }

    fn table_for(kind: EventKind) -> &'static str {
        match kind {
            EventKind::Trade => "trades",
            EventKind::Quote => "quotes",
            EventKind::Bar(BarInterval::OneSecond) => "bars_1s",
            EventKind::Bar(BarInterval::OneMinute) => "bars_1m",
            EventKind::Bar(BarInterval::FiveMinute) => "bars_5m",
            EventKind::Bar(BarInterval::FifteenMinute) => "bars_15m",
            EventKind::Bar(BarInterval::OneHour) => "bars_1h",
            EventKind::Bar(BarInterval::OneDay) => "bars_1d",
        }
    }

    fn coverage_id(key: &CacheKey) -> String {
        format!(
            "{}|{}|{}",
            key.instrument.provider(),
            key.instrument.symbol(),
            Self::table_for(key.kind)
        )
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "owned err matches `.map_err(map_err)` callsite ergonomics — borrowing here would force closures at every callsite"
)]
fn map_err(err: surrealdb::Error) -> Error {
    Error::Storage(format!("surrealdb: {err}"))
}

// ---------------------------------------------------------------------------
// Stored row shapes
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
struct TradeRow {
    provider: String,
    symbol: String,
    /// Source timestamp, nanoseconds since epoch.
    source_ts: i64,
    rx_ts: i64,
    /// Price in datamancer-core internal units.
    price_raw: i64,
    size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
struct QuoteRow {
    provider: String,
    symbol: String,
    source_ts: i64,
    rx_ts: i64,
    bid_raw: i64,
    bid_size: u64,
    ask_raw: i64,
    ask_size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
struct BarRow {
    provider: String,
    symbol: String,
    source_ts: i64,
    rx_ts: i64,
    open_raw: i64,
    high_raw: i64,
    low_raw: i64,
    close_raw: i64,
    volume: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, SurrealValue)]
struct CoverageDoc {
    /// Sorted, non-overlapping [from, to] (in nanos).
    segments: Vec<(i64, i64)>,
    event_count: u64,
}

impl CoverageDoc {
    fn merge_in(&mut self, from: i64, to: i64, added_events: u64) {
        if to <= from {
            return;
        }
        self.event_count = self.event_count.saturating_add(added_events);
        let mut new_seg = (from, to);
        let mut merged: Vec<(i64, i64)> = Vec::with_capacity(self.segments.len() + 1);
        let mut consumed = false;
        for &(a, b) in &self.segments {
            if b < new_seg.0 {
                merged.push((a, b));
            } else if a > new_seg.1 {
                if !consumed {
                    merged.push(new_seg);
                    consumed = true;
                }
                merged.push((a, b));
            } else {
                new_seg.0 = new_seg.0.min(a);
                new_seg.1 = new_seg.1.max(b);
            }
        }
        if !consumed {
            merged.push(new_seg);
        }
        self.segments = merged;
    }

    fn intersect(&self, from: i64, to: i64) -> Option<(i64, i64)> {
        let mut best: Option<(i64, i64)> = None;
        for &(a, b) in &self.segments {
            let lo = a.max(from);
            let hi = b.min(to);
            if lo < hi && best.is_none_or(|(prev_lo, prev_hi)| hi - lo > prev_hi - prev_lo) {
                best = Some((lo, hi));
            }
        }
        best
    }

    fn gaps_within(&self, from: i64, to: i64) -> Vec<(i64, i64)> {
        if from >= to {
            return Vec::new();
        }
        let mut cursor = from;
        let mut gaps = Vec::new();
        for &(a, b) in &self.segments {
            if b <= cursor {
                continue;
            }
            if a >= to {
                break;
            }
            if a > cursor {
                gaps.push((cursor, a.min(to)));
            }
            cursor = cursor.max(b);
            if cursor >= to {
                break;
            }
        }
        if cursor < to {
            gaps.push((cursor, to));
        }
        gaps
    }
}

// ---------------------------------------------------------------------------
// HistoricalCache impl
// ---------------------------------------------------------------------------

#[async_trait]
impl HistoricalCache for SurrealCache {
    async fn lookup(&self, key: &CacheKey) -> Result<Option<CacheCoverage>> {
        let id = Self::coverage_id(key);
        let doc: Option<CoverageDoc> = self.db.select(("coverage", id)).await.map_err(map_err)?;
        let Some(doc) = doc else { return Ok(None) };
        let Some((from, to)) = doc.intersect(key.from.0, key.to.0) else {
            return Ok(None);
        };
        let count = self.count_events_in(key, from, to).await?;
        Ok(Some(CacheCoverage {
            from: Timestamp(from),
            to: Timestamp(to),
            event_count: count,
            first_seq: None,
            last_seq: None,
        }))
    }

    async fn store(&self, key: &CacheKey, events: &[MarketEvent]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let table = Self::table_for(key.kind);
        let provider = key.instrument.provider().as_str().to_string();
        let symbol = key.instrument.symbol().to_string();
        let mut stored: u64 = 0;

        for ev in events {
            let ts = match ev {
                MarketEvent::Trade(t) => Some(t.source_ts.0),
                MarketEvent::Quote(q) => Some(q.source_ts.0),
                MarketEvent::Bar(b) => Some(b.source_ts.0),
                _ => None,
            };
            let Some(ts) = ts else { continue };

            let row_id = format!("{provider}|{symbol}|{ts:020}");
            match ev {
                MarketEvent::Trade(t) => {
                    let row = TradeRow {
                        provider: provider.clone(),
                        symbol: symbol.clone(),
                        source_ts: t.source_ts.0,
                        rx_ts: t.rx_ts.0,
                        price_raw: t.price.raw(),
                        size: t.size,
                    };
                    let _: Option<TradeRow> = self
                        .db
                        .upsert((table, row_id))
                        .content(row)
                        .await
                        .map_err(map_err)?;
                }
                MarketEvent::Quote(q) => {
                    let row = QuoteRow {
                        provider: provider.clone(),
                        symbol: symbol.clone(),
                        source_ts: q.source_ts.0,
                        rx_ts: q.rx_ts.0,
                        bid_raw: q.bid.raw(),
                        bid_size: q.bid_size,
                        ask_raw: q.ask.raw(),
                        ask_size: q.ask_size,
                    };
                    let _: Option<QuoteRow> = self
                        .db
                        .upsert((table, row_id))
                        .content(row)
                        .await
                        .map_err(map_err)?;
                }
                MarketEvent::Bar(b) => {
                    let row = BarRow {
                        provider: provider.clone(),
                        symbol: symbol.clone(),
                        source_ts: b.source_ts.0,
                        rx_ts: b.rx_ts.0,
                        open_raw: b.open.raw(),
                        high_raw: b.high.raw(),
                        low_raw: b.low.raw(),
                        close_raw: b.close.raw(),
                        volume: b.volume,
                    };
                    let _: Option<BarRow> = self
                        .db
                        .upsert((table, row_id))
                        .content(row)
                        .await
                        .map_err(map_err)?;
                }
                _ => continue,
            }
            stored += 1;
        }

        // Coverage reflects exactly the range the caller asserts was fetched
        // (the CacheKey), NOT the span of whatever events happened to arrive.
        // Callers (e.g. the read-through fetch loop) pass a key range that
        // reflects only what was actually, successfully fetched, so an
        // interrupted fetch leaves the unfetched remainder reported as a gap.
        self.update_coverage(key, key.from.0, key.to.0, stored)
            .await?;
        Ok(())
    }

    async fn gaps(&self, key: &CacheKey) -> Result<Vec<GapSpan>> {
        let id = Self::coverage_id(key);
        let doc: Option<CoverageDoc> = self.db.select(("coverage", id)).await.map_err(map_err)?;
        let doc = doc.unwrap_or_default();
        Ok(doc
            .gaps_within(key.from.0, key.to.0)
            .into_iter()
            .map(|(a, b)| GapSpan {
                from_source_ts: Timestamp(a),
                to_source_ts: Timestamp(b),
            })
            .collect())
    }

    fn as_replay_source(&self, key: CacheKey) -> Box<dyn ReplaySource> {
        Box::new(SurrealReplaySource {
            db: self.db.clone(),
            key,
        })
    }
}

impl SurrealCache {
    async fn update_coverage(
        &self,
        key: &CacheKey,
        from: i64,
        to: i64,
        added_events: u64,
    ) -> Result<()> {
        let id = Self::coverage_id(key);
        let existing: Option<CoverageDoc> = self
            .db
            .select(("coverage", id.clone()))
            .await
            .map_err(map_err)?;
        let mut doc = existing.unwrap_or_default();
        doc.merge_in(from, to, added_events);
        let _: Option<CoverageDoc> = self
            .db
            .upsert(("coverage", id))
            .content(doc)
            .await
            .map_err(map_err)?;
        Ok(())
    }

    async fn count_events_in(&self, key: &CacheKey, from: i64, to: i64) -> Result<u64> {
        let table = Self::table_for(key.kind);
        let provider = key.instrument.provider().as_str().to_string();
        let symbol = key.instrument.symbol().to_string();
        let mut response = self
            .db
            .query(
                "SELECT count() AS n FROM type::table($tbl) \
                 WHERE provider = $prov AND symbol = $sym \
                 AND source_ts >= $from AND source_ts < $to GROUP ALL",
            )
            .bind(("tbl", table.to_string()))
            .bind(("prov", provider))
            .bind(("sym", symbol))
            .bind(("from", from))
            .bind(("to", to))
            .await
            .map_err(map_err)?;
        let rows: Vec<CountRow> = response.take(0).map_err(map_err)?;
        Ok(rows.first().map_or(0, |r| r.n))
    }
}

#[derive(Debug, Serialize, Deserialize, SurrealValue)]
struct CountRow {
    n: u64,
}

// ---------------------------------------------------------------------------
// ReplaySource impl
// ---------------------------------------------------------------------------

struct SurrealReplaySource {
    db: Surreal<Db>,
    key: CacheKey,
}

#[async_trait]
impl ReplaySource for SurrealReplaySource {
    #[allow(
        clippy::too_many_lines,
        reason = "linear query/decode/merge pipeline kept inline; extraction would obscure the per-kind handling"
    )]
    async fn open(&self, request: ReplayRequest) -> Result<BoxStream<'static, MarketEvent>> {
        let provider = self.key.instrument.provider().as_str().to_string();
        let kind = self.key.kind;
        // ReplayRequest may narrow the cache key; honor its from/to and
        // intersect with the original key. Also intersect instruments and
        // kinds (we only pull events that match this key's kind).
        let from = request.from.0.max(self.key.from.0);
        let to = request.to.0.min(self.key.to.0);
        let instruments: Vec<Instrument> = if request.instruments.is_empty() {
            vec![self.key.instrument.clone()]
        } else {
            request
                .instruments
                .iter()
                .filter(|i| **i == self.key.instrument)
                .cloned()
                .collect()
        };
        if instruments.is_empty()
            || (!request.kinds.is_empty() && !request.kinds.contains(&kind))
            || from >= to
        {
            return Ok(stream::empty().boxed());
        }
        let symbol = self.key.instrument.symbol().to_string();
        let table = SurrealCache::table_for(kind);

        let events: Vec<MarketEvent> = match kind {
            EventKind::Trade => {
                let rows: Vec<TradeRow> = self
                    .db
                    .query(
                        "SELECT * FROM type::table($tbl) \
                         WHERE provider = $prov AND symbol = $sym \
                         AND source_ts >= $from AND source_ts < $to \
                         ORDER BY source_ts ASC",
                    )
                    .bind(("tbl", table.to_string()))
                    .bind(("prov", provider))
                    .bind(("sym", symbol))
                    .bind(("from", from))
                    .bind(("to", to))
                    .await
                    .map_err(map_err)?
                    .take(0)
                    .map_err(map_err)?;
                let instrument = self.key.instrument.clone();
                rows.into_iter()
                    .map(|r| {
                        MarketEvent::Trade(Trade {
                            instrument: instrument.clone(),
                            source_ts: Timestamp(r.source_ts),
                            rx_ts: Timestamp(r.rx_ts),
                            seq: Seq(0),
                            price: Price::from_raw(r.price_raw),
                            size: r.size,
                        })
                    })
                    .collect()
            }
            EventKind::Quote => {
                let rows: Vec<QuoteRow> = self
                    .db
                    .query(
                        "SELECT * FROM type::table($tbl) \
                         WHERE provider = $prov AND symbol = $sym \
                         AND source_ts >= $from AND source_ts < $to \
                         ORDER BY source_ts ASC",
                    )
                    .bind(("tbl", table.to_string()))
                    .bind(("prov", provider))
                    .bind(("sym", symbol))
                    .bind(("from", from))
                    .bind(("to", to))
                    .await
                    .map_err(map_err)?
                    .take(0)
                    .map_err(map_err)?;
                let instrument = self.key.instrument.clone();
                rows.into_iter()
                    .map(|r| {
                        MarketEvent::Quote(Quote {
                            instrument: instrument.clone(),
                            source_ts: Timestamp(r.source_ts),
                            rx_ts: Timestamp(r.rx_ts),
                            seq: Seq(0),
                            bid: Price::from_raw(r.bid_raw),
                            bid_size: r.bid_size,
                            ask: Price::from_raw(r.ask_raw),
                            ask_size: r.ask_size,
                        })
                    })
                    .collect()
            }
            EventKind::Bar(interval) => {
                let rows: Vec<BarRow> = self
                    .db
                    .query(
                        "SELECT * FROM type::table($tbl) \
                         WHERE provider = $prov AND symbol = $sym \
                         AND source_ts >= $from AND source_ts < $to \
                         ORDER BY source_ts ASC",
                    )
                    .bind(("tbl", table.to_string()))
                    .bind(("prov", provider))
                    .bind(("sym", symbol))
                    .bind(("from", from))
                    .bind(("to", to))
                    .await
                    .map_err(map_err)?
                    .take(0)
                    .map_err(map_err)?;
                let instrument = self.key.instrument.clone();
                rows.into_iter()
                    .map(|r| {
                        MarketEvent::Bar(Bar {
                            instrument: instrument.clone(),
                            interval,
                            source_ts: Timestamp(r.source_ts),
                            rx_ts: Timestamp(r.rx_ts),
                            seq: Seq(0),
                            open: Price::from_raw(r.open_raw),
                            high: Price::from_raw(r.high_raw),
                            low: Price::from_raw(r.low_raw),
                            close: Price::from_raw(r.close_raw),
                            volume: r.volume,
                        })
                    })
                    .collect()
            }
        };

        Ok(stream::iter(events).boxed())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

#[allow(dead_code)]
pub(crate) fn ts_from_chrono(dt: DateTime<Utc>) -> Timestamp {
    Timestamp(dt.timestamp_nanos_opt().unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coverage_merges_overlapping_segments() {
        let mut c = CoverageDoc::default();
        c.merge_in(10, 20, 1);
        c.merge_in(15, 30, 1);
        assert_eq!(c.segments, vec![(10, 30)]);
        c.merge_in(50, 60, 1);
        assert_eq!(c.segments, vec![(10, 30), (50, 60)]);
        c.merge_in(25, 55, 1);
        assert_eq!(c.segments, vec![(10, 60)]);
    }

    #[test]
    fn coverage_gaps_within_request() {
        let mut c = CoverageDoc::default();
        c.merge_in(10, 20, 1);
        c.merge_in(40, 50, 1);
        assert_eq!(c.gaps_within(0, 60), vec![(0, 10), (20, 40), (50, 60)]);
        assert_eq!(c.gaps_within(10, 50), vec![(20, 40)]);
        assert_eq!(c.gaps_within(0, 5), vec![(0, 5)]);
        assert!(c.gaps_within(10, 20).is_empty());
    }

    #[test]
    fn coverage_intersect_picks_widest_overlap() {
        let mut c = CoverageDoc::default();
        c.merge_in(0, 100, 1);
        c.merge_in(200, 210, 1);
        assert_eq!(c.intersect(50, 150), Some((50, 100)));
        assert_eq!(c.intersect(150, 250), Some((200, 210)));
    }
}
