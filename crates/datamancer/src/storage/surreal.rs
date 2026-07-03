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
//! - `trades` — `{ provider, symbol, source_ts, rx_ts, price_raw, size_raw,
//!   adjustment }`
//! - `quotes` — `{ provider, symbol, source_ts, rx_ts, bid_raw, bid_size_raw,
//!   ask_raw, ask_size_raw, adjustment }`
//! - `bars_1s`, `bars_1m`, `bars_5m`, `bars_15m`, `bars_1h`, `bars_1d`
//!   — one table per supported [`BarInterval`]; OHLCV columns plus the
//!   common `provider`, `symbol`, `source_ts`, `rx_ts`, `adjustment`.
//!
//! Every row carries an `adjustment` discriminant (the corporate-action mode
//! the data was fetched under). Trades and quotes are never adjusted, so theirs
//! is always `"raw"`; bars vary by mode. It segregates adjusted from raw bars
//! for the same `(symbol, range)` — see the record id and read filter below.
//!
//! Each row's record id is the string
//! `"{provider}|{symbol}|{adjustment}|{ts:020}"` — the 20-digit zero-padded
//! `source_ts` (nanoseconds since epoch as `i64`) ensures that lexicographic
//! ordering on the record id matches source-time ordering. The id is doing two
//! jobs: it gives upserts a natural primary key (re-ingest of the same
//! `(provider, symbol, adjustment, ts)` overwrites rather than duplicates), and
//! it groups rows for one instrument together on disk. Folding `adjustment`
//! into the id lets adjusted and raw rows for the same `(provider, symbol, ts)`
//! coexist instead of upserting over one another.
//!
//! Reads use plain `SELECT … FROM <table> WHERE provider = $prov AND
//! symbol = $sym AND adjustment = $adj AND source_ts >= $from AND
//! source_ts < $to ORDER BY source_ts ASC` — half-open range filter on the
//! indexed `source_ts` column, scoped to one adjustment mode so a fresh read
//! never surfaces orphaned rows from another mode. The store DELETE and count
//! are filtered by `adjustment` for the same reason. The tables are SCHEMALESS
//! so no indices are defined today; this is fine for the test/dev access
//! pattern but a follow-up should consider explicit indices on
//! `(provider, symbol, adjustment, source_ts)` if range scans get heavy.
//!
//! Coverage of stored ranges is recorded in a per-key `coverage` table —
//! one document per `(provider, symbol, kind, adjustment)` holding a list of merged,
//! non-overlapping `[from, to]` segments. `lookup` reports the segment
//! that intersects the requested range; `gaps` enumerates the holes.
//! `store` always claims exactly the requested key range as covered, so a
//! successfully-fetched but empty range (e.g. market-closed interval or
//! pre-inception symbol) is still recorded and will not be re-fetched as a gap.

use std::path::Path;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use datamancer_core::{
    Adjustment, AssetClass, Bar, BarInterval, CacheCatalogEntry, CacheCoverage, CacheKey, Error,
    EventKind, GapSpan, HistoricalCache, Instrument, MarketEvent, Price, Quantity, Quote,
    ReplayRequest, ReplaySource, Result, Seq, Timestamp, Trade,
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

    /// Inverse of [`table_for`](Self::table_for): map a coverage-id table token
    /// back to its [`EventKind`]. Returns `None` for an unrecognized token so a
    /// malformed coverage id is skipped rather than panicking.
    fn kind_for(table: &str) -> Option<EventKind> {
        Some(match table {
            "trades" => EventKind::Trade,
            "quotes" => EventKind::Quote,
            "bars_1s" => EventKind::Bar(BarInterval::OneSecond),
            "bars_1m" => EventKind::Bar(BarInterval::OneMinute),
            "bars_5m" => EventKind::Bar(BarInterval::FiveMinute),
            "bars_15m" => EventKind::Bar(BarInterval::FifteenMinute),
            "bars_1h" => EventKind::Bar(BarInterval::OneHour),
            "bars_1d" => EventKind::Bar(BarInterval::OneDay),
            _ => return None,
        })
    }

    /// Logical bytes per stored row for a kind: the sum of the fixed
    /// `i64`/`u64` numeric fields. A *logical* estimate of the serialized field
    /// payload — it ignores the variable provider/symbol/adjustment strings,
    /// index, coverage-doc, and MVCC overhead. SCHEMALESS rows have no true
    /// on-disk size available from the SDK.
    const fn bytes_per_row(kind: EventKind) -> u64 {
        match kind {
            // source_ts, rx_ts, price_raw, size
            EventKind::Trade => 4 * 8,
            // source_ts, rx_ts, bid_raw, bid_size, ask_raw, ask_size
            EventKind::Quote => 6 * 8,
            // source_ts, rx_ts, OHLC (4), volume
            EventKind::Bar(_) => 7 * 8,
        }
    }

    /// Adjustment a row is actually stored under. Trades and quotes are never
    /// corporate-action adjusted, so they always key under `Raw` regardless of
    /// the session-wide mode carried on the key; only bars segregate by mode.
    fn effective_adjustment(key: &CacheKey) -> Adjustment {
        match key.kind {
            EventKind::Bar(_) => key.adjustment,
            EventKind::Trade | EventKind::Quote => Adjustment::Raw,
        }
    }

    fn coverage_id(key: &CacheKey) -> String {
        format!(
            "{}|{}|{}|{}",
            key.instrument.provider(),
            key.instrument.symbol(),
            Self::table_for(key.kind),
            Self::effective_adjustment(key).as_str(),
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
    /// Size in raw `Quantity` units (1e-9 of a base unit).
    size_raw: u64,
    /// Adjustment discriminant. Trades are never adjusted, so this is always
    /// `"raw"`; it keeps the mode-scoped `WHERE adjustment = $adj` filter on
    /// the shared store DELETE / count uniform across kinds.
    adjustment: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
struct QuoteRow {
    provider: String,
    symbol: String,
    source_ts: i64,
    rx_ts: i64,
    bid_raw: i64,
    /// Bid size in raw `Quantity` units (1e-9 of a base unit).
    bid_size_raw: u64,
    ask_raw: i64,
    /// Ask size in raw `Quantity` units (1e-9 of a base unit).
    ask_size_raw: u64,
    /// Adjustment discriminant; always `"raw"` for quotes. See [`TradeRow`].
    adjustment: String,
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
    /// Volume in raw `Quantity` units (1e-9 of a base unit).
    volume_raw: u64,
    /// Corporate-action adjustment mode this bar was fetched under. Segregates
    /// adjusted vs raw bars for the same `(symbol, range)` so a mode-scoped
    /// SELECT never returns orphaned rows from another mode.
    adjustment: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, SurrealValue)]
struct CoverageDoc {
    /// Sorted, non-overlapping [from, to] (in nanos).
    segments: Vec<(i64, i64)>,
    event_count: u64,
    /// Asset class of the covered instrument, recorded so the catalog can
    /// reconstruct a faithful `Instrument`. `None` for rows written before this
    /// field existed (the id and row shapes do not otherwise carry it).
    #[serde(default)]
    asset_class: Option<String>,
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
        let table = Self::table_for(key.kind);
        let provider = key.instrument.provider().as_str().to_string();
        let symbol = key.instrument.symbol().to_string();
        let adj = Self::effective_adjustment(key).as_str();

        // Replace the claimed range: clear any existing rows in [from, to)
        // before inserting. `store` records coverage for the whole key range,
        // so a re-store that returns fewer events than a prior entry (notably
        // a `refresh`) must not leave stale rows behind that a later replay
        // would surface as if current. For the read-through gap-fill path the
        // range is previously uncovered, so this DELETE matches nothing.
        //
        // The DELETE is scoped to this adjustment mode (`AND adjustment =
        // $adj`): a store under one mode must never clear another mode's rows
        // in the same `(symbol, range)`.
        self.db
            .query(
                "DELETE FROM type::table($tbl) \
                 WHERE provider = $prov AND symbol = $sym \
                 AND adjustment = $adj \
                 AND source_ts >= $from AND source_ts < $to",
            )
            .bind(("tbl", table.to_string()))
            .bind(("prov", provider.clone()))
            .bind(("sym", symbol.clone()))
            .bind(("adj", adj.to_string()))
            .bind(("from", key.from.0))
            .bind(("to", key.to.0))
            .await
            .map_err(map_err)?;

        let mut stored: u64 = 0;

        for ev in events {
            let ts = match ev {
                MarketEvent::Trade(t) => Some(t.source_ts.0),
                MarketEvent::Quote(q) => Some(q.source_ts.0),
                MarketEvent::Bar(b) => Some(b.source_ts.0),
                _ => None,
            };
            let Some(ts) = ts else { continue };

            // Mode is part of the row id so adjusted and raw rows for the same
            // `(provider, symbol, ts)` coexist instead of upserting over one
            // another.
            let row_id = format!("{provider}|{symbol}|{adj}|{ts:020}");
            match ev {
                MarketEvent::Trade(t) => {
                    let row = TradeRow {
                        provider: provider.clone(),
                        symbol: symbol.clone(),
                        source_ts: t.source_ts.0,
                        rx_ts: t.rx_ts.0,
                        price_raw: t.price.raw(),
                        size_raw: t.size.raw(),
                        adjustment: adj.to_string(),
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
                        bid_size_raw: q.bid_size.raw(),
                        ask_raw: q.ask.raw(),
                        ask_size_raw: q.ask_size.raw(),
                        adjustment: adj.to_string(),
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
                        volume_raw: b.volume.raw(),
                        adjustment: adj.to_string(),
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

    async fn catalog(&self) -> Result<Vec<CacheCatalogEntry>> {
        // The `coverage` table is the authoritative "what is cached" record.
        // `meta::id(id)` returns just the string key (`provider|symbol|table|
        // adjustment`), sidestepping any RecordId-shape coupling.
        let mut response = self
            .db
            .query(
                "SELECT meta::id(id) AS coverage_id, segments, event_count, asset_class \
                 FROM coverage",
            )
            .await
            .map_err(map_err)?;
        let rows: Vec<CatalogRow> = response.take(0).map_err(map_err)?;

        let mut entries = Vec::with_capacity(rows.len());
        for row in rows {
            let parts: Vec<&str> = row.coverage_id.split('|').collect();
            let [provider, symbol, table, adjustment] = parts.as_slice() else {
                tracing::warn!(
                    coverage_id = %row.coverage_id,
                    "skipping malformed coverage id (expected 4 |-separated parts)"
                );
                continue;
            };
            let Some(kind) = Self::kind_for(table) else {
                tracing::warn!(
                    coverage_id = %row.coverage_id,
                    table = %table,
                    "skipping coverage id with unknown table token"
                );
                continue;
            };
            let Some(adjustment) = Adjustment::from_token(adjustment) else {
                tracing::warn!(
                    coverage_id = %row.coverage_id,
                    adjustment = %adjustment,
                    "skipping coverage id with unknown adjustment token"
                );
                continue;
            };
            let segments = row
                .segments
                .into_iter()
                .map(|(a, b)| GapSpan {
                    from_source_ts: Timestamp(a),
                    to_source_ts: Timestamp(b),
                })
                .collect();
            let est_bytes = Some(row.event_count.saturating_mul(Self::bytes_per_row(kind)));
            entries.push(
                CacheCatalogEntry::new(
                    datamancer_core::ProviderId::new((*provider).to_string()),
                    (*symbol).to_string(),
                    kind,
                    adjustment,
                    segments,
                    row.event_count,
                )
                .with_asset_class(row.asset_class.as_deref().and_then(asset_class_from_str))
                .with_est_bytes(est_bytes),
            );
        }
        Ok(entries)
    }

    fn as_replay_source(&self, key: CacheKey) -> Box<dyn ReplaySource> {
        Box::new(SurrealReplaySource {
            db: self.db.clone(),
            key,
        })
    }
}

/// Row shape for the [`catalog`](SurrealCache::catalog) scan. `coverage_id` is
/// the `meta::id(id)` string key; `asset_class` is absent for legacy rows.
#[derive(Debug, Deserialize, SurrealValue)]
struct CatalogRow {
    coverage_id: String,
    segments: Vec<(i64, i64)>,
    event_count: u64,
    #[serde(default)]
    asset_class: Option<String>,
}

/// Inverse of [`AssetClass`]'s `Display`. Unknown tokens (or future variants
/// from a newer writer) yield `None` rather than a fabricated identity.
fn asset_class_from_str(s: &str) -> Option<AssetClass> {
    match s {
        "equity" => Some(AssetClass::Equity),
        "etf" => Some(AssetClass::Etf),
        "crypto" => Some(AssetClass::Crypto),
        _ => None,
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
        // `merge_in` bumps `event_count` additively, which drifts upward on a
        // re-store/refresh: `store` DELETEs the range then re-inserts, but the
        // additive count never subtracts the removed rows. Recompute the count
        // from the actual stored rows over the union of covered segments so the
        // catalog reports current contents, not a running sum of every write.
        // Segments are half-open [from, to) (see `intersect`/`gaps_within`) and
        // `count_events_in` is likewise half-open, so each segment counts
        // directly. Touching segments are merged, so no row is double-counted at
        // a boundary.
        let mut total: u64 = 0;
        for &(seg_from, seg_to) in &doc.segments {
            total = total.saturating_add(self.count_events_in(key, seg_from, seg_to).await?);
        }
        doc.event_count = total;
        // Record the asset class so the catalog can reconstruct a faithful
        // `Instrument` (the id and row shapes do not otherwise carry it).
        doc.asset_class = Some(key.instrument.asset_class().to_string());
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
        let adj = Self::effective_adjustment(key).as_str();
        let mut response = self
            .db
            .query(
                "SELECT count() AS n FROM type::table($tbl) \
                 WHERE provider = $prov AND symbol = $sym \
                 AND adjustment = $adj \
                 AND source_ts >= $from AND source_ts < $to GROUP ALL",
            )
            .bind(("tbl", table.to_string()))
            .bind(("prov", provider))
            .bind(("sym", symbol))
            .bind(("adj", adj.to_string()))
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
        let adj = SurrealCache::effective_adjustment(&self.key)
            .as_str()
            .to_string();

        let events: Vec<MarketEvent> = match kind {
            EventKind::Trade => {
                let rows: Vec<TradeRow> = self
                    .db
                    .query(
                        "SELECT * FROM type::table($tbl) \
                         WHERE provider = $prov AND symbol = $sym \
                         AND adjustment = $adj \
                         AND source_ts >= $from AND source_ts < $to \
                         ORDER BY source_ts ASC",
                    )
                    .bind(("tbl", table.to_string()))
                    .bind(("prov", provider))
                    .bind(("sym", symbol))
                    .bind(("adj", adj.clone()))
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
                            size: Quantity::from_raw(r.size_raw),
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
                         AND adjustment = $adj \
                         AND source_ts >= $from AND source_ts < $to \
                         ORDER BY source_ts ASC",
                    )
                    .bind(("tbl", table.to_string()))
                    .bind(("prov", provider))
                    .bind(("sym", symbol))
                    .bind(("adj", adj.clone()))
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
                            bid_size: Quantity::from_raw(r.bid_size_raw),
                            ask: Price::from_raw(r.ask_raw),
                            ask_size: Quantity::from_raw(r.ask_size_raw),
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
                         AND adjustment = $adj \
                         AND source_ts >= $from AND source_ts < $to \
                         ORDER BY source_ts ASC",
                    )
                    .bind(("tbl", table.to_string()))
                    .bind(("prov", provider))
                    .bind(("sym", symbol))
                    .bind(("adj", adj.clone()))
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
                            volume: Quantity::from_raw(r.volume_raw),
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

    #[test]
    fn kind_for_inverts_table_for() {
        for kind in [
            EventKind::Trade,
            EventKind::Quote,
            EventKind::Bar(BarInterval::OneSecond),
            EventKind::Bar(BarInterval::OneMinute),
            EventKind::Bar(BarInterval::FiveMinute),
            EventKind::Bar(BarInterval::FifteenMinute),
            EventKind::Bar(BarInterval::OneHour),
            EventKind::Bar(BarInterval::OneDay),
        ] {
            assert_eq!(
                SurrealCache::kind_for(SurrealCache::table_for(kind)),
                Some(kind)
            );
        }
        assert_eq!(SurrealCache::kind_for("not_a_table"), None);
    }

    #[tokio::test]
    async fn catalog_skips_malformed_coverage_id() {
        let cache = SurrealCache::open(SurrealCacheConfig::Memory)
            .await
            .unwrap();

        // One valid entry through the normal write path.
        let key = CacheKey {
            instrument: Instrument::new(
                datamancer_core::ProviderId::from_static("alpaca"),
                AssetClass::Equity,
                "AAPL",
            ),
            kind: EventKind::Trade,
            from: Timestamp(0),
            to: Timestamp(100),
            adjustment: Adjustment::Raw,
        };
        cache
            .store(
                &key,
                &[MarketEvent::Trade(Trade {
                    instrument: key.instrument.clone(),
                    source_ts: Timestamp(10),
                    rx_ts: Timestamp(10),
                    seq: Seq(0),
                    price: Price::from_f64_round(1.0),
                    size: Quantity::from_units(1),
                })],
            )
            .await
            .unwrap();

        // Inject a coverage row whose id is NOT `provider|symbol|table|adjustment`.
        let _: Option<CoverageDoc> = cache
            .db
            .upsert(("coverage", "broken-id-without-pipes"))
            .content(CoverageDoc {
                segments: vec![(0, 50)],
                event_count: 3,
                asset_class: None,
            })
            .await
            .unwrap();

        // And one with the right shape but an unknown table token.
        let _: Option<CoverageDoc> = cache
            .db
            .upsert(("coverage", "alpaca|AAPL|not_a_table|raw"))
            .content(CoverageDoc {
                segments: vec![(0, 50)],
                event_count: 3,
                asset_class: None,
            })
            .await
            .unwrap();

        let catalog = cache.catalog().await.unwrap();
        assert_eq!(catalog.len(), 1, "malformed ids are skipped, not panicked");
        assert_eq!(catalog[0].symbol, "AAPL");
        assert_eq!(catalog[0].kind, EventKind::Trade);
    }
}
