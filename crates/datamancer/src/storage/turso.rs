//! Turso-backed [`HistoricalCache`] (and [`ReplaySource`]).
//!
//! Semantics ported 1:1 from the retired prior backend: one table per
//! kind (`trades`, `quotes`, `bars_1s` … `bars_1d`), a `coverage` table of
//! merged half-open segments per `(provider, symbol, kind, adjustment)`,
//! store-claims-exactly-the-key-range (so a fetched-but-empty range is not
//! re-fetched), adjustment-mode scoping of rows, and replay in `source_ts`
//! order with `Seq(0)` (the session re-stamps `seq` on delivery).
//!
//! # Schema (one file per cache)
//!
//! Event tables share the composite PRIMARY KEY
//! `(provider, symbol, adjustment, source_ts)` — it is both the upsert
//! identity (re-ingest overwrites) and the range-scan index the prior
//! module doc wished for. `coverage` rows are keyed by the same
//! `"{provider}|{symbol}|{table}|{adjustment}"` string id the prior
//! backend used, so catalog parsing is unchanged. Segments are a JSON
//! `[[from,to],…]` column. Schema version rides `PRAGMA user_version`.
//!
//! # Writes
//!
//! All mutations go through the one mutex-guarded write connection inside a
//! `BEGIN`/`COMMIT` (see `turso_common` for why single-writer is load-bearing).
//!
//! # The drain-cursor hazard (turso 0.6.1)
//!
//! A write issued on a connection whose earlier `query()` `Rows` cursor is
//! un-drained is **silently lost** — visible in-connection, never reaches
//! disk (see `turso_common::check_or_stamp_user_version` for the first
//! occurrence of this). [`TursoCache::load_coverage`] and
//! [`TursoCache::count_events_in`] both run a `query()` on the *write*
//! connection inside `store`'s transaction, so both fully drain their
//! cursor (loop `rows.next()` to `None`) in an inner scope before the
//! transaction issues its next `execute`.

use std::path::Path;

use async_trait::async_trait;
use datamancer_core::{
    Adjustment, AssetClass, Bar, BarInterval, CacheCatalogEntry, CacheCoverage, CacheKey, Error,
    EventKind, GapSpan, HistoricalCache, MarketEvent, Price, ProviderId, Quantity, Quote,
    ReplayRequest, ReplaySource, Result, Seq, Timestamp, Trade,
};
use futures::stream::{self, BoxStream, StreamExt};
use tokio::sync::Mutex;

use super::coverage::CoverageDoc;
use super::turso_common::{
    DbLocation, check_or_stamp_user_version, connect, execute_retry, map_err, open_database,
};

/// `PRAGMA user_version` for this cache's schema. Fresh lineage (no carry-over
/// from the prior backend's numbering). Deliberately disjoint from the tap
/// log's range (cache uses the `1x` band, tap log the `2x` band) so a file
/// opened as the wrong store type is refused by the version guard instead of
/// silently colliding.
const CACHE_SCHEMA_VERSION: i64 = 10;

/// Where the cache is stored.
#[derive(Clone, Debug)]
pub enum TursoCacheConfig {
    /// In-process, ephemeral. Good for tests.
    Memory,
    /// A database file at `path` (parent directories created if absent).
    Embedded { path: std::path::PathBuf },
}

impl TursoCacheConfig {
    /// Convenience: an embedded cache at `path`.
    pub fn embedded(path: impl AsRef<Path>) -> Self {
        Self::Embedded {
            path: path.as_ref().to_path_buf(),
        }
    }
}

/// Turso-backed historical cache.
pub struct TursoCache {
    db: ::turso::Database,
    /// The one write connection; every mutation locks it (single-writer
    /// discipline — see `turso_common`). Reads open their own connections.
    write: Mutex<::turso::Connection>,
}

const EVENT_TABLES: [&str; 8] = [
    "trades", "quotes", "bars_1s", "bars_1m", "bars_5m", "bars_15m", "bars_1h", "bars_1d",
];

impl TursoCache {
    /// Open the cache, creating the schema on first use.
    ///
    /// # Errors
    ///
    /// `Error::Storage` if the engine fails to open, schema creation fails,
    /// or the file's `user_version` does not match this build.
    pub async fn open(cfg: TursoCacheConfig) -> Result<Self> {
        let location = match cfg {
            TursoCacheConfig::Memory => DbLocation::Memory,
            TursoCacheConfig::Embedded { path } => DbLocation::File(path),
        };
        let db = open_database(&location).await?;
        let write = connect(&db).await?;
        for table in EVENT_TABLES {
            let cols = match table {
                "trades" => "price_raw INTEGER NOT NULL, size_raw INTEGER NOT NULL",
                "quotes" => {
                    "bid_raw INTEGER NOT NULL, bid_size_raw INTEGER NOT NULL, \
                     ask_raw INTEGER NOT NULL, ask_size_raw INTEGER NOT NULL"
                }
                _ => {
                    "open_raw INTEGER NOT NULL, high_raw INTEGER NOT NULL, \
                     low_raw INTEGER NOT NULL, close_raw INTEGER NOT NULL, \
                     volume_raw INTEGER NOT NULL"
                }
            };
            execute_retry(
                &write,
                &format!(
                    "CREATE TABLE IF NOT EXISTS {table} (\
                       provider TEXT NOT NULL, symbol TEXT NOT NULL, \
                       adjustment TEXT NOT NULL, source_ts INTEGER NOT NULL, \
                       rx_ts INTEGER NOT NULL, {cols}, \
                       PRIMARY KEY (provider, symbol, adjustment, source_ts))"
                ),
                (),
            )
            .await?;
        }
        execute_retry(
            &write,
            "CREATE TABLE IF NOT EXISTS coverage (\
               id TEXT PRIMARY KEY, segments TEXT NOT NULL, \
               event_count INTEGER NOT NULL, asset_class TEXT)",
            (),
        )
        .await?;
        check_or_stamp_user_version(&write, CACHE_SCHEMA_VERSION, "cache").await?;
        Ok(Self {
            db,
            write: Mutex::new(write),
        })
    }

    pub(crate) fn table_for(kind: EventKind) -> &'static str {
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

    /// Inverse of [`table_for`](Self::table_for); `None` for an unrecognized
    /// token so a malformed coverage id is skipped rather than panicking.
    pub(crate) fn kind_for(table: &str) -> Option<EventKind> {
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

    /// Logical bytes per stored row (fixed numeric fields only) — same
    /// best-effort estimate the prior backend reported.
    const fn bytes_per_row(kind: EventKind) -> u64 {
        match kind {
            EventKind::Trade => 4 * 8,
            EventKind::Quote => 6 * 8,
            EventKind::Bar(_) => 7 * 8,
        }
    }

    /// Trades/quotes are never corporate-action adjusted: they store under
    /// `Raw` regardless of the key's mode; only bars segregate by mode.
    pub(crate) fn effective_adjustment(key: &CacheKey) -> Adjustment {
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

    /// Loads the coverage doc for `id`, if any.
    ///
    /// Fully drains the `Rows` cursor before returning — see the module-level
    /// drain-cursor note. This matters when `conn` is the write connection
    /// (called from inside `store`'s transaction): an un-drained cursor
    /// silently swallows the next `execute` on the same connection.
    async fn load_coverage(conn: &::turso::Connection, id: &str) -> Result<Option<CoverageDoc>> {
        let doc = {
            let mut rows = conn
                .query(
                    "SELECT segments, event_count, asset_class FROM coverage WHERE id = ?1",
                    (id.to_string(),),
                )
                .await
                .map_err(map_err)?;
            let first = rows.next().await.map_err(map_err)?;
            let doc = match first {
                None => None,
                Some(row) => {
                    let segments_json: String = row.get(0).map_err(map_err)?;
                    let event_count: i64 = row.get(1).map_err(map_err)?;
                    let asset_class: Option<String> = row.get(2).map_err(map_err)?;
                    let segments: Vec<(i64, i64)> = serde_json::from_str(&segments_json)
                        .map_err(|e| Error::Storage(format!("coverage segments decode: {e}")))?;
                    Some(CoverageDoc {
                        segments,
                        event_count: event_count.cast_unsigned(),
                        asset_class,
                    })
                }
            };
            // Drain any remaining rows (there should be at most one, given
            // `id` is the coverage table's PRIMARY KEY) before the cursor
            // drops, per the module-level drain-cursor note.
            while rows.next().await.map_err(map_err)?.is_some() {}
            doc
        };
        Ok(doc)
    }

    /// Counts rows for `key` in `[from, to)`. Same drain discipline as
    /// [`load_coverage`](Self::load_coverage).
    async fn count_events_in(
        conn: &::turso::Connection,
        key: &CacheKey,
        from: i64,
        to: i64,
    ) -> Result<u64> {
        let table = Self::table_for(key.kind);
        let n: i64 = {
            let mut rows = conn
                .query(
                    &format!(
                        "SELECT count(*) FROM {table} \
                         WHERE provider = ?1 AND symbol = ?2 AND adjustment = ?3 \
                         AND source_ts >= ?4 AND source_ts < ?5"
                    ),
                    (
                        key.instrument.provider().as_str().to_string(),
                        key.instrument.symbol().to_string(),
                        Self::effective_adjustment(key).as_str().to_string(),
                        from,
                        to,
                    ),
                )
                .await
                .map_err(map_err)?;
            let row = rows
                .next()
                .await
                .map_err(map_err)?
                .ok_or_else(|| Error::Storage("count(*) returned no row".to_string()))?;
            let n: i64 = row.get(0).map_err(map_err)?;
            // `count(*)` always returns exactly one row, but drain any
            // trailing cursor state before it drops so a subsequent
            // same-connection `execute` (this is called on the write
            // connection from inside `store`'s transaction) is never lost.
            while rows.next().await.map_err(map_err)?.is_some() {}
            n
        };
        Ok(n.cast_unsigned())
    }
}

#[async_trait]
impl HistoricalCache for TursoCache {
    async fn lookup(&self, key: &CacheKey) -> Result<Option<CacheCoverage>> {
        let conn = connect(&self.db).await?;
        let Some(doc) = Self::load_coverage(&conn, &Self::coverage_id(key)).await? else {
            return Ok(None);
        };
        let Some((from, to)) = doc.intersect(key.from.0, key.to.0) else {
            return Ok(None);
        };
        let count = Self::count_events_in(&conn, key, from, to).await?;
        Ok(Some(CacheCoverage {
            from: Timestamp(from),
            to: Timestamp(to),
            event_count: count,
            first_seq: None,
            last_seq: None,
        }))
    }

    async fn store(&self, key: &CacheKey, events: &[MarketEvent]) -> Result<()> {
        let write = self.write.lock().await;
        execute_retry(&write, "BEGIN", ()).await?;
        let res = store_in_tx(&write, key, events).await;
        match res {
            Ok(()) => match execute_retry(&write, "COMMIT", ()).await {
                Ok(_) => Ok(()),
                Err(e) => {
                    // A failed COMMIT can still leave the transaction open on
                    // the shared write connection; every later BEGIN would
                    // then fail until the process restarts. Best-effort
                    // rollback so the connection is usable again — the
                    // original COMMIT error is still the story.
                    let _ = write.execute("ROLLBACK", ()).await;
                    Err(e)
                }
            },
            Err(e) => {
                // Best-effort rollback; the original error is the story.
                let _ = write.execute("ROLLBACK", ()).await;
                Err(e)
            }
        }
    }

    async fn gaps(&self, key: &CacheKey) -> Result<Vec<GapSpan>> {
        let conn = connect(&self.db).await?;
        let doc = Self::load_coverage(&conn, &Self::coverage_id(key))
            .await?
            .unwrap_or_default();
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
        let conn = connect(&self.db).await?;
        let mut rows = conn
            .query(
                "SELECT id, segments, event_count, asset_class FROM coverage",
                (),
            )
            .await
            .map_err(map_err)?;
        let mut entries = Vec::new();
        while let Some(row) = rows.next().await.map_err(map_err)? {
            let coverage_id: String = row.get(0).map_err(map_err)?;
            let segments_json: String = row.get(1).map_err(map_err)?;
            let event_count: i64 = row.get(2).map_err(map_err)?;
            let asset_class: Option<String> = row.get(3).map_err(map_err)?;

            let parts: Vec<&str> = coverage_id.split('|').collect();
            let [provider, symbol, table, adjustment] = parts.as_slice() else {
                tracing::warn!(coverage_id = %coverage_id,
                    "skipping malformed coverage id (expected 4 |-separated parts)");
                continue;
            };
            let Some(kind) = Self::kind_for(table) else {
                tracing::warn!(coverage_id = %coverage_id, table = %table,
                    "skipping coverage id with unknown table token");
                continue;
            };
            let Some(adjustment) = Adjustment::from_token(adjustment) else {
                tracing::warn!(coverage_id = %coverage_id, adjustment = %adjustment,
                    "skipping coverage id with unknown adjustment token");
                continue;
            };
            let segments: Vec<(i64, i64)> = match serde_json::from_str(&segments_json) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(coverage_id = %coverage_id, error = %e,
                        "skipping coverage row with undecodable segments");
                    continue;
                }
            };
            let event_count = event_count.cast_unsigned();
            let est_bytes = Some(event_count.saturating_mul(Self::bytes_per_row(kind)));
            entries.push(
                CacheCatalogEntry::new(
                    ProviderId::new((*provider).to_string()),
                    (*symbol).to_string(),
                    kind,
                    adjustment,
                    segments
                        .into_iter()
                        .map(|(a, b)| GapSpan {
                            from_source_ts: Timestamp(a),
                            to_source_ts: Timestamp(b),
                        })
                        .collect(),
                    event_count,
                )
                .with_asset_class(asset_class.as_deref().and_then(asset_class_from_str))
                .with_est_bytes(est_bytes),
            );
        }
        Ok(entries)
    }

    fn as_replay_source(&self, key: CacheKey) -> Box<dyn ReplaySource> {
        // Cheap handle: clones the shared `Database` and the requested key so
        // `open` can run its own scan later, independent of this cache value.
        Box::new(TursoCacheReplaySource {
            db: self.db.clone(),
            key,
        })
    }
}

/// The body of `store`, run inside the write transaction. Replaces the
/// claimed range (mode-scoped DELETE then INSERT OR REPLACE), then updates
/// coverage: merge the key range in, recount rows over the merged segments
/// (so re-stores do not drift the count upward), and upsert the doc — all
/// atomically with the row writes.
#[allow(
    clippy::too_many_lines,
    reason = "linear delete/insert-per-kind/coverage-update pipeline kept inline; \
              extraction would obscure the single-transaction ordering"
)]
async fn store_in_tx(
    write: &::turso::Connection,
    key: &CacheKey,
    events: &[MarketEvent],
) -> Result<()> {
    let table = TursoCache::table_for(key.kind);
    let provider = key.instrument.provider().as_str().to_string();
    let symbol = key.instrument.symbol().to_string();
    let adj = TursoCache::effective_adjustment(key).as_str().to_string();

    execute_retry(
        write,
        &format!(
            "DELETE FROM {table} WHERE provider = ?1 AND symbol = ?2 \
             AND adjustment = ?3 AND source_ts >= ?4 AND source_ts < ?5"
        ),
        (
            provider.clone(),
            symbol.clone(),
            adj.clone(),
            key.from.0,
            key.to.0,
        ),
    )
    .await?;

    for ev in events {
        match ev {
            MarketEvent::Trade(t) => {
                execute_retry(
                    write,
                    "INSERT OR REPLACE INTO trades \
                     (provider, symbol, adjustment, source_ts, rx_ts, price_raw, size_raw) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    (
                        provider.clone(),
                        symbol.clone(),
                        adj.clone(),
                        t.source_ts.0,
                        t.rx_ts.0,
                        t.price.raw(),
                        t.size.raw().cast_signed(),
                    ),
                )
                .await?;
            }
            MarketEvent::Quote(q) => {
                execute_retry(
                    write,
                    "INSERT OR REPLACE INTO quotes \
                     (provider, symbol, adjustment, source_ts, rx_ts, \
                      bid_raw, bid_size_raw, ask_raw, ask_size_raw) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                    (
                        provider.clone(),
                        symbol.clone(),
                        adj.clone(),
                        q.source_ts.0,
                        q.rx_ts.0,
                        q.bid.raw(),
                        q.bid_size.raw().cast_signed(),
                        q.ask.raw(),
                        q.ask_size.raw().cast_signed(),
                    ),
                )
                .await?;
            }
            MarketEvent::Bar(b) => {
                execute_retry(
                    write,
                    &format!(
                        "INSERT OR REPLACE INTO {table} \
                         (provider, symbol, adjustment, source_ts, rx_ts, \
                          open_raw, high_raw, low_raw, close_raw, volume_raw) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)"
                    ),
                    (
                        provider.clone(),
                        symbol.clone(),
                        adj.clone(),
                        b.source_ts.0,
                        b.rx_ts.0,
                        b.open.raw(),
                        b.high.raw(),
                        b.low.raw(),
                        b.close.raw(),
                        b.volume.raw().cast_signed(),
                    ),
                )
                .await?;
            }
            _ => {}
        }
    }

    // Coverage reflects exactly the caller-asserted key range, NOT the span
    // of whatever events arrived (fetched-but-empty ranges stay covered).
    let id = TursoCache::coverage_id(key);
    let mut doc = TursoCache::load_coverage(write, &id)
        .await?
        .unwrap_or_default();
    doc.merge_in(key.from.0, key.to.0, 0);
    let mut total: u64 = 0;
    for &(seg_from, seg_to) in &doc.segments {
        total =
            total.saturating_add(TursoCache::count_events_in(write, key, seg_from, seg_to).await?);
    }
    doc.event_count = total;
    let asset_class = key.instrument.asset_class().to_string();
    let segments_json = serde_json::to_string(&doc.segments)
        .map_err(|e| Error::Storage(format!("coverage segments encode: {e}")))?;
    execute_retry(
        write,
        "INSERT OR REPLACE INTO coverage (id, segments, event_count, asset_class) \
         VALUES (?1, ?2, ?3, ?4)",
        (
            id,
            segments_json,
            doc.event_count.cast_signed(),
            asset_class,
        ),
    )
    .await?;
    Ok(())
}

/// Inverse of [`AssetClass`]'s `Display`. Unknown tokens yield `None` rather
/// than a fabricated identity.
fn asset_class_from_str(s: &str) -> Option<AssetClass> {
    match s {
        "equity" => Some(AssetClass::Equity),
        "etf" => Some(AssetClass::Etf),
        "crypto" => Some(AssetClass::Crypto),
        _ => None,
    }
}

/// Cache replay source: replays `key`'s rows (narrowed by the incoming
/// [`ReplayRequest`]) in `source_ts` ascending order.
struct TursoCacheReplaySource {
    db: ::turso::Database,
    key: CacheKey,
}

#[async_trait]
impl ReplaySource for TursoCacheReplaySource {
    #[allow(
        clippy::too_many_lines,
        reason = "one match arm per event kind, each a linear query/decode/push loop; \
                  splitting per-kind would scatter the shared row-decode shape across \
                  several small functions for no clarity gain"
    )]
    async fn open(&self, request: ReplayRequest) -> Result<BoxStream<'static, MarketEvent>> {
        // `ReplayRequest` may narrow the cache key; intersect from/to,
        // instruments, and kinds exactly as the prior source did.
        let kind = self.key.kind;
        let from = request.from.0.max(self.key.from.0);
        let to = request.to.0.min(self.key.to.0);
        let instrument_matches =
            request.instruments.is_empty() || request.instruments.contains(&self.key.instrument);
        if !instrument_matches
            || (!request.kinds.is_empty() && !request.kinds.contains(&kind))
            || from >= to
        {
            return Ok(stream::empty().boxed());
        }
        let conn = connect(&self.db).await?;
        let table = TursoCache::table_for(kind);
        let params = (
            self.key.instrument.provider().as_str().to_string(),
            self.key.instrument.symbol().to_string(),
            TursoCache::effective_adjustment(&self.key)
                .as_str()
                .to_string(),
            from,
            to,
        );
        let instrument = self.key.instrument.clone();
        let events: Vec<MarketEvent> = match kind {
            EventKind::Trade => {
                let mut rows = conn
                    .query(
                        "SELECT source_ts, rx_ts, price_raw, size_raw FROM trades \
                         WHERE provider = ?1 AND symbol = ?2 AND adjustment = ?3 \
                         AND source_ts >= ?4 AND source_ts < ?5 \
                         ORDER BY source_ts ASC",
                        params,
                    )
                    .await
                    .map_err(map_err)?;
                let mut out = Vec::new();
                while let Some(row) = rows.next().await.map_err(map_err)? {
                    let size_raw: i64 = row.get(3).map_err(map_err)?;
                    out.push(MarketEvent::Trade(Trade {
                        instrument: instrument.clone(),
                        source_ts: Timestamp(row.get(0).map_err(map_err)?),
                        rx_ts: Timestamp(row.get(1).map_err(map_err)?),
                        seq: Seq(0),
                        price: Price::from_raw(row.get(2).map_err(map_err)?),
                        size: Quantity::from_raw(size_raw.cast_unsigned()),
                    }));
                }
                out
            }
            EventKind::Quote => {
                let mut rows = conn
                    .query(
                        "SELECT source_ts, rx_ts, bid_raw, bid_size_raw, ask_raw, ask_size_raw \
                         FROM quotes \
                         WHERE provider = ?1 AND symbol = ?2 AND adjustment = ?3 \
                         AND source_ts >= ?4 AND source_ts < ?5 \
                         ORDER BY source_ts ASC",
                        params,
                    )
                    .await
                    .map_err(map_err)?;
                let mut out = Vec::new();
                while let Some(row) = rows.next().await.map_err(map_err)? {
                    let bid_size: i64 = row.get(3).map_err(map_err)?;
                    let ask_size: i64 = row.get(5).map_err(map_err)?;
                    out.push(MarketEvent::Quote(Quote {
                        instrument: instrument.clone(),
                        source_ts: Timestamp(row.get(0).map_err(map_err)?),
                        rx_ts: Timestamp(row.get(1).map_err(map_err)?),
                        seq: Seq(0),
                        bid: Price::from_raw(row.get(2).map_err(map_err)?),
                        bid_size: Quantity::from_raw(bid_size.cast_unsigned()),
                        ask: Price::from_raw(row.get(4).map_err(map_err)?),
                        ask_size: Quantity::from_raw(ask_size.cast_unsigned()),
                    }));
                }
                out
            }
            EventKind::Bar(interval) => {
                let mut rows = conn
                    .query(
                        &format!(
                            "SELECT source_ts, rx_ts, open_raw, high_raw, low_raw, close_raw, \
                             volume_raw FROM {table} \
                             WHERE provider = ?1 AND symbol = ?2 AND adjustment = ?3 \
                             AND source_ts >= ?4 AND source_ts < ?5 \
                             ORDER BY source_ts ASC"
                        ),
                        params,
                    )
                    .await
                    .map_err(map_err)?;
                let mut out = Vec::new();
                while let Some(row) = rows.next().await.map_err(map_err)? {
                    let volume_raw: i64 = row.get(6).map_err(map_err)?;
                    out.push(MarketEvent::Bar(Bar {
                        instrument: instrument.clone(),
                        interval,
                        source_ts: Timestamp(row.get(0).map_err(map_err)?),
                        rx_ts: Timestamp(row.get(1).map_err(map_err)?),
                        seq: Seq(0),
                        open: Price::from_raw(row.get(2).map_err(map_err)?),
                        high: Price::from_raw(row.get(3).map_err(map_err)?),
                        low: Price::from_raw(row.get(4).map_err(map_err)?),
                        close: Price::from_raw(row.get(5).map_err(map_err)?),
                        volume: Quantity::from_raw(volume_raw.cast_unsigned()),
                    }));
                }
                out
            }
        };
        Ok(stream::iter(events).boxed())
    }
}
