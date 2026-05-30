//! SurrealDB-backed [`TapLog`] (and [`ReplaySource`]).
//!
//! Records the live event stream in **arrival order**. The sole ordering key
//! is `seq`, assigned by this log (not the session-local seq). `seq` is a pure
//! total order — contiguous by construction, never gapped for drop detection.
//!
//! # Schema (namespace `datamancer`, database `taplog`)
//!
//! - One **shard** table per `(instrument, kind)`, e.g. `tap_000000`, holding
//!   homogeneous single-kind rows (`seq`, `source_ts`, `rx_ts`, payload). One
//!   instrument's same-kind events live together, which compresses well.
//! - `streams` — registry mapping each `(instrument, kind)` to its shard table
//!   name. Drives write-path shard resolution and replay shard enumeration.
//! - `meta` — a single row (`hwm`, `next_shard`) holding the global `seq`
//!   high-water mark and the next shard ordinal to allocate.
//!
//! # Durability
//!
//! `append` enqueues onto an unbounded channel and returns; a background writer
//! task performs the actual inserts so the live stream never stalls on disk.
//! `flush` drains the queue up to a barrier and reports the most recent write
//! error, if any. Writes are best-effort: a failing write is logged and never
//! propagated into the live session.

use std::collections::HashMap;
use std::path::Path;

use async_trait::async_trait;
use datamancer_core::{
    AssetClass, Bar, BarInterval, Error, EventKind, Instrument, MarketEvent, Price, ProviderId,
    Quote, ReplayRequest, ReplaySource, Result, Seq, TapLog, Timestamp, Trade,
};
use futures::stream::{self, BoxStream, StreamExt};
use serde::{Deserialize, Serialize};
use surrealdb::{Surreal, engine::local::Db, types::SurrealValue};
use tokio::sync::{mpsc, oneshot};

/// Where the tap log is stored. Mirrors `SurrealCacheConfig`.
#[derive(Clone, Debug)]
pub enum SurrealTapLogConfig {
    /// In-process, ephemeral. Good for tests.
    Memory,
    /// Embedded `SurrealKV` at `path`. Created if absent.
    Embedded { path: std::path::PathBuf },
    /// Remote `SurrealDB`. Not yet wired (see `SurrealCacheConfig::Remote`).
    Remote { url: String },
}

impl SurrealTapLogConfig {
    /// Convenience: an embedded tap log at `path`.
    pub fn embedded(path: impl AsRef<Path>) -> Self {
        Self::Embedded {
            path: path.as_ref().to_path_buf(),
        }
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "owned err matches `.map_err(map_err)` callsite ergonomics"
)]
fn map_err(err: surrealdb::Error) -> Error {
    Error::Storage(format!("surrealdb: {err}"))
}

// ---------------------------------------------------------------------------
// Stored row shapes — one per kind. No provider/symbol columns: a shard holds
// exactly one (instrument, kind), so identity comes from the registry. Keeping
// rows minimal is the compression win.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
struct TapTradeRow {
    seq: u64,
    source_ts: i64,
    rx_ts: i64,
    price_raw: i64,
    size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
struct TapQuoteRow {
    seq: u64,
    source_ts: i64,
    rx_ts: i64,
    bid_raw: i64,
    bid_size: u64,
    ask_raw: i64,
    ask_size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
struct TapBarRow {
    seq: u64,
    source_ts: i64,
    rx_ts: i64,
    open_raw: i64,
    high_raw: i64,
    low_raw: i64,
    close_raw: i64,
    volume: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
struct StreamRow {
    provider: String,
    asset_class: String,
    symbol: String,
    kind_tag: String,
    table: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, SurrealValue)]
struct MetaRow {
    /// Global `seq` high-water mark: the last seq assigned.
    hwm: u64,
    /// Next shard ordinal to allocate.
    next_shard: u64,
}

// ---------------------------------------------------------------------------
// Encode/decode helpers
// ---------------------------------------------------------------------------

fn kind_tag(kind: EventKind) -> &'static str {
    match kind {
        EventKind::Trade => "trade",
        EventKind::Quote => "quote",
        EventKind::Bar(BarInterval::OneSecond) => "bar_1s",
        EventKind::Bar(BarInterval::OneMinute) => "bar_1m",
        EventKind::Bar(BarInterval::FiveMinute) => "bar_5m",
        EventKind::Bar(BarInterval::FifteenMinute) => "bar_15m",
        EventKind::Bar(BarInterval::OneHour) => "bar_1h",
        EventKind::Bar(BarInterval::OneDay) => "bar_1d",
    }
}

fn kind_from_tag(tag: &str) -> Option<EventKind> {
    Some(match tag {
        "trade" => EventKind::Trade,
        "quote" => EventKind::Quote,
        "bar_1s" => EventKind::Bar(BarInterval::OneSecond),
        "bar_1m" => EventKind::Bar(BarInterval::OneMinute),
        "bar_5m" => EventKind::Bar(BarInterval::FiveMinute),
        "bar_15m" => EventKind::Bar(BarInterval::FifteenMinute),
        "bar_1h" => EventKind::Bar(BarInterval::OneHour),
        "bar_1d" => EventKind::Bar(BarInterval::OneDay),
        _ => return None,
    })
}

fn asset_class_tag(asset: AssetClass) -> &'static str {
    match asset {
        AssetClass::Equity => "equity",
        AssetClass::Etf => "etf",
        AssetClass::Crypto => "crypto",
        _ => "unknown",
    }
}

fn asset_class_from_tag(tag: &str) -> Option<AssetClass> {
    Some(match tag {
        "equity" => AssetClass::Equity,
        "etf" => AssetClass::Etf,
        "crypto" => AssetClass::Crypto,
        _ => return None,
    })
}

/// Deterministic, **injective** record id for a `(instrument, kind)` registry
/// entry. Each component is length-prefixed (`<byte-len>:<bytes>`) before
/// concatenation, so distinct tuples can never alias onto one id even when
/// `provider` or `symbol` contains a separator character — a plain
/// delimiter-join (`a|b|c`) could collide two different streams onto a single
/// `streams` record and corrupt shard resolution after a reopen.
///
/// (The tuple-form `db.upsert(("streams", id))` separately escapes the id for
/// storage; that is about persistence, not about the injectivity of the id we
/// construct here. Only the *shard table name* must be a plain token, which is
/// why shards are allocated as `tap_NNNNNN`.)
fn registry_id(instrument: &Instrument, kind: EventKind) -> String {
    let mut id = String::new();
    for part in [
        instrument.provider().as_str(),
        asset_class_tag(instrument.asset_class()),
        instrument.symbol(),
        kind_tag(kind),
    ] {
        // `<byte-len>:<bytes>` — reading the count then exactly that many bytes
        // is unambiguous regardless of what the bytes contain.
        id.push_str(&part.len().to_string());
        id.push(':');
        id.push_str(part);
    }
    id
}

fn instrument_from_row(row: &StreamRow) -> Option<(Instrument, EventKind)> {
    let asset = asset_class_from_tag(&row.asset_class)?;
    let kind = kind_from_tag(&row.kind_tag)?;
    let instrument = Instrument::new(ProviderId::new(row.provider.clone()), asset, &row.symbol);
    Some((instrument, kind))
}

fn event_seq(ev: &MarketEvent) -> u64 {
    match ev {
        MarketEvent::Trade(t) => t.seq.0,
        MarketEvent::Quote(q) => q.seq.0,
        MarketEvent::Bar(b) => b.seq.0,
        MarketEvent::Control(c) => c.seq.0,
        // Non-data variants are never stored in the tap log; this arm is
        // defensive and its return value is never used for ordering.
        _ => 0,
    }
}

// ---------------------------------------------------------------------------
// Writer command channel
// ---------------------------------------------------------------------------

enum WriteCmd {
    Event(MarketEvent),
    Flush(oneshot::Sender<Result<()>>),
}

/// SurrealDB-backed tap log.
pub struct SurrealTapLog {
    db: Surreal<Db>,
    tx: mpsc::UnboundedSender<WriteCmd>,
}

impl SurrealTapLog {
    /// Open the tap log, creating the `meta`/`streams` tables on first use and
    /// re-defining known shard tables so replay can `SELECT` them after reopen.
    ///
    /// # Errors
    ///
    /// Returns `Error::Storage` if the engine fails to open, the namespace/
    /// database statement fails, or schema/registry load fails.
    pub async fn open(cfg: SurrealTapLogConfig) -> Result<Self> {
        let db: Surreal<Db> = match cfg {
            SurrealTapLogConfig::Memory => Surreal::new::<surrealdb::engine::local::Mem>(())
                .await
                .map_err(map_err)?,
            SurrealTapLogConfig::Embedded { path } => {
                Surreal::new::<surrealdb::engine::local::SurrealKv>(
                    path.to_string_lossy().into_owned(),
                )
                .await
                .map_err(map_err)?
            }
            SurrealTapLogConfig::Remote { .. } => {
                return Err(Error::Storage(
                    "remote SurrealDB connections require additional surrealdb feature flags; \
                     enable them in Cargo.toml and revise SurrealTapLog::open"
                        .to_string(),
                ));
            }
        };
        db.use_ns("datamancer")
            .use_db("taplog")
            .await
            .map_err(map_err)?;
        db.query(
            "DEFINE TABLE IF NOT EXISTS meta SCHEMALESS; \
             DEFINE TABLE IF NOT EXISTS streams SCHEMALESS;",
        )
        .await
        .map_err(map_err)?;

        let meta: Option<MetaRow> = db.select(("meta", "singleton")).await.map_err(map_err)?;
        let meta = meta.unwrap_or_default();

        // Load the registry, rebuild the in-memory shard map, and re-DEFINE each
        // shard table (SurrealDB rejects SELECT against an undefined table after
        // a reopen even though the data persists).
        let rows: Vec<StreamRow> = db.select("streams").await.map_err(map_err)?;
        let mut shards: HashMap<(Instrument, EventKind), String> = HashMap::new();
        for row in &rows {
            db.query(format!(
                "DEFINE TABLE IF NOT EXISTS {} SCHEMALESS",
                row.table
            ))
            .await
            .map_err(map_err)?;
            if let Some((instrument, kind)) = instrument_from_row(row) {
                shards.insert((instrument, kind), row.table.clone());
            }
        }

        let (tx, rx) = mpsc::unbounded_channel();
        let writer = Writer {
            db: db.clone(),
            hwm: meta.hwm,
            next_shard: meta.next_shard,
            shards,
            last_error: None,
        };
        tokio::spawn(writer.run(rx));

        Ok(Self { db, tx })
    }
}

#[async_trait]
impl TapLog for SurrealTapLog {
    async fn append(&self, ev: &MarketEvent) -> Result<()> {
        // Unbounded, non-blocking enqueue: the live stream never waits on disk.
        // A send error means the writer task is gone (log being dropped); that
        // is not a live-session-fatal condition, so swallow it.
        let _ = self.tx.send(WriteCmd::Event(ev.clone()));
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        let (ack_tx, ack_rx) = oneshot::channel();
        if self.tx.send(WriteCmd::Flush(ack_tx)).is_err() {
            return Ok(()); // writer gone; nothing buffered to lose
        }
        match ack_rx.await {
            Ok(res) => res,
            Err(_) => Ok(()), // writer dropped before replying
        }
    }

    fn as_replay_source(&self) -> Box<dyn ReplaySource> {
        Box::new(SurrealTapReplaySource {
            db: self.db.clone(),
        })
    }
}

// ---------------------------------------------------------------------------
// Background writer
// ---------------------------------------------------------------------------

struct Writer {
    db: Surreal<Db>,
    hwm: u64,
    next_shard: u64,
    shards: HashMap<(Instrument, EventKind), String>,
    last_error: Option<Error>,
}

impl Writer {
    async fn run(mut self, mut rx: mpsc::UnboundedReceiver<WriteCmd>) {
        while let Some(cmd) = rx.recv().await {
            match cmd {
                WriteCmd::Event(ev) => {
                    if let Err(e) = self.write_event(ev).await {
                        tracing::warn!(error = %e, "tap log write failed");
                        self.last_error = Some(e);
                    }
                }
                WriteCmd::Flush(ack) => {
                    // Events ahead of this barrier are already written (we drain
                    // serially). Report and clear the most recent error, if any.
                    let res = match self.last_error.take() {
                        Some(e) => Err(e),
                        None => Ok(()),
                    };
                    let _ = ack.send(res);
                }
            }
        }
    }

    async fn write_event(&mut self, ev: MarketEvent) -> Result<()> {
        let (instrument, kind) = match &ev {
            MarketEvent::Trade(t) => (t.instrument.clone(), EventKind::Trade),
            MarketEvent::Quote(q) => (q.instrument.clone(), EventKind::Quote),
            MarketEvent::Bar(b) => (b.instrument.clone(), EventKind::Bar(b.interval)),
            // Non-data events are not tapped; the session gate also filters
            // these, so this is defensive only.
            _ => return Ok(()),
        };

        let shard = self.resolve_shard(&instrument, kind).await?;

        // Reserve seq and persist the high-water mark BEFORE inserting the row.
        // A crash between persist and insert leaves an unused seq value — a
        // harmless gap, since seq carries no drop-detection meaning — never a
        // reused value that would corrupt ordering.
        self.hwm += 1;
        let seq = self.hwm;
        self.persist_meta().await?;

        match ev {
            MarketEvent::Trade(t) => {
                let row = TapTradeRow {
                    seq,
                    source_ts: t.source_ts.0,
                    rx_ts: t.rx_ts.0,
                    price_raw: t.price.raw(),
                    size: t.size,
                };
                let _: Option<TapTradeRow> = self
                    .db
                    .create(shard.as_str())
                    .content(row)
                    .await
                    .map_err(map_err)?;
            }
            MarketEvent::Quote(q) => {
                let row = TapQuoteRow {
                    seq,
                    source_ts: q.source_ts.0,
                    rx_ts: q.rx_ts.0,
                    bid_raw: q.bid.raw(),
                    bid_size: q.bid_size,
                    ask_raw: q.ask.raw(),
                    ask_size: q.ask_size,
                };
                let _: Option<TapQuoteRow> = self
                    .db
                    .create(shard.as_str())
                    .content(row)
                    .await
                    .map_err(map_err)?;
            }
            MarketEvent::Bar(b) => {
                let row = TapBarRow {
                    seq,
                    source_ts: b.source_ts.0,
                    rx_ts: b.rx_ts.0,
                    open_raw: b.open.raw(),
                    high_raw: b.high.raw(),
                    low_raw: b.low.raw(),
                    close_raw: b.close.raw(),
                    volume: b.volume,
                };
                let _: Option<TapBarRow> = self
                    .db
                    .create(shard.as_str())
                    .content(row)
                    .await
                    .map_err(map_err)?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Resolve the shard table for `(instrument, kind)`, allocating + recording
    /// a new one in the registry on first sight. The shard name is an opaque
    /// `tap_NNNNNN` token (valid as a `SurrealDB` table identifier) regardless of
    /// the symbol's characters.
    async fn resolve_shard(&mut self, instrument: &Instrument, kind: EventKind) -> Result<String> {
        if let Some(name) = self.shards.get(&(instrument.clone(), kind)) {
            return Ok(name.clone());
        }

        if asset_class_tag(instrument.asset_class()) == "unknown" {
            tracing::warn!(
                instrument = %instrument,
                "tap log: unknown asset class; this shard will not survive a reopen"
            );
        }

        let ordinal = self.next_shard;
        self.next_shard += 1;
        let name = format!("tap_{ordinal:06}");
        // If a step below fails after this point, the error propagates and this
        // ordinal is simply skipped — a harmless gap in shard numbering, never
        // a reused or colliding shard name.
        self.persist_meta().await?;

        self.db
            .query(format!("DEFINE TABLE IF NOT EXISTS {name} SCHEMALESS"))
            .await
            .map_err(map_err)?;

        let reg = StreamRow {
            provider: instrument.provider().as_str().to_string(),
            asset_class: asset_class_tag(instrument.asset_class()).to_string(),
            symbol: instrument.symbol().to_string(),
            kind_tag: kind_tag(kind).to_string(),
            table: name.clone(),
        };
        let _: Option<StreamRow> = self
            .db
            .upsert(("streams", registry_id(instrument, kind)))
            .content(reg)
            .await
            .map_err(map_err)?;

        self.shards.insert((instrument.clone(), kind), name.clone());
        Ok(name)
    }

    async fn persist_meta(&self) -> Result<()> {
        let row = MetaRow {
            hwm: self.hwm,
            next_shard: self.next_shard,
        };
        let _: Option<MetaRow> = self
            .db
            .upsert(("meta", "singleton"))
            .content(row)
            .await
            .map_err(map_err)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ReplaySource
// ---------------------------------------------------------------------------

struct SurrealTapReplaySource {
    db: Surreal<Db>,
}

#[async_trait]
#[allow(
    clippy::too_many_lines,
    reason = "linear query/decode/merge pipeline kept inline; extraction would obscure the per-kind handling"
)]
impl ReplaySource for SurrealTapReplaySource {
    async fn open(&self, request: ReplayRequest) -> Result<BoxStream<'static, MarketEvent>> {
        let from = request.from.0;
        let to = request.to.0;
        if from >= to {
            return Ok(stream::empty().boxed());
        }

        let regs: Vec<StreamRow> = self.db.select("streams").await.map_err(map_err)?;

        let mut all: Vec<MarketEvent> = Vec::new();
        for row in &regs {
            let Some((instrument, kind)) = instrument_from_row(row) else {
                continue;
            };
            if !request.instruments.is_empty() && !request.instruments.contains(&instrument) {
                continue;
            }
            if !request.kinds.is_empty() && !request.kinds.contains(&kind) {
                continue;
            }

            // Per-shard query: rows in the source_ts window, seq-ordered. Each
            // shard's rows are already a sorted run; merging happens below.
            match kind {
                EventKind::Trade => {
                    let rows: Vec<TapTradeRow> = self
                        .db
                        .query(
                            "SELECT * FROM type::table($tbl) \
                             WHERE source_ts >= $from AND source_ts < $to \
                             ORDER BY seq ASC",
                        )
                        .bind(("tbl", row.table.clone()))
                        .bind(("from", from))
                        .bind(("to", to))
                        .await
                        .map_err(map_err)?
                        .take(0)
                        .map_err(map_err)?;
                    all.extend(rows.into_iter().map(|r| {
                        MarketEvent::Trade(Trade {
                            instrument: instrument.clone(),
                            source_ts: Timestamp(r.source_ts),
                            rx_ts: Timestamp(r.rx_ts),
                            seq: Seq(r.seq),
                            price: Price::from_raw(r.price_raw),
                            size: r.size,
                        })
                    }));
                }
                EventKind::Quote => {
                    let rows: Vec<TapQuoteRow> = self
                        .db
                        .query(
                            "SELECT * FROM type::table($tbl) \
                             WHERE source_ts >= $from AND source_ts < $to \
                             ORDER BY seq ASC",
                        )
                        .bind(("tbl", row.table.clone()))
                        .bind(("from", from))
                        .bind(("to", to))
                        .await
                        .map_err(map_err)?
                        .take(0)
                        .map_err(map_err)?;
                    all.extend(rows.into_iter().map(|r| {
                        MarketEvent::Quote(Quote {
                            instrument: instrument.clone(),
                            source_ts: Timestamp(r.source_ts),
                            rx_ts: Timestamp(r.rx_ts),
                            seq: Seq(r.seq),
                            bid: Price::from_raw(r.bid_raw),
                            bid_size: r.bid_size,
                            ask: Price::from_raw(r.ask_raw),
                            ask_size: r.ask_size,
                        })
                    }));
                }
                EventKind::Bar(interval) => {
                    let rows: Vec<TapBarRow> = self
                        .db
                        .query(
                            "SELECT * FROM type::table($tbl) \
                             WHERE source_ts >= $from AND source_ts < $to \
                             ORDER BY seq ASC",
                        )
                        .bind(("tbl", row.table.clone()))
                        .bind(("from", from))
                        .bind(("to", to))
                        .await
                        .map_err(map_err)?
                        .take(0)
                        .map_err(map_err)?;
                    all.extend(rows.into_iter().map(|r| {
                        MarketEvent::Bar(Bar {
                            instrument: instrument.clone(),
                            interval,
                            source_ts: Timestamp(r.source_ts),
                            rx_ts: Timestamp(r.rx_ts),
                            seq: Seq(r.seq),
                            open: Price::from_raw(r.open_raw),
                            high: Price::from_raw(r.high_raw),
                            low: Price::from_raw(r.low_raw),
                            close: Price::from_raw(r.close_raw),
                            volume: r.volume,
                        })
                    }));
                }
            }
        }

        // Merge the per-shard sorted runs into one globally seq-ordered stream.
        // seq is a unique global total order, so a single sort by seq IS the
        // k-way merge result. (Materialize-then-sort mirrors the cache's replay
        // shape; a streaming cursor merge is a future memory optimization.)
        all.sort_by_key(event_seq);
        Ok(stream::iter(all).boxed())
    }
}
