//! SurrealDB-backed [`TapLog`] (and [`ReplaySource`]).
//!
//! Records the live event stream in **arrival order**. The sole ordering key
//! is `seq` — the **source `seq`**, stamped by the session controller before
//! the tap-log tee. The log persists that value verbatim and does **not** mint
//! its own, so tap-log replay reproduces the delivered stream's `seq` exactly
//! (Phase-1 convergence). `seq` is a per-symbol total order.
//!
//! # Schema (namespace `datamancer`, database `taplog`)
//!
//! - One **shard** table per `(instrument, kind)`, e.g. `tap_000000`, holding
//!   homogeneous single-kind rows (`seq`, `source_ts`, `rx_ts`, payload). One
//!   instrument's same-kind events live together, which compresses well.
//! - `streams` — registry mapping each `(instrument, kind)` to its shard table
//!   name. Drives write-path shard resolution and replay shard enumeration.
//! - `meta` — a single row holding the next shard ordinal (`next_shard`) and
//!   the global append-ordinal high-water mark (`next_ord`) to allocate.
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
    Quantity, Quote, ReplayRequest, ReplaySource, Result, Seq, TapLog, Timestamp, Trade,
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

// Each row carries `ord`: a tap-local, strictly monotonic append ordinal that
// is unique across the whole log (every shard, every session/process lifetime).
// The source `seq` is per-symbol and resets to 0 with each new controller, so it
// is not unique within a long-lived shard and cannot order replay on its own.
// `ord` is the replay ordering key; `seq` is preserved verbatim as the delivered
// event's `seq`.

#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
struct TapTradeRow {
    // `Option` so rows written before `ord` existed deserialize as `None` (the
    // `SurrealValue` derive does not honor `#[serde(default)]` for an absent
    // field); such legacy rows read back as ordinal 0 at replay.
    #[serde(default)]
    ord: Option<u64>,
    seq: u64,
    source_ts: i64,
    rx_ts: i64,
    price_raw: i64,
    /// Size in raw `Quantity` units (1e-9 of a base unit).
    size_raw: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
struct TapQuoteRow {
    // `Option` so rows written before `ord` existed deserialize as `None` (the
    // `SurrealValue` derive does not honor `#[serde(default)]` for an absent
    // field); such legacy rows read back as ordinal 0 at replay.
    #[serde(default)]
    ord: Option<u64>,
    seq: u64,
    source_ts: i64,
    rx_ts: i64,
    bid_raw: i64,
    /// Bid size in raw `Quantity` units (1e-9 of a base unit).
    bid_size_raw: u64,
    ask_raw: i64,
    /// Ask size in raw `Quantity` units (1e-9 of a base unit).
    ask_size_raw: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
struct TapBarRow {
    // `Option` so rows written before `ord` existed deserialize as `None` (the
    // `SurrealValue` derive does not honor `#[serde(default)]` for an absent
    // field); such legacy rows read back as ordinal 0 at replay.
    #[serde(default)]
    ord: Option<u64>,
    seq: u64,
    source_ts: i64,
    rx_ts: i64,
    open_raw: i64,
    high_raw: i64,
    low_raw: i64,
    close_raw: i64,
    /// Volume in raw `Quantity` units (1e-9 of a base unit).
    volume_raw: u64,
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
    /// Next shard ordinal to allocate.
    next_shard: u64,
    /// High-water mark of the global append ordinal (`ord`). Persisted as a
    /// reservation boundary, not per write, so a crash skips the unused tail of
    /// the current batch (a harmless gap in `ord`) but never reuses an ordinal.
    /// `Option` (not `#[serde(default)]`) so a `meta` row written before this
    /// field existed deserializes as `None` under the `SurrealValue` derive
    /// (which, unlike serde, does not honor `default` for an absent field).
    #[serde(default)]
    next_ord: Option<u64>,
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

/// Stable on-disk tag for an asset class. `AssetClass` is `#[non_exhaustive]`,
/// so the wildcard is mandatory — but a new variant tagged `"unknown"` would
/// not round-trip through [`asset_class_from_tag`]. The writer refuses to tap
/// an `"unknown"` class (see `resolve_shard`) rather than persist an
/// unreadable shard. **Adding an `AssetClass` variant requires updating this
/// function and [`asset_class_from_tag`] in lockstep.**
fn asset_class_tag(asset: AssetClass) -> &'static str {
    match asset {
        AssetClass::Equity => "equity",
        AssetClass::Etf => "etf",
        AssetClass::Crypto => "crypto",
        _ => "unknown",
    }
}

/// Inverse of [`asset_class_tag`]. Returns `None` for an unrecognized tag;
/// keep the two in lockstep when adding an `AssetClass` variant.
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
            next_shard: meta.next_shard,
            // Resume the append ordinal at the persisted high-water mark (0 for a
            // legacy `meta` row predating the field). Any ordinals reserved but
            // unused before the last shutdown are skipped, never reused.
            next_ord: meta.next_ord.unwrap_or(0),
            ord_high: meta.next_ord.unwrap_or(0),
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

/// Number of append ordinals reserved per persisted high-water bump. Larger =
/// fewer meta writes but a larger gap skipped on an unclean shutdown.
const ORD_BATCH: u64 = 1024;

struct Writer {
    db: Surreal<Db>,
    next_shard: u64,
    /// Next append ordinal to hand out; advances in memory per write.
    next_ord: u64,
    /// Exclusive upper bound of the currently reserved ordinal batch.
    ord_high: u64,
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

        // Persist the source `seq` verbatim — the session controller already
        // stamped it before the tee. The tap log no longer mints its own seq,
        // so replay reproduces the delivered stream's `seq` (convergence).
        let seq = event_seq(&ev);
        // Assign the global append ordinal that orders replay. Unlike `seq` (per
        // symbol, resets per controller), `ord` is unique across every shard and
        // session lifetime, so replay is a faithful, unambiguous append order.
        let ord = self.allocate_ord().await?;

        match ev {
            MarketEvent::Trade(t) => {
                let row = TapTradeRow {
                    ord: Some(ord),
                    seq,
                    source_ts: t.source_ts.0,
                    rx_ts: t.rx_ts.0,
                    price_raw: t.price.raw(),
                    size_raw: t.size.raw(),
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
                    ord: Some(ord),
                    seq,
                    source_ts: q.source_ts.0,
                    rx_ts: q.rx_ts.0,
                    bid_raw: q.bid.raw(),
                    bid_size_raw: q.bid_size.raw(),
                    ask_raw: q.ask.raw(),
                    ask_size_raw: q.ask_size.raw(),
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
                    ord: Some(ord),
                    seq,
                    source_ts: b.source_ts.0,
                    rx_ts: b.rx_ts.0,
                    open_raw: b.open.raw(),
                    high_raw: b.high.raw(),
                    low_raw: b.low.raw(),
                    close_raw: b.close.raw(),
                    volume_raw: b.volume.raw(),
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

        // Refuse to allocate a shard whose asset class has no stable encoding.
        // Such a row would deserialize to `None` in `instrument_from_row` on
        // reopen — silently dropping the shard from the in-memory map and
        // orphaning its data while leaking an ordinal. Failing loudly here (a
        // best-effort write error, logged and surfaced at the next `flush`) is
        // far safer than that silent corruption. See `asset_class_tag`.
        if asset_class_tag(instrument.asset_class()) == "unknown" {
            return Err(Error::Storage(format!(
                "tap log: asset class of {instrument} has no stable on-disk encoding; \
                 refusing to tap it. Add the variant to asset_class_tag/asset_class_from_tag \
                 in lockstep."
            )));
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

    /// Reserve a fresh batch of append ordinals when the current one is spent,
    /// persisting the new high-water mark first so a crash can only skip the
    /// unused tail (a harmless `ord` gap), never reuse an ordinal. Returns the
    /// next ordinal to assign.
    async fn allocate_ord(&mut self) -> Result<u64> {
        if self.next_ord >= self.ord_high {
            self.ord_high = self.next_ord.saturating_add(ORD_BATCH);
            self.persist_meta().await?;
        }
        let ord = self.next_ord;
        self.next_ord = self.next_ord.saturating_add(1);
        Ok(ord)
    }

    async fn persist_meta(&self) -> Result<()> {
        let row = MetaRow {
            next_shard: self.next_shard,
            // Persist the reservation boundary, not the live counter.
            next_ord: Some(self.ord_high),
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

        let mut all: Vec<(u64, MarketEvent)> = Vec::new();
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
                             ORDER BY ord ASC",
                        )
                        .bind(("tbl", row.table.clone()))
                        .bind(("from", from))
                        .bind(("to", to))
                        .await
                        .map_err(map_err)?
                        .take(0)
                        .map_err(map_err)?;
                    all.extend(rows.into_iter().map(|r| {
                        (
                            r.ord.unwrap_or(0),
                            MarketEvent::Trade(Trade {
                                instrument: instrument.clone(),
                                source_ts: Timestamp(r.source_ts),
                                rx_ts: Timestamp(r.rx_ts),
                                seq: Seq(r.seq),
                                price: Price::from_raw(r.price_raw),
                                size: Quantity::from_raw(r.size_raw),
                            }),
                        )
                    }));
                }
                EventKind::Quote => {
                    let rows: Vec<TapQuoteRow> = self
                        .db
                        .query(
                            "SELECT * FROM type::table($tbl) \
                             WHERE source_ts >= $from AND source_ts < $to \
                             ORDER BY ord ASC",
                        )
                        .bind(("tbl", row.table.clone()))
                        .bind(("from", from))
                        .bind(("to", to))
                        .await
                        .map_err(map_err)?
                        .take(0)
                        .map_err(map_err)?;
                    all.extend(rows.into_iter().map(|r| {
                        (
                            r.ord.unwrap_or(0),
                            MarketEvent::Quote(Quote {
                                instrument: instrument.clone(),
                                source_ts: Timestamp(r.source_ts),
                                rx_ts: Timestamp(r.rx_ts),
                                seq: Seq(r.seq),
                                bid: Price::from_raw(r.bid_raw),
                                bid_size: Quantity::from_raw(r.bid_size_raw),
                                ask: Price::from_raw(r.ask_raw),
                                ask_size: Quantity::from_raw(r.ask_size_raw),
                            }),
                        )
                    }));
                }
                EventKind::Bar(interval) => {
                    let rows: Vec<TapBarRow> = self
                        .db
                        .query(
                            "SELECT * FROM type::table($tbl) \
                             WHERE source_ts >= $from AND source_ts < $to \
                             ORDER BY ord ASC",
                        )
                        .bind(("tbl", row.table.clone()))
                        .bind(("from", from))
                        .bind(("to", to))
                        .await
                        .map_err(map_err)?
                        .take(0)
                        .map_err(map_err)?;
                    all.extend(rows.into_iter().map(|r| {
                        (
                            r.ord.unwrap_or(0),
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
                                volume: Quantity::from_raw(r.volume_raw),
                            }),
                        )
                    }));
                }
            }
        }

        // Merge the per-shard runs into one stream by the global append ordinal
        // `ord`. `ord` is a unique, monotonic total order across every shard and
        // session lifetime, so a single sort by `ord` IS the k-way merge result
        // and reproduces the original delivered (arrival) order — including
        // across symbols, which a sort by per-symbol `seq` could not. (Source
        // `seq` is preserved on each event; it just is not the ordering key.)
        all.sort_by_key(|(ord, _)| *ord);
        Ok(stream::iter(all.into_iter().map(|(_, ev)| ev)).boxed())
    }
}
