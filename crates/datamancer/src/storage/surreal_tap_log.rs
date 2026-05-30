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
    AssetClass, BarInterval, Error, EventKind, Instrument, MarketEvent, ProviderId, ReplayRequest,
    ReplaySource, Result, TapLog,
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

#[allow(dead_code, reason = "used by writer task in Task 3")]
#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
struct TapTradeRow {
    seq: u64,
    source_ts: i64,
    rx_ts: i64,
    price_raw: i64,
    size: u64,
}

#[allow(dead_code, reason = "used by writer task in Task 3")]
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

#[allow(dead_code, reason = "used by writer task in Task 3")]
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

#[allow(dead_code, reason = "used by writer task in Task 3")]
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

#[allow(dead_code, reason = "used by registry_id in Task 3")]
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

#[allow(dead_code, reason = "used by writer shard allocation in Task 3")]
/// Deterministic record id for a `(instrument, kind)` registry entry. The
/// tuple-form `db.select(("streams", id))` escapes arbitrary id content, so a
/// symbol like `BTC/USD` is safe here; only the *shard table name* must be a
/// plain token, which is why shards are allocated as `tap_NNNNNN`.
fn registry_id(instrument: &Instrument, kind: EventKind) -> String {
    format!(
        "{}|{}|{}|{}",
        instrument.provider().as_str(),
        asset_class_tag(instrument.asset_class()),
        instrument.symbol(),
        kind_tag(kind),
    )
}

fn instrument_from_row(row: &StreamRow) -> Option<(Instrument, EventKind)> {
    let asset = asset_class_from_tag(&row.asset_class)?;
    let kind = kind_from_tag(&row.kind_tag)?;
    let instrument = Instrument::new(ProviderId::new(row.provider.clone()), asset, &row.symbol);
    Some((instrument, kind))
}

#[allow(dead_code, reason = "used by writer task in Task 3")]
fn event_seq(ev: &MarketEvent) -> u64 {
    match ev {
        MarketEvent::Trade(t) => t.seq.0,
        MarketEvent::Quote(q) => q.seq.0,
        MarketEvent::Bar(b) => b.seq.0,
        MarketEvent::Control(c) => c.seq.0,
        _ => 0,
    }
}

// ---------------------------------------------------------------------------
// Writer command channel
// ---------------------------------------------------------------------------

#[allow(dead_code, reason = "variants constructed by append/flush in Task 3")]
enum WriteCmd {
    Event(MarketEvent),
    Flush(oneshot::Sender<Result<()>>),
}

/// SurrealDB-backed tap log.
pub struct SurrealTapLog {
    db: Surreal<Db>,
    #[allow(dead_code, reason = "used by append/flush in Task 3")]
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
    async fn append(&self, _ev: &MarketEvent) -> Result<()> {
        // Filled in by Task 3.
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        // Filled in by Task 3.
        Ok(())
    }

    fn as_replay_source(&self) -> Box<dyn ReplaySource> {
        // Filled in by Task 4.
        Box::new(SurrealTapReplaySource {
            db: self.db.clone(),
        })
    }
}

// ---------------------------------------------------------------------------
// Background writer (Task 3 fills in the body)
// ---------------------------------------------------------------------------

struct Writer {
    db: Surreal<Db>,
    hwm: u64,
    next_shard: u64,
    shards: HashMap<(Instrument, EventKind), String>,
    last_error: Option<Error>,
}

impl Writer {
    async fn run(self, mut rx: mpsc::UnboundedReceiver<WriteCmd>) {
        // Filled in by Task 3. Silence unused-field warnings until then.
        let _ = (
            &self.db,
            self.hwm,
            self.next_shard,
            &self.shards,
            &self.last_error,
        );
        while let Some(cmd) = rx.recv().await {
            match cmd {
                WriteCmd::Event(_) => {}
                WriteCmd::Flush(ack) => {
                    let _ = ack.send(Ok(()));
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// ReplaySource (Task 4 fills in `open`)
// ---------------------------------------------------------------------------

struct SurrealTapReplaySource {
    db: Surreal<Db>,
}

#[async_trait]
impl ReplaySource for SurrealTapReplaySource {
    async fn open(&self, _request: ReplayRequest) -> Result<BoxStream<'static, MarketEvent>> {
        // Filled in by Task 4.
        let _ = &self.db;
        Ok(stream::empty().boxed())
    }
}
