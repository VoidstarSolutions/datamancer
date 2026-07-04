//! Turso-backed [`TapLog`] (and [`ReplaySource`]).
//!
//! Arrival-order record of the live stream. The persisted `seq` is the
//! source seq, verbatim; the replay ordering key is `ord`, a tap-local,
//! strictly monotonic append ordinal unique across shards and process
//! lifetimes (`ord INTEGER PRIMARY KEY` on every shard table).
//!
//! # Schema (one file per log; `PRAGMA user_version` = 20 — the `2x` band,
//! disjoint from the cache's `1x` band, so opening the wrong file type refuses)
//!
//! - `meta` — one row: `next_shard`, `next_ord`, upserted inside **every**
//!   commit, so a crash resumes the counters exactly (tighter than the
//!   prior backend's batch reservation; still satisfies "gaps allowed,
//!   reuse never").
//! - `streams` — registry `(id, provider, asset_class, symbol, kind_tag,
//!   shard_table)`; drives write-path shard resolution and replay
//!   enumeration. (`shard_table`, not `table`: `table` is an SQL keyword.)
//! - `tap_NNNNNN` — one shard table per `(instrument, kind)`, homogeneous
//!   per-kind rows keyed by `ord`.
//!
//! # Durability (load-bearing — spec constraint 2)
//!
//! `append` enqueues; the writer task drains the queue into one open
//! transaction and COMMITs (with `synchronous=FULL`, an fsync) whenever the
//! queue is momentarily empty and always before acking a `flush`. So: a
//! completed `flush` survives SIGKILL; between flushes, durability rides the
//! queue-drain commits (near-per-event at low rates, batched under load).

use std::collections::HashMap;
use std::path::Path;

use async_trait::async_trait;
use datamancer_core::{
    Bar, Error, EventKind, Instrument, MarketEvent, Price, ProviderId, Quantity, Quote,
    ReplayRequest, ReplaySource, Result, Seq, TapLog, Timestamp, Trade,
};
use futures::stream::{self, BoxStream, StreamExt};
use tokio::sync::{mpsc, oneshot};

use super::turso_common::{
    DbLocation, asset_class_from_tag, asset_class_tag, connect, event_columns, execute_retry,
    map_err, open_database, preflight_user_version, stamp_user_version,
};

/// `PRAGMA user_version` for the tap log's schema. Deliberately disjoint from
/// the cache's range (tap log uses the `2x` band, cache the `1x` band) so a
/// file opened as the wrong store type is refused by the version guard
/// instead of silently colliding.
const TAP_SCHEMA_VERSION: i64 = 20;

/// Where the tap log is stored. Mirrors `TursoCacheConfig`.
#[derive(Clone, Debug)]
pub enum TursoTapLogConfig {
    /// In-process, ephemeral. Good for tests.
    Memory,
    /// A database file at `path` (parent directories created if absent).
    Embedded { path: std::path::PathBuf },
}

impl TursoTapLogConfig {
    /// Convenience: an embedded tap log at `path`.
    pub fn embedded(path: impl AsRef<Path>) -> Self {
        Self::Embedded {
            path: path.as_ref().to_path_buf(),
        }
    }
}

// ---------------------------------------------------------------------------
// Encode/decode helpers — ported verbatim from the retired tap-log module (deleted
// with it in Task 9).
// ---------------------------------------------------------------------------

fn kind_tag(kind: EventKind) -> &'static str {
    match kind {
        EventKind::Trade => "trade",
        EventKind::Quote => "quote",
        EventKind::Bar(datamancer_core::BarInterval::OneSecond) => "bar_1s",
        EventKind::Bar(datamancer_core::BarInterval::OneMinute) => "bar_1m",
        EventKind::Bar(datamancer_core::BarInterval::FiveMinute) => "bar_5m",
        EventKind::Bar(datamancer_core::BarInterval::FifteenMinute) => "bar_15m",
        EventKind::Bar(datamancer_core::BarInterval::OneHour) => "bar_1h",
        EventKind::Bar(datamancer_core::BarInterval::OneDay) => "bar_1d",
    }
}

fn kind_from_tag(tag: &str) -> Option<EventKind> {
    use datamancer_core::BarInterval;
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

/// Deterministic, **injective** record id for a `(instrument, kind)` registry
/// entry. Each component is length-prefixed (`<byte-len>:<bytes>`) before
/// concatenation, so distinct tuples can never alias onto one id even when
/// `provider` or `symbol` contains a separator character — a plain
/// delimiter-join (`a|b|c`) could collide two different streams onto a single
/// `streams` record and corrupt shard resolution after a reopen.
///
/// (Only the *shard table name* must be a plain token, which is why shards
/// are allocated as `tap_NNNNNN`; this id is just the `streams` primary key.)
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

/// A persisted shard name must be exactly the generated `tap_` + digits form
/// before it is interpolated into SQL as an identifier — `streams.shard_table`
/// is data, and a tampered or corrupted row must not become executable SQL.
fn is_valid_shard_name(name: &str) -> bool {
    name.strip_prefix("tap_")
        .is_some_and(|d| !d.is_empty() && d.bytes().all(|b| b.is_ascii_digit()))
}

/// Per-shard INSERT statement, formatted **once per shard** (cached in the
/// writer's shard map) rather than per event — the write path runs on every
/// live market event.
fn insert_sql(shard: &str, kind: EventKind) -> String {
    match kind {
        EventKind::Trade => format!(
            "INSERT INTO {shard} (ord, seq, source_ts, rx_ts, price_raw, size_raw) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)"
        ),
        EventKind::Quote => format!(
            "INSERT INTO {shard} (ord, seq, source_ts, rx_ts, bid_raw, \
             bid_size_raw, ask_raw, ask_size_raw) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)"
        ),
        EventKind::Bar(_) => format!(
            "INSERT INTO {shard} (ord, seq, source_ts, rx_ts, open_raw, high_raw, \
             low_raw, close_raw, volume_raw) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)"
        ),
    }
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

/// `meta.next_shard` is persisted by batch commits, not by the autocommit
/// CREATE TABLE + registry INSERT pair in `ensure_shard`, so a crash in that
/// window reopens with a stale counter. Take the floor from the shard tables
/// that actually exist: reusing a live ordinal would point two registry rows
/// at one shard table and cross-attribute their events on replay.
async fn recompute_next_shard(conn: &::turso::Connection, persisted: u64) -> Result<u64> {
    let mut floor = persisted;
    let mut rows = conn
        .query(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'tap_%'",
            (),
        )
        .await
        .map_err(map_err)?;
    while let Some(row) = rows.next().await.map_err(map_err)? {
        let name: String = row.get(0).map_err(map_err)?;
        if let Some(n) = name
            .strip_prefix("tap_")
            .and_then(|d| d.parse::<u64>().ok())
        {
            floor = floor.max(n.saturating_add(1));
        }
    }
    Ok(floor)
}

// ---------------------------------------------------------------------------
// Writer command channel
// ---------------------------------------------------------------------------

enum WriteCmd {
    Event(MarketEvent),
    Flush(oneshot::Sender<Result<()>>),
}

/// Turso-backed tap log.
pub struct TursoTapLog {
    db: ::turso::Database,
    tx: mpsc::UnboundedSender<WriteCmd>,
}

impl TursoTapLog {
    /// Open the tap log, creating the `meta`/`streams` tables on first use.
    ///
    /// # Errors
    ///
    /// Returns `Error::Storage` if the engine fails to open, schema creation
    /// fails, or the file's `user_version` does not match this build.
    pub async fn open(cfg: TursoTapLogConfig) -> Result<Self> {
        let location = match cfg {
            TursoTapLogConfig::Memory => DbLocation::Memory,
            TursoTapLogConfig::Embedded { path } => DbLocation::File(path),
        };
        let db = open_database(&location).await?;
        let conn = connect(&db).await?;
        // Version preflight BEFORE any DDL: a cache file (or a
        // wrong-generation tap log) is refused without polluting it with
        // `meta`/`streams`. Stamp only after the schema exists.
        let fresh = preflight_user_version(&conn, TAP_SCHEMA_VERSION, "tap log").await?;
        execute_retry(
            &conn,
            "CREATE TABLE IF NOT EXISTS meta (id INTEGER PRIMARY KEY CHECK (id = 0), \
             next_shard INTEGER NOT NULL, next_ord INTEGER NOT NULL)",
            (),
        )
        .await?;
        execute_retry(
            &conn,
            "CREATE TABLE IF NOT EXISTS streams (id TEXT PRIMARY KEY, \
             provider TEXT NOT NULL, asset_class TEXT NOT NULL, symbol TEXT NOT NULL, \
             kind_tag TEXT NOT NULL, shard_table TEXT NOT NULL)",
            (),
        )
        .await?;
        if fresh {
            stamp_user_version(&conn, TAP_SCHEMA_VERSION).await?;
        }

        // Load counters + registry (shard tables persist across reopen; no
        // re-DDL needed, unlike the prior backend's re-DEFINE quirk). Both queries
        // fully drain their `Rows` cursor to `None` in an inner scope before
        // the connection is handed to the writer task — an un-drained cursor
        // silently swallows the next same-connection write under turso 0.6.1
        // (see `turso_common::preflight_user_version`).
        let (next_shard, next_ord) = {
            let mut rows = conn
                .query("SELECT next_shard, next_ord FROM meta WHERE id = 0", ())
                .await
                .map_err(map_err)?;
            let counters = match rows.next().await.map_err(map_err)? {
                Some(row) => {
                    let shard: i64 = row.get(0).map_err(map_err)?;
                    let ord: i64 = row.get(1).map_err(map_err)?;
                    (shard.cast_unsigned(), ord.cast_unsigned())
                }
                None => (0, 0),
            };
            // Drain any remaining rows (there should be at most one, `id = 0`
            // being the primary key) before the cursor drops.
            while rows.next().await.map_err(map_err)?.is_some() {}
            counters
        };
        let mut shards: HashMap<Instrument, HashMap<EventKind, String>> = HashMap::new();
        {
            let mut rows = conn
                .query(
                    "SELECT provider, asset_class, symbol, kind_tag, shard_table FROM streams",
                    (),
                )
                .await
                .map_err(map_err)?;
            while let Some(row) = rows.next().await.map_err(map_err)? {
                let provider: String = row.get(0).map_err(map_err)?;
                let asset_class: String = row.get(1).map_err(map_err)?;
                let symbol: String = row.get(2).map_err(map_err)?;
                let kind_tag_s: String = row.get(3).map_err(map_err)?;
                let shard_table: String = row.get(4).map_err(map_err)?;
                let (Some(asset), Some(kind)) = (
                    asset_class_from_tag(&asset_class),
                    kind_from_tag(&kind_tag_s),
                ) else {
                    continue;
                };
                if !is_valid_shard_name(&shard_table) {
                    tracing::warn!(provider = %provider, symbol = %symbol,
                        shard_table = %shard_table,
                        "skipping streams row whose shard_table is not a generated tap_N name");
                    continue;
                }
                let instrument = Instrument::new(ProviderId::new(provider), asset, &symbol);
                shards
                    .entry(instrument)
                    .or_default()
                    .insert(kind, insert_sql(&shard_table, kind));
            }
            // `while let ... = rows.next()` above already drains to `None`.
        }

        let next_shard = recompute_next_shard(&conn, next_shard).await?;

        let (tx, rx) = mpsc::unbounded_channel();
        let writer = Writer {
            conn,
            next_shard,
            next_ord,
            shards,
            tx_open: false,
            last_error: None,
        };
        tokio::spawn(writer.run(rx));
        Ok(Self { db, tx })
    }
}

#[async_trait]
impl TapLog for TursoTapLog {
    async fn append(&self, ev: &MarketEvent) -> Result<()> {
        // Unbounded, non-blocking enqueue: the live stream never waits on
        // disk. A send error means the writer task is gone (log being
        // dropped); that is not a live-session-fatal condition.
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
        Box::new(TursoTapReplaySource {
            db: self.db.clone(),
        })
    }
}

// ---------------------------------------------------------------------------
// Background writer — the durability core.
// ---------------------------------------------------------------------------

struct Writer {
    conn: ::turso::Connection,
    next_shard: u64,
    next_ord: u64,
    /// Per-stream prebuilt INSERT SQL (see [`insert_sql`]) — the value is the
    /// statement, not the shard name, so the hot write path never formats.
    shards: HashMap<Instrument, HashMap<EventKind, String>>,
    /// A `BEGIN` has been issued and not yet committed.
    tx_open: bool,
    last_error: Option<Error>,
}

impl Writer {
    async fn run(mut self, mut rx: mpsc::UnboundedReceiver<WriteCmd>) {
        while let Some(cmd) = rx.recv().await {
            self.handle(cmd).await;
            // Drain whatever queued behind it, then commit: the durability
            // boundary tracks the queue's momentary-empty points.
            while let Ok(cmd) = rx.try_recv() {
                self.handle(cmd).await;
            }
            if let Err(e) = self.commit_if_open().await {
                tracing::warn!(error = %e, "tap log commit failed");
                self.last_error = Some(e);
            }
        }
        let _ = self.commit_if_open().await;
    }

    async fn handle(&mut self, cmd: WriteCmd) {
        match cmd {
            WriteCmd::Event(ev) => {
                if let Err(e) = self.write_event(&ev).await {
                    tracing::warn!(error = %e, "tap log write failed");
                    self.last_error = Some(e);
                }
            }
            WriteCmd::Flush(ack) => {
                let commit_res = self.commit_if_open().await;
                // Report the freshest failure and clear the buffered one. A
                // commit error outranks a buffered write error: both fail the
                // flush's durability claim, but the commit fault is current
                // (the write error was already logged when it happened).
                let res = match (self.last_error.take(), commit_res) {
                    (_, Err(commit)) => Err(commit),
                    (Some(write), Ok(())) => Err(write),
                    (None, Ok(())) => Ok(()),
                };
                let _ = ack.send(res);
            }
        }
    }

    async fn begin_if_needed(&mut self) -> Result<()> {
        if !self.tx_open {
            execute_retry(&self.conn, "BEGIN", ()).await?;
            self.tx_open = true;
        }
        Ok(())
    }

    /// Persist the counters and COMMIT. On failure, roll back (the batch's
    /// events are lost — best-effort contract; the error surfaces at flush).
    async fn commit_if_open(&mut self) -> Result<()> {
        if !self.tx_open {
            return Ok(());
        }
        self.tx_open = false;
        let persist = execute_retry(
            &self.conn,
            "INSERT OR REPLACE INTO meta (id, next_shard, next_ord) VALUES (0, ?1, ?2)",
            (self.next_shard.cast_signed(), self.next_ord.cast_signed()),
        )
        .await;
        let res = match persist {
            Ok(_) => execute_retry(&self.conn, "COMMIT", ()).await.map(|_| ()),
            Err(e) => Err(e),
        };
        if res.is_err() {
            let _ = self.conn.execute("ROLLBACK", ()).await;
        }
        res
    }

    async fn write_event(&mut self, ev: &MarketEvent) -> Result<()> {
        let (instrument, kind) = match ev {
            MarketEvent::Trade(t) => (&t.instrument, EventKind::Trade),
            MarketEvent::Quote(q) => (&q.instrument, EventKind::Quote),
            MarketEvent::Bar(b) => (&b.instrument, EventKind::Bar(b.interval)),
            // Non-data events are not tapped (defensive; the session gate
            // also filters these).
            _ => return Ok(()),
        };
        self.ensure_shard(instrument, kind).await?;
        let seq = event_seq(ev).cast_signed();
        let ord = self.next_ord.cast_signed();
        self.next_ord = self.next_ord.saturating_add(1);
        self.begin_if_needed().await?;
        let sql = self
            .shards
            .get(instrument)
            .and_then(|kinds| kinds.get(&kind))
            .ok_or_else(|| {
                Error::Storage(format!(
                    "tap log: shard for {instrument} vanished after ensure_shard"
                ))
            })?;
        match ev {
            MarketEvent::Trade(t) => {
                execute_retry(
                    &self.conn,
                    sql,
                    (
                        ord,
                        seq,
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
                    &self.conn,
                    sql,
                    (
                        ord,
                        seq,
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
                    &self.conn,
                    sql,
                    (
                        ord,
                        seq,
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
        Ok(())
    }

    /// Ensure the shard table for `(instrument, kind)` exists and its INSERT
    /// is cached (allocating on first sight). DDL cannot ride the open batch
    /// transaction safely, so a new shard commits the open batch first, then
    /// runs CREATE TABLE + registry upsert autocommit.
    async fn ensure_shard(&mut self, instrument: &Instrument, kind: EventKind) -> Result<()> {
        if self
            .shards
            .get(instrument)
            .is_some_and(|kinds| kinds.contains_key(&kind))
        {
            return Ok(());
        }
        // Refuse an asset class with no stable on-disk encoding — a row that
        // cannot round-trip would orphan the shard on reopen. (Port of the
        // prior backend's guard; see asset_class_tag.)
        if asset_class_tag(instrument.asset_class()) == "unknown" {
            return Err(Error::Storage(format!(
                "tap log: asset class of {instrument} has no stable on-disk encoding; \
                 refusing to tap it. Add the variant to asset_class_tag/asset_class_from_tag \
                 in lockstep."
            )));
        }
        self.commit_if_open().await?;
        let ordinal = self.next_shard;
        self.next_shard += 1;
        let name = format!("tap_{ordinal:06}");
        // A crash anywhere between here and the next batch commit leaves
        // `meta.next_shard` stale; reopen recomputes the counter from
        // `sqlite_master` (see `open`), so the ordinal is never reused — a
        // half-created shard (table without a registry row, or with one) is
        // left behind as a harmless dead table rather than being handed to a
        // different stream.
        execute_retry(
            &self.conn,
            &format!(
                "CREATE TABLE IF NOT EXISTS {name} (ord INTEGER PRIMARY KEY, \
                 seq INTEGER NOT NULL, source_ts INTEGER NOT NULL, \
                 rx_ts INTEGER NOT NULL, {})",
                event_columns(kind)
            ),
            (),
        )
        .await?;
        execute_retry(
            &self.conn,
            "INSERT OR REPLACE INTO streams \
             (id, provider, asset_class, symbol, kind_tag, shard_table) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            (
                registry_id(instrument, kind),
                instrument.provider().as_str(),
                asset_class_tag(instrument.asset_class()),
                instrument.symbol(),
                kind_tag(kind),
                name.as_str(),
            ),
        )
        .await?;
        self.shards
            .entry(instrument.clone())
            .or_default()
            .insert(kind, insert_sql(&name, kind));
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ReplaySource
// ---------------------------------------------------------------------------

/// Replays the tap log: enumerate `streams`, filter by requested
/// instruments/kinds, run a per-shard `source_ts`-windowed scan ordered by
/// `ord`, then merge all shards with one global sort on `ord` (unique across
/// shards and process lifetimes, so this sort *is* the k-way merge and
/// reproduces original arrival order).
struct TursoTapReplaySource {
    db: ::turso::Database,
}

#[async_trait]
#[allow(
    clippy::too_many_lines,
    reason = "linear query/decode/merge pipeline kept inline; extraction would obscure the per-kind handling"
)]
impl ReplaySource for TursoTapReplaySource {
    async fn open(&self, request: ReplayRequest) -> Result<BoxStream<'static, MarketEvent>> {
        let from = request.from.0;
        let to = request.to.0;
        if from >= to {
            return Ok(stream::empty().boxed());
        }
        let conn = connect(&self.db).await?;

        // Registry scan, filtered in memory (few streams).
        let mut regs: Vec<(Instrument, EventKind, String)> = Vec::new();
        {
            let mut rows = conn
                .query(
                    "SELECT provider, asset_class, symbol, kind_tag, shard_table FROM streams",
                    (),
                )
                .await
                .map_err(map_err)?;
            while let Some(row) = rows.next().await.map_err(map_err)? {
                let provider: String = row.get(0).map_err(map_err)?;
                let asset_class: String = row.get(1).map_err(map_err)?;
                let symbol: String = row.get(2).map_err(map_err)?;
                let kind_tag_s: String = row.get(3).map_err(map_err)?;
                let shard_table: String = row.get(4).map_err(map_err)?;
                let (Some(asset), Some(kind)) = (
                    asset_class_from_tag(&asset_class),
                    kind_from_tag(&kind_tag_s),
                ) else {
                    continue;
                };
                if !is_valid_shard_name(&shard_table) {
                    tracing::warn!(provider = %provider, symbol = %symbol,
                        shard_table = %shard_table,
                        "skipping streams row whose shard_table is not a generated tap_N name");
                    continue;
                }
                let instrument = Instrument::new(ProviderId::new(provider), asset, &symbol);
                if !request.instruments.is_empty() && !request.instruments.contains(&instrument) {
                    continue;
                }
                if !request.kinds.is_empty() && !request.kinds.contains(&kind) {
                    continue;
                }
                regs.push((instrument, kind, shard_table));
            }
        }

        // Per-shard windowed scans; each is an ord-sorted run, merged below
        // by one global sort (ord is unique across shards and lifetimes).
        let mut all: Vec<(u64, MarketEvent)> = Vec::new();
        for (instrument, kind, shard) in regs {
            match kind {
                EventKind::Trade => {
                    let mut rows = conn
                        .query(
                            &format!(
                                "SELECT ord, seq, source_ts, rx_ts, price_raw, size_raw \
                                 FROM {shard} WHERE source_ts >= ?1 AND source_ts < ?2 \
                                 ORDER BY ord ASC"
                            ),
                            (from, to),
                        )
                        .await
                        .map_err(map_err)?;
                    while let Some(row) = rows.next().await.map_err(map_err)? {
                        let ord: i64 = row.get(0).map_err(map_err)?;
                        let seq: i64 = row.get(1).map_err(map_err)?;
                        let size_raw: i64 = row.get(5).map_err(map_err)?;
                        all.push((
                            ord.cast_unsigned(),
                            MarketEvent::Trade(Trade {
                                instrument: instrument.clone(),
                                source_ts: Timestamp(row.get(2).map_err(map_err)?),
                                rx_ts: Timestamp(row.get(3).map_err(map_err)?),
                                seq: Seq(seq.cast_unsigned()),
                                price: Price::from_raw(row.get(4).map_err(map_err)?),
                                size: Quantity::from_raw(size_raw.cast_unsigned()),
                            }),
                        ));
                    }
                }
                EventKind::Quote => {
                    let mut rows = conn
                        .query(
                            &format!(
                                "SELECT ord, seq, source_ts, rx_ts, bid_raw, bid_size_raw, \
                                 ask_raw, ask_size_raw FROM {shard} \
                                 WHERE source_ts >= ?1 AND source_ts < ?2 ORDER BY ord ASC"
                            ),
                            (from, to),
                        )
                        .await
                        .map_err(map_err)?;
                    while let Some(row) = rows.next().await.map_err(map_err)? {
                        let ord: i64 = row.get(0).map_err(map_err)?;
                        let seq: i64 = row.get(1).map_err(map_err)?;
                        let bid_size: i64 = row.get(5).map_err(map_err)?;
                        let ask_size: i64 = row.get(7).map_err(map_err)?;
                        all.push((
                            ord.cast_unsigned(),
                            MarketEvent::Quote(Quote {
                                instrument: instrument.clone(),
                                source_ts: Timestamp(row.get(2).map_err(map_err)?),
                                rx_ts: Timestamp(row.get(3).map_err(map_err)?),
                                seq: Seq(seq.cast_unsigned()),
                                bid: Price::from_raw(row.get(4).map_err(map_err)?),
                                bid_size: Quantity::from_raw(bid_size.cast_unsigned()),
                                ask: Price::from_raw(row.get(6).map_err(map_err)?),
                                ask_size: Quantity::from_raw(ask_size.cast_unsigned()),
                            }),
                        ));
                    }
                }
                EventKind::Bar(interval) => {
                    let mut rows = conn
                        .query(
                            &format!(
                                "SELECT ord, seq, source_ts, rx_ts, open_raw, high_raw, \
                                 low_raw, close_raw, volume_raw FROM {shard} \
                                 WHERE source_ts >= ?1 AND source_ts < ?2 ORDER BY ord ASC"
                            ),
                            (from, to),
                        )
                        .await
                        .map_err(map_err)?;
                    while let Some(row) = rows.next().await.map_err(map_err)? {
                        let ord: i64 = row.get(0).map_err(map_err)?;
                        let seq: i64 = row.get(1).map_err(map_err)?;
                        let volume_raw: i64 = row.get(8).map_err(map_err)?;
                        all.push((
                            ord.cast_unsigned(),
                            MarketEvent::Bar(Bar {
                                instrument: instrument.clone(),
                                interval,
                                source_ts: Timestamp(row.get(2).map_err(map_err)?),
                                rx_ts: Timestamp(row.get(3).map_err(map_err)?),
                                seq: Seq(seq.cast_unsigned()),
                                open: Price::from_raw(row.get(4).map_err(map_err)?),
                                high: Price::from_raw(row.get(5).map_err(map_err)?),
                                low: Price::from_raw(row.get(6).map_err(map_err)?),
                                close: Price::from_raw(row.get(7).map_err(map_err)?),
                                volume: Quantity::from_raw(volume_raw.cast_unsigned()),
                            }),
                        ));
                    }
                }
            }
        }

        // One sort by the globally unique append ordinal IS the k-way merge:
        // it reproduces original arrival order across shards and symbols.
        all.sort_by_key(|(ord, _)| *ord);
        Ok(stream::iter(all.into_iter().map(|(_, ev)| ev)).boxed())
    }
}

#[cfg(test)]
mod tests {
    use datamancer_core::{
        AssetClass, Instrument, MarketEvent, Price, ProviderId, Quantity, Quote, Seq, TapLog as _,
        Timestamp, Trade,
    };

    use super::super::turso::{TursoCache, TursoCacheConfig};
    use super::super::turso_common::{DbLocation, connect, open_database};
    use super::{TursoTapLog, TursoTapLogConfig};

    fn inst() -> Instrument {
        Instrument::new(
            ProviderId::from_static("alpaca"),
            AssetClass::Equity,
            "AAPL",
        )
    }

    fn trade(n: i64) -> MarketEvent {
        MarketEvent::Trade(Trade {
            instrument: inst(),
            source_ts: Timestamp(n),
            rx_ts: Timestamp(n),
            seq: Seq(n.cast_unsigned()),
            price: Price::from_units(100),
            size: Quantity::from_units(1),
        })
    }

    fn quote(n: i64) -> MarketEvent {
        MarketEvent::Quote(Quote {
            instrument: inst(),
            source_ts: Timestamp(n),
            rx_ts: Timestamp(n),
            seq: Seq(n.cast_unsigned()),
            bid: Price::from_units(99),
            bid_size: Quantity::from_units(1),
            ask: Price::from_units(101),
            ask_size: Quantity::from_units(1),
        })
    }

    /// A cache file must be refused by the version preflight BEFORE any DDL:
    /// the refusal must not pollute the file with `meta`/`streams`.
    #[tokio::test]
    async fn cache_file_is_refused_before_any_tap_ddl() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cache.db");
        drop(
            TursoCache::open(TursoCacheConfig::embedded(&path))
                .await
                .unwrap(),
        );

        let Err(err) = TursoTapLog::open(TursoTapLogConfig::embedded(&path)).await else {
            panic!("a cache file must refuse to open as a tap log");
        };
        assert!(
            err.to_string().contains("schema version"),
            "unexpected error: {err}"
        );

        let db = open_database(&DbLocation::File(path)).await.unwrap();
        let conn = connect(&db).await.unwrap();
        let mut rows = conn
            .query(
                "SELECT name FROM sqlite_master WHERE name IN ('meta', 'streams')",
                (),
            )
            .await
            .unwrap();
        assert!(
            rows.next().await.unwrap().is_none(),
            "tap-log DDL leaked into a cache file"
        );
    }

    /// A crash between shard allocation and the next batch commit leaves
    /// `meta.next_shard` stale; reopen must recompute it from the shard
    /// tables that exist rather than hand a live ordinal to a new stream.
    #[tokio::test]
    async fn stale_next_shard_is_recomputed_on_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tap.db");

        // Stream A allocates tap_000000 and commits.
        {
            let log = TursoTapLog::open(TursoTapLogConfig::embedded(&path))
                .await
                .unwrap();
            log.append(&trade(1)).await.unwrap();
            log.flush().await.unwrap();
        }

        // Simulate the crash window: the shard exists and is registered, but
        // the persisted counter never advanced.
        {
            let db = open_database(&DbLocation::File(path.clone()))
                .await
                .unwrap();
            let conn = connect(&db).await.unwrap();
            conn.execute("UPDATE meta SET next_shard = 0 WHERE id = 0", ())
                .await
                .unwrap();
        }

        // Stream B (same instrument, different kind) must get a fresh shard,
        // not tap_000000.
        {
            let log = TursoTapLog::open(TursoTapLogConfig::embedded(&path))
                .await
                .unwrap();
            log.append(&quote(2)).await.unwrap();
            log.flush().await.unwrap();
        }

        let db = open_database(&DbLocation::File(path)).await.unwrap();
        let conn = connect(&db).await.unwrap();
        let mut rows = conn
            .query(
                "SELECT kind_tag, shard_table FROM streams ORDER BY kind_tag",
                (),
            )
            .await
            .unwrap();
        let mut seen = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            let kind_tag: String = row.get(0).unwrap();
            let shard: String = row.get(1).unwrap();
            seen.push((kind_tag, shard));
        }
        assert_eq!(
            seen,
            vec![
                ("quote".to_string(), "tap_000001".to_string()),
                ("trade".to_string(), "tap_000000".to_string()),
            ],
            "the stale counter must not hand tap_000000 to a second stream"
        );
    }
}
