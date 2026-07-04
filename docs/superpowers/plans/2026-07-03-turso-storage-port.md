# Turso Storage Backend Port — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the SurrealDB storage backend with a Turso-based one (same `HistoricalCache`/`TapLog`/`ReplaySource` semantics), then delete `storage-surreal` and its `deny.toml` transitional block.

**Architecture:** Two new backend modules (`storage/turso.rs` cache, `storage/turso_tap_log.rs` tap log) over a shared plumbing module (`storage/turso_common.rs`) and an extracted pure coverage-segment module (`storage/coverage.rs`). All writes per database file go through exactly one connection (cache: a mutex-guarded write connection; tap log: the writer task's own connection) because turso 0.6.1 surfaces overlapping writers as an immediate `Busy` and was observed to occasionally wedge — a bounded busy-retry converts any residual conflict into a loud `Error::Storage`, never a hang. Durability rides `PRAGMA synchronous=FULL` + WAL; the tap-log writer commits a transaction at every queue-drain/flush boundary so a completed `flush` survives SIGKILL.

**Tech Stack:** `turso = "0.6"` with `default-features = false` (drops tantivy/FTS and mimalloc; spike-verified the full access pattern still passes), `serde_json` for the coverage-segments column, SQLite-compatible SQL subset only (spec constraint 1).

**Spec:** `docs/superpowers/specs/2026-07-03-turso-storage-design.md`. Spike evidence: scratchpad project `turso-spike` (validated: WAL default, `synchronous=FULL`, `INSERT OR REPLACE`, `BEGIN/COMMIT`, half-open range SELECT/DELETE, composite-index query plan, >2^53 integer round-trip, reopen persistence, kill-durability, and the Busy behavior described above).

## Global Constraints

- **SQLite-compatible SQL subset only** — `CREATE TABLE`, `CREATE INDEX`, `INSERT`, half-open-range `SELECT`/`DELETE`, transactions. No engine-specific SQL (spec constraint 1, binding).
- **Crash-durability tests on the tap log before `storage-turso` becomes a default feature** (spec constraint 2, binding). Task 8 must land before Task 9.
- `turso` dependency: `version = "0.6"`, `default-features = false`, optional.
- **Single-writer discipline per database file**; all busy-retries bounded (no unbounded loops).
- Workspace lints (`clippy::pedantic = deny`) and `#![forbid(unsafe_code)]` apply; run `cargo clippy --all-targets --all-features -- -D warnings` before every commit.
- Semantics ported **1:1** from `surreal.rs`/`surreal_tap_log.rs`: per-kind tables, coverage intersection/gaps, store-claims-whole-key-range, adjustment-mode scoping, source `seq` persisted verbatim, `ord` as the tap replay ordering key, cache replay emits `Seq(0)`.
- Cutover (Task 9) deletes the surreal backend, its feature, the `deny.toml` transitional exceptions/ignores, and the surreal-specific daemon config tokens — all in one change.
- API-uncertainty rule: the turso Rust API surface used here (`Builder::new_local`, `Connection::{execute, query}`, `Rows::next`, `Row::{get, get_value, column_count}`, tuple params) is spike-verified. If a trait path differs (e.g. `IntoParams`' module), check docs.rs/turso/0.6.1 and adjust the signature — do not change the design.
- u64 ↔ SQLite INTEGER: store `u64` fields (`size_raw`, `volume_raw`, `seq`, `ord`, counters) as `i64` via `.cast_signed()`, read back via `.cast_unsigned()` — bit-preserving round-trip. None of these columns is range-compared in SQL except `source_ts` (already `i64`) and `ord` (an append counter that never plausibly exceeds `i64::MAX`, and `Seq::SYNTHETIC` is never tapped — controls are not tapped at all).

---

### Task 1: Feature scaffolding + shared turso plumbing

**Files:**
- Modify: `crates/datamancer/Cargo.toml` (features + deps)
- Modify: `crates/datamancer/src/storage/mod.rs`
- Create: `crates/datamancer/src/storage/turso_common.rs`

**Interfaces:**
- Consumes: nothing (first task).
- Produces (used by Tasks 3–7):
  - `pub(crate) fn map_err(err: ::turso::Error) -> Error`
  - `pub(crate) async fn open_database(location: &DbLocation) -> Result<::turso::Database>` with `pub(crate) enum DbLocation { Memory, File(std::path::PathBuf) }`
  - `pub(crate) async fn connect(db: &::turso::Database) -> Result<::turso::Connection>` (applies `PRAGMA synchronous=FULL`)
  - `pub(crate) async fn execute_retry(conn: &::turso::Connection, sql: &str, params: impl ::turso::IntoParams + Clone) -> Result<u64>`
  - `pub(crate) async fn check_or_stamp_user_version(conn: &::turso::Connection, expected: i64, store: &str) -> Result<()>`

- [ ] **Step 1: Add the feature and dependency**

In `crates/datamancer/Cargo.toml`, add to `[features]` (leave `default` unchanged — the flip happens in Task 9):

```toml
storage-turso = ["dep:turso", "dep:serde", "dep:serde_json"]
```

Add to `[dependencies]` (new section comment mirroring the existing `# storage-surreal` one):

```toml
# storage-turso
turso = { version = "0.6", optional = true, default-features = false }
serde_json = { workspace = true, optional = true }
```

Note `serde` is already an optional dep (used by storage-surreal); `serde_json` moves from dev-only to also-optional-normal. Keep the `[dev-dependencies] serde_json` line as is (workspace dep, same version).

- [ ] **Step 2: Register the module**

Append to `crates/datamancer/src/storage/mod.rs`:

```rust
#[cfg(feature = "storage-turso")]
pub(crate) mod turso_common;
```

- [ ] **Step 3: Write `turso_common.rs` with its unit tests**

Create `crates/datamancer/src/storage/turso_common.rs`:

```rust
//! Shared plumbing for the Turso-backed stores: database open, connection
//! init, bounded busy-retry, and the `PRAGMA user_version` schema guard.
//!
//! # Single-writer discipline (load-bearing)
//!
//! turso 0.6.1 surfaces an overlapping writer on a second connection as an
//! **immediate** `Busy` error (`PRAGMA busy_timeout` is accepted but not
//! honored), and the evaluation spike twice observed a wedge where the lock
//! never cleared for a fresh connection. Both stores therefore route every
//! write through exactly one connection (cache: mutex-guarded; tap log: the
//! writer task's own), and [`execute_retry`] bounds any residual conflict so
//! a wedge becomes a loud [`Error::Storage`], never a hang.

use std::path::PathBuf;
use std::time::Duration;

use datamancer_core::{Error, Result};

/// Bounded busy-retry budget: writes are serialized by design, so `Busy`
/// here is unexpected; retry briefly, then fail loudly.
const BUSY_RETRIES: u32 = 200;
const BUSY_BACKOFF: Duration = Duration::from_millis(5);

/// Where a turso database lives.
pub(crate) enum DbLocation {
    Memory,
    File(PathBuf),
}

pub(crate) fn map_err(err: ::turso::Error) -> Error {
    Error::Storage(format!("turso: {err}"))
}

/// turso 0.6.1 renders write-lock conflicts as "database is locked"; match on
/// the message rather than the error variant so a variant rename in a patch
/// release degrades to no-retry instead of a compile error.
fn is_busy(err: &::turso::Error) -> bool {
    err.to_string().contains("locked")
}

/// Open (creating parent directories for the file case — `new_local` creates
/// the file but not its directories).
pub(crate) async fn open_database(location: &DbLocation) -> Result<::turso::Database> {
    let path = match location {
        DbLocation::Memory => ":memory:".to_string(),
        DbLocation::File(path) => {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| Error::Storage(format!("create {}: {e}", parent.display())))?;
            }
            path.to_str()
                .ok_or_else(|| {
                    Error::Storage(format!("non-UTF-8 database path: {}", path.display()))
                })?
                .to_string()
        }
    };
    ::turso::Builder::new_local(&path)
        .build()
        .await
        .map_err(map_err)
}

/// New connection with the durability knob applied. `synchronous=FULL` fsyncs
/// the WAL on every commit — the tap log's flush contract (a completed flush
/// survives process death) depends on it. Turso supports only OFF and FULL.
pub(crate) async fn connect(db: &::turso::Database) -> Result<::turso::Connection> {
    let conn = db.connect().map_err(map_err)?;
    conn.execute("PRAGMA synchronous=FULL", ())
        .await
        .map_err(map_err)?;
    Ok(conn)
}

/// `execute` with a bounded retry on write-lock conflicts.
pub(crate) async fn execute_retry(
    conn: &::turso::Connection,
    sql: &str,
    params: impl ::turso::IntoParams + Clone,
) -> Result<u64> {
    let mut attempts = 0u32;
    loop {
        match conn.execute(sql, params.clone()).await {
            Ok(n) => return Ok(n),
            Err(e) if is_busy(&e) && attempts < BUSY_RETRIES => {
                attempts += 1;
                tokio::time::sleep(BUSY_BACKOFF).await;
            }
            Err(e) => {
                return Err(Error::Storage(format!(
                    "turso: {e} (after {attempts} busy retries)"
                )));
            }
        }
    }
}

/// Schema-version guard via `PRAGMA user_version` (the idiomatic SQLite
/// mechanism; supersedes the surreal backends' meta-table markers). A fresh
/// file reads 0 and is stamped; anything else must match exactly. There is no
/// pre-versioning turso lineage — version numbering starts at 1 per store.
pub(crate) async fn check_or_stamp_user_version(
    conn: &::turso::Connection,
    expected: i64,
    store: &str,
) -> Result<()> {
    let mut rows = conn
        .query("PRAGMA user_version", ())
        .await
        .map_err(map_err)?;
    let row = rows
        .next()
        .await
        .map_err(map_err)?
        .ok_or_else(|| Error::Storage("PRAGMA user_version returned no row".to_string()))?;
    let version: i64 = row.get(0).map_err(map_err)?;
    if version == expected {
        return Ok(());
    }
    if version == 0 {
        // PRAGMA assignment cannot be parameterized; `expected` is a trusted
        // compile-time constant.
        conn.execute(&format!("PRAGMA user_version = {expected}"), ())
            .await
            .map_err(map_err)?;
        return Ok(());
    }
    Err(Error::Storage(format!(
        "{store} schema version {version} does not match this build's {expected}; \
         read it with a matching build or delete the file (data is recoverable \
         from providers)"
    )))
}

#[cfg(test)]
mod tests {
    use super::{DbLocation, check_or_stamp_user_version, connect, execute_retry, open_database};

    fn assert_send_sync<T: Send + Sync>() {}

    /// The whole design holds handles across `.await` in spawned tasks; if
    /// this stops compiling the single-writer layout needs rethinking, not an
    /// `unsafe` shim.
    #[test]
    fn turso_handles_are_send_sync() {
        assert_send_sync::<::turso::Database>();
        assert_send_sync::<::turso::Connection>();
    }

    #[tokio::test]
    async fn fresh_database_is_stamped_and_reopens() {
        let dir = tempfile::tempdir().unwrap();
        let loc = DbLocation::File(dir.path().join("v.db"));
        {
            let db = open_database(&loc).await.unwrap();
            let conn = connect(&db).await.unwrap();
            check_or_stamp_user_version(&conn, 1, "test store").await.unwrap();
        }
        let db = open_database(&loc).await.unwrap();
        let conn = connect(&db).await.unwrap();
        check_or_stamp_user_version(&conn, 1, "test store").await.unwrap();
    }

    #[tokio::test]
    async fn mismatched_user_version_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let loc = DbLocation::File(dir.path().join("v.db"));
        {
            let db = open_database(&loc).await.unwrap();
            let conn = connect(&db).await.unwrap();
            check_or_stamp_user_version(&conn, 999, "test store").await.unwrap();
        }
        let db = open_database(&loc).await.unwrap();
        let conn = connect(&db).await.unwrap();
        let err = check_or_stamp_user_version(&conn, 1, "test store")
            .await
            .expect_err("mismatch must refuse");
        assert!(err.to_string().contains("999"), "unexpected error: {err}");
    }

    #[tokio::test]
    async fn execute_retry_passes_through_success_and_real_errors() {
        let db = open_database(&DbLocation::Memory).await.unwrap();
        let conn = connect(&db).await.unwrap();
        execute_retry(&conn, "CREATE TABLE t (id INTEGER PRIMARY KEY)", ())
            .await
            .unwrap();
        let err = execute_retry(&conn, "INSERT INTO nonexistent VALUES (1)", ())
            .await
            .expect_err("real SQL errors must not be retried into oblivion");
        assert!(err.to_string().contains("turso"), "unexpected error: {err}");
    }
}
```

- [ ] **Step 4: Run the tests**

Run: `cargo test -p datamancer --no-default-features --features storage-turso turso_common`
Expected: 4 passed. (If `::turso::IntoParams` is not the trait's path in 0.6, fix the import per docs.rs — the spike proved tuple params work.)

- [ ] **Step 5: Clippy + commit**

Run: `cargo clippy -p datamancer --all-targets --features storage-turso -- -D warnings`

```bash
git add crates/datamancer/Cargo.toml crates/datamancer/src/storage/mod.rs crates/datamancer/src/storage/turso_common.rs
git commit -m "feat(storage): turso feature scaffolding + shared plumbing

Bounded busy-retry, PRAGMA user_version guard, synchronous=FULL connection
init. default-features = false (no FTS/mimalloc) per the evaluation spike."
```

---

### Task 2: Extract the pure coverage-segment logic

**Files:**
- Create: `crates/datamancer/src/storage/coverage.rs`
- Modify: `crates/datamancer/src/storage/mod.rs`

**Interfaces:**
- Produces (used by Tasks 3–5):
  - `pub(crate) struct CoverageDoc { pub segments: Vec<(i64, i64)>, pub event_count: u64, pub asset_class: Option<String> }` (derives `Debug, Clone, Default, serde::Serialize, serde::Deserialize`)
  - `pub(crate) fn merge_in(&mut self, from: i64, to: i64, added_events: u64)`
  - `pub(crate) fn intersect(&self, from: i64, to: i64) -> Option<(i64, i64)>`
  - `pub(crate) fn gaps_within(&self, from: i64, to: i64) -> Vec<(i64, i64)>`

- [ ] **Step 1: Create the module by extraction**

Create `crates/datamancer/src/storage/coverage.rs` by copying, from `crates/datamancer/src/storage/surreal.rs`, the `CoverageDoc` struct (lines ~346–356) and its `impl` block (`merge_in`, `intersect`, `gaps_within`, lines ~358–425) **verbatim**, with these changes only:

- Drop the `SurrealValue` derive (keep `Debug, Clone, Default, Serialize, Deserialize`).
- Make the struct, its fields, and the three methods `pub(crate)`.
- Add module doc: `//! Pure coverage-segment bookkeeping shared by storage backends: sorted, non-overlapping half-open [from, to) segments with merge/intersect/gap queries. No I/O.`

Also copy the three coverage unit tests from `surreal.rs`'s test module (`coverage_merges_overlapping_segments`, `coverage_gaps_within_request`, `coverage_intersect_picks_widest_overlap`) into a `#[cfg(test)] mod tests` here, unchanged. Do **not** modify `surreal.rs` — it keeps its private copy until Task 9 deletes it wholesale.

Register in `storage/mod.rs`:

```rust
#[cfg(feature = "storage-turso")]
pub(crate) mod coverage;
```

- [ ] **Step 2: Run the tests**

Run: `cargo test -p datamancer --no-default-features --features storage-turso coverage`
Expected: 3 passed.

- [ ] **Step 3: Clippy + commit**

Run: `cargo clippy -p datamancer --all-targets --features storage-turso -- -D warnings`

```bash
git add crates/datamancer/src/storage/coverage.rs crates/datamancer/src/storage/mod.rs
git commit -m "refactor(storage): extract pure CoverageDoc segment logic

Verbatim from surreal.rs (which keeps its copy until deletion at cutover);
the turso cache builds on this shared module."
```

---

### Task 3: TursoCache — open, schema, store, lookup

**Files:**
- Create: `crates/datamancer/src/storage/turso.rs`
- Modify: `crates/datamancer/src/storage/mod.rs`
- Test: `crates/datamancer/tests/turso_cache.rs` (created here; grows in Tasks 4–5)
- Modify: `crates/datamancer/Cargo.toml` (test target)

**Interfaces:**
- Consumes: Task 1 helpers, Task 2 `CoverageDoc`.
- Produces:
  - `pub enum TursoCacheConfig { Memory, Embedded { path: PathBuf } }` with `pub fn embedded(path: impl AsRef<Path>) -> Self`
  - `pub struct TursoCache` with `pub async fn open(cfg: TursoCacheConfig) -> Result<Self>`, implementing `HistoricalCache` (this task: `lookup` + `store`; Task 4 adds `gaps`/`catalog`; Task 5 adds `as_replay_source`)
  - `pub(crate) fn table_for(kind: EventKind) -> &'static str`, `pub(crate) fn kind_for(table: &str) -> Option<EventKind>`, `pub(crate) fn effective_adjustment(key: &CacheKey) -> Adjustment` (same behavior as the surreal versions)

- [ ] **Step 1: Port the failing test subset**

Copy `crates/datamancer/tests/surreal_cache.rs` to `crates/datamancer/tests/turso_cache.rs`, then:

1. Change the cfg line to `#![cfg(feature = "storage-turso")]`.
2. Replace `use datamancer::storage::{SurrealCache, SurrealCacheConfig};` with `use datamancer::storage::{TursoCache, TursoCacheConfig};` and every `SurrealCache`/`SurrealCacheConfig` token with `TursoCache`/`TursoCacheConfig` (mechanical rename; no other edits — the fixtures and assertions are the parity suite).
3. Comment out (with `// TODO(task-4)` / `// TODO(task-5)` markers) every test that needs `gaps`, `catalog`, or replay: `gaps_reports_uncovered_subranges`, `fully_covered_range_reports_no_gaps`, and any test calling `as_replay_source`/`ReplayRequest` or `catalog()` (that includes `store_then_replay_round_trip_preserves_order_and_values`, `embedded_round_trip_persists_to_disk`, `store_replaces_existing_rows_in_the_claimed_range`, `bars_segregate_by_adjustment_mode` — check each test body; keep every test that only uses `open`/`lookup`/`store`, e.g. `lookup_returns_none_for_empty_cache`, `store_claims_exactly_the_key_range_not_the_event_span`, `store_of_empty_range_marks_it_covered`).

Register the test target in `crates/datamancer/Cargo.toml`:

```toml
[[test]]
name = "turso_cache"
required-features = ["storage-turso"]
```

(Also add the same `[[test]]` stanzas for the existing autodiscovered tests only if autodiscovery complains — the surreal tests rely on `#![cfg(feature = ...)]` making them empty when the feature is off, and the turso tests use the same pattern, so a `[[test]]` stanza is only needed if the build errors; prefer matching the existing pattern: check whether surreal_cache has a stanza, and mirror it.)

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p datamancer --no-default-features --features storage-turso --test turso_cache`
Expected: COMPILE ERROR — `TursoCache` not found.

- [ ] **Step 3: Implement open/schema/store/lookup**

Create `crates/datamancer/src/storage/turso.rs`:

```rust
//! Turso-backed [`HistoricalCache`] (and [`ReplaySource`]).
//!
//! Semantics ported 1:1 from the retired SurrealDB backend: one table per
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
//! identity (re-ingest overwrites) and the range-scan index the surreal
//! module doc wished for. `coverage` rows are keyed by the same
//! `"{provider}|{symbol}|{table}|{adjustment}"` string id the surreal
//! backend used, so catalog parsing is unchanged. Segments are a JSON
//! `[[from,to],…]` column. Schema version rides `PRAGMA user_version`.
//!
//! # Writes
//!
//! All mutations go through the one mutex-guarded write connection inside a
//! `BEGIN`/`COMMIT` (see `turso_common` for why single-writer is load-bearing).

use std::path::Path;

use async_trait::async_trait;
use datamancer_core::{
    Adjustment, AssetClass, Bar, BarInterval, CacheCatalogEntry, CacheCoverage, CacheKey, Error,
    EventKind, GapSpan, HistoricalCache, Instrument, MarketEvent, Price, ProviderId, Quantity,
    Quote, ReplayRequest, ReplaySource, Result, Seq, Timestamp, Trade,
};
use futures::stream::{self, BoxStream, StreamExt};
use tokio::sync::Mutex;

use super::coverage::CoverageDoc;
use super::turso_common::{
    DbLocation, check_or_stamp_user_version, connect, execute_retry, map_err, open_database,
};

/// `PRAGMA user_version` for this cache's schema. Fresh lineage (no carry-over
/// from the surreal backend's numbering).
const CACHE_SCHEMA_VERSION: i64 = 1;

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
    /// best-effort estimate the surreal backend reported.
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

    async fn load_coverage(
        conn: &::turso::Connection,
        id: &str,
    ) -> Result<Option<CoverageDoc>> {
        let mut rows = conn
            .query(
                "SELECT segments, event_count, asset_class FROM coverage WHERE id = ?1",
                (id.to_string(),),
            )
            .await
            .map_err(map_err)?;
        let Some(row) = rows.next().await.map_err(map_err)? else {
            return Ok(None);
        };
        let segments_json: String = row.get(0).map_err(map_err)?;
        let event_count: i64 = row.get(1).map_err(map_err)?;
        let asset_class: Option<String> = row.get(2).map_err(map_err)?;
        let segments: Vec<(i64, i64)> = serde_json::from_str(&segments_json)
            .map_err(|e| Error::Storage(format!("coverage segments decode: {e}")))?;
        Ok(Some(CoverageDoc {
            segments,
            event_count: event_count.cast_unsigned(),
            asset_class,
        }))
    }

    async fn count_events_in(
        conn: &::turso::Connection,
        key: &CacheKey,
        from: i64,
        to: i64,
    ) -> Result<u64> {
        let table = Self::table_for(key.kind);
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
            Ok(()) => {
                execute_retry(&write, "COMMIT", ()).await?;
                Ok(())
            }
            Err(e) => {
                // Best-effort rollback; the original error is the story.
                let _ = write.execute("ROLLBACK", ()).await;
                Err(e)
            }
        }
    }

    fn as_replay_source(&self, key: CacheKey) -> Box<dyn ReplaySource> {
        // Implemented in Task 5; this placeholder keeps the trait total.
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
    let mut doc = TursoCache::load_coverage(write, &id).await?.unwrap_or_default();
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

/// Cache replay source — fleshed out in Task 5.
struct TursoCacheReplaySource {
    db: ::turso::Database,
    key: CacheKey,
}

#[async_trait]
impl ReplaySource for TursoCacheReplaySource {
    async fn open(&self, _request: ReplayRequest) -> Result<BoxStream<'static, MarketEvent>> {
        Ok(stream::empty().boxed())
    }
}
```

Adjust to reality where the API demands it (e.g. if `::turso::Database` is not `Clone`, hold `Arc<::turso::Database>` in both structs — the design does not change). Check how the surreal backend records `asset_class` in coverage (it writes it during `update_coverage` — mirror exactly: read `crates/datamancer/src/storage/surreal.rs` `update_coverage` lines ~708-720 for the value written; it uses the instrument's asset class `Display` string, `None` never written on the new path).

Register in `storage/mod.rs`:

```rust
#[cfg(feature = "storage-turso")]
pub mod turso;

#[cfg(feature = "storage-turso")]
pub use turso::{TursoCache, TursoCacheConfig};
```

- [ ] **Step 4: Run the ported subset**

Run: `cargo test -p datamancer --no-default-features --features storage-turso --test turso_cache`
Expected: PASS for the enabled tests (`lookup_returns_none_for_empty_cache`, `store_claims_exactly_the_key_range_not_the_event_span`, `store_of_empty_range_marks_it_covered`, plus any other open/store/lookup-only tests).

Also run: `cargo test -p datamancer --no-default-features --features storage-turso` (unit tests still green).

- [ ] **Step 5: Clippy + commit**

Run: `cargo clippy -p datamancer --all-targets --features storage-turso -- -D warnings`

```bash
git add crates/datamancer/src/storage/turso.rs crates/datamancer/src/storage/mod.rs crates/datamancer/tests/turso_cache.rs crates/datamancer/Cargo.toml
git commit -m "feat(storage): TursoCache open/schema/store/lookup

Composite (provider, symbol, adjustment, source_ts) PRIMARY KEY doubles as
the range index; coverage doc ported 1:1 (merge, recount, claim-whole-range).
Writes serialized through one mutex-guarded connection in a transaction."
```

---

### Task 4: TursoCache — gaps + catalog

**Files:**
- Modify: `crates/datamancer/src/storage/turso.rs`
- Modify: `crates/datamancer/tests/turso_cache.rs` (re-enable the `TODO(task-4)` tests)

**Interfaces:**
- Consumes: Task 3's `TursoCache`, `load_coverage`, `kind_for`, `bytes_per_row`.
- Produces: `HistoricalCache::gaps` and `HistoricalCache::catalog` overrides on `TursoCache`.

- [ ] **Step 1: Re-enable the gaps/catalog tests and watch them fail**

Un-comment `gaps_reports_uncovered_subranges` and `fully_covered_range_reports_no_gaps` (and any catalog test present in the ported file) in `tests/turso_cache.rs`.

Run: `cargo test -p datamancer --no-default-features --features storage-turso --test turso_cache`
Expected: FAIL — the default `gaps` derives only fringe gaps from `lookup` (mid-range holes missing), so `gaps_reports_uncovered_subranges` asserts wrong values.

- [ ] **Step 2: Implement the overrides**

Add to the `impl HistoricalCache for TursoCache` block (port of the surreal bodies, lines ~569–646 of `surreal.rs`):

```rust
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
```

And the free function (port of `surreal.rs` lines ~667–676):

```rust
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
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p datamancer --no-default-features --features storage-turso --test turso_cache`
Expected: PASS (all enabled tests, including the two gap tests).

- [ ] **Step 4: Clippy + commit**

Run: `cargo clippy -p datamancer --all-targets --features storage-turso -- -D warnings`

```bash
git add crates/datamancer/src/storage/turso.rs crates/datamancer/tests/turso_cache.rs
git commit -m "feat(storage): TursoCache gaps + catalog

Mid-range hole enumeration from the coverage doc; whole-cache catalog with
the same |-separated id parsing and skip-don't-panic malformed-row handling."
```

---

### Task 5: TursoCache — ReplaySource + full cache parity suite

**Files:**
- Modify: `crates/datamancer/src/storage/turso.rs` (flesh out `TursoCacheReplaySource`)
- Modify: `crates/datamancer/tests/turso_cache.rs` (re-enable all remaining tests)

**Interfaces:**
- Consumes: Task 3's structs and helpers.
- Produces: working `HistoricalCache::as_replay_source` — replay in `source_ts ASC` order, `Seq(0)` on every event, request intersected with the key (instruments, kinds, from/to).

- [ ] **Step 1: Re-enable all remaining tests, watch replay tests fail**

Un-comment every remaining test in `tests/turso_cache.rs`.

Run: `cargo test -p datamancer --no-default-features --features storage-turso --test turso_cache`
Expected: FAIL — replay round-trip tests get an empty stream from the Task-3 placeholder.

- [ ] **Step 2: Implement the replay source**

Replace the placeholder `impl ReplaySource for TursoCacheReplaySource` body with the port of `surreal.rs`'s `SurrealReplaySource::open` (lines ~755–920). Structure:

```rust
#[async_trait]
impl ReplaySource for TursoCacheReplaySource {
    async fn open(&self, request: ReplayRequest) -> Result<BoxStream<'static, MarketEvent>> {
        // ReplayRequest may narrow the cache key; intersect from/to,
        // instruments, and kinds exactly as the surreal source did.
        let kind = self.key.kind;
        let from = request.from.0.max(self.key.from.0);
        let to = request.to.0.min(self.key.to.0);
        let instrument_matches = request.instruments.is_empty()
            || request.instruments.contains(&self.key.instrument);
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
            TursoCache::effective_adjustment(&self.key).as_str().to_string(),
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
```

- [ ] **Step 3: Run the full cache parity suite**

Run: `cargo test -p datamancer --no-default-features --features storage-turso --test turso_cache`
Expected: PASS — every test from the surreal suite, including `embedded_round_trip_persists_to_disk` (which also exercises the reopen + user_version happy path) and `bars_segregate_by_adjustment_mode`.

- [ ] **Step 4: Clippy + commit**

Run: `cargo clippy -p datamancer --all-targets --features storage-turso -- -D warnings`

```bash
git add crates/datamancer/src/storage/turso.rs crates/datamancer/tests/turso_cache.rs
git commit -m "feat(storage): TursoCache replay source; full cache parity suite green

source_ts-ordered replay with Seq(0) (session re-stamps), request
intersected with the key exactly as the surreal source did."
```

---

### Task 6: TursoTapLog — open, writer task, append/flush

**Files:**
- Create: `crates/datamancer/src/storage/turso_tap_log.rs`
- Modify: `crates/datamancer/src/storage/mod.rs`
- Test: `crates/datamancer/tests/turso_tap_log.rs` (created here; replay tests enabled in Task 7)
- Modify: `crates/datamancer/Cargo.toml` (test target)

**Interfaces:**
- Consumes: Task 1 helpers.
- Produces:
  - `pub enum TursoTapLogConfig { Memory, Embedded { path: PathBuf } }` with `pub fn embedded(path: impl AsRef<Path>) -> Self`
  - `pub struct TursoTapLog` with `pub async fn open(cfg: TursoTapLogConfig) -> Result<Self>`, implementing `TapLog` (`append` non-blocking enqueue; `flush` = durability barrier: the writer **commits its open transaction** before acking; `as_replay_source` returns the Task-7 source)

- [ ] **Step 1: Port the failing test subset**

Copy `crates/datamancer/tests/surreal_tap_log.rs` to `crates/datamancer/tests/turso_tap_log.rs`; change the cfg to `#![cfg(feature = "storage-turso")]`, rename `SurrealTapLog` → `TursoTapLog` and `SurrealTapLogConfig` → `TursoTapLogConfig` throughout (mechanical; keep every fixture and assertion). Comment out with `// TODO(task-7)` every test that replays (`replay_count` helper users, `as_replay_source` callers) — for this task keep only tests exercising open/append/flush without replay (`open_empty_log_replays_nothing` needs replay: comment it too; if every test replays, keep just this new one):

```rust
#[tokio::test]
async fn append_then_flush_reports_ok() {
    let log = TursoTapLog::open(TursoTapLogConfig::Memory).await.unwrap();
    log.append(&trade("AAPL", 100, 100, 0, 150.10, 1)).await.unwrap();
    log.flush().await.unwrap();
}
```

Add the test target:

```toml
[[test]]
name = "turso_tap_log"
required-features = ["storage-turso"]
```

Run: `cargo test -p datamancer --no-default-features --features storage-turso --test turso_tap_log`
Expected: COMPILE ERROR — `TursoTapLog` not found.

- [ ] **Step 2: Implement the tap log**

Create `crates/datamancer/src/storage/turso_tap_log.rs`. Port `surreal_tap_log.rs` semantics with these turso-specific translations (everything else — registry ids, kind/asset-class tags, unknown-asset-class refusal, seq-verbatim, best-effort writes with error surfaced at flush — verbatim):

```rust
//! Turso-backed [`TapLog`] (and [`ReplaySource`]).
//!
//! Arrival-order record of the live stream. The persisted `seq` is the
//! source seq, verbatim; the replay ordering key is `ord`, a tap-local,
//! strictly monotonic append ordinal unique across shards and process
//! lifetimes (`ord INTEGER PRIMARY KEY` on every shard table).
//!
//! # Schema (one file per log; `PRAGMA user_version` = 1)
//!
//! - `meta` — one row: `next_shard`, `next_ord`, upserted inside **every**
//!   commit, so a crash resumes the counters exactly (tighter than the
//!   surreal backend's batch reservation; still satisfies "gaps allowed,
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
    AssetClass, Bar, BarInterval, Error, EventKind, Instrument, MarketEvent, Price, ProviderId,
    Quantity, Quote, ReplayRequest, ReplaySource, Result, Seq, TapLog, Timestamp, Trade,
};
use futures::stream::{self, BoxStream, StreamExt};
use tokio::sync::{mpsc, oneshot};

use super::turso_common::{
    DbLocation, check_or_stamp_user_version, connect, execute_retry, map_err, open_database,
};

const TAP_SCHEMA_VERSION: i64 = 1;

/// Where the tap log is stored. Mirrors `TursoCacheConfig`.
#[derive(Clone, Debug)]
pub enum TursoTapLogConfig {
    Memory,
    Embedded { path: std::path::PathBuf },
}

impl TursoTapLogConfig {
    pub fn embedded(path: impl AsRef<Path>) -> Self {
        Self::Embedded { path: path.as_ref().to_path_buf() }
    }
}

enum WriteCmd {
    Event(MarketEvent),
    Flush(oneshot::Sender<Result<()>>),
}

pub struct TursoTapLog {
    db: ::turso::Database,
    tx: mpsc::UnboundedSender<WriteCmd>,
}

impl TursoTapLog {
    pub async fn open(cfg: TursoTapLogConfig) -> Result<Self> {
        let location = match cfg {
            TursoTapLogConfig::Memory => DbLocation::Memory,
            TursoTapLogConfig::Embedded { path } => DbLocation::File(path),
        };
        let db = open_database(&location).await?;
        let conn = connect(&db).await?;
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
        check_or_stamp_user_version(&conn, TAP_SCHEMA_VERSION, "tap log").await?;

        // Load counters + registry (shard tables persist across reopen; no
        // re-DDL needed, unlike SurrealDB's re-DEFINE quirk).
        let (next_shard, next_ord) = {
            let mut rows = conn
                .query("SELECT next_shard, next_ord FROM meta WHERE id = 0", ())
                .await
                .map_err(map_err)?;
            match rows.next().await.map_err(map_err)? {
                Some(row) => {
                    let shard: i64 = row.get(0).map_err(map_err)?;
                    let ord: i64 = row.get(1).map_err(map_err)?;
                    (shard.cast_unsigned(), ord.cast_unsigned())
                }
                None => (0, 0),
            }
        };
        let mut shards = HashMap::new();
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
                let (Some(asset), Some(kind)) =
                    (asset_class_from_tag(&asset_class), kind_from_tag(&kind_tag_s))
                else {
                    continue;
                };
                let instrument = Instrument::new(ProviderId::new(provider), asset, &symbol);
                shards.insert((instrument, kind), shard_table);
            }
        }

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
        // Unbounded, non-blocking; a send error means the writer is gone
        // (log being dropped) — not live-session-fatal.
        let _ = self.tx.send(WriteCmd::Event(ev.clone()));
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        let (ack_tx, ack_rx) = oneshot::channel();
        if self.tx.send(WriteCmd::Flush(ack_tx)).is_err() {
            return Ok(());
        }
        match ack_rx.await {
            Ok(res) => res,
            Err(_) => Ok(()),
        }
    }

    fn as_replay_source(&self) -> Box<dyn ReplaySource> {
        Box::new(TursoTapReplaySource { db: self.db.clone() })
    }
}
```

Writer (the durability core — commit at every queue-drain boundary and before every flush ack):

```rust
struct Writer {
    conn: ::turso::Connection,
    next_shard: u64,
    next_ord: u64,
    shards: HashMap<(Instrument, EventKind), String>,
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
                if let Err(e) = self.write_event(ev).await {
                    tracing::warn!(error = %e, "tap log write failed");
                    self.last_error = Some(e);
                }
            }
            WriteCmd::Flush(ack) => {
                let commit_res = self.commit_if_open().await;
                // Report the most recent error (write or commit) and clear it.
                let res = match self.last_error.take() {
                    Some(e) => Err(e),
                    None => commit_res,
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

    async fn write_event(&mut self, ev: MarketEvent) -> Result<()> {
        let (instrument, kind) = match &ev {
            MarketEvent::Trade(t) => (t.instrument.clone(), EventKind::Trade),
            MarketEvent::Quote(q) => (q.instrument.clone(), EventKind::Quote),
            MarketEvent::Bar(b) => (b.instrument.clone(), EventKind::Bar(b.interval)),
            // Non-data events are not tapped (defensive; the session gate
            // also filters these).
            _ => return Ok(()),
        };
        let shard = self.resolve_shard(&instrument, kind).await?;
        let seq = event_seq(&ev).cast_signed();
        let ord = self.next_ord.cast_signed();
        self.next_ord = self.next_ord.saturating_add(1);
        self.begin_if_needed().await?;
        match ev {
            MarketEvent::Trade(t) => {
                execute_retry(
                    &self.conn,
                    &format!(
                        "INSERT INTO {shard} (ord, seq, source_ts, rx_ts, price_raw, size_raw) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6)"
                    ),
                    (ord, seq, t.source_ts.0, t.rx_ts.0, t.price.raw(),
                     t.size.raw().cast_signed()),
                )
                .await?;
            }
            MarketEvent::Quote(q) => {
                execute_retry(
                    &self.conn,
                    &format!(
                        "INSERT INTO {shard} (ord, seq, source_ts, rx_ts, bid_raw, \
                         bid_size_raw, ask_raw, ask_size_raw) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)"
                    ),
                    (ord, seq, q.source_ts.0, q.rx_ts.0, q.bid.raw(),
                     q.bid_size.raw().cast_signed(), q.ask.raw(),
                     q.ask_size.raw().cast_signed()),
                )
                .await?;
            }
            MarketEvent::Bar(b) => {
                execute_retry(
                    &self.conn,
                    &format!(
                        "INSERT INTO {shard} (ord, seq, source_ts, rx_ts, open_raw, high_raw, \
                         low_raw, close_raw, volume_raw) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)"
                    ),
                    (ord, seq, b.source_ts.0, b.rx_ts.0, b.open.raw(), b.high.raw(),
                     b.low.raw(), b.close.raw(), b.volume.raw().cast_signed()),
                )
                .await?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Resolve (allocating on first sight) the shard table. DDL cannot ride
    /// the open batch transaction safely, so a new shard commits the open
    /// batch first, then runs CREATE TABLE + registry upsert autocommit.
    async fn resolve_shard(&mut self, instrument: &Instrument, kind: EventKind) -> Result<String> {
        if let Some(name) = self.shards.get(&(instrument.clone(), kind)) {
            return Ok(name.clone());
        }
        // Refuse an asset class with no stable on-disk encoding — a row that
        // cannot round-trip would orphan the shard on reopen. (Port of the
        // surreal backend's guard; see asset_class_tag.)
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
        let cols = match kind {
            EventKind::Trade => "price_raw INTEGER NOT NULL, size_raw INTEGER NOT NULL",
            EventKind::Quote => {
                "bid_raw INTEGER NOT NULL, bid_size_raw INTEGER NOT NULL, \
                 ask_raw INTEGER NOT NULL, ask_size_raw INTEGER NOT NULL"
            }
            EventKind::Bar(_) => {
                "open_raw INTEGER NOT NULL, high_raw INTEGER NOT NULL, \
                 low_raw INTEGER NOT NULL, close_raw INTEGER NOT NULL, \
                 volume_raw INTEGER NOT NULL"
            }
        };
        execute_retry(
            &self.conn,
            &format!(
                "CREATE TABLE IF NOT EXISTS {name} (ord INTEGER PRIMARY KEY, \
                 seq INTEGER NOT NULL, source_ts INTEGER NOT NULL, \
                 rx_ts INTEGER NOT NULL, {cols})"
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
                instrument.provider().as_str().to_string(),
                asset_class_tag(instrument.asset_class()).to_string(),
                instrument.symbol().to_string(),
                kind_tag(kind).to_string(),
                name.clone(),
            ),
        )
        .await?;
        self.shards.insert((instrument.clone(), kind), name.clone());
        Ok(name)
    }
}
```

Port **verbatim** from `surreal_tap_log.rs` (they are deleted with it in Task 9): `kind_tag` / `kind_from_tag` (lines ~159–184), `asset_class_tag` / `asset_class_from_tag` (lines ~186–210, including the lockstep doc comments), `registry_id` (lines ~212–238, length-prefixed injective id), `event_seq` (lines ~247–257).

Add a placeholder replay source (Task 7 fleshes it out):

```rust
struct TursoTapReplaySource {
    db: ::turso::Database,
}

#[async_trait]
impl ReplaySource for TursoTapReplaySource {
    async fn open(&self, _request: ReplayRequest) -> Result<BoxStream<'static, MarketEvent>> {
        Ok(stream::empty().boxed())
    }
}
```

Register in `storage/mod.rs`:

```rust
#[cfg(feature = "storage-turso")]
pub mod turso_tap_log;

#[cfg(feature = "storage-turso")]
pub use turso_tap_log::{TursoTapLog, TursoTapLogConfig};
```

- [ ] **Step 3: Run the enabled tests**

Run: `cargo test -p datamancer --no-default-features --features storage-turso --test turso_tap_log`
Expected: PASS for the enabled subset.

- [ ] **Step 4: Clippy + commit**

Run: `cargo clippy -p datamancer --all-targets --features storage-turso -- -D warnings`

```bash
git add crates/datamancer/src/storage/turso_tap_log.rs crates/datamancer/src/storage/mod.rs crates/datamancer/tests/turso_tap_log.rs crates/datamancer/Cargo.toml
git commit -m "feat(storage): TursoTapLog writer with commit-at-drain durability

Shard-per-(instrument,kind) with ord INTEGER PRIMARY KEY as the replay key;
meta counters persisted inside every commit; flush acks only after COMMIT
(synchronous=FULL), which is the kill-durability contract."
```

---

### Task 7: TursoTapLog — ReplaySource + full tap parity suite

**Files:**
- Modify: `crates/datamancer/src/storage/turso_tap_log.rs`
- Modify: `crates/datamancer/tests/turso_tap_log.rs` (re-enable all tests)

**Interfaces:**
- Consumes: Task 6's schema and helpers.
- Produces: working `TapLog::as_replay_source` — enumerate `streams`, filter by request instruments/kinds, per-shard `source_ts`-windowed SELECT ordered by `ord`, k-way merge by global `ord` sort, `seq` verbatim.

- [ ] **Step 1: Re-enable all tap tests, watch replay fail**

Un-comment every `TODO(task-7)` test in `tests/turso_tap_log.rs`.

Run: `cargo test -p datamancer --no-default-features --features storage-turso --test turso_tap_log`
Expected: FAIL — replay returns empty streams.

- [ ] **Step 2: Implement the replay source**

Replace the placeholder body (port of `surreal_tap_log.rs` lines ~633–763):

```rust
#[async_trait]
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
                let (Some(asset), Some(kind)) =
                    (asset_class_from_tag(&asset_class), kind_from_tag(&kind_tag_s))
                else {
                    continue;
                };
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
```

- [ ] **Step 3: Run the full tap parity suite**

Run: `cargo test -p datamancer --no-default-features --features storage-turso --test turso_tap_log`
Expected: PASS — including `embedded_round_trip_persists_and_continues_seq` (reopen + counters), `replay_preserves_arrival_order_not_source_ts_order`, `replay_merges_shards_by_seq_across_instruments`, `awkward_symbol_round_trips`.

- [ ] **Step 4: Clippy + commit**

Run: `cargo clippy -p datamancer --all-targets --features storage-turso -- -D warnings`

```bash
git add crates/datamancer/src/storage/turso_tap_log.rs crates/datamancer/tests/turso_tap_log.rs
git commit -m "feat(storage): TursoTapLog replay source; full tap parity suite green"
```

---

### Task 8: Crash-durability tests (spec constraint 2 — gate for the default flip)

**Files:**
- Test: `crates/datamancer/tests/turso_crash.rs`
- Modify: `crates/datamancer/Cargo.toml` (test target)

**Interfaces:**
- Consumes: `TursoTapLog`, `TursoTapLogConfig` (Task 6/7).
- Produces: the CI-runnable evidence that a completed `flush` survives SIGKILL. Runs in normal CI (no live services, no `#[ignore]`).

- [ ] **Step 1: Write the kill test**

Create `crates/datamancer/tests/turso_crash.rs`:

```rust
//! Kill-durability for the tap log (spec constraint 2): every event whose
//! `flush` completed before SIGKILL must replay after reopen.
//!
//! Re-exec harness: the parent test spawns THIS test binary filtered to
//! `crash_child_writer` with `TURSO_CRASH_DB` set; the child appends
//! batches, flushes, and prints `flushed {batch}` (explicitly flushing
//! stdout — it is block-buffered when piped); the parent SIGKILLs it after
//! a few confirmed flushes and asserts every confirmed batch replays.

#![cfg(feature = "storage-turso")]

use std::io::{BufRead as _, Write as _};

use datamancer::storage::{TursoTapLog, TursoTapLogConfig};
use datamancer::{
    AssetClass, EventKind, Instrument, MarketEvent, Price, ProviderId, Quantity, Seq, TapLog,
    Timestamp, Trade,
};
use datamancer_core::ReplayRequest;
use futures::StreamExt as _;

const EVENTS_PER_BATCH: u64 = 10;

fn inst() -> Instrument {
    Instrument::new(ProviderId::from_static("alpaca"), AssetClass::Equity, "AAPL")
}

fn trade(n: u64) -> MarketEvent {
    MarketEvent::Trade(Trade {
        instrument: inst(),
        source_ts: Timestamp(n.cast_signed()),
        rx_ts: Timestamp(n.cast_signed()),
        seq: Seq(n),
        price: Price::from_units(100),
        size: Quantity::from_units(1),
    })
}

/// Child mode: no-op unless `TURSO_CRASH_DB` is set (so the normal test run
/// skips it). Appends and flushes batches forever; the parent kills it.
#[test]
fn crash_child_writer() {
    let Ok(db_path) = std::env::var("TURSO_CRASH_DB") else {
        return;
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        let log = TursoTapLog::open(TursoTapLogConfig::embedded(&db_path))
            .await
            .unwrap();
        let mut stdout = std::io::stdout();
        for batch in 0..u64::MAX {
            for i in 0..EVENTS_PER_BATCH {
                log.append(&trade(batch * EVENTS_PER_BATCH + i)).await.unwrap();
            }
            log.flush().await.unwrap();
            // Claim durability ONLY after flush returned Ok.
            writeln!(stdout, "flushed {batch}").unwrap();
            stdout.flush().unwrap();
        }
    });
}

#[tokio::test]
async fn completed_flush_survives_sigkill() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("tap.db");

    let exe = std::env::current_exe().unwrap();
    let mut child = std::process::Command::new(exe)
        .args(["crash_child_writer", "--exact", "--nocapture"])
        .env("TURSO_CRASH_DB", db_path.to_str().unwrap())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();

    let stdout = child.stdout.take().unwrap();
    let reader = std::io::BufReader::new(stdout);
    let mut confirmed: Vec<u64> = Vec::new();
    for line in reader.lines() {
        let line = line.unwrap();
        if let Some(id) = line.strip_prefix("flushed ") {
            confirmed.push(id.trim().parse().unwrap());
            if confirmed.len() >= 3 {
                break;
            }
        }
    }
    child.kill().unwrap(); // SIGKILL: no drop-glue, no final commit
    let _ = child.wait();

    let last = *confirmed.last().expect("child confirmed at least one flush");
    let expected = (last + 1) * EVENTS_PER_BATCH;

    let log = TursoTapLog::open(TursoTapLogConfig::embedded(&db_path))
        .await
        .expect("reopen after SIGKILL");
    let source = log.as_replay_source();
    let mut stream = source
        .open(ReplayRequest {
            instruments: vec![inst()],
            kinds: vec![EventKind::Trade],
            from: Timestamp(i64::MIN),
            to: Timestamp(i64::MAX),
        })
        .await
        .expect("replay after SIGKILL");
    let mut seqs = Vec::new();
    while let Some(ev) = stream.next().await {
        if let MarketEvent::Trade(t) = ev {
            seqs.push(t.seq.0);
        }
    }
    for n in 0..expected {
        assert!(
            seqs.contains(&n),
            "event {n} was covered by a completed flush (batches 0..={last}) \
             but did not survive SIGKILL; {} events survived",
            seqs.len()
        );
    }
}
```

Add the test target to `crates/datamancer/Cargo.toml`:

```toml
[[test]]
name = "turso_crash"
required-features = ["storage-turso"]
```

- [ ] **Step 2: Run it (several times — it guards a race)**

Run: `for i in 1 2 3 4 5; do cargo test -p datamancer --no-default-features --features storage-turso --test turso_crash -- completed_flush_survives_sigkill || break; done`
Expected: PASS ×5. If it ever fails, that is a real durability hole — stop and investigate before Task 9 (the spec forbids flipping the default without this passing).

- [ ] **Step 3: Clippy + commit**

Run: `cargo clippy -p datamancer --all-targets --features storage-turso -- -D warnings`

```bash
git add crates/datamancer/tests/turso_crash.rs crates/datamancer/Cargo.toml
git commit -m "test(storage): tap-log kill-durability — completed flush survives SIGKILL

Spec constraint 2 satisfied; gates the storage-turso default flip."
```

---

### Task 9: Cutover — flip default, wire the daemon, delete surreal, clean deny.toml + docs

**Files:**
- Modify: `crates/datamancer/Cargo.toml` (default features, delete surreal feature/dep/test-target/example gates)
- Modify: `crates/datamancer/src/storage/mod.rs`
- Delete: `crates/datamancer/src/storage/surreal.rs`, `crates/datamancer/src/storage/surreal_tap_log.rs`, `crates/datamancer/tests/surreal_cache.rs`, `crates/datamancer/tests/surreal_tap_log.rs`
- Modify: `crates/datamancer/examples/cached_history.rs`, `crates/datamancer/examples/tap_replay.rs`, `crates/datamancer/examples/resume.rs`
- Modify: `crates/datamancerd/src/config.rs`, `crates/datamancerd/src/web/refresh.rs`
- Modify: `deny.toml`
- Modify: `CLAUDE.md`, `crates/datamancer/CLAUDE.md` (if it mentions surreal), `crates/datamancer/README.md`, `crates/datamancerd/README.md`

- [ ] **Step 1: Flip features and delete the surreal backend**

In `crates/datamancer/Cargo.toml`:
- `default = ["provider-alpaca", "storage-turso"]`
- Delete the `storage-surreal` feature line, the `# storage-surreal` dep section (`surrealdb`), and the surreal test/example `required-features` references (`[[test]]` stanzas for surreal tests if present; change the three examples' `required-features` to `["storage-turso"]`).
- Check whether `chrono` is still needed by `provider-alpaca` (it is — keep it there; remove it from any deleted storage-surreal feature list only).

Delete the four surreal files. In `storage/mod.rs`, remove the surreal blocks so only the turso + coverage + common modules remain.

- [ ] **Step 2: Update the examples**

In each of `cached_history.rs`, `tap_replay.rs`, `resume.rs`: replace `SurrealCache`/`SurrealCacheConfig`/`SurrealTapLog`/`SurrealTapLogConfig` with the `Turso*` equivalents (config variants have identical names: `Memory`, `embedded(path)`). Read each example first — if one passes a *directory* to `embedded(...)`, append a file name (`dir.join("cache.db")`), since turso paths are files.

- [ ] **Step 3: Wire the daemon**

In `crates/datamancerd/src/config.rs`:
- Import `TursoCache, TursoCacheConfig, TursoTapLog, TursoTapLogConfig` instead of the surreal names.
- Rename the backend tokens (operator contract — pre-deployment, no compatibility alias):

```rust
/// Supported storage backends.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StorageBackend {
    /// A database file on disk (path from `path`, or the platform data dir).
    Embedded,
    /// In-process, ephemeral. Good for tests.
    Memory,
}
```

- `storage_to_cache_config` / `storage_to_tap_config` map `Memory → Turso*Config::Memory`, `Embedded → Turso*Config::embedded(embedded_path(...))`.
- `embedded_path`: now names a **file**: default `default_data_dir().join("cache.db")` / `join("taplog.db")` (keep the explicit-`path` override behavior; update the doc comment and the error message to say `embedded` instead of `surreal-embedded`).
- `build_runtime`: `TursoCache::open(...)` / `TursoTapLog::open(...)`.
- Update the config tests in the same file (they construct `StorageBackend::SurrealEmbedded` — rename; any TOML fixtures using `backend = "surreal-embedded"` become `backend = "embedded"`).

In `crates/datamancerd/src/web/refresh.rs` tests: swap `SurrealCache::open(SurrealCacheConfig::Memory)` for `TursoCache::open(TursoCacheConfig::Memory)`.

Run: `grep -rn "urreal" crates/ --include="*.rs" --include="*.toml"` — expect zero hits when done (except none).

- [ ] **Step 4: Clean `deny.toml`**

Remove the transitional block: the `[[licenses.exceptions]]`-style entries naming `surrealdb`/`surrealdb-core`/`surrealdb-types`/`surrealdb-protocol`/`surrealdb-types-derive` (lines ~33–52), both advisory ignores (`RUSTSEC-2025-0141`, `RUSTSEC-2023-0071`, lines ~63–67), and the `Unlicense` / `BSL-1.0` allow-list entries **only if** `cargo deny check` passes without them (they were justified by surreal-tree crates; another dep may still need them — let the tool decide).

Run: `cargo deny check` — expected: clean.

- [ ] **Step 5: Update docs**

- Root `CLAUDE.md`: `storage/surreal` → `storage/turso` in the workspace description; `Default features: provider-alpaca, storage-surreal` → `storage-turso`; the Scope reminders paragraph's "surreal tap log" wording → "tap log".
- `crates/datamancer/README.md` + `crates/datamancerd/README.md`: `grep -n "urreal"` and rewrite each hit (backend name, config schema `backend = "embedded" | "memory"`, `path` now names a database file).
- `docs/superpowers/specs/2026-07-03-turso-storage-design.md`: append a one-line `**Status:** implemented` note referencing this plan.

- [ ] **Step 6: Full verification (mirrors CI)**

Run, expecting all green:

```bash
cargo fmt
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo deny check
cargo build   # default features now pull turso, not surrealdb
grep -rn "urreal" crates/ deny.toml CLAUDE.md   # expect no output
```

Also confirm the dependency win: `cargo tree -e normal | grep -c surrealdb` → 0.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat(storage)!: cut over to Turso; delete the SurrealDB backend

storage-turso becomes the default; storage-surreal, the surrealdb dependency
tree (BUSL-1.1 + two no-fix advisories), the deny.toml transitional block,
and the surreal config tokens are gone. Daemon backend tokens are now
'embedded' / 'memory'; embedded paths name database files."
```

---

## Self-Review Notes

- **Spec coverage:** engine choice + no-default-features (Task 1); SQLite-subset SQL only (all SQL in Tasks 3–7 is plain CREATE/INSERT/SELECT/DELETE/BEGIN/COMMIT — constraint 1); crash-durability before default (Task 8 gates Task 9 — constraint 2); 1:1 semantics via the ported parity suites (Tasks 3–7); composite index (Task 3's PRIMARY KEY); async-native, no `spawn_blocking` (throughout); cutover deletes surreal + deny.toml block + daemon keys (Task 9); out-of-scope items (eviction, MVCC/`BEGIN CONCURRENT`, data migration) appear in no task.
- **Spike mitigations encoded:** single-writer discipline (cache mutex, Task 3; writer-task-owned connection, Task 6), bounded busy-retry (Task 1), `PRAGMA user_version` (Task 1), `default-features = false` (Task 1).
- **Type consistency:** `TursoCacheConfig`/`TursoTapLogConfig` variants `Memory` / `Embedded { path }` + `embedded(path)` helper used identically in Tasks 3, 6, 8, 9; `execute_retry(conn, sql, params) -> Result<u64>` used with `.await?` everywhere; `cast_signed()`/`cast_unsigned()` applied at every u64 column boundary (`size_raw`, `bid_size_raw`, `ask_size_raw`, `volume_raw`, `seq`, `ord`, `event_count`, `next_shard`, `next_ord`).
- **Known API risk (accepted):** exact turso 0.6 trait paths (`IntoParams`) and `Database: Clone` are verify-at-Task-1/3 items; the spike validated the runtime behavior, and the fallback (`Arc<Database>`) changes no interfaces between tasks other than the two struct fields.
