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
//!
//! Consumed by both [`super::turso::TursoCache`] and
//! [`super::turso_tap_log::TursoTapLog`], the two Turso-backed stores.

use std::path::PathBuf;
use std::time::Duration;

use datamancer_core::{AssetClass, BarInterval, Error, EventKind, Result};

/// Bounded busy-retry budget: writes are serialized by design, so `Busy`
/// here is unexpected; retry briefly, then fail loudly.
const BUSY_RETRIES: u32 = 200;
const BUSY_BACKOFF: Duration = Duration::from_millis(5);

/// Where a turso database lives.
pub(crate) enum DbLocation {
    Memory,
    File(PathBuf),
}

// Takes `err` by value (rather than `&::turso::Error`) so it can be passed
// directly as `.map_err(map_err)`, which requires an `FnOnce(E) -> F`.
#[allow(clippy::needless_pass_by_value)]
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

/// Schema-version preflight via `PRAGMA user_version` (the idiomatic `SQLite`
/// mechanism; supersedes the prior backends' meta-table markers). Runs
/// **before any DDL** so a file belonging to another store (or another schema
/// generation) is refused without polluting it with this store's tables.
/// Returns `true` when the file is fresh (version 0): the caller must create
/// its schema and then [`stamp_user_version`]. There is no pre-versioning
/// turso lineage — version numbering starts fresh per store.
pub(crate) async fn preflight_user_version(
    conn: &::turso::Connection,
    expected: i64,
    store: &str,
) -> Result<bool> {
    let version: i64 = {
        let mut rows = conn
            .query("PRAGMA user_version", ())
            .await
            .map_err(map_err)?;
        let first = rows.next().await.map_err(map_err)?;
        // Defer the decode until the cursor is fully drained: an un-stepped
        // `Rows` leaves its statement unfinalized, and turso 0.6.1 silently
        // drops a same-connection write issued while an earlier read
        // statement is still open (observed via a spike where
        // `PRAGMA user_version = N` read back correctly in-connection but
        // never reached disk). Erroring out of this scope before the drain
        // would leave the connection in exactly that state.
        let raw = first.map(|row| row.get::<i64>(0));
        while rows.next().await.map_err(map_err)?.is_some() {}
        raw.ok_or_else(|| Error::Storage("PRAGMA user_version returned no row".to_string()))?
            .map_err(map_err)?
    };
    if version == expected {
        return Ok(false);
    }
    if version == 0 {
        return Ok(true);
    }
    Err(Error::Storage(format!(
        "{store} schema version {version} does not match this build's {expected}; \
         read it with a matching build or delete the file (data is recoverable \
         from providers)"
    )))
}

/// Stamp a fresh file's `PRAGMA user_version` — call only after schema
/// creation succeeded, so a half-initialized file is re-checked (and its
/// `CREATE TABLE IF NOT EXISTS` re-run) on the next open rather than passing
/// the version gate with missing tables.
pub(crate) async fn stamp_user_version(conn: &::turso::Connection, expected: i64) -> Result<()> {
    // PRAGMA assignment cannot be parameterized; `expected` is a trusted
    // compile-time constant.
    conn.execute(&format!("PRAGMA user_version = {expected}"), ())
        .await
        .map_err(map_err)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Shared on-disk vocabulary — one definition for both stores.
//
// The cache and the tap log are independent files with independent schema
// versions, but their per-kind row shapes and asset-class tags are the same
// vocabulary on purpose; defining it once keeps the two from drifting when a
// kind or class is added.
// ---------------------------------------------------------------------------

/// Column DDL for a `kind`'s fixed numeric payload (shared by the cache's
/// per-kind tables and the tap log's shard tables).
pub(crate) const fn event_columns(kind: EventKind) -> &'static str {
    match kind {
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
    }
}

/// Every persistable event kind, for schema-creation loops.
pub(crate) const ALL_KINDS: [EventKind; 8] = [
    EventKind::Trade,
    EventKind::Quote,
    EventKind::Bar(BarInterval::OneSecond),
    EventKind::Bar(BarInterval::OneMinute),
    EventKind::Bar(BarInterval::FiveMinute),
    EventKind::Bar(BarInterval::FifteenMinute),
    EventKind::Bar(BarInterval::OneHour),
    EventKind::Bar(BarInterval::OneDay),
];

/// Stable on-disk tag for an asset class. `AssetClass` is `#[non_exhaustive]`,
/// so the wildcard is mandatory — but a new variant tagged `"unknown"` would
/// not round-trip through [`asset_class_from_tag`]. Writers refuse to persist
/// an `"unknown"` class rather than store an unreadable row. **Adding an
/// `AssetClass` variant requires updating this function and
/// [`asset_class_from_tag`] in lockstep.**
pub(crate) fn asset_class_tag(asset: AssetClass) -> &'static str {
    match asset {
        AssetClass::Equity => "equity",
        AssetClass::Etf => "etf",
        AssetClass::Crypto => "crypto",
        _ => "unknown",
    }
}

/// Inverse of [`asset_class_tag`]. Returns `None` for an unrecognized tag;
/// keep the two in lockstep when adding an `AssetClass` variant.
pub(crate) fn asset_class_from_tag(tag: &str) -> Option<AssetClass> {
    Some(match tag {
        "equity" => AssetClass::Equity,
        "etf" => AssetClass::Etf,
        "crypto" => AssetClass::Crypto,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        DbLocation, connect, execute_retry, open_database, preflight_user_version,
        stamp_user_version,
    };

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
            let fresh = preflight_user_version(&conn, 1, "test store")
                .await
                .unwrap();
            assert!(fresh, "a new file must read as fresh");
            stamp_user_version(&conn, 1).await.unwrap();
        }
        let db = open_database(&loc).await.unwrap();
        let conn = connect(&db).await.unwrap();
        let fresh = preflight_user_version(&conn, 1, "test store")
            .await
            .unwrap();
        assert!(!fresh, "a stamped file must not read as fresh");
    }

    #[tokio::test]
    async fn mismatched_user_version_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let loc = DbLocation::File(dir.path().join("v.db"));
        {
            let db = open_database(&loc).await.unwrap();
            let conn = connect(&db).await.unwrap();
            stamp_user_version(&conn, 999).await.unwrap();
        }
        let db = open_database(&loc).await.unwrap();
        let conn = connect(&db).await.unwrap();
        let err = preflight_user_version(&conn, 1, "test store")
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
