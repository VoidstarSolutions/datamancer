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
