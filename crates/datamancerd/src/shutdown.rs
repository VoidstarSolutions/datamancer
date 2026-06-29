//! Graceful-shutdown drain ordering.
//!
//! On SIGTERM/SIGINT the daemon runs a **bounded, serialized** drain in a fixed
//! order:
//!
//! 1. stop accepting control requests,
//! 2. stop the diagnostics ticker,
//! 3. per client: **close** the client session and drain its pump, so terminal
//!    events the close emits (`SessionClosing`) actually reach the sink instead
//!    of being severed by an immediate pump abort,
//! 4. drop the startup anchors (releases the last authoritative refcounts, so
//!    each authoritative session's final teed events land in the tap log),
//! 5. **flush the shared tap log** (the durable record), *before* the
//!    best-effort per-client sink flushes — the load-bearing
//!    tap-log-before-sink-flush contract,
//! 6. per client: flush the sink (deliver buffered events, incl. `SessionClosing`),
//! 7. drop the clients/sinks (service drop, last).
//!
//! The whole drain is wrapped in a timeout by the caller so a disk-stalled
//! tap-log flush cannot hang shutdown forever. The order is the load-bearing
//! contract, exercised by [`tests::shutdown_drain_order`] with recording fakes.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datamancer::TapLog;

/// An ordered record of drain phases, for observability and the order test. In
/// production this is a no-op sink; in tests the fakes push their own labels so
/// the full interleaving can be asserted.
#[derive(Clone, Default)]
pub struct DrainRecorder {
    log: Arc<Mutex<Vec<String>>>,
}

impl DrainRecorder {
    /// Append one phase label.
    pub fn record(&self, label: impl Into<String>) {
        if let Ok(mut log) = self.log.lock() {
            log.push(label.into());
        }
    }

    /// Snapshot the recorded labels.
    #[must_use]
    pub fn entries(&self) -> Vec<String> {
        self.log.lock().map(|l| l.clone()).unwrap_or_default()
    }
}

/// A per-client handle the drain can close then flush. Implemented by the real
/// `ClientEntry` (closes its client session + drains its pump, flushes its
/// iceoryx2 sink) and by the test fake.
#[async_trait]
pub trait DrainClient: Send {
    /// The client's control-protocol name (for the recorder).
    fn name(&self) -> &str;
    /// Close the client session and drain its pump, so terminal events the close
    /// emits (`SessionClosing`) reach the sink. Releases authoritative refcounts.
    async fn close(&mut self);
    /// Flush the per-client sink so buffered events (incl. the terminal ones
    /// drained above) reach subscribers. Called after the tap-log flush.
    async fn flush_sink(&self);
}

/// Run the ordered drain. `stop_accept` and `stop_diagnostics` are run first
/// (closing the listener and halting the ticker); then each client is flushed
/// and closed; then the anchors are dropped; then the tap log is flushed.
///
/// `anchors` is an opaque list whose `Drop` releases the last authoritative
/// refcounts — the daemon passes its held `Session` anchors here.
pub async fn drain<A: Send>(
    recorder: &DrainRecorder,
    stop_accept: impl FnOnce() + Send,
    stop_diagnostics: impl FnOnce() + Send,
    mut clients: Vec<Box<dyn DrainClient>>,
    anchors: Vec<A>,
    tap_log: Option<Arc<dyn TapLog>>,
) {
    recorder.record("stop-accept");
    stop_accept();

    recorder.record("diagnostics-stop");
    stop_diagnostics();

    // Close each session and drain its pump first, so a `SessionClosing` (or any
    // event the close emits) is delivered into the sink rather than severed by an
    // immediate pump abort.
    for client in &mut clients {
        recorder.record(format!("client-close:{}", client.name()));
        client.close().await;
    }

    // Drop the anchors so each authoritative session's final teed events land in
    // the tap log before it is flushed.
    recorder.record("anchor-drop");
    drop(anchors);

    // Flush the durable tap log BEFORE the best-effort per-client sink flushes
    // (the load-bearing tap-log-before-sink-flush contract): if the remaining
    // best-effort steps stall, the durable record is already safe.
    recorder.record("tap-log-flush");
    if let Some(tap_log) = &tap_log
        && let Err(e) = tap_log.flush().await
    {
        tracing::warn!(error = %e, "tap-log flush failed during shutdown");
    }

    // Now flush the per-client sinks (delivering buffered events, incl. the
    // terminal ones drained above), then drop them (service drop, last).
    for client in &mut clients {
        recorder.record(format!("client-flush:{}", client.name()));
        client.flush_sink().await;
    }
    drop(clients);
}

#[cfg(test)]
mod tests {
    use super::*;
    use datamancer::{MarketEvent, Result as LibResult};

    struct FakeClient {
        name: String,
        recorder: DrainRecorder,
    }

    #[async_trait]
    impl DrainClient for FakeClient {
        fn name(&self) -> &str {
            &self.name
        }
        async fn close(&mut self) {
            self.recorder.record(format!("session-closed:{}", self.name));
        }
        async fn flush_sink(&self) {
            self.recorder.record(format!("sink-flushed:{}", self.name));
        }
    }

    struct FakeTapLog {
        recorder: DrainRecorder,
    }

    #[async_trait]
    impl TapLog for FakeTapLog {
        async fn append(&self, _ev: &MarketEvent) -> LibResult<()> {
            Ok(())
        }
        async fn flush(&self) -> LibResult<()> {
            self.recorder.record("taplog-flushed");
            Ok(())
        }
        fn as_replay_source(&self) -> Box<dyn datamancer::ReplaySource> {
            unreachable!("not used in this test")
        }
    }

    struct AnchorGuard {
        recorder: DrainRecorder,
    }
    impl Drop for AnchorGuard {
        fn drop(&mut self) {
            self.recorder.record("anchor-dropped");
        }
    }

    #[tokio::test]
    async fn shutdown_drain_order() {
        let recorder = DrainRecorder::default();
        let clients: Vec<Box<dyn DrainClient>> = vec![
            Box::new(FakeClient {
                name: "a".to_string(),
                recorder: recorder.clone(),
            }),
            Box::new(FakeClient {
                name: "b".to_string(),
                recorder: recorder.clone(),
            }),
        ];
        let anchors = vec![AnchorGuard {
            recorder: recorder.clone(),
        }];
        let tap_log: Arc<dyn TapLog> = Arc::new(FakeTapLog {
            recorder: recorder.clone(),
        });

        let r = recorder.clone();
        drain(
            &recorder,
            move || r.record("accept-stopped"),
            {
                let r = recorder.clone();
                move || r.record("diagnostics-stopped")
            },
            clients,
            anchors,
            Some(tap_log),
        )
        .await;

        assert_eq!(
            recorder.entries(),
            vec![
                "stop-accept",
                "accept-stopped",
                "diagnostics-stop",
                "diagnostics-stopped",
                // Close + pump-drain every client first (terminal events reach sinks).
                "client-close:a",
                "session-closed:a",
                "client-close:b",
                "session-closed:b",
                // Anchors drop, then the durable tap log flushes BEFORE sink flushes.
                "anchor-drop",
                "anchor-dropped",
                "tap-log-flush",
                "taplog-flushed",
                // Best-effort per-client sink flushes last, then service drop.
                "client-flush:a",
                "sink-flushed:a",
                "client-flush:b",
                "sink-flushed:b",
            ]
        );
    }
}
