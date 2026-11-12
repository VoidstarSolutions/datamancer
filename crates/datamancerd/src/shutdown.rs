//! Graceful-shutdown drain ordering.
//!
//! On SIGTERM/SIGINT the daemon runs a **bounded** drain in a fixed order:
//!
//! 1. stop accepting control requests,
//! 2. stop the diagnostics ticker,
//! 3. **close** every client concurrently, each draining its pump, so terminal
//!    events the close emits (`SessionClosing`) actually reach the sink instead
//!    of being severed by an immediate pump abort; the closes share one time
//!    limit,
//! 4. drop the startup anchors, releasing the last authoritative refcounts; an
//!    authoritative session still taking provider events may tee more after
//!    step 5, which only step 6's second flush covers,
//! 5. **flush the shared tap log** (the durable record), *before* the
//!    best-effort per-client sink flushes — the load-bearing
//!    tap-log-before-sink-flush contract,
//! 6. stop what the time limit cut off, and flush the tap log again if any
//!    client waited on a provider, the limit cut a client off, or a WS
//!    connection was aborted, since each may release its session after step 5,
//! 7. per client: flush the sink (deliver buffered events, incl. `SessionClosing`),
//! 8. drop the clients/sinks (service drop, last).
//!
//! Steps 4 and 5 wait only for the clients that cannot wait on a provider: a
//! client with a subscribe in flight, or one already closing, closes alongside
//! them, under the same time limit. The whole drain is wrapped in a timeout by
//! the caller so a disk-stalled tap-log flush cannot hang shutdown forever.
//! The order is the load-bearing contract, exercised by
//! [`tests::shutdown_drain_order`] with recording fakes.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use datamancer::TapLog;
use futures::future::join_all;

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
    /// Whether [`close`](Self::close) may wait on a provider: the drain then
    /// flushes the tap log without waiting for it.
    fn waits_on_provider(&self) -> bool {
        false
    }
    /// Stop whatever a close cut off by the time limit left running.
    fn abort(&mut self) {}
}

/// Run the ordered drain. `stop_accept` and `stop_diagnostics` are run first
/// (closing the listener and halting the ticker); then every client is
/// closed, all under `close_limit`; the anchors are dropped and the tap log
/// flushed once every client that does not wait on a provider has closed;
/// then each sink is flushed.
///
/// `anchors` is an opaque list whose `Drop` releases the last authoritative
/// refcounts — the daemon passes its held `Session` anchors here.
/// `connections_aborted` says a WS connection was aborted before this drain,
/// so the tap log is flushed a second time.
#[allow(
    clippy::too_many_arguments,
    reason = "each drain input is a distinct phase's; a struct would only rename them"
)]
pub async fn drain<A: Send>(
    recorder: &DrainRecorder,
    stop_accept: impl FnOnce() + Send,
    stop_diagnostics: impl FnOnce() + Send,
    clients: Vec<Box<dyn DrainClient>>,
    close_limit: Duration,
    anchors: Vec<A>,
    tap_log: Option<Arc<dyn TapLog>>,
    connections_aborted: bool,
) {
    recorder.record("stop-accept");
    stop_accept();

    recorder.record("diagnostics-stop");
    stop_diagnostics();

    // Close each session and drain its pump first, so a `SessionClosing` (or any
    // event the close emits) is delivered into the sink rather than severed by an
    // immediate pump abort.
    let close_deadline = tokio::time::Instant::now() + close_limit;
    let (mut waiting, mut ready): (Vec<_>, Vec<_>) = clients
        .into_iter()
        .partition(|client| client.waits_on_provider());
    for client in ready.iter().chain(&waiting) {
        recorder.record(format!("client-close:{}", client.name()));
    }
    let waiting_closes = tokio::time::timeout_at(
        close_deadline,
        join_all(waiting.iter_mut().map(|client| client.close())),
    );
    let durable = async {
        let cut_off = tokio::time::timeout_at(
            close_deadline,
            join_all(ready.iter_mut().map(|client| client.close())),
        )
        .await
        .is_err();
        if cut_off {
            recorder.record("client-close-timeout");
            tracing::warn!("client closes ran past their time limit during shutdown");
        }

        // Drop the anchors so each authoritative session's final teed events
        // land in the tap log before it is flushed.
        recorder.record("anchor-drop");
        drop(anchors);

        // Flush the durable tap log BEFORE the best-effort per-client sink
        // flushes (the load-bearing tap-log-before-sink-flush contract): if the
        // remaining best-effort steps stall, the durable record is already safe.
        flush_tap_log(recorder, tap_log.as_ref()).await;
        cut_off
    };
    let (ready_cut_off, waited) = tokio::join!(durable, waiting_closes);
    if waited.is_err() {
        recorder.record("client-close-timeout");
        tracing::warn!("a client waiting on its provider did not close during shutdown");
    }
    let flush_again = connections_aborted || ready_cut_off || !waiting.is_empty();
    let mut clients = ready;
    clients.append(&mut waiting);
    for client in &mut clients {
        client.abort();
    }
    // Whatever closed behind the flush released its refcounts after it.
    if flush_again {
        flush_tap_log(recorder, tap_log.as_ref()).await;
    }

    // Now flush the per-client sinks (delivering buffered events, incl. the
    // terminal ones drained above), then drop them (service drop, last).
    for client in &mut clients {
        recorder.record(format!("client-flush:{}", client.name()));
        client.flush_sink().await;
    }
    drop(clients);
}

/// Flush `tap_log`, if there is one, logging a failure.
async fn flush_tap_log(recorder: &DrainRecorder, tap_log: Option<&Arc<dyn TapLog>>) {
    recorder.record("tap-log-flush");
    if let Some(tap_log) = tap_log
        && let Err(e) = tap_log.flush().await
    {
        tracing::warn!(error = %e, "tap-log flush failed during shutdown");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datamancer::{MarketEvent, Result as LibResult};

    struct FakeClient {
        name: String,
        recorder: DrainRecorder,
        /// Its close never returns.
        stuck: bool,
        /// It reports waiting on a provider.
        waits: bool,
    }

    impl FakeClient {
        fn boxed(name: &str, recorder: &DrainRecorder, stuck: bool) -> Box<dyn DrainClient> {
            Box::new(Self {
                name: name.to_string(),
                recorder: recorder.clone(),
                stuck,
                waits: stuck,
            })
        }

        /// A client that does not report waiting on a provider, whose close
        /// never returns.
        fn stalled(name: &str, recorder: &DrainRecorder) -> Box<dyn DrainClient> {
            Box::new(Self {
                name: name.to_string(),
                recorder: recorder.clone(),
                stuck: true,
                waits: false,
            })
        }
    }

    #[async_trait]
    impl DrainClient for FakeClient {
        fn name(&self) -> &str {
            &self.name
        }
        async fn close(&mut self) {
            if self.stuck {
                std::future::pending::<()>().await;
            }
            self.recorder
                .record(format!("session-closed:{}", self.name));
        }
        async fn flush_sink(&self) {
            self.recorder.record(format!("sink-flushed:{}", self.name));
        }
        fn waits_on_provider(&self) -> bool {
            self.waits
        }
        fn abort(&mut self) {
            self.recorder.record(format!("aborted:{}", self.name));
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
        let clients = vec![
            FakeClient::boxed("a", &recorder, false),
            FakeClient::boxed("b", &recorder, false),
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
            Duration::from_secs(5),
            anchors,
            Some(tap_log),
            false,
        )
        .await;

        assert_eq!(
            recorder.entries(),
            vec![
                "stop-accept",
                "accept-stopped",
                "diagnostics-stop",
                "diagnostics-stopped",
                // Close + pump-drain every client first, concurrently
                // (terminal events reach sinks).
                "client-close:a",
                "client-close:b",
                "session-closed:a",
                "session-closed:b",
                // Anchors drop, then the durable tap log flushes BEFORE sink flushes.
                "anchor-drop",
                "anchor-dropped",
                "tap-log-flush",
                "taplog-flushed",
                "aborted:a",
                "aborted:b",
                // Best-effort per-client sink flushes last, then service drop.
                "client-flush:a",
                "sink-flushed:a",
                "client-flush:b",
                "sink-flushed:b",
            ]
        );
    }

    /// A client waiting on its provider does not hold up the anchor drop or
    /// the tap-log flush, and the drain stops it at the time limit.
    #[tokio::test]
    async fn a_client_waiting_on_its_provider_does_not_hold_the_tap_log_flush() {
        let recorder = DrainRecorder::default();
        let clients = vec![
            FakeClient::boxed("subscribing", &recorder, true),
            FakeClient::boxed("idle", &recorder, false),
        ];
        let tap_log: Arc<dyn TapLog> = Arc::new(FakeTapLog {
            recorder: recorder.clone(),
        });

        let started = tokio::time::Instant::now();
        drain(
            &recorder,
            || {},
            || {},
            clients,
            Duration::from_millis(200),
            Vec::<AnchorGuard>::new(),
            Some(tap_log),
            false,
        )
        .await;
        assert!(started.elapsed() < Duration::from_secs(1));

        let entries = recorder.entries();
        let at = |label: &str| {
            entries
                .iter()
                .position(|entry| entry == label)
                .unwrap_or_else(|| panic!("no {label}: {entries:?}"))
        };
        assert!(
            at("taplog-flushed") < at("client-close-timeout"),
            "{entries:?}"
        );
        assert!(at("session-closed:idle") < at("anchor-drop"), "{entries:?}");
        assert!(
            at("client-close-timeout") < at("aborted:subscribing"),
            "{entries:?}"
        );
        assert!(
            !entries.contains(&"session-closed:subscribing".to_string()),
            "{entries:?}"
        );
        assert_eq!(
            entries
                .iter()
                .filter(|entry| *entry == "taplog-flushed")
                .count(),
            2,
            "the tap log is flushed again after the cut-off close: {entries:?}"
        );
    }

    /// A ready client the time limit cuts off still releases its refcounts
    /// later, so the tap log is flushed again after it.
    #[tokio::test]
    async fn a_ready_client_cut_off_by_the_limit_flushes_the_tap_log_again() {
        let recorder = DrainRecorder::default();
        let clients = vec![FakeClient::stalled("stalled", &recorder)];
        let tap_log: Arc<dyn TapLog> = Arc::new(FakeTapLog {
            recorder: recorder.clone(),
        });

        drain(
            &recorder,
            || {},
            || {},
            clients,
            Duration::from_millis(100),
            Vec::<AnchorGuard>::new(),
            Some(tap_log),
            false,
        )
        .await;

        let entries = recorder.entries();
        assert!(
            entries.contains(&"client-close-timeout".to_string()),
            "{entries:?}"
        );
        assert_eq!(
            entries
                .iter()
                .filter(|entry| *entry == "taplog-flushed")
                .count(),
            2,
            "the tap log is flushed again after the cut-off close: {entries:?}"
        );
    }

    /// A WS connection aborted before the drain releases its session later,
    /// so the tap log is flushed again with no client involved.
    #[tokio::test]
    async fn an_aborted_connection_flushes_the_tap_log_again() {
        let recorder = DrainRecorder::default();
        let tap_log: Arc<dyn TapLog> = Arc::new(FakeTapLog {
            recorder: recorder.clone(),
        });

        drain(
            &recorder,
            || {},
            || {},
            Vec::new(),
            Duration::from_secs(1),
            Vec::<AnchorGuard>::new(),
            Some(tap_log),
            true,
        )
        .await;

        let entries = recorder.entries();
        assert_eq!(
            entries
                .iter()
                .filter(|entry| *entry == "taplog-flushed")
                .count(),
            2,
            "{entries:?}"
        );
    }
}
