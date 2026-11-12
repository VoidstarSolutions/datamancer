//! The per-client data-plane sink (`impl EventSink`) and its announcement
//! service.
//!
//! One iceoryx2 pub-sub service per client carries the multiplexed data
//! substreams as [`DataPayload`]; a second per-client service carries
//! [`SymbolAnnouncement`]s so subscribers resolve `SymbolId -> Instrument`.
//! Interning + POD conversion live entirely here; core `MarketEvent` is
//! untouched.
//!
//! A client data service takes one subscriber; a second is refused at open
//! while the first is attached, or after it crashed until iceoryx2 cleans up
//! the dead node.
//!
//! A full subscriber queue makes `publish` wait rather than drop: the data
//! service disables safe overflow, a sample a full subscriber cannot take is
//! not delivered, and `publish` sends it again after a short sleep until the
//! subscriber has room. The wait is an async sleep, so it holds no thread and
//! dropping the future ends it.
//!
//! With no subscriber attached, `publish` sends a sample once and reports it
//! delivered. The service keeps the last 16 samples as history: a subscriber
//! that attaches later receives those and nothing before them, a `seq` hole
//! with no `Gap`. A subscriber that replaces a departed one while a sample is
//! being sent again can receive that sample twice, from the history and from
//! the resend.

use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use datamancer_core::{EventSink, MarketEvent, PublishOutcome, Result as CoreResult};
use iceoryx2::port::publisher::Publisher;
use iceoryx2::prelude::{BackpressureStrategy, Node, ipc_threadsafe};
use iceoryx2::service::port_factory::{PortFactory as _, publish_subscribe::PortFactory};

use crate::error::{Result, TransportError};
use crate::naming::DataServiceName;
use crate::payload::{DataPayload, to_pod};
use crate::symbol_table::{SymbolAnnouncement, SymbolId, SymbolTable};

/// Subscribers a client data service takes: its one consumer.
const DATA_MAX_SUBSCRIBERS: usize = 1;

/// Default subscriber count for the per-client announcement service.
const DEFAULT_MAX_SUBSCRIBERS: usize = 8;

/// Default retained-history depth for late-joiner catch-up on the data plane.
const DEFAULT_DATA_HISTORY: usize = 16;

/// Republish the full announcement table every this many data sends. With
/// single-shot announcements a subscriber that joins after a symbol's
/// announcement has aged out of the announcement-service history would hold its
/// samples forever. A periodic idempotent republish keeps the recent history
/// ring populated with the whole table, bounding the stranding window for late
/// joiners to at most this many events (independent of `flush` cadence).
const REANNOUNCE_INTERVAL: u64 = 256;

/// First wait before a data sample a full subscriber refused is sent again.
const RESEND_FIRST_WAIT: Duration = Duration::from_millis(1);

/// Longest wait between two sends of a refused data sample; the wait doubles
/// from [`RESEND_FIRST_WAIT`] up to this.
const RESEND_MAX_WAIT: Duration = Duration::from_millis(20);

/// Mutable interning state shared behind the sink's lock. `announced` tracks
/// which interned ids have had their announcement published, so a new symbol is
/// announced exactly once (plus periodic full-table republish via [`flush`]).
struct State {
    table: SymbolTable,
    announced: std::collections::HashSet<u32>,
}

/// Per-client iceoryx2 data-plane sink. Not cloneable; dropped on client-session
/// close (after [`flush`](EventSink::flush)).
pub struct Iceoryx2DataSink {
    /// The data service's name, exactly as created; what the daemon
    /// advertises in its `open-client` / `open-query` reply.
    name: DataServiceName,
    data_service: PortFactory<ipc_threadsafe::Service, DataPayload, ()>,
    data: Publisher<ipc_threadsafe::Service, DataPayload, ()>,
    announcements: Publisher<ipc_threadsafe::Service, SymbolAnnouncement, ()>,
    state: Mutex<State>,
    /// Data-send counter driving the periodic announcement-table republish (see
    /// [`REANNOUNCE_INTERVAL`]).
    sends: std::sync::atomic::AtomicU64,
}

impl Iceoryx2DataSink {
    /// Create the client's data + announcement services on `node` under
    /// `name` (`{prefix}/v{WIRE_VERSION}/data/{client_id}` and
    /// `.../symbols/{client_id}`, see [`DataServiceName`]) and their
    /// publishers. A [`DataServiceName`] only exists for a validated
    /// [`ServicePrefix`](crate::ServicePrefix), so daemons with different
    /// prefixes never share a service and no name is re-checked here. `node`
    /// is the one process-wide iceoryx2 node (Phase 5 owns its lifetime); the
    /// caller keeps it alive for the sink's lifetime.
    ///
    /// # Errors
    ///
    /// Returns [`TransportError::Service`] if a service or a publisher port
    /// cannot be created.
    pub fn new(node: &Node<ipc_threadsafe::Service>, name: DataServiceName) -> Result<Self> {
        let (data_name, ann_name) = name.service_names();

        let data_service = node
            .service_builder(&data_name)
            .publish_subscribe::<DataPayload>()
            // No silent overflow: a full subscriber refuses the sample and
            // `deliver` sends it again.
            .enable_safe_overflow(false)
            .history_size(DEFAULT_DATA_HISTORY)
            // The subscriber buffer must exceed the history size; over-provision.
            .subscriber_max_buffer_size(DEFAULT_DATA_HISTORY * 2)
            .max_subscribers(DATA_MAX_SUBSCRIBERS)
            .open_or_create()
            .map_err(|e| crate::error::open_or_create_error(data_name.as_str(), e))?;
        let ann_history = DEFAULT_MAX_SUBSCRIBERS * 64;
        let ann_service = node
            .service_builder(&ann_name)
            .publish_subscribe::<SymbolAnnouncement>()
            // Announcements are idempotent upserts; safe overflow is fine and a
            // generous history lets late joiners drain the full table.
            .history_size(ann_history)
            .subscriber_max_buffer_size(ann_history * 2)
            .max_subscribers(DEFAULT_MAX_SUBSCRIBERS)
            .open_or_create()
            .map_err(|e| crate::error::open_or_create_error(ann_name.as_str(), e))?;

        let data = data_service
            .publisher_builder()
            .backpressure_strategy(BackpressureStrategy::DiscardData)
            .create()
            .map_err(|e| TransportError::Service(format!("{e:?}")))?;
        let announcements = ann_service
            .publisher_builder()
            .create()
            .map_err(|e| TransportError::Service(format!("{e:?}")))?;

        Ok(Self {
            name,
            data_service,
            data,
            announcements,
            state: Mutex::new(State {
                table: SymbolTable::new(),
                announced: std::collections::HashSet::new(),
            }),
            sends: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// The data service's name, exactly as created on the node. The daemon
    /// advertises its `Display` form; a consumer parses that back and opens
    /// it (see [`DataSubscriber::open`](crate::DataSubscriber::open)).
    #[must_use]
    pub fn data_service_name(&self) -> &DataServiceName {
        &self.name
    }

    /// Publish one announcement sample.
    fn send_announcement(&self, announcement: SymbolAnnouncement) -> Result<()> {
        let sample = self
            .announcements
            .loan_uninit()
            .map_err(|e| TransportError::Send(format!("{e:?}")))?;
        let sample = sample.write_payload(announcement);
        sample
            .send()
            .map_err(|e| TransportError::Send(format!("{e:?}")))?;
        Ok(())
    }

    /// Send one data sample. Returns how many subscribers received it.
    fn send_data(&self, pod: DataPayload) -> Result<usize> {
        let sample = self
            .data
            .loan_uninit()
            .map_err(|e| TransportError::Send(format!("{e:?}")))?;
        sample
            .write_payload(pod)
            .send()
            .map_err(|e| TransportError::Send(format!("{e:?}")))
    }

    /// Whether a subscriber is connected to the data service.
    fn has_data_subscriber(&self) -> bool {
        self.data_service.dynamic_config().number_of_subscribers() > 0
    }

    /// Convert to POD (interning + announce-if-new), then publish on the data
    /// plane. Returns `Ok(true)` if delivered (or deliberately suppressed),
    /// `Ok(false)` if the transport refused it (caller should buffer).
    ///
    /// While a subscriber is connected and refuses the sample because its
    /// queue is full, the same sample is sent again after a sleep that starts
    /// at [`RESEND_FIRST_WAIT`] and doubles up to [`RESEND_MAX_WAIT`]. Whether
    /// one is connected is read before the first send: with none, the sample
    /// is sent once and stays in the service's history, where a subscriber
    /// that attaches later receives it.
    async fn deliver(&self, ev: &MarketEvent) -> Result<bool> {
        self.deliver_observed(ev, || {}).await
    }

    /// [`Self::deliver`], calling `after_first_send` once the sample's first
    /// send has returned.
    async fn deliver_observed(
        &self,
        ev: &MarketEvent,
        after_first_send: impl FnOnce() + Send,
    ) -> Result<bool> {
        let (pod, pending_announcement) = {
            let mut state = self.state.lock().expect("sink state poisoned");
            let Some(pod) = to_pod(ev, &mut state.table)
                .map_err(|e| TransportError::Interning(e.to_string()))?
            else {
                // `to_pod` returns `None` for two reasons, which must not be
                // conflated. A `Control` is *intentional* suppression
                // (connection-scoped controls ride the diagnostics plane) —
                // legitimately "delivered". Any non-`Control` `None` is an
                // unknown future `MarketEvent` data variant this transport build
                // cannot encode (`control_to_pod` is exhaustive, so a Control is
                // never the unknown case); surface it rather than silently
                // acking it as delivered. (It becomes `Rejected` upstream; a
                // build whose core outpaces its transport is the signal to
                // update this crate.)
                if matches!(ev, MarketEvent::Control(_)) {
                    return Ok(true);
                }
                return Err(TransportError::Unsupported(format!(
                    "MarketEvent variant not encodable by this transport build: {ev:?}"
                )));
            };
            // Decide whether to announce by *reading* the set — do not mark it
            // announced yet. Marking before the send succeeds would, on a send
            // failure, leave the symbol permanently flagged-but-unannounced
            // (unresolvable by subscribers until the next flush).
            let announcement =
                if pod.symbol != SymbolId::CONNECTION && !state.announced.contains(&pod.symbol.0) {
                    state.table.announcement(pod.symbol)
                } else {
                    None
                };
            (pod, announcement)
        };

        if let Some(announcement) = pending_announcement {
            // Propagate a send failure (`?`) *before* marking announced, so a
            // failed announcement is retried on the next event for this symbol.
            self.send_announcement(announcement)?;
            self.state
                .lock()
                .expect("sink state poisoned")
                .announced
                .insert(pod.symbol.0);
        }

        // Read before the send: a subscriber that registers after it receives
        // this sample from the history, so a resend would give it a second
        // copy.
        let connected = self.has_data_subscriber();
        let mut received = self.send_data(pod)?;
        after_first_send();
        let mut wait = RESEND_FIRST_WAIT;
        while received == 0 && connected && self.has_data_subscriber() {
            tokio::time::sleep(wait).await;
            wait = (wait * 2).min(RESEND_MAX_WAIT);
            received = self.send_data(pod)?;
        }

        // Periodically refresh the announcement history ring so a subscriber that
        // joined after a symbol's one-shot announcement aged out can still
        // resolve it (single-shot announcements would otherwise strand late
        // joiners). Idempotent upserts; bounded by the symbol-table size.
        let n = self
            .sends
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if (n + 1).is_multiple_of(REANNOUNCE_INTERVAL) {
            self.republish_all_announcements()?;
        }
        Ok(true)
    }

    /// Republish the entire symbol table (idempotent upserts) — used on flush so
    /// a late joiner that aged announcements out of history can recover.
    fn republish_all_announcements(&self) -> Result<()> {
        let announcements: Vec<SymbolAnnouncement> = {
            let state = self.state.lock().expect("sink state poisoned");
            state.table.announcements().collect()
        };
        for announcement in announcements {
            self.send_announcement(announcement)?;
        }
        Ok(())
    }
}

#[async_trait]
impl EventSink for Iceoryx2DataSink {
    async fn publish(&self, ev: MarketEvent) -> PublishOutcome {
        match self.deliver(&ev).await {
            Ok(true) => PublishOutcome::Delivered,
            Ok(false) | Err(_) => PublishOutcome::Rejected(ev),
        }
    }

    async fn publish_borrowed(&self, ev: &MarketEvent) -> PublishOutcome {
        match self.deliver(ev).await {
            Ok(true) => PublishOutcome::Delivered,
            Ok(false) | Err(_) => PublishOutcome::Rejected(ev.clone()),
        }
    }

    async fn flush(&self) -> CoreResult<()> {
        // iceoryx2 `send` copies into shared memory synchronously, so there is
        // no application-side data buffer to drain. `flush` is a final
        // full-table announcement republish so a late joiner can resolve every
        // symbol referenced by samples still in the data history.
        self.republish_all_announcements()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use datamancer_core::{
        AssetClass, EventSink, Instrument, MarketEvent, Price, ProviderId, Quantity, Seq,
        Timestamp, Trade,
    };
    use iceoryx2::prelude::{NodeBuilder, ipc_threadsafe};

    use super::Iceoryx2DataSink;
    use crate::{DataServiceName, DataSubscriber, ServicePrefix};

    fn trade(seq: u64) -> MarketEvent {
        let ts = i64::try_from(seq).expect("small");
        MarketEvent::Trade(Trade {
            instrument: Instrument::new(ProviderId::from_static("p"), AssetClass::Equity, "AAPL"),
            source_ts: Timestamp(ts),
            rx_ts: Timestamp(ts),
            seq: Seq(seq),
            price: Price(100),
            size: Quantity::from_raw(1),
        })
    }

    /// A subscriber that attaches between a sample's first send and the
    /// check for a connected subscriber receives that sample once, from the
    /// history, and no resend repeats it.
    #[tokio::test]
    #[ignore = "needs the iceoryx2 runtime"]
    async fn a_subscriber_attaching_during_a_send_receives_the_sample_once() {
        let node = NodeBuilder::new()
            .create::<ipc_threadsafe::Service>()
            .expect("node");
        let prefix = ServicePrefix::new("sink-attach-test")
            .expect("prefix")
            .boot_scoped(0, std::process::id())
            .expect("fits");
        let sink = Iceoryx2DataSink::new(&node, DataServiceName::new(&prefix, 1)).expect("sink");
        let attached = Mutex::new(None);
        let delivered = sink
            .deliver_observed(&trade(0), || {
                *attached.lock().expect("lock") =
                    Some(DataSubscriber::open(&node, sink.data_service_name()).expect("open"));
            })
            .await
            .expect("deliver");
        assert!(delivered);
        sink.publish(trade(1)).await;
        // Republish the announcement the subscriber attached after.
        sink.flush().await.expect("flush");
        let mut subscriber = attached.lock().expect("lock").take().expect("attached");

        let mut seqs = Vec::new();
        for _ in 0..200 {
            for ev in subscriber.poll().expect("poll") {
                if let MarketEvent::Trade(t) = ev {
                    seqs.push(t.seq.0);
                }
            }
            if seqs.len() >= 2 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        for ev in subscriber.poll().expect("poll") {
            if let MarketEvent::Trade(t) = ev {
                seqs.push(t.seq.0);
            }
        }
        assert_eq!(seqs, vec![0, 1]);
    }
}
