//! The per-client data-plane sink (`impl EventSink`) and its announcement
//! service.
//!
//! One iceoryx2 pub-sub service per client carries the multiplexed data
//! substreams as [`DataPayload`]; a second per-client service carries
//! [`SymbolAnnouncement`]s so subscribers resolve `SymbolId -> Instrument`.
//! Interning + POD conversion live entirely here; core `MarketEvent` is
//! untouched.
//!
//! Backpressure is **blocking**: the data service disables safe overflow and
//! the publisher retries until delivered, so a full subscriber queue stalls the
//! publisher rather than silently dropping — loss is then accounted by the
//! core-side resume buffer as a `Control::Gap`.

use std::sync::Mutex;

use async_trait::async_trait;
use datamancer_core::{EventSink, MarketEvent, PublishOutcome, Result as CoreResult};
use iceoryx2::port::publisher::Publisher;
use iceoryx2::prelude::{BackpressureStrategy, Node, ServiceName, ipc_threadsafe};

use crate::error::{Result, TransportError};
use crate::payload::{DataPayload, to_pod};
use crate::symbol_table::{SymbolAnnouncement, SymbolId, SymbolTable};

/// Default over-provisioned subscriber count for the per-client services.
/// Over-provisioned because service resources are fixed at creation; the
/// realistic fan-out (a single consumer process, plus the Phase-5 node) is
/// small.
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
    data: Publisher<ipc_threadsafe::Service, DataPayload, ()>,
    announcements: Publisher<ipc_threadsafe::Service, SymbolAnnouncement, ()>,
    state: Mutex<State>,
    /// Data-send counter driving the periodic announcement-table republish (see
    /// [`REANNOUNCE_INTERVAL`]).
    sends: std::sync::atomic::AtomicU64,
}

impl Iceoryx2DataSink {
    /// Create the per-client data + announcement services on `node` and their
    /// publishers. `node` is the one process-wide iceoryx2 node (Phase 5 owns
    /// its lifetime); the caller keeps it alive for the sink's lifetime.
    ///
    /// # Errors
    ///
    /// Returns [`TransportError`] if a service name is invalid or a service /
    /// publisher port cannot be created.
    pub fn new(node: &Node<ipc_threadsafe::Service>, client_id: u64) -> Result<Self> {
        let data_name = service_name(&format!("datamancer/data/{client_id}"))?;
        let ann_name = service_name(&format!("datamancer/symbols/{client_id}"))?;

        let data_service = node
            .service_builder(&data_name)
            .publish_subscribe::<DataPayload>()
            // Blocking backpressure: no silent overflow; the publisher retries.
            .enable_safe_overflow(false)
            .history_size(DEFAULT_DATA_HISTORY)
            // The subscriber buffer must exceed the history size; over-provision.
            .subscriber_max_buffer_size(DEFAULT_DATA_HISTORY * 2)
            .max_subscribers(DEFAULT_MAX_SUBSCRIBERS)
            .open_or_create()
            .map_err(|e| TransportError::Service(format!("{e:?}")))?;
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
            .map_err(|e| TransportError::Service(format!("{e:?}")))?;

        let data = data_service
            .publisher_builder()
            .backpressure_strategy(BackpressureStrategy::RetryUntilDelivered)
            .create()
            .map_err(|e| TransportError::Service(format!("{e:?}")))?;
        let announcements = ann_service
            .publisher_builder()
            .create()
            .map_err(|e| TransportError::Service(format!("{e:?}")))?;

        Ok(Self {
            data,
            announcements,
            state: Mutex::new(State {
                table: SymbolTable::new(),
                announced: std::collections::HashSet::new(),
            }),
            sends: std::sync::atomic::AtomicU64::new(0),
        })
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

    /// Convert to POD (interning + announce-if-new), then publish on the data
    /// plane. Returns `Ok(true)` if delivered (or deliberately suppressed),
    /// `Ok(false)` if the transport refused it (caller should buffer).
    fn deliver(&self, ev: &MarketEvent) -> Result<bool> {
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

        let sample = self
            .data
            .loan_uninit()
            .map_err(|e| TransportError::Send(format!("{e:?}")))?;
        let sample = sample.write_payload(pod);
        sample
            .send()
            .map_err(|e| TransportError::Send(format!("{e:?}")))?;

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
        match self.deliver(&ev) {
            Ok(true) => PublishOutcome::Delivered,
            Ok(false) | Err(_) => PublishOutcome::Rejected(ev),
        }
    }

    async fn publish_borrowed(&self, ev: &MarketEvent) -> PublishOutcome {
        match self.deliver(ev) {
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

fn service_name(name: &str) -> Result<ServiceName> {
    name.try_into()
        .map_err(|e| TransportError::BadServiceName(format!("{e:?}")))
}
