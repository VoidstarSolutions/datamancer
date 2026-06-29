//! Subscriber-side helper: resolve `SymbolId -> Instrument` and reconstruct
//! logical events, holding data samples that outrun their announcement.
//!
//! The data service and the announcement service are two independent iceoryx2
//! services with **no mutual delivery-order guarantee**: a data sample
//! referencing `SymbolId(k)` can arrive before the `SymbolAnnouncement` for
//! `k`. [`HoldBuffer`] therefore queues an unresolved sample and replays it once
//! the announcement resolves it, rather than dropping or erroring. This logic is
//! pure (no iceoryx2) so it is unit-tested without the runtime; [`DataSubscriber`]
//! wraps it around the live services.

use datamancer_core::MarketEvent;

use crate::payload::{DataPayload, FromPodError, from_pod};
use crate::symbol_table::{SymbolAnnouncement, SymbolDecodeError, SymbolResolver};

/// Reorders data samples against announcements: holds any sample whose
/// `SymbolId` is not yet announced and replays it once it resolves.
#[derive(Debug, Default)]
pub struct HoldBuffer {
    resolver: SymbolResolver,
    held: Vec<DataPayload>,
}

impl HoldBuffer {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply an announcement (idempotent upsert) and return any previously held
    /// samples that this announcement (or earlier ones) now resolves, in held
    /// order.
    ///
    /// # Errors
    ///
    /// Returns [`SymbolDecodeError`] if the announcement tuple is malformed; the
    /// held buffer is left unchanged in that case.
    pub fn apply_announcement(
        &mut self,
        announcement: &SymbolAnnouncement,
    ) -> Result<Vec<MarketEvent>, SymbolDecodeError> {
        self.resolver.apply(announcement)?;
        Ok(self.drain_resolved())
    }

    /// Offer a freshly received data sample. Returns the events that are now
    /// reconstructable (this sample if resolvable, plus any held ones that a
    /// prior announcement already covers but were queued behind it). An
    /// unresolved sample is held.
    pub fn offer(&mut self, payload: DataPayload) -> Vec<MarketEvent> {
        self.held.push(payload);
        self.drain_resolved()
    }

    /// Number of samples currently held awaiting their announcement.
    #[must_use]
    pub fn held_len(&self) -> usize {
        self.held.len()
    }

    /// Reconstruct every held sample that now resolves, removing it from the
    /// hold queue and preserving held order. A sample whose discriminant is
    /// malformed is dropped (it can never resolve).
    fn drain_resolved(&mut self) -> Vec<MarketEvent> {
        let mut out = Vec::new();
        let mut still_held = Vec::new();
        for payload in std::mem::take(&mut self.held) {
            match from_pod(&payload, &self.resolver) {
                Ok(ev) => out.push(ev),
                Err(FromPodError::Unresolved(_)) => still_held.push(payload),
                Err(FromPodError::BadDiscriminant) => {} // unrecoverable: drop
            }
        }
        self.held = still_held;
        out
    }
}

pub use runtime::DataSubscriber;

mod runtime {
    use super::HoldBuffer;
    use crate::error::{Result, TransportError};
    use crate::payload::DataPayload;
    use crate::symbol_table::SymbolAnnouncement;
    use datamancer_core::MarketEvent;
    use iceoryx2::port::subscriber::Subscriber;
    use iceoryx2::prelude::{Node, ServiceName, ipc_threadsafe};

    /// Live subscriber over a client's data + announcement services. Public so
    /// the Phase-5 fan-out node can reuse it.
    pub struct DataSubscriber {
        data: Subscriber<ipc_threadsafe::Service, DataPayload, ()>,
        announcements: Subscriber<ipc_threadsafe::Service, SymbolAnnouncement, ()>,
        buffer: HoldBuffer,
    }

    impl DataSubscriber {
        /// Open the data and announcement services for `client_id` on `node`.
        ///
        /// # Errors
        ///
        /// Returns [`TransportError`] if either service or subscriber port
        /// cannot be opened.
        pub fn open(node: &Node<ipc_threadsafe::Service>, client_id: u64) -> Result<Self> {
            let data_name: ServiceName = format!("datamancer/data/{client_id}")
                .as_str()
                .try_into()
                .map_err(|e| TransportError::BadServiceName(format!("{e:?}")))?;
            let ann_name: ServiceName = format!("datamancer/symbols/{client_id}")
                .as_str()
                .try_into()
                .map_err(|e| TransportError::BadServiceName(format!("{e:?}")))?;

            let data_service = node
                .service_builder(&data_name)
                .publish_subscribe::<DataPayload>()
                .open_or_create()
                .map_err(|e| TransportError::Service(format!("{e:?}")))?;
            let ann_service = node
                .service_builder(&ann_name)
                .publish_subscribe::<SymbolAnnouncement>()
                .open_or_create()
                .map_err(|e| TransportError::Service(format!("{e:?}")))?;

            let data = data_service
                .subscriber_builder()
                .create()
                .map_err(|e| TransportError::Service(format!("{e:?}")))?;
            let announcements = ann_service
                .subscriber_builder()
                .create()
                .map_err(|e| TransportError::Service(format!("{e:?}")))?;

            Ok(Self {
                data,
                announcements,
                buffer: HoldBuffer::new(),
            })
        }

        /// Drain all currently available announcements then all data samples,
        /// returning the reconstructable logical events in arrival order.
        /// Unresolved samples are held for a later call.
        ///
        /// # Errors
        ///
        /// Returns [`TransportError`] on an iceoryx2 receive failure.
        pub fn poll(&mut self) -> Result<Vec<MarketEvent>> {
            let mut events = Vec::new();
            while let Some(sample) = self
                .announcements
                .receive()
                .map_err(|e| TransportError::Send(format!("{e:?}")))?
            {
                // Surface a malformed announcement instead of silently dropping
                // it: discarding leaves every data sample for that symbol held in
                // the buffer forever with no signal.
                let resolved = self
                    .buffer
                    .apply_announcement(&sample)
                    .map_err(|e| TransportError::Interning(e.to_string()))?;
                events.extend(resolved);
            }
            while let Some(sample) = self
                .data
                .receive()
                .map_err(|e| TransportError::Send(format!("{e:?}")))?
            {
                events.extend(self.buffer.offer(*sample));
            }
            Ok(events)
        }

        /// Number of data samples held awaiting their announcement.
        #[must_use]
        pub fn held_len(&self) -> usize {
            self.buffer.held_len()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::HoldBuffer;
    use crate::payload::to_pod;
    use crate::symbol_table::SymbolTable;
    use datamancer_core::{
        AssetClass, Instrument, MarketEvent, Price, ProviderId, Seq, Timestamp, Trade,
    };

    fn trade(symbol: &str, seq: u64) -> MarketEvent {
        let ts = i64::try_from(seq).unwrap();
        MarketEvent::Trade(Trade {
            instrument: Instrument::new(
                ProviderId::from_static("alpaca"),
                AssetClass::Crypto,
                symbol,
            ),
            source_ts: Timestamp(ts),
            rx_ts: Timestamp(ts + 1),
            seq: Seq(seq),
            price: Price(100),
            size: 1,
        })
    }

    #[test]
    fn data_before_announcement_resolves() {
        let mut table = SymbolTable::new();
        let ev = trade("BTC/USD", 5);
        let pod = to_pod(&ev, &mut table).unwrap().unwrap();
        let announcement = table.announcement(pod.symbol).unwrap();

        let mut buffer = HoldBuffer::new();
        // Data arrives first: it has no announcement yet, so it is held.
        let early = buffer.offer(pod);
        assert!(early.is_empty());
        assert_eq!(buffer.held_len(), 1);

        // Announcement arrives: the held sample now resolves.
        let resolved = buffer.apply_announcement(&announcement).unwrap();
        assert_eq!(resolved, vec![ev]);
        assert_eq!(buffer.held_len(), 0);
    }

    #[test]
    fn announcement_first_resolves_immediately() {
        let mut table = SymbolTable::new();
        let ev = trade("ETH/USD", 1);
        let pod = to_pod(&ev, &mut table).unwrap().unwrap();
        let announcement = table.announcement(pod.symbol).unwrap();

        let mut buffer = HoldBuffer::new();
        assert!(buffer.apply_announcement(&announcement).unwrap().is_empty());
        let out = buffer.offer(pod);
        assert_eq!(out, vec![ev]);
    }

    #[test]
    fn multiple_held_samples_release_in_order() {
        let mut table = SymbolTable::new();
        let e0 = trade("BTC/USD", 0);
        let e1 = trade("BTC/USD", 1);
        let p0 = to_pod(&e0, &mut table).unwrap().unwrap();
        let p1 = to_pod(&e1, &mut table).unwrap().unwrap();
        let announcement = table.announcement(p0.symbol).unwrap();

        let mut buffer = HoldBuffer::new();
        buffer.offer(p0);
        buffer.offer(p1);
        assert_eq!(buffer.held_len(), 2);
        let resolved = buffer.apply_announcement(&announcement).unwrap();
        assert_eq!(resolved, vec![e0, e1]);
    }
}
