//! Event delivery trait surface.
//!
//! [`EventSink`] is the seam between an authoritative per-`(instrument, kind)`
//! session and whatever transport carries its events to a consumer — today an
//! in-process channel, in a later phase an iceoryx2 publisher. The session's
//! controller owns the tap-log tee and the resume buffer *core-side* of the
//! sink, so every sink implementation inherits them; the sink is responsible
//! only for moving a fully-formed, already-`seq`-stamped event onto its
//! transport in delivery order.

use async_trait::async_trait;

use crate::{error::Result, event::MarketEvent};

/// Receives an authoritative per-`(instrument, kind)` session's events in `seq`
/// order. Implementations own their transport (an in-process channel, an
/// iceoryx2 publisher, ...). `seq` is already stamped at the source before
/// `publish`; the sink must preserve order and never renumber.
#[async_trait]
pub trait EventSink: Send + Sync {
    /// Publish one fully-formed, `seq`-stamped event in delivery order.
    ///
    /// Returns [`PublishOutcome::Rejected`] (handing the event back) when the
    /// transport cannot accept it — e.g. the in-process consumer dropped its
    /// stream — so the caller can divert it to the resume buffer.
    async fn publish(&self, ev: MarketEvent) -> PublishOutcome;

    /// Publish one fully-formed, `seq`-stamped event from a borrow.
    ///
    /// For transports that serialize out of a borrow (so the caller keeps
    /// ownership). The default clones and forwards to [`publish`](Self::publish)
    /// and discards a `Rejected` payload, since the borrow path cannot hand a
    /// borrowed event back. Serializing sinks (Phase 4) override this.
    async fn publish_borrowed(&self, ev: &MarketEvent) -> PublishOutcome {
        self.publish(ev.clone()).await
    }

    /// Flush any transport-side buffering (shutdown ordering). The core-side
    /// resume buffer is NOT the sink's concern — the controller flushes that.
    ///
    /// # Errors
    ///
    /// Returns an error if the transport's flush fails. The in-process sink is
    /// always `Ok` (no transport buffer).
    async fn flush(&self) -> Result<()>;
}

/// Outcome of an [`EventSink::publish`] call.
#[derive(Debug)]
pub enum PublishOutcome {
    /// The transport accepted the event.
    Delivered,
    /// The transport refused the event; ownership is returned for buffering.
    Rejected(MarketEvent),
}
