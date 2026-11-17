//! The remote WebSocket client surface (single bidirectional connection = one
//! client). Owns the listener, per-connection bridge, and the WS control
//! protocol. The event wire format + sink + writer live in the
//! `datamancer-transport-ws` crate; this module owns the part that touches the
//! orchestrator (`ClientSession`).

mod conn;
mod listener;
mod protocol;

pub use listener::serve;

/// The client sessions of the WS connections that have not closed theirs.
/// The shutdown drain reads it to wait for the ones an aborted connection
/// left to end by themselves.
#[derive(Clone, Default)]
pub struct WsSessions(
    std::sync::Arc<std::sync::Mutex<std::collections::HashSet<datamancer::ClientSessionId>>>,
);

impl WsSessions {
    /// Note that a connection opened session `id`.
    pub(crate) fn opened(&self, id: datamancer::ClientSessionId) {
        if let Ok(mut ids) = self.0.lock() {
            ids.insert(id);
        }
    }

    /// Note that the connection holding session `id` closed it.
    pub(crate) fn closed(&self, id: datamancer::ClientSessionId) {
        if let Ok(mut ids) = self.0.lock() {
            ids.remove(&id);
        }
    }

    /// Whether `id` is a session some connection opened and did not close.
    pub fn holds(&self, id: datamancer::ClientSessionId) -> bool {
        self.0.lock().is_ok_and(|ids| ids.contains(&id))
    }
}
pub use protocol::{WsHealthPush, WsReply, WsRequest};
