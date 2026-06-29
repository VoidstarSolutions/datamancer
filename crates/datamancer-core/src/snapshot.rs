//! Introspection identifiers shared across the orchestrator and (later) the
//! diagnostics plane.
//!
//! Phase 2 only needs [`ClientSessionId`]; Phase 3 grows this module into the
//! full system-snapshot type surface. Keeping the id here resolves the layering
//! sub-checkpoint: a client-session identity is referenced both by the
//! orchestrator (`datamancer`) and by serde-derived snapshot types that must
//! live in `datamancer-core`.

use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

/// Process-scoped identity for a multiplexing client session.
///
/// Allocated from a monotonic process-global counter via [`ClientSessionId::next`];
/// never persisted and not meaningful across processes. Phase 3 surfaces it in
/// the live-state snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClientSessionId(pub u64);

impl ClientSessionId {
    /// Allocate the next process-global client-session id.
    #[must_use]
    pub fn next() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        Self(COUNTER.fetch_add(1, Ordering::Relaxed))
    }
}

#[cfg(test)]
mod tests {
    use super::ClientSessionId;

    #[test]
    fn ids_are_monotonic_and_distinct() {
        let a = ClientSessionId::next();
        let b = ClientSessionId::next();
        assert_ne!(a, b);
        assert!(b.0 > a.0);
    }
}
