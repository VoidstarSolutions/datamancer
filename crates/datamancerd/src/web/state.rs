//! [`WebState`] — the cheap-`Clone` handle the web handlers read from.
//!
//! Handlers are **lock-free and never block the shared runtime**: they read
//! pre-assembled [`SystemSnapshot`]s out of two independent [`ArcSwap`]s, never
//! the on-demand (potentially-blocking) `Datamancer::snapshot()` accessor. The
//! daemon owns two refresh tasks (see [`crate::web::refresh`]) that publish into
//! these `ArcSwap`s on independent cadences:
//!
//! - the **live-state** swap, refreshed at `live_state_cadence_ms` and streamed
//!   to the browser over SSE;
//! - the **cache-catalog** swap, refreshed at the slower `cache_catalog_cadence_ms`
//!   so a catalog walk never stalls live updates.
//!
//! `WebState` carries no `Datamancer` handle at all, so a handler *cannot*
//! invoke the on-demand accessor — the non-blocking property holds by
//! construction (guarded by `web_handler_does_not_block_runtime`).

use std::sync::Arc;

use arc_swap::ArcSwap;
use datamancer::SystemSnapshot;
use tokio::sync::watch;

/// A lock-free, cheap-`Clone` handle to the latest published snapshots.
#[derive(Clone)]
pub struct WebState {
    /// Live-state swap (full snapshot; refreshed on the fast cadence).
    live: Arc<ArcSwap<SystemSnapshot>>,
    /// Cache-catalog swap (full snapshot; refreshed on the slow cadence).
    cache: Arc<ArcSwap<SystemSnapshot>>,
    /// Bumped by the live-refresh task on every publish; drives SSE wakeups.
    live_version: watch::Receiver<u64>,
    /// The active credential-store backend name (`"keychain"`,
    /// `"secret-service"`, `"credential-manager"`, `"file"`), stamped into
    /// `/api/health`'s
    /// `HealthView.daemon.credential_backend` — the same bootstrap fact the
    /// daemon actor stamps onto `ping`/`Health` dispatch, threaded here so the
    /// web layer is another consumer of one fact, not a second source of it.
    credential_backend: &'static str,
}

impl WebState {
    /// Build from the two shared swaps, the live-version receiver, and the
    /// bootstrap-time credential-backend name. The daemon wires this; tests
    /// use [`WebState::fixed`].
    #[must_use]
    pub fn new(
        live: Arc<ArcSwap<SystemSnapshot>>,
        cache: Arc<ArcSwap<SystemSnapshot>>,
        live_version: watch::Receiver<u64>,
        credential_backend: &'static str,
    ) -> Self {
        Self {
            live,
            cache,
            live_version,
            credential_backend,
        }
    }

    /// Convenience for tests over static snapshots: wrap the given snapshots in
    /// fresh swaps with an idle version channel.
    #[cfg(test)]
    #[must_use]
    pub fn fixed(live: SystemSnapshot, cache: SystemSnapshot) -> Self {
        let (keep_alive, rx) = watch::channel(0);
        // Keep the sender alive for the lifetime of the receiver so SSE can
        // still observe the initial value; leaking one tiny sender per fixed
        // state is acceptable in the test path.
        std::mem::forget(keep_alive);
        Self::new(
            Arc::new(ArcSwap::from_pointee(live)),
            Arc::new(ArcSwap::from_pointee(cache)),
            rx,
            "keychain",
        )
    }

    /// The latest live-state snapshot (lock-free `load`).
    #[must_use]
    pub fn live_snapshot(&self) -> Arc<SystemSnapshot> {
        self.live.load_full()
    }

    /// The latest cache-catalog snapshot (lock-free `load`).
    #[must_use]
    pub fn cache_snapshot(&self) -> Arc<SystemSnapshot> {
        self.cache.load_full()
    }

    /// A clone of the live-version receiver, for SSE change-notification.
    #[must_use]
    pub(crate) fn live_version(&self) -> watch::Receiver<u64> {
        self.live_version.clone()
    }

    /// The active credential-store backend name, stamped into `/api/health`.
    #[must_use]
    pub(crate) fn credential_backend(&self) -> &'static str {
        self.credential_backend
    }
}
