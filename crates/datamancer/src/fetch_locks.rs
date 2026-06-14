//! Process-local single-flight registry for historical fetches.
//!
//! At most one task may hold the fetch slot for a given [`CacheKey`] at a
//! time. Concurrent acquirers for the *same* key queue; acquirers for
//! *distinct* keys never contend. The returned guard releases the slot on
//! drop — including task cancellation — so a winner that is dropped mid-fetch
//! never strands its waiters.
//!
//! This is the read-through coalescer: it bounds a cold-cache parameter sweep
//! to one provider fetch per key instead of one per session. It is in-process
//! only (one `Datamancer` instance); cross-process coalescing is the parked
//! consumer-transport design, explicitly out of scope.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use datamancer_core::CacheKey;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

/// Hands out a per-`CacheKey` async guard with mutual exclusion. Cheap to
/// clone (an `Arc` around the shared map); every clone shares one registry.
#[derive(Clone, Default)]
#[allow(dead_code)] // wired in Task 2
pub(crate) struct FetchLocks {
    map: Arc<Mutex<HashMap<CacheKey, Weak<AsyncMutex<()>>>>>,
}

#[allow(dead_code)] // wired in Task 2
impl FetchLocks {
    /// Acquire the fetch slot for `key`, waiting if another task holds it.
    ///
    /// The map holds a `Weak` to each key's lock so an entry whose holders
    /// have all gone away can be replaced on the next request (mirrors the
    /// `live_sessions` registry). Distinct keys never block one another.
    pub(crate) async fn acquire(&self, key: &CacheKey) -> OwnedMutexGuard<()> {
        let lock = {
            let mut map = self.map.lock().expect("fetch-locks mutex poisoned");
            if let Some(existing) = map.get(key).and_then(Weak::upgrade) {
                existing
            } else {
                let fresh = Arc::new(AsyncMutex::new(()));
                map.insert(key.clone(), Arc::downgrade(&fresh));
                fresh
            }
        };
        lock.lock_owned().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datamancer_core::{AssetClass, BarInterval, EventKind, Instrument, ProviderId, Timestamp};
    use futures::FutureExt;

    fn key(from: i64, to: i64) -> CacheKey {
        CacheKey {
            instrument: Instrument::new(ProviderId::from_static("rec"), AssetClass::Equity, "AAPL"),
            kind: EventKind::Bar(BarInterval::OneMinute),
            from: Timestamp(from),
            to: Timestamp(to),
        }
    }

    #[tokio::test]
    async fn same_key_serializes() {
        let locks = FetchLocks::default();
        let k = key(0, 1000);

        let first = locks.acquire(&k).await;
        assert!(
            locks.acquire(&k).now_or_never().is_none(),
            "same-key acquire must wait while the slot is held"
        );

        drop(first);
        assert!(
            locks.acquire(&k).now_or_never().is_some(),
            "slot must be acquirable after the holder drops"
        );
    }

    #[tokio::test]
    async fn distinct_keys_do_not_contend() {
        let locks = FetchLocks::default();
        let a = locks.acquire(&key(0, 1000)).await;
        assert!(
            locks.acquire(&key(1000, 2000)).now_or_never().is_some(),
            "distinct keys must not block each other"
        );
        drop(a);
    }
}
