# Single-Flight Cache Fetch (Per-`CacheKey` Queueing) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ensure at most one outstanding provider fetch per `CacheKey` within one `Datamancer` process, so a cold-cache parameter sweep collapses hundreds of identical history fetches to one per key.

**Architecture:** A process-local `FetchLocks` registry (per-`CacheKey` async mutex) lives on the shared `DatamancerInner`. The cached read-through path (`Controller::run_historical_cached`) does an unlocked `gaps()` pre-check; if there is anything to fetch it acquires the key's slot, **re-runs `gaps()` against fresh coverage** (re-tile), and only then fetches — holding the slot across the fetch. Waiters that wake to find the range now covered release immediately and replay from cache. Correctness under partial-fetch/failure falls out of re-tiling: coverage, not the winner's success, drives what each session fetches.

**Tech Stack:** Rust (edition 2024), Tokio (`tokio::sync::Mutex`/`OwnedMutexGuard`), `async-trait`, in-memory `SurrealCache` for tests. Workspace lints: `clippy::pedantic = deny`.

---

## File Structure

- **Create** `crates/datamancer/src/fetch_locks.rs` — the `FetchLocks` registry. One responsibility: hand out a per-`CacheKey` async guard with mutual exclusion. Self-contained, unit-tested in-module.
- **Modify** `crates/datamancer/src/session.rs`
  - register `mod fetch_locks;` and import `FetchLocks`,
  - add a `fetch_locks: FetchLocks` field to `DatamancerInner` (struct ~line 165) and initialize it in `DatamancerBuilder::build`,
  - add a `fetch_locks: FetchLocks` field to `Controller` (struct ~line 690) and populate it where the `Controller` is constructed (~line 288),
  - wrap the gap-fetch decision in `run_historical_cached` (~line 1067) with the single-flight acquire + re-tile.
- **Modify** `crates/datamancer/tests/historical_cache.rs` — add a gated provider and the concurrency/resilience integration tests.
- **Modify** `crates/datamancer/README.md` — add a short single-flight note under the cache section; cross-reference the spec.

No `datamancer-core` changes. No `HistoricalCache` trait changes. `run_backfill` (the stitched-live backfill, ~line 1118) uses the same gap pattern but is **out of scope** here — note left in code, not wired.

---

## Task 1: `FetchLocks` registry module

**Files:**
- Create: `crates/datamancer/src/fetch_locks.rs`
- Modify: `crates/datamancer/src/session.rs` (add `mod fetch_locks;`)

- [ ] **Step 1: Create the module with `mod` registration**

Add this line to `crates/datamancer/src/session.rs` near the other `use`/module lines at the top of the file (after the existing `use` block, before `pub struct Datamancer`):

```rust
mod fetch_locks;
use fetch_locks::FetchLocks;
```

Create `crates/datamancer/src/fetch_locks.rs` with the implementation:

```rust
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
pub(crate) struct FetchLocks {
    map: Arc<Mutex<HashMap<CacheKey, Weak<AsyncMutex<()>>>>>,
}

impl FetchLocks {
    /// Acquire the fetch slot for `key`, waiting if another task holds it.
    ///
    /// The map holds a `Weak` to each key's lock so an entry whose holders
    /// have all gone away can be replaced on the next request (mirrors the
    /// `live_sessions` registry). Distinct keys never block one another.
    pub(crate) async fn acquire(&self, key: &CacheKey) -> OwnedMutexGuard<()> {
        let lock = {
            let mut map = self.map.lock().expect("fetch-locks mutex poisoned");
            match map.get(key).and_then(Weak::upgrade) {
                Some(existing) => existing,
                None => {
                    let fresh = Arc::new(AsyncMutex::new(()));
                    map.insert(key.clone(), Arc::downgrade(&fresh));
                    fresh
                }
            }
        };
        lock.lock_owned().await
    }
}
```

- [ ] **Step 2: Write the failing unit tests**

Append to `crates/datamancer/src/fetch_locks.rs`:

```rust
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
        // A second acquire for the same key must not be ready while held.
        assert!(
            locks.acquire(&k).now_or_never().is_none(),
            "same-key acquire must wait while the slot is held"
        );

        drop(first);
        // Once released, it acquires.
        assert!(
            locks.acquire(&k).now_or_never().is_some(),
            "slot must be acquirable after the holder drops"
        );
    }

    #[tokio::test]
    async fn distinct_keys_do_not_contend() {
        let locks = FetchLocks::default();
        let a = locks.acquire(&key(0, 1000)).await;
        // A different key acquires immediately, even while `a` is held.
        assert!(
            locks.acquire(&key(1000, 2000)).now_or_never().is_some(),
            "distinct keys must not block each other"
        );
        drop(a);
    }
}
```

- [ ] **Step 3: Run the tests to verify they pass (module + tests written together compile-first)**

Run: `cargo test -p datamancer --lib fetch_locks`
Expected: PASS — `same_key_serializes` and `distinct_keys_do_not_contend` both green. If `now_or_never` is unresolved, confirm `futures` is a dev-dependency of `datamancer` (it is — used in integration tests; for `--lib` it must be under `[dev-dependencies]`, verify with `cargo tree -p datamancer -e dev | grep futures`).

- [ ] **Step 4: Lint the new module**

Run: `cargo clippy -p datamancer --all-targets -- -D warnings`
Expected: no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/datamancer/src/fetch_locks.rs crates/datamancer/src/session.rs
git commit -m "feat(datamancer): add per-CacheKey FetchLocks single-flight registry"
```

---

## Task 2: Wire single-flight into the read-through path

**Files:**
- Modify: `crates/datamancer/src/session.rs` (`DatamancerInner`, `DatamancerBuilder::build`, `Controller`, `run_historical_cached`)
- Test: `crates/datamancer/tests/historical_cache.rs`

- [ ] **Step 1: Write the failing concurrency test**

Add a gated provider and the headline test to `crates/datamancer/tests/historical_cache.rs`. Put the provider near `RecordingProvider` and the test in the tests section. (Imports needed at top of file: add `use std::sync::atomic::{AtomicUsize, Ordering};` and `use tokio::sync::watch;` — `Arc`/`Mutex` and `mpsc` are already imported.)

```rust
// --- gated provider (forces genuine fetch overlap) --------------------------

/// Counts `fetch_history` calls and blocks inside each fetch until released,
/// so a test can guarantee multiple sessions are contending before the winner
/// finishes. Serves the same dataset filtered to the requested range.
struct GatedProvider {
    id: String,
    data: Vec<MarketEvent>,
    calls: Arc<AtomicUsize>,
    started: Arc<tokio::sync::Notify>,
    release: watch::Receiver<bool>,
}

#[async_trait]
impl Provider for GatedProvider {
    fn id(&self) -> &str {
        &self.id
    }
    fn supports(&self, _instrument: &Instrument, _kind: EventKind) -> bool {
        true
    }
    async fn start_live(&self, _sink: mpsc::Sender<MarketEvent>) -> Result<Box<dyn LiveHandle>> {
        Ok(Box::new(NoopLive))
    }
    async fn fetch_history(
        &self,
        request: HistoryRequest,
        sink: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.started.notify_one();
        // Block until the test releases the gate (no lost-wakeup: watch
        // retains the latest value, so a late check still observes `true`).
        let mut rx = self.release.clone();
        while !*rx.borrow() {
            rx.changed().await.ok();
        }
        for ev in &self.data {
            let ts = match ev {
                MarketEvent::Bar(b) => b.source_ts.0,
                _ => continue,
            };
            if ts < request.from.0 || ts >= request.to.0 {
                continue;
            }
            if sink.send(ev.clone()).await.is_err() {
                return Ok(());
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn concurrent_identical_requests_fetch_once() {
    const N: usize = 8;
    let data = vec![bar(100, 1.0), bar(200, 2.0), bar(300, 3.0)];
    let calls = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(tokio::sync::Notify::new());
    let (release_tx, release_rx) = watch::channel(false);

    let provider = GatedProvider {
        id: "rec".to_string(),
        data,
        calls: calls.clone(),
        started: started.clone(),
        release: release_rx,
    };
    let cache = Arc::new(SurrealCache::open(SurrealCacheConfig::Memory).await.unwrap());
    let dm = Datamancer::builder()
        .provider_arc(Arc::new(provider))
        .historical_cache_arc(cache.clone())
        .build()
        .unwrap();
    let dm = Arc::new(dm);

    // Launch N concurrent cached sessions over the identical cold range.
    let mut handles = Vec::new();
    for _ in 0..N {
        let dm = dm.clone();
        handles.push(tokio::spawn(async move {
            let session = dm
                .session(
                    inst(),
                    EventKind::Bar(BarInterval::OneMinute),
                    Scope::Historical {
                        from: Timestamp(0),
                        to: Timestamp(1000),
                    },
                    PersistenceOptions::cached(),
                )
                .await
                .unwrap();
            drain(&session).await.0
        }));
    }

    // Wait until the winner is actually inside fetch_history (so the other
    // N-1 are contending for the slot), then release the gate.
    started.notified().await;
    release_tx.send(true).unwrap();

    let mut results = Vec::new();
    for h in handles {
        results.push(h.await.unwrap());
    }

    // The single-flight guarantee: exactly one provider fetch for the key.
    assert_eq!(calls.load(Ordering::SeqCst), 1, "exactly one provider fetch");
    // Every session received the full ordered dataset (seq is per-session).
    for bars in &results {
        assert_eq!(
            bars,
            &vec![(100, 0), (200, 1), (300, 2)],
            "every consumer gets the full range"
        );
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p datamancer --test historical_cache concurrent_identical_requests_fetch_once`
Expected: FAIL — `assert_eq!(calls, 1)` fails with a count > 1 (today every session fetches independently). The test compiles because `Controller`/`DatamancerInner` are unchanged so far.

- [ ] **Step 3: Add the `fetch_locks` field to `DatamancerInner` and initialize it**

In `crates/datamancer/src/session.rs`, add the field to `struct DatamancerInner` (after `resume_buffer_events`):

```rust
    resume_buffer_events: usize,
    /// Per-`CacheKey` single-flight registry: at most one outstanding
    /// provider fetch per key (see `fetch_locks`).
    fetch_locks: FetchLocks,
```

Find where `DatamancerInner` is constructed inside `DatamancerBuilder::build` (search for `DatamancerInner {`) and add the initializer alongside the other fields:

```rust
            fetch_locks: FetchLocks::default(),
```

- [ ] **Step 4: Add the `fetch_locks` field to `Controller` and populate it**

Add the field to `struct Controller` (around `ring_capacity: usize,`):

```rust
    ring_capacity: usize,
    fetch_locks: FetchLocks,
```

Populate it where the `Controller` is constructed in `session()` (the literal with `tap_log: self.inner.tap_log.clone(),` etc.):

```rust
            ring_capacity: self.inner.resume_buffer_events,
            fetch_locks: self.inner.fetch_locks.clone(),
```

- [ ] **Step 5: Wrap the gap-fetch decision in `run_historical_cached` with single-flight + re-tile**

Replace the gap-planning block in `run_historical_cached` (the `let gaps = if options.read_cache { ... } else { whole };` through the `tile`/`stream_segments`/`finish_historical` tail) with:

```rust
        // Single-flight: an unlocked pre-check lets a fully-covered range
        // replay without ever touching the fetch slot. If there is anything
        // to fetch, acquire the per-key slot and RE-TILE against fresh
        // coverage — a concurrent winner may have just filled some or all of
        // it. We hold the slot across the fetch only when we actually fetch.
        let mut fetch_guard = None;
        let gaps = if options.read_cache {
            let initial = match cache.gaps(&plan_key).await {
                Ok(g) => g,
                Err(e) => {
                    tracing::warn!(
                        instrument = %self.inner.instrument,
                        error = %e,
                        "cache gaps() failed; treating whole range as a gap"
                    );
                    vec![GapSpan { from_source_ts: from, to_source_ts: to }]
                }
            };
            if initial.is_empty() {
                initial
            } else {
                let guard = self.fetch_locks.acquire(&plan_key).await;
                let regaps = match cache.gaps(&plan_key).await {
                    Ok(g) => g,
                    Err(e) => {
                        tracing::warn!(
                            instrument = %self.inner.instrument,
                            error = %e,
                            "cache gaps() failed after acquiring fetch slot; \
                             treating whole range as a gap"
                        );
                        vec![GapSpan { from_source_ts: from, to_source_ts: to }]
                    }
                };
                // Hold the slot across the fetch only if a gap remains.
                if !regaps.is_empty() {
                    fetch_guard = Some(guard);
                }
                regaps
            }
        } else {
            vec![GapSpan { from_source_ts: from, to_source_ts: to }]
        };
        let segments = tile(from, to, &gaps);

        let outcome = self.stream_segments(segments, options, cmd_rx, None).await;
        // Release the fetch slot (if held) before finishing, so a queued
        // waiter proceeds as soon as our store has landed.
        drop(fetch_guard);
        if outcome == SegmentOutcome::Closed {
            return;
        }

        self.finish_historical(cmd_rx).await;
```

(Remove the old `let whole = vec![...];` binding above this block — it is now inlined per branch.)

- [ ] **Step 6: Run the concurrency test to verify it passes**

Run: `cargo test -p datamancer --test historical_cache concurrent_identical_requests_fetch_once`
Expected: PASS — `calls == 1`, every consumer received `[(100,0),(200,1),(300,2)]`.

- [ ] **Step 7: Run the full cache test file to verify no regression**

Run: `cargo test -p datamancer --test historical_cache`
Expected: PASS — including the existing `cold_fetch_populates_cache_and_streams_in_order`, `fully_cached_serves_without_touching_provider`, and `partial_overlap_fetches_only_the_gaps_and_merges_in_order`.

- [ ] **Step 8: Lint**

Run: `cargo clippy -p datamancer --all-targets -- -D warnings`
Expected: no warnings. (Watch for `clippy::single_match_else` or `clippy::option_if_let_else` from pedantic — if flagged on the new code, prefer the explicit `match` shown above and add a targeted `#[allow(...)]` with a one-line reason only if pedantic forces an awkward rewrite.)

- [ ] **Step 9: Commit**

```bash
git add crates/datamancer/src/session.rs crates/datamancer/tests/historical_cache.rs
git commit -m "feat(datamancer): single-flight read-through fetch per CacheKey"
```

---

## Task 3: Resilience tests — failure release and no false serialization

These assert two spec properties. They are expected to pass against the Task 2 implementation (the guard releases on every exit path, including the failure path, because it drops when `fetch_guard` goes out of scope). If either fails, the implementation — not the test — is wrong; fix Task 2.

**Files:**
- Test: `crates/datamancer/tests/historical_cache.rs`

- [ ] **Step 1: Write the winner-failure-releases-the-slot test**

A winner whose fetch fails mid-way must release the slot so a later session re-tiles to the still-uncovered remainder and fetches it. (Sequential A-then-B proves release-on-failure without timing.) Add:

```rust
#[tokio::test]
async fn failed_fetch_releases_slot_for_next_session() {
    let cache = Arc::new(SurrealCache::open(SurrealCacheConfig::Memory).await.unwrap());

    // Session A: provider fails at ts >= 200, so only [.., 200) of the data
    // is delivered/stored; the remainder is reported as a Gap.
    let data = vec![bar(100, 1.0), bar(200, 2.0), bar(300, 3.0)];
    let (failing, _f1) = RecordingProvider::new("rec", data.clone());
    let failing = failing.with_fail_at(200);
    let dm_a = Datamancer::builder()
        .provider_arc(Arc::new(failing))
        .historical_cache_arc(cache.clone())
        .build()
        .unwrap();
    let session_a = dm_a
        .session(
            inst(),
            EventKind::Bar(BarInterval::OneMinute),
            Scope::Historical { from: Timestamp(0), to: Timestamp(1000) },
            PersistenceOptions::cached(),
        )
        .await
        .unwrap();
    let (bars_a, gaps_a) = drain(&session_a).await;
    assert_eq!(bars_a.iter().map(|b| b.0).collect::<Vec<_>>(), vec![100]);
    assert!(!gaps_a.is_empty(), "A reports the unfetched remainder as a gap");

    // Session B: a healthy provider on the SAME cache. The slot must have
    // been released by A's failure, and B must fetch the still-uncovered
    // remainder and deliver the full range.
    let (healthy, f2) = RecordingProvider::new("rec", data);
    let dm_b = Datamancer::builder()
        .provider_arc(Arc::new(healthy))
        .historical_cache_arc(cache.clone())
        .build()
        .unwrap();
    let session_b = dm_b
        .session(
            inst(),
            EventKind::Bar(BarInterval::OneMinute),
            Scope::Historical { from: Timestamp(0), to: Timestamp(1000) },
            PersistenceOptions::cached(),
        )
        .await
        .unwrap();
    let (bars_b, _gaps_b) = drain(&session_b).await;
    assert_eq!(
        bars_b.iter().map(|b| b.0).collect::<Vec<_>>(),
        vec![100, 200, 300],
        "B sees the full range after re-tiling the remainder"
    );
    // B only fetched the still-uncovered part, not the already-cached prefix.
    assert!(
        !f2.lock().unwrap().is_empty(),
        "B fetched the remainder rather than serving a permanently-masked gap"
    );
}
```

Note: each `Datamancer` instance has its own `FetchLocks`, so A and B here exercise release across the *shared cache*, not the shared registry. The registry's same-instance release is covered by the concurrency test in Task 2 (the N-1 waiters proceed only because the winner's guard dropped) and the `same_key_serializes` unit test in Task 1.

- [ ] **Step 2: Write the no-false-serialization test**

Distinct keys must fetch concurrently — the lock is per key, not global. Two different ranges (distinct `CacheKey`s) both fetch:

```rust
#[tokio::test]
async fn distinct_ranges_each_fetch() {
    let data = vec![bar(100, 1.0), bar(1100, 2.0)];
    let (provider, fetched) = RecordingProvider::new("rec", data);
    let cache = Arc::new(SurrealCache::open(SurrealCacheConfig::Memory).await.unwrap());
    let dm = Datamancer::builder()
        .provider_arc(Arc::new(provider))
        .historical_cache_arc(cache.clone())
        .build()
        .unwrap();

    for (from, to) in [(0_i64, 1000_i64), (1000, 2000)] {
        let session = dm
            .session(
                inst(),
                EventKind::Bar(BarInterval::OneMinute),
                Scope::Historical { from: Timestamp(from), to: Timestamp(to) },
                PersistenceOptions::cached(),
            )
            .await
            .unwrap();
        let _ = drain(&session).await;
    }

    let fetched = fetched.lock().unwrap().clone();
    assert!(
        fetched.contains(&(0, 1000)) && fetched.contains(&(1000, 2000)),
        "distinct cache keys each fetch their own range: {fetched:?}"
    );
}
```

- [ ] **Step 3: Run the new tests**

Run: `cargo test -p datamancer --test historical_cache failed_fetch_releases_slot_for_next_session distinct_ranges_each_fetch`
Expected: PASS both.

- [ ] **Step 4: Lint**

Run: `cargo clippy -p datamancer --all-targets -- -D warnings`
Expected: no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/datamancer/tests/historical_cache.rs
git commit -m "test(datamancer): single-flight failure-release and distinct-key concurrency"
```

---

## Task 4: Docs and full verification

**Files:**
- Modify: `crates/datamancer/README.md`

- [ ] **Step 1: Document the single-flight behavior**

In `crates/datamancer/README.md`, in the "Persistence — Historical Cache" section, after the "How read-through works" subsection, add:

```markdown
### Single-flight fetch

Within one `Datamancer` process, at most one provider fetch is outstanding per
`CacheKey`. Concurrent `cached()` sessions requesting the same uncovered range
do not each hit the provider: the first to need a fetch takes a per-key slot
and fetches; the rest wait, then re-evaluate coverage and serve from cache what
the winner just stored (re-fetching only any still-uncovered remainder). A
cold-cache parameter sweep that opens hundreds of sessions over the same window
therefore fetches it once. This is in-process only; coordinating fetches across
processes is out of scope (see the consumer-transport design).
```

- [ ] **Step 2: Run the whole workspace test suite**

Run: `cargo test`
Expected: PASS — all unit + integration tests (the `#[ignore]`d `alpaca_real` is skipped).

- [ ] **Step 3: Full clippy and fmt**

Run: `cargo clippy --all-targets -- -D warnings && cargo fmt --check`
Expected: no warnings; formatting clean. (If `cargo fmt --check` reports diffs, run `cargo fmt` and re-stage.)

- [ ] **Step 4: Commit**

```bash
git add crates/datamancer/README.md
git commit -m "docs(datamancer): document single-flight read-through fetch"
```

---

## Self-Review Notes (for the implementer)

- **Spec coverage.** Headline "one fetch per key" → Task 2. Re-tile-after-wait → Task 2 Step 5. Partial-fetch/winner-failure recovery → Task 3 Step 1. No false serialization (per-key, not global) → Task 3 Step 2. Late-waiter / no-lost-wakeup → `watch` gate (test) + `tokio::sync::Mutex` fairness (impl); the `same_key_serializes` unit test pins the wait/release contract. Cancellation safety → guard is an `OwnedMutexGuard` that releases on drop; documented, exercised implicitly when spawned session tasks complete.
- **Scope.** `run_backfill` deliberately untouched — the sweep uses `Scope::Historical`, not stitched-live. A follow-up could factor the acquire+re-tile into a shared helper and wire it there.
- **Stats observability** (seeing how much is cached) is a separate sibling spec, not in this plan.
- **Type consistency.** `FetchLocks` (clone-shared), `acquire(&CacheKey) -> OwnedMutexGuard<()>`, field name `fetch_locks` on both `DatamancerInner` and `Controller`. `GapSpan { from_source_ts, to_source_ts }` matches existing usage in `run_historical_cached`.
