# Historical Read-Through Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a historical `Session` serve cached ranges from a `HistoricalCache`, fetch only the uncovered gaps from the provider, splice the two into one ordered stream, and record coverage honestly — controlled by a `PersistenceOptions` block.

**Architecture:** `gaps()` tiles the requested range into ordered, disjoint Covered/Gap segments. Covered segments replay from the cache; gap segments fetch from the provider, forward to the consumer, and (when `write_cache`) store back. Emitting segments left-to-right keeps the merged stream `source_ts`-ordered by construction, so `seq` (assigned by the existing `forward()`) rises with `source_ts`. A `persist: bool` argument is replaced by `PersistenceOptions { read_cache, write_cache }`.

**Tech Stack:** Rust 2024, tokio, `async-trait`, `futures` streams, SurrealDB (embedded/mem), the existing `datamancer` orchestrator + `datamancer-core` trait surface.

**Design doc:** `docs/superpowers/specs/2026-05-29-historical-read-through-cache-design.md`

**Conventions for every task:**
- Run `cargo test -p datamancer` (default features include `storage-surreal`) unless a narrower command is given.
- After the implementation steps of each task, before committing, run `cargo fmt` and `cargo clippy --all-targets -- -D warnings` and fix any findings.
- Commit messages use Conventional Commits.

---

## Task 1: `PersistenceOptions` type

**Files:**
- Modify: `crates/datamancer/src/session.rs` (add the type near the `Scope` enum, ~line 77)
- Modify: `crates/datamancer/src/lib.rs:37` (re-export)

- [ ] **Step 1: Write the failing test**

Add to the bottom of `crates/datamancer/src/session.rs` (create a `#[cfg(test)] mod tests` block if none exists):

```rust
#[cfg(test)]
mod persistence_options_tests {
    use super::PersistenceOptions;

    #[test]
    fn presets_compose_the_four_modes() {
        assert_eq!(PersistenceOptions::none(), PersistenceOptions::default());
        assert!(!PersistenceOptions::none().read_cache && !PersistenceOptions::none().write_cache);
        assert!(PersistenceOptions::cached().read_cache && PersistenceOptions::cached().write_cache);
        assert!(PersistenceOptions::read_only().read_cache && !PersistenceOptions::read_only().write_cache);
        assert!(!PersistenceOptions::refresh().read_cache && PersistenceOptions::refresh().write_cache);
    }

    #[test]
    fn uses_cache_is_true_when_any_axis_set() {
        assert!(!PersistenceOptions::none().uses_cache());
        assert!(PersistenceOptions::cached().uses_cache());
        assert!(PersistenceOptions::read_only().uses_cache());
        assert!(PersistenceOptions::refresh().uses_cache());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p datamancer persistence_options_tests`
Expected: FAIL — `cannot find type PersistenceOptions` / does not compile.

- [ ] **Step 3: Add the type**

In `crates/datamancer/src/session.rs`, immediately after the `Scope` enum (after its closing `}` around line 77), add:

```rust
/// How a session interacts with the configured persistence layer.
///
/// The two cache axes compose into the full historical option space:
///
/// | `read_cache` | `write_cache` | mode      | behavior                                    |
/// |--------------|---------------|-----------|---------------------------------------------|
/// | `false`      | `false`       | ephemeral | always hit the provider, store nothing      |
/// | `true`       | `true`        | cached    | serve covered ranges, fetch & store gaps    |
/// | `true`       | `false`       | read-only | serve cache + fetch gaps, don't persist     |
/// | `false`      | `true`        | refresh   | ignore coverage, re-fetch range, overwrite  |
///
/// `#[non_exhaustive]`: later work (tap log, resume) adds axes additively.
/// Construct via the presets, or mutate the public fields on an owned value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct PersistenceOptions {
    /// Historical scope: serve covered subranges from the cache and fetch only
    /// the gaps. When false, always fetch the full range from the provider.
    pub read_cache: bool,
    /// Historical scope: write fetched gap data back to the cache.
    pub write_cache: bool,
}

impl PersistenceOptions {
    /// No persistence: always hit the provider, store nothing.
    #[must_use]
    pub const fn none() -> Self {
        Self { read_cache: false, write_cache: false }
    }

    /// Read-through cache: serve covered ranges, fetch and store only gaps.
    #[must_use]
    pub const fn cached() -> Self {
        Self { read_cache: true, write_cache: true }
    }

    /// Serve from cache and fetch gaps for this run, but do not persist them.
    #[must_use]
    pub const fn read_only() -> Self {
        Self { read_cache: true, write_cache: false }
    }

    /// Ignore cached coverage, re-fetch the whole range, overwrite the cache.
    #[must_use]
    pub const fn refresh() -> Self {
        Self { read_cache: false, write_cache: true }
    }

    /// True if either axis touches the historical cache.
    #[must_use]
    pub const fn uses_cache(self) -> bool {
        self.read_cache || self.write_cache
    }
}
```

- [ ] **Step 4: Re-export from the crate root**

In `crates/datamancer/src/lib.rs`, change the `pub use session::{...}` line (line 37) to add `PersistenceOptions`:

```rust
pub use session::{
    Datamancer, DatamancerBuilder, EventStream, PersistenceOptions, ReconnectPolicy, Scope, Session,
};
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cargo test -p datamancer persistence_options_tests`
Expected: PASS (2 tests).

- [ ] **Step 6: fmt + clippy + commit**

```bash
cargo fmt
cargo clippy --all-targets -- -D warnings
git add crates/datamancer/src/session.rs crates/datamancer/src/lib.rs
git commit -m "feat(datamancer): add PersistenceOptions control block"
```

---

## Task 2: Migrate `session()` / `Session` / controller from `persist: bool`

This is a refactor: behavior is unchanged (read-through isn't wired until Task 5). All existing tests must stay green.

**Files:**
- Modify: `crates/datamancer/src/session.rs` (signature, `SessionInner`, builder check, controller command, `Session` methods)
- Modify: `crates/datamancer/tests/session_integration.rs` (call-site migration)
- Modify: `crates/datamancer/tests/alpaca_real.rs` (call-site migration)
- Modify: `crates/datamancer/examples/crypto_ticker.rs:81` (call-site migration)

- [ ] **Step 1: Change `Datamancer::session` signature and the PersistenceRequired check**

In `crates/datamancer/src/session.rs`, change the `session` method signature (line ~131) from `persist: bool` to `options: PersistenceOptions`, and replace the guard at the top (lines 138-140):

```rust
    pub async fn session(
        &self,
        instrument: Instrument,
        kind: EventKind,
        scope: Scope,
        options: PersistenceOptions,
    ) -> Result<Session> {
        if options.uses_cache() && self.inner.historical_cache.is_none() {
            return Err(Error::PersistenceRequired);
        }
```

(The tap-log axis is Spec B; only the cache is consulted here.)

- [ ] **Step 2: Store `PersistenceOptions` on `SessionInner` instead of `AtomicBool`**

Change the `persisting` field on `SessionInner` (line 375) from:

```rust
    persisting: std::sync::atomic::AtomicBool,
```

to:

```rust
    persistence: std::sync::Mutex<PersistenceOptions>,
```

Update its initializer in `session()` (line 182) from `persisting: std::sync::atomic::AtomicBool::new(persist),` to:

```rust
            persistence: std::sync::Mutex::new(options),
```

- [ ] **Step 3: Update the `Session` accessor/mutator methods**

Replace `Session::set_persisting` (lines 434-442) and `Session::is_persisting` (lines 474-479) with:

```rust
    /// Replace the persistence options at runtime. Affects future writes;
    /// an in-flight historical fetch keeps the plan it started with.
    ///
    /// # Errors
    ///
    /// Returns `Error::PersistenceRequired` if the new options require a cache
    /// that is not configured; `Error::SessionClosed` if the controller has
    /// shut down.
    pub async fn set_persistence(&self, options: PersistenceOptions) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SessionCommand::SetPersistence(options, tx))
            .await
            .map_err(|_| Error::SessionClosed)?;
        rx.await.map_err(|_| Error::SessionClosed)?
    }

    #[must_use]
    pub fn persistence(&self) -> PersistenceOptions {
        *self
            .inner
            .persistence
            .lock()
            .expect("persistence mutex poisoned")
    }
```

- [ ] **Step 4: Update the command enum and controller command handling**

Change the `SessionCommand` variant (line 501) from `SetPersisting(bool, oneshot::Sender<Result<()>>)` to:

```rust
    SetPersistence(PersistenceOptions, oneshot::Sender<Result<()>>),
```

Replace `Controller::handle_command`'s `SetPersisting` arm (lines 711-715) and `Controller::apply_persisting` (lines 725-733):

```rust
            Some(SessionCommand::SetPersistence(options, ack)) => {
                let res = self.apply_persistence(options);
                let _ = ack.send(res);
                true
            }
```

```rust
    fn apply_persistence(&self, options: PersistenceOptions) -> Result<()> {
        if options.uses_cache() && self.historical_cache.is_none() {
            return Err(Error::PersistenceRequired);
        }
        *self
            .inner
            .persistence
            .lock()
            .expect("persistence mutex poisoned") = options;
        Ok(())
    }
```

- [ ] **Step 5: Import `PersistenceOptions` into scope where the controller needs it**

`PersistenceOptions` is defined in this same module, so no `use` is needed inside `session.rs`. Confirm the file compiles after the edits below; the controller already has `historical_cache: Option<Arc<dyn HistoricalCache>>` (line 517).

Remove the now-stale `TODO(persistence)` comment in `forward()` is **not** part of this task (live write-through is Spec B); leave `forward()` untouched.

- [ ] **Step 6: Migrate call sites in tests and the example**

In `crates/datamancer/tests/session_integration.rs`, for every `.session(...)` call the final argument is currently `false` or `true`. Replace `false` → `PersistenceOptions::none()` and `true` → `PersistenceOptions::cached()`. Add `PersistenceOptions` to the `use datamancer::{...}` import list at the top of the file.

The persistence-required test (`persist_true_without_persistence_layer_errors`, line ~318) passes `true` → `PersistenceOptions::cached()`; it still expects `Error::PersistenceRequired` because no cache is configured. Keep its assertion.

In `crates/datamancer/tests/alpaca_real.rs`, do the same mechanical replacement at both `.session(...)` call sites (lines 35, 85) and add the import.

In `crates/datamancer/examples/crypto_ticker.rs`, change the `false` at line 81 to `PersistenceOptions::none()` and add `PersistenceOptions` to the `use datamancer::{...}` list (line 25).

- [ ] **Step 7: Run the full suite to verify the refactor is green**

Run: `cargo test -p datamancer`
Expected: PASS (all existing tests, including `session_integration` and `surreal_cache`). The `alpaca_real` test stays `#[ignore]`d.

Also confirm the example builds:
Run: `cargo build --example crypto_ticker`
Expected: builds clean.

- [ ] **Step 8: fmt + clippy + commit**

```bash
cargo fmt
cargo clippy --all-targets -- -D warnings
git add crates/datamancer/src/session.rs crates/datamancer/tests/session_integration.rs crates/datamancer/tests/alpaca_real.rs crates/datamancer/examples/crypto_ticker.rs
git commit -m "refactor(datamancer): replace persist bool with PersistenceOptions at call boundary"
```

---

## Task 3: `tile()` segment helper

A pure function that partitions `[from, to)` into ordered, disjoint `Covered`/`Gap` segments given the cache's reported gaps.

**Files:**
- Modify: `crates/datamancer/src/session.rs` (add `Segment` + `tile` near the other free functions, e.g. after `source_ts`, ~line 792)

- [ ] **Step 1: Write the failing tests**

Add a test module at the bottom of `crates/datamancer/src/session.rs`:

```rust
#[cfg(test)]
mod tile_tests {
    use super::{Segment, tile};
    use datamancer_core::{GapSpan, Timestamp};

    fn gap(a: i64, b: i64) -> GapSpan {
        GapSpan { from_source_ts: Timestamp(a), to_source_ts: Timestamp(b) }
    }

    fn segs(v: &[Segment]) -> Vec<(char, i64, i64)> {
        v.iter()
            .map(|s| match *s {
                Segment::Covered { from, to } => ('C', from.0, to.0),
                Segment::Gap { from, to } => ('G', from.0, to.0),
            })
            .collect()
    }

    #[test]
    fn no_gaps_is_one_covered_segment() {
        let t = tile(Timestamp(0), Timestamp(100), &[]);
        assert_eq!(segs(&t), vec![('C', 0, 100)]);
    }

    #[test]
    fn whole_range_gap_is_one_gap_segment() {
        let t = tile(Timestamp(0), Timestamp(100), &[gap(0, 100)]);
        assert_eq!(segs(&t), vec![('G', 0, 100)]);
    }

    #[test]
    fn leading_trailing_and_middle_gaps_interleave() {
        // covered [10,20) and [40,50); gaps [0,10),[20,40),[50,60)
        let t = tile(Timestamp(0), Timestamp(60), &[gap(0, 10), gap(20, 40), gap(50, 60)]);
        assert_eq!(
            segs(&t),
            vec![('G', 0, 10), ('C', 10, 20), ('G', 20, 40), ('C', 40, 50), ('G', 50, 60)]
        );
    }

    #[test]
    fn gap_flush_with_start_emits_no_empty_covered() {
        // gap begins exactly at `from`: no zero-width Covered prefix.
        let t = tile(Timestamp(0), Timestamp(30), &[gap(0, 10)]);
        assert_eq!(segs(&t), vec![('G', 0, 10), ('C', 10, 30)]);
    }

    #[test]
    fn gap_flush_with_end_emits_no_empty_covered() {
        // gap ends exactly at `to`: no zero-width Covered suffix.
        let t = tile(Timestamp(0), Timestamp(30), &[gap(20, 30)]);
        assert_eq!(segs(&t), vec![('C', 0, 20), ('G', 20, 30)]);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p datamancer tile_tests`
Expected: FAIL — `cannot find type Segment` / `cannot find function tile`.

- [ ] **Step 3: Implement `Segment` + `tile`**

Add to `crates/datamancer/src/session.rs` (near the other free functions, after `fn source_ts`):

```rust
/// One slice of a requested historical range: either already in the cache
/// (`Covered`) or not yet fetched (`Gap`). Half-open `[from, to)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Segment {
    Covered { from: Timestamp, to: Timestamp },
    Gap { from: Timestamp, to: Timestamp },
}

/// Partition `[from, to)` into ordered, disjoint segments. `gaps` are the
/// uncovered subranges (ordered, disjoint, within `[from, to)`) as reported by
/// [`HistoricalCache::gaps`]; everything between them is covered. Zero-width
/// pieces are never emitted.
fn tile(from: Timestamp, to: Timestamp, gaps: &[datamancer_core::GapSpan]) -> Vec<Segment> {
    let mut out = Vec::with_capacity(gaps.len() * 2 + 1);
    let mut cursor = from;
    for g in gaps {
        let g_from = g.from_source_ts;
        let g_to = g.to_source_ts;
        if g_from > cursor {
            out.push(Segment::Covered { from: cursor, to: g_from });
        }
        if g_to > g_from {
            out.push(Segment::Gap { from: g_from, to: g_to });
        }
        cursor = cursor.max(g_to);
    }
    if cursor < to {
        out.push(Segment::Covered { from: cursor, to });
    }
    out
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p datamancer tile_tests`
Expected: PASS (5 tests).

- [ ] **Step 5: fmt + clippy + commit**

```bash
cargo fmt
cargo clippy --all-targets -- -D warnings
git add crates/datamancer/src/session.rs
git commit -m "feat(datamancer): add tile() to partition a range into covered/gap segments"
```

---

## Task 4: Tighten `SurrealCache::store` coverage claim

Coverage must reflect exactly the `CacheKey` range the caller asserts was fetched, not widen to the event span. This is what lets the controller claim honest partial coverage on a failed fetch.

**Files:**
- Modify: `crates/datamancer/src/storage/surreal.rs` (the `store` coverage computation, lines 396-402)
- Modify: `crates/datamancer/tests/surreal_cache.rs` (add an exact-claim test)

- [ ] **Step 1: Write the failing test**

Add to `crates/datamancer/tests/surreal_cache.rs`:

```rust
#[tokio::test]
async fn store_claims_exactly_the_key_range_not_the_event_span() {
    let cache = SurrealCache::open(SurrealCacheConfig::Memory)
        .await
        .unwrap();
    // Key range is [100, 200) but the events sit at 100 and 250 — outside the
    // key's upper bound. Coverage must NOT extend to 250.
    let k = key(EventKind::Trade, 100, 200);
    cache
        .store(&k, &[trade("AAPL", 100, 1.0, 1), trade("AAPL", 250, 2.0, 1)])
        .await
        .unwrap();

    // A probe of [200, 300) must report a gap (the 250 event did not extend
    // coverage past 200).
    let probe = key(EventKind::Trade, 200, 300);
    let gaps = cache.gaps(&probe).await.unwrap();
    assert_eq!(
        gaps,
        vec![GapSpan {
            from_source_ts: Timestamp(200),
            to_source_ts: Timestamp(300),
        }]
    );
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p datamancer --test surreal_cache store_claims_exactly_the_key_range_not_the_event_span`
Expected: FAIL — current code extends `to` to `max_ts + 1` (251), so the probe reports no gap.

- [ ] **Step 3: Tighten the coverage claim**

In `crates/datamancer/src/storage/surreal.rs`, replace the coverage computation in `store` (lines 396-402) — the block that currently reads:

```rust
        // Coverage segment: prefer the request's [from, to] when it fully
        // contains the events; otherwise use the actual event-ts span.
        let from = key.from.0.min(min_ts);
        // The coverage should bound [from, to) — events at exactly `to` would
        // typically not be returned by a `from..to` half-open scan.
        let to = key.to.0.max(max_ts.saturating_add(1));
        self.update_coverage(key, from, to, stored).await?;
        Ok(())
```

with:

```rust
        // Coverage reflects exactly the range the caller asserts was fetched
        // (the CacheKey), NOT the span of whatever events happened to arrive.
        // Callers (e.g. the read-through fetch loop) pass a key range that
        // reflects only what was actually, successfully fetched, so an
        // interrupted fetch leaves the unfetched remainder reported as a gap.
        // `min_ts`/`max_ts` are no longer used for the claim.
        let _ = (min_ts, max_ts);
        self.update_coverage(key, key.from.0, key.to.0, stored).await?;
        Ok(())
```

(If clippy flags the now-unused `min_ts`/`max_ts` accumulators, remove their `let mut`/update lines entirely instead of the `let _ =` discard — whichever keeps `-D warnings` clean. The `let _ =` form is the minimal diff; prefer removing the dead accumulator code if the loop no longer needs them.)

- [ ] **Step 4: Run the targeted test and the full surreal suite**

Run: `cargo test -p datamancer --test surreal_cache`
Expected: PASS — the new test passes and the existing `store_then_replay_round_trip_preserves_order_and_values`, `gaps_*`, and `fully_covered_*` tests still pass (their store calls already bound their events within the key range; the `coverage.to.0 >= 400` assertion holds because the key's `to` is 400).

- [ ] **Step 5: fmt + clippy + commit**

```bash
cargo fmt
cargo clippy --all-targets -- -D warnings
git add crates/datamancer/src/storage/surreal.rs crates/datamancer/tests/surreal_cache.rs
git commit -m "fix(datamancer): SurrealCache::store claims exactly the key range"
```

---

## Task 5: Read-through fetch loop (`run_historical_cached`) + cold-fetch test

This task adds the read-through path and proves it with a cold fetch (empty cache). It first extracts two reusable helpers, then branches `run_historical`.

**Files:**
- Modify: `crates/datamancer/src/session.rs` (imports; extract `finish_historical` + `emit_gap`; route `run_live` backfill gap through `emit_gap`; branch `run_historical`; add `run_historical_cached`)
- Create: `crates/datamancer/tests/historical_cache.rs` (test scaffolding + cold-fetch test)

- [ ] **Step 1: Write the failing integration test (scaffolding + cold fetch)**

Create `crates/datamancer/tests/historical_cache.rs`:

```rust
//! Integration tests for the read-through historical cache path.
//!
//! Uses an in-memory SurrealCache and a synthetic provider that records the
//! ranges it was asked to fetch (and can be told to fail mid-fetch), so the
//! tests assert exactly which gaps hit the provider.

#![cfg(feature = "storage-surreal")]

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datamancer::storage::{SurrealCache, SurrealCacheConfig};
use datamancer::{
    AssetClass, Bar, BarInterval, CacheKey, ControlKind, Datamancer, EventKind, Instrument,
    LiveHandle, MarketEvent, PersistenceOptions, Price, Provider, ProviderId, Result, Scope, Seq,
    Timestamp,
};
use datamancer_core::HistoryRequest;
use futures::StreamExt;
use tokio::sync::mpsc;

// --- synthetic provider -----------------------------------------------------

/// Serves a fixed, source_ts-sorted dataset for whatever sub-range is
/// requested, recording each requested `[from, to)`. With `fail_at = Some(ts)`
/// it returns an error upon reaching the first event whose `source_ts >= ts`
/// (that event and everything after it is NOT sent).
struct RecordingProvider {
    id: String,
    data: Vec<MarketEvent>,
    fetched: Arc<Mutex<Vec<(i64, i64)>>>,
    fail_at: Option<i64>,
}

impl RecordingProvider {
    fn new(id: &str, data: Vec<MarketEvent>) -> (Self, Arc<Mutex<Vec<(i64, i64)>>>) {
        let fetched = Arc::new(Mutex::new(Vec::new()));
        (
            Self { id: id.to_string(), data, fetched: fetched.clone(), fail_at: None },
            fetched,
        )
    }

    fn with_fail_at(mut self, ts: i64) -> Self {
        self.fail_at = Some(ts);
        self
    }
}

#[async_trait]
impl Provider for RecordingProvider {
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
        self.fetched
            .lock()
            .unwrap()
            .push((request.from.0, request.to.0));
        for ev in &self.data {
            let ts = match ev {
                MarketEvent::Bar(b) => b.source_ts.0,
                MarketEvent::Trade(t) => t.source_ts.0,
                _ => continue,
            };
            if ts < request.from.0 || ts >= request.to.0 {
                continue;
            }
            if let Some(fail) = self.fail_at
                && ts >= fail
            {
                return Err(datamancer::Error::Provider {
                    provider: self.id.clone(),
                    message: "synthetic mid-fetch failure".to_string(),
                });
            }
            if sink.send(ev.clone()).await.is_err() {
                return Ok(());
            }
        }
        Ok(())
    }
}

struct NoopLive;
#[async_trait]
impl LiveHandle for NoopLive {
    async fn subscribe(&self, _i: Instrument, _k: EventKind) -> Result<()> {
        Ok(())
    }
    async fn unsubscribe(&self, _i: Instrument, _k: EventKind) -> Result<()> {
        Ok(())
    }
    async fn close(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

// --- helpers ----------------------------------------------------------------

fn inst() -> Instrument {
    Instrument::new(ProviderId::from_static("rec"), AssetClass::Equity, "AAPL")
}

fn bar(ts: i64, close: f64) -> MarketEvent {
    MarketEvent::Bar(Bar {
        instrument: inst(),
        interval: BarInterval::OneMinute,
        source_ts: Timestamp(ts),
        rx_ts: Timestamp(ts),
        seq: Seq(0),
        open: Price::from_f64_round(close),
        high: Price::from_f64_round(close),
        low: Price::from_f64_round(close),
        close: Price::from_f64_round(close),
        volume: 1,
    })
}

fn key(from: i64, to: i64) -> CacheKey {
    CacheKey {
        instrument: inst(),
        kind: EventKind::Bar(BarInterval::OneMinute),
        from: Timestamp(from),
        to: Timestamp(to),
    }
}

/// Drain a historical session to completion, returning bar source_ts/seq pairs
/// (in arrival order) and any Gap control spans seen.
async fn drain(session: &mut datamancer::Session) -> (Vec<(i64, u64)>, Vec<(i64, i64)>) {
    let mut stream = session.take_events().unwrap();
    let mut bars = Vec::new();
    let mut gaps = Vec::new();
    while let Some(ev) = stream.next().await {
        match ev {
            MarketEvent::Bar(b) => bars.push((b.source_ts.0, b.seq.0)),
            MarketEvent::Control(c) => match c.kind {
                ControlKind::Gap { span, .. } => {
                    gaps.push((span.from_source_ts.0, span.to_source_ts.0));
                }
                ControlKind::SessionClosing => break,
                _ => {}
            },
            _ => {}
        }
    }
    (bars, gaps)
}

// --- tests ------------------------------------------------------------------

#[tokio::test]
async fn cold_fetch_populates_cache_and_streams_in_order() {
    let data = vec![bar(100, 1.0), bar(200, 2.0), bar(300, 3.0)];
    let (provider, fetched) = RecordingProvider::new("rec", data);
    let cache = Arc::new(SurrealCache::open(SurrealCacheConfig::Memory).await.unwrap());
    let dm = Datamancer::builder()
        .provider_arc(Arc::new(provider))
        .historical_cache_arc(cache.clone())
        .build()
        .unwrap();

    let mut session = dm
        .session(
            inst(),
            EventKind::Bar(BarInterval::OneMinute),
            Scope::Historical { from: Timestamp(0), to: Timestamp(1000) },
            PersistenceOptions::cached(),
        )
        .await
        .unwrap();

    let (bars, gaps) = drain(&mut session).await;
    assert_eq!(bars, vec![(100, 0), (200, 1), (300, 2)], "ordered, monotonic seq");
    assert!(gaps.is_empty());
    // Whole range was one gap → provider asked exactly once for [0,1000).
    assert_eq!(*fetched.lock().unwrap(), vec![(0, 1000)]);
    // Coverage now recorded.
    assert!(cache.lookup(&key(0, 1000)).await.unwrap().is_some());
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p datamancer --test historical_cache cold_fetch_populates_cache_and_streams_in_order`
Expected: FAIL — the read-through path doesn't exist; with `cached()` the current `run_historical` still streams from the provider but **does not store**, so `cache.lookup(...).is_some()` fails (and/or the provider is asked but nothing is cached).

- [ ] **Step 3: Add imports needed by the new code**

In `crates/datamancer/src/session.rs`, update the `use datamancer_core::{...}` block (lines 55-58) to add `CacheKey`, `GapSpan`, `HistoricalCache` (already present), `ReplayRequest`, and `ReplaySource`; and add `use futures::StreamExt;` next to the existing `use futures::stream::Stream;`:

```rust
use datamancer_core::{
    Bar, CacheKey, Control, ControlKind, Error, EventKind, GapSpan, HistoricalCache,
    HistoryRequest, Instrument, LiveHandle, MarketEvent, Provider, Quote, ReplayRequest,
    ReplaySource, Result, Seq, TapLog, Timestamp, Trade,
};
use futures::StreamExt;
use futures::stream::Stream;
```

- [ ] **Step 4: Extract `finish_historical` and `emit_gap` helpers (no behavior change)**

In `crates/datamancer/src/session.rs`, replace the post-fetch tail of `run_historical` (lines 576-606, from the `if !self.inner.stream_taken...` block through the final `self.shutdown().await;`) with a single call:

```rust
        self.finish_historical(&mut cmd_rx).await;
```

Then add these two methods to the `impl Controller` block (e.g. after `run_historical`):

```rust
    /// Shared post-fetch handshake for historical scopes. If the consumer
    /// never took the stream, shut down immediately (nobody to drain to).
    /// Otherwise emit `SessionClosing`, then wait for the stream to drain or
    /// drop and auto-close.
    async fn finish_historical(&mut self, cmd_rx: &mut mpsc::Receiver<SessionCommand>) {
        if !self
            .inner
            .stream_taken
            .load(std::sync::atomic::Ordering::Acquire)
        {
            self.shutdown().await;
            return;
        }
        let now = wall_clock_ts();
        let seq = Seq(self.next_seq);
        self.next_seq += 1;
        let _ = self
            .events_tx
            .send(MarketEvent::Control(Control {
                source_ts: now,
                rx_ts: now,
                seq,
                kind: ControlKind::SessionClosing,
            }))
            .await;
        loop {
            tokio::select! {
                cmd = cmd_rx.recv() => {
                    if !self.handle_command(cmd).await {
                        return;
                    }
                }
                () = self.events_tx.closed() => break,
            }
        }
        self.shutdown().await;
    }

    /// Forward an in-band `Gap` control covering `[from, to)` for this
    /// session's instrument. Goes through `forward()` so it gets a `seq`.
    async fn emit_gap(&mut self, from: Timestamp, to: Timestamp) {
        let now = wall_clock_ts();
        self.forward(MarketEvent::Control(Control {
            source_ts: now,
            rx_ts: now,
            seq: Seq(0),
            kind: ControlKind::Gap {
                provider: self.provider.id().to_string(),
                instrument: self.inner.instrument.clone(),
                span: GapSpan { from_source_ts: from, to_source_ts: to },
            },
        }))
        .await;
    }
```

Now route the `run_live` backfill placeholder through `emit_gap` for DRY. Replace the body of the `if let Some(from) = backfill_from {` block in `run_live` (lines 620-641) with:

```rust
        if let Some(from) = backfill_from {
            // TODO(resume-primitive): replay from persistence across the seam.
            // For now surface a placeholder Gap [from, now) so consumers can
            // see the seam; live events follow.
            let now = wall_clock_ts();
            self.emit_gap(from, now).await;
        }
```

(This preserves the existing `live_with_backfill_emits_placeholder_seam_gap` test behavior — a Gap with `from_source_ts == from`.)

- [ ] **Step 5: Branch `run_historical` onto the cached path**

At the very top of `run_historical` (right after the function opens, before building the `HistoryRequest`), add the branch:

```rust
    async fn run_historical(
        mut self,
        from: Timestamp,
        to: Timestamp,
        provider_tx: mpsc::Sender<MarketEvent>,
        mut provider_rx: mpsc::Receiver<MarketEvent>,
        mut cmd_rx: mpsc::Receiver<SessionCommand>,
    ) {
        let options = *self
            .inner
            .persistence
            .lock()
            .expect("persistence mutex poisoned");
        if options.uses_cache() && self.historical_cache.is_some() {
            // The cached read-through path runs its own per-gap channels; the
            // default single-fetch plumbing is unused here.
            drop(provider_tx);
            drop(provider_rx);
            self.run_historical_cached(from, to, options, &mut cmd_rx).await;
            return;
        }
        // ... existing single-fetch body unchanged ...
```

(Leave the rest of `run_historical` exactly as-is below this branch — it now ends with the `self.finish_historical(&mut cmd_rx).await;` call from Step 4.)

- [ ] **Step 6: Implement `run_historical_cached`**

Add this method to the `impl Controller` block:

```rust
    /// Read-through historical fetch. Tiles `[from, to)` into covered/gap
    /// segments (from `gaps()` when `read_cache`, else the whole range), then
    /// streams them in order: covered segments replay from the cache, gap
    /// segments fetch from the provider — forwarded to the consumer and, when
    /// `write_cache`, stored back. On a gap-fetch failure, only the confirmed
    /// prefix is claimed, an in-band `Gap` is emitted for the remainder, and
    /// the remaining segments are abandoned.
    async fn run_historical_cached(
        &mut self,
        from: Timestamp,
        to: Timestamp,
        options: PersistenceOptions,
        cmd_rx: &mut mpsc::Receiver<SessionCommand>,
    ) {
        let cache = self
            .historical_cache
            .clone()
            .expect("cached path requires a historical cache");
        let instrument = self.inner.instrument.clone();
        let kind = self.inner.kind;
        let plan_key = CacheKey { instrument: instrument.clone(), kind, from, to };

        let whole = vec![GapSpan { from_source_ts: from, to_source_ts: to }];
        let gaps = if options.read_cache {
            cache.gaps(&plan_key).await.unwrap_or(whole)
        } else {
            whole
        };
        let segments = tile(from, to, &gaps);

        for seg in segments {
            match seg {
                Segment::Covered { from: f, to: t } => {
                    let source = cache.as_replay_source(CacheKey {
                        instrument: instrument.clone(),
                        kind,
                        from: f,
                        to: t,
                    });
                    let req = ReplayRequest {
                        instruments: vec![instrument.clone()],
                        kinds: vec![kind],
                        from: f,
                        to: t,
                    };
                    let mut stream = match source.open(req).await {
                        Ok(s) => s,
                        Err(_) => continue,
                    };
                    loop {
                        tokio::select! {
                            cmd = cmd_rx.recv() => {
                                if !self.handle_command(cmd).await { return; }
                            }
                            ev = stream.next() => {
                                match ev {
                                    Some(ev) => self.forward(ev).await,
                                    None => break,
                                }
                            }
                        }
                    }
                }
                Segment::Gap { from: f, to: t } => {
                    let (tx, mut rx) = mpsc::channel::<MarketEvent>(default_buffer());
                    let provider = self.provider.clone();
                    let request = HistoryRequest {
                        instrument: instrument.clone(),
                        kind,
                        from: f,
                        to: t,
                    };
                    let fetch_task =
                        tokio::spawn(async move { provider.fetch_history(request, tx).await });

                    let mut batch: Vec<MarketEvent> = Vec::new();
                    loop {
                        tokio::select! {
                            cmd = cmd_rx.recv() => {
                                if !self.handle_command(cmd).await {
                                    fetch_task.abort();
                                    return;
                                }
                            }
                            ev = rx.recv() => {
                                match ev {
                                    Some(ev) => {
                                        batch.push(ev.clone());
                                        self.forward(ev).await;
                                    }
                                    None => break,
                                }
                            }
                        }
                    }

                    let succeeded = matches!(fetch_task.await, Ok(Ok(())));
                    if succeeded {
                        if options.write_cache {
                            let _ = cache
                                .store(
                                    &CacheKey { instrument: instrument.clone(), kind, from: f, to: t },
                                    &batch,
                                )
                                .await;
                        }
                    } else {
                        // Honest coverage: claim only the confirmed prefix
                        // (+1 keeps the last received event inside the
                        // half-open coverage); r.from if nothing arrived.
                        let confirmed_to = batch
                            .iter()
                            .filter_map(source_ts)
                            .max()
                            .map_or(f, |m| Timestamp(m.0 + 1));
                        if options.write_cache {
                            let _ = cache
                                .store(
                                    &CacheKey {
                                        instrument: instrument.clone(),
                                        kind,
                                        from: f,
                                        to: confirmed_to,
                                    },
                                    &batch,
                                )
                                .await;
                        }
                        self.emit_gap(confirmed_to, t).await;
                        break;
                    }
                }
            }
        }

        self.finish_historical(cmd_rx).await;
    }
```

Note `run_historical_cached` and `emit_gap`/`finish_historical` take `&mut self` (unlike `run_historical` which takes `self`). `run_historical` calls `self.run_historical_cached(..)` and then `return`s, so the move/borrow is fine (it borrows `self` mutably for the duration of the call, then returns).

- [ ] **Step 7: Run the cold-fetch test**

Run: `cargo test -p datamancer --test historical_cache cold_fetch_populates_cache_and_streams_in_order`
Expected: PASS.

- [ ] **Step 8: Run the full suite (no regressions)**

Run: `cargo test -p datamancer`
Expected: PASS — including the unchanged `session_integration` tests and `live_with_backfill_emits_placeholder_seam_gap`.

- [ ] **Step 9: fmt + clippy + commit**

```bash
cargo fmt
cargo clippy --all-targets -- -D warnings
git add crates/datamancer/src/session.rs crates/datamancer/tests/historical_cache.rs
git commit -m "feat(datamancer): read-through historical cache fetch (gap-fill + splice)"
```

---

## Task 6: Fully-cached and partial-overlap tests (the conundrum)

Prove that nothing is re-fetched and the splice is ordered.

**Files:**
- Modify: `crates/datamancer/tests/historical_cache.rs`

- [ ] **Step 1: Write the failing tests**

Append to `crates/datamancer/tests/historical_cache.rs`:

```rust
#[tokio::test]
async fn fully_cached_serves_without_touching_provider() {
    let cache = Arc::new(SurrealCache::open(SurrealCacheConfig::Memory).await.unwrap());
    // Pre-populate the whole range.
    cache
        .store(&key(0, 1000), &[bar(100, 1.0), bar(900, 2.0)])
        .await
        .unwrap();

    // Provider has no data and should never be asked.
    let (provider, fetched) = RecordingProvider::new("rec", vec![]);
    let dm = Datamancer::builder()
        .provider_arc(Arc::new(provider))
        .historical_cache_arc(cache.clone())
        .build()
        .unwrap();

    let mut session = dm
        .session(
            inst(),
            EventKind::Bar(BarInterval::OneMinute),
            Scope::Historical { from: Timestamp(0), to: Timestamp(1000) },
            PersistenceOptions::cached(),
        )
        .await
        .unwrap();

    let (bars, gaps) = drain(&mut session).await;
    assert_eq!(bars.iter().map(|b| b.0).collect::<Vec<_>>(), vec![100, 900]);
    assert!(gaps.is_empty());
    assert!(fetched.lock().unwrap().is_empty(), "provider must not be asked");
}

#[tokio::test]
async fn partial_overlap_fetches_only_the_gaps_and_merges_in_order() {
    let cache = Arc::new(SurrealCache::open(SurrealCacheConfig::Memory).await.unwrap());
    // Pre-cache the middle [300, 600).
    cache
        .store(&key(300, 600), &[bar(350, 5.0), bar(550, 6.0)])
        .await
        .unwrap();

    // Provider serves the two flanking gaps.
    let data = vec![bar(100, 1.0), bar(250, 2.0), bar(700, 7.0), bar(900, 9.0)];
    let (provider, fetched) = RecordingProvider::new("rec", data);
    let dm = Datamancer::builder()
        .provider_arc(Arc::new(provider))
        .historical_cache_arc(cache.clone())
        .build()
        .unwrap();

    let mut session = dm
        .session(
            inst(),
            EventKind::Bar(BarInterval::OneMinute),
            Scope::Historical { from: Timestamp(0), to: Timestamp(1000) },
            PersistenceOptions::cached(),
        )
        .await
        .unwrap();

    let (bars, gaps) = drain(&mut session).await;
    // Cached (350,550) spliced with fetched (100,250,700,900), ordered.
    assert_eq!(
        bars.iter().map(|b| b.0).collect::<Vec<_>>(),
        vec![100, 250, 350, 550, 700, 900]
    );
    // seq is contiguous across the covered+gap boundaries.
    assert_eq!(bars.iter().map(|b| b.1).collect::<Vec<_>>(), vec![0, 1, 2, 3, 4, 5]);
    assert!(gaps.is_empty());
    // Provider asked ONLY for the two gaps.
    assert_eq!(*fetched.lock().unwrap(), vec![(0, 300), (600, 1000)]);
    // The whole range is now covered.
    assert!(cache.gaps(&key(0, 1000)).await.unwrap().is_empty());
}
```

- [ ] **Step 2: Run the tests**

Run: `cargo test -p datamancer --test historical_cache`
Expected: PASS for both new tests plus the cold-fetch test. (These should pass against the Task 5 implementation with no source changes — they assert the gap-only fetching and ordering the loop already produces.)

- [ ] **Step 3: commit**

(`fmt`/`clippy` already clean from Task 5; re-run if the tests prompted any change.)

```bash
cargo fmt
cargo clippy --all-targets -- -D warnings
git add crates/datamancer/tests/historical_cache.rs
git commit -m "test(datamancer): fully-cached and partial-overlap read-through cases"
```

---

## Task 7: Failure semantics test (coverage truth + Gap control + re-request)

**Files:**
- Modify: `crates/datamancer/tests/historical_cache.rs`

- [ ] **Step 1: Write the failing test**

Append to `crates/datamancer/tests/historical_cache.rs`:

```rust
#[tokio::test]
async fn failed_gap_fetch_claims_only_prefix_emits_gap_and_re_request_resumes() {
    let cache = Arc::new(SurrealCache::open(SurrealCacheConfig::Memory).await.unwrap());

    // First provider: has 100,200,300,400 but fails on reaching ts >= 300.
    let data = vec![bar(100, 1.0), bar(200, 2.0), bar(300, 3.0), bar(400, 4.0)];
    let (provider, fetched1) = RecordingProvider::new("rec", data.clone());
    let provider = provider.with_fail_at(300);
    let dm = Datamancer::builder()
        .provider_arc(Arc::new(provider))
        .historical_cache_arc(cache.clone())
        .build()
        .unwrap();

    let mut session = dm
        .session(
            inst(),
            EventKind::Bar(BarInterval::OneMinute),
            Scope::Historical { from: Timestamp(0), to: Timestamp(1000) },
            PersistenceOptions::cached(),
        )
        .await
        .unwrap();

    let (bars, gaps) = drain(&mut session).await;
    // Only 100 and 200 were forwarded before the failure at 300.
    assert_eq!(bars.iter().map(|b| b.0).collect::<Vec<_>>(), vec![100, 200]);
    // A Gap was emitted for the unfetched remainder [201, 1000).
    assert_eq!(gaps, vec![(201, 1000)]);
    assert_eq!(*fetched1.lock().unwrap(), vec![(0, 1000)]);
    // Coverage claims only the confirmed prefix [0, 201).
    let remaining = cache.gaps(&key(0, 1000)).await.unwrap();
    assert_eq!(
        remaining.iter().map(|g| (g.from_source_ts.0, g.to_source_ts.0)).collect::<Vec<_>>(),
        vec![(201, 1000)]
    );
    drop(session);

    // Second run with a healthy provider: only the remaining gap is fetched,
    // and the merged stream is complete and ordered.
    let (provider2, fetched2) = RecordingProvider::new("rec", data);
    let dm2 = Datamancer::builder()
        .provider_arc(Arc::new(provider2))
        .historical_cache_arc(cache.clone())
        .build()
        .unwrap();
    let mut session2 = dm2
        .session(
            inst(),
            EventKind::Bar(BarInterval::OneMinute),
            Scope::Historical { from: Timestamp(0), to: Timestamp(1000) },
            PersistenceOptions::cached(),
        )
        .await
        .unwrap();
    let (bars2, gaps2) = drain(&mut session2).await;
    assert_eq!(bars2.iter().map(|b| b.0).collect::<Vec<_>>(), vec![100, 200, 300, 400]);
    assert!(gaps2.is_empty());
    // Provider only asked for the previously-missing span.
    assert_eq!(*fetched2.lock().unwrap(), vec![(201, 1000)]);
}
```

- [ ] **Step 2: Run the test**

Run: `cargo test -p datamancer --test historical_cache failed_gap_fetch_claims_only_prefix_emits_gap_and_re_request_resumes`
Expected: PASS against the Task 5 implementation (failure path already stores the confirmed prefix, emits the Gap, and aborts). If the assertion on `confirmed_to` (201) is off, re-check the `+1` logic in `run_historical_cached`'s failure arm and the `store` claim from Task 4.

- [ ] **Step 3: commit**

```bash
cargo fmt
cargo clippy --all-targets -- -D warnings
git add crates/datamancer/tests/historical_cache.rs
git commit -m "test(datamancer): coverage truth on failed gap fetch + re-request resume"
```

---

## Task 8: Mode-matrix tests (`read_only`, `refresh`)

**Files:**
- Modify: `crates/datamancer/tests/historical_cache.rs`

- [ ] **Step 1: Write the failing tests**

Append to `crates/datamancer/tests/historical_cache.rs`:

```rust
#[tokio::test]
async fn read_only_fetches_gaps_but_does_not_persist() {
    let cache = Arc::new(SurrealCache::open(SurrealCacheConfig::Memory).await.unwrap());
    let data = vec![bar(100, 1.0), bar(200, 2.0)];
    let (provider, fetched) = RecordingProvider::new("rec", data);
    let dm = Datamancer::builder()
        .provider_arc(Arc::new(provider))
        .historical_cache_arc(cache.clone())
        .build()
        .unwrap();

    let mut session = dm
        .session(
            inst(),
            EventKind::Bar(BarInterval::OneMinute),
            Scope::Historical { from: Timestamp(0), to: Timestamp(1000) },
            PersistenceOptions::read_only(),
        )
        .await
        .unwrap();

    let (bars, _gaps) = drain(&mut session).await;
    assert_eq!(bars.iter().map(|b| b.0).collect::<Vec<_>>(), vec![100, 200]);
    // The gap was fetched...
    assert_eq!(*fetched.lock().unwrap(), vec![(0, 1000)]);
    // ...but nothing was persisted: the whole range is still a gap.
    assert!(cache.lookup(&key(0, 1000)).await.unwrap().is_none());
}

#[tokio::test]
async fn refresh_refetches_whole_range_despite_coverage() {
    let cache = Arc::new(SurrealCache::open(SurrealCacheConfig::Memory).await.unwrap());
    // Pre-cache the whole range with STALE data.
    cache
        .store(&key(0, 1000), &[bar(500, 99.0)])
        .await
        .unwrap();

    // Provider serves FRESH data across the whole range.
    let data = vec![bar(100, 1.0), bar(900, 9.0)];
    let (provider, fetched) = RecordingProvider::new("rec", data);
    let dm = Datamancer::builder()
        .provider_arc(Arc::new(provider))
        .historical_cache_arc(cache.clone())
        .build()
        .unwrap();

    let mut session = dm
        .session(
            inst(),
            EventKind::Bar(BarInterval::OneMinute),
            Scope::Historical { from: Timestamp(0), to: Timestamp(1000) },
            PersistenceOptions::refresh(),
        )
        .await
        .unwrap();

    let (bars, _gaps) = drain(&mut session).await;
    // Served from the provider (fresh), not the stale cached 500/99.0.
    assert_eq!(bars.iter().map(|b| b.0).collect::<Vec<_>>(), vec![100, 900]);
    // Whole range was re-fetched despite existing coverage.
    assert_eq!(*fetched.lock().unwrap(), vec![(0, 1000)]);
}
```

- [ ] **Step 2: Run the tests**

Run: `cargo test -p datamancer --test historical_cache`
Expected: PASS for all tests in the file.

- [ ] **Step 3: commit**

```bash
cargo fmt
cargo clippy --all-targets -- -D warnings
git add crates/datamancer/tests/historical_cache.rs
git commit -m "test(datamancer): read_only and refresh mode-matrix cases"
```

---

## Task 9: Self-contained example

A runnable, credential-free example that demonstrates the cache: first run fetches+caches, second run serves from cache with zero provider calls.

**Files:**
- Create: `crates/datamancer/examples/cached_history.rs`
- Modify: `crates/datamancer/Cargo.toml` (declare the example, require `storage-surreal`)

- [ ] **Step 1: Declare the example**

In `crates/datamancer/Cargo.toml`, after the existing `[[example]]` block (lines 34-36), add:

```toml
[[example]]
name = "cached_history"
required-features = ["storage-surreal"]
```

- [ ] **Step 2: Write the example**

Create `crates/datamancer/examples/cached_history.rs`:

```rust
//! Historical read-through cache demo (no credentials, no network).
//!
//! A synthetic provider serves a fixed set of daily bars and counts how many
//! times it is asked to fetch. We open the same historical session twice
//! against an embedded SurrealKV cache:
//!
//! 1. Cold run — the cache is empty, so the provider is hit once and the data
//!    is stored.
//! 2. Warm run — the same range is fully covered, so the provider is NOT hit
//!    and every bar is served from disk.
//!
//! Run with:
//!
//! ```text
//! cargo run --example cached_history
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use datamancer::storage::{SurrealCache, SurrealCacheConfig};
use datamancer::{
    AssetClass, Bar, BarInterval, Datamancer, EventKind, Instrument, LiveHandle, MarketEvent,
    PersistenceOptions, Price, Provider, ProviderId, Result, Scope, Seq, Session, Timestamp,
};
use datamancer_core::HistoryRequest;
use futures::StreamExt;
use tokio::sync::mpsc;

const PROVIDER: &str = "synthetic";

/// Serves `count` daily bars and tracks how many fetches it has served.
struct SyntheticProvider {
    bars: Vec<MarketEvent>,
    fetch_count: Arc<AtomicUsize>,
}

impl SyntheticProvider {
    fn new(symbol: &str, count: i64, fetch_count: Arc<AtomicUsize>) -> Self {
        const DAY_NS: i64 = 86_400 * 1_000_000_000;
        let bars = (0..count)
            .map(|i| {
                MarketEvent::Bar(Bar {
                    instrument: instrument(symbol),
                    interval: BarInterval::OneDay,
                    source_ts: Timestamp(i * DAY_NS),
                    rx_ts: Timestamp(i * DAY_NS),
                    seq: Seq(0),
                    open: Price::from_units(100 + i),
                    high: Price::from_units(101 + i),
                    low: Price::from_units(99 + i),
                    close: Price::from_units(100 + i),
                    volume: 1_000,
                })
            })
            .collect();
        Self { bars, fetch_count }
    }
}

#[async_trait]
impl Provider for SyntheticProvider {
    fn id(&self) -> &str {
        PROVIDER
    }
    fn supports(&self, _instrument: &Instrument, kind: EventKind) -> bool {
        matches!(kind, EventKind::Bar(BarInterval::OneDay))
    }
    async fn start_live(&self, _sink: mpsc::Sender<MarketEvent>) -> Result<Box<dyn LiveHandle>> {
        Err(datamancer::Error::Provider {
            provider: PROVIDER.to_string(),
            message: "synthetic provider is historical-only".to_string(),
        })
    }
    async fn fetch_history(
        &self,
        request: HistoryRequest,
        sink: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        self.fetch_count.fetch_add(1, Ordering::SeqCst);
        for ev in &self.bars {
            let MarketEvent::Bar(b) = ev else { continue };
            if b.source_ts.0 >= request.from.0 && b.source_ts.0 < request.to.0 {
                if sink.send(ev.clone()).await.is_err() {
                    return Ok(());
                }
            }
        }
        Ok(())
    }
}

fn instrument(symbol: &str) -> Instrument {
    Instrument::new(ProviderId::from_static(PROVIDER), AssetClass::Equity, symbol)
}

async fn run_once(dm: &Datamancer, label: &str) -> usize {
    let mut session: Session = dm
        .session(
            instrument("ACME"),
            EventKind::Bar(BarInterval::OneDay),
            Scope::Historical {
                from: Timestamp(0),
                to: Timestamp(i64::MAX),
            },
            PersistenceOptions::cached(),
        )
        .await
        .expect("open session");
    let mut stream = session.take_events().expect("take events");
    let mut bars = 0usize;
    while let Some(ev) = stream.next().await {
        if let MarketEvent::Bar(_) = ev {
            bars += 1;
        }
    }
    println!("{label}: received {bars} bars");
    bars
}

#[tokio::main]
async fn main() -> Result<()> {
    let dir = std::env::temp_dir().join("datamancer-cached-history-demo");
    let _ = std::fs::remove_dir_all(&dir); // start clean for the demo

    let fetch_count = Arc::new(AtomicUsize::new(0));
    let provider = SyntheticProvider::new("ACME", 30, fetch_count.clone());
    let cache = SurrealCache::open(SurrealCacheConfig::embedded(&dir)).await?;

    let dm = Datamancer::builder()
        .provider_arc(Arc::new(provider))
        .historical_cache(Box::new(cache))
        .build()?;

    let cold = run_once(&dm, "cold run").await;
    let warm = run_once(&dm, "warm run").await;

    let fetches = fetch_count.load(Ordering::SeqCst);
    println!("\nprovider fetches total: {fetches}");
    println!("cold bars == warm bars: {}", cold == warm);
    assert_eq!(cold, warm, "both runs return the same data");
    assert_eq!(fetches, 1, "warm run served entirely from cache");
    println!("\n✓ the warm run hit the cache, not the provider.");

    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
```

- [ ] **Step 3: Run the example**

Run: `cargo run --example cached_history`
Expected output ends with `✓ the warm run hit the cache, not the provider.` and the process exits 0 (the `assert_eq!(fetches, 1, ...)` holds).

- [ ] **Step 4: fmt + clippy + commit**

```bash
cargo fmt
cargo clippy --all-targets -- -D warnings
git add crates/datamancer/Cargo.toml crates/datamancer/examples/cached_history.rs
git commit -m "docs(datamancer): cached_history example demonstrating read-through"
```

---

## Task 10: README persistence section + final verification

**Files:**
- Modify: `crates/datamancer/README.md` (the `## Persistence (Future)` section, line ~100)

- [ ] **Step 1: Replace the README persistence section**

In `crates/datamancer/README.md`, replace the `## Persistence (Future)` section (from that heading up to the next `##` heading) with:

````markdown
## Persistence — Historical Cache

Datamancer can back a historical session with a `HistoricalCache` (the bundled
`SurrealCache` stores to SurrealKV on disk, or in-memory for tests). Caching is
controlled per-session by `PersistenceOptions`:

| `read_cache` | `write_cache` | mode      | behavior                                        |
|--------------|---------------|-----------|-------------------------------------------------|
| `false`      | `false`       | ephemeral | always fetch from the provider, store nothing   |
| `true`       | `true`        | cached    | serve covered ranges, fetch & store only gaps   |
| `true`       | `false`       | read-only | serve cache + fetch gaps, don't persist them    |
| `false`      | `true`        | refresh   | ignore coverage, re-fetch the range, overwrite  |

```rust
let dm = Datamancer::builder()
    .provider_arc(provider)
    .historical_cache(Box::new(SurrealCache::open(cfg).await?))
    .build()?;

let mut session = dm
    .session(instrument, kind, scope, PersistenceOptions::cached())
    .await?;
```

### How read-through works

For a `cached()` historical session over `[from, to)`, the cache's `gaps()`
report tiles the range into ordered, disjoint segments: covered subranges
replay from disk; the uncovered gaps are fetched from the provider, forwarded
to the consumer, and stored back. Because segments are emitted in time order,
the merged stream is `source_ts`-ordered and `seq` is monotonic — requesting a
year and later requesting ten years only ever fetches the missing nine.

Coverage is recorded honestly: a range is "covered" only once its fetch
completes. If a provider fetch fails partway, only the confirmed prefix is
stored, an in-band `Control::Gap` marks the remainder, and a later request
re-fetches what is still missing. An empty result over a successfully-fetched
range is legitimately covered (markets close; symbols have an inception date).

### Deferred

Cache **volume** is not yet bounded — a very large fetch can fill the disk; no
eviction or granularity policy exists. The live **tap log** and the
**resume primitive** (replay-on-retake, historical→live backfill seam) are
tracked separately and not yet wired.

See `examples/cached_history.rs` for a runnable, credential-free demo.
````

(Preserve the surrounding sections; only the persistence section changes.)

- [ ] **Step 2: Full verification across the workspace**

Run each and confirm clean:

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

Expected:
- `fmt --check`: no diff.
- `clippy`: no warnings (workspace `pedantic = deny`).
- `cargo test`: all tests pass — `datamancer-core`, `datamancer` unit tests (`persistence_options_tests`, `tile_tests`), and integration suites (`session_integration`, `surreal_cache`, `historical_cache`). `alpaca_real` stays skipped (`#[ignore]`).

- [ ] **Step 3: Verify the example still runs end-to-end**

Run: `cargo run --example cached_history`
Expected: exits 0 with the success line.

- [ ] **Step 4: commit**

```bash
git add crates/datamancer/README.md
git commit -m "docs(datamancer): document historical read-through cache"
```

---

## Done

The historical read-through cache is implemented, tested (cold / fully-cached /
partial-overlap / failure / read-only / refresh), demonstrated by a runnable
example, and documented. Spec B (`SurrealTapLog` + live write-through) and
Spec C (resume primitive) build on this foundation next.
