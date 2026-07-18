# Live "latest value" seed — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** On a pure-live subscription, concurrently fetch the provider's latest value and deliver it as the first data event for instant UI feedback — discarding it if a real live value arrives first.

**Architecture:** Add a provided `Provider::latest()` cold-boundary method (default `None`). In `create_authoritative`, for `Scope::Live { backfill_from: None }` only, spawn a non-blocking task that resolves `latest()` and sends the result over a `oneshot` into the authoritative `Controller`. `run_live` gains a guarded `select!` branch that injects the seed via `forward` (stamp → tee → fan-out) only if no data event (`Trade`/`Quote`/`Bar`) has been delivered yet, tracked by a new `Controller.data_forwarded` flag. Alpaca stock and crypto providers override `latest()` against their snapshot REST endpoints.

**Tech Stack:** Rust (edition 2024), tokio, async-trait, oxidized_alpaca 0.0.10.

## Global Constraints

- Workspace: resolver 3, edition 2024. `#![forbid(unsafe_code)]` in all crates.
- Lints: `clippy::pedantic = deny`. Run `cargo clippy --all-targets -- -D warnings`.
- Source-agnostic output: the seed is delivered as a plain `Trade`/`Quote`/`Bar` — no snapshot marker on the event, wire, or tap log.
- Ordering invariant: the authoritative `Controller` is the single `seq` writer. The seed must be stamped by it (via `forward`), never carry a pre-assigned `seq`.
- Trigger is default-on but **pure-live only** (`Scope::Live { backfill_from: None }`). Backfill sessions and historical sessions are untouched.
- Discard rule: only a delivered `Trade`/`Quote`/`Bar` cancels the seed; a `Control` (e.g. `ProviderConnected`) does not.
- `tokio::sync::oneshot` is already imported in `session.rs:72`.
- Before PR: `cargo deny check` and `.github/scripts/semver-checks.sh origin/main` (needs `cargo-semver-checks`).
- Design doc: `docs/superpowers/specs/2026-07-18-live-latest-seed-design.md`.

---

### Task 1: Add `Provider::latest()` provided method to core

**Files:**
- Modify: `crates/datamancer-core/src/traits/provider.rs` (add method to trait ~line 109; add test in the `tests` module ~line 210)

**Interfaces:**
- Produces: `async fn Provider::latest(&self, instrument: &Instrument, kind: EventKind) -> Result<Option<MarketEvent>>` with a default body returning `Ok(None)`.

- [ ] **Step 1: Write the failing test**

Add to the `#[cfg(test)] mod tests` block in `crates/datamancer-core/src/traits/provider.rs` (after `default_provider_metrics_is_none`). Note the module already imports `Provider`, `EventKind`, `Instrument`, and `MarketEvent`; add the `ProviderId`/`AssetClass` path inline as shown.

```rust
    #[tokio::test]
    async fn default_provider_latest_is_none() {
        use crate::instrument::{AssetClass, ProviderId};
        let p = BareProvider;
        let inst = Instrument::new(ProviderId::from_static("bare"), AssetClass::Equity, "AAPL");
        let got = p.latest(&inst, EventKind::Trade).await.unwrap();
        assert!(got.is_none());
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p datamancer-core default_provider_latest_is_none`
Expected: FAIL to compile — `no method named 'latest' found for struct 'BareProvider'`.

- [ ] **Step 3: Add the provided method to the `Provider` trait**

In `crates/datamancer-core/src/traits/provider.rs`, insert this method inside `pub trait Provider` immediately before the closing `}` of the trait (after `fn enabled(&self) -> bool { true }` at ~line 111):

```rust
    /// One-shot most-recent value for a symbol, for immediate consumer feedback
    /// when a live subscription opens. Cold-boundary, off the per-message hot
    /// path — datamancer calls this at most once per authoritative live session
    /// and never per websocket frame.
    ///
    /// Returns the most recent [`MarketEvent`] of `kind` for `instrument`, or
    /// `None` when the provider has no snapshot surface or nothing is available.
    /// `seq` on the returned event is a placeholder (`Seq(0)`); the authoritative
    /// controller re-stamps it in canonical delivery order, exactly as for live
    /// and backfill data.
    ///
    /// Default returns `None` — providers without a snapshot/latest endpoint
    /// (test fakes, replay-only sources) leave this alone and the live-seed step
    /// gracefully no-ops.
    async fn latest(
        &self,
        instrument: &Instrument,
        kind: EventKind,
    ) -> Result<Option<MarketEvent>> {
        let _ = (instrument, kind);
        Ok(None)
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p datamancer-core default_provider_latest_is_none`
Expected: PASS.

- [ ] **Step 5: Lint**

Run: `cargo clippy -p datamancer-core --all-targets -- -D warnings`
Expected: no warnings.

- [ ] **Step 6: Commit**

```bash
git add crates/datamancer-core/src/traits/provider.rs
git commit -m "feat(core): add provided Provider::latest() for live seed"
```

---

### Task 2: Wire the seed through the orchestrator

**Files:**
- Modify: `crates/datamancer/src/session.rs`
  - `Controller` struct (~line 1170) — add `data_forwarded: bool`
  - both `Controller { .. }` literals (~line 297 historical, ~line 457 live) — add `data_forwarded: false`
  - `create_authoritative` (~line 472-480) — spawn the seed fetch, pass `seed_rx` to `run_live`
  - `forward` (~line 1970) — set `data_forwarded` on data events
  - `run_live` (~line 1811) — new `seed_rx` param + guarded `select!` branch
- Test: `crates/datamancer/tests/session_integration.rs` — extend `FakeProvider` and add five tests

**Interfaces:**
- Consumes: `Provider::latest` (Task 1); existing `Controller::forward`, `oneshot` (imported at `session.rs:72`).
- Produces: `Controller.data_forwarded: bool`; `run_live(.., seed_rx: Option<oneshot::Receiver<Option<MarketEvent>>>)`.

- [ ] **Step 1: Extend the fake provider with a controllable `latest`**

In `crates/datamancer/tests/session_integration.rs`:

(a) Add fields to `FakeProviderState` (struct at ~line 25):

```rust
#[derive(Default)]
struct FakeProviderState {
    sink: Option<mpsc::Sender<MarketEvent>>,
    history: Vec<MarketEvent>,
    history_error: Option<String>,
    /// Value returned by `latest`; `None` models a provider with no snapshot.
    latest: Option<MarketEvent>,
    /// Count of `latest` calls, so tests can assert it is never called under backfill.
    latest_calls: usize,
    /// When set, `latest` awaits this before returning — lets a test force a
    /// live data event to win the race.
    latest_gate: Option<Arc<tokio::sync::Notify>>,
}
```

(b) Add controller methods to `impl FakeController` (after `set_history_error`, ~line 78):

```rust
    async fn set_latest(&self, ev: MarketEvent) {
        self.state.lock().await.latest = Some(ev);
    }

    async fn set_latest_gate(&self, gate: Arc<tokio::sync::Notify>) {
        self.state.lock().await.latest_gate = Some(gate);
    }

    async fn latest_calls(&self) -> usize {
        self.state.lock().await.latest_calls
    }
```

(c) Add the `latest` override to `impl Provider for FakeProvider` (after `fetch_history`, ~line 118):

```rust
    async fn latest(
        &self,
        _instrument: &Instrument,
        _kind: EventKind,
    ) -> Result<Option<MarketEvent>> {
        let (ev, gate) = {
            let mut guard = self.state.lock().await;
            guard.latest_calls += 1;
            (guard.latest.clone(), guard.latest_gate.clone())
        };
        if let Some(gate) = gate {
            gate.notified().await;
        }
        Ok(ev)
    }
```

- [ ] **Step 2: Write the five failing tests**

Append to `crates/datamancer/tests/session_integration.rs`. These reuse existing helpers `inst`, `trade(symbol, ts, price)` (line 153), `live_trade` (line 703), `drain_n` (line 684), and the tap-log replay pattern (line 748). `futures::StreamExt` is already imported, so `stream.next().await` works.

```rust
// ---------------------------------------------------------------------------
// Live latest-value seed tests
// ---------------------------------------------------------------------------

fn trade_price(ev: &MarketEvent) -> f64 {
    match ev {
        MarketEvent::Trade(t) => t.price.to_f64(),
        other => panic!("expected Trade, got {other:?}"),
    }
}

#[tokio::test]
async fn seed_wins_when_no_live_data_yet() {
    let (provider, ctrl) = FakeProvider::new("fake");
    ctrl.set_latest(trade("AAPL", 5, 999.0)).await;
    let dm = Datamancer::builder().provider_arc(provider).build().unwrap();

    let session = dm
        .session(
            inst("AAPL"),
            EventKind::Trade,
            Scope::Live { backfill_from: None },
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    let mut stream = session.take_events().await.expect("take events");

    // No live data pushed yet: the seed is the first event, stamped seq 0.
    let first = stream.next().await.expect("seed event");
    assert_eq!(trade_price(&first), 999.0);
    assert_eq!(first.seq(), Some(Seq(0)));

    // Live ticks follow, stamped after the seed.
    ctrl.push_live(trade("AAPL", 100, 10.0)).await;
    let second = stream.next().await.expect("live event");
    assert_eq!(trade_price(&second), 10.0);
    assert_eq!(second.seq(), Some(Seq(1)));
}

#[tokio::test]
async fn seed_discarded_when_live_data_wins() {
    let (provider, ctrl) = FakeProvider::new("fake");
    let gate = Arc::new(tokio::sync::Notify::new());
    ctrl.set_latest(trade("AAPL", 5, 999.0)).await; // distinctive price
    ctrl.set_latest_gate(gate.clone()).await; // latest() blocks until released
    let dm = Datamancer::builder().provider_arc(provider).build().unwrap();

    let session = dm
        .session(
            inst("AAPL"),
            EventKind::Trade,
            Scope::Live { backfill_from: None },
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    let mut stream = session.take_events().await.expect("take events");

    // A live trade arrives and is delivered while latest() is still gated.
    ctrl.push_live(trade("AAPL", 100, 10.0)).await;
    let first = stream.next().await.expect("live event");
    assert_eq!(trade_price(&first), 10.0);
    assert_eq!(first.seq(), Some(Seq(0)));

    // Release the seed: data was already delivered, so it must be discarded
    // (consuming no seq). A following live trade proves the seed never appears.
    gate.notify_one();
    ctrl.push_live(trade("AAPL", 200, 11.0)).await;
    let second = stream.next().await.expect("live event");
    assert_eq!(trade_price(&second), 11.0);
    assert_eq!(second.seq(), Some(Seq(1)));
}

#[tokio::test]
async fn no_seed_when_latest_returns_none() {
    let (provider, ctrl) = FakeProvider::new("fake");
    // latest not set -> returns None.
    let dm = Datamancer::builder().provider_arc(provider).build().unwrap();

    let session = dm
        .session(
            inst("AAPL"),
            EventKind::Trade,
            Scope::Live { backfill_from: None },
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    let mut stream = session.take_events().await.expect("take events");

    ctrl.push_live(trade("AAPL", 100, 10.0)).await;
    let first = stream.next().await.expect("live event");
    assert_eq!(trade_price(&first), 10.0);
    assert_eq!(first.seq(), Some(Seq(0))); // no seed consumed seq 0
}

#[tokio::test]
async fn no_seed_under_backfill() {
    let (provider, ctrl) = FakeProvider::new("fake");
    ctrl.set_latest(trade("AAPL", 5, 999.0)).await;
    ctrl.set_history(vec![trade("AAPL", 1, 1.0)]).await;
    let dm = Datamancer::builder().provider_arc(provider.clone()).build().unwrap();

    let session = dm
        .session(
            inst("AAPL"),
            EventKind::Trade,
            Scope::Live { backfill_from: Some(Timestamp(1)) },
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    let mut stream = session.take_events().await.expect("take events");
    // Drain the backfilled event so the session is fully wired before asserting.
    let _ = stream.next().await;

    assert_eq!(ctrl.latest_calls().await, 0, "latest must not be called under backfill");
}

#[tokio::test]
async fn seed_is_teed_to_tap_log() {
    let (provider, ctrl) = FakeProvider::new("fake");
    ctrl.set_latest(trade("AAPL", 5, 999.0)).await;
    let log = std::sync::Arc::new(TursoTapLog::open(TursoTapLogConfig::Memory).await.unwrap());
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .tap_log_arc(log.clone())
        .build()
        .unwrap();

    let session = dm
        .session(
            inst("AAPL"),
            EventKind::Trade,
            Scope::Live { backfill_from: None },
            PersistenceOptions::none().with_tap_log(true),
        )
        .await
        .unwrap();
    let mut stream = session.take_events().await.expect("take events");

    let first = stream.next().await.expect("seed event");
    assert_eq!(trade_price(&first), 999.0);
    log.flush().await.unwrap();

    let source = log.as_replay_source();
    let mut replay = source
        .open(ReplayRequest {
            instruments: vec![inst("AAPL")],
            kinds: vec![EventKind::Trade],
            from: Timestamp(i64::MIN),
            to: Timestamp(i64::MAX),
        })
        .await
        .unwrap();
    let mut tss = Vec::new();
    while let Some(ev) = replay.next().await {
        if let MarketEvent::Trade(t) = ev {
            tss.push(t.source_ts.0);
        }
    }
    assert_eq!(tss, vec![5], "seed must be persisted to the tap log");
}
```

Note: `FakeProvider::new` returns `(Arc<Self>, FakeController)`; the `no_seed_under_backfill` test uses `provider.clone()` before `.build()`. If `provider_arc` consumes the `Arc`, clone as shown so the test keeps no extra handle it does not need — actually only `ctrl` is needed after build, so `provider.clone()` is unnecessary; use `.provider_arc(provider)` directly and drop the `.clone()`. (Both compile; prefer no clone.)

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p datamancer --test session_integration 2>&1 | tail -30` (the
whole target — `cargo test` takes only one name filter, so `seed_ no_seed_` is
invalid; the full target covers both cohorts).
Expected: compile error — `run_live` has no `seed_rx` and `Controller` has no `data_forwarded` (behavior not yet wired). The five new tests fail.

- [ ] **Step 4: Add the `data_forwarded` field to `Controller`**

In `crates/datamancer/src/session.rs`, add to the `struct Controller` definition (after `connection_seen: bool,` ~line 1204):

```rust
    /// True once any data event (Trade/Quote/Bar) has been delivered on this
    /// authoritative session. Gates the pure-live "latest value" seed: once a
    /// real live value has been delivered, a late-arriving seed is discarded.
    data_forwarded: bool,
```

- [ ] **Step 5: Initialize the field at both `Controller` literals**

Historical literal (~line 297-311): add `data_forwarded: false,` after `connection_seen: false,`.
Live literal in `create_authoritative` (~line 457-471): add `data_forwarded: false,` after `connection_seen: false,`.

- [ ] **Step 6: Set `data_forwarded` in `forward`**

In `Controller::forward` (~line 1970), after `let ev = self.stamp(ev);` and before the `if let MarketEvent::Control(c) = &ev` block, insert:

```rust
        if matches!(
            ev,
            MarketEvent::Trade(_) | MarketEvent::Quote(_) | MarketEvent::Bar(_)
        ) {
            self.data_forwarded = true;
        }
```

- [ ] **Step 7: Spawn the seed fetch in `create_authoritative`**

In `create_authoritative`, the `backfill_from` binding is computed at ~line 472. Immediately after it, and before `accounting.record_live_start();` (~line 476), insert:

```rust
        // Pure-live only: fire a non-gating "latest value" fetch concurrently
        // with the live connect. The result is injected as the first data event
        // (immediate UI feedback) unless a real live value wins the race, in
        // which case run_live discards it. Backfill sessions already supply a
        // first value, so they skip this.
        let seed_rx = if backfill_from.is_none() {
            let (seed_tx, seed_rx) = oneshot::channel();
            let p = provider.clone();
            let inst = instrument.clone();
            tokio::spawn(async move {
                let seed = match p.latest(&inst, kind).await {
                    Ok(seed) => seed,
                    Err(e) => {
                        tracing::debug!(
                            instrument = %inst,
                            error = %e,
                            "latest() fetch failed; no live seed"
                        );
                        None
                    }
                };
                // Receiver may already be gone (fast teardown / live won the
                // race); a failed send is expected and ignored.
                let _ = seed_tx.send(seed);
            });
            Some(seed_rx)
        } else {
            None
        };
```

- [ ] **Step 8: Pass `seed_rx` to `run_live`**

Change the spawn at ~line 480 from:

```rust
        tokio::spawn(controller.run_live(live, backfill_from, provider_rx, cmd_rx, remove_rx));
```

to:

```rust
        tokio::spawn(controller.run_live(live, backfill_from, provider_rx, cmd_rx, remove_rx, seed_rx));
```

- [ ] **Step 9: Add the `seed_rx` param and select branch to `run_live`**

Change the `run_live` signature (~line 1811-1818) to add the new parameter:

```rust
    async fn run_live(
        mut self,
        live: Box<dyn LiveHandle>,
        backfill_from: Option<Timestamp>,
        mut provider_rx: mpsc::Receiver<MarketEvent>,
        mut cmd_rx: mpsc::Receiver<SessionCommand>,
        mut remove_rx: mpsc::UnboundedReceiver<SubscriberId>,
        mut seed_rx: Option<oneshot::Receiver<Option<MarketEvent>>>,
    ) {
```

Then add a branch to the `tokio::select!` inside the main `loop` (the block at ~line 1837-1855), after the `ev = provider_rx.recv()` arm:

```rust
                res = async { seed_rx.as_mut().unwrap().await }, if seed_rx.is_some() => {
                    // Fire once, then disable this branch for the rest of the loop.
                    seed_rx = None;
                    if let Ok(Some(seed)) = res {
                        if !self.data_forwarded {
                            // Stamp (after any connect control already forwarded),
                            // tee to the tap log, and fan out. `forward` sets
                            // data_forwarded so nothing else can seed.
                            self.forward(seed).await;
                        }
                        // else: a live data event already won — discard the seed.
                    }
                    // Err(RecvError) / Ok(None): nothing to seed.
                }
```

- [ ] **Step 10: Run the tests to verify they pass**

Run: `cargo test -p datamancer --test session_integration 2>&1 | tail -20`
Expected: all tests PASS, including the five new ones. The existing live tests still pass (the fake's new `latest` returns `None`, so no seed is injected for them).

- [ ] **Step 11: Lint**

Run: `cargo clippy -p datamancer --all-targets -- -D warnings`
Expected: no warnings. (If clippy flags the `async { seed_rx.as_mut().unwrap().await }` block, keep it — the `, if seed_rx.is_some()` guard makes the `unwrap` sound; add `#[allow(clippy::unwrap_used)]` only if a lint actually fires.)

- [ ] **Step 12: Commit**

```bash
git add crates/datamancer/src/session.rs crates/datamancer/tests/session_integration.rs
git commit -m "feat: seed pure-live subscriptions with provider latest value"
```

---

### Task 3: Wire `latest()` for the Alpaca stock provider

**Files:**
- Modify: `crates/datamancer/src/providers/alpaca.rs`
  - `impl Provider for AlpacaProvider` (~line 264) — add `latest`
  - add a `snapshot_to_event` helper + a unit test near the existing `translate_*` helpers (~line 956) and the `#[cfg(test)]` module

**Interfaces:**
- Consumes: `Provider::latest` (Task 1); oxidized_alpaca `MarketDataClient::stock_snapshot(symbol) -> StockSnapshotRequest`, `StockSnapshot { latest_trade: Option<StockTrade>, latest_quote: Option<StockQuote>, minute_bar: Option<Bar>, daily_bar: Option<Bar>, .. }`.
- Produces: `AlpacaProvider::latest`.

- [ ] **Step 1: Write the failing mapping test**

Add to the `#[cfg(test)] mod tests` block in `crates/datamancer/src/providers/alpaca.rs`. This tests the pure mapping helper (no network). Check the module's imports; add any missing (`EventKind`, `BarInterval`, `MarketEvent`) via the existing `use super::*;` if present, otherwise import explicitly.

```rust
    #[test]
    fn snapshot_maps_kind_to_event() {
        use oxidized_alpaca::restful::market_data::stock::snapshots::StockSnapshot;

        // Deserialize a snapshot fixture (Alpaca's wire field names).
        let json = r#"{
            "latestTrade": {"t":"2024-01-02T15:00:00Z","x":"V","p":187.5,"s":10,"c":[],"z":"C"},
            "latestQuote": {"t":"2024-01-02T15:00:00Z","bx":"V","bp":187.4,"bs":3,"ax":"V","ap":187.6,"as":4,"c":[],"z":"C"},
            "minuteBar": {"t":"2024-01-02T15:00:00Z","o":187.0,"c":187.5,"h":188.0,"l":186.5,"v":1000,"n":50,"vw":187.3},
            "dailyBar": {"t":"2024-01-02T00:00:00Z","o":185.0,"c":187.5,"h":189.0,"l":184.0,"v":900000,"n":5000,"vw":186.9},
            "prevDailyBar": null
        }"#;
        let snap: StockSnapshot = serde_json::from_str(json).unwrap();
        let inst = provider_instrument("AAPL");
        let rx = Timestamp(42);

        assert!(matches!(
            snapshot_to_event(&snap, &inst, EventKind::Trade, rx),
            Some(MarketEvent::Trade(_))
        ));
        assert!(matches!(
            snapshot_to_event(&snap, &inst, EventKind::Quote, rx),
            Some(MarketEvent::Quote(_))
        ));
        assert!(matches!(
            snapshot_to_event(&snap, &inst, EventKind::Bar(BarInterval::OneMinute), rx),
            Some(MarketEvent::Bar(_))
        ));
        assert!(matches!(
            snapshot_to_event(&snap, &inst, EventKind::Bar(BarInterval::OneDay), rx),
            Some(MarketEvent::Bar(_))
        ));
        // Unsupported interval -> None.
        assert!(snapshot_to_event(&snap, &inst, EventKind::Bar(BarInterval::FiveMinute), rx).is_none());
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p datamancer --lib snapshot_maps_kind_to_event 2>&1 | tail -20`
Expected: FAIL to compile — `cannot find function 'snapshot_to_event'`.

- [ ] **Step 3: Add the mapping helper**

In `crates/datamancer/src/providers/alpaca.rs`, near the other `translate_*` helpers (~line 993, after `translate_bar`), add. The snapshot's REST types are `StockTrade`/`StockQuote`/`Bar` (fields: trade `timestamp/price/size:u32`; quote `timestamp/bid_price/bid_size:u32/ask_price/ask_size:u32`; bar `time/open/high/low/close/volume`). Mirror `fetch_history_via`'s conversions (`u64::from` for `u32` sizes, `Quantity::from_units` for bar volume).

```rust
/// Map a stock snapshot onto the canonical event for `kind`, or `None` when the
/// snapshot lacks that datum (or the bar interval has no snapshot field). `seq`
/// is a placeholder; the authoritative controller re-stamps on delivery.
fn snapshot_to_event(
    snap: &oxidized_alpaca::restful::market_data::stock::snapshots::StockSnapshot,
    instrument: &Instrument,
    kind: EventKind,
    rx: Timestamp,
) -> Option<MarketEvent> {
    match kind {
        EventKind::Trade => snap.latest_trade.as_ref().map(|t| {
            MarketEvent::Trade(Trade {
                instrument: instrument.clone(),
                source_ts: chrono_to_ts(t.timestamp),
                rx_ts: rx,
                seq: Seq(0),
                price: Price::from_f64_round(t.price),
                size: Quantity::from_units(u64::from(t.size)),
            })
        }),
        EventKind::Quote => snap.latest_quote.as_ref().map(|q| {
            MarketEvent::Quote(Quote {
                instrument: instrument.clone(),
                source_ts: chrono_to_ts(q.timestamp),
                rx_ts: rx,
                seq: Seq(0),
                bid: Price::from_f64_round(q.bid_price),
                bid_size: Quantity::from_units(u64::from(q.bid_size)),
                ask: Price::from_f64_round(q.ask_price),
                ask_size: Quantity::from_units(u64::from(q.ask_size)),
            })
        }),
        EventKind::Bar(interval) => {
            let bar = match interval {
                BarInterval::OneMinute => snap.minute_bar.as_ref(),
                BarInterval::OneDay => snap.daily_bar.as_ref(),
                _ => None,
            }?;
            Some(MarketEvent::Bar(Bar {
                instrument: instrument.clone(),
                interval,
                source_ts: chrono_to_ts(bar.time),
                rx_ts: rx,
                seq: Seq(0),
                open: Price::from_f64_round(bar.open),
                high: Price::from_f64_round(bar.high),
                low: Price::from_f64_round(bar.low),
                close: Price::from_f64_round(bar.close),
                volume: Quantity::from_units(bar.volume.max(0).cast_unsigned()),
            }))
        }
    }
}
```

Note: confirm the REST `Bar.volume` type. If it is `u64` (not `i64`), replace `bar.volume.max(0).cast_unsigned()` with `bar.volume`. Check with: `grep -n "pub volume" ~/.cargo/registry/src/*/oxidized_alpaca-0.0.10/src/restful/market_data/stock/mod.rs`.

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p datamancer --lib snapshot_maps_kind_to_event 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 5: Add the `latest` method to the provider**

In `impl Provider for AlpacaProvider` (~line 264), after `fetch_history` (~line 299), add:

```rust
    async fn latest(
        &self,
        instrument: &Instrument,
        kind: EventKind,
    ) -> Result<Option<MarketEvent>> {
        let rest = self
            .rest_clients()
            .market_data
            .ok_or_else(|| Error::Provider {
                provider: PROVIDER_ID.to_string(),
                message: "REST client not initialized (Alpaca credentials missing?)".to_string(),
            })?;
        let snap = rest
            .stock_snapshot(instrument.symbol())
            .execute()
            .await
            .map_err(|e| Error::Provider {
                provider: PROVIDER_ID.to_string(),
                message: format!("stock_snapshot: {e}"),
            })?;
        Ok(snapshot_to_event(&snap, instrument, kind, wall_clock_ts()))
    }
```

Verify `rest_clients()` and `wall_clock_ts()` are in scope here (they are used by `fetch_history` and the translate helpers in this file).

- [ ] **Step 6: Build and lint**

Run: `cargo clippy -p datamancer --all-targets -- -D warnings`
Expected: no warnings.

- [ ] **Step 7: Commit**

```bash
git add crates/datamancer/src/providers/alpaca.rs
git commit -m "feat(alpaca): implement Provider::latest via stock snapshot"
```

---

### Task 4: Wire `latest()` for the Alpaca crypto provider

**Files:**
- Modify: `crates/datamancer/src/providers/alpaca_crypto.rs`
  - `RestState` (~line 149) — add a `market_data` client
  - a `build_market_data` fn + wire it into `RestState::new`/rebuild path (mirror `build_trading` ~line 162)
  - `impl Provider for AlpacaCryptoProvider` (~line 245) — add `latest`
  - a `crypto_snapshot_to_event` helper + unit test

**Interfaces:**
- Consumes: `Provider::latest` (Task 1); oxidized_alpaca `MarketDataClient::crypto_snapshots(&[&str], CryptoLocation) -> HashMap<String, CryptoSnapshot>`, `CryptoSnapshot { latest_trade: Option<CryptoTrade>, latest_quote: Option<CryptoQuote>, minute_bar/daily_bar: Option<CryptoBar>, .. }`; `CryptoLocation::{Us, Us1, Eu1}`.
- Produces: `AlpacaCryptoProvider::latest`.

- [ ] **Step 1: Inspect the crypto REST-client construction to mirror it**

Run: `grep -n "fn rest_clients\|market_data\|MarketDataClient\|fn build_trading\|current_trading\|fn trading_client\|RestState" crates/datamancer/src/providers/alpaca_crypto.rs`
Read the trading-client rebuild path (`build_trading` at ~line 162 and where `RestState` is built at ~line 199 and rebuilt) so the new `market_data` field follows the same watch/rebuild discipline. Confirm the crypto module imports `MarketDataClient` (it imports `TradingClient`; add `MarketDataClient` to the same `use oxidized_alpaca::{...}` line).

- [ ] **Step 2: Write the failing mapping test**

Add to the `#[cfg(test)] mod tests` block in `crates/datamancer/src/providers/alpaca_crypto.rs`:

```rust
    #[test]
    fn crypto_snapshot_maps_kind_to_event() {
        use oxidized_alpaca::restful::market_data::crypto::CryptoSnapshot;

        let json = r#"{
            "latestTrade": {"t":"2024-01-02T15:00:00Z","p":42000.0,"s":0.5,"i":1,"tks":"B"},
            "latestQuote": {"t":"2024-01-02T15:00:00Z","bp":41999.0,"bs":1.2,"ap":42001.0,"as":0.8},
            "minuteBar": {"t":"2024-01-02T15:00:00Z","o":42000.0,"h":42100.0,"l":41900.0,"c":42050.0,"v":12.5},
            "dailyBar": {"t":"2024-01-02T00:00:00Z","o":41000.0,"h":42500.0,"l":40800.0,"c":42050.0,"v":900.0},
            "prevDailyBar": null
        }"#;
        let snap: CryptoSnapshot = serde_json::from_str(json).unwrap();
        let inst = provider_instrument("BTC/USD");
        let rx = Timestamp(42);

        assert!(matches!(
            crypto_snapshot_to_event(&snap, &inst, EventKind::Trade, rx),
            Some(MarketEvent::Trade(_))
        ));
        assert!(matches!(
            crypto_snapshot_to_event(&snap, &inst, EventKind::Quote, rx),
            Some(MarketEvent::Quote(_))
        ));
        assert!(matches!(
            crypto_snapshot_to_event(&snap, &inst, EventKind::Bar(BarInterval::OneMinute), rx),
            Some(MarketEvent::Bar(_))
        ));
        assert!(matches!(
            crypto_snapshot_to_event(&snap, &inst, EventKind::Bar(BarInterval::OneDay), rx),
            Some(MarketEvent::Bar(_))
        ));
        assert!(
            crypto_snapshot_to_event(&snap, &inst, EventKind::Bar(BarInterval::FiveMinute), rx).is_none()
        );
    }
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cargo test -p datamancer --lib crypto_snapshot_maps_kind_to_event 2>&1 | tail -20`
Expected: FAIL to compile — `cannot find function 'crypto_snapshot_to_event'`.

- [ ] **Step 4: Add the crypto mapping helper**

In `crates/datamancer/src/providers/alpaca_crypto.rs`, near `translate_bar` (~line 994). REST field types: crypto trade `timestamp/price:f64/size:f64`; quote `timestamp/bid_price:f64/bid_size:f64/ask_price:f64/ask_size:f64`; bar `timestamp/open/high/low/close/volume:f64`. Use `Quantity::from_f64_round` for sizes/volume (matches the crypto `translate_*` helpers).

```rust
/// Map a crypto snapshot onto the canonical event for `kind`, or `None` when the
/// snapshot lacks that datum. `seq` is a placeholder; the controller re-stamps.
fn crypto_snapshot_to_event(
    snap: &oxidized_alpaca::restful::market_data::crypto::CryptoSnapshot,
    instrument: &Instrument,
    kind: EventKind,
    rx: Timestamp,
) -> Option<MarketEvent> {
    match kind {
        EventKind::Trade => snap.latest_trade.as_ref().map(|t| {
            MarketEvent::Trade(Trade {
                instrument: instrument.clone(),
                source_ts: chrono_to_ts(t.timestamp),
                rx_ts: rx,
                seq: Seq(0),
                price: Price::from_f64_round(t.price),
                size: Quantity::from_f64_round(t.size),
            })
        }),
        EventKind::Quote => snap.latest_quote.as_ref().map(|q| {
            MarketEvent::Quote(Quote {
                instrument: instrument.clone(),
                source_ts: chrono_to_ts(q.timestamp),
                rx_ts: rx,
                seq: Seq(0),
                bid: Price::from_f64_round(q.bid_price),
                bid_size: Quantity::from_f64_round(q.bid_size),
                ask: Price::from_f64_round(q.ask_price),
                ask_size: Quantity::from_f64_round(q.ask_size),
            })
        }),
        EventKind::Bar(interval) => {
            let bar = match interval {
                BarInterval::OneMinute => snap.minute_bar.as_ref(),
                BarInterval::OneDay => snap.daily_bar.as_ref(),
                _ => None,
            }?;
            Some(MarketEvent::Bar(Bar {
                instrument: instrument.clone(),
                interval,
                source_ts: chrono_to_ts(bar.timestamp),
                rx_ts: rx,
                seq: Seq(0),
                open: Price::from_f64_round(bar.open),
                high: Price::from_f64_round(bar.high),
                low: Price::from_f64_round(bar.low),
                close: Price::from_f64_round(bar.close),
                volume: Quantity::from_f64_round(bar.volume),
            }))
        }
    }
}
```

- [ ] **Step 5: Run the mapping test to verify it passes**

Run: `cargo test -p datamancer --lib crypto_snapshot_maps_kind_to_event 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 6: Add a `market_data` REST client to the crypto provider**

Follow the existing watch/rebuild pattern. In `RestState` (~line 149) add:

```rust
    /// Market-data API client, used for the snapshot/latest surface. `None`
    /// when credentials aren't available.
    market_data: Option<MarketDataClient>,
```

Add a builder mirroring `build_trading` (~line 162):

```rust
fn build_market_data(cfg: &AlpacaCryptoProviderConfig) -> Option<MarketDataClient> {
    let settings = cfg.settings.current()?;
    match cfg.credentials.current() {
        Resolved::Env => MarketDataClient::new(settings.account_type).ok(),
        Resolved::Creds(c) => {
            MarketDataClient::new_with_credentials(settings.account_type, c.to_api_key()).ok()
        }
        Resolved::Missing => None,
    }
}
```

Wire `market_data: build_market_data(&cfg),` into the `RestState { .. }` literal in `AlpacaCryptoProvider::new` (~line 199), and into any rebuild site that reconstructs `trading` on a watch bump (search for `build_trading(` and add the parallel `build_market_data(` assignment there). Add a `current_market_data()` accessor mirroring however `trading` is read out of the mutex (search for where `trading` is cloned out; add the parallel method returning `Option<MarketDataClient>`).

- [ ] **Step 7: Map the crypto venue to `CryptoLocation`**

Add near the venue→`CryptoFeed` mapping (~line 423):

```rust
fn venue_location(venue: AlpacaCryptoVenue) -> oxidized_alpaca::restful::market_data::crypto::CryptoLocation {
    use oxidized_alpaca::restful::market_data::crypto::CryptoLocation;
    match venue {
        AlpacaCryptoVenue::Us => CryptoLocation::Us,
        AlpacaCryptoVenue::UsKraken => CryptoLocation::Us1,
        AlpacaCryptoVenue::EuKraken => CryptoLocation::Eu1,
    }
}
```

- [ ] **Step 8: Add the `latest` method to the crypto provider**

In `impl Provider for AlpacaCryptoProvider` (~line 245), after `fetch_history` (~line 279), add. Use the accessor added in Step 6 and the current settings' venue:

```rust
    async fn latest(
        &self,
        instrument: &Instrument,
        kind: EventKind,
    ) -> Result<Option<MarketEvent>> {
        let rest = self.current_market_data().ok_or_else(|| Error::Provider {
            provider: PROVIDER_ID.to_string(),
            message: "Market-data client not initialized (Alpaca credentials missing?)".to_string(),
        })?;
        let venue = self
            .cfg
            .settings
            .current()
            .map(|s| s.venue)
            .unwrap_or(AlpacaCryptoVenue::Us);
        let symbol = instrument.symbol();
        let mut snaps = rest
            .crypto_snapshots(&[symbol], venue_location(venue))
            .await
            .map_err(|e| Error::Provider {
                provider: PROVIDER_ID.to_string(),
                message: format!("crypto_snapshots: {e}"),
            })?;
        let Some(snap) = snaps.remove(symbol) else {
            return Ok(None);
        };
        Ok(crypto_snapshot_to_event(&snap, instrument, kind, wall_clock_ts()))
    }
```

Verify field/accessor names against this file: `self.cfg.settings.current()` returns the `AlpacaCryptoSettings` (has a `venue` field per ~line 113); `wall_clock_ts()` is used by the crypto translate helpers; `instrument.symbol()` yields e.g. `"BTC/USD"`. Adjust `self.cfg.settings.current().map(|s| s.venue)` if `venue` lives elsewhere on the settings type.

- [ ] **Step 9: Build, test, lint**

Run: `cargo test -p datamancer --lib 2>&1 | tail -20`
Expected: PASS (both mapping tests).
Run: `cargo clippy -p datamancer --all-targets -- -D warnings`
Expected: no warnings.

- [ ] **Step 10: Commit**

```bash
git add crates/datamancer/src/providers/alpaca_crypto.rs
git commit -m "feat(alpaca-crypto): implement Provider::latest via crypto snapshot"
```

---

### Task 5: Version bump, gates, and full verification

**Files:**
- Modify: `Cargo.toml` (workspace `version` at line 18)

**Interfaces:**
- Consumes: all prior tasks.

- [ ] **Step 1: Bump the workspace version**

All crates use `version.workspace = true`, so one bump moves them in lockstep. In the workspace root `Cargo.toml`, change `version = "0.5.0"` (line 18) to:

```toml
version = "0.6.0"
```

This is a minor bump for the additive `Provider::latest` method (no breaking change; no wire/control-protocol change).

- [ ] **Step 2: Update the lockfile**

Run: `cargo update -p datamancer-core -p datamancer -p datamancer-client -p datamancer-credentials -p datamancer-transport-iceoryx2 -p datamancer-transport-ws -p datamancerd --precise 0.6.0`
(scoped to the workspace crates rather than `cargo update -w`, which can rewrite
unrelated dependency resolutions). Then inspect the `Cargo.lock` diff and keep
only the intended `0.5.0 → 0.6.0` version bumps.
Expected: workspace crates updated to 0.6.0 in `Cargo.lock`, no unrelated churn.

- [ ] **Step 3: Full workspace build + tests**

Run: `cargo test 2>&1 | tail -25`
Expected: all tests pass (skips `#[ignore]`d real-Alpaca/daemon-e2e tests).

- [ ] **Step 4: Full clippy**

Run: `cargo clippy --all-targets -- -D warnings`
Expected: no warnings.

- [ ] **Step 5: Format**

Run: `cargo fmt --all && git diff --stat`
Expected: no unexpected reformatting outside the touched files. Stage any fmt changes.

- [ ] **Step 6: License / advisory gate**

Run: `cargo deny check`
Expected: PASS (no new dependencies were added — oxidized_alpaca 0.0.10 is already a dep).

- [ ] **Step 7: Semver gate**

Run: `git fetch origin main && .github/scripts/semver-checks.sh origin/main 2>&1 | tail -30`
Expected: reports the `datamancer-core` change as a **minor** (non-breaking) addition, satisfied by the 0.6.0 bump. If it flags a required bump not yet applied, apply it and re-run.

- [ ] **Step 8: Manual real-endpoint check (optional, needs credentials)**

If Alpaca credentials are available, sanity-check the seed end-to-end against the real snapshot endpoint alongside the existing ignored suite:
Run: `cargo test -p datamancer --test alpaca_real -- --ignored 2>&1 | tail -20`
Expected: the ignored suite passes, including the two `latest()` smoke tests this
work adds — `stock_latest_returns_snapshot_event` and
`crypto_latest_returns_snapshot_event` — which exercise the real snapshot
endpoint end-to-end (the crypto one guards the symbol-keyed-map lookup).

- [ ] **Step 9: Commit**

```bash
git add Cargo.toml Cargo.lock
git commit -m "chore: bump workspace 0.5.0 -> 0.6.0 for Provider::latest"
```

---

## Self-Review

**Spec coverage:**
- Provided `Provider::latest` (default None) → Task 1. ✅
- Default-on, pure-live-only trigger → Task 2 Step 7 (`if backfill_from.is_none()`). ✅
- Non-gating concurrent fetch → Task 2 Step 7 (`tokio::spawn`, never awaited before `start_live`). ✅
- Discard when a data event wins; controls don't cancel → Task 2 Steps 6, 9 (`data_forwarded` set only on Trade/Quote/Bar; seed branch checks it). ✅
- Seed injected via `forward` (stamped in order, teed to tap log, fanned out) → Task 2 Step 9. ✅
- Plain, indistinguishable seed (no event-model change) → no event struct touched; only `Seq(0)` placeholder re-stamped. ✅
- Both Alpaca stock + crypto wired → Tasks 3, 4. ✅
- Tests: seed-wins ordering, live-wins discard, default-None, no-seed-under-backfill, tap-log fidelity → Task 2 Step 2; snapshot mapping → Tasks 3, 4. ✅
- Minor version bump + CI gates → Task 5. ✅

**Placeholder scan:** No TBD/TODO; every code step shows full code. Two explicit "verify field name / volume type" notes (Task 3 Step 3, Task 4 Steps 6/8) are grounded lookups against the already-inspected oxidized_alpaca surface, not deferred design.

**Type consistency:** `data_forwarded: bool` defined (Task 2 Step 4) and used (Steps 6, 9); `run_live` gains `seed_rx: Option<oneshot::Receiver<Option<MarketEvent>>>` (Step 9) matching the spawn (Step 7) and call site (Step 8). `snapshot_to_event`/`crypto_snapshot_to_event` signatures identical across their test and impl. `Seq`, `Quantity::from_units`/`from_f64_round`, `Price::from_f64_round`, `chrono_to_ts`, `wall_clock_ts` all match existing in-file usage.
