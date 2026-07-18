# Live "latest value" seed for pure-live subscriptions

**Date:** 2026-07-18
**Status:** Approved (design)

## Problem

The pure subscription model can be slow to return a first value for live
applications: a `Scope::Live { backfill_from: None }` session emits nothing until
the upstream feed produces its first tick. For a UI that opens a symbol and wants
to paint a price immediately, that latency is user-visible and unbounded (a quiet
symbol may not tick for a long time).

## Goal

When a **pure-live** authoritative session opens, fire a **concurrent,
non-gating** one-shot "latest value" fetch. Deliver it as the first data event
so consumers get immediate feedback — unless a real live value beats it, in which
case the seed is discarded.

Constraints from the request:

- The latest-value fetch **must not gate** `start_live` / `subscribe`. It runs
  concurrently and its failure/slowness never delays the live connection.
- If a **live value** arrives before the fetch resolves, the fetch result is
  **discarded**.

## Non-goals

- Not a backfill. Backfill (`Scope::Live { backfill_from: Some(t) }`) already
  supplies a first value by stitching history before live; those sessions are
  untouched.
- Not a snapshot marker on the wire. The seed is delivered as a plain
  `Trade`/`Quote`/`Bar`, indistinguishable from a live tick (see Decisions).
- No dedup against the first live tick.

## Decisions (from brainstorming)

1. **Trigger:** default-on, **pure-live only** — `Scope::Live { backfill_from:
   None }`. Backfill sessions skip it. No consumer API change; no opt-out knob
   (YAGNI — can be added later if a consumer needs to suppress it).
2. **Mechanism:** a new **provided** `Provider::latest()` trait method (default
   `Ok(None)`), not a reuse of ranged `fetch_history`. Providers opt in by
   overriding it against their native latest/snapshot endpoints.
3. **Seed marking:** none. Delivered as a plain data event. Its older `source_ts`
   and `seq` (stamped in canonical order) are the only signals. Honors the
   source-agnostic-output invariant and leaves the event model / transport /
   tap-log encoding unchanged.
4. **Discard rule:** only a delivered **data** event (`Trade`/`Quote`/`Bar`)
   cancels the seed. A `ProviderConnected` (or any connectivity `Control`) does
   **not** — so the seed lands right after the connect control, before the first
   live tick.
5. **Tap log:** the seed **is** teed to the tap log (delivered via `forward`),
   so tap-log replay reproduces the delivered stream faithfully.
6. **Providers wired now:** both `alpaca` (stock) and `alpaca_crypto`.

## Design

### Core trait change — `datamancer-core/src/traits/provider.rs`

Add a provided method to `Provider`:

```rust
/// One-shot most-recent value for a symbol, for immediate consumer feedback
/// when a live subscription opens. Cold-boundary, off the per-message hot path.
///
/// Returns the most recent `MarketEvent` of `kind` for `instrument`, or `None`
/// when the provider has no snapshot surface or nothing is available. `seq` on
/// the returned event is a placeholder (`Seq(0)`); the authoritative controller
/// re-stamps it in canonical delivery order, exactly as for live/backfill data.
///
/// Default returns `None` — providers without a snapshot/latest endpoint
/// (test fakes, replay-only sources) leave this alone and the live-seed step
/// gracefully no-ops.
async fn latest(
    &self,
    instrument: &Instrument,
    kind: EventKind,
) -> Result<Option<MarketEvent>> {
    Ok(None)
}
```

Adding a **provided** method to a public trait is non-breaking for implementors.
`datamancer-core`'s public API grows, so expect a **minor** version bump; the
`cargo-semver-checks` gate should report this as non-breaking (no new enum
variant, no new pub field on a constructible struct).

### Orchestrator wiring — `datamancer/src/session.rs`

**Spawn the fetch (in `create_authoritative`).** After `backfill_from` is
computed and only when it is `None`, spawn a detached task that resolves the
latest value and sends it over a `oneshot` into the controller. Clone
`instrument` before it is moved into `live.subscribe(...)`.

```rust
let seed_rx = if backfill_from.is_none() {
    let (seed_tx, seed_rx) = tokio::sync::oneshot::channel();
    let p = provider.clone();
    let inst = instrument.clone();
    tokio::spawn(async move {
        let seed = match p.latest(&inst, kind).await {
            Ok(seed) => seed,
            Err(e) => {
                tracing::debug!(instrument = %inst, error = %e,
                    "latest() fetch failed; no live seed");
                None
            }
        };
        // Receiver may already be gone (fast teardown / live won the race);
        // a failed send is expected and ignored.
        let _ = seed_tx.send(seed);
    });
    Some(seed_rx)
} else {
    None
};
```

The spawned task borrows nothing from the controller and holds only an
`Arc<dyn Provider>` clone; if the session tears down first, the `oneshot` send
fails silently and the task drops.

**Controller state.** Add one field:

```rust
/// True once any data event (Trade/Quote/Bar) has been delivered on this
/// authoritative session. Gates the pure-live "latest value" seed: once a real
/// live value has been delivered, a late-arriving seed is discarded.
data_forwarded: bool,
```

Initialized `false` in `create_authoritative`. Set `true` inside `forward` when
the event is a data event:

```rust
if matches!(ev, MarketEvent::Trade(_) | MarketEvent::Quote(_) | MarketEvent::Bar(_)) {
    self.data_forwarded = true;
}
```

(Placed after stamping, alongside the existing accounting in `forward`. The
pure-live data path only delivers through `forward`; `emit` is used by the
historical/backfill segments, which never coexist with a seed.)

**`run_live` gains the seed receiver and a guarded select branch.**

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
    // ... existing preamble + backfill ...
    loop {
        if self.live_should_teardown() { break; }
        tokio::select! {
            cmd = cmd_rx.recv() => { /* unchanged */ }
            id  = remove_rx.recv() => { /* unchanged */ }
            ev  = provider_rx.recv() => { /* unchanged: self.forward(ev) or break */ }
            res = async { seed_rx.as_mut().unwrap().await }, if seed_rx.is_some() => {
                seed_rx = None; // fire once, then disable the branch
                if let Ok(Some(seed)) = res {
                    if !self.data_forwarded {
                        // Stamp (after any connect control), tee to tap log,
                        // fan out. Sets data_forwarded so nothing else seeds.
                        self.forward(seed).await;
                    }
                    // else: a live data event already won — discard.
                }
                // Err(RecvError) / Ok(None): nothing to seed.
            }
        }
    }
    self.teardown_upstream(&live).await;
}
```

`select!` completes the seed future before running the handler, so the
`seed_rx.as_mut().unwrap()` borrow ends before `seed_rx = None`. The `, if
seed_rx.is_some()` guard means the branch is never polled once disabled.

The single call site (`create_authoritative`) passes `seed_rx`; the
`tokio::spawn(controller.run_live(...))` argument list gains the new parameter.

### Race analysis

The `run_live` loop is the single writer and the single point of decision.
`tokio::select!` picks a ready branch (unbiased). Enumerating the orderings:

- **Seed ready, no data yet** → seed forwarded as first data event (seq lands
  after any `ProviderConnected` control already forwarded). ✅ immediate feedback.
- **Data event ready first** → `forward` sets `data_forwarded = true`; when the
  seed branch later fires it is discarded. ✅ live wins.
- **`ProviderConnected` control ready first** → forwarded; `data_forwarded`
  stays `false`; seed still injected after it. ✅ (Decision 4).
- **Seed resolves to `None` / `Err`** → no-op. ✅
- **Teardown before seed resolves** → loop exits; `oneshot` send fails; task
  drops. ✅

There is no shared mutable state between the fetch task and the controller — the
`oneshot` is the only channel, and inject/discard is decided entirely inside the
single-writer loop.

### Provider impls

**`datamancer/src/providers/alpaca.rs`** — override `latest`:

- Require the market-data REST client (same guard as `fetch_history`; missing
  credentials → `Error::Provider`, surfaced as a debug-logged `None` by the
  spawn wrapper).
- `rest.stock_snapshot(symbol).execute().await` → `StockSnapshot { latest_trade,
  latest_quote, minute_bar, daily_bar, prev_daily_bar }`.
- Map by `kind`, reusing the existing `translate_*` shapes / `Seq(0)` placeholder
  and `rx = wall_clock_ts()`:
  - `EventKind::Trade`  → `latest_trade`  → `MarketEvent::Trade`
  - `EventKind::Quote`  → `latest_quote`  → `MarketEvent::Quote`
  - `EventKind::Bar(OneMinute)` → `minute_bar` → `MarketEvent::Bar`
  - `EventKind::Bar(OneDay)`    → `daily_bar`  → `MarketEvent::Bar`
  - any other `Bar(_)` → `Ok(None)` (already excluded by `supports`).
- A `None` field (e.g. no recent trade) → `Ok(None)`.

**`datamancer/src/providers/alpaca_crypto.rs`** — override `latest`:

- `rest.crypto_snapshots(&[symbol], loc).await` → `HashMap<String,
  CryptoSnapshot>`; take the entry for `symbol`.
- Same `kind` → field mapping as stock, using the crypto `translate_*` shapes.
- Missing client / missing symbol / missing field → `Ok(None)`.

Neither impl touches the historical cache; the seed is a direct provider REST
call, orthogonal to `PersistenceOptions`.

## Testing

**Core (`datamancer/tests/` or in-crate)** with a controllable fake provider:

1. **Seed wins / ordering** — fake `latest` returns a known `Trade`; fake
   `start_live` emits `ProviderConnected` then withholds data. Assert the
   consumer sees the connect control, then the seed trade, then live ticks, with
   monotonic `seq`.
2. **Live wins / discard** — fake `start_live` emits a data event immediately;
   `latest` returns after a delay. Assert the seed never appears and `seq` has no
   gap from a discarded stamp (discard happens before `forward`, so no seq is
   consumed).
3. **Default `latest` → None** — a fake that does not override `latest`. Assert
   the stream is byte-identical to today (no seed).
4. **No seed under backfill** — `Scope::Live { backfill_from: Some(t) }`. Assert
   `latest` is never called and behavior is unchanged.
5. **Tap-log fidelity** — with a tap log configured, assert the seed is recorded
   (delivered via `forward`) with its canonical `seq`.

**Alpaca** — snapshot→event mapping unit test over a deserialized
`StockSnapshot` / `CryptoSnapshot` fixture (per `kind`, including `None` fields).
Real-endpoint coverage behind `#[ignore]`, alongside `alpaca_real.rs`.

## Rollout / gates

- Run the CI gates locally before the PR: `cargo deny check` and
  `.github/scripts/semver-checks.sh origin/main`.
- Expect a **minor** `datamancer-core` bump for the new provided method;
  `datamancer` bumps with it. No `datamancer-client` / `datamancerd` protocol
  change (no wire/control-vocabulary change), so those do not bump in lockstep
  for this change.
- `#![forbid(unsafe_code)]` unaffected; no new deps.

## Risks

- **Duplicate first value.** The seed can repeat the first live tick under a
  different `seq`. Accepted: UI paints immediately and corrects on the next tick.
- **Stale seed.** The snapshot may be a few seconds old (off-hours: last close).
  Accepted — it is explicitly a "latest known value," and live overwrites it.
- **Extra REST call per pure-live open.** One additional cold-boundary request
  per authoritative session (not per referrer — shared sessions reuse the same
  authoritative controller, which fetches once). Negligible.
