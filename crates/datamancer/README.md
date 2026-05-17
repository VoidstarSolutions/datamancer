# Datamancer

A unified subscription and replay layer for financial market data. Datamancer talks to whatever providers it's configured against, normalizes their messages into typed events, and produces a single ordered event stream that downstream consumers (analysis engines, persistence sinks, UIs) consume without caring which provider any given event came from.

## Status and Scope

Datamancer is an early-stage open-source library. The public API is still co-evolving with its first real consumers, and breaking changes should be expected until that surface stabilizes.

The workspace currently holds two crates — `datamancer-core` (types and trait surface) and `datamancer` (the session orchestrator, plus provider and storage backends behind cargo features). Provider integrations and persistence backends are expected to split into their own sibling crates once the boundaries are obvious from working code; until that split is motivated by real coupling pain, they live in `datamancer` behind features and grow organically. Consumers bring in `datamancer` plus the providers and persistence backends they actually need.

The first supported provider is Alpaca. Provider integration is meant to be additive: adding a second provider should not require changing any consumer code.

## What Datamancer Does

- **Provider integration.** Per-provider transports (websocket for live, REST for historical), authentication, rate-limit handling, and reconnect logic, isolated behind a unified surface.
- **Typed event production.** Provider-native messages are converted into datamancer's public event types. Consumers never see provider-specific shapes.
- **Subscription management.** A live session's subscription set is mutable at runtime: instruments and event kinds can be added or removed without tearing down the underlying connection.
- **Historical fetch.** Pulling bar (and eventually trade and quote) history for an instrument set over a date range, with pagination and rate-limit handling abstracted away.
- **Replay.** Presenting historical or persisted data as an ordered event stream that is indistinguishable in shape from a live stream. Replay always runs as fast as the consumer can drain it.
- **Stitched streams.** "Backfill the last N days, then continue with live" is a first-class operation, not something the consumer assembles by hand.
- **Connectivity reporting.** Gaps, reconnects, subscription state changes, and provider errors are reported in-band as event-stream entries, not via side channels.
- **Persistence (in scope, deferred).** A live tap log of received events, a local cache of historical fetches, and replay from either of those, all eventually first-class. Not yet implemented; the API is being kept free of choices that would preclude these later.

## What Datamancer Does Not Do

- **Per-instrument demultiplexing.** Datamancer emits one ordered stream of events; consumers that want per-instrument streams demux downstream.
- **Semantic enrichment.** No "join this trade with the most recent quote to compute the trade side." Datamancer surfaces the events; analysis on top of them belongs to consumers.
- **Provider-side time reordering.** Events are emitted in the order they were received, not re-sorted by source timestamp. Consumers that need strict timestamp ordering buffer themselves.
- **Throttled or wall-clock-paced replay.** Replay produces events as fast as the consumer drains. Modeling latency or simulating real-time pacing is a research-tool concern, not a data-layer one.

## Event Model

Datamancer's public output is a stream of `MarketEvent`. Variants currently planned:

- `Trade { instrument, source_ts, rx_ts, seq, price, size }`
- `Bar { instrument, interval, source_ts, rx_ts, seq, open, high, low, close, volume }`
- `Quote { instrument, source_ts, rx_ts, seq, bid, ask, ... }`
- `Control(SessionEvent)` — connectivity, subscription state, gap notifications

Every data variant carries three timestamp/identifier fields, with distinct roles that should not be conflated:

- **`source_ts`** — the timestamp the provider reported for the event. Source of truth for "when did this happen in the market" and the **only** timestamp engine logic should reason about. Sourced verbatim from provider data; never assigned by datamancer.
- **`seq: u64`** — a session-monotonic sequence number assigned by datamancer at receipt. **The sole ordering field** for the stream. Live mode assigns `seq` in arrival order, so replaying in `seq` order reproduces the consumer's original experience exactly. Historical fetch assigns `seq` in source-timestamp order during fetch, so `seq` order matches market order. Persistence sinks use `seq` gaps to detect drops.
- **`rx_ts`** — wall-clock at the moment the bytes were received from the provider, captured pre-parse. **Observability only.** Used for measuring provider-to-engine latency (`rx_ts - source_ts`), correlating engine state with external wall-clock events (logs, traces, debugger sessions), and operational monitoring. **Engine decision logic must never depend on `rx_ts`** — doing so re-introduces wall-clock as a determinism hazard. For replay-from-historical-fetch, where there is no live arrival to record, `rx_ts` collapses to `source_ts`.

`Control` events ride the same stream as data events because connectivity changes are part of the session's truth: a gap can invalidate downstream signals, and forcing consumers to acknowledge it in-band is safer than offering it as a separate stream they may forget to subscribe to.

## Sessions

A session is the unit of consumption. Three constructors, all returning the same `Session` type:

```rust
let live = datamancer.live(LiveConfig { providers, credentials, ... })?;
let backtest = datamancer.replay(ReplayConfig { source, instruments, range })?;
let warm_start = datamancer.stitched(StitchConfig { backfill_from, ... })?;
```

A `Session` exposes:

- `events()` — the single output stream (`Stream<Item = MarketEvent>`).
- `subscribe(Subscription)` / `unsubscribe(Subscription)` — mutate the active subscription set. Live and stitched sessions accept these throughout their lifetime; replay sessions fix the subscription set at construction (the subscription set is part of what defines a reproducible analysis).
- `close()` — explicit shutdown.

The choice of explicit `close` over reference-counted lifetime keeps subscription teardown visible in code, which matters once persistence is wired up and shutdown order affects whether buffered events make it to disk.

## Subscriptions

A subscription is `(instrument, set-of-event-kinds)`:

```rust
session.subscribe(Subscription {
    instrument: Instrument::from("AAPL"),
    kinds: [EventKind::Trade, EventKind::Quote].into(),
}).await?;
```

Subscriptions accumulate; the single output stream multiplexes everything that has been requested. Adding the same instrument with a new event kind extends the existing subscription rather than duplicating it.

## Configuration

A `LiveConfig` covers:

- Provider selection and credentials (secrets handling — env, file, or OS keychain — is an implementation choice deferred until there's a second provider to motivate it).
- Per-instrument provider mapping, once more than one provider is supported.
- Reconnect and retry policy.
- Buffer sizes and backpressure behavior.

A `ReplayConfig` covers:

- The replay source (historical fetch from a provider, a local tap log, or a local fetch cache once persistence lands).
- The instrument set and event-kind selection.
- The date range.

A `StitchConfig` is essentially a `ReplayConfig` for the backfill window plus a `LiveConfig` for the tail, with datamancer responsible for handling the seam (and reporting any gap or overlap as a `Control` event).

## Instrument Identity

`Instrument` is an opaque newtype wrapping a symbol string for now. Asset class, exchange, contract specification, and other structured fields will be added when there is a real cross-provider or non-equity use case driving them. Keeping the type opaque from day one means callers won't need to be revised when that growth happens.

## Persistence (Future)

When persistence lands, it will take three forms:

- **Live tap log** — append-only record of every event received in a live session, capturing both `rx_ts` and `seq` so that replay reproduces the engine's experience exactly.
- **Historical fetch cache** — canonical store of historical data keyed by `(provider, instrument, granularity, range)`, so re-running a research job does not re-hit the provider.
- **Local replay source** — `replay()` accepts either of the above as a source.

The session API is being kept free of design choices that would preclude transparently teeing a live session to a tap log, or transparently serving a historical fetch from a local cache.

## Non-goals

- A trading or analysis framework. Datamancer produces events; what to do with them is the consumer's problem.
- A storage engine in its own right. The persistence flavors above are about preserving and replaying datamancer's output, not about providing a general-purpose time-series store.
- Cross-provider event reconciliation or canonicalization beyond shape (e.g., no attempt to reconcile a trade reported by two venues into a single canonical trade).

## License

To be determined. Datamancer will be released under a permissive open-source license once one is selected.
