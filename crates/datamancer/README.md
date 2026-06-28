# Datamancer

A unified subscription and replay layer for financial market data. Datamancer talks to whatever providers it's configured against, normalizes their messages into typed events, and produces a single ordered event stream that downstream consumers (analysis engines, persistence sinks, UIs) consume without caring which provider any given event came from.

## Status and Scope

Datamancer is an early-stage open-source library. The public API is still co-evolving with its first real consumers, and breaking changes should be expected until that surface stabilizes.

The workspace currently holds two crates — `datamancer-core` (types and trait surface) and `datamancer` (the session orchestrator, plus provider and storage backends behind cargo features). Provider integrations and persistence backends are expected to split into their own sibling crates once the boundaries are obvious from working code; until that split is motivated by real coupling pain, they live in `datamancer` behind features and grow organically. Consumers bring in `datamancer` plus the providers and persistence backends they actually need.

The first supported provider is Alpaca. Provider integration is meant to be additive: adding a second provider should not require changing any consumer code.

## What Datamancer Does

- **Provider integration.** Per-provider transports (websocket for live, REST for historical), authentication, rate-limit handling, and reconnect logic, isolated behind a unified surface.
- **Typed event production.** Provider-native messages are converted into datamancer's public event types. Consumers never see provider-specific shapes.
- **Subscription management (roadmap).** A live session is currently scoped to a single `(instrument, kind)` pair, fixed at construction. Making a session's subscription set mutable at runtime — adding or removing instruments and event kinds without tearing down the underlying connection — is planned (see [Subscriptions](#subscriptions-roadmap)).
- **Historical fetch.** Pulling bar (and eventually trade and quote) history for an instrument set over a date range, with pagination and rate-limit handling abstracted away.
- **Replay.** Presenting historical or persisted data as an ordered event stream that is indistinguishable in shape from a live stream. Replay always runs as fast as the consumer can drain it.
- **Stitched streams.** "Backfill the last N days, then continue with live" is a first-class operation, not something the consumer assembles by hand.
- **Connectivity reporting.** Gaps, reconnects, subscription state changes, and provider errors are reported in-band as event-stream entries, not via side channels.
- **Persistence.** A live tap log of received events and a local cache of historical fetches are implemented and first-class. Replay from a tap log is in scope; the session API is kept free of choices that would preclude it.

## What Datamancer Does Not Do

- **Per-instrument demultiplexing.** Datamancer emits one ordered stream of events; consumers that want per-instrument streams demux downstream.
- **Semantic enrichment.** No "join this trade with the most recent quote to compute the trade side." Datamancer surfaces the events; analysis on top of them belongs to consumers.
- **Provider-side time reordering.** Events are emitted in the order they were received, not re-sorted by source timestamp. Consumers that need strict timestamp ordering buffer themselves.
- **Throttled or wall-clock-paced replay.** Replay produces events as fast as the consumer drains. Modeling latency or simulating real-time pacing is a research-tool concern, not a data-layer one.

## Event Model

Datamancer's public output is a stream of `MarketEvent`. The variants:

- `Trade { instrument, source_ts, rx_ts, seq, price, size }`
- `Bar { instrument, interval, source_ts, rx_ts, seq, open, high, low, close, volume }`
- `Quote { instrument, source_ts, rx_ts, seq, bid, bid_size, ask, ask_size }`
- `Control(Control)` — connectivity, subscription state, and gap notifications, carried as a `ControlKind`

Every data variant carries three timestamp/identifier fields, with distinct roles that should not be conflated:

- **`source_ts`** — the timestamp the provider reported for the event. Source of truth for "when did this happen in the market" and the **only** timestamp engine logic should reason about. Sourced verbatim from provider data; never assigned by datamancer.
- **`seq: u64`** — a session-monotonic sequence number stamped by datamancer at delivery into the consumer stream. **The sole ordering field** for the stream. Live mode stamps `seq` in arrival order, so replaying in `seq` order reproduces the consumer's original experience exactly. Historical fetch stamps `seq` in source-timestamp order, so `seq` order matches market order. `seq` is contiguous by construction and carries no drop-detection role: datamancer numbers only the events it delivers, so a provider-side drop is never a hole in `seq`. Real gaps are a `source_ts`/coverage concept, surfaced in-band as `Control::Gap` events.
- **`rx_ts`** — wall-clock at the moment the bytes were received from the provider, captured pre-parse. **Observability only.** Used for measuring provider-to-engine latency (`rx_ts - source_ts`), correlating engine state with external wall-clock events (logs, traces, debugger sessions), and operational monitoring. **Engine decision logic must never depend on `rx_ts`** — doing so re-introduces wall-clock as a determinism hazard. For replay-from-historical-fetch, where there is no live arrival to record, `rx_ts` collapses to `source_ts`.

`Control` events ride the same stream as data events because connectivity changes are part of the session's truth: a gap can invalidate downstream signals, and forcing consumers to acknowledge it in-band is safer than offering it as a separate stream they may forget to subscribe to.

## Sessions

A session is the unit of consumption. There is one constructor; the kind of
session is selected by the `Scope` argument, and every session returns the same
`Session` type:

```rust
let session = datamancer
    .session(instrument, kind, scope, PersistenceOptions::cached())
    .await?;
```

`Scope::Live { backfill_from }` opens a live session (optionally stitching
history ahead of the live tail — see [Resume](#resume)); `Scope::Historical { from, to }`
replays a bounded range. Each session is scoped to a single `(instrument, kind)`
pair, fixed at construction.

A `Session` exposes:

- `take_events()` — take the single output stream (an `EventStream`, which is
  `Stream<Item = MarketEvent>`). Live scope is multi-shot: drop the stream and
  re-take later (see [Resume](#resume)). Historical scope is single-shot.
- `set_persistence()` / `persistence()` — replace or read the session's
  `PersistenceOptions` at runtime.
- `instrument()` / `kind()` / `scope()` — inspect what the session is bound to.
- `close()` — explicit shutdown.

The choice of explicit `close` over reference-counted lifetime keeps session
teardown visible in code, which matters once persistence is wired up and shutdown
order affects whether buffered events make it to disk.

### Roadmap: multi-subscription sessions

The intended end state is a richer session API: dedicated `live` / `replay` /
`stitched` constructors taking `LiveConfig` / `ReplayConfig` / `StitchConfig`,
and a mutable, multi-instrument subscription set per session
(`subscribe` / `unsubscribe`) multiplexed into the one ordered stream. The
current one-pair-per-session model is the implemented subset; the
[Subscriptions](#subscriptions-roadmap) and [Configuration](#configuration)
sections below describe that roadmap, not today's API.

## Subscriptions (roadmap)

> **Not yet implemented.** Today a session is fixed to one `(instrument, kind)`
> pair at construction. The model below is the planned multi-subscription API.

A subscription is `(instrument, set-of-event-kinds)`:

```rust
session.subscribe(Subscription {
    instrument: Instrument::from("AAPL"),
    kinds: [EventKind::Trade, EventKind::Quote].into(),
}).await?;
```

Subscriptions accumulate; the single output stream multiplexes everything that has been requested. Adding the same instrument with a new event kind extends the existing subscription rather than duplicating it.

## Configuration

Process-wide wiring is done through `DatamancerBuilder`: registering providers
(`provider` / `provider_arc`), pinning an instrument to a specific provider
(`pin`), attaching a tap log (`tap_log`) and a historical cache
(`historical_cache`), bounding the resume buffer (`resume_buffer_events`), and
selecting the corporate-action `adjustment` mode. Per-session behavior is then
selected by the `Scope` and `PersistenceOptions` arguments to `session()`.

### Roadmap: per-session config structs

> **Not yet implemented.** The richer per-session configuration below pairs with
> the multi-subscription session API above.

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

### Single-flight fetch

Within one `Datamancer` process, at most one provider fetch is outstanding per
`CacheKey`. Concurrent `cached()` sessions requesting the same uncovered range
do not each hit the provider: the first to need a fetch takes a per-key slot
and fetches; the rest wait, then re-evaluate coverage and serve from cache what
the winner just stored (re-fetching only any still-uncovered remainder). A
cold-cache parameter sweep that opens hundreds of sessions over the same window
therefore fetches it once. This is in-process only; coordinating fetches across
processes is out of scope (see the consumer-transport design).

### Deferred

Cache **volume** is not yet bounded — a very large fetch can fill the disk; no
eviction or granularity policy exists.

See `examples/cached_history.rs` for a runnable, credential-free demo.

## Resume

Live sessions survive consumer absence. The `Session` handle is the lifecycle
anchor: hold it and the session keeps running (and recording, when
configured) whether or not a stream is attached. `take_events` is async and
multi-shot for live scope — drop the stream, re-take later, and delivery
resumes from a bounded in-memory buffer (`DatamancerBuilder::resume_buffer_events`,
default 65 536 events). If the buffer overflowed, one
in-band `Control::Gap` reports exactly the evicted span before the survivors
flow. `seq` is stamped at delivery from a counter shared across re-takes, so
the delivered stream is always contiguous — an evicted event is a reported
gap, never a `seq` hole.

`Scope::Live { backfill_from: Some(t) }` stitches history ahead of the live
tail: the window `[t, live-edge)` is served through the historical
read-through path (cache + provider gap-fetch, honoring the session's
`read_cache`/`write_cache` axes) while live arrivals buffer; the seam drains
in arrival order. Coverage for the segment touching the live edge is claimed
conservatively (history endpoints lag the live feed), so a later request
re-fetches the sliver instead of permanently masking it. The tap log captures
only the live tail — backfill data belongs to the cache.

See `examples/resume.rs` for a runnable, credential-free demo.

## Non-goals

- A trading or analysis framework. Datamancer produces events; what to do with them is the consumer's problem.
- A storage engine in its own right. The persistence flavors above are about preserving and replaying datamancer's output, not about providing a general-purpose time-series store.
- Cross-provider event reconciliation or canonicalization beyond shape (e.g., no attempt to reconcile a trade reported by two venues into a single canonical trade).

## License

To be determined. Datamancer will be released under a permissive open-source license once one is selected.
