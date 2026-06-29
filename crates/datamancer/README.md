# Datamancer

A unified subscription and replay layer for financial market data. Datamancer talks to whatever providers it's configured against, normalizes their messages into typed events, and presents them through a multiplexed client-session stream that downstream consumers (analysis engines, persistence sinks, UIs) consume without caring which provider any given event came from. Ordering is **per symbol** — each instrument's substream is a source-stamped within-instrument total order (`(instrument, seq)`); across instruments the multiplex interleaves in arrival order rather than computing a global order.

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
- **Persistence.** A live tap log of received events and a local cache of historical fetches are implemented and first-class. Replay from a tap log is in scope; the session API is kept free of choices that would preclude it.

## What Datamancer Does Not Do

- **Per-instrument demultiplexing.** A client session presents one multiplexed stream over its subscription set (per-symbol deterministic, arrival-order across symbols); consumers that want per-instrument streams demux downstream.
- **Global / cross-symbol ordering.** There is no total order across instruments. The multiplex interleaves (ordering key `(instrument, seq)`); a globally merged, cross-symbol-sorted stream is an explicit non-goal. Consumers needing strict global timestamp order buffer themselves.
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
- **`seq: u64`** — a per-symbol ordering number stamped **once at the source** of the authoritative per-`(instrument, kind)` stream, in canonical delivery order, before any sink — so it is identical across all consumers of that symbol (not a per-consumer poll artifact). **The sole ordering field** for the stream, and per-symbol only (there is no cross-instrument order; the multiplex key is `(instrument, seq)`). Live mode stamps `seq` in arrival order, so replaying in `seq` order reproduces the consumer's original experience exactly. Historical fetch stamps `seq` in source-timestamp order during fetch, so `seq` order matches market order. The delivered stream is contiguous *only while nothing is lost*: a consumer that misses events (resume-buffer eviction, late join) sees a real `seq` hole, surfaced in-band as a `Control::Gap`.
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

Subscriptions accumulate; the client session's multiplexed stream **interleaves** everything that has been requested — per-symbol deterministic (`(instrument, seq)`, source-stamped within each instrument), arrival-order across symbols, never globally merge-sorted. Each `(instrument, kind)` pair is backed by a refcounted shared **authoritative session**, so two consumers of the same pair observe identical `(seq, source_ts)`. Adding the same instrument with a new event kind extends the subscription set rather than duplicating it.

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
flow. `seq` is stamped once at the source (not per-consumer), so survivors keep
their original `seq` and an evicted event is a reported gap **and** a real `seq`
hole at the evicted span — the delivered stream is contiguous only while
nothing is lost.

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
