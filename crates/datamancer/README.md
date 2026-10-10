# Datamancer

A unified subscription and replay layer for financial market data. Datamancer talks to whatever providers it's configured against, normalizes their messages into typed events, and presents them through a multiplexed client-session stream that downstream consumers (analysis engines, persistence sinks, UIs) consume without caring which provider any given event came from. Ordering is **per symbol** — each instrument's substream is a source-stamped within-instrument total order (`(instrument, seq)`); across instruments the multiplex interleaves in arrival order rather than computing a global order.

## Status and Scope

Datamancer is an early-stage open-source library. The public API is still co-evolving with its first real consumers, and breaking changes should be expected until that surface stabilizes.

The workspace holds eight crates. `datamancer-core` carries the types and trait surface; `datamancer` is the session orchestrator, with provider and storage backends behind cargo features. The transports (`datamancer-transport-iceoryx2`, `datamancer-transport-ws`), the consumer-side client (`datamancer-client`), credential storage (`datamancer-credentials`), the Windows security primitives (`datamancer-winsec`), and the server binary (`datamancerd`) have each split out into their own crate as the boundary became obvious from working code. Provider integrations and storage backends have **not** split yet — they still live in `datamancer` behind features, and will move when real coupling pain motivates it. Consumers bring in `datamancer` plus the providers and persistence backends they actually need; separate consumer *processes* depend on `datamancer-client` instead and never link the orchestrator.

The supported providers are Alpaca equities and Alpaca crypto. Provider integration is additive: adding a provider does not require changing any consumer code. An IBKR provider is reserved in the core wire types (`ProviderCredentials::Gateway`, `ProviderState::CompanionUnreachable`, `DisconnectCause::CompanionUnreachable`) but is **not implemented**.

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

Datamancer's public output is a stream of `MarketEvent` (`#[non_exhaustive]`):

- `Trade { instrument, source_ts, rx_ts, seq, price, size }`
- `Bar { instrument, interval, source_ts, rx_ts, seq, open, high, low, close, volume }`
- `Quote { instrument, source_ts, rx_ts, seq, bid, ask, … }`
- `Control(Control)` — connectivity, subscription state, gap notifications, session close

`EventKind` is the subscription selector and maps 1:1 onto the data variants: `Trade`,
`Quote`, and `Bar(BarInterval)` over the six intervals (`OneSecond`, `OneMinute`,
`FiveMinute`, `FifteenMinute`, `OneHour`, `OneDay`). `EventKind::enumerate()` walks the
whole finite kind space, which is what makes per-instrument capability discovery possible.

> `EventKind`, `BarInterval`, and `Surface` are deliberately **not** `#[non_exhaustive]`,
> so adding an interval or a kind is a declared breaking change that every provider must
> consciously answer. Prices and sizes are fixed-point (`Price`, `Quantity`) — `1e-9`
> scaled `i64`/`u64` on the wire, not floats.

Every data variant carries three timestamp/identifier fields, with distinct roles that should not be conflated:

- **`source_ts`** — the timestamp the provider reported for the event. Source of truth for "when did this happen in the market" and the **only** timestamp engine logic should reason about. Sourced verbatim from provider data; never assigned by datamancer.
- **`seq: u64`** — a per-symbol ordering number stamped **once at the source** of the authoritative per-`(instrument, kind)` stream, in canonical delivery order, before any sink — so it is identical across all consumers of that symbol (not a per-consumer poll artifact). **The sole ordering field** for the stream, and per-symbol only (there is no cross-instrument order; the multiplex key is `(instrument, seq)`). Live mode stamps `seq` in arrival order, so replaying a symbol's substream in `seq` order reproduces that substream exactly (per-symbol; not the cross-symbol interleave of the multiplexed stream). Historical fetch stamps `seq` in source-timestamp order during fetch, so `seq` order matches market order. The delivered stream is contiguous *only while nothing is lost*: a consumer that misses events (resume-buffer eviction, late join) sees a real `seq` hole, surfaced in-band as a `Control::Gap`.
- **`rx_ts`** — wall-clock at the moment the bytes were received from the provider, captured pre-parse. **Observability only.** Used for measuring provider-to-engine latency (`rx_ts - source_ts`), correlating engine state with external wall-clock events (logs, traces, debugger sessions), and operational monitoring. **Engine decision logic must never depend on `rx_ts`** — doing so re-introduces wall-clock as a determinism hazard. For replay-from-historical-fetch, where there is no live arrival to record, `rx_ts` collapses to `source_ts`.

`Control` events ride the same stream as data events because connectivity changes are part of the session's truth: a gap can invalidate downstream signals, and forcing consumers to acknowledge it in-band is safer than offering it as a separate stream they may forget to subscribe to.

## Sessions

There are two consumption handles, both fed by the same authoritative machinery.

**`Session`** is the single-pair case — one `(instrument, kind)`. It is opened from a built
`Datamancer`, and what would once have been three separate constructors is one call whose
`Scope` argument selects the shape:

```rust
let dm = Datamancer::builder().provider_arc(provider).build()?;

// bounded replay: the stream completes when `to` is reached
let backtest = dm.session(instrument.clone(), kind, Scope::Historical { from, to },
                          PersistenceOptions::cached()).await?;

// pure live, from "now"
let live = dm.session(instrument.clone(), kind, Scope::Live { backfill_from: None },
                      PersistenceOptions::cached_with_tap()).await?;

// stitched: backfill from `t` to the live edge, then seam into the live tail
let warm_start = dm.session(instrument, kind, Scope::Live { backfill_from: Some(t) },
                            PersistenceOptions::cached()).await?;
```

Opening is **eager** — the live subscription or historical fetch begins before the call
returns. A second live open for the same pair *shares* the authoritative session rather
than conflicting with it.

**`ClientSession`** (`dm.client_session()`) is the primary consumer handle: it holds a
mutable `(instrument, kind)` subscription set and presents one multiplexed stream over all
of it. `Session`'s live path is a referrer onto the same shared authoritative sessions that
back `ClientSession`.

Both expose:

- `events()` — the output stream (`Stream<Item = MarketEvent>`). Multi-shot: dropping and
  re-taking it is the resume primitive, and events missed in between surface as a
  `Control::Gap` rather than vanishing.
- `close()` — explicit shutdown.

`ClientSession` additionally exposes `subscribe` / `unsubscribe` to mutate its set at
runtime without tearing down the underlying connection.

The choice of explicit `close` over reference-counted lifetime keeps subscription teardown visible in code, which matters once persistence is wired up and shutdown order affects whether buffered events make it to disk.

## The `Provider` trait

`Provider` is the extension point; adding a source is purely additive at the consumer
layer. Dynamic dispatch lives at the **cold** boundary (start, subscribe, history fetch) —
a provider's per-message decode loop stays monomorphic behind its own concrete
`mpsc::Sender<MarketEvent>`.

| Method | Role |
| --- | --- |
| `id()` | stable provider id, used in config, control events, and storage keys |
| `supports(instrument, kind, surface)` | capability predicate, answered **per `Surface`** |
| `start_live(sink)` | open a streaming subscription, returning a `LiveHandle` |
| `fetch_history(request, sink)` | serve a bounded range in source-timestamp order |
| `list_instruments()` | bulk catalog for the instrument picker (default: empty) |
| `capabilities(instrument)` | on-demand per-contract lookup (default: `None`) |
| `latest(instrument, kind)` | one-shot most-recent value, to seed a live subscription (default: `None`) |
| `metrics()` | optional byte / rate-limit counters from inside the decode loop |
| `enabled()` | whether a runtime settings source has this provider parked |

`Surface::{Live, History}` is the axis that keeps the two data paths honest. They genuinely
differ and neither contains the other — Alpaca's equity websocket streams only minute and
daily bars while its REST endpoint serves five intervals. Collapsing them into one
predicate makes a provider either over-promise a backfill it cannot serve or reject a
request it could have served; datamancer shipped both bugs before this axis existed.

Capability answers are **best-effort and may be partial**: an absent field means
*unknown*, never *unsupported*.

## Subscriptions

A subscription is one `(instrument, kind)` pair, added to a `ClientSession`'s set:

```rust
let aapl = Instrument::new("alpaca", AssetClass::Equity, "AAPL");

let mut client = dm.client_session();
client.subscribe(aapl.clone(), EventKind::Trade, scope, options).await?;
client.subscribe(aapl, EventKind::Quote, scope, options).await?;
```

Subscriptions accumulate; the client session's multiplexed stream **interleaves** everything that has been requested — per-symbol deterministic (`(instrument, seq)`, source-stamped within each instrument), arrival-order across symbols, never globally merge-sorted. Each `(instrument, kind)` pair is backed by a refcounted shared **authoritative session**, so two consumers of the same pair observe identical `(seq, source_ts)`. Adding the same instrument with a new event kind extends the subscription set rather than duplicating it.

## Configuration

There is no monolithic session config. What a session needs is split across three places:

**The builder** (`DatamancerBuilder`) registers providers, the optional `HistoricalCache`
and `TapLog`, and process-wide knobs such as the per-client resume-buffer size.

**Per-session arguments** — `Scope` and `PersistenceOptions` — are passed at `session()` /
`subscribe()` time, so one `Datamancer` serves bounded-replay, pure-live, and stitched
consumers simultaneously. Datamancer owns the backfill→live seam and reports any gap or
overlap at it as a `Control` event.

**Per-provider configuration** is the provider's own struct, carrying two hot-reloadable
sources rather than being fixed at build time:

- `SettingsSource<T>` — `Static(T)` or `Watch(rx)`. `Watch(None)` parks a compiled-in
  provider *disabled* without tearing it down, so enabling or disabling it is a settings
  hot-apply rather than a restart. This is what `datamancerd`'s `configure-provider` /
  `remove-provider` ops drive.
- `CredentialsSource` — `Env` (the deprecated legacy `ALPACA_*` variables), `Static`, or
  `Watch`. `datamancerd` wires `Watch` to its credential broker (`datamancer-credentials`:
  OS keychain / secret-service with a locked-down file fallback), so `set-credentials`
  hot-applies to a running provider.

Keeping these on the provider rather than on the builder is deliberate: the orchestrator
never gains a credential-source API, and a provider crate stays depending on
`datamancer-core` alone.

## Instrument Identity

`Instrument` is the qualifying tuple `(provider, asset_class, symbol)`. The triple is what
makes the id unique across the union of all sources: the same ticker can name an equity and
an ETF, and the same crypto pair trades on several venues. Symbol *grammar* stays
provider-specific (`"AAPL"` on Alpaca equities, `"BTC/USD"` on Alpaca crypto), and engine
code holding an `Instrument` can round-trip back to the right provider with no external
lookup.

Beyond that triple the type stays **opaque**. Exchange, contract specification, expiry,
multiplier, and the like are not fields — a provider that needs them resolves
symbol→contract *inside itself*, which is what keeps output source-agnostic. `AssetClass`
is `#[non_exhaustive]` (`Equity`, `Etf`, `Crypto` today) so new classes are additive.

This is a live constraint rather than a settled one: a structured-contract provider such as
IBKR is the use case most likely to force the question, and the recorded position is that
`Instrument` stays opaque until a real cross-provider collision forces it, with a
provider-qualified instrument namespace as the likely eventual shape.

## Persistence — Historical Cache

Datamancer can back a historical session with a `HistoricalCache` (the bundled
`TursoCache` stores to a Turso/SQLite-compatible database file on disk, or
in-memory for tests). Caching is controlled per-session by `PersistenceOptions`:

| `read_cache` | `write_cache` | mode      | behavior                                        |
|--------------|---------------|-----------|-------------------------------------------------|
| `false`      | `false`       | ephemeral | always fetch from the provider, store nothing   |
| `true`       | `true`        | cached    | serve covered ranges, fetch & store only gaps   |
| `true`       | `false`       | read-only | serve cache + fetch gaps, don't persist them    |
| `false`      | `true`        | refresh   | ignore coverage, re-fetch the range, overwrite  |

```rust
let dm = Datamancer::builder()
    .provider_arc(provider)
    .historical_cache(Box::new(TursoCache::open(cfg).await?))
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

## Introspection

`Datamancer::snapshot()` (async, fallible) returns a `SystemSnapshot`: a
consolidated, `Serialize + Deserialize` view of runtime state, with no
transport or daemon. It composes three things:

- **Provider accounting** (`ProviderSnapshot`) — per-provider counters:
  `history_fetches` (counted per gap *segment*, not per `session()` call),
  `history_fetch_coalesced` (single-flight dedups; backfill bypasses the
  coalescer and never counts here), `live_starts`, `subscribes`/`unsubscribes`
  (call counts, **not** active-subscription deltas — stock subscribe is a
  full-snapshot and reconnect re-applies the full list), `active_subscriptions`
  (the live substreams currently requested: this provider's authoritative
  sessions in the registry at assembly), `reconnects`, `connection_state`,
  `gaps_emitted`, `last_error`, and `messages` (live data forwarded to
  consumers only — cache-replay/backfill is not provider traffic). `bytes` and
  `rate_limit_hits` are `Option` and stay `None` until a provider implements
  the optional `Provider::metrics()` hook.
- **Cache catalog** (`CacheSnapshot.entries`, via `HistoricalCache::catalog()`)
  — every stored `(provider, symbol, kind, adjustment)` key with its actual
  covered segments and a *logical* volume estimate (`event_count ×
  bytes_per_row`; it ignores index/MVCC overhead). The catalog reports the
  adjustment rows are **stored** under, so trades/quotes always read `Raw`
  regardless of the requested mode. It carries no `seq` (seq is a live,
  per-symbol property, not a cache property).
- **Live state** — per-`(instrument, kind)` `AuthoritativeSessionSnapshot`
  (subscriber refcount, last source/rx timestamps, `latency_ns =
  rx_ts − source_ts`, per-symbol gap count, seq position, the substream's own
  `connection` phase — `pending`/`up`/`down`, each with the receipt time of the
  control that entered it — and `last_error_rx_ts`) and per-client
  `ClientSessionSnapshot` (subscriptions + resume-buffer occupancy/drops).
  The timestamps, `latency_ns` and the provider's `messages` describe live
  arrivals only: the pure-live latest-value seed is delivered, teed and takes
  a `seq` (so it moves the seq position), but it is a `Provider::latest`
  snapshot, not live traffic, so a stream that has seen only its seed has no
  timestamps or latency yet and its health reads `Idle`.

The snapshot is **sampled, not transactional**: per-symbol fields are read from
`Relaxed` atomics and the session registry lock is held only to clone handles
(never across an `.await`), so fields may skew by nanoseconds across symbols —
fine, because determinism is per-symbol. `latency_ns`/`rx_ts` are
**observability only** and must never feed engine logic.

## Transports

By default a `Session`'s events are consumed in-process. The optional
`transport-iceoryx2` feature adds a **same-host, zero-copy** transport
(`datamancer::transport`, the `datamancer-transport-iceoryx2` crate) that
carries a client's multiplexed stream to a separate consumer process. Two planes
ride one logical client connection:

- **Data plane** — one iceoryx2 pub-sub service per client carrying that client's
  multiplexed `(instrument, seq)` interleave as a flat `#[repr(C)]` POD
  `DataPayload`. The payload carries a compact, **sink-local** `SymbolId` instead
  of the heap-backed `Instrument`; a low-rate per-client *announcement* service
  publishes the `SymbolId → Instrument` mapping. `SymbolId`/interning are a
  transport compaction handle only — **not** a public-API or global-identity
  concept (two clients may map the same id to different instruments). The data
  plane carries the per-symbol-deterministic interleave and makes **no
  cross-symbol ordering claim** — the multiplex is an interleave, never a global
  merge-sort.
- **Diagnostics plane** — a separate service publishing the serialized
  `SystemSnapshot` (provider health/connectivity, cache catalog, live state).
  Connection-scoped controls (`ProviderConnected`/`Disconnected`/`ProviderError`)
  are **suppressed** on the data plane and surface here instead; remote consumers
  read provider connectivity + last-error from `ProviderSnapshot`. Per-symbol
  controls (`Gap`, `SubscriptionChanged`) and `SessionClosing` still ride the
  data plane.

The POD payload preserves the timestamp triple end-to-end — `rx_ts` stays
**observability-only** and is never reconstructed/synthesized by the subscriber.

### WebSocket transport

The optional `transport-ws` feature (`datamancer::transport_ws`, the
`datamancer-transport-ws` crate) is the second worked example of the same seam: a
network-reachable transport where one connection is one client, carrying JSON control and
event frames. It does **no** symbol interning — the `Instrument` rides inline on every
frame — and it is not zero-copy. Prices and sizes stay fixed-point `i64`/`u64` on the wire,
so a consumer that parses them as IEEE doubles silently corrupts values.

It exists for two reasons: remote consumers, and Windows, where iceoryx2's shared memory is
not viable and WS-over-loopback carries the data plane instead. Between them, the two
transports are the input to a future unified client-transport trait; `datamancer-client`
already presents both behind one generic `Client`.

### Standalone server

The library stays primary: embedders that want zero hops consume a `Session` /
`ClientSession` in-process. The `datamancerd` crate is the **thin standalone
wrapper** — a same-host daemon that builds a `Datamancer` from a TOML config,
serves multiple consumer processes (one iceoryx2 data-plane service per client),
holds authoritative sessions alive as the cross-process lifecycle anchor, and
exposes a Unix-socket + newline-JSON control surface. It adds no new semantics;
see `crates/datamancerd/README.md`.

**Subscriber rule.** The data and announcement services are two independent
iceoryx2 services with **no mutual delivery-order guarantee**: a data sample can
arrive before the `SymbolAnnouncement` for its `SymbolId`. The subscriber helper
(`DataSubscriber`/`HoldBuffer`) therefore **holds** an unresolved sample and
replays it once the announcement resolves it — never dropping or erroring.

**Flush / shutdown ordering** (load-bearing): **tap-log flush before sink flush
before service drop**. The sink never drops samples that `flush` promised to
deliver, but makes no guarantee a crashed/slow subscriber consumed them
(same-host best-effort; cross-process backpressure is a recorded deferral).

## Non-goals

- A trading or analysis framework. Datamancer produces events; what to do with them is the consumer's problem.
- A storage engine in its own right. The persistence flavors above are about preserving and replaying datamancer's output, not about providing a general-purpose time-series store.
- Cross-provider event reconciliation or canonicalization beyond shape (e.g., no attempt to reconcile a trade reported by two venues into a single canonical trade).

## License

To be determined. Datamancer will be released under a permissive open-source license once one is selected.
