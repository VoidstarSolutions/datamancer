# Consumer Transport Seam + `datamancerd` — Design

**Date:** 2026-06-14
**Status:** Approved approach, pre-implementation
**Crate:** `datamancer-core` (new `EventSink` trait + `seq` invariant change),
`datamancer` (in-process sink, iceoryx2 sink behind a feature, symbol interning),
new `datamancerd` binary crate.

## Context

Datamancer is a library today: a consumer builds a `Session` in-process and
drains `events()` as a `Stream`. Two pressures motivate adding a server-shaped
deployment without abandoning the library:

1. **Decoupling execution from analysis.** These become two separate consumer
   processes. They are *mostly independent* (different subscription sets;
   analysis runs lower-frequency data than execution) but **must agree on the
   shared subset of instruments both watch** — same events, same order.
2. **Provider-health observability.** The application layer cannot currently
   surface enough about the underlying provider's health (connectivity, gaps,
   latency, reconnects) to operate it confidently.
3. **Independent deployment.** A long-lived data process that can be restarted
   and upgraded independently of any single engine.

Everything runs **same-host today.** The user is considering
[iceoryx2](https://github.com/eclipse-iceoryx/iceoryx2) (zero-copy shared-memory
pub-sub) as the consumer-facing transport to drive the fan-out hop toward zero
overhead, and needs assurance that doing so does not foreclose a future
multi-host deployment.

## The core finding: iceoryx2 does not foreclose distributed operation

Two facts must be kept separate:

- **iceoryx2 is same-host by design.** It is zero-copy shared memory; there is
  no cross-host wire in the core. Cross-host has always been a *gateway*
  pattern (a process subscribes to the local service and re-publishes over a
  network transport). A future iceoryx2 node-to-node story exists on their
  roadmap but is **not treated as load-bearing here.** Distributed operation,
  when wanted, is an additive fan-out node — adopting iceoryx2 locally does not
  prevent it.

- **Zero-copy forces a POD payload.** iceoryx2 payloads must be
  shared-memory-safe: `#[repr(C)]`, no heap, no `String`/`Vec`/`Box`. The
  public `MarketEvent` carries an `Instrument` (a `String` newtype). The
  **foreclosure risk is not the transport — it is letting that POD constraint
  leak into the public event model.**

The design rule that resolves both: **the logical `MarketEvent` (rich, owned)
stays the public contract; the POD/interned shm layout is an implementation
detail of the iceoryx2 sink.** A future network transport derives its own wire
format from the same logical type. The library API never knows which transport
it is on.

This reframes the decision. It is not *library vs server*. It is **"is the
consumer boundary a pluggable transport?"** Get that right and library,
same-host server (iceoryx2), and a future distributed fan-out node are points
on one axis, not three rewrites.

## Goals

- Introduce a single new seam — the **consumer transport** — between the
  session core and its consumers.
- Keep the in-process `Stream` consumer path working unchanged for embedders.
- Add an iceoryx2 publishing sink (behind a cargo feature) that fans one
  authoritative session out to multiple local subscribers.
- Make `seq` agreement across consumers correct by construction.
- Ship a thin `datamancerd` binary that runs a session and publishes it.
- Keep the storage-networking decision entirely out of Datamancer (the DB
  client owns local-vs-networked; Datamancer only holds a client).

## Non-goals

- **Network/cross-host transport.** Not built now. The seam must not preclude
  it; an explicit "fan-out node" sketch is recorded under Deferred, nothing
  more.
- **A storage transport owned by Datamancer.** The cache/DB layer uses a
  database whose own client handles transport (SurrealDB local or networked).
  Datamancer builds no cache wire.
- **Inverting the primary artifact.** The library stays primary; `datamancerd`
  is a thin wrapper, not a rewrite that makes embedding the exception.
- **Wall-clock-paced fan-out, per-subscriber reordering, or demux.** Unchanged
  non-goals — the server fans out the same single ordered stream.

## The layering

```
provider edge  →  session core  →  consumer transport
(hot, local,      (ordering, seq    (in-proc Stream sink
 monomorphic       stamping, subs,   OR iceoryx2 pub-sub sink)
 decode)           cache via DB
                   client)
```

Only the rightmost boundary is new. The provider edge and the cache-via-DB-client
path are untouched.

## The `EventSink` seam

A sink is where the session core hands a fully-formed, `seq`-stamped
`MarketEvent` to a transport. The current in-process delivery becomes one
implementation.

```rust
/// Receives session events in delivery order. Implementations own their
/// transport: an in-process channel, an iceoryx2 publisher, etc.
pub trait EventSink: Send + Sync {
    /// Publish one event. Called in `seq` order; the sink must preserve it.
    async fn publish(&self, ev: &MarketEvent) -> Result<()>;

    /// Flush any buffered events to the transport (shutdown ordering).
    async fn flush(&self) -> Result<()>;
}
```

- The **in-process sink** wraps the existing bounded channel / resume buffer;
  the embedder's `events()` `Stream` drains it. Behavior is byte-for-byte the
  current path.
- The **iceoryx2 sink** (feature `transport-iceoryx2`) converts the logical
  event to its POD shm layout (below) and publishes to an iceoryx2 service that
  local subscribers attach to. Multiple subscribers is native iceoryx2 pub-sub.

The session core depends only on `EventSink`. Which sink is wired is a builder
choice, exactly as `HistoricalCache` and `TapLog` are today.

> **Open question (for the implementation plan):** whether `EventSink`
> coexists with, or supersedes, the existing internal forwarding path. The
> in-process sink is expected to *wrap* today's channel rather than replace the
> resume-buffer machinery; confirm during planning that the tap-log tee and
> resume buffer sit on the core side of the sink (so every sink inherits them),
> not inside the in-process sink.

## `seq` becomes a property of the shared stream — invariant change

This is the load-bearing semantic change and must be reflected in
`datamancer-core` docs, the crate README, and the root `CLAUDE.md`.

**Today:** `seq` is stamped *per consumer at delivery*, from a counter shared
across re-takes of one session (`EventStream` stamps on poll). That model
assumes a single logical consumer.

**Problem:** execution and analysis are *separate* consumers that must agree on
the shared instrument subset. Two independently-stamping deliveries would
number the same event differently — defeating agreement.

**Change:** the **authoritative session stamps `seq` once at the source**,
before the event reaches any sink, and fan-out delivers the *identical* `seq`
to every subscriber. `seq` is now a property of the shared stream, not of a
delivery.

Consequences:
- The agreement guarantee strengthens: on shared instruments, execution and
  analysis see identical `(seq, source_ts, payload)` triples.
- A subscriber that detaches and re-attaches still observes contiguous `seq`
  for what it received; the resume buffer + `Control::Gap` story is unchanged,
  it just moves to source-side stamping.
- The "stamp on poll from a shared counter" wording in the invariant is
  replaced with "stamp once at source, identical across all sinks/subscribers."
- A per-subscriber view that starts mid-stream sees `seq` beginning at whatever
  the source counter held when it attached — `seq` is contiguous *per stream*,
  not *per subscriber session*. The resume buffer's overflow→`Control::Gap`
  rule already expresses "you missed a numbered span"; it now applies across
  the subscribe boundary too.

## iceoryx2 POD payload + symbol interning

The shm payload is `#[repr(C)]`, fixed-size, `Copy`. The only non-POD field in
today's events is `Instrument` (a `String`). Two options were considered;
**symbol interning** is chosen:

- The session core owns a **symbol table** mapping `Instrument` ↔ a small
  integer `SymbolId`. Interning happens once, at the source, when an
  instrument is first subscribed.
- The shm event carries `SymbolId`, not a string.
- The table is published on its own iceoryx2 service (a compact, low-rate
  "symbol announcement" stream) so subscribers resolve `SymbolId → Instrument`.
  A late-joining subscriber requests a snapshot of the table before consuming
  data, or reads it from the service's retained history.

Rejected alternative: fixed-capacity inline strings
(`iceoryx2_bb_container::byte_string`). Simpler (no announcement stream) but
caps symbol length, wastes shm bandwidth on every event, and still needs a cap
decision. Interning keeps the hot payload minimal and is the layout a network
transport would want anyway.

The interning + POD conversion lives **entirely inside the iceoryx2 sink and
its symbol-table component.** The public `MarketEvent` and `Instrument` are
untouched.

## `datamancerd` — the server binary

A new binary crate that is intentionally thin:

- Builds a `Datamancer` (provider creds, cache/DB client, tap log) from a
  config file.
- Opens a `Session` and wires the iceoryx2 `EventSink`.
- Holds the session handle so it keeps running (and recording) regardless of
  subscriber presence — this is exactly the existing **resume** lifecycle
  anchor, now spanning processes.
- Exposes a control surface for runtime `subscribe`/`unsubscribe` (mechanism —
  e.g. a small iceoryx2 request-response service or a local admin socket — is
  an implementation-plan decision, not fixed here).
- Exposes the **provider-health observability surface** (next section).
- Handles graceful shutdown: stop accepting, `flush()` sinks and tap log, drain.

Embedders who want zero hops (e.g. execution truly in-process) still link the
library and use the in-process sink. `datamancerd` is for the decoupled,
multi-consumer deployment.

## Observability surface

The observability driver is satisfied along two paths:

1. **In-band, already designed.** `Control` events (gaps, reconnects,
   subscription-state, provider errors) ride the stream and now fan out to
   *every* subscriber via the sink. Both execution and analysis see provider
   health changes in-band, ordered against data. No new mechanism — this is the
   existing `Control`-on-the-data-stream invariant, finally observable from
   multiple processes at once.
2. **Out-of-band health/metrics from `datamancerd`.** The server aggregates
   what the session core already knows and exposes it for operators:
   connection state per provider, last `source_ts` / `rx_ts` per instrument,
   `rx_ts - source_ts` latency, gap counts, resume-buffer occupancy,
   subscriber count. Surface form (Prometheus endpoint vs. a queryable iceoryx2
   service) is an implementation-plan decision. `rx_ts` stays observability-only
   here — it never feeds engine logic.

## Testing

- **In-process sink** — existing session/stream integration tests pass
  unchanged (the sink wraps today's path). This is the regression guard for the
  `seq`-at-source refactor.
- **`seq`-at-source** — a test with two in-process subscribers to one session
  asserts identical `(seq, source_ts)` on shared instruments and independent
  delivery for private ones.
- **iceoryx2 sink** — a same-host integration test (feature-gated, likely
  `#[ignore]`d in CI if it needs the iceoryx2 runtime) publishes a known event
  sequence and asserts a subscriber reconstructs the logical stream, including
  `SymbolId → Instrument` resolution and a mid-stream late join.
- **Symbol table** — round-trip `Instrument → SymbolId → Instrument`; late
  subscriber resolves all ids seen in data.
- **Shutdown ordering** — `flush()` on sink + tap log drains buffered events
  before the process exits (mirrors the existing tap-log flush test).

## Deferred (recorded, not built)

- **Network/cross-host transport.** A "fan-out node" subscribes to the local
  iceoryx2 service and re-publishes over a network transport (gRPC/QUIC/Aeron,
  or iceoryx2's own gateway when mature). The `EventSink` seam + interned
  symbol table are explicitly the pieces that make this additive. **No code
  now.**
- **Authn/z on the control surface.** Same-host, single-operator today.
- **Backpressure policy across the process boundary.** iceoryx2's pub-sub
  history/overflow config interacts with the existing resume-buffer semantics;
  reconcile the two when the iceoryx2 sink is actually built.
- **Cache eviction / volume bounding** and **tap-log `seq` rebase** — unchanged
  pre-existing deferrals, untouched by this work.

## Phasing (for the implementation plan)

1. **`EventSink` seam + in-process sink + `seq`-at-source.** Pure refactor of
   the existing path behind the new trait, plus moving `seq` stamping to the
   source. No new transport. Lands the invariant change and its doc updates.
   Fully tested by the existing suite + the two-subscriber `seq` test.
2. **iceoryx2 sink + symbol interning** behind `transport-iceoryx2`. The POD
   layout, symbol table, and announcement service.
3. **`datamancerd` binary** — config, lifecycle, control surface, health
   surface.

Each phase is independently shippable; phase 1 delivers the multi-consumer
`seq` guarantee even before iceoryx2 exists.
