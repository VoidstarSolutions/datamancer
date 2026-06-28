# Datamancer → Standalone Data Server — Roadmap

**Date:** 2026-06-28
**Status:** Approved roadmap, pre-implementation
**Supersedes nothing; extends:** [`2026-06-14-consumer-transport-seam-design.md`](2026-06-14-consumer-transport-seam-design.md)
(the approved transport-seam + `datamancerd` design — read it first; this roadmap
sequences and completes it, and *corrects* its implicit single-merged-stream
assumption — see "Determinism scope" below).

## Context

Datamancer is a library today: a consumer builds a `Session` in-process and
drains its events as a `Stream`. The goal is to make Datamancer deployable as a
**standalone data server** that provides a unified interface to financial data
to multiple consumer processes over a transport — **without abandoning the
library**. Embedding stays the primary artifact; the server is a thin wrapper.

The motivating pressures (from the transport-seam design): decoupling execution
from analysis into separate processes that must **agree on the shared subset of
instruments** (same events, same order *per instrument*); first-class
**provider-health observability** *available to consumer processes, not only an
operator*; and **independent deployment** of a long-lived data process.

This roadmap is the master plan for that transformation. Each phase below gets
its own detailed implementation plan (via `writing-plans`) when it is executed.

## Decisions locked in scoping

These were settled before drafting and bound the whole roadmap:

- **Same-host only.** Transports in scope are the in-process `Stream` and a
  same-host iceoryx2 sink. Network/cross-host stays a recorded "fan-out node"
  sketch — **no code, no phase.**
- **Per-symbol determinism; no inter-symbol sequencing.** Exact ordering is a
  *within-instrument* property. Ordering *across* instruments is explicitly **out
  of scope** — a non-goal, not a deferral (established on the Citadel side).
- **One multiplexed stream per client.** A client consumes all of its subscribed
  instruments over a single stream / single connection. The **client session is
  the primary consumer handle**; "I want one instrument" is the one-subscription
  case.
- **The consumer transport carries two planes:** the **data plane** (the
  multiplexed `MarketEvent` stream) and a **diagnostics plane** (the introspection
  snapshot — provider health/accounting, cache catalog, system state). Both reach
  client processes; the diagnostics plane is built on the same transport, not
  bolted on only for the operator UI.
- **Fan-out of configured sessions + runtime sub/unsub.** The active instrument
  set is mutable at runtime via a control surface. Not an arbitrary on-demand
  query API.
- **One connection (one iceoryx2 service) per client** for the data plane. Client
  count is small, so duplicating a symbol's events into each subscribing client's
  stream is acceptable and preferred over per-symbol services + client-side
  multiplexing.
- **Alpaca-only.** Multi-provider routing stays additive and out of scope.
- **Deliverable shape:** this roadmap doc now, plus a per-phase implementation
  plan when each phase is executed.

## Determinism scope and the consumer model

The transport-seam design reads as if it wanted *one* authoritative session
merging all instruments under a single, globally-monotonic `seq`. That
over-delivers: **cross-instrument ordering is not needed.** The only ordering
guarantee consumers require is **within a single instrument**, and the only
*agreement* requirement is that two clients watching the same instrument see the
**identical `(seq, source_ts)`** for it.

The model that satisfies exactly that, and no more:

- **The authoritative per-`(instrument, kind)` session is the deterministic
  unit** — its own strictly-ordered stream with its own `seq`, **source-stamped**.
  It is a **shared singleton** (the existing one-live-session-per-pair registry
  already enforces this); every client watching that instrument references the
  same one, so agreement holds by construction.
- **`seq` is per-symbol**, not global. In a client's multiplexed stream the
  ordering key is **`(instrument, seq)`** — `seq` is monotonic *within* each
  instrument's substream and carries no meaning across instruments.
- **The client session interleaves** its subscribed instruments' streams into one
  multiplexed output (arrival order across symbols, deterministic within each).
  This is an **interleave, not a merge-sort** — there is no cross-symbol order to
  compute, which is what makes it cheap.

**Two alternatives were considered and rejected:**

- *One unified multi-instrument session with a global `seq`* — delivers
  cross-instrument determinism nobody needs, at the cost of the largest and
  riskiest change (a core-controller merge rewrite).
- *Many separate per-symbol streams/connections to each client* — avoids a
  multiplex but maps poorly to the per-client connection model the transports
  give us.

**Symbol interning returns** under this model — not for global ordering, but
because a client's multiplexed stream carries **many symbols over one
connection**, so the POD/shm payload needs a compact `SymbolId`. Interning is a
multiplexing detail of the iceoryx2 sink (Phase 4), not a public-API concern.

## Architecture

```
provider edge   →  authoritative per-(instrument,kind)  →  client session       →  consumer transport (EventSink)
(hot, local,        sessions                                (interleave subscribed   data plane: in-process Stream
 monomorphic        (deterministic unit, shared             symbols into ONE         OR per-client iceoryx2 service
 decode)            singleton, source-stamped               multiplexed stream;     ┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄
                    per-symbol seq)                          subscription mgmt,       diagnostics plane: introspection
                                                             refcounted sharing)      snapshot over a separate service
```

- **One new seam — `EventSink`** — between the session core and consumers for the
  **data plane**. The current in-process delivery becomes one sink
  implementation; iceoryx2 becomes another. Which sink is wired is a builder
  choice, exactly as `HistoricalCache` and `TapLog` are today. The trait lives in
  `datamancer-core`; see the transport-seam design for its shape (`publish` in
  `seq` order, `flush` for shutdown).
- The **diagnostics plane** is a separate publisher over the Phase-3 introspection
  snapshot — lower-rate and structured, with different transport characteristics
  from the hot data plane. In-process embedders read the snapshot directly; the
  iceoryx2 sink exposes it as its own service (Phase 4).
- The **provider edge** and the **cache-via-DB-client** path are untouched by
  this work.

## Phases

| # | Phase | Ships | Depends on |
|---|-------|-------|------------|
| 1 | `EventSink` seam + `seq`-at-source | Per-symbol `seq` agreement across clients; the seam | — |
| 2 | Client session (multiplex + subscription mgmt) | One multiplexed stream per client, runtime sub/unsub | 1 |
| 3 | Library introspection interfaces | Provider accounting, cache catalog, snapshot API | 2 |
| 4 | iceoryx2 sink — data + diagnostics planes | Zero-copy same-host fan-out (data) + diagnostics transport | 1, 2, 3 |
| 5 | `datamancerd` binary | The server: config, lifecycle, control surface | 4 |
| 6 | Introspection web UI | Operator web view over the snapshot | 5, 3 |

Each phase is independently shippable and testable. Phases 1–3 deliver real value
to embedders (the multiplexed multi-instrument client session, plus programmatic
introspection) with **no transport code at all**. Introspection (Phase 3) now
lands before the transport so the transport can carry it (Phase 4's diagnostics
plane).

---

### Phase 1 — `EventSink` seam + `seq`-at-source

**Goal.** Introduce the consumer-transport seam and move `seq` stamping to the
source of each authoritative per-`(instrument, kind)` stream, as a
behavior-preserving refactor.

**Work.**
- Add the `EventSink` trait (`datamancer-core`).
- Make today's in-process delivery an **in-process sink** wrapping the existing
  bounded channel / resume buffer; the embedder's stream drains it. Behavior is
  byte-for-byte the current path.
- Move `seq` stamping from `EventStream::poll` to the **source** (per authoritative
  session, before the event reaches any sink).
- Confirm the **tap-log tee and resume buffer sit core-side of the sink**, so
  every sink inherits them.

**Load-bearing invariant change.** `seq` moves from *"stamped per consumer at
delivery"* to *"stamped once at the source of each per-symbol stream; identical
across all clients consuming that symbol."* It remains **per-symbol** (not
global). A consequence: a client that misses events (resume-buffer eviction, late
mid-stream join) can observe a **hole** in that symbol's `seq`, surfaced in-band
as a `Control::Gap`. This **revises the `seq` wording corrected in the 2026-06-28
doc-baseline cleanup** (`datamancer-core/src/event.rs`, the crate README, root
`CLAUDE.md`); those updates land here.

**Tests.** Existing session/stream suite passes unchanged (regression guard for
the refactor). New two-subscriber test: two in-process subscribers to one
authoritative instrument see identical `(seq, source_ts)`.

**Open questions (for the plan).**
- Does `EventSink` coexist with, or supersede, the existing internal forwarding
  path? (Expectation: the in-process sink *wraps* today's channel.)
- Exact placement of the resume buffer relative to the sink boundary.

**Risk.** Low — behavior-preserving refactor with the existing suite as guard.

---

### Phase 2 — Client session (multiplex + subscription management)

**Goal.** Introduce the **client session** as the primary consumer handle: it
holds a mutable set of `(instrument, kind)` subscriptions and presents **one
multiplexed stream** combining them, over the existing per-symbol authoritative
sessions. In-process only — no transport.

**Work.**
- Client session holds a subscription set and **interleaves** the subscribed
  authoritative per-symbol streams into one output stream (arrival order across
  symbols; deterministic within each). Ordering key is `(instrument, seq)`.
- Runtime `subscribe` / `unsubscribe` mutate the set live.
- **Refcounted sharing** of authoritative per-`(instrument, kind)` sessions: many
  client sessions reference one authoritative session (singleton via the existing
  registry); the last referrer leaving tears it down.
- The standalone per-pair `Session` becomes the **internal authoritative unit**;
  the client session is the public handle (single-instrument = one subscription).
- **`Control` scoping:** per-symbol `Control` (`Gap`, `SubscriptionChanged`) sit
  in that symbol's substream carrying its instrument; connection-scoped `Control`
  (`ProviderConnected`/`Disconnected`) rides the multiplexed stream **once**, not
  duplicated per symbol.
- Resume buffer now backs the per-client multiplexed stream.

**Docs.** README + `CLAUDE.md` updated to the consumer model: one multiplexed
stream per client, **deterministic per symbol, no cross-symbol ordering**, client
session as the primary handle. (This is *not* the old "merge all subscriptions
into one globally-ordered stream" — call out the distinction explicitly.)

**Tests.** Multiplex interleaving of several instruments; runtime add/remove
mid-stream; refcounted teardown when the last client drops a symbol; per-symbol
`seq` continuity + `Control::Gap` on overflow; connection-scoped `Control`
appears once.

**Open questions (for the plan).**
- Resume/gap granularity: per-symbol vs per-multiplexed-stream buffering.
- Whether today's `Session` type evolves into the client session or a new type is
  introduced with `Session` retained internally.

**Risk.** Moderate — lifecycle, refcounting, and `Control` routing. No
merge-sort, so well short of the unified-`seq` rewrite this replaces.

---

### Phase 3 — Library-level introspection interfaces

**Goal.** The library exposes a programmatic view of its own state. No web, no
daemon, no transport dependency — embedders get this too, and it is the data the
Phase-4 diagnostics plane will carry.

**Work (in `datamancer` / `datamancer-core`).**
1. **Provider-call accounting** — counters at the provider edge: history-fetch
   count, live reconnects, rate-limit hits, message/byte throughput, last error,
   connection state.
2. **Cache-catalog enumeration** — a new `HistoricalCache` method to *list* what
   is cached (today's `gaps()` answers coverage for one key; introspection needs
   the catalog: keys + covered ranges + on-disk volume).
3. **System-state snapshot API** — a consolidated, **serializable** snapshot of
   provider accounting, the cache catalog, and live state: authoritative
   per-symbol sessions, client sessions and their subscriptions, per-symbol
   subscriber/refcount, per-symbol `seq` position, per-instrument last
   `source_ts`/`rx_ts` and `rx_ts − source_ts` latency, resume-buffer occupancy,
   gap counts. Serializability matters: the same snapshot feeds the in-process
   reader, the Phase-4 diagnostics transport, and the Phase-6 web UI.

**Tests.** Library-level: counters increment under fetches/reconnects; cache
catalog round-trips against known stored ranges; snapshot reflects live client
and authoritative sessions; snapshot serializes/deserializes round-trip.

**Open questions (for the plan).**
- In-memory snapshot vs retained time-series; how much history the snapshot
  carries.
- Snapshot serialization format (shared by the diagnostics transport and the UI).

**Depends on:** Phase 2; existing cache/provider for the rest. **Independent of
the transport.**

**Risk.** Low–moderate — off the hot path, but new public surface in the core
crates.

---

### Phase 4 — iceoryx2 sink — data + diagnostics planes

**Goal.** Same-host, zero-copy fan-out of each client's multiplexed **data**
stream to its consumer process, **and** a **diagnostics** transport carrying the
Phase-3 introspection snapshot to those same processes — behind a
`transport-iceoryx2` cargo feature.

**Work — data plane.**
- **One iceoryx2 service per client** carrying that client's multiplexed subset.
  With a small client count, duplicating a symbol's events into each subscribing
  client's service is acceptable and matches the one-connection-per-client model.
- POD `#[repr(C)]`, fixed-size, `Copy` shm payload carrying `SymbolId` (the
  multiplexed stream carries many symbols → a compact id is required).
- A **symbol table** mapping `Instrument ↔ SymbolId`, interned at the source,
  published on a low-rate **announcement service** so subscribers resolve
  `SymbolId → Instrument` (including a late-joiner snapshot from retained
  history).
- Interning + POD conversion live **entirely inside the sink** — `MarketEvent`
  and `Instrument` are untouched.

**Work — diagnostics plane.**
- A **separate iceoryx2 service** publishing the Phase-3 introspection snapshot
  (provider health/accounting, cache catalog, system state) to client processes.
  This is the out-of-band complement to the in-band `Control` health that already
  rides the data plane — it gives consumers the *richer* operational picture the
  provider-health-observability driver calls for.
- This plane is **low-rate and structured**, so it does not need the hot-path
  POD/zero-copy treatment: a serialized-snapshot payload is fine. Delivery mode
  (periodic publish vs request-response) is an open question below.

**Constraint to resolve in the plan.** Both crates set
`#![forbid(unsafe_code)]`. iceoryx2 interop may force the sink into its **own
crate** (e.g. `datamancer-transport-iceoryx2`) without the forbid, or a tightly
scoped `allow`.

**Tests.** Feature-gated same-host integration test (likely `#[ignore]`d in CI if
it needs the iceoryx2 runtime): publish a known sequence and assert a subscriber
reconstructs the logical data stream, including `SymbolId → Instrument` resolution
and a mid-stream late join; **a subscriber reads the diagnostics service and
reconstructs a Phase-3 snapshot.** Symbol-table round-trip. Shutdown ordering:
`flush()` drains before exit.

**Open questions (for the plan).**
- Diagnostics delivery mode: periodic publish vs request-response.
- Data-plane backpressure: iceoryx2 pub-sub history/overflow vs the resume buffer.
- Exact POD field layout (data plane); diagnostics payload serialization (shared
  with Phase 3).

**Risk.** Moderate — new transport, shm-safety constraints, CI ergonomics, and
two transport shapes (hot pub-sub + structured diagnostics).

---

### Phase 5 — `datamancerd` binary

**Goal.** A thin binary crate that runs the authoritative sessions and serves
clients — the server product.

**Work.**
- Build a `Datamancer` (provider creds, cache/DB client, tap log) from a config
  file.
- Per connected client, create a **client session** (Phase 2) wired to a
  **per-client iceoryx2 service** (Phase 4); publish the **diagnostics plane**
  (Phase 4) from the Phase-3 snapshot.
- Hold authoritative sessions as the **lifecycle anchor** so they keep running
  (and recording) across client presence per config/refcount — the existing
  **resume** lifecycle, now spanning processes.
- **Control surface** for runtime `subscribe`/`unsubscribe`, driving a client
  session's subscription set.
- Graceful shutdown: stop accepting, `flush()` sinks and tap log, drain.

In-band `Control` health rides the data plane; the richer out-of-band snapshot
rides the diagnostics plane (Phase 4) and feeds the operator UI (Phase 6).

Embedders who want zero hops still link the library and use the in-process sink.

**Open questions (for the plan).**
- Config format.
- Control-surface transport (iceoryx2 request-response vs a local admin socket).
- Authz is **deferred** (same-host, single-operator).

**Risk.** Moderate — process lifecycle, config, shutdown ordering.

---

### Phase 6 — Introspection web UI

**Goal.** `datamancerd` hosts an HTTP server rendering the Phase-3 snapshot for
operators — read-only.

**Work (daemon-side).**
- HTTP server (likely `axum`) exposing JSON endpoints over the Phase-3 snapshot
  API, plus the UI itself (use the `frontend-design` skill when building it).
- Optionally a `/metrics` Prometheus endpoint off the same data.

**Surfaces** the cache catalog and coverage, provider call counts / rate-limit
usage / throughput, and live state (client sessions, subscriptions, per-symbol
latency, gaps, connection health).

**Open questions (for the plan).**
- UI tech: server-rendered vs lightweight SPA.
- Read-only now vs later unifying with the Phase-5 control surface (trigger fetch
  / sub-unsub from the UI).
- Auth **deferred** (same-host, single-operator).

**Depends on:** Phase 5 (daemon to host it) + Phase 3 (the snapshot).

**Risk.** Moderate — new HTTP/UI surface, isolated from core ordering.

---

## Invariant and documentation changes

- **Phase 1** rewrites `seq` semantics (stamp-at-source, **per-symbol**, identical
  across clients of a symbol; per-client holes reported via `Control::Gap`) in
  `datamancer-core/src/event.rs`, the crate README, and root `CLAUDE.md`.
- **Phase 2** rewrites the consumer model (one multiplexed stream per client,
  **deterministic per symbol with no cross-symbol ordering**, `(instrument, seq)`
  ordering key, client session as the primary handle) in the README and the
  "single ordered stream" invariant in `CLAUDE.md`.

The 2026-06-28 doc-baseline cleanup left a faithful snapshot of today's behavior
plus a marked roadmap; Phases 1–2 are where that becomes reality, and these are
the precise spots to edit.

## Crate structure changes

- `datamancer-core`: add the `EventSink` trait; the per-symbol `seq` invariant
  change; the introspection snapshot types and the `HistoricalCache` catalog
  method.
- `datamancer`: the in-process sink (Phase 1); the client session (Phase 2);
  provider-call accounting and snapshot assembly (Phase 3). The iceoryx2 sink
  (both planes) may live here behind `transport-iceoryx2`, **or** in a new
  `datamancer-transport-iceoryx2` crate if `forbid(unsafe_code)` requires it
  (Phase 4 decision). `SymbolId`/interning are sink-local, not core.
- New `datamancerd` binary crate (Phase 5); its web server (Phase 6).

## Cross-cutting risks

- `seq`-at-source × resume/backfill interactions need care in Phases 1–2.
- iceoryx2 runtime + `forbid(unsafe_code)` ergonomics, and the two transport
  shapes (hot data pub-sub + structured diagnostics), in Phase 4.
- Doc/invariant churn is concentrated in Phases 1–2; keeping it there avoids
  scattering semantic changes across the roadmap.

## Non-goals

- **Inter-symbol / global ordering.** Determinism is per-symbol; cross-symbol
  order in the multiplex is arrival-order and carries no guarantee. This is a
  non-goal, not a deferral.
- **On-demand arbitrary-query data API.** The server fans out configured sessions
  with runtime sub/unsub, not arbitrary consumer queries.

## Deferred (recorded, not built)

- **Network / cross-host transport** — a future "fan-out node" subscribes to the
  local iceoryx2 services and re-publishes over a network transport. The
  `EventSink` seam + interned symbol table are the pieces that make this additive.
  No code.
- **Multi-provider routing.**
- **Control-surface authz**; **cross-process backpressure policy**.
- **Cache eviction / volume bounding** and **tap-log `seq` rebase** — pre-existing
  deferrals, untouched here.

## Next step

Per-phase implementation plans, authored with `writing-plans` as each phase is
executed, starting with **Phase 1** (`EventSink` seam + `seq`-at-source).
