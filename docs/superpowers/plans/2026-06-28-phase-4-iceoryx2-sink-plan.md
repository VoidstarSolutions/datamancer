# Phase 4 — iceoryx2 sink — data + diagnostics planes

**Fidelity:** design-level + external research (integration firms up after 1-3)

_Part of the datamancer standalone-server roadmap. See `docs/superpowers/specs/2026-06-28-datamancer-server-roadmap.md`._

---

> **Reconciliation pass — authoritative; supersedes any conflicting text below.** Applied from the [cross-phase consistency report](2026-06-28-server-plan-consistency-report.md). Architect decisions: registry/ids/stats built in **Phase 2** (Issue 3); diagnostics snapshot **split** (Issue 6).
>
> Resolutions affecting this phase:
> - **Serialize via `publish_borrowed` (Issue 2):** attach the data-plane sink through Phase 1's `publish_borrowed(&self, &MarketEvent)`. The owned `publish(MarketEvent) -> PublishOutcome` is the in-process path. No borrowed-`Result`.
> - **Attach at the per-client output (Issue 1):** `Iceoryx2DataSink` attaches at the per-client `Box<dyn EventSink>` output Phase 2 exposes (one service per client), not at the authoritative controller.
> - **Diagnostics sizing (Issue 6 — decided: split):** size the fixed-capacity fast diagnostics service to Phase 3's bounded **live-state snapshot**; publish the heavier **cache catalog** on a separate service (larger-cap / chunked / slower cadence). Do not size one fixed payload for the whole `SystemSnapshot`.
> - **POD `seq` tolerates the sentinel (Issue 8):** the POD payload's `seq: u64` carries `Seq::SYNTHETIC` verbatim for synthetic controls; do not fold it into monotonicity assumptions.

> **Detailed-planning hardening (gotcha pass, 2026-06-28) — authoritative.** Adversarial review against the iceoryx2 research + `event.rs`/`price.rs`/`instrument.rs`. `Price` is POD-safe (`i64`); `Instrument` already derives serde; the `String` inside `Instrument` is what forces `SymbolId`. Supersedes conflicting body text.
>
> **Locked decisions:**
> - **Connection-scoped controls are DIAGNOSTICS-PLANE ONLY.** `ProviderConnected`/`Disconnected`/`ProviderError` are **suppressed at the per-client iceoryx2 sink** — never on the POD data service. Remote consumers read provider connectivity + last-error from the diagnostics live-state snapshot (`ProviderSnapshot`). Per-symbol controls (`Gap`, `SubscriptionChanged`) still ride the data plane (they carry a `SymbolId`). **Documented divergence:** in-process consumers (Phase 2) get connection controls in-band; remote consumers get them via diagnostics. **Bonus:** the POD `Control` variant now needs **no free-text fields** — only `Gap`(span) and `SubscriptionChanged`(active bool).
> - **Backpressure = blocking** (`enable_safe_overflow=false`). A full subscriber queue backpressures the per-client publisher → stalls the per-client controller → Phase 2's "drop this client with a `Control::Gap`" isolation fires. Loss is always accounted, never silent. Cross-process backpressure tuning deferred.
>
> **Spike-gate (EXT-1) before committing the POD design:** pin the iceoryx2 version; verify the `#![forbid(unsafe_code)]` gate — a `#[repr(C)] Copy` payload deriving `ZeroCopySend` compiles with **no caller-side `unsafe`**. If yes → sink stays under `forbid`; if no → a new **`datamancer-transport-iceoryx2`** crate (deps: `datamancer-core` + iceoryx2 only) with a scoped `allow`. Also verify: pub-sub builder/loan/send/receive surface; `send` semantics under `overflow=false` (blocks vs errors); `history_size` (publisher-alive-only); request-response API; slice payloads; `max_subscribers`/history config; WaitSet ↔ tokio integration.
>
> **POD payload:** flat `#[repr(C)]`, `Copy`, fixed-size tagged record sized to the largest variant; `SymbolId` (not `Instrument`); `seq: u64` carries `Seq::SYNTHETIC` verbatim. No `FixedSizeByteString` needed (connection controls are off the data plane). Spike a `#[repr(C)] union` (scoped `unsafe`) only if padding cost proves real.
>
> **Symbol table:** intern `Instrument ↔ SymbolId` at the source on first subscribe; one announcement service; republish the full table every **5s** + on `flush`. The **subscriber helper is public API** in the transport crate (Phase 5 reuses it) and **holds** data referencing an unresolved `SymbolId` until the announcement resolves it (the two services have no mutual ordering); a no-history late joiner tolerates ≤~5s startup — test with a timeout.
>
> **Diagnostics plane:** live-state snapshot via **periodic pub-sub** (`history_size(1)`, bounded fixed payload to Phase 3's live-state cap); **cache catalog via request-response pull** (per-request chunking). Provider connectivity/last-error surface here.
>
> **Service lifecycle:** the sink owns its `Node` + per-client service; not cloneable; dropped on client-session close. Per-client service **over-provisions** `max_subscribers`/history — document the symbol-cap assumption; dynamic service recreation on cap-exceed is a Phase-5 concern.
>
> **Tests (feature-gated; likely `#[ignore]` in CI):** POD round-trip per variant; `SymbolId → Instrument` resolution incl. **data-before-announcement hold**; no-history late-joiner recovery (timeout-bounded); blocking-backpressure → Phase-2 drop-to-Gap; diagnostics snapshot pub + catalog request-response chunk reassembly; flush/shutdown drain; synthetic-`seq` sentinel survives round-trip.

## Context & goal

Phase 4 delivers the `transport-iceoryx2` capability: same-host, zero-copy fan-out of
each client's multiplexed **data** stream to its consumer process, plus a separate
**diagnostics** transport carrying the Phase-3 introspection snapshot to those same
processes. It is the first phase that introduces transport code; Phases 1–3 ship the
seam (`EventSink`), the client session (multiplex + refcounted sharing), and the
serializable snapshot with **no transport at all** (roadmap table, rows 1–4).

Two planes over one logical client connection:

- **Data plane** — one iceoryx2 pub-sub *service per client* carrying that client's
  multiplexed `(instrument, seq)` substream set. POD `#[repr(C)]` `Copy` payload that
  carries a compact `SymbolId` instead of the heap-backed `Instrument`. A symbol
  table interns `Instrument → SymbolId` at the source and publishes the reverse
  mapping on a low-rate **announcement service** so subscribers (including late
  joiners) resolve `SymbolId → Instrument`. Interning + POD conversion live
  **entirely inside the sink**; core `MarketEvent`/`Instrument` are untouched
  (roadmap Phase 4 "Work — data plane").
- **Diagnostics plane** — a separate iceoryx2 service publishing the Phase-3 snapshot
  (provider health/accounting, cache catalog, live state). Low-rate, structured,
  **serialized** (CBOR/JSON/bincode-class bytes in a byte-slice payload) — explicitly
  **not** the POD/zero-copy hot path (roadmap Phase 4 "Work — diagnostics plane").

Hard constraints honored throughout: determinism is **per-symbol only** (the
multiplex is an interleave, never a global merge-sort — `(instrument, seq)` is the
only ordering key); one multiplexed stream per client over one connection; `seq` is
per-symbol, source-stamped, identical across clients of a symbol; the authoritative
per-`(instrument, kind)` session is the shared refcounted singleton; same-host only;
Alpaca-only; **both `datamancer-core` and `datamancer` keep `#![forbid(unsafe_code)]`**
(verified present today at `datamancer-core/src/lib.rs:13` and
`datamancer/src/lib.rs:25`).

This plan is **design-level**. It commits the architecture, crate placement, the
`forbid(unsafe_code)` resolution, payload-layout shape, service topology, and
flush/shutdown ordering, but it defers exact iceoryx2 method-call wiring and final
field offsets to implementation time (the version-churn and verify-during-impl gates
in the research brief). Concrete integration firms up only after Phases 1–3 land —
hence the RE-PLAN CHECKPOINTs below. Per the fidelity tier, Phase-5/6 detail (daemon
lifecycle, control-surface transport, web UI) is referenced only where it constrains a
Phase-4 decision, not designed here.

## Prerequisites / assumptions

This phase depends on Phases 1, 2, and 3 (roadmap table: "Depends on 1, 2, 3"). Each
dependency edge below states the assumption explicitly; where the upstream output is
not yet pinned, it is flagged as a **RE-PLAN CHECKPOINT** to revisit before
implementation begins.

### From Phase 1 (`EventSink` seam + seq-at-source)

- **Assumption P1-A.** `datamancer-core` defines an `EventSink` trait with roughly the
  transport-seam shape (roadmap line 110–111: "`publish` in `seq` order, `flush` for
  shutdown"): events are handed to the sink in per-symbol `seq` order, and a `flush`
  enforces shutdown ordering. The sink is `Send + Sync`. The iceoryx2 sink is an
  additional `impl EventSink`, selected by a builder choice exactly like
  `HistoricalCache`/`TapLog`. **Naming note:** `session.rs:719` already defines an
  internal `Sink` enum (`Attached`/`Detached`); the new public trait is `EventSink`
  and must not be confused with it — the iceoryx2 type lives behind that trait, not in
  the enum.
- **Assumption P1-B.** The tap-log tee (`session.rs:1411` `forward()` / `session.rs:1442`
  `tee()`) and the resume buffer / `EventRing` (`session.rs:1659`) sit **core-side of
  the sink** (roadmap Phase 1: "Confirm the tap-log tee and resume buffer sit core-side
  of the sink, so every sink inherits them"). The iceoryx2 sink therefore inherits
  durability and resume buffering for free and does not re-implement them. Cross-process
  backpressure is a separate, explicitly deferred concern (roadmap "Deferred:
  cross-process backpressure policy"); see Open Questions.
- **Assumption P1-C (most load-bearing).** `seq` is stamped once at the authoritative
  per-`(instrument, kind)` session before any sink sees the event, identical across all
  sinks/clients of that symbol, and per-client holes surface in-band as `Control::Gap`
  (roadmap Phase 1 "Load-bearing invariant change"). **Current-code contrast worth
  pinning:** today `seq` is the opposite — session-monotonic, stamped *at delivery* in
  `EventStream::poll_next` from a single shared `seq_counter`
  (`datamancer-core/src/lib.rs:670-691`, `seq_counter` at `session.rs:289`). Phase 1
  inverts this. The iceoryx2 sink's whole cross-client agreement guarantee
  (`two_clients_same_symbol_see_identical_seq`) rests on Phase 1 having completed that
  inversion; if Phase 1 ships a different shape, this plan's payload semantics are
  unaffected but the *guarantee* must be re-derived.
- **RE-PLAN CHECKPOINT P1-1.** The exact `EventSink` signature (sync vs `async_trait`,
  `publish(&MarketEvent)` vs owned value, batched `publish_many`/drained-slice vs
  one-at-a-time, and the error type) is **not yet final**. The POD-conversion call site
  (Step 2/3) binds to it. Revisit the conversion entry point and error mapping once
  Phase 1 lands.
- **RE-PLAN CHECKPOINT P1-2.** Whether the resume buffer is per-symbol or
  per-multiplexed-stream (Phase 2 open question, roadmap line 210) changes how the sink
  maps a resume-buffer-overflow `Control::Gap` onto a per-`SymbolId` gap on the wire.
  Assume per-multiplexed-stream buffering until Phase 2 decides; the wire `Gap` carries
  its `SymbolId` regardless, so the payload is unaffected — only the granularity of
  *when* a gap is emitted changes.

### From Phase 2 (client session: multiplex + subscription mgmt)

- **Assumption P2-A.** A **client session** type exists as the primary consumer handle
  (roadmap Phase 2). It owns a mutable subscription set of `(instrument, kind)` and
  **interleaves** the subscribed authoritative substreams into one output, ordering key
  `(instrument, seq)` — an interleave, *not* a merge-sort (roadmap lines 76–79). One
  iceoryx2 data service is created per client session.
- **Assumption P2-B.** Per-symbol `Control` (`Gap`, `SubscriptionChanged`) rides that
  symbol's substream carrying its instrument; connection-scoped `Control`
  (`ProviderConnected`/`Disconnected`, `ProviderError`, `SessionClosing`) rides the
  multiplexed stream **once**, not duplicated per symbol (roadmap Phase 2 "Control
  scoping"). The sink must preserve this: per-symbol controls map to a `SymbolId`;
  connection-scoped controls map to a reserved sentinel `SymbolId` so subscribers can
  route them.
- **Assumption P2-C.** Refcounted sharing of the authoritative session is implemented
  via the existing registry + `RegistrySentinel` (`session.rs:1538`, `Drop` at
  `session.rs:1543`); the iceoryx2 sink attaches at the client-session layer,
  downstream of refcounting, so it never observes the shared-singleton mechanics
  directly.
- **RE-PLAN CHECKPOINT P2-1.** Whether the client session is a new type or the evolved
  `Session` (Phase 2 open question, roadmap line 211–212) determines exactly where the
  sink is wired (the builder method and the lifecycle anchor that owns the iceoryx2
  publisher). Revisit the wiring site once Phase 2 names the type.
- **RE-PLAN CHECKPOINT P2-2.** Whether runtime `subscribe`/`unsubscribe` causes the
  client's iceoryx2 *service* to be recreated (service resources — `max_subscribers`,
  buffer sizes, `history_size` — are fixed at creation and cannot grow) or whether we
  over-provision fixed capacity at service creation. Assume over-provisioned fixed
  capacity: the data payload is a fixed POD over event kinds (Step 2), so adding a
  symbol does **not** change payload size; only the announcement *content* grows, which
  is fine because the `SymbolId` space is pre-sized. Confirm the symbol cap is generous
  and documented.

### From Phase 3 (introspection snapshot)

- **Assumption P3-A.** A consolidated, `serde::Serialize`/`Deserialize` snapshot type
  exists (provider accounting, cache catalog, live state — roadmap Phase 3 item 3) with
  a stable wire format, and a function to produce a fresh snapshot on demand. The
  diagnostics plane serializes this snapshot and publishes it.
- **RE-PLAN CHECKPOINT P3-1.** The snapshot's serialization format (Phase 3 open
  question, roadmap line 247: "format shared by the diagnostics transport and the UI")
  is **not yet pinned**. The diagnostics payload is `serialize(snapshot) → bytes →
  fixed-cap byte-slice payload`. Revisit the chosen codec and the **maximum serialized
  size** (which sizes the diagnostics service's fixed payload capacity) once Phase 3
  lands. If a snapshot can exceed the fixed iceoryx2 payload cap, a
  chunking/fragmentation scheme is required — flagged in Open Questions.
- **RE-PLAN CHECKPOINT P3-2.** The snapshot's update cadence and push-vs-pull mode is
  the diagnostics delivery-mode open question (roadmap line 300). Assume **periodic
  publish** with `history_size(1)` so a late joiner immediately reads the last snapshot;
  revisit if Phase 5's control surface argues for request-response.

### External / environment

- **Assumption EXT-A.** iceoryx2 is pinned to a single verified version at impl time
  (research brief: latest ≈ 0.9.2; some snippets cite 0.8.1 — pin and verify). All
  method-name references in this plan (`service_builder`, `publish_subscribe`,
  `loan_uninit`/`write_payload`/`send`, `history_size`, `enable_safe_overflow`,
  `ZeroCopySend` derive, `iceoryx2-bb-container` fixed-size types) are
  **verify-against-pinned-version** and may shift.
- **RE-PLAN CHECKPOINT EXT-1 (the `forbid(unsafe_code)` gate).** Before committing to
  the in-crate approach, write **one** POD payload deriving `ZeroCopySend` and confirm
  it compiles under `#![forbid(unsafe_code)]` using only the derive (no hand-written
  `unsafe impl`). The crate-placement decision in Step 0 branches on this result.

## Step-by-step implementation

### Step 0 — Resolve `forbid(unsafe_code)` crate placement (do first)

The constraint is hard: both existing crates set `#![forbid(unsafe_code)]` (verified).
The research brief establishes that iceoryx2 zero-copy is safe **iff** every payload
uses `#[derive(ZeroCopySend)]` (a *safe* generated impl) plus fixed-size containers
from `iceoryx2-bb-container`; a hand-written `unsafe impl ZeroCopySend` would violate
the invariant.

Decision procedure:

1. Spike the EXT-1 gate (one derived POD payload under `forbid(unsafe_code)`).
2. **If the derive suffices** (expected): place the sink in a **new crate
   `datamancer-transport-iceoryx2`** that itself sets `#![forbid(unsafe_code)]`. New
   crate (not in-`datamancer`) is preferred regardless of the gate because:
   - it isolates the heavy iceoryx2 + `iceoryx2-bb-container` dependency tree behind a
     hard crate boundary, keeping `datamancer`'s default build lean;
   - it depends on `datamancer-core` for `EventSink` + the event model, and on
     `datamancer` only if it needs client-session wiring (prefer core-only — see Public
     API);
   - it matches the roadmap's stated structure option (roadmap lines 389–391: "may live
     here behind `transport-iceoryx2`, **or** in a new `datamancer-transport-iceoryx2`
     crate if `forbid(unsafe_code)` requires it").
   The `datamancer` crate gains a `transport-iceoryx2` feature that pulls in the new
   crate as an optional dependency and re-exports its sink constructor, so embedders
   still enable it via one feature flag.
3. **If the derive does NOT suffice** (any payload needs a hand-written `unsafe impl`):
   the new crate is **mandatory** and it must drop `#![forbid(unsafe_code)]`, replacing
   it with `#![deny(unsafe_code)]` plus a single tightly-scoped, documented
   `#[allow(unsafe_code)]` on the exact impl, with a `// SAFETY:` proof referencing the
   `#[repr(C)]` POD guarantees. `datamancer-core` and `datamancer` keep
   `forbid(unsafe_code)` untouched. This quarantines any unsafe to one auditable
   location and keeps the invariant intact for the two crates the constraint names.

**Decision recorded:** new crate `datamancer-transport-iceoryx2`, target
`#![forbid(unsafe_code)]`, falling back to scoped `allow` only if EXT-1 fails. The two
named crates never relax their forbid.

### Step 1 — Symbol interning component (sink-local)

Create `symbol_table` inside the transport crate. Responsibilities:

- `SymbolId(u32)` newtype, POD (`#[repr(transparent)]`, `Copy`). Reserve
  `SymbolId::CONNECTION = SymbolId(u32::MAX)` for connection-scoped controls so they
  are distinguishable from any real instrument on the wire; intern real instruments
  densely from `0` upward (dense ids keep subscriber-side resolution array-indexable).
- An interner: `intern(&Instrument) -> SymbolId` (monotonic, first-seen order) plus the
  reverse map for announcement publishing. Interning happens at the source the first
  time the sink sees an instrument — lazily on `publish`, or **eagerly on `subscribe`**
  if Phase 2 exposes a subscription hook. Prefer eager so the announcement is *attempted*
  before the first data sample for that symbol (note this does not by itself guarantee
  cross-service delivery order — see Step 6 / Risks).
- The interner is owned by the per-client sink instance. `seq`/identity must be
  *consistent across clients of a symbol* for the **data payload semantics**, but the
  `SymbolId` itself is a *per-service local handle* (each client has its own service and
  its own announcement stream), so the id space can be per-client. Document this:
  `SymbolId` is **not** a global identity, only a per-service compaction handle;
  `(SymbolId → Instrument)` is resolved per service via that service's announcement
  stream. `seq` agreement is still by-construction because it is carried verbatim from
  the source-stamped event (P1-C).

### Step 2 — POD data payload

Define the `#[repr(C)]`, fixed-size, `Copy` wire struct in the transport crate,
deriving `ZeroCopySend` (per Step 0). Grounded against the current event model
(`datamancer-core/src/event.rs`): `Price(pub i64)` and `Timestamp(pub i64)` are already
`Copy` scalars, so the timestamp/price fields are POD-trivial; `BarInterval` and
`AssetClass` are `#[repr(Rust)]` C-like enums that map to a `#[repr(u8)]` tag. Shape
(final offsets verified at impl time):

- A `kind` discriminant (`#[repr(u8)]`) selecting Trade / Quote / Bar / Control.
- `symbol: SymbolId`.
- `source_ts: i64`, `rx_ts: i64`, `seq: u64` (the three timestamp/order fields,
  preserved end-to-end per the CLAUDE.md invariant — `seq` sole ordering key, `rx_ts`
  observability-only, never reconstructed by the subscriber).
- A fixed-size body covering the largest variant (Bar: open/high/low/close as `i64`
  from `Price`, `volume: u64`, `interval` as `#[repr(u8)]`; Quote: bid/ask/bid_size/
  ask_size; Trade: price/size; Control: a `#[repr(u8)]` control-kind tag + `GapSpan`
  (`from_source_ts`, `to_source_ts` as `i64`) for `Gap`, and `active: bool` + `kind`
  tag for `SubscriptionChanged`). Because Rust unions interact poorly with the
  `ZeroCopySend` derive and `forbid(unsafe_code)` (union field access is unsafe), model
  the body as a **flat struct of all possible fields** sized to the max variant rather
  than a `union` — wasted bytes are acceptable at this payload size and it keeps the
  derive safe. Validate this against the EXT-1 spike.
- **Control string-field correctness (grounded in `event.rs:135-161`).** Every
  string-bearing `ControlKind` carries `provider: String`, and crucially the
  *per-symbol* controls do too: `Gap { provider, instrument, span }` and
  `SubscriptionChanged { provider, instrument, kind, active }` both carry a `provider`
  string in addition to their ids. Because the roadmap is **Alpaca-only**, `provider`
  is effectively constant; encode it as a small `#[repr(u8)]` provider tag (or omit it
  and let the subscriber assume Alpaca), **not** a heap string on the hot path. The
  `instrument` field of these controls is already represented by `SymbolId`; the
  `EventKind`/`BarInterval` of `SubscriptionChanged` maps to `#[repr(u8)]` tags. The
  remaining string-bearing, *connection-scoped* controls
  (`ProviderConnected{provider}`, `ProviderDisconnected{provider, reason}`,
  `ProviderError{provider, message}`) carry free-text (`reason`, `message`) that does
  not belong on the POD hot path: surface these on the diagnostics plane and/or a small
  `FixedSizeByteString` control field. `SessionClosing` is a bare tag. Finalize the
  exact routing once Phase 2 fixes control scoping (P2-B). Mark as **RE-PLAN
  CHECKPOINT P2-3.**

A pure conversion `to_pod(&MarketEvent, &mut SymbolTable) -> DataPayload` lives next to
the payload type and is the single place the logical→POD mapping happens. A reverse
`from_pod(&DataPayload, &SymbolResolver) -> MarketEvent` lives in the subscriber-helper
module (Step 6) so the integration test and any in-tree consumer can reconstruct
logical events. `from_pod` must reconstruct `provider` (constant Alpaca) to rebuild a
faithful `Instrument`/`ControlKind`.

### Step 3 — Data-plane sink (`impl EventSink`)

Implement `Iceoryx2DataSink` in the transport crate:

- Constructor takes a service name derived per client (hierarchical, e.g.
  `"datamancer/data/{client_id}"`), opens/creates the iceoryx2 `Node` and a
  `publish_subscribe::<DataPayload>()` service via `open_or_create()`. Service builder
  config: `enable_safe_overflow(...)` per the backpressure decision (Open Questions —
  default to a non-silently-dropping mode and rely on core `Control::Gap`),
  `history_size(N)` modest for brief late-joiner catch-up, `max_subscribers` sized for
  the future fan-out node (small), caps over-provisioned per P2-2.
- `publish(&self, ev)` — intern instrument → `SymbolId` (announce if new, Step 4),
  `to_pod`, `loan_uninit()` / `write_payload()` / `send()` (verify against pinned API).
  Map send failure to the `EventSink` error type per P1-1.
- `flush(&self)` — iceoryx2 `send` copies into shared memory synchronously, so
  "buffered" here means: ensure all `publish` calls have completed and any pending
  announcement samples are sent. Because the resume buffer/tee are core-side (P1-B),
  `flush` does not drain application buffers — it is a fence on the publisher plus a
  final symbol-table announcement republish. Document this precisely.

### Step 4 — Symbol announcement service

A second iceoryx2 pub-sub service per client, `"datamancer/symbols/{client_id}"`,
payload = a POD `SymbolAnnouncement` (`SymbolId` + a `FixedSizeByteString` carrying the
serialized instrument tuple `provider|asset_class|symbol` — matching the actual
`Instrument` fields at `instrument.rs:91-95`, capacity-checked; instruments longer than
the cap are a hard error logged once). Configured with `history_size >=` the symbol cap
so a late joiner reads **all** prior announcements from retained history.

**History caveat (research brief):** pub-sub `history_size` only delivers history while
the **publisher process is alive** — exactly our case (the server publishes for the
life of the client service), so retained-history late-join works. We do **not** rely on
persistence-after-writer-exit (that is the blackboard pattern, out of scope). If a late
joiner attaches after some announcements have aged out of `history_size`, the sink
republishes the full table on a low-rate timer and on `flush`; subscribers treat
announcements as idempotent upserts keyed by `SymbolId`. Mark the
republish-vs-pure-history choice as a verify-at-impl item.

### Step 5 — Diagnostics-plane publisher

A third iceoryx2 service, single instance (not per client — one diagnostics service the
operator/clients subscribe to), `"datamancer/diagnostics"`. Payload = fixed-capacity
byte-slice (`publish_subscribe::<[u8]>()` or a `FixedSizeByteString`-style container
sized to the max serialized snapshot, P3-1). A background task — embedded in the
existing tokio runtime, **never a second runtime** — periodically:

1. Calls the Phase-3 snapshot producer (P3-A).
2. Serializes it (codec per P3-1).
3. If it fits the fixed payload cap, publishes one sample with `history_size(1)` so late
   joiners get the current snapshot immediately; if it does not fit, applies the
   chunking scheme (Open Questions).

This plane is deliberately **not** zero-copy/POD — serialized bytes are fine at this
rate. Periodic publish is the assumed delivery mode (P3-2); request-response is the
recorded alternative. (Phase 4 builds the publisher component; Phase 5's daemon drives
its lifecycle.)

### Step 6 — Subscriber-side helper (test + future fan-out node)

Provide a small in-tree subscriber helper (behind the same feature, used by tests +
examples): opens the data + announcement services, maintains a `SymbolId → Instrument`
resolver fed by the announcement stream (with late-join history drain), and exposes
`from_pod` reconstruction. This is what the integration test and any future fan-out
node consume; it proves the wire format round-trips without shipping a public client SDK
in this phase.

**Cross-service ordering (correctness-critical).** The data service and the announcement
service are *two independent iceoryx2 services with no mutual delivery-order guarantee*.
A data sample referencing `SymbolId(k)` can therefore arrive at the subscriber **before**
the `SymbolAnnouncement` for `k`. The helper must handle an unresolved `SymbolId` by
**holding/queuing** the affected data samples until the announcement resolves it (and
draining announcement history on attach), rather than dropping or erroring. This is a
required behavior of the helper, not optional — see the new test
`data_before_announcement_resolves`.

### Step 7 — Flush / shutdown ordering

Ordering is load-bearing and must be explicit (mirrors the existing tap-log flush in
`Session::shutdown`, `session.rs:1513-1525`, where `SessionClosing` is emitted then
`log.flush()` runs at `session.rs:1523`). On graceful shutdown of a client session /
the server:

1. Stop accepting new subscriptions (Phase 5 owns "stop accepting"; here we expose the
   hook).
2. Core-side: the authoritative session emits `SessionClosing`; the tee flushes the tap
   log (existing path, `session.rs:1522-1523`).
3. **`EventSink::flush()`** on the iceoryx2 data sink — ensures the final data samples
   (including the `SessionClosing` control routed to `SymbolId::CONNECTION`) and a final
   symbol-table announcement republish are in shared memory before teardown.
4. Diagnostics task publishes one final snapshot (reflecting `closing` state) then stops.
5. Drop the iceoryx2 publishers/services last, after `flush` returns, so a subscriber
   mid-`receive()` still drains what is in the buffer.

Document the invariant: **tap-log flush before sink flush before service drop**; the
sink never drops samples that `flush` promised to deliver, but it makes no guarantee a
crashed/slow subscriber consumed them (same-host best-effort; cross-process backpressure
is a recorded deferral).

## Public API / type changes

Kept minimal and additive; per the constraints, **`MarketEvent` and `Instrument` are
untouched**, and `SymbolId`/interning are sink-local (roadmap line 391: "`SymbolId`/
interning are sink-local, not core").

- **New crate** `datamancer-transport-iceoryx2` (workspace member). Depends on
  `datamancer-core` (for `EventSink`, `MarketEvent`, `Instrument`, `ControlKind`,
  `Seq`, `Price`, `Timestamp`), iceoryx2 + `iceoryx2-bb-container`. Public items:
  - `Iceoryx2DataSink` (`impl datamancer_core::EventSink`) + a builder/constructor.
  - `Iceoryx2DiagnosticsPublisher` (takes a Phase-3 snapshot producer closure/handle).
  - Subscriber helper (`DataSubscriber`, `SymbolResolver`) — `pub` so the Phase-5
    fan-out / control work can reuse it (preferred over test-only).
  - `SymbolId`, `SymbolAnnouncement`, `DataPayload` — `pub` so external consumers can
    decode the wire format; documented as a transport-internal layout that may version.
- **`datamancer` crate:** new optional feature `transport-iceoryx2` that pulls in the
  new crate and re-exports the sink constructor + a builder method to wire it onto a
  client session (exact method per P2-1). No change to default features.
- **No `datamancer-core` changes** beyond what Phase 1 already added (`EventSink`). If
  Phase 1 did not expose a way to construct/own an external sink on the client-session
  builder, add that additive builder method in `datamancer` here (flagged P2-1).
- **Workspace `Cargo.toml`:** add the new member and pinned iceoryx2 deps under
  `[workspace.dependencies]` (version per EXT-A); add the member to `[lints] workspace`
  so pedantic/forbid apply. axum is **not** introduced here (that is Phase 6).

## Test plan

All iceoryx2 integration tests are feature-gated (`transport-iceoryx2`) and `#[ignore]`d
in CI if they need the iceoryx2 runtime (mirrors `alpaca_real.rs`), run explicitly with
`cargo test -p datamancer-transport-iceoryx2 -- --ignored` or a dedicated `--test`
target.

Unit tests (no runtime needed — these run in normal CI and protect the wire format):

- `pod_payload_compiles_under_forbid_unsafe` — the EXT-1 gate as a permanent guard:
  `DataPayload` + `SymbolAnnouncement` derive `ZeroCopySend` and the crate compiles
  (compile-time, asserted by the crate building under its forbid/deny lint).
- `to_pod_from_pod_round_trips` — every `MarketEvent` variant (Trade/Quote/Bar across
  each `BarInterval`; each per-symbol `Control`) survives `to_pod` → `from_pod`
  byte-identical for `(source_ts, rx_ts, seq, instrument, payload)`. **Regression guard
  for the timestamp-triple + seq preservation invariant**, including that `rx_ts` is
  carried, never synthesized.
- `symbol_table_round_trip` — `intern(instrument)` → announce → resolver
  `resolve(symbol_id)` returns the identical `Instrument`; reserved `CONNECTION` id is
  never assigned to a real instrument.
- `symbol_id_is_not_global_identity` — two independent tables may assign the same
  `SymbolId` to different instruments (documents per-service id semantics).
- `instrument_over_capacity_is_rejected` — an instrument string exceeding the
  `FixedSizeByteString` cap is a clean error, not a panic/truncation.
- `connection_scoped_control_routes_to_sentinel` — `ProviderConnected/Disconnected/
  Error` and `SessionClosing` map to `SymbolId::CONNECTION`; per-symbol `Gap`/
  `SubscriptionChanged` map to their real `SymbolId` and carry the Alpaca provider tag,
  not a heap string. **Regression guard for the per-symbol vs connection-scoped split
  (P2-B) and the provider-string-on-control catch.**
- `diagnostics_snapshot_serde_round_trips` — serialize→deserialize the Phase-3 snapshot
  through the chosen codec at the diagnostics payload cap; oversize triggers the
  chunking path (or a documented error if chunking deferred).

Integration tests (runtime-gated, `#[ignore]`):

- `data_plane_reconstructs_logical_stream` — publish a known multi-symbol sequence with
  per-symbol monotonic `seq`; a subscriber reconstructs the logical stream and asserts
  `(instrument, seq)` ordering holds **within** each symbol, with **no cross-symbol
  assertion** (per-symbol-only determinism). **Regression guard:
  `multiplex_is_per_symbol_only`.**
- `data_before_announcement_resolves` — feed the subscriber a data sample whose
  `SymbolId` has no announcement yet, then the announcement; the held sample resolves
  and reconstructs correctly. **Regression guard for the two-services-no-ordering edge
  (Step 6).**
- `late_joiner_resolves_all_symbols` — subscriber attaches mid-stream, drains
  announcement history, and resolves every `SymbolId` seen in data.
- `diagnostics_subscriber_reconstructs_phase3_snapshot` — a subscriber reads the
  diagnostics service and deserializes a valid Phase-3 snapshot; a late joiner gets the
  current snapshot from `history_size(1)`.
- `flush_drains_before_exit` — publish, then `flush()`, then drop services; a subscriber
  started before drop reads every published sample including the final `SessionClosing`
  routing. **Regression guard for shutdown ordering (Step 7); mirrors the tap-log flush
  test.**
- `two_clients_same_symbol_see_identical_seq` — two per-client data services carrying the
  same instrument deliver identical `(seq, source_ts, payload)` for that symbol
  (cross-client agreement, the Phase-1 P1-C guarantee observed through the transport).

## Doc / invariant updates

- `crates/datamancer/README.md` (authoritative design doc): add a "Transports" section
  documenting the two-plane model, that `SymbolId`/interning are sink-local and **not** a
  public-API or global-identity concept, that the data plane carries the
  per-symbol-deterministic `(instrument, seq)` interleave (restate the cross-symbol
  non-goal), and the two-services-have-no-mutual-order subscriber rule (Step 6).
- New `crates/datamancer-transport-iceoryx2/CLAUDE.md` (mirror the core crate's CLAUDE.md
  style): records the `forbid(unsafe_code)` stance (forbid, or the single scoped `allow`
  with SAFETY proof if EXT-1 fails), the pinned iceoryx2 version, the
  wire-format-may-version disclaimer, and the rule that all payloads use the
  `ZeroCopySend` derive + fixed-size containers only.
- Root `CLAUDE.md` "Workspace" + "Scope reminders": note the new optional crate and the
  `transport-iceoryx2` feature; reaffirm both core crates keep `#![forbid(unsafe_code)]`.
- The timestamp-triple invariant (root `CLAUDE.md`): state that the POD payload preserves
  all three fields and that `rx_ts` remains observability-only across the transport
  boundary (never reconstructed/synthesized by the subscriber helper).
- Flush/shutdown-ordering invariant (Step 7) documented in the README's lifecycle
  section.
- (Note: the `seq`/consumer-model invariant rewrites land in Phases 1–2 per roadmap
  lines 370–376; Phase 4 only *references* the post-Phase-1/2 wording, it does not edit
  `event.rs`.)

## Open questions

- **Data-plane backpressure vs resume buffer.** iceoryx2 `enable_safe_overflow(true)` is
  lossy-newest-wins; `false` is backpressure. With the resume buffer core-side (P1-B),
  the safest mapping is: keep the resume buffer authoritative for "what was missed →
  `Control::Gap`", and run the iceoryx2 service in a mode that does not silently drop
  without accounting. Final choice needs the Phase-2 buffering-granularity decision
  (P1-2) and a same-host throughput measurement. Cross-process backpressure *policy* is
  an explicit roadmap deferral. **Open.**
- **Diagnostics delivery mode** — periodic publish (assumed, P3-2) vs request-response;
  request-response pairs naturally with Phase 5's control surface. Defer to Phase 5.
- **Diagnostics payload size / chunking** — if a serialized Phase-3 snapshot can exceed
  the fixed iceoryx2 payload cap, a fragmentation/reassembly scheme (or a generous fixed
  cap) is needed. Depends on P3-1 (codec + max size).
- **String-bearing controls on the wire** (P2-3) — diagnostics plane vs a
  `FixedSizeByteString` control sub-channel for `reason`/`message`; provider field is an
  Alpaca-constant tag. Resolve with Phase 2's control routing.
- **`SymbolId` width and cap** — `u32` with an over-provisioned per-service cap is
  assumed; confirm against realistic subscription counts and the fixed-service-resource
  constraint (P2-2).
- **iceoryx2 version pin** (EXT-A) and exact method surface for pub-sub, history, and the
  `ZeroCopySend` derive — verify at impl time against the pinned version.

## Risks

- **`forbid(unsafe_code)` gate (EXT-1) fails.** If any required payload cannot derive
  `ZeroCopySend` safely, the new crate relaxes to a single scoped `allow`. Mitigation:
  spike the gate **first** (Step 0); the two named crates never relax.
- **iceoryx2 version churn / API drift.** Builder surface differs across 0.8.x↔0.9.x.
  Mitigation: pin one version, treat every API reference here as verify-at-impl, keep
  all iceoryx2 contact inside the one crate.
- **Two services have no mutual delivery order** (data vs announcement). A data sample
  can outrun its `SymbolId` announcement. Mitigation: subscriber holds unresolved
  samples + drains history (Step 6); guarded by `data_before_announcement_resolves`.
- **Fixed service resources vs runtime sub/unsub** (P2-2). Mitigation: payload size is
  symbol-count-independent (flat POD over event kinds); over-provision symbol/
  announcement caps and document the ceiling.
- **Late-joiner history depends on publisher liveness.** Acceptable because the server
  publishes for the service's life; persistence-after-exit (blackboard) is out of scope.
  Mitigation: periodic full-table republish + idempotent upsert.
- **Per-symbol-only determinism eroded by a tempting global merge.** The multiplex is an
  interleave; any path that sorts across symbols violates the non-goal. Mitigation: the
  `multiplex_is_per_symbol_only` guard and explicit README/CLAUDE text.
- **Phase-1 `seq` inversion incomplete.** The cross-client agreement guarantee depends
  on Phase 1 having moved `seq` from delivery-stamped (current `lib.rs:690`) to
  source-stamped per-symbol (P1-C). Mitigation: `two_clients_same_symbol_see_identical_seq`
  fails loudly if it has not.
- **CI ergonomics.** Runtime tests `#[ignore]`d like `alpaca_real.rs`; the unit-level
  POD/round-trip/compile guards run in normal CI. Mitigation: split per the Test plan.
- **Cross-layer dependency on three unfinished phases.** Decisions are checkpointed
  (P1-1/2, P2-1/2/3, P3-1/2, EXT-1). Mitigation: design-level by charter; revisit each
  RE-PLAN CHECKPOINT before writing integration code.

## Review notes

Changes made to the draft during adversarial review (claims verified against the
current tree and the roadmap at
`docs/superpowers/specs/2026-06-28-datamancer-server-roadmap.md`):

- **Corrected a code line claim.** `tee()` is at `session.rs:1442`, not 1438 (draft was
  off). `forward()` (1411), `EventRing` (1659), `RegistrySentinel` (1538), and the
  tap-log `flush()` (1523) were verified accurate; tap-log flush context tightened to
  `Session::shutdown` (1513–1525).
- **Surfaced the `seq` contrast.** Current `seq` is session-monotonic and stamped *at
  delivery* (`datamancer-core/src/lib.rs:670-691`, `seq_counter` at `session.rs:289`) —
  the opposite of the per-symbol source-stamped model Phase 4 relies on. Elevated P1-C
  to "most load-bearing" and added a dedicated risk so the dependency is an explicit
  checkpoint, not a hidden guess.
- **Fixed a payload-layout correctness error.** The draft said per-symbol controls carry
  "only booleans/ids inline." In fact (`event.rs:135-161`) both per-symbol controls
  carry a `provider: String` (`Gap` and `SubscriptionChanged`), and `SubscriptionChanged`
  carries `EventKind`/`active`. Since the roadmap is Alpaca-only, specified `provider`
  as a `#[repr(u8)]` tag and routed only the free-text connection-scoped fields
  (`reason`/`message`) off the hot path. Added a regression assertion to the control
  test.
- **Grounded the POD claim.** Confirmed `Price(pub i64)` and `Timestamp(pub i64)` are
  `Copy` scalars and `Instrument` is `(provider, asset_class, symbol)` — the
  announcement tuple encoding now matches the real type, and the i64 field choices are
  stated as facts rather than "verify."
- **Added a real correctness gap + test.** Two iceoryx2 services have no mutual delivery
  ordering, so a data sample can arrive before its `SymbolId` announcement. Added the
  subscriber hold/queue requirement (Step 6), a risk, and a new integration test
  `data_before_announcement_resolves`. The draft's "eager announce before first data"
  was softened (it does not cross-service-order).
- **Naming-collision note.** Flagged the existing internal `Sink` enum (`session.rs:719`)
  so the new `EventSink` trait is not confused with it.
- **Scoped the doc edits correctly.** Clarified that the `seq`/consumer-model invariant
  rewrites land in Phases 1–2 (roadmap 370–376); Phase 4 references, not edits,
  `event.rs`. Pinned every checkpoint/assumption to a roadmap line where one exists, so
  dependency edges are explicit RE-PLAN CHECKPOINTs rather than guesses.
- **Altitude.** Left the design at the requested tier — committed architecture, crate
  placement, payload shape, topology, flush ordering — while keeping all iceoryx2 method
  surface, codec choice, field offsets, and Phase-5 lifecycle as verify-at-impl /
  later-phase items.

Unresolved concerns (genuinely blocked on upstream phases, all checkpointed):
- The `EventSink` exact signature (P1-1) and resume-buffer granularity (P1-2) — Phase 1/2.
- Client-session type identity and the sink wiring site (P2-1) — Phase 2.
- Control routing for free-text controls (P2-3) — Phase 2.
- Snapshot codec + max size, hence diagnostics payload cap and chunking (P3-1) — Phase 3.
- The `forbid(unsafe_code)` derive gate (EXT-1) — must be spiked before crate-placement
  is final.
